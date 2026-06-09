#!/usr/bin/env python3
"""
Traffic Generator Server
Receives TCP or UDP connections and collects per-connection statistics.
"""

import asyncio
import argparse
import atexit
import hashlib
import os
import platform
import resource
import signal
import socket
import statistics
import struct
import sys
import textwrap
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# Dashboard integration
try:
    from metrics_reporter import (
        MetricsReporter,
        MetricsConfig,
        ServerMetricsTracker,
        start_metrics_reporting
    )
    DASHBOARD_AVAILABLE = True
except ImportError:
    DASHBOARD_AVAILABLE = False


# Timestamp cache for performance optimization
_TIMESTAMP_CACHE = {}

# TCP_INFO socket option constant (Linux-specific)
_TCP_INFO_SOCKOPT = 11

def _format_timestamp() -> str:
    """Optimized timestamp formatting with caching. Returns ISO-8601 format with milliseconds."""
    t = time.time()
    sec = int(t)
    # Cache the formatted second to avoid repeated strftime calls
    if sec not in _TIMESTAMP_CACHE:
        # Keep cache small (last 60 seconds) - clear before adding new entry
        if len(_TIMESTAMP_CACHE) > 60:
            _TIMESTAMP_CACHE.clear()
        _TIMESTAMP_CACHE[sec] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sec))
    ms = int((t % 1) * 1000)
    return f"{_TIMESTAMP_CACHE[sec]}.{ms:03d}"

# Must match client.py constants
_FILE_HEADER_PREFIX = b"TGEN_FILE:"
_FILE_HEADER_LEN = len(_FILE_HEADER_PREFIX) + 64 + 1 + 10 + 1  # 86 bytes
_SERVER_MAX_FILE_SIZE = 128 * 1024 * 1024  # 128 MiB — matches client limit

# Global flag for quiet mode
_QUIET_MODE = False
_OUTPUT_FILE = None


def _log_connection(message: str):
    """Log connection messages respecting quiet mode and output file."""
    if _OUTPUT_FILE:
        _OUTPUT_FILE.write(message + "\n")
        _OUTPUT_FILE.flush()
    if not _QUIET_MODE:
        print(message)


class Tee:
    def __init__(self, filename):
        self.stdout = sys.stdout
        self.file = None
        try:
            self.file = open(filename, "a")
            atexit.register(self.close)
        except Exception:
            if self.file:
                self.file.close()
            raise

    def write(self, data):
        self.stdout.write(data)
        if self.file:
            self.file.write(data)

    def flush(self):
        self.stdout.flush()
        if self.file and not self.file.closed:
            self.file.flush()

    def close(self):
        if self.file and not self.file.closed:
            self.file.close()


@dataclass(slots=True)
class ConnectionRecord:
    """Connection record with __slots__ for memory efficiency (Python 3.10+)."""
    conn_id: int
    client_addr: str
    connect_time: float
    disconnect_time: Optional[float] = None
    disconnect_reason: str = "normal"
    bytes_received: int = 0
    messages_received: int = 0
    # File-transfer fields (populated only in file-transfer mode)
    file_transfer: bool = False
    checksum_ok: Optional[bool] = None   # True=PASS, False=FAIL, None=N/A
    # TCP statistics (Linux only)
    tcp_retransmits: Optional[int] = None
    tcp_rtt_ms: Optional[float] = None
    tcp_rtt_var_ms: Optional[float] = None
    tcp_snd_cwnd: Optional[int] = None
    tcp_lost_packets: Optional[int] = None
    tcp_reordering: Optional[int] = None
    # UDP statistics (simple packet counting)
    udp_expected_packets: int = 0  # Expected total packets (from client)
    udp_highest_seq: int = -1  # Highest sequence number seen
    udp_lost_packets: int = 0

    @property
    def duration(self) -> Optional[float]:
        if self.disconnect_time is not None and self.connect_time is not None:
            return self.disconnect_time - self.connect_time
        return None

    @property
    def pps_observed(self) -> Optional[float]:
        """Packets per second observed for this connection."""
        dur = self.duration
        if dur is not None and dur > 0:
            return self.messages_received / dur
        return None

    def row(self, show_tcp_stats: bool = False, show_udp_stats: bool = False) -> str:
        dur = f"{self.duration:.3f}s" if self.duration is not None else "open  "
        pps = f"{self.pps_observed:.1f}" if self.pps_observed is not None else "  -  "
        checksum_col = ""
        if self.file_transfer:
            if self.checksum_ok is True:
                checksum_col = " | PASS"
            elif self.checksum_ok is False:
                checksum_col = " | FAIL"
            else:
                checksum_col = " | ?"
        
        base_row = (
            f"  {self.conn_id:>6} | {self.client_addr:<22} | "
            f"{dur:>10} | {self.bytes_received:>8}B | {self.messages_received:>5} msg | {pps:>8} pkt/s"
            f"{checksum_col}"
        )
        
        if show_tcp_stats and self.tcp_retransmits is not None:
            tcp_stats = f" | retx={self.tcp_retransmits:>3}"
            if self.tcp_rtt_ms is not None:
                tcp_stats += f" rtt={self.tcp_rtt_ms:>5.1f}ms"
            if self.tcp_lost_packets is not None:
                tcp_stats += f" lost={self.tcp_lost_packets:>3}"
            base_row += tcp_stats
        
        if show_udp_stats and self.udp_expected_packets > 0:
            # Calculate loss percentage: (expected - received) / expected
            loss_pct = (self.udp_lost_packets / self.udp_expected_packets * 100) if self.udp_expected_packets > 0 else 0
            udp_stats = f" | exp={self.udp_expected_packets:>4} rcv={self.messages_received:>4} lost={self.udp_lost_packets:>3} ({loss_pct:>4.1f}%)"
            base_row += udp_stats
        
        return base_row


# Disconnect reason labels for TCP statistics formatting
_DISCONNECT_LABELS = [
    ("normal", "normal"),
    ("RST", "RST"),
    ("aborted", "aborted"),
    ("broken", "broken_pipe"),
    ("incomplete", "incomplete"),
    ("other", "other"),
]


@dataclass
class SummaryStats:
    """Aggregated statistics from a single pass over records."""
    total_records: int = 0
    completed_records: List[ConnectionRecord] = field(default_factory=list)
    total_packets: int = 0
    total_bytes: int = 0
    
    # File transfer stats
    ft_records: List[ConnectionRecord] = field(default_factory=list)
    ft_pass: int = 0
    ft_fail: int = 0
    
    # TCP stats
    tcp_records: List[ConnectionRecord] = field(default_factory=list)
    tcp_all_records: List[ConnectionRecord] = field(default_factory=list)
    has_tcp_stats: bool = False
    
    # UDP stats
    udp_records: List[ConnectionRecord] = field(default_factory=list)
    has_udp_stats: bool = False
    
    # Flags
    has_ft: bool = False


class ServerStats:
    def __init__(self):
        self.records: List[ConnectionRecord] = []
        self.start_time = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None
        self._next_id = 1

    def _get_lock(self) -> asyncio.Lock:
        """Lazy-initialize the lock inside a running event loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock  # always non-None after assignment above

    async def new_connection(self, client_addr: str) -> ConnectionRecord:
        async with self._get_lock():
            rec = ConnectionRecord(
                conn_id=self._next_id,
                client_addr=client_addr,
                connect_time=time.monotonic(),
            )
            self._next_id += 1
            self.records.append(rec)
        return rec

    def new_connection_sync(self, client_addr: str) -> ConnectionRecord:
        """Synchronous variant for use in non-async callbacks (e.g. UDP datagram_received).
        Safe because the event loop is single-threaded; no lock needed."""
        rec = ConnectionRecord(
            conn_id=self._next_id,
            client_addr=client_addr,
            connect_time=time.monotonic(),
        )
        self._next_id += 1
        self.records.append(rec)
        return rec

    def summary(self) -> str:
        """Generate comprehensive server statistics summary."""
        elapsed = time.monotonic() - self.start_time
        
        # Single-pass data collection
        stats = self._collect_summary_stats()
        
        # Calculate derived metrics
        rate = stats.total_records / elapsed if elapsed > 0 else 0
        
        # Build summary sections
        lines = self._format_header_section(
            elapsed, stats.total_records, len(stats.completed_records),
            rate, stats.total_bytes, stats.total_packets,
            len(stats.ft_records), stats.ft_pass, stats.ft_fail
        )
        
        # Duration and PPS statistics
        lines.extend(self._format_duration_pps_stats(stats.completed_records))
        
        # Connection details table
        if self.records:
            lines.extend(self._format_connection_table(
                stats.has_ft, stats.has_tcp_stats, stats.has_udp_stats
            ))
            
            # Protocol-specific statistics
            lines.extend(self._format_tcp_statistics(stats.tcp_records, stats.tcp_all_records))
            lines.extend(self._format_udp_statistics(stats.udp_records))
        
        # Footer
        lines.extend([
            "",
            f"  Final total bytes received: {stats.total_bytes}",
            "=" * 92
        ])
        
        return "\n".join(lines)
    
    def _collect_summary_stats(self) -> SummaryStats:
        """Collect all statistics in a single pass over records."""
        stats = SummaryStats()
        
        for r in self.records:
            stats.total_records += 1
            stats.total_packets += r.messages_received
            stats.total_bytes += r.bytes_received
            
            if r.duration is not None:
                stats.completed_records.append(r)
            
            if r.file_transfer:
                stats.has_ft = True
                stats.ft_records.append(r)
                if r.checksum_ok is True:
                    stats.ft_pass += 1
                elif r.checksum_ok is False:
                    stats.ft_fail += 1
            
            if r.tcp_retransmits is not None:
                stats.has_tcp_stats = True
                stats.tcp_records.append(r)
            
            if ":" in r.client_addr:  # TCP connection (IPv4 or IPv6 with colons)
                stats.tcp_all_records.append(r)
            
            if r.udp_expected_packets > 0:
                stats.has_udp_stats = True
                stats.udp_records.append(r)
        
        return stats
    
    def _format_header_section(
        self,
        elapsed: float,
        total: int,
        completed_count: int,
        rate: float,
        total_bytes: int,
        total_pkts: int,
        ft_count: int = 0,
        ft_pass: int = 0,
        ft_fail: int = 0
    ) -> List[str]:
        """Generate the header section of the summary."""
        lines = [
            "\n" + "=" * 92,
            "  SERVER SUMMARY",
            "=" * 92,
            f"  Elapsed time        : {elapsed:.2f}s",
            f"  Total connections   : {total}",
            f"  Completed           : {completed_count}",
            f"  Still open          : {total - completed_count}",
            f"  Observed rate       : {rate:.2f} conn/s",
            f"  Total bytes received: {total_bytes}",
            f"  Total packets recv  : {total_pkts}",
        ]
        
        if ft_count > 0:
            lines.append(
                f"  File transfers      : {ft_count} "
                f"(checksum PASS={ft_pass} FAIL={ft_fail})"
            )
        
        return lines
    
    def _format_duration_pps_stats(self, completed: List[ConnectionRecord]) -> List[str]:
        """Generate duration and PPS statistics for completed connections."""
        if not completed:
            return []
        
        # Filter out any None durations defensively (race at shutdown)
        durations = [r.duration for r in completed if r.duration is not None]
        if not durations:
            return []
        
        lines = [
            f"  Duration avg        : {sum(durations)/len(durations):.3f}s",
            f"  Duration min        : {min(durations):.3f}s",
            f"  Duration max        : {max(durations):.3f}s",
        ]
        
        # Exclude file-transfer connections from PPS stats (1 msg / ~0s = nonsensical)
        pps_vals = [
            r.pps_observed
            for r in completed
            if not r.file_transfer and r.pps_observed is not None
        ]
        
        if pps_vals:
            lines.extend([
                f"  PPS avg (recv)      : {sum(pps_vals)/len(pps_vals):.1f} pkt/s",
                f"  PPS min (recv)      : {min(pps_vals):.1f} pkt/s",
                f"  PPS max (recv)      : {max(pps_vals):.1f} pkt/s",
            ])
        
        return lines
    
    def _format_connection_table(
        self,
        has_ft: bool,
        has_tcp_stats: bool,
        has_udp_stats: bool
    ) -> List[str]:
        """Generate the connection details table."""
        checksum_hdr = " | Checksum" if has_ft else ""
        tcp_hdr = " | TCP Stats (retx/rtt/lost)" if has_tcp_stats else ""
        udp_hdr = " | UDP Stats (exp/rcv/lost/%)" if has_udp_stats else ""
        
        lines = [
            "",
            f"  {'ID':>6} | {'Client':<22} | {'Duration':>10} | {'Bytes':>9} | Messages | PPS (recv){checksum_hdr}{tcp_hdr}{udp_hdr}",
            "  " + "-" * (80 + (10 if has_ft else 0) + (35 if has_tcp_stats else 0) + (35 if has_udp_stats else 0)),
        ]
        
        for rec in self.records:
            lines.append(rec.row(show_tcp_stats=has_tcp_stats, show_udp_stats=has_udp_stats))
        
        return lines
    
    def _format_rtt_stats(self, label: str, values: List[float]) -> List[str]:
        """Format RTT statistics (avg/min/max) for a given metric.
        
        Args:
            label: Metric label (e.g., "RTT", "RTT var")
            values: List of metric values
            
        Returns:
            List of formatted statistics lines, empty if no values
        """
        if not values:
            return []
        
        avg = sum(values) / len(values)
        return [
            f"    {label} avg       : {avg:.2f}ms",
            f"    {label} min       : {min(values):.2f}ms",
            f"    {label} max       : {max(values):.2f}ms",
        ]
    
    def _format_disconnect_reasons(self, disconnect_counts: Dict[str, int]) -> List[str]:
        """Format disconnect reason statistics.
        
        Args:
            disconnect_counts: Dictionary mapping disconnect reasons to counts
            
        Returns:
            List of formatted disconnect reason lines
        """
        return [
            f"    Disconnect {label:<10}: {disconnect_counts[key]}"
            for label, key in _DISCONNECT_LABELS
        ]
    
    def _format_tcp_statistics(
        self,
        tcp_records: List[ConnectionRecord],
        tcp_all_records: List[ConnectionRecord]
    ) -> List[str]:
        """Generate TCP statistics summary section.
        
        Args:
            tcp_records: Records with TCP statistics (completed connections)
            tcp_all_records: All TCP records (including incomplete)
            
        Returns:
            List of formatted statistics lines
        """
        if not tcp_records:
            return []
        
        # Single-pass aggregation of TCP metrics
        total_retx = 0
        total_lost = 0
        tcp_retx_conns = 0
        tcp_loss_conns = 0
        rtt_vals = []
        rtt_var_vals = []
        
        for r in tcp_records:
            retx = r.tcp_retransmits or 0
            lost = r.tcp_lost_packets or 0
            
            total_retx += retx
            total_lost += lost
            
            if retx > 0:
                tcp_retx_conns += 1
            if lost > 0:
                tcp_loss_conns += 1
            
            if r.tcp_rtt_ms is not None:
                rtt_vals.append(r.tcp_rtt_ms)
            if r.tcp_rtt_var_ms is not None:
                rtt_var_vals.append(r.tcp_rtt_var_ms)
        
        # Calculate averages for all TCP connections using statistics.mean
        tcp_avg_bytes = statistics.mean(r.bytes_received for r in tcp_all_records) if tcp_all_records else 0.0
        tcp_avg_msgs = statistics.mean(r.messages_received for r in tcp_all_records) if tcp_all_records else 0.0
        
        # Disconnect reasons
        disconnect_counts = self._count_disconnect_reasons(tcp_all_records)
        
        lines = [
            "",
            "  TCP Statistics Summary:",
            f"    Total retransmits    : {total_retx}",
            f"    Total lost packets   : {total_lost}",
            f"    Connections w/ retx  : {tcp_retx_conns}",
            f"    Connections w/ loss  : {tcp_loss_conns}",
            f"    Avg bytes / conn     : {tcp_avg_bytes:.1f}",
            f"    Avg msgs / conn      : {tcp_avg_msgs:.1f}",
        ]
        
        lines.extend(self._format_rtt_stats("RTT", rtt_vals))
        lines.extend(self._format_rtt_stats("RTT var", rtt_var_vals))
        lines.extend(self._format_disconnect_reasons(disconnect_counts))
        
        return lines
    
    def _count_disconnect_reasons(self, records: List[ConnectionRecord]) -> Dict[str, int]:
        """Count disconnect reasons for TCP connections."""
        reasons = {"normal": 0, "RST": 0, "aborted": 0, "broken_pipe": 0, "incomplete": 0, "other": 0}
        known_reasons = {"normal", "RST", "aborted", "broken_pipe", "incomplete"}
        
        for r in records:
            if r.disconnect_reason in known_reasons:
                reasons[r.disconnect_reason] += 1
            else:
                reasons["other"] += 1
        
        return reasons
    
    def _format_udp_statistics(self, udp_records: List[ConnectionRecord]) -> List[str]:
        """Generate UDP statistics summary section."""
        if not udp_records:
            return []
        
        total_expected = sum(r.udp_expected_packets for r in udp_records)
        total_received = sum(r.messages_received for r in udp_records)
        total_lost = sum(r.udp_lost_packets for r in udp_records)
        loss_pct = (total_lost / total_expected * 100) if total_expected > 0 else 0
        
        return [
            "",
            "  UDP Statistics Summary:",
            f"    Total expected    : {total_expected}",
            f"    Total received    : {total_received}",
            f"    Total lost        : {total_lost} ({loss_pct:.2f}%)",
        ]



def calculate_timeout(cps: float = 0) -> float:
    """
    Calculate connection timeout based on connections per second (CPS).
    Higher CPS = more server load = need longer timeout to avoid premature disconnects.
    
    This matches the client-side timeout calculation to ensure compatibility.
    
    Args:
        cps: Connections per second rate (0 = use default)
    
    Returns:
        Timeout in seconds
    """
    if cps >= 1000:
        return 120.0  # Very high rate: 2 minutes
    elif cps >= 500:
        return 90.0   # High rate: 1.5 minutes
    elif cps >= 100:
        return 60.0   # Medium-high rate: 1 minute
    elif cps >= 50:
        return 45.0   # Medium rate: 45 seconds
    else:
        return 30.0   # Low rate: 30 seconds (default)

# ─── TCP ─────────────────────────────────────────────────────────────────────

# TCP keepalive constants (always enabled for TCP connections)
_KA_IDLE = 10      # seconds idle before first probe
_KA_INTERVAL = 10  # seconds between probes
_KA_COUNT = 5      # unacked probes before dropping


def apply_tcp_optimizations(sock: socket.socket) -> None:
    """Apply TCP optimizations: keepalive, nodelay, and buffer sizes."""
    # Enable TCP keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    
    # Disable Nagle's algorithm for lower latency (TCP_NODELAY)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    
    # Increase socket buffers for high throughput (256KB)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 262144)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
    except OSError:
        pass  # Ignore if system doesn't allow buffer size changes

    system = platform.system()
    if system == "Linux":
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, _KA_IDLE)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, _KA_INTERVAL)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, _KA_COUNT)
    elif system == "Darwin":  # macOS
        TCP_KEEPALIVE = 0x10  # equivalent to TCP_KEEPIDLE on macOS
        sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, _KA_IDLE)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, _KA_INTERVAL)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, _KA_COUNT)


def apply_keepalive(sock: socket.socket) -> None:
    """Deprecated: Use apply_tcp_optimizations() instead."""
    apply_tcp_optimizations(sock)


def get_tcp_info(sock: socket.socket) -> Tuple[Optional[int], Optional[float], Optional[float],
                                                 Optional[int], Optional[int], Optional[int]]:
    """
    Retrieve TCP socket statistics (Linux only).
    Returns: (retransmits, rtt_ms, rtt_var_ms, snd_cwnd, lost_packets, reordering)
    """
    if platform.system() != "Linux":
        return (None, None, None, None, None, None)

    try:
        # Get TCP_INFO structure (size varies by kernel version, but we only need first ~200 bytes)
        # This can raise OSError (errno 5 = EIO) if socket is already closed or in bad state
        tcp_info = sock.getsockopt(socket.IPPROTO_TCP, _TCP_INFO_SOCKOPT, 256)

        # Parse relevant fields from tcp_info structure
        # Offsets based on Linux kernel struct tcp_info (kernel 4.x+, x86_64)

        # Stable offsets for commonly used struct tcp_info fields:
        # tcpi_retransmits (u8) at offset 2
        # tcpi_lost (u32) at offset 32
        # tcpi_rtt (u32) at offset 68 (microseconds)
        # tcpi_rttvar (u32) at offset 72 (microseconds)
        # tcpi_snd_cwnd (u32) at offset 80
        # tcpi_reordering (u32) at offset 88

        retransmits = struct.unpack_from('B', tcp_info, 2)[0]   # tcpi_retransmits (u8)
        rtt_us = struct.unpack_from('I', tcp_info, 68)[0]       # tcpi_rtt (u32, microseconds)
        rtt_var_us = struct.unpack_from('I', tcp_info, 72)[0]   # tcpi_rttvar (u32, microseconds)
        snd_cwnd = struct.unpack_from('I', tcp_info, 80)[0]     # tcpi_snd_cwnd (u32)
        lost = struct.unpack_from('I', tcp_info, 32)[0]         # tcpi_lost (u32)
        reordering = struct.unpack_from('I', tcp_info, 88)[0]   # tcpi_reordering (u32)

        # Convert microseconds to milliseconds
        rtt_ms = rtt_us / 1000.0 if rtt_us > 0 else None
        rtt_var_ms = rtt_var_us / 1000.0 if rtt_var_us > 0 else None

        return (retransmits, rtt_ms, rtt_var_ms, snd_cwnd, lost, reordering)
    except (OSError, struct.error):
        # Socket errors (e.g., errno 5 = EIO when socket already closed)
        # or struct parsing failures - both expected when client disconnects abruptly
        return (None, None, None, None, None, None)
async def _send_response(writer: asyncio.StreamWriter, message: bytes) -> None:
    """Send response message and ensure it's flushed."""
    writer.write(message)
    await writer.drain()


async def _parse_file_header(
    reader: asyncio.StreamReader,
    first_chunk: bytes,
    timeout: float
) -> Tuple[str, int, bytes]:
    """
    Parse file transfer header and return (expected_sha256, expected_size, leftover_bytes).
    Raises ValueError with user-friendly message on parse failure.
    """
    # Use readexactly for cleaner buffering
    needed = _FILE_HEADER_LEN - len(first_chunk)
    if needed > 0:
        try:
            more = await asyncio.wait_for(
                reader.readexactly(needed),
                timeout=timeout
            )
            header = first_chunk + more
        except asyncio.IncompleteReadError:
            raise ValueError("incomplete header")
    else:
        header = first_chunk[:_FILE_HEADER_LEN]
    
    leftover = first_chunk[_FILE_HEADER_LEN:] if len(first_chunk) > _FILE_HEADER_LEN else b""
    
    # Parse: TGEN_FILE:<sha256>:<size>\n
    try:
        inner = header[len(_FILE_HEADER_PREFIX):].rstrip(b"\n")
        sha256_expected, size_str = inner.split(b":", 1)
        return sha256_expected.decode(), int(size_str.decode()), leftover
    except Exception:
        raise ValueError("malformed header")


async def _receive_and_hash_file(
    reader: asyncio.StreamReader,
    expected_size: int,
    leftover: bytes,
    timeout: float,
    chunk_size: int = 65536
) -> Tuple[int, str]:
    """
    Stream file data, compute SHA256, return (bytes_read, sha256_hex).
    """
    sha256_hasher = hashlib.sha256()
    bytes_read = 0
    
    if leftover:
        sha256_hasher.update(leftover)
        bytes_read = len(leftover)
    
    while bytes_read < expected_size:
        to_read = min(chunk_size, expected_size - bytes_read)
        chunk = await asyncio.wait_for(reader.read(to_read), timeout=timeout)
        if not chunk:
            break
        sha256_hasher.update(chunk)
        bytes_read += len(chunk)
    
    return bytes_read, sha256_hasher.hexdigest()



async def _handle_file_transfer(reader: asyncio.StreamReader,
                                 writer: asyncio.StreamWriter,
                                 rec: ConnectionRecord,
                                 first_chunk: bytes,
                                 timeout: float = 30.0) -> None:
    """
    Handle a file-transfer connection.

    Protocol (client sends):
        TGEN_FILE:<sha256hex(64)>:<size(10 digits)>\n  [86 bytes]
        <raw file bytes>

    Server responds with a single line:
        OK\n          — checksum matched
        FAIL:<msg>\n  — checksum mismatch or error
    """
    rec.file_transfer = True

    # Parse header
    try:
        expected_sha256, expected_size, leftover = await _parse_file_header(
            reader, first_chunk, timeout
        )
    except ValueError as e:
        await _send_response(writer, f"FAIL:{e}\n".encode())
        rec.checksum_ok = False
        return

    # Enforce server-side file size limit to prevent memory exhaustion
    if expected_size > _SERVER_MAX_FILE_SIZE:
        await _send_response(writer, b"FAIL:file too large\n")
        rec.checksum_ok = False
        _log_connection(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | "
            f"FAIL file too large ({expected_size}B)"
        )
        return

    # Receive and hash file
    bytes_read, actual_sha256 = await _receive_and_hash_file(
        reader, expected_size, leftover, timeout
    )
    
    # Update connection record
    rec.bytes_received += bytes_read
    rec.messages_received = 1

    # Validate size
    if bytes_read != expected_size:
        reason = f"size mismatch: got {bytes_read} expected {expected_size}"
        await _send_response(writer, f"FAIL:{reason}\n".encode())
        rec.checksum_ok = False
        _log_connection(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | FAIL {reason}"
        )
        return

    # Validate checksum
    rec.checksum_ok = (actual_sha256 == expected_sha256)
    
    if rec.checksum_ok:
        await _send_response(writer, b"OK\n")
        _log_connection(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | "
            f"{bytes_read}B | checksum=PASS"
        )
    else:
        reason = f"sha256 got={actual_sha256[:16]}... expected={expected_sha256[:16]}..."
        await _send_response(writer, f"FAIL:{reason}\n".encode())
        _log_connection(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | "
            f"{bytes_read}B | checksum=FAIL"
        )


async def _read_normal_tcp_stream(reader: asyncio.StreamReader, initial_buf: bytes, timeout: float = 30.0) -> tuple[int, int]:
    """Read a normal TCP stream and return total bytes and message count."""
    total_bytes = 0
    message_count = 0

    if initial_buf:
        total_bytes += len(initial_buf)
        message_count += 1

    while True:
        data = await asyncio.wait_for(
            reader.read(65536),
            timeout=timeout
        )
        if not data:
            break
        total_bytes += len(data)
        message_count += 1

    return total_bytes, message_count


async def handle_tcp_client(reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter,
                             stats: ServerStats,
                             dashboard_tracker=None,
                             timeout: float = 30.0):
    addr = writer.get_extra_info("peername")
    addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"

    # Always apply TCP optimizations on accepted TCP sockets
    sock = writer.get_extra_info("socket")
    if sock is not None:
        apply_tcp_optimizations(sock)

    rec = await stats.new_connection(addr_str)
    timestamp = _format_timestamp()
    _log_connection(f"[{timestamp}] [{rec.conn_id:>6}] TCP connect    | {addr_str} ka=on")
    
    # Update dashboard
    if dashboard_tracker:
        dashboard_tracker.record_connection()

    disconnect_reason = "normal"
    exception_reason_map = {
        ConnectionResetError: "RST",
        ConnectionAbortedError: "aborted",
        BrokenPipeError: "broken_pipe",
        asyncio.IncompleteReadError: "incomplete",
    }

    try:
        # buffer enough bytes to determine if it's a file header
        prefix_len = len(_FILE_HEADER_PREFIX)
        
        # We need to read *at least* prefix_len bytes, but if the first read is smaller,
        # we still can't be sure it's not a file transfer (due to TCP stream fragmentation).
        buf: bytes = b""
        while len(buf) < prefix_len:
            # Apply timeout to initial read to prevent indefinite blocking
            chunk = await asyncio.wait_for(
                reader.read(min(65536, prefix_len - len(buf))),
                timeout=timeout
            )
            if not chunk:
                break
            buf += chunk

        if buf.startswith(_FILE_HEADER_PREFIX):
            # File-transfer mode: pass the buffered header prefix along
            await _handle_file_transfer(reader, writer, rec, buf, timeout)
        elif buf:
            rec.bytes_received, rec.messages_received = await _read_normal_tcp_stream(reader, buf, timeout)
    except asyncio.TimeoutError:
        # Connection timed out waiting for data
        disconnect_reason = "timeout"
        timestamp = _format_timestamp()
        _log_connection(f"[{timestamp}] [{rec.conn_id:>6}] TCP timeout    | {addr_str} | {timeout}s")
    except asyncio.CancelledError:
        disconnect_reason = "cancelled"
        raise  # Re-raise to allow proper cleanup
    except Exception as e:
        disconnect_reason = exception_reason_map.get(type(e))
        if disconnect_reason is None:
            if isinstance(e, OSError):
                disconnect_reason = f"error:{e.errno}" if hasattr(e, 'errno') else "error"
            else:
                disconnect_reason = f"exception:{type(e).__name__}"
    finally:
        # Collect TCP statistics before closing
        # Wrap in try/except to handle socket errors (e.g., socket already closed)
        if sock is not None:
            try:
                (rec.tcp_retransmits, rec.tcp_rtt_ms, rec.tcp_rtt_var_ms,
                 rec.tcp_snd_cwnd, rec.tcp_lost_packets, rec.tcp_reordering) = get_tcp_info(sock)
            except (OSError, Exception):
                # Socket may already be closed or in bad state
                # Set all TCP stats to None
                (rec.tcp_retransmits, rec.tcp_rtt_ms, rec.tcp_rtt_var_ms,
                 rec.tcp_snd_cwnd, rec.tcp_lost_packets, rec.tcp_reordering) = (None, None, None, None, None, None)
        
        # Update dashboard
        if dashboard_tracker:
            dashboard_tracker.record_disconnection()
            dashboard_tracker.record_bytes(rec.bytes_received, 0)
        
        rec.disconnect_reason = disconnect_reason
        rec.disconnect_time = time.monotonic()
        dur = rec.duration if rec.duration is not None else 0.0
        disconnect_timestamp = _format_timestamp()
        
        # Enhanced disconnect message with TCP stats and disconnect reason
        reason_suffix = f" ({disconnect_reason})" if disconnect_reason != "normal" else ""
        disconnect_msg = (
            f"[{disconnect_timestamp}] [{rec.conn_id:>6}] TCP disconnect | {addr_str} | "
            f"dur={dur:.3f}s | {rec.bytes_received}B{reason_suffix}"
        )
        if rec.tcp_retransmits is not None:
            disconnect_msg += f" | retx={rec.tcp_retransmits}"
            if rec.tcp_rtt_ms is not None:
                disconnect_msg += f" rtt={rec.tcp_rtt_ms:.1f}ms"
            if rec.tcp_lost_packets is not None and rec.tcp_lost_packets > 0:
                disconnect_msg += f" lost={rec.tcp_lost_packets}"
        _log_connection(disconnect_msg)
        
        # Close connection gracefully
        try:
            if not writer.is_closing():
                writer.close()
        except Exception:
            pass  # Ignore errors during close()
        
        # Wait for connection to fully close
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError, RuntimeError, asyncio.CancelledError):
            # Expected when client closes abruptly (RST, timeout, etc.)
            pass
        except Exception as e:
            # Unexpected error - log but don't crash
            if not _QUIET_MODE:
                timestamp = _format_timestamp()
                print(f"[{timestamp}] Warning: Cleanup error for {addr_str}: {type(e).__name__}: {e}")


async def run_tcp_server(host: str, port: int, stats: ServerStats, dashboard_tracker=None, timeout: float = 30.0):
    async def handler(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            await handle_tcp_client(r, w, stats, dashboard_tracker, timeout)
        except Exception as e:
            # Catch any unhandled exceptions to prevent them from propagating
            # to asyncio's internal error handler
            addr = w.get_extra_info("peername")
            addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
            timestamp = _format_timestamp()
            # Use repr() to get full exception details including type and errno
            print(f"[{timestamp}] Error handling TCP client {addr_str}: {type(e).__name__}: {e}")

    server = await asyncio.start_server(
        handler,
        host, port,
        backlog=65535,  # Increase from default 100 to handle high connection rates
        reuse_port=True  # Allow multiple server instances on same port (Linux 3.9+)
    )
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    print(f"TCP server listening on {addrs}")
    async with server:
        await server.serve_forever()


# ─── UDP ─────────────────────────────────────────────────────────────────────

class UDPServerProtocol(asyncio.DatagramProtocol):
    """
    UDP is connectionless; we track 'connections' per unique (host, port) pair.
    A connection is considered closed after `timeout` seconds of inactivity.
    """
    
    # Dynamic timeout calculation constants
    UDP_TIMEOUT_MULTIPLIER = 3.0  # Multiply packet interval by this factor
    UDP_TIMEOUT_MIN = 5.0         # Minimum timeout in seconds
    UDP_TIMEOUT_MAX = 300.0       # Maximum timeout in seconds

    def __init__(self, stats: ServerStats, timeout: float = 5.0, dashboard_tracker=None):
        self.stats = stats
        self.timeout = timeout
        self.dashboard_tracker = dashboard_tracker
        self._sessions: Dict[tuple, ConnectionRecord] = {}
        self._timers: Dict[tuple, asyncio.TimerHandle] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport):
        self._loop = asyncio.get_running_loop()
        self.transport = transport # type: ignore
        
        # Increase UDP receive buffer for high-throughput scenarios (60K+ connections)
        sock = transport.get_extra_info('socket')
        if sock:
            try:
                # Set receive buffer to 25MB to handle massive packet rates
                # Critical for 60K connections sending packets simultaneously
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 26214400)
            except OSError:
                pass  # Ignore if system doesn't allow buffer size changes

    def _parse_udp_packet(self, data: bytes, rec: ConnectionRecord) -> float:
        """
        Parse UDP packet to extract sequence number and calculate dynamic timeout.
        
        Args:
            data: The raw packet data
            rec: The connection record to update
            
        Returns:
            float: The calculated timeout value for this packet
        """
        # Default to standard timeout
        dynamic_timeout = self.timeout
        rec.messages_received += 1
        
        # Extract sequence number and interval from payload (first 8 bytes: seq_num + interval_ms)
        if len(data) >= 8:
            try:
                seq_num, interval_ms = struct.unpack('!II', data[:8])
                
                # Track the highest sequence number seen
                if seq_num > rec.udp_highest_seq:
                    rec.udp_highest_seq = seq_num
                
                # Calculate dynamic timeout based on packet interval
                # Use multiplier * interval as timeout, with min and max bounds
                if interval_ms > 0:
                    dynamic_timeout = max(
                        self.UDP_TIMEOUT_MIN,
                        min(self.UDP_TIMEOUT_MAX, (interval_ms / 1000.0) * self.UDP_TIMEOUT_MULTIPLIER)
                    )
                # else: interval_ms == 0 means single packet or no periodic traffic, use default
            except Exception:
                # If extraction fails, use defaults (already set above)
                pass
        
        return dynamic_timeout
    
    def _reset_session_timer(self, addr: tuple, timeout: float) -> None:
        """
        Reset the inactivity timer for a UDP session.
        
        Args:
            addr: The client address tuple (host, port)
            timeout: The timeout value in seconds
        """
        # Cancel existing timer if present
        if addr in self._timers:
            self._timers[addr].cancel()
        
        # Schedule new timer
        loop = self._loop
        if loop is not None:
            self._timers[addr] = loop.call_later(timeout, self._expire_session, addr)

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        """Handle incoming UDP datagram."""
        addr_str = f"{addr[0]}:{addr[1]}"

        # Get or create session record
        if addr not in self._sessions:
            # New "connection" — use sync helper (event loop is single-threaded here)
            rec = self.stats.new_connection_sync(addr_str)
            self._sessions[addr] = rec
            timestamp = _format_timestamp()
            _log_connection(f"[{timestamp}] [{rec.conn_id:>6}] UDP new sender | {addr_str}")
            
            # Update dashboard
            if self.dashboard_tracker:
                self.dashboard_tracker.record_connection()
        else:
            rec = self._sessions[addr]
        
        # Update basic stats
        rec.bytes_received += len(data)
        
        # Parse packet and calculate dynamic timeout
        dynamic_timeout = self._parse_udp_packet(data, rec)
        
        # Reset inactivity timer
        self._reset_session_timer(addr, dynamic_timeout)

    def _expire_session(self, addr: tuple):
        rec = self._sessions.pop(addr, None)
        self._timers.pop(addr, None)
        if rec:
            # Calculate expected packets from highest sequence number if not provided by client
            if rec.udp_expected_packets == 0 and rec.udp_highest_seq >= 0:
                # Sequence numbers are 0-based, so highest_seq + 1 = total expected
                rec.udp_expected_packets = rec.udp_highest_seq + 1
            
            # Calculate final loss: expected - received
            if rec.udp_expected_packets > 0:
                rec.udp_lost_packets = rec.udp_expected_packets - rec.messages_received
            
            # Update dashboard
            if self.dashboard_tracker:
                self.dashboard_tracker.record_disconnection()
                self.dashboard_tracker.record_bytes(rec.bytes_received, 0)
            
            rec.disconnect_time = time.monotonic()
            dur = rec.duration if rec.duration is not None else 0.0
            msg = (
                f"[{rec.conn_id:>6}] UDP expired    | {rec.client_addr} | "
                f"dur={dur:.3f}s | {rec.bytes_received}B"
            )
            if rec.udp_expected_packets > 0:
                loss_pct = (rec.udp_lost_packets / rec.udp_expected_packets * 100) if rec.udp_expected_packets > 0 else 0
                msg += f" | exp={rec.udp_expected_packets} rcv={rec.messages_received} lost={rec.udp_lost_packets} ({loss_pct:.1f}%)"
            print(msg)

    def error_received(self, exc):
        timestamp = _format_timestamp()
        # Identify specific UDP error types
        if isinstance(exc, ConnectionRefusedError):
            error_detail = "port_unreachable (ICMP)"
        elif isinstance(exc, OSError):
            if hasattr(exc, 'errno'):
                if exc.errno == 111:  # ECONNREFUSED
                    error_detail = "port_unreachable (ICMP)"
                elif exc.errno == 101:  # ENETUNREACH
                    error_detail = "network_unreachable"
                elif exc.errno == 113:  # EHOSTUNREACH
                    error_detail = "host_unreachable"
                elif exc.errno == 90:  # EMSGSIZE
                    error_detail = "message_too_long"
                else:
                    error_detail = f"error:{exc.errno} - {exc}"
            else:
                error_detail = str(exc)
        else:
            error_detail = f"{type(exc).__name__}: {exc}"
        
        print(f"[{timestamp}] UDP error: {error_detail}", file=sys.stderr)


async def run_udp_server(host: str, port: int, stats: ServerStats, dashboard_tracker=None):
    loop = asyncio.get_running_loop()  # get_event_loop() is deprecated in async context
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(stats, dashboard_tracker=dashboard_tracker),
        local_addr=(host, port),
        reuse_port=True  # Allow multiple server instances on same port (Linux 3.9+)
    )
    print(f"UDP server listening on {host}:{port}")
    try:
        await asyncio.Future()  # run forever
    finally:
        transport.close()


# ─── Entry Point ─────────────────────────────────────────────────────────────

async def run_server(args):
    stats = ServerStats()
    
    # Calculate timeout based on expected CPS
    timeout = calculate_timeout(args.cps if hasattr(args, 'cps') else 0)
    
    # Dashboard integration
    dashboard_reporter = None
    dashboard_tracker = None
    dashboard_task = None
    
    if hasattr(args, 'dashboard') and args.dashboard and DASHBOARD_AVAILABLE:
        config = MetricsConfig(
            dashboard_url=args.dashboard,
            report_interval=1.0,
            enabled=True
        )
        dashboard_reporter = MetricsReporter(config, 'server')
        dashboard_tracker = ServerMetricsTracker(
            protocol=args.protocol,
            port=args.port,
            processes=1
        )
        dashboard_task = asyncio.create_task(
            start_metrics_reporting(dashboard_reporter, dashboard_tracker, interval=1.0)
        )

    print("Starting traffic receiver")
    print(f"  Protocol : {args.protocol.upper()}")
    print(f"  Bind     : {args.host}:{args.port}")
    if args.protocol == "tcp":
        print(f"  Keepalive: ON (idle={_KA_IDLE}s, intvl={_KA_INTERVAL}s, cnt={_KA_COUNT})")
        print(f"  Timeout  : {timeout}s (read timeout per connection)")
    print("-" * 55)

    loop = asyncio.get_running_loop()

    def shutdown(*_):
        shutdown_msg = "\nShutting down..."
        summary_output = stats.summary()
        
        # Write to output file if configured
        if _OUTPUT_FILE:
            _OUTPUT_FILE.write(shutdown_msg + "\n")
            _OUTPUT_FILE.write(summary_output + "\n")
            _OUTPUT_FILE.flush()
        
        # Print to console unless in quiet mode
        if not _QUIET_MODE:
            print(shutdown_msg)
            print(summary_output)
        
        # Stop dashboard reporting
        if dashboard_task:
            dashboard_task.cancel()
        
        for task in asyncio.all_tasks(loop):
            task.cancel()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    try:
        if args.protocol == "tcp":
            await run_tcp_server(args.host, args.port, stats, dashboard_tracker, timeout)
        else:
            await run_udp_server(args.host, args.port, stats, dashboard_tracker)
    except asyncio.CancelledError:
        pass


_SERVER_HELP_EPILOG = textwrap.dedent("""\
SYNTAX
------
  python server.py --port PORT [options]

OPTIONS
-------
  --host HOST           Address to bind on
  --port PORT           Port to listen on (required)
  --protocol {tcp,udp}  Protocol to use
  --cps CPS             Expected connections per second (for timeout calculation)
  --output PATH         Optional file path to log the output results
  --quiet               Suppress per-connection messages (still written to output file)
  --dashboard URL       Dashboard metrics API URL
  -h, --help            Show this help message and exit

NOTES
-----
  * TCP keepalive is automatically enabled for all accepted TCP connections
    (idle=10s, interval=10s, count=5).
  * TCP read timeout is calculated based on expected CPS to prevent indefinite blocking:
    - CPS >= 1000: 120s timeout (very high load)
    - CPS >= 500:  90s timeout (high load)
    - CPS >= 100:  60s timeout (medium-high load)
    - CPS >= 50:   45s timeout (medium load)
    - CPS < 50:    30s timeout (default, low load)
  * UDP sessions are tracked per unique (host, port) pair with dynamic timeout:
    - Timeout = 3x packet interval (min: 5s, max: 300s)
    - Client sends interval in each packet for automatic adjustment
    - Example: pps=0.033 (30s interval) → timeout=90s
    - Example: pps=1 (1s interval) → timeout=5s (minimum)
  * Default values are shown automatically in the argparse options list.
  * Press Ctrl+C to stop the server and print the full statistics summary.

EXAMPLES
--------
  # TCP server on port 9000 (default 30s timeout)
  python server.py --port 9000

  # TCP server expecting high load (90s timeout)
  python server.py --port 9000 --cps 500

  # UDP server on port 9001
  python server.py --port 9001 --protocol udp

  # TCP server bound to a specific interface, logging output to a file
  python server.py --host 192.168.1.10 --port 9000 --output server.log
""")


def _port_type(value: str) -> int:
    """Validate TCP/UDP port numbers."""
    port = int(value)
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be in range 1-65535")
    return port


def parse_args():
    parser = argparse.ArgumentParser(
        description="Traffic Generator Server — receives TCP/UDP connections and collects stats.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_SERVER_HELP_EPILOG,
    )
    parser.add_argument("--host", default="0.0.0.0", help="Address to bind on")
    parser.add_argument("--port", type=_port_type, required=True, help="Port to listen on")
    parser.add_argument("--protocol", choices=["tcp", "udp"], default="tcp",
                        help="Protocol to use")
    parser.add_argument("--cps", type=float, default=0,
                        help="Expected connections per second (CPS) - used to calculate read timeout. "
                             "Higher CPS = longer timeout (30s-120s). Default: 30s timeout.")
    parser.add_argument("--output", type=str, default=None,
                        help="Optional file path to log the output results")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress per-connection messages (still written to output file)")
    parser.add_argument("--dashboard", type=str, default=None,
                        help="Dashboard metrics API URL (e.g., http://localhost:8081)")
    return parser.parse_args()


def check_and_set_ulimit():
    """
    Check and adjust ulimit (open files) on Linux if needed.
    Target: 4194304 (4M) open files for high-performance server.
    """
    if platform.system() != "Linux":
        return
    
    try:
        # Get current soft and hard limits
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        target_limit = 4194304
        
        if soft_limit < target_limit:
            # Try to set to target, but don't exceed hard limit
            new_limit = min(target_limit, hard_limit)
            
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (new_limit, hard_limit))
                print(f"Adjusted ulimit from {soft_limit} to {new_limit} open files")
            except (ValueError, OSError) as e:
                # If we can't set to target, try to set to hard limit
                if new_limit != hard_limit:
                    try:
                        resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))
                        print(f"Adjusted ulimit from {soft_limit} to {hard_limit} open files (max available)")
                    except (ValueError, OSError):
                        print(f"Warning: Could not adjust ulimit (current: {soft_limit}, target: {target_limit})", file=sys.stderr)
                        print(f"Consider running: ulimit -n {target_limit}", file=sys.stderr)
                else:
                    print(f"Warning: Could not adjust ulimit (current: {soft_limit}, target: {target_limit})", file=sys.stderr)
                    print(f"Consider running: ulimit -n {target_limit}", file=sys.stderr)
        else:
            print(f"ulimit already sufficient: {soft_limit} open files")
    except Exception as e:
        print(f"Warning: Could not check ulimit: {type(e).__name__}: {e}", file=sys.stderr)


def main():
    global _QUIET_MODE, _OUTPUT_FILE
    args = parse_args()
    
    # Check and adjust ulimit on Linux
    check_and_set_ulimit()
    
    # Set quiet mode globally
    _QUIET_MODE = args.quiet
    
    if args.output:
        try:
            _OUTPUT_FILE = open(args.output, "a")
            atexit.register(lambda: _OUTPUT_FILE.close() if _OUTPUT_FILE and not _OUTPUT_FILE.closed else None)
            # Also tee regular output to the file
            sys.stdout = Tee(args.output)
        except Exception as e:
            print(f"Error opening output file: {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        asyncio.run(run_server(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
