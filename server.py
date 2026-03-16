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
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


def _format_timestamp() -> str:
    """Fast timestamp formatting for logging. Returns ISO-8601 format with milliseconds."""
    t = time.time()
    ms = int((t % 1) * 1000)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)) + f".{ms:03d}"

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
        if self.file:
            self.file.flush()

    def close(self):
        if self.file and not self.file.closed:
            self.file.close()


@dataclass
class ConnectionRecord:
    conn_id: int
    client_addr: str
    connect_time: float
    disconnect_time: Optional[float] = None
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
        elapsed = time.monotonic() - self.start_time
        completed = [r for r in self.records if r.duration is not None]
        total = len(self.records)
        rate = total / elapsed if elapsed > 0 else 0
        total_pkts = sum(r.messages_received for r in self.records)
        total_bytes = sum(r.bytes_received for r in self.records)

        lines = [
            "\n" + "=" * 92,
            "  SERVER SUMMARY",
            "=" * 92,
            f"  Elapsed time        : {elapsed:.2f}s",
            f"  Total connections   : {total}",
            f"  Completed           : {len(completed)}",
            f"  Still open          : {total - len(completed)}",
            f"  Observed rate       : {rate:.2f} conn/s",
            f"  Total bytes received: {total_bytes}",
            f"  Total packets recv  : {total_pkts}",
        ]

        # File-transfer checksum summary
        ft_records = [r for r in self.records if r.file_transfer]
        if ft_records:
            ft_pass = sum(1 for r in ft_records if r.checksum_ok is True)
            ft_fail = sum(1 for r in ft_records if r.checksum_ok is False)
            lines += [
                f"  File transfers      : {len(ft_records)} "
                f"(checksum PASS={ft_pass} FAIL={ft_fail})",
            ]

        if completed:
            # Filter out any None durations defensively (race at shutdown)
            durations: List[float] = [r.duration for r in completed if r.duration is not None]  # type: ignore[misc]
            if durations:
                avg_dur = sum(durations) / len(durations)
                # Exclude file-transfer connections from PPS stats (1 msg / ~0s = nonsensical)
                pps_vals: List[float] = []
                for _r in completed:
                    if _r.file_transfer:
                        continue
                    _p = _r.pps_observed
                    if _p is not None:
                        pps_vals.append(_p)
                lines += [
                    f"  Duration avg        : {avg_dur:.3f}s",
                    f"  Duration min        : {min(durations):.3f}s",
                    f"  Duration max        : {max(durations):.3f}s",
                ]
                if pps_vals:
                    lines += [
                        f"  PPS avg (recv)      : {sum(pps_vals)/len(pps_vals):.1f} pkt/s",
                        f"  PPS min (recv)      : {min(pps_vals):.1f} pkt/s",
                        f"  PPS max (recv)      : {max(pps_vals):.1f} pkt/s",
                    ]

        if self.records:
            has_ft = any(r.file_transfer for r in self.records)
            has_tcp_stats = any(r.tcp_retransmits is not None for r in self.records)
            # Only show UDP stats if we have UDP connections with expected packet counts
            has_udp_stats = any(r.udp_expected_packets > 0 for r in self.records)
            checksum_hdr = " | Checksum" if has_ft else ""
            tcp_hdr = " | TCP Stats (retx/rtt/lost)" if has_tcp_stats else ""
            udp_hdr = " | UDP Stats (exp/rcv/lost/%)" if has_udp_stats else ""
            
            lines += [
                "",
                f"  {'ID':>6} | {'Client':<22} | {'Duration':>10} | {'Bytes':>9} | Messages | PPS (recv){checksum_hdr}{tcp_hdr}{udp_hdr}",
                "  " + "-" * (80 + (10 if has_ft else 0) + (35 if has_tcp_stats else 0) + (35 if has_udp_stats else 0)),
            ]
            for rec in self.records:
                lines.append(rec.row(show_tcp_stats=has_tcp_stats, show_udp_stats=has_udp_stats))
            
            # Add TCP statistics summary if available
            if has_tcp_stats:
                tcp_records = [r for r in self.records if r.tcp_retransmits is not None]
                total_retx = sum(r.tcp_retransmits for r in tcp_records if r.tcp_retransmits is not None)
                total_lost = sum(r.tcp_lost_packets for r in tcp_records if r.tcp_lost_packets is not None)
                rtt_vals = [r.tcp_rtt_ms for r in tcp_records if r.tcp_rtt_ms is not None]
                
                lines.append("")
                lines.append(f"  TCP Statistics Summary:")
                lines.append(f"    Total retransmits : {total_retx}")
                lines.append(f"    Total lost packets: {total_lost}")
                if rtt_vals:
                    lines.append(f"    RTT avg           : {sum(rtt_vals)/len(rtt_vals):.2f}ms")
                    lines.append(f"    RTT min           : {min(rtt_vals):.2f}ms")
                    lines.append(f"    RTT max           : {max(rtt_vals):.2f}ms")
            
            # Add UDP statistics summary if available
            if has_udp_stats:
                udp_records = [r for r in self.records if r.udp_expected_packets > 0]
                total_expected = sum(r.udp_expected_packets for r in udp_records)
                total_received = sum(r.messages_received for r in udp_records)
                total_lost = sum(r.udp_lost_packets for r in udp_records)
                loss_pct = (total_lost / total_expected * 100) if total_expected > 0 else 0
                
                lines.append("")
                lines.append(f"  UDP Statistics Summary:")
                lines.append(f"    Total expected    : {total_expected}")
                lines.append(f"    Total received    : {total_received}")
                lines.append(f"    Total lost        : {total_lost} ({loss_pct:.2f}%)")

        lines.append("=" * 92)
        return "\n".join(lines)


# ─── TCP ─────────────────────────────────────────────────────────────────────

# TCP keepalive constants (always enabled for TCP connections)
_KA_IDLE = 10      # seconds idle before first probe
_KA_INTERVAL = 10  # seconds between probes
_KA_COUNT = 5      # unacked probes before dropping


def apply_keepalive(sock: socket.socket) -> None:
    """Enable TCP keepalive with fixed 10s idle/interval settings."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

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


def get_tcp_info(sock: socket.socket) -> Tuple[Optional[int], Optional[float], Optional[float],
                                                 Optional[int], Optional[int], Optional[int]]:
    """
    Retrieve TCP socket statistics (Linux only).
    Returns: (retransmits, rtt_ms, rtt_var_ms, snd_cwnd, lost_packets, reordering)
    """
    system = platform.system()
    if system != "Linux":
        return (None, None, None, None, None, None)
    
    try:
        # TCP_INFO socket option (Linux-specific)
        # struct tcp_info is defined in <linux/tcp.h>
        TCP_INFO = 11
        
        # Get TCP_INFO structure (size varies by kernel version, but we only need first ~200 bytes)
        tcp_info = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 256)
        
        # Parse relevant fields from tcp_info structure
        # Offsets based on Linux kernel struct tcp_info (kernel 4.x+, x86_64)
        import struct
        
        # Correct offsets for struct tcp_info:
        # tcpi_state (u8) at offset 0
        # tcpi_retransmits (u8) at offset 2  ← CORRECTED
        # tcpi_lost (u32) at offset 32
        # tcpi_rtt (u32) at offset 68 (in microseconds)  ← CORRECTED
        # tcpi_rttvar (u32) at offset 72 (in microseconds)  ← CORRECTED
        # tcpi_snd_cwnd (u32) at offset 80  ← CORRECTED
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
    except Exception:
        # TCP_INFO not available or parsing failed
        return (None, None, None, None, None, None)

async def _handle_file_transfer(reader: asyncio.StreamReader,
                                 writer: asyncio.StreamWriter,
                                 rec: ConnectionRecord,
                                 first_chunk: bytes) -> None:
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

    # Safely buffer until we have exactly _FILE_HEADER_LEN bytes
    buf = first_chunk
    while len(buf) < _FILE_HEADER_LEN:
        more = await reader.read(_FILE_HEADER_LEN - len(buf))
        if not more:
            writer.write(b"FAIL:incomplete header\n")
            await writer.drain()
            rec.checksum_ok = False
            return
        buf += more

    header = buf[:_FILE_HEADER_LEN]
    leftover = buf[_FILE_HEADER_LEN:]

    # Parse header: TGEN_FILE:<sha256>:<size>\n
    try:
        inner = header[len(_FILE_HEADER_PREFIX):].rstrip(b"\n")
        sha256_expected, size_str = inner.split(b":", 1)
        expected_sha256 = sha256_expected.decode()
        expected_size = int(size_str.decode())
    except Exception:
        writer.write(b"FAIL:malformed header\n")
        await writer.drain()
        rec.checksum_ok = False
        return

    # Enforce server-side file size limit to prevent memory exhaustion
    if expected_size > _SERVER_MAX_FILE_SIZE:
        writer.write(b"FAIL:file too large\n")
        await writer.drain()
        rec.checksum_ok = False
        print(f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | FAIL file too large ({expected_size}B)")
        return

    # Read and hash chunk-by-chunk to avoid loading the whole file into memory
    sha256_hasher = hashlib.sha256()
    bytes_read = 0

    if leftover:
        sha256_hasher.update(leftover)
        bytes_read = len(leftover)
        rec.bytes_received += bytes_read  # count leftover bytes received

    while bytes_read < expected_size:
        chunk = await reader.read(min(65536, expected_size - bytes_read))
        if not chunk:
            break
        sha256_hasher.update(chunk)
        bytes_read += len(chunk)
        rec.bytes_received += len(chunk)

    rec.messages_received = 1

    if bytes_read != expected_size:
        reason = f"size mismatch: got {bytes_read} expected {expected_size}"
        writer.write(f"FAIL:{reason}\n".encode())
        await writer.drain()
        rec.checksum_ok = False
        print(f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | FAIL {reason}")
        return

    actual_sha256 = sha256_hasher.hexdigest()
    if actual_sha256 == expected_sha256:
        writer.write(b"OK\n")
        await writer.drain()
        rec.checksum_ok = True
        print(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | "
            f"{bytes_read}B | checksum=PASS"
        )
    else:
        reason = f"sha256 got={actual_sha256[:16]}... expected={expected_sha256[:16]}..."
        writer.write(f"FAIL:{reason}\n".encode())
        await writer.drain()
        rec.checksum_ok = False
        print(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | "
            f"{bytes_read}B | checksum=FAIL"
        )


async def handle_tcp_client(reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter,
                             stats: ServerStats):
    addr = writer.get_extra_info("peername")
    addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"

    # Always apply keepalive on accepted TCP sockets
    sock = writer.get_extra_info("socket")
    if sock is not None:
        apply_keepalive(sock)

    rec = await stats.new_connection(addr_str)
    timestamp = _format_timestamp()
    _log_connection(f"[{timestamp}] [{rec.conn_id:>6}] TCP connect    | {addr_str} ka=on")

    try:
        # buffer enough bytes to determine if it's a file header
        prefix_len = len(_FILE_HEADER_PREFIX)
        
        # We need to read *at least* prefix_len bytes, but if the first read is smaller,
        # we still can't be sure it's not a file transfer (due to TCP stream fragmentation).
        buf: bytes = b""
        while len(buf) < prefix_len:
            chunk = await reader.read(min(65536, prefix_len - len(buf)))
            if not chunk:
                break
            buf += chunk

        if buf.startswith(_FILE_HEADER_PREFIX):
            # File-transfer mode: pass the buffered header prefix along
            await _handle_file_transfer(reader, writer, rec, buf)
        elif buf:
            # Normal traffic mode: count what we buffered, then keep reading
            rec.bytes_received += len(buf)
            rec.messages_received += 1
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                rec.bytes_received += len(data)
                rec.messages_received += 1
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    finally:
        # Collect TCP statistics before closing
        if sock is not None:
            (rec.tcp_retransmits, rec.tcp_rtt_ms, rec.tcp_rtt_var_ms,
             rec.tcp_snd_cwnd, rec.tcp_lost_packets, rec.tcp_reordering) = get_tcp_info(sock)
        
        rec.disconnect_time = time.monotonic()
        dur = rec.duration if rec.duration is not None else 0.0
        timestamp = _format_timestamp()
        
        # Enhanced disconnect message with TCP stats
        disconnect_msg = (
            f"[{timestamp}] [{rec.conn_id:>6}] TCP disconnect | {addr_str} | "
            f"dur={dur:.3f}s | {rec.bytes_received}B"
        )
        if rec.tcp_retransmits is not None:
            disconnect_msg += f" | retx={rec.tcp_retransmits}"
            if rec.tcp_rtt_ms is not None:
                disconnect_msg += f" rtt={rec.tcp_rtt_ms:.1f}ms"
            if rec.tcp_lost_packets is not None and rec.tcp_lost_packets > 0:
                disconnect_msg += f" lost={rec.tcp_lost_packets}"
        _log_connection(disconnect_msg)
        
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_tcp_server(host: str, port: int, stats: ServerStats):
    async def handler(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            await handle_tcp_client(r, w, stats)
        except Exception as e:
            # Catch any unhandled exceptions to prevent them from propagating
            # to asyncio's internal error handler
            addr = w.get_extra_info("peername")
            addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
            print(f"Error handling TCP client {addr_str}: {e}")

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

    def __init__(self, stats: ServerStats, timeout: float = 5.0):
        self.stats = stats
        self.timeout = timeout
        self._sessions: Dict[tuple, ConnectionRecord] = {}
        self._timers: Dict[tuple, asyncio.TimerHandle] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport):
        self._loop = asyncio.get_running_loop()
        self.transport = transport # type: ignore

    def datagram_received(self, data: bytes, addr: tuple):
        addr_str = f"{addr[0]}:{addr[1]}"

        if addr not in self._sessions:
            # New "connection" — use sync helper (event loop is single-threaded here)
            rec = self.stats.new_connection_sync(addr_str)
            self._sessions[addr] = rec
            timestamp = _format_timestamp()
            _log_connection(f"[{timestamp}] [{rec.conn_id:>6}] UDP new sender | {addr_str}")

        rec = self._sessions[addr]
        rec.bytes_received += len(data)
        
        # Extract expected packet count from payload (first 8 bytes: seq_num + expected_total)
        if len(data) >= 8:
            try:
                import struct
                seq_num, expected_total = struct.unpack('!II', data[:8])
                
                # Store expected packet count from first packet
                if rec.udp_expected_packets == 0:
                    rec.udp_expected_packets = expected_total
                
                rec.messages_received += 1
            except Exception:
                # If extraction fails, just count the packet
                rec.messages_received += 1
        else:
            # Packet too small, just count it
            rec.messages_received += 1

        # Reset inactivity timer
        if addr in self._timers:
            self._timers[addr].cancel()
        loop = self._loop
        if loop is not None:
            self._timers[addr] = loop.call_later(
                self.timeout, self._expire_session, addr
            )

    def _expire_session(self, addr: tuple):
        rec = self._sessions.pop(addr, None)
        self._timers.pop(addr, None)
        if rec:
            # Calculate final loss: expected - received
            if rec.udp_expected_packets > 0:
                rec.udp_lost_packets = rec.udp_expected_packets - rec.messages_received
            
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
        print(f"UDP error: {exc}", file=sys.stderr)


async def run_udp_server(host: str, port: int, stats: ServerStats):
    loop = asyncio.get_running_loop()  # get_event_loop() is deprecated in async context
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(stats),
        local_addr=(host, port),
    )
    print(f"UDP server listening on {host}:{port}")
    try:
        await asyncio.Future()  # run forever
    finally:
        transport.close()


# ─── Entry Point ─────────────────────────────────────────────────────────────

async def run_server(args):
    stats = ServerStats()

    print("Starting traffic receiver")
    print(f"  Protocol : {args.protocol.upper()}")
    print(f"  Bind     : {args.host}:{args.port}")
    if args.protocol == "tcp":
        print(f"  Keepalive: ON (idle={_KA_IDLE}s, intvl={_KA_INTERVAL}s, cnt={_KA_COUNT})")
    print("-" * 55)

    loop = asyncio.get_running_loop()

    def shutdown(*_):
        print("\nShutting down...")
        print(stats.summary())
        for task in asyncio.all_tasks(loop):
            task.cancel()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    try:
        if args.protocol == "tcp":
            await run_tcp_server(args.host, args.port, stats)
        else:
            await run_udp_server(args.host, args.port, stats)
    except asyncio.CancelledError:
        pass


def parse_args():
    epilog = """\
SYNTAX
------
  python server.py --port PORT [options]

OPTIONS
-------
  --host HOST           Address to bind on (default: 0.0.0.0)
  --port PORT           Port to listen on (required)
  --protocol {tcp,udp}  Protocol to use (default: tcp)
  --output PATH         Optional file path to log the output results
  --quiet               Suppress per-connection messages (still written to output file)
  -h, --help            Show this help message and exit

NOTES
-----
  * TCP keepalive is automatically enabled for all accepted TCP connections
    (idle=10s, interval=10s, count=5).
  * UDP sessions are tracked per unique (host, port) pair and expire after
    5 seconds of inactivity.
  * Press Ctrl+C to stop the server and print the full statistics summary.

EXAMPLES
--------
  # TCP server on port 9000
  python server.py --port 9000

  # UDP server on port 9001
  python server.py --port 9001 --protocol udp

  # TCP server bound to a specific interface, logging output to a file
  python server.py --host 192.168.1.10 --port 9000 --output server.log
"""
    parser = argparse.ArgumentParser(
        description="Traffic Generator Server — receives TCP/UDP connections and collects stats.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", default=argparse.SUPPRESS,
                        help="Show this help message and exit")
    parser.add_argument("--host", default="0.0.0.0", help="Address to bind on (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on (required)")
    parser.add_argument("--protocol", choices=["tcp", "udp"], default="tcp",
                        help="Protocol to use: tcp or udp (default: tcp)")
    parser.add_argument("--output", type=str, default=None,
                        help="Optional file path to log the output results")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress per-connection messages (still written to output file)")
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
        print(f"Warning: Could not check ulimit: {e}", file=sys.stderr)


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
            print(f"Error opening output file: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        asyncio.run(run_server(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
