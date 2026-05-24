#!/usr/bin/env python3
"""
Traffic Generator Client
Generates TCP or UDP traffic with configurable rate and connection duration.
"""

import asyncio
import argparse
import atexit
import hashlib
import os
import platform
import resource
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

# Dashboard integration
try:
    from metrics_reporter import (
        MetricsReporter,
        MetricsConfig,
        ClientMetricsTracker,
        start_metrics_reporting
    )
    DASHBOARD_AVAILABLE = True
except ImportError:
    DASHBOARD_AVAILABLE = False


# Timestamp cache for performance optimization
_TIMESTAMP_CACHE = {}

def _format_timestamp() -> str:
    """Optimized timestamp formatting with caching. Returns ISO-8601 format with milliseconds."""
    t = time.time()
    sec = int(t)
    # Cache the formatted second to avoid repeated strftime calls
    if sec not in _TIMESTAMP_CACHE:
        _TIMESTAMP_CACHE[sec] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sec))
        # Keep cache small (last 60 seconds)
        if len(_TIMESTAMP_CACHE) > 60:
            _TIMESTAMP_CACHE.clear()
    ms = int((t % 1) * 1000)
    return f"{_TIMESTAMP_CACHE[sec]}.{ms:03d}"

# Maximum allowed file size for -F / --file transfers (128 MiB)
_MAX_FILE_SIZE = 128 * 1024 * 1024

# Header format sent before file data:
#   "TGEN_FILE:<sha256hex>:<size_10digits>\n"  (86 bytes total, fixed-width)
_FILE_HEADER_PREFIX = b"TGEN_FILE:"
_FILE_HEADER_LEN = len(_FILE_HEADER_PREFIX) + 64 + 1 + 10 + 1  # 86


def load_file_metadata(path: str) -> tuple[int, str]:
    """Read file size, enforce 128 MiB limit, compute SHA-256 without loading into memory."""
    size = os.path.getsize(path)
    if size > _MAX_FILE_SIZE:
        raise ValueError(
            f"File '{path}' is {size} bytes, exceeds 128 MiB limit ({_MAX_FILE_SIZE} bytes)"
        )
    sha256 = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(65536):
            sha256.update(chunk)
    return size, sha256.hexdigest()


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
class ConnectionResult:
    """Connection result with __slots__ for memory efficiency (Python 3.10+)."""
    conn_id: int
    success: bool
    latency_ms: float
    packets_sent: int = 0
    bytes_sent: int = 0
    error: Optional[str] = None
    retry_attempts: int = 0
    # TCP statistics (Linux only)
    tcp_retransmits: Optional[int] = None
    tcp_rtt_ms: Optional[float] = None
    tcp_rtt_var_ms: Optional[float] = None
    tcp_snd_cwnd: Optional[int] = None
    tcp_lost_packets: Optional[int] = None
    tcp_reordering: Optional[int] = None


@dataclass
class Stats:
    total: int = 0
    success: int = 0
    failed: int = 0
    total_packets: int = 0
    total_bytes_sent: int = 0
    total_retry_attempts: int = 0
    latencies: List[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)
    tcp_retransmits_list: List[int] = field(default_factory=list)
    tcp_rtt_list: List[float] = field(default_factory=list)
    tcp_rtt_var_list: List[float] = field(default_factory=list)
    tcp_cwnd_list: List[int] = field(default_factory=list)
    tcp_lost_list: List[int] = field(default_factory=list)
    tcp_reordering_list: List[int] = field(default_factory=list)
    rst_failures: int = 0
    timeout_failures: int = 0
    refused_failures: int = 0
    aborted_failures: int = 0
    broken_pipe_failures: int = 0
    other_failures: int = 0
    # Batch processing for performance
    _batch_buffer: List[ConnectionResult] = field(default_factory=list)
    _batch_size: int = 100

    def record(self, result: ConnectionResult):
        """Record connection result with batch processing for performance."""
        self._batch_buffer.append(result)
        if len(self._batch_buffer) >= self._batch_size:
            self._flush_batch()
    
    def _flush_batch(self):
        """Process all buffered results at once for better performance."""
        for result in self._batch_buffer:
            self.total += 1
            if result.success:
                self.success += 1
                self.latencies.append(result.latency_ms)
                self.total_packets += result.packets_sent
                self.total_bytes_sent += result.bytes_sent
                self.total_retry_attempts += result.retry_attempts
                if result.tcp_retransmits is not None:
                    self.tcp_retransmits_list.append(result.tcp_retransmits)
                if result.tcp_rtt_ms is not None:
                    self.tcp_rtt_list.append(result.tcp_rtt_ms)
                if result.tcp_rtt_var_ms is not None:
                    self.tcp_rtt_var_list.append(result.tcp_rtt_var_ms)
                if result.tcp_snd_cwnd is not None:
                    self.tcp_cwnd_list.append(result.tcp_snd_cwnd)
                if result.tcp_lost_packets is not None:
                    self.tcp_lost_list.append(result.tcp_lost_packets)
                if result.tcp_reordering is not None:
                    self.tcp_reordering_list.append(result.tcp_reordering)
            else:
                self.failed += 1
                self.total_bytes_sent += result.bytes_sent
                self.total_retry_attempts += result.retry_attempts
                error_text = result.error or ""
                if error_text.startswith("RST:"):
                    self.rst_failures += 1
                elif error_text.startswith("timeout "):
                    self.timeout_failures += 1
                elif error_text.startswith("refused:"):
                    self.refused_failures += 1
                elif error_text.startswith("aborted:"):
                    self.aborted_failures += 1
                elif error_text.startswith("broken_pipe:"):
                    self.broken_pipe_failures += 1
                else:
                    self.other_failures += 1
        self._batch_buffer.clear()
    
    def finalize(self):
        """Flush any remaining buffered results before generating summary."""
        if self._batch_buffer:
            self._flush_batch()

    def summary(self) -> str:
        """Generate summary statistics. Flushes any pending batch updates first."""
        self.finalize()  # Flush any remaining buffered results
        elapsed = time.monotonic() - self.start_time
        rate = self.total / elapsed if elapsed > 0 else 0
        lines = [
            "\n" + "=" * 55,
            "  CLIENT SUMMARY",
            "=" * 55,
            f"  Elapsed time      : {elapsed:.2f}s",
            f"  Total connections : {self.total}",
            f"  Successful        : {self.success}",
            f"  Failed            : {self.failed}",
            f"  Observed rate     : {rate:.2f} conn/s",
            f"  Total packets sent: {self.total_packets}",
            f"  Total bytes sent  : {self.total_bytes_sent}",
        ]
        if self.latencies:
            avg = sum(self.latencies) / len(self.latencies)
            lines += [
                f"  Latency avg       : {avg:.2f}ms",
                f"  Latency min       : {min(self.latencies):.2f}ms",
                f"  Latency max       : {max(self.latencies):.2f}ms",
            ]

        # TCP statistics summary
        if self.tcp_retransmits_list:
            total_retx = sum(self.tcp_retransmits_list)
            retx_conns = sum(1 for v in self.tcp_retransmits_list if v > 0)
            loss_conns = sum(1 for v in self.tcp_lost_list if v > 0)
            avg_bytes = (self.total_bytes_sent / self.success) if self.success else 0.0
            avg_pkts = (self.total_packets / self.success) if self.success else 0.0
            lines += [
                "",
                "  TCP Statistics:",
                f"    Total retransmits : {total_retx}",
                f"    Avg retransmits   : {total_retx/len(self.tcp_retransmits_list):.1f} per conn",
                f"    Connections w/ retx: {retx_conns}",
                f"    Connections w/ loss: {loss_conns}",
                f"    Avg bytes / conn  : {avg_bytes:.1f}",
                f"    Avg pkts / conn   : {avg_pkts:.1f}",
                f"    Total retries     : {self.total_retry_attempts}",
            ]
        if self.tcp_rtt_list:
            avg_rtt = sum(self.tcp_rtt_list) / len(self.tcp_rtt_list)
            lines += [
                f"    RTT avg           : {avg_rtt:.2f}ms",
                f"    RTT min           : {min(self.tcp_rtt_list):.2f}ms",
                f"    RTT max           : {max(self.tcp_rtt_list):.2f}ms",
            ]
        if self.tcp_rtt_var_list:
            lines += [
                f"    RTT var avg       : {sum(self.tcp_rtt_var_list)/len(self.tcp_rtt_var_list):.2f}ms",
                f"    RTT var min       : {min(self.tcp_rtt_var_list):.2f}ms",
                f"    RTT var max       : {max(self.tcp_rtt_var_list):.2f}ms",
            ]
        if self.tcp_lost_list:
            total_lost = sum(self.tcp_lost_list)
            if total_lost > 0:
                lines += [
                    f"    Total lost packets: {total_lost}",
                ]
        if self.failed:
            lines += [
                f"    Failures RST      : {self.rst_failures}",
                f"    Failures timeout  : {self.timeout_failures}",
                f"    Failures refused  : {self.refused_failures}",
                f"    Failures aborted  : {self.aborted_failures}",
                f"    Failures broken   : {self.broken_pipe_failures}",
                f"    Failures other    : {self.other_failures}",
            ]

        lines.append("=" * 55)
        return "\n".join(lines)


def calculate_timeout(cps: float) -> float:
    """
    Calculate connection timeout based on connections per second (CPS).
    Higher CPS = more server load = need longer timeout to avoid ETIMEDOUT (error 110).
    
    Args:
        cps: Connections per second rate
    
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


def _format_timeout_detail(phase: str, host: str, port: int, timeout: float,
                           elapsed_ms: float, attempt: Optional[int] = None,
                           max_retries: Optional[int] = None,
                           bytes_sent: Optional[int] = None,
                           packets_sent: Optional[int] = None) -> str:
    """Build a detailed timeout message for logging and summaries."""
    parts = [
        f"timeout phase={phase}",
        f"target={host}:{port}",
        f"timeout={timeout:.1f}s",
        f"elapsed={elapsed_ms:.1f}ms",
    ]
    if attempt is not None and max_retries is not None:
        parts.append(f"attempt={attempt}/{max_retries}")
    if bytes_sent is not None:
        parts.append(f"bytes_sent={bytes_sent}")
    if packets_sent is not None:
        parts.append(f"packets_sent={packets_sent}")
    return " | ".join(parts)


async def connect_with_retry(host: str, port: int, timeout: float,
                             max_retries: int = 3, conn_id: int = 0) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """
    Attempt to connect with exponential backoff retry logic.
    
    Retry Strategy:
    - Attempt 1: timeout=T, no delay
    - Attempt 2: delay 2s, timeout=T
    - Attempt 3: delay 5s, timeout=T
    
    This gives the server time to recover between attempts while keeping
    timeout constant to avoid excessive total wait time.
    
    Retries on:
    - asyncio.TimeoutError: Connection timeout (errno 110)
    - ConnectionResetError: Connection reset by peer (errno 104)
    
    Does NOT retry on:
    - ConnectionRefusedError: Server not listening (fail fast)
    - OSError: Other OS-level errors (fail fast)
    
    Args:
        host: Target host
        port: Target port
        timeout: Connection timeout in seconds (kept constant across retries)
        max_retries: Maximum number of retry attempts (default: 3)
        conn_id: Connection ID for logging
    
    Returns:
        Tuple of (reader, writer) on success
    
    Raises:
        asyncio.TimeoutError: If all retry attempts fail with timeout
        ConnectionResetError: If all retry attempts fail with RST
        ConnectionRefusedError: If connection is refused (no retry)
        OSError: For other connection errors (no retry)
    """
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Apply backoff delay before retry (not on first attempt)
            if attempt > 0:
                # Exponential backoff: 2s, 5s, 10s
                backoff_delay = min(2.0 * (2.5 ** (attempt - 1)), 10.0)
                timestamp = _format_timestamp()
                print(f"[{timestamp}] [{conn_id:>6}] Retry {attempt}/{max_retries-1} after {backoff_delay:.1f}s delay | timeout={timeout:.1f}s")
                await asyncio.sleep(backoff_delay)
            
            # Attempt connection with consistent timeout
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
            
            # Success
            if attempt > 0:
                timestamp = _format_timestamp()
                print(f"[{timestamp}] [{conn_id:>6}] ✓ Retry succeeded after {attempt} attempt(s)")
            
            return reader, writer
            
        except asyncio.TimeoutError as e:
            last_error = e
            timestamp = _format_timestamp()
            elapsed_ms = timeout * 1000
            timeout_detail = _format_timeout_detail(
                phase="connect",
                host=host,
                port=port,
                timeout=timeout,
                elapsed_ms=elapsed_ms,
                attempt=attempt + 1,
                max_retries=max_retries,
            )
            if attempt < max_retries - 1:
                print(f"[{timestamp}] [{conn_id:>6}] ✗ {timeout_detail}")
            else:
                # Final attempt failed
                print(f"[{timestamp}] [{conn_id:>6}] ✗ All attempts failed | {timeout_detail}")
                raise
        
        except ConnectionResetError as e:
            # Retry on RST - may be transient server issue (crash, overload, etc.)
            last_error = e
            timestamp = _format_timestamp()
            if attempt < max_retries - 1:
                print(f"[{timestamp}] [{conn_id:>6}] ✗ Connection reset on attempt {attempt + 1}/{max_retries}")
            else:
                # Final attempt failed
                print(f"[{timestamp}] [{conn_id:>6}] ✗ All {max_retries} attempts failed with RST")
                raise
        
        except (ConnectionRefusedError, OSError) as e:
            # Don't retry on connection refused or other OS errors
            # These indicate server is down/unreachable, not transient issues
            timestamp = _format_timestamp()
            error_type = type(e).__name__
            print(f"[{timestamp}] [{conn_id:>6}] ✗ {error_type} - no retry")
            raise
    
    # Should not reach here, but raise last error if we do
    if last_error:
        raise last_error
    raise RuntimeError("Unexpected retry loop exit")


def make_payload(args) -> bytes:
    """Build the payload bytes.  File mode is handled separately via make_file_transfer()."""
    if args.payload_size > 0:
        return os.urandom(args.payload_size)
    if args.payload:
        return args.payload.encode()
    return b"PING"


def make_file_header(path: str) -> tuple:
    """
    Build (header_bytes, size, sha256) for the given file path.
    Caller is responsible for pre-validating the file (existence + size limit).
    """
    size, sha256 = load_file_metadata(path)
    size_str = f"{size:010d}"
    header = _FILE_HEADER_PREFIX + sha256.encode() + b":" + size_str.encode() + b"\n"
    return header, size, sha256


# TCP keepalive profiles
# Standard: Default settings for stable networks (60s detection time)
_KA_STANDARD = {
    'idle': 10,      # seconds idle before first probe
    'interval': 10,  # seconds between probes
    'count': 5,      # unacked probes before dropping
    'heartbeat': 15  # heartbeat interval in seconds
}

# Aggressive: Fast detection for lossy networks (20s detection time)
_KA_AGGRESSIVE = {
    'idle': 5,       # seconds idle before first probe
    'interval': 5,   # seconds between probes
    'count': 3,      # unacked probes before dropping
    'heartbeat': 10  # heartbeat interval in seconds
}


def apply_tcp_optimizations(sock: socket.socket, ka_idle: int = 10, ka_interval: int = 10, ka_count: int = 5) -> None:
    """
    Apply TCP optimizations: keepalive, nodelay, and buffer sizes.
    
    Args:
        sock: Socket to configure
        ka_idle: Seconds idle before first keepalive probe
        ka_interval: Seconds between keepalive probes
        ka_count: Number of unacked probes before dropping connection
    """
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
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, ka_idle)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, ka_interval)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, ka_count)
    elif system == "Darwin":  # macOS
        TCP_KEEPALIVE = 0x10  # equivalent to TCP_KEEPIDLE on macOS
        sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, ka_idle)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, ka_interval)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, ka_count)


def apply_keepalive(sock: socket.socket) -> None:
    """Deprecated: Use apply_tcp_optimizations() instead."""
    apply_tcp_optimizations(sock)


def get_tcp_info(sock: socket.socket) -> Tuple[Optional[int], Optional[float], Optional[float], Optional[int], Optional[int], Optional[int]]:
    """
    Retrieve TCP socket statistics (Linux only).
    Returns: (retransmits, rtt_ms, rtt_var_ms, snd_cwnd, lost_packets, reordering)
    """
    system = platform.system()
    if system != "Linux":
        return (None, None, None, None, None, None)

    try:
        # TCP_INFO socket option (Linux-specific)
        TCP_INFO = 11

        # Get TCP_INFO structure
        tcp_info = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 256)

        # Parse relevant fields from tcp_info structure
        # Offsets based on struct tcp_info (Linux kernel 4.x+, x86_64)
        import struct

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
        return (None, None, None, None, None, None)


async def tcp_file_transfer(conn_id: int, host: str, port: int,
                             header: bytes, file_data: bytes, size: int,
                             stats: Stats, timeout: float = 30.0, dashboard_tracker=None):
    """Send a file over TCP with a checksum header; report PASS/FAIL."""
    t0 = time.monotonic()
    result: Optional[ConnectionResult] = None
    bytes_sent = 0
    retry_attempts = 0
    try:
        # Use retry logic for connection establishment
        reader, writer = await connect_with_retry(host, port, timeout, max_retries=3, conn_id=conn_id)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            apply_tcp_optimizations(sock)

        latency_ms = (time.monotonic() - t0) * 1000

        # Calculate transfer timeout based on file size (assume 1MB/s minimum transfer rate)
        # Use max of connection timeout or estimated transfer time + 30s buffer
        estimated_transfer_time = size / (1024 * 1024)  # seconds at 1MB/s
        transfer_timeout = max(timeout, estimated_transfer_time + 30.0)

        # Send header then file data in chunks
        writer.write(header)
        bytes_sent += len(header)
        # Apply timeout to detect server hangs during file transfer
        await asyncio.wait_for(writer.drain(), timeout=transfer_timeout)

        # Send file data in 64KB chunks to avoid blocking the event loop
        chunk_size = 65536
        for i in range(0, len(file_data), chunk_size):
            chunk = file_data[i:i + chunk_size]
            writer.write(chunk)
            bytes_sent += len(chunk)
            # Apply timeout to detect server hangs during file transfer
            await asyncio.wait_for(writer.drain(), timeout=transfer_timeout)

        # Read server response: "OK\n" or "FAIL:<reason>\n"
        response = await asyncio.wait_for(reader.readline(), timeout=timeout)
        response_str = response.decode(errors="replace").strip()

        # Collect TCP statistics before closing
        tcp_retx, tcp_rtt, tcp_rtt_var, tcp_cwnd, tcp_lost, tcp_reordering = get_tcp_info(sock) if sock else (None, None, None, None, None, None)

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        elapsed_ms = (time.monotonic() - t0) * 1000
        if response_str == "OK":
            msg = (
                f"[{conn_id:>6}] TCP file xfer  | latency={latency_ms:.1f}ms "
                f"total={elapsed_ms:.0f}ms {size}B | checksum=PASS"
            )
            if tcp_retx is not None:
                msg += f" | retx={tcp_retx}"
                if tcp_rtt is not None:
                    msg += f" rtt={tcp_rtt:.1f}ms"
            print(msg)
            result = ConnectionResult(conn_id=conn_id, success=True,
                                      latency_ms=latency_ms, packets_sent=1, bytes_sent=bytes_sent,
                                      retry_attempts=retry_attempts,
                                      tcp_retransmits=tcp_retx, tcp_rtt_ms=tcp_rtt,
                                      tcp_rtt_var_ms=tcp_rtt_var, tcp_snd_cwnd=tcp_cwnd,
                                      tcp_lost_packets=tcp_lost, tcp_reordering=tcp_reordering)

            # Update dashboard if enabled
            if dashboard_tracker:
                dashboard_tracker.record_connection(True, latency_ms, 1)
        else:
            print(
                f"[{conn_id:>6}] TCP file xfer  | latency={latency_ms:.1f}ms "
                f"total={elapsed_ms:.0f}ms {size}B | checksum=FAIL ({response_str})"
            )
            result = ConnectionResult(conn_id=conn_id, success=False,
                                      latency_ms=latency_ms, bytes_sent=bytes_sent,
                                      error=f"checksum mismatch: {response_str}")
    except ConnectionResetError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | target={host}:{port} | RST: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"RST: {e}")
    except ConnectionRefusedError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | target={host}:{port} | refused: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"refused: {e}")
    except ConnectionAbortedError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | target={host}:{port} | aborted: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"aborted: {e}")
    except BrokenPipeError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | target={host}:{port} | broken_pipe: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"broken_pipe: {e}")
    except asyncio.TimeoutError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        timeout_detail = _format_timeout_detail(
            phase="file_transfer",
            host=host,
            port=port,
            timeout=timeout,
            elapsed_ms=latency_ms,
            bytes_sent=bytes_sent,
            packets_sent=1 if bytes_sent > 0 else 0,
        )
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | {timeout_detail}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=timeout_detail)
    except OSError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        error_detail = f"error:{e.errno}" if hasattr(e, 'errno') else str(e)
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | target={host}:{port} | {error_detail}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=error_detail)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        error_detail = f"{type(e).__name__}: {e}"
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | target={host}:{port} | {error_detail}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=error_detail)
    if result is not None:
        stats.record(result)
        # Update dashboard for failed file transfers
        if dashboard_tracker and not result.success:
            dashboard_tracker.record_connection(False, result.latency_ms, 0)


async def tcp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats, args=None, timeout: float = 30.0,
                         dashboard_tracker=None):
    t0 = time.monotonic()
    packets_sent = 0
    bytes_sent = 0
    retry_attempts = 0
    pps = args.pps if args else 0
    result: Optional[ConnectionResult] = None
    writer = None  # Declare at function scope for timeout handler
    try:
        # Use retry logic for connection establishment
        reader, writer = await connect_with_retry(host, port, timeout, max_retries=3, conn_id=conn_id)

        # Apply TCP optimizations (keepalive, nodelay, buffers)
        # Get keepalive settings from args
        ka_mode = args.keepalive_mode if args and hasattr(args, 'keepalive_mode') else 'standard'
        ka_profile = _KA_AGGRESSIVE if ka_mode == 'aggressive' else _KA_STANDARD
        
        sock = writer.get_extra_info("socket")
        if sock is not None:
            apply_tcp_optimizations(sock, ka_profile['idle'], ka_profile['interval'], ka_profile['count'])

        now = time.monotonic()
        latency_ms = (now - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP connected  | latency={latency_ms:.1f}ms ka=on")

        # Calculate per-operation timeout for drain() calls
        # This should be generous to handle slow networks, but detect actual hangs
        # Use the connection timeout (30-120s based on CPS), not duration-based
        # Duration controls how long the connection stays open, not individual operations
        drain_timeout = timeout

        if pps > 0 and duration > 0:
            interval = 1.0 / pps
            deadline = now + duration
            while True:
                now = time.monotonic()
                if now >= deadline:
                    break
                writer.write(payload)
                bytes_sent += len(payload)
                # Apply timeout to detect server hangs during data transfer
                await asyncio.wait_for(writer.drain(), timeout=drain_timeout)
                packets_sent += 1
                now = time.monotonic()
                remaining = deadline - now
                if remaining <= 0:
                    break
                await asyncio.sleep(min(interval, remaining))
        else:
            # Single send (original behaviour)
            writer.write(payload)
            bytes_sent += len(payload)
            # Apply timeout to detect server hangs during data transfer
            await asyncio.wait_for(writer.drain(), timeout=drain_timeout)
            packets_sent = 1
            
            if duration > 0:
                # Check if heartbeat is enabled via --heartbeat flag
                enable_heartbeat = args and hasattr(args, 'heartbeat') and args.heartbeat
                
                if enable_heartbeat:
                    # Get heartbeat interval from keepalive profile
                    ka_mode = args.keepalive_mode if args and hasattr(args, 'keepalive_mode') else 'standard'
                    ka_profile = _KA_AGGRESSIVE if ka_mode == 'aggressive' else _KA_STANDARD
                    heartbeat_interval = float(ka_profile['heartbeat'])
                    max_heartbeat_failures = 3  # Allow 3 consecutive failures before giving up
                    consecutive_failures = 0
                    remaining = duration
                    
                    while remaining > 0:
                        sleep_time = min(heartbeat_interval, remaining)
                        await asyncio.sleep(sleep_time)
                        remaining -= sleep_time
                        
                        if remaining > 0:
                            # Send small heartbeat packet to keep connection alive
                            heartbeat = b"HeartBeat"
                            writer.write(heartbeat)
                            try:
                                await asyncio.wait_for(writer.drain(), timeout=drain_timeout)
                                packets_sent += 1
                                bytes_sent += len(heartbeat)
                                consecutive_failures = 0  # Reset on success
                            except asyncio.TimeoutError:
                                # Heartbeat failed - increment failure count
                                consecutive_failures += 1
                                timestamp = _format_timestamp()
                                print(f"[{timestamp}] [{conn_id:>6}] DEBUG: Heartbeat timeout #{consecutive_failures} after {duration - remaining:.1f}s")
                                
                                if consecutive_failures >= max_heartbeat_failures:
                                    # Too many consecutive failures - connection is dead
                                    raise asyncio.TimeoutError(f"Heartbeat failed {consecutive_failures} times - connection dead")
                                # Otherwise continue - transient network issue
                else:
                    # No heartbeat - just sleep for the full duration (original behavior)
                    await asyncio.sleep(duration)

        # Collect TCP statistics before closing
        tcp_retx, tcp_rtt, tcp_rtt_var, tcp_cwnd, tcp_lost, tcp_reordering = get_tcp_info(sock) if sock else (None, None, None, None, None, None)

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        result = ConnectionResult(conn_id=conn_id, success=True,
                                  latency_ms=latency_ms, packets_sent=packets_sent, bytes_sent=bytes_sent,
                                  retry_attempts=retry_attempts,
                                  tcp_retransmits=tcp_retx, tcp_rtt_ms=tcp_rtt,
                                  tcp_rtt_var_ms=tcp_rtt_var, tcp_snd_cwnd=tcp_cwnd,
                                  tcp_lost_packets=tcp_lost, tcp_reordering=tcp_reordering)

        # Update dashboard if enabled
        if dashboard_tracker:
            dashboard_tracker.record_connection(True, latency_ms, packets_sent)
    except ConnectionResetError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | target={host}:{port} | RST: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"RST: {e}")
    except ConnectionRefusedError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | target={host}:{port} | refused: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"refused: {e}")
    except ConnectionAbortedError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | target={host}:{port} | aborted: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"aborted: {e}")
    except BrokenPipeError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | target={host}:{port} | broken_pipe: {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=f"broken_pipe: {e}")
    except asyncio.TimeoutError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        
        # Enhanced debug logging for timeout diagnosis
        # Determine which phase timed out based on bytes_sent
        if bytes_sent == 0:
            if packets_sent == 0:
                phase = "connect"  # Never established connection
                debug_info = "Connection establishment timed out - no SYN/ACK received"
            else:
                phase = "send"  # Connected but first send failed
                debug_info = f"Connected but failed to send first packet (attempted {packets_sent} sends)"
        else:
            phase = "drain"  # Data sent but drain() timed out
            debug_info = f"Sent {bytes_sent} bytes ({packets_sent} packets) but drain() timed out - data may be stuck in send buffer"
        
        # Get TCP socket info for additional diagnostics
        sock = None
        try:
            # writer is declared at function scope, safe to access
            if writer is not None:
                sock = writer.get_extra_info("socket")
        except Exception:
            pass
        
        tcp_retx, tcp_rtt, tcp_rtt_var, tcp_cwnd, tcp_lost, tcp_reordering = get_tcp_info(sock) if sock else (None, None, None, None, None, None)
        
        # Build detailed timeout message
        timeout_detail = _format_timeout_detail(
            phase=phase,
            host=host,
            port=port,
            timeout=timeout,
            elapsed_ms=latency_ms,
            bytes_sent=bytes_sent,
            packets_sent=packets_sent,
        )
        
        # Log comprehensive debug information
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | {timeout_detail}")
        print(f"[{timestamp}] [{conn_id:>6}] DEBUG: {debug_info}")
        print(f"[{timestamp}] [{conn_id:>6}] DEBUG: bytes_sent={bytes_sent} packets_sent={packets_sent} elapsed={latency_ms:.1f}ms")
        
        if tcp_retx is not None or tcp_rtt is not None:
            tcp_stats = []
            if tcp_retx is not None:
                tcp_stats.append(f"retx={tcp_retx}")
            if tcp_rtt is not None:
                tcp_stats.append(f"rtt={tcp_rtt:.1f}ms")
            if tcp_cwnd is not None:
                tcp_stats.append(f"cwnd={tcp_cwnd}")
            if tcp_lost is not None:
                tcp_stats.append(f"lost={tcp_lost}")
            print(f"[{timestamp}] [{conn_id:>6}] DEBUG: TCP stats: {' '.join(tcp_stats)}")
        else:
            print(f"[{timestamp}] [{conn_id:>6}] DEBUG: TCP stats unavailable (socket may not exist)")
        
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=timeout_detail,
                                  tcp_retransmits=tcp_retx, tcp_rtt_ms=tcp_rtt,
                                  tcp_rtt_var_ms=tcp_rtt_var, tcp_snd_cwnd=tcp_cwnd,
                                  tcp_lost_packets=tcp_lost, tcp_reordering=tcp_reordering)
    except OSError as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        error_detail = f"error:{e.errno}" if hasattr(e, 'errno') else str(e)
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | target={host}:{port} | {error_detail}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=error_detail)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        error_detail = f"{type(e).__name__}: {e}"
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | target={host}:{port} | {error_detail}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=error_detail)
    if result is not None:
        stats.record(result)
        # Update dashboard for failed connections
        if dashboard_tracker and not result.success:
            dashboard_tracker.record_connection(False, result.latency_ms, 0)

        # Update dashboard metrics if enabled
        if 'dashboard_tracker' in globals() and globals()['dashboard_tracker']:
            globals()['dashboard_tracker'].record_connection(
                success=result.success,
                latency_ms=result.latency_ms,
                packets=result.packets_sent
            )


async def udp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats, args=None, dashboard_tracker=None):
    t0 = time.monotonic()
    loop = asyncio.get_running_loop()
    packets_sent = 0
    pps = args.pps if args else 0
    transport = None
    bytes_sent = 0
    result: Optional[ConnectionResult] = None
    try:
        transport, protocol = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol,
            remote_addr=(host, port)
        )
        
        # Increase UDP send buffer for high-throughput scenarios
        sock = transport.get_extra_info('socket')
        if sock:
            try:
                # Set send buffer to 4MB (sufficient for client sending)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4194304)
            except OSError:
                pass  # Ignore if system doesn't allow buffer size changes
        
        # Capture latency right after endpoint creation (before any sends)
        now = time.monotonic()
        latency_ms = (now - t0) * 1000
        if pps > 0 and duration > 0:
            interval = 1.0 / pps
            deadline = now + duration
            # Calculate expected packet count
            expected_packets = int(duration * pps)
            seq_num = 0
            while True:
                now = time.monotonic()
                if now >= deadline:
                    break
                # Prepend sequence number (4 bytes) and expected total (4 bytes) to payload
                import struct
                packet = struct.pack('!II', seq_num, expected_packets) + payload
                transport.sendto(packet)
                bytes_sent += len(packet)
                packets_sent += 1
                seq_num += 1
                now = time.monotonic()
                remaining = deadline - now
                if remaining <= 0:
                    break
                await asyncio.sleep(min(interval, remaining))
            timestamp = _format_timestamp()
            print(f"[{timestamp}] [{conn_id:>6}] UDP sent       | latency={latency_ms:.1f}ms | {packets_sent} pkts | {len(payload)+8}B each")
        else:
            # Single packet with sequence number 0 and total 1
            import struct
            packet = struct.pack('!II', 0, 1) + payload
            transport.sendto(packet)
            bytes_sent += len(packet)
            packets_sent = 1
            timestamp = _format_timestamp()
            print(f"[{timestamp}] [{conn_id:>6}] UDP sent       | latency={latency_ms:.1f}ms | {len(packet)}B")
            if duration > 0:
                await asyncio.sleep(duration)

        result = ConnectionResult(conn_id=conn_id, success=True,
                                  latency_ms=latency_ms, packets_sent=packets_sent, bytes_sent=bytes_sent)
        
        # Update dashboard if enabled
        if dashboard_tracker:
            dashboard_tracker.record_connection(True, latency_ms, packets_sent)
    except OSError as e:
        # UDP-specific errors:
        # ECONNREFUSED (111): ICMP port unreachable received
        # ENETUNREACH (101): Network unreachable
        # EHOSTUNREACH (113): Host unreachable
        # EMSGSIZE (90): Message too long
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        if hasattr(e, 'errno'):
            if e.errno == 111:  # ECONNREFUSED
                error_detail = "port_unreachable (ICMP)"
            elif e.errno == 101:  # ENETUNREACH
                error_detail = "network_unreachable"
            elif e.errno == 113:  # EHOSTUNREACH
                error_detail = "host_unreachable"
            elif e.errno == 90:  # EMSGSIZE
                error_detail = "message_too_long"
            else:
                error_detail = f"error:{e.errno}"
        else:
            error_detail = str(e)
        print(f"[{timestamp}] [{conn_id:>6}] UDP FAILED     | target={host}:{port} | {error_detail}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=error_detail)
        
        # Update dashboard if enabled
        if dashboard_tracker:
            dashboard_tracker.record_connection(False, latency_ms, 0)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        error_detail = f"{type(e).__name__}: {e}"
        print(f"[{timestamp}] [{conn_id:>6}] UDP FAILED     | target={host}:{port} | {error_detail}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=error_detail)
    finally:
        if transport is not None:
            transport.close()  # always close, even on exception
    if result is not None:
        stats.record(result)


async def run_client(args):
    stats = Stats()
    interval = 1.0 / args.cps if args.cps > 0 else 0.0
    conn_id = 0
    tasks = set()
    
    # Calculate timeout based on CPS to handle high connection rates
    connection_timeout = calculate_timeout(args.cps)
    
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
        dashboard_reporter = MetricsReporter(config, 'client')
        dashboard_tracker = ClientMetricsTracker(
            protocol=args.protocol,
            processes=1
        )
        dashboard_task = asyncio.create_task(
            start_metrics_reporting(dashboard_reporter, dashboard_tracker, interval=1.0)
        )

    # File-transfer mode: load file into memory once (file already validated in main())
    if args.file:
        file_header, file_size, sha256 = make_file_header(args.file)
        # Read file content once and cache in memory for all connections
        with open(args.file, "rb") as fh:
            file_data = fh.read()
        file_transfer = True
        payload = b""
    else:
        file_header = b""
        file_size = 0
        file_data = b""
        sha256 = ""
        file_transfer = False
        payload = make_payload(args)

    print("Starting traffic generator")
    print(f"  Target   : {args.protocol.upper()} {args.host}:{args.port}")
    print(f"  CPS      : {args.cps} conn/s  (interval={interval*1000:.1f}ms)")
    print(f"  Timeout  : {connection_timeout:.0f}s (scaled for CPS)")
    if file_transfer:
        print(f"  Mode     : file transfer (-F {args.file})")
        print(f"  File size: {file_size} bytes")
        print(f"  Checksum : SHA-256 {sha256}")
    else:
        print(f"  Duration : {'long-lived (' + str(args.duration) + 's)' if args.duration > 0 else 'short-lived'}")
        print(f"  PPS      : {'%g pkt/s per connection' % args.pps if args.pps > 0 else 'one-shot (no PPS)'}")
        print(f"  Payload  : {len(payload)} bytes")
    print(f"  Total    : {'infinite' if args.total == 0 else args.total}")
    if args.protocol == "tcp":
        ka_mode = args.keepalive_mode if hasattr(args, 'keepalive_mode') else 'standard'
        ka_profile = _KA_AGGRESSIVE if ka_mode == 'aggressive' else _KA_STANDARD
        print(f"  Keepalive: {ka_mode.upper()} (idle={ka_profile['idle']}s, intvl={ka_profile['interval']}s, cnt={ka_profile['count']})")
        if hasattr(args, 'heartbeat') and args.heartbeat:
            print(f"  Heartbeat: ON (interval={ka_profile['heartbeat']}s, max_failures=3)")
    print("-" * 55)

    # Use absolute deadline-based scheduling to eliminate cumulative drift
    start_time = time.monotonic()
    
    # Batch size for task creation: create multiple tasks before yielding to event loop
    # This reduces context switching overhead when CPS is high
    # Scale batch size with CPS: 1% of CPS, min 1, max 100
    batch_size = max(1, min(100, int(args.cps / 100)))
    
    try:
        while args.total == 0 or conn_id < args.total:
            # Create a batch of tasks
            batch_end = min(conn_id + batch_size, args.total) if args.total > 0 else conn_id + batch_size
            
            for _ in range(batch_size):
                if args.total > 0 and conn_id >= args.total:
                    break
                    
                conn_id += 1
                if file_transfer:
                    # File-transfer mode: TCP only (UDP is unreliable for integrity checks)
                    coro = tcp_file_transfer(conn_id, args.host, args.port,
                                             file_header, file_data, file_size, stats,
                                             timeout=connection_timeout)
                elif args.protocol == "tcp":
                    coro = tcp_connection(conn_id, args.host, args.port, payload,
                                          args.duration, stats, args,
                                          timeout=connection_timeout,
                                          dashboard_tracker=dashboard_tracker)
                else:
                    coro = udp_connection(conn_id, args.host, args.port, payload,
                                          args.duration, stats, args,
                                          dashboard_tracker=dashboard_tracker)

                task = asyncio.create_task(coro)
                tasks.add(task)
                task.add_done_callback(tasks.discard)

            # Deadline-based scheduling: sleep until next connection should start
            # This eliminates cumulative drift from task creation overhead
            next_conn_time = start_time + (conn_id * interval)
            sleep_duration = next_conn_time - time.monotonic()
            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)
            else:
                # If we're behind schedule, yield to event loop briefly
                await asyncio.sleep(0)

        # Wait for all in-flight connections to finish
        # Use duration + 10s buffer for long-lived connections, or 30s for short-lived
        wait_timeout = (args.duration + 10.0) if args.duration else 30.0
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=wait_timeout)
            if pending:
                print(f"Warning: {len(pending)} tasks still running after {wait_timeout:.0f}s.",
                      file=sys.stderr)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        # Cancel remaining tasks (skip already-completed ones)
        for t in list(tasks):
            if not t.done():
                t.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=3.0)
            if pending:
                print(f"Warning: {len(pending)} tasks did not finish gracefully within timeout.", file=sys.stderr)
        summary_output = stats.summary()
        print(summary_output)
        sys.stdout.flush()  # Ensure summary is written to file when using --output
        
        # Stop dashboard reporting
        if dashboard_task:
            dashboard_task.cancel()
            try:
                await dashboard_task
            except asyncio.CancelledError:
                pass


def parse_args():
    epilog = """\
SYNTAX
------
  python client.py --port PORT [options]

OPTIONS
-------
  --host HOST           Target host (default: 127.0.0.1)
  --port PORT           Target port (required)
  --protocol {tcp,udp}  Protocol to use (default: tcp)
  --cps CPS             Connections per second (default: 1.0)
  --total N             Total connections to make; 0 = infinite (default: 1000)
  --duration SECS       Seconds to hold each connection open; 0 = short-lived (default: 0.0)
  --payload TEXT        Payload string to send (default: PING)
  --payload-size BYTES  Random payload size in bytes; overrides --payload (default: 0)
  --pps PPS             Packets per second per connection; requires --duration > 0 (default: 0.0)
  --keepalive-mode MODE TCP keepalive mode: standard or aggressive (default: standard)
  --heartbeat           Enable heartbeat packets when pps=0 (default: disabled)
  -F, --file PATH       File to send over each TCP connection (max 128 MiB);
                        server verifies SHA-256 checksum per connection
  --output PATH         Optional file path to log the output results
  -h, --help            Show this help message and exit

NOTES
-----
  * TCP keepalive modes:
    - standard: idle=10s, interval=10s, count=5 (60s detection, heartbeat=15s)
    - aggressive: idle=5s, interval=5s, count=3 (20s detection, heartbeat=10s)
  * --heartbeat sends application-level keepalive when pps=0 (tolerates 3 failures).
  * -F/--file is TCP-only; --duration, --pps, and --payload are ignored in file mode.
  * --pps has no effect unless --duration > 0.

EXAMPLES
--------
  # 10 short-lived TCP connections at 5/sec
  python client.py --port 9000 --cps 5 --total 10

  # Long-lived UDP connections (2s each) at 2/sec, 1000 total (default)
  python client.py --port 9001 --protocol udp --cps 2 --duration 2

  # 100 packets/s per connection for 5 seconds, 2 connections/sec
  python client.py --port 9000 --cps 2 --duration 5 --pps 100

  # 1 KB random payload, 20 connections at 10/sec
  python client.py --port 9000 --cps 10 --total 20 --payload-size 1024

  # Send a file over 5 TCP connections at 2/sec; server verifies checksum each time
  python client.py --port 9000 --cps 2 --total 5 -F /path/to/file.bin

  # Log all output to a file
  python client.py --port 9000 --cps 5 --total 10 --output results.txt
"""
    parser = argparse.ArgumentParser(
        description="Traffic Generator Client — sends TCP/UDP traffic at a configurable rate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", default=argparse.SUPPRESS,
                        help="Show this help message and exit")
    parser.add_argument("--host", default="127.0.0.1", help="Target host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, required=True, help="Target port (required)")
    parser.add_argument("--protocol", choices=["tcp", "udp"], default="tcp",
                        help="Protocol to use: tcp or udp (default: tcp)")
    parser.add_argument("--cps", type=float, default=1.0,
                        help="Connections per second (default: 1.0)")
    parser.add_argument("--total", type=int, default=1000,
                        help="Total connections to make; 0 = infinite (default: 1000)")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="Seconds to hold each connection open; 0 = short-lived (default: 0.0)")
    parser.add_argument("--payload", default="PING",
                        help="Payload string to send (default: PING)")
    parser.add_argument("--payload-size", type=int, default=0,
                        help="Random payload size in bytes; overrides --payload (default: 0)")
    parser.add_argument("--pps", type=float, default=0.0,
                        help="Packets per second per connection; requires --duration > 0 (default: 0.0)")
    parser.add_argument("--keepalive-mode", type=str, choices=['standard', 'aggressive'], default='standard',
                        help="TCP keepalive mode: standard (10s/10s/5, 60s detection) or aggressive (5s/5s/3, 20s detection) (default: standard)")
    parser.add_argument("--heartbeat", action="store_true", default=False,
                        help="Enable heartbeat packets when pps=0 (15s for standard, 10s for aggressive) (default: disabled)")
    parser.add_argument("-F", "--file", type=str, default=None,
                        help="File to send over each TCP connection (max 128 MiB); "
                             "server verifies SHA-256 checksum per connection")
    parser.add_argument("--output", type=str, default=None,
                        help="Optional file path to log the output results")
    parser.add_argument("--dashboard", type=str, default=None,
                        help="Dashboard metrics API URL (e.g., http://localhost:8081)")
    return parser.parse_args()


def check_and_set_ulimit():
    """
    Check and adjust ulimit (open files) on Linux if needed.
    Target: 1048576 (1M) open files for high-performance client.
    """
    if platform.system() != "Linux":
        return
    
    try:
        # Get current soft and hard limits
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        target_limit = 1048576
        
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
    # Check and adjust ulimit on Linux
    check_and_set_ulimit()
    
    args = parse_args()
    if args.cps <= 0:
        print("Error: --cps must be > 0", file=sys.stderr)
        sys.exit(1)
    if args.pps < 0:
        print("Error: --pps must be >= 0", file=sys.stderr)
        sys.exit(1)
    if args.duration < 0:
        print("Error: --duration must be >= 0", file=sys.stderr)
        sys.exit(1)
    if args.pps > 0 and args.duration == 0:
        print("Warning: --pps has no effect without --duration > 0 (single-shot mode)",
              file=sys.stderr)
    if args.file and args.protocol == "udp":
        print("Error: -F/--file is only supported with TCP (UDP is unreliable for integrity checks)",
              file=sys.stderr)
        sys.exit(1)
    if args.file:
        try:
            load_file_metadata(args.file)  # validate early: existence + size
        except (OSError, ValueError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    if args.output:
        try:
            sys.stdout = Tee(args.output)
        except Exception as e:
            print(f"Error opening output file: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        asyncio.run(run_client(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
