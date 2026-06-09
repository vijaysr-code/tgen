#!/usr/bin/env python3
"""
Traffic Generator Client
Generates TCP or UDP traffic with configurable rate and connection duration.
"""

import asyncio
import argparse
import atexit
import hashlib
import logging
import os
import platform
import resource
import socket
import struct
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
        # Keep cache small (last 60 seconds) - clear before adding new entry
        if len(_TIMESTAMP_CACHE) > 60:
            _TIMESTAMP_CACHE.clear()
        _TIMESTAMP_CACHE[sec] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(sec))
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


# Formatting constants for summary output
_SUMMARY_WIDTH = 55
_SUMMARY_SEPARATOR = "=" * _SUMMARY_WIDTH
_INDENT = "  "
_TCP_INDENT = "    "


def _safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division returning default when denominator is zero."""
    return numerator / denominator if denominator > 0 else default


@dataclass
class SummaryStats:
    """Pre-computed statistics for efficient summary generation."""
    elapsed: float
    rate: float
    # Latency statistics
    avg_latency: float = 0.0
    min_latency: float = 0.0
    max_latency: float = 0.0
    # TCP statistics
    total_retransmits: int = 0
    avg_retransmits: float = 0.0
    retx_connections: int = 0
    loss_connections: int = 0
    avg_bytes_per_conn: float = 0.0
    avg_packets_per_conn: float = 0.0
    # RTT statistics
    avg_rtt: float = 0.0
    min_rtt: float = 0.0
    max_rtt: float = 0.0
    # RTT variance statistics
    avg_rtt_var: float = 0.0
    min_rtt_var: float = 0.0
    max_rtt_var: float = 0.0
    # Lost packets
    total_lost: int = 0


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
    # Cached statistics for performance
    _cached_stats: Optional[SummaryStats] = None

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
        """Flush any remaining buffered results and compute cached statistics."""
        if self._batch_buffer:
            self._flush_batch()
        
        # Compute all statistics once for efficient summary generation
        elapsed = time.monotonic() - self.start_time
        rate = _safe_divide(self.total, elapsed)
        
        # Latency statistics
        avg_latency = _safe_divide(sum(self.latencies), len(self.latencies)) if self.latencies else 0.0
        min_latency = min(self.latencies) if self.latencies else 0.0
        max_latency = max(self.latencies) if self.latencies else 0.0
        
        # TCP statistics
        total_retransmits = sum(self.tcp_retransmits_list) if self.tcp_retransmits_list else 0
        avg_retransmits = _safe_divide(total_retransmits, len(self.tcp_retransmits_list))
        retx_connections = sum(1 for v in self.tcp_retransmits_list if v > 0) if self.tcp_retransmits_list else 0
        loss_connections = sum(1 for v in self.tcp_lost_list if v > 0) if self.tcp_lost_list else 0
        avg_bytes_per_conn = _safe_divide(self.total_bytes_sent, self.success)
        avg_packets_per_conn = _safe_divide(self.total_packets, self.success)
        
        # RTT statistics
        avg_rtt = _safe_divide(sum(self.tcp_rtt_list), len(self.tcp_rtt_list)) if self.tcp_rtt_list else 0.0
        min_rtt = min(self.tcp_rtt_list) if self.tcp_rtt_list else 0.0
        max_rtt = max(self.tcp_rtt_list) if self.tcp_rtt_list else 0.0
        
        # RTT variance statistics
        avg_rtt_var = _safe_divide(sum(self.tcp_rtt_var_list), len(self.tcp_rtt_var_list)) if self.tcp_rtt_var_list else 0.0
        min_rtt_var = min(self.tcp_rtt_var_list) if self.tcp_rtt_var_list else 0.0
        max_rtt_var = max(self.tcp_rtt_var_list) if self.tcp_rtt_var_list else 0.0
        
        # Lost packets
        total_lost = sum(self.tcp_lost_list) if self.tcp_lost_list else 0
        
        self._cached_stats = SummaryStats(
            elapsed=elapsed,
            rate=rate,
            avg_latency=avg_latency,
            min_latency=min_latency,
            max_latency=max_latency,
            total_retransmits=total_retransmits,
            avg_retransmits=avg_retransmits,
            retx_connections=retx_connections,
            loss_connections=loss_connections,
            avg_bytes_per_conn=avg_bytes_per_conn,
            avg_packets_per_conn=avg_packets_per_conn,
            avg_rtt=avg_rtt,
            min_rtt=min_rtt,
            max_rtt=max_rtt,
            avg_rtt_var=avg_rtt_var,
            min_rtt_var=min_rtt_var,
            max_rtt_var=max_rtt_var,
            total_lost=total_lost,
        )
    
    def _format_basic_stats(self, stats: SummaryStats) -> List[str]:
        """Format basic connection statistics."""
        timestamp = _format_timestamp()
        return [
            f"{_INDENT}CLIENT SUMMARY [{timestamp}]",
            _SUMMARY_SEPARATOR,
            f"{_INDENT}Elapsed time      : {stats.elapsed:.2f}s",
            f"{_INDENT}Total connections : {self.total}",
            f"{_INDENT}Successful        : {self.success}",
            f"{_INDENT}Failed            : {self.failed}",
            f"{_INDENT}Observed rate     : {stats.rate:.2f} conn/s",
            f"{_INDENT}Total packets sent: {self.total_packets}",
            f"{_INDENT}Total bytes sent  : {self.total_bytes_sent}",
        ]
    
    def _format_latency_stats(self, stats: SummaryStats) -> List[str]:
        """Format latency statistics."""
        if not self.latencies:
            return []
        return [
            f"{_INDENT}Latency avg       : {stats.avg_latency:.2f}ms",
            f"{_INDENT}Latency min       : {stats.min_latency:.2f}ms",
            f"{_INDENT}Latency max       : {stats.max_latency:.2f}ms",
        ]
    
    def _format_tcp_stats(self, stats: SummaryStats) -> List[str]:
        """Format TCP statistics."""
        if not self.tcp_retransmits_list:
            return []
        return [
            "",
            f"{_INDENT}TCP Statistics:",
            f"{_TCP_INDENT}Total retransmits : {stats.total_retransmits}",
            f"{_TCP_INDENT}Avg retransmits   : {stats.avg_retransmits:.1f} per conn",
            f"{_TCP_INDENT}Connections w/ retx: {stats.retx_connections}",
            f"{_TCP_INDENT}Connections w/ loss: {stats.loss_connections}",
            f"{_TCP_INDENT}Avg bytes / conn  : {stats.avg_bytes_per_conn:.1f}",
            f"{_TCP_INDENT}Avg pkts / conn   : {stats.avg_packets_per_conn:.1f}",
            f"{_TCP_INDENT}Total retries     : {self.total_retry_attempts}",
        ]
    
    def _format_rtt_stats(self, stats: SummaryStats) -> List[str]:
        """Format RTT and RTT variance statistics."""
        lines = []
        if self.tcp_rtt_list:
            lines.extend([
                f"{_TCP_INDENT}RTT avg           : {stats.avg_rtt:.2f}ms",
                f"{_TCP_INDENT}RTT min           : {stats.min_rtt:.2f}ms",
                f"{_TCP_INDENT}RTT max           : {stats.max_rtt:.2f}ms",
            ])
        if self.tcp_rtt_var_list:
            lines.extend([
                f"{_TCP_INDENT}RTT var avg       : {stats.avg_rtt_var:.2f}ms",
                f"{_TCP_INDENT}RTT var min       : {stats.min_rtt_var:.2f}ms",
                f"{_TCP_INDENT}RTT var max       : {stats.max_rtt_var:.2f}ms",
            ])
        return lines
    
    def _format_lost_packets(self, stats: SummaryStats) -> List[str]:
        """Format lost packets statistics."""
        if not self.tcp_lost_list or stats.total_lost == 0:
            return []
        return [f"{_TCP_INDENT}Total lost packets: {stats.total_lost}"]
    
    def _format_failure_stats(self) -> List[str]:
        """Format failure statistics by type."""
        if not self.failed:
            return []
        return [
            f"{_TCP_INDENT}Failures RST      : {self.rst_failures}",
            f"{_TCP_INDENT}Failures timeout  : {self.timeout_failures}",
            f"{_TCP_INDENT}Failures refused  : {self.refused_failures}",
            f"{_TCP_INDENT}Failures aborted  : {self.aborted_failures}",
            f"{_TCP_INDENT}Failures broken   : {self.broken_pipe_failures}",
            f"{_TCP_INDENT}Failures other    : {self.other_failures}",
        ]

    def summary(self) -> str:
        """Generate summary statistics. Flushes any pending batch updates first."""
        self.finalize()  # Flush any remaining buffered results and compute cached stats
        
        if self._cached_stats is None:
            return "\n" + _SUMMARY_SEPARATOR + "\n" + f"{_INDENT}No statistics available" + "\n" + _SUMMARY_SEPARATOR
        
        stats = self._cached_stats
        lines = ["\n" + _SUMMARY_SEPARATOR]
        lines.extend(self._format_basic_stats(stats))
        lines.extend(self._format_latency_stats(stats))
        lines.extend(self._format_tcp_stats(stats))
        lines.extend(self._format_rtt_stats(stats))
        lines.extend(self._format_lost_packets(stats))
        lines.extend(self._format_failure_stats())
        lines.append(_SUMMARY_SEPARATOR)
        
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


def _format_debug_log(conn_id: int, message: str, **kwargs) -> str:
    """
    Format a structured debug log entry with consistent formatting.
    
    Args:
        conn_id: Connection ID
        message: Main debug message
        **kwargs: Additional key-value pairs to include in the log
    
    Returns:
        Formatted debug log string
    """
    timestamp = _format_timestamp()
    parts = [f"[{timestamp}] [{conn_id:>6}] DEBUG: {message}"]
    
    if kwargs:
        details = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        parts.append(f" | {details}")
    
    return "".join(parts)


def _calculate_backoff_delay(attempt: int) -> float:
    """
    Calculate exponential backoff delay for retry attempts.
    
    Strategy:
    - Attempt 0 (first): 0s (no delay)
    - Attempt 1 (first retry): 2s
    - Attempt 2 (second retry): 5s
    - Attempt 3+ (third+ retry): 10s (capped)
    
    Args:
        attempt: Zero-based attempt number (0 = first attempt, 1 = first retry)
    
    Returns:
        Delay in seconds before the next attempt
    """
    if attempt == 0:
        return 0.0
    return min(2.0 * (2.5 ** (attempt - 1)), 10.0)


def _log_retry_attempt(error: Exception, conn_id: int, attempt: int, 
                       max_retries: int, host: str, port: int, 
                       timeout: float, is_final: bool,
                       logger: Optional[logging.Logger] = None) -> None:
    """
    Log retry attempt with appropriate detail based on error type and attempt number.
    
    Args:
        error: The exception that triggered the retry
        conn_id: Connection ID for logging
        attempt: Zero-based attempt number
        max_retries: Maximum number of retry attempts
        host: Target host
        port: Target port
        timeout: Connection timeout in seconds
        is_final: Whether this is the final attempt
        logger: Optional logger instance (uses print if None)
    """
    timestamp = _format_timestamp()
    
    def _log(message: str):
        if logger:
            if is_final:
                logger.error(message)
            else:
                logger.warning(message)
        else:
            print(message)
    
    if isinstance(error, asyncio.TimeoutError):
        elapsed_ms = timeout * 1000
        detail = _format_timeout_detail(
            phase="connect", host=host, port=port, timeout=timeout,
            elapsed_ms=elapsed_ms, attempt=attempt + 1, max_retries=max_retries
        )
        if is_final:
            _log(f"[{timestamp}] [{conn_id:>6}] ✗ All attempts failed | {detail}")
        else:
            _log(f"[{timestamp}] [{conn_id:>6}] ✗ {detail}")
    
    elif isinstance(error, ConnectionResetError):
        if is_final:
            _log(f"[{timestamp}] [{conn_id:>6}] ✗ All {max_retries} attempts failed with RST")
        else:
            _log(f"[{timestamp}] [{conn_id:>6}] ✗ Connection reset on attempt {attempt + 1}/{max_retries}")


async def connect_with_retry(host: str, port: int, timeout: float,
                             max_retries: int = 3, conn_id: int = 0,
                             logger: Optional[logging.Logger] = None) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
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
        logger: Optional logger instance (uses print if None)
    
    Returns:
        Tuple of (reader, writer) on success
    
    Raises:
        asyncio.TimeoutError: If all retry attempts fail with timeout
        ConnectionResetError: If all retry attempts fail with RST
        ConnectionRefusedError: If connection is refused (no retry)
        OSError: For other connection errors (no retry)
    """
    def _log(message: str, level: str = 'info'):
        """Helper to log via logger or print."""
        if logger:
            getattr(logger, level)(message)
        else:
            print(message)
    
    for attempt in range(max_retries):
        try:
            # Apply backoff delay before retry (not on first attempt)
            backoff_delay = _calculate_backoff_delay(attempt)
            if backoff_delay > 0:
                timestamp = _format_timestamp()
                _log(f"[{timestamp}] [{conn_id:>6}] Retry {attempt}/{max_retries-1} after {backoff_delay:.1f}s delay | timeout={timeout:.1f}s")
                await asyncio.sleep(backoff_delay)
            
            # Attempt connection with consistent timeout
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )
            
            # Success - log if this was a retry
            if attempt > 0:
                timestamp = _format_timestamp()
                _log(f"[{timestamp}] [{conn_id:>6}] ✓ Connected after {attempt} retry(s)")
            
            return reader, writer
            
        except (asyncio.TimeoutError, ConnectionResetError) as e:
            # Retry on timeout or RST - may be transient server issue
            is_final_attempt = (attempt >= max_retries - 1)
            _log_retry_attempt(e, conn_id, attempt, max_retries, host, port, timeout, is_final_attempt, logger)
            if is_final_attempt:
                raise
        
        except (ConnectionRefusedError, OSError) as e:
            # Don't retry on connection refused or other OS errors
            # These indicate server is down/unreachable, not transient issues
            timestamp = _format_timestamp()
            error_type = type(e).__name__
            _log(f"[{timestamp}] [{conn_id:>6}] ✗ {error_type} - no retry", level='error')
            raise
    
    # Unreachable: loop always raises on final attempt
    raise RuntimeError("connect_with_retry: unexpected code path")


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
    local_addr = "unknown"  # Declare at function scope for error handlers
    try:
        # Use retry logic for connection establishment
        reader, writer = await connect_with_retry(host, port, timeout, max_retries=3, conn_id=conn_id)

        # Apply TCP optimizations (keepalive, nodelay, buffers)
        # Get keepalive settings from args
        ka_mode = args.keepalive_mode if args and hasattr(args, 'keepalive_mode') else 'standard'
        ka_profile = _KA_AGGRESSIVE if ka_mode == 'aggressive' else _KA_STANDARD
        
        sock = writer.get_extra_info("socket")
        local_addr = "unknown"
        if sock is not None:
            apply_tcp_optimizations(sock, ka_profile['idle'], ka_profile['interval'], ka_profile['count'])
            try:
                local_ip, local_port = sock.getsockname()
                local_addr = f"{local_ip}:{local_port}"
            except Exception:
                pass

        now = time.monotonic()
        latency_ms = (now - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP connected  | local={local_addr} | target={host}:{port} | latency={latency_ms:.1f}ms ka=on")

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
                                print(_format_debug_log(
                                    conn_id,
                                    f"Heartbeat timeout #{consecutive_failures}",
                                    elapsed=f"{duration - remaining:.1f}s",
                                    max_failures=max_heartbeat_failures
                                ))
                                
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

        # Log successful close with stats
        elapsed = time.monotonic() - t0
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP closed     | local={local_addr} | target={host}:{port} | sent={packets_sent}pkts/{bytes_sent}B | elapsed={elapsed:.1f}s")

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
        elapsed = time.monotonic() - t0
        latency_ms = elapsed * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | local={local_addr} | target={host}:{port} | dur={elapsed:.3f}s | RST: {e} | sent={packets_sent}pkts/{bytes_sent}B")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, packets_sent=packets_sent, error=f"RST: {e}")
    except ConnectionRefusedError as e:
        elapsed = time.monotonic() - t0
        latency_ms = elapsed * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | local={local_addr} | target={host}:{port} | dur={elapsed:.3f}s | refused: {e} | sent={packets_sent}pkts/{bytes_sent}B")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, packets_sent=packets_sent, error=f"refused: {e}")
    except ConnectionAbortedError as e:
        elapsed = time.monotonic() - t0
        latency_ms = elapsed * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | local={local_addr} | target={host}:{port} | dur={elapsed:.3f}s | aborted: {e} | sent={packets_sent}pkts/{bytes_sent}B")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, packets_sent=packets_sent, error=f"aborted: {e}")
    except BrokenPipeError as e:
        elapsed = time.monotonic() - t0
        latency_ms = elapsed * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | local={local_addr} | target={host}:{port} | dur={elapsed:.3f}s | broken_pipe: {e} | sent={packets_sent}pkts/{bytes_sent}B")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, packets_sent=packets_sent, error=f"broken_pipe: {e}")
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
        
        # Log failure with timeout details
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | {timeout_detail}")
        
        # Build consolidated debug information
        debug_parts = {
            'phase': debug_info,
            'bytes_sent': bytes_sent,
            'packets_sent': packets_sent,
            'elapsed': f"{latency_ms:.1f}ms"
        }
        
        # Add TCP stats if available
        if tcp_retx is not None or tcp_rtt is not None:
            if tcp_retx is not None:
                debug_parts['tcp_retx'] = tcp_retx
            if tcp_rtt is not None:
                debug_parts['tcp_rtt'] = f"{tcp_rtt:.1f}ms"
            if tcp_cwnd is not None:
                debug_parts['tcp_cwnd'] = tcp_cwnd
            if tcp_lost is not None:
                debug_parts['tcp_lost'] = tcp_lost
        else:
            debug_parts['tcp_stats'] = 'unavailable'
        
        # Print single consolidated debug log
        print(_format_debug_log(conn_id, "Timeout details", **debug_parts))
        
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, error=timeout_detail,
                                  tcp_retransmits=tcp_retx, tcp_rtt_ms=tcp_rtt,
                                  tcp_rtt_var_ms=tcp_rtt_var, tcp_snd_cwnd=tcp_cwnd,
                                  tcp_lost_packets=tcp_lost, tcp_reordering=tcp_reordering)
    except OSError as e:
        elapsed = time.monotonic() - t0
        latency_ms = elapsed * 1000
        timestamp = _format_timestamp()
        error_detail = f"error:{e.errno}" if hasattr(e, 'errno') else str(e)
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | local={local_addr} | target={host}:{port} | dur={elapsed:.3f}s | {error_detail} | sent={packets_sent}pkts/{bytes_sent}B")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, packets_sent=packets_sent, error=error_detail)
    except Exception as e:
        elapsed = time.monotonic() - t0
        latency_ms = elapsed * 1000
        timestamp = _format_timestamp()
        error_detail = f"{type(e).__name__}: {e}"
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | local={local_addr} | target={host}:{port} | dur={elapsed:.3f}s | {error_detail} | sent={packets_sent}pkts/{bytes_sent}B")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, bytes_sent=bytes_sent, packets_sent=packets_sent, error=error_detail)
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


def _build_udp_packet(seq_num: int, interval_ms: int, payload: bytes) -> bytes:
    """Build UDP packet with sequence number and interval header."""
    return struct.pack('!II', seq_num, interval_ms) + payload

# UDP send buffer size constant (4MB)
_UDP_SEND_BUFFER_SIZE = 4 * 1024 * 1024


def _configure_udp_send_buffer(transport, buffer_size: int = _UDP_SEND_BUFFER_SIZE) -> bool:
    """Configure UDP socket send buffer size.
    
    Args:
        transport: The datagram transport
        buffer_size: Desired buffer size in bytes (default: 4MB)
    
    Returns:
        True if successful, False otherwise
    """
    sock = transport.get_extra_info('socket')
    if not sock:
        return False
    
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buffer_size)
        return True
    except OSError as e:
        # Log warning but don't fail - system may not allow buffer changes
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [WARN  ] UDP buffer config failed: {e}")
        return False


def _format_udp_error(e: Exception) -> str:
    """Format UDP error with errno-specific messages.
    
    Args:
        e: The exception to format
    
    Returns:
        Human-readable error description
    """
    if isinstance(e, OSError) and hasattr(e, 'errno') and e.errno is not None:
        error_map = {
            111: "port_unreachable (ICMP)",
            101: "network_unreachable",
            113: "host_unreachable",
            90: "message_too_long"
        }
        return error_map.get(e.errno, f"error:{e.errno}")
    return f"{type(e).__name__}: {e}" if not isinstance(e, OSError) else str(e)


async def _send_udp_packets(transport, payload: bytes, pps: int, duration: float, 
                           start_time: float) -> Tuple[int, int]:
    """Send UDP packets at specified rate for given duration.
    
    Args:
        transport: The datagram transport
        payload: Payload bytes to send
        pps: Packets per second rate
        duration: Duration in seconds
        start_time: Start time from monotonic clock
    
    Returns:
        Tuple of (packets_sent, bytes_sent)
    """
    interval = 1.0 / pps
    interval_ms = int(interval * 1000)
    deadline = start_time + duration
    packets_sent = 0
    bytes_sent = 0
    seq_num = 0
    now = start_time
    
    while now < deadline:
        # Prepend sequence number (4 bytes) and interval_ms (4 bytes) to payload
        # Server uses interval_ms to calculate dynamic timeout: interval_ms * 3
        packet = _build_udp_packet(seq_num, interval_ms, payload)
        transport.sendto(packet)
        bytes_sent += len(packet)
        packets_sent += 1
        seq_num += 1
        
        next_send = now + interval
        if next_send >= deadline:
            break
        
        # Sleep until next send time, accounting for drift and preventing negative sleep
        sleep_time = max(0, next_send - time.monotonic())
        await asyncio.sleep(sleep_time)
        now = time.monotonic()
    
    return packets_sent, bytes_sent


def _log_udp_success(conn_id: int, latency_ms: float, packets_sent: int, 
                     packet_size: int, is_periodic: bool):
    """Log successful UDP transmission.
    
    Args:
        conn_id: Connection ID
        latency_ms: Latency in milliseconds
        packets_sent: Number of packets sent
        packet_size: Size of each packet in bytes
        is_periodic: True if periodic traffic, False if single packet
    """
    timestamp = _format_timestamp()
    if is_periodic:
        print(f"[{timestamp}] [{conn_id:>6}] UDP sent       | "
              f"latency={latency_ms:.1f}ms | {packets_sent} pkts | {packet_size}B each")
    else:
        print(f"[{timestamp}] [{conn_id:>6}] UDP sent       | "
              f"latency={latency_ms:.1f}ms | {packet_size}B")


def _log_udp_failure(conn_id: int, host: str, port: int, error_detail: str):
    """Log UDP connection failure.
    
    Args:
        conn_id: Connection ID
        host: Target host
        port: Target port
        error_detail: Error description
    """
    timestamp = _format_timestamp()
    print(f"[{timestamp}] [{conn_id:>6}] UDP FAILED     | target={host}:{port} | {error_detail}")


async def _send_single_udp_packet(transport, payload: bytes, duration: float) -> Tuple[int, int]:
    """Send a single UDP packet and optionally sleep for duration.
    
    Args:
        transport: The datagram transport
        payload: Payload bytes to send
        duration: Duration to sleep after sending (0 = no sleep)
    
    Returns:
        Tuple of (packets_sent, bytes_sent)
    """
    packet = _build_udp_packet(0, 0, payload)
    transport.sendto(packet)
    bytes_sent = len(packet)
    packets_sent = 1
    
    if duration > 0:
        await asyncio.sleep(duration)
    
    return packets_sent, bytes_sent


def _create_success_result(conn_id: int, latency_ms: float, packets_sent: int,
                          bytes_sent: int, dashboard_tracker) -> ConnectionResult:
    """Create success result and update dashboard if enabled.
    
    Args:
        conn_id: Connection identifier
        latency_ms: Latency in milliseconds
        packets_sent: Number of packets sent
        bytes_sent: Total bytes sent
        dashboard_tracker: Optional dashboard metrics tracker
    
    Returns:
        ConnectionResult with success=True
    """
    if dashboard_tracker:
        dashboard_tracker.record_connection(True, latency_ms, packets_sent)
    
    return ConnectionResult(
        conn_id=conn_id,
        success=True,
        latency_ms=latency_ms,
        packets_sent=packets_sent,
        bytes_sent=bytes_sent
    )


def _create_error_result(conn_id: int, host: str, port: int, t0: float,
                        bytes_sent: int, e: Exception, dashboard_tracker) -> ConnectionResult:
    """Create error result, log failure, and update dashboard if enabled.
    
    Args:
        conn_id: Connection identifier
        host: Target hostname or IP
        port: Target port
        t0: Start time from monotonic clock
        bytes_sent: Bytes sent before error
        e: The exception that occurred
        dashboard_tracker: Optional dashboard metrics tracker
    
    Returns:
        ConnectionResult with success=False
    """
    latency_ms = (time.monotonic() - t0) * 1000
    error_detail = _format_udp_error(e)
    _log_udp_failure(conn_id, host, port, error_detail)
    
    if dashboard_tracker:
        dashboard_tracker.record_connection(False, latency_ms, 0)
    
    return ConnectionResult(
        conn_id=conn_id,
        success=False,
        latency_ms=latency_ms,
        bytes_sent=bytes_sent,
        error=error_detail
    )



async def udp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats, args=None, dashboard_tracker=None):
    """Send UDP traffic to target host:port.
    
    Args:
        conn_id: Connection identifier
        host: Target hostname or IP
        port: Target port
        payload: Payload bytes to send
        duration: Connection duration in seconds
        stats: Statistics tracker
        args: Optional arguments (for pps rate)
        dashboard_tracker: Optional dashboard metrics tracker
    """
    t0 = time.monotonic()
    loop = asyncio.get_running_loop()
    pps = args.pps if args else 0
    transport = None
    bytes_sent = 0
    result: Optional[ConnectionResult] = None
    
    try:
        transport, protocol = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol,
            remote_addr=(host, port)
        )
        
        # Configure UDP send buffer for high-throughput scenarios
        _configure_udp_send_buffer(transport)
        
        # Capture latency right after endpoint creation (before any sends)
        now = time.monotonic()
        latency_ms = (now - t0) * 1000
        
        if pps > 0 and duration > 0:
            # Send packets at specified rate for duration
            packets_sent, bytes_sent = await _send_udp_packets(
                transport, payload, pps, duration, now
            )
            _log_udp_success(conn_id, latency_ms, packets_sent, len(payload) + 8, True)
        else:
            # Single packet with optional sleep for duration
            packets_sent, bytes_sent = await _send_single_udp_packet(
                transport, payload, duration
            )
            _log_udp_success(conn_id, latency_ms, packets_sent, len(payload) + 8, False)

        result = _create_success_result(
            conn_id, latency_ms, packets_sent, bytes_sent, dashboard_tracker
        )
            
    except Exception as e:
        result = _create_error_result(
            conn_id, host, port, t0, bytes_sent, e, dashboard_tracker
        )
            
    finally:
        if transport is not None:
            transport.close()
    
    # CRITICAL FIX: Record result in stats (was missing before)
    if result is not None:
        stats.record(result)
    
    return result


async def run_client(args):
    stats = Stats()
    interval = 1.0 / args.cps if args.cps > 0 else 0.0
    conn_id = 0
    tasks = set()
    
    # Calculate timeout based on TOTAL CPS (not per-worker CPS in multiprocess mode)
    # Use original_cps if provided by multiprocess client, otherwise use args.cps
    # This ensures timeout scales with actual server load, not per-worker rate
    timeout_cps = getattr(args, 'original_cps', args.cps)
    connection_timeout = calculate_timeout(timeout_cps)
    
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
    last_progress_time = start_time
    progress_interval = 60.0  # Print progress every 1 minute
    
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
            # Print periodic progress while waiting for connections to complete
            wait_start = time.monotonic()
            while tasks:
                # Wait for tasks with a timeout for progress updates
                remaining_time = wait_timeout - (time.monotonic() - wait_start)
                if remaining_time <= 0:
                    print(f"Warning: {len(tasks)} tasks still running after {wait_timeout:.0f}s.",
                          file=sys.stderr)
                    break
                
                wait_duration = min(progress_interval, remaining_time)
                done, pending = await asyncio.wait(tasks, timeout=wait_duration)
                
                # Print progress update if interval has passed
                current_time = time.monotonic()
                elapsed = current_time - start_time
                if (current_time - last_progress_time) >= progress_interval:
                    # Flush batch buffer to get current error counts
                    stats.finalize()
                    active_tasks = len(tasks)
                    timestamp = _format_timestamp()
                    # Build error breakdown if there are errors
                    error_detail = ""
                    if stats.failed > 0:
                        error_parts = []
                        if stats.rst_failures > 0:
                            error_parts.append(f"{stats.rst_failures} RST")
                        if stats.timeout_failures > 0:
                            error_parts.append(f"{stats.timeout_failures} timeout")
                        if stats.refused_failures > 0:
                            error_parts.append(f"{stats.refused_failures} refused")
                        if stats.aborted_failures > 0:
                            error_parts.append(f"{stats.aborted_failures} aborted")
                        if stats.broken_pipe_failures > 0:
                            error_parts.append(f"{stats.broken_pipe_failures} broken")
                        if stats.other_failures > 0:
                            error_parts.append(f"{stats.other_failures} other")
                        if error_parts:
                            error_detail = f" ({', '.join(error_parts)})"
                    
                    print(f"[{timestamp}] {conn_id} connections created, "
                          f"{active_tasks} active, {stats.failed} errors{error_detail}, elapsed {elapsed:.0f}s")
                    sys.stdout.flush()  # Ensure progress message is written immediately
                    last_progress_time = current_time
                
                # If no tasks remain, exit the loop
                if not tasks:
                    break

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
        
        # Return stats object for programmatic access (e.g., multiprocess workers)
        return stats


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
  * --heartbeat sends application-level keepalive for TCP when pps=0 (tolerates 3 failures).
  * UDP sessions use dynamic timeout on server: 3x packet interval (min 5s, max 300s).
  * -F/--file is TCP-only; --duration, --pps, and --payload are ignored in file mode.
  * --pps has no effect unless --duration > 0.
  * --pps accepts fractional values for low-rate traffic (see PPS REFERENCE below).

PPS REFERENCE
-------------
  Common PPS values for different use cases:

  High-Rate Traffic (Throughput Testing):
    --pps 1000      = 1,000 packets/sec (1 ms interval)
    --pps 100       = 100 packets/sec (10 ms interval)
    --pps 10        = 10 packets/sec (100 ms interval)

  Medium-Rate Traffic (Moderate Load):
    --pps 1         = 1 packet/sec (1 second interval)
    --pps 0.5       = 1 packet every 2 seconds
    --pps 0.2       = 1 packet every 5 seconds

  Low-Rate Traffic (Keepalive/NAT Traversal):
    --pps 0.1       = 1 packet every 10 seconds (aggressive keepalive)
    --pps 0.067     = 1 packet every 15 seconds (standard keepalive)
    --pps 0.05      = 1 packet every 20 seconds
    --pps 0.033     = 1 packet every 30 seconds (typical NAT timeout)
    --pps 0.017     = 1 packet every 60 seconds (1 minute interval)
    --pps 0.0083    = 1 packet every 2 minutes
    --pps 0.0033    = 1 packet every 5 minutes

  Formula: pps = 1 / interval_seconds
  Example: For 45-second interval: pps = 1/45 = 0.022

EXAMPLES
--------
  # 10 short-lived TCP connections at 5/sec
  python client.py --port 9000 --cps 5 --total 10

  # Long-lived UDP connections (2s each) at 2/sec, 1000 total (default)
  python client.py --port 9001 --protocol udp --cps 2 --duration 2

  # High-rate: 100 packets/s per connection for 5 seconds, 2 connections/sec
  python client.py --port 9000 --cps 2 --duration 5 --pps 100

  # Low-rate: UDP keepalive every 30 seconds for 5 minutes (NAT traversal)
  python client.py --port 9001 --protocol udp --duration 300 --pps 0.033

  # Low-rate: TCP with 1 packet every 15 seconds for 10 minutes
  python client.py --port 9000 --duration 600 --pps 0.067

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
        print(f"Warning: Could not check ulimit: {type(e).__name__}: {e}", file=sys.stderr)


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
            print(f"Error: {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(1)

    if args.output:
        try:
            sys.stdout = Tee(args.output)
        except Exception as e:
            print(f"Error opening output file: {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        asyncio.run(run_client(args))
    except KeyboardInterrupt:
        pass
    finally:
        # Ensure output is flushed and closed when using --output
        if args.output:
            sys.stdout.flush()
            if hasattr(sys.stdout, 'close'):
                sys.stdout.close()


if __name__ == "__main__":
    main()
