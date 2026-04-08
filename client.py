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


def _format_timestamp() -> str:
    """Fast timestamp formatting for logging. Returns ISO-8601 format with milliseconds."""
    t = time.time()
    ms = int((t % 1) * 1000)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t)) + f".{ms:03d}"

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


@dataclass
class ConnectionResult:
    conn_id: int
    success: bool
    latency_ms: float
    packets_sent: int = 0
    error: Optional[str] = None
    # TCP statistics (Linux only)
    tcp_retransmits: Optional[int] = None
    tcp_rtt_ms: Optional[float] = None
    tcp_lost_packets: Optional[int] = None


@dataclass
class Stats:
    total: int = 0
    success: int = 0
    failed: int = 0
    total_packets: int = 0
    latencies: List[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)
    tcp_retransmits_list: List[int] = field(default_factory=list)
    tcp_rtt_list: List[float] = field(default_factory=list)
    tcp_lost_list: List[int] = field(default_factory=list)

    def record(self, result: ConnectionResult):
        self.total += 1
        if result.success:
            self.success += 1
            self.latencies.append(result.latency_ms)
            self.total_packets += result.packets_sent
            if result.tcp_retransmits is not None:
                self.tcp_retransmits_list.append(result.tcp_retransmits)
            if result.tcp_rtt_ms is not None:
                self.tcp_rtt_list.append(result.tcp_rtt_ms)
            if result.tcp_lost_packets is not None:
                self.tcp_lost_list.append(result.tcp_lost_packets)
        else:
            self.failed += 1

    def summary(self) -> str:
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
            lines += [
                "",
                "  TCP Statistics:",
                f"    Total retransmits : {total_retx}",
                f"    Avg retransmits   : {total_retx/len(self.tcp_retransmits_list):.1f} per conn",
            ]
        if self.tcp_rtt_list:
            avg_rtt = sum(self.tcp_rtt_list) / len(self.tcp_rtt_list)
            lines += [
                f"    RTT avg           : {avg_rtt:.2f}ms",
                f"    RTT min           : {min(self.tcp_rtt_list):.2f}ms",
                f"    RTT max           : {max(self.tcp_rtt_list):.2f}ms",
            ]
        if self.tcp_lost_list:
            total_lost = sum(self.tcp_lost_list)
            if total_lost > 0:
                lines += [
                    f"    Total lost packets: {total_lost}",
                ]
        
        lines.append("=" * 55)
        return "\n".join(lines)


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


def get_tcp_info(sock: socket.socket) -> Tuple[Optional[int], Optional[float], Optional[int]]:
    """
    Retrieve TCP socket statistics (Linux only).
    Returns: (retransmits, rtt_ms, lost_packets)
    """
    system = platform.system()
    if system != "Linux":
        return (None, None, None)
    
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
        lost = struct.unpack_from('I', tcp_info, 32)[0]         # tcpi_lost (u32)
        
        # Convert microseconds to milliseconds
        rtt_ms = rtt_us / 1000.0 if rtt_us > 0 else None
        
        return (retransmits, rtt_ms, lost)
    except Exception:
        return (None, None, None)


async def tcp_file_transfer(conn_id: int, host: str, port: int,
                             header: bytes, file_data: bytes, size: int,
                             stats: Stats):
    """Send a file over TCP with a checksum header; report PASS/FAIL."""
    t0 = time.monotonic()
    result: Optional[ConnectionResult] = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=30
        )
        sock = writer.get_extra_info("socket")
        if sock is not None:
            apply_tcp_optimizations(sock)

        latency_ms = (time.monotonic() - t0) * 1000

        # Send header then file data in chunks
        writer.write(header)
        await writer.drain()
        
        # Send file data in 64KB chunks to avoid blocking the event loop
        chunk_size = 65536
        for i in range(0, len(file_data), chunk_size):
            writer.write(file_data[i:i + chunk_size])
            await writer.drain()

        # Read server response: "OK\n" or "FAIL:<reason>\n"
        response = await asyncio.wait_for(reader.readline(), timeout=30)
        response_str = response.decode(errors="replace").strip()

        # Collect TCP statistics before closing
        tcp_retx, tcp_rtt, tcp_lost = get_tcp_info(sock) if sock else (None, None, None)
        
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
                                      latency_ms=latency_ms, packets_sent=1,
                                      tcp_retransmits=tcp_retx, tcp_rtt_ms=tcp_rtt,
                                      tcp_lost_packets=tcp_lost)
        else:
            print(
                f"[{conn_id:>6}] TCP file xfer  | latency={latency_ms:.1f}ms "
                f"total={elapsed_ms:.0f}ms {size}B | checksum=FAIL ({response_str})"
            )
            result = ConnectionResult(conn_id=conn_id, success=False,
                                      latency_ms=latency_ms,
                                      error=f"checksum mismatch: {response_str}")
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP file FAILED | target={host}:{port} | {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, error=str(e))
    if result is not None:
        stats.record(result)


async def tcp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats, args=None):
    t0 = time.monotonic()
    packets_sent = 0
    pps = args.pps if args else 0
    result: Optional[ConnectionResult] = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=30
        )

        # Apply TCP optimizations (keepalive, nodelay, buffers)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            apply_tcp_optimizations(sock)

        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP connected  | latency={latency_ms:.1f}ms ka=on")

        if pps > 0 and duration > 0:
            interval = 1.0 / pps
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline:
                writer.write(payload)
                await writer.drain()
                packets_sent += 1
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(interval, remaining))
        else:
            # Single send (original behaviour)
            writer.write(payload)
            await writer.drain()
            packets_sent = 1
            if duration > 0:
                await asyncio.sleep(duration)

        # Collect TCP statistics before closing
        tcp_retx, tcp_rtt, tcp_lost = get_tcp_info(sock) if sock else (None, None, None)
        
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        result = ConnectionResult(conn_id=conn_id, success=True,
                                  latency_ms=latency_ms, packets_sent=packets_sent,
                                  tcp_retransmits=tcp_retx, tcp_rtt_ms=tcp_rtt,
                                  tcp_lost_packets=tcp_lost)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] TCP FAILED     | target={host}:{port} | {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, error=str(e))
    if result is not None:
        stats.record(result)


async def udp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats, args=None):
    t0 = time.monotonic()
    loop = asyncio.get_running_loop()
    packets_sent = 0
    pps = args.pps if args else 0
    transport = None
    result: Optional[ConnectionResult] = None
    try:
        transport, protocol = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol,
            remote_addr=(host, port)
        )
        # Capture latency right after endpoint creation (before any sends)
        latency_ms = (time.monotonic() - t0) * 1000
        if pps > 0 and duration > 0:
            interval = 1.0 / pps
            deadline = time.monotonic() + duration
            # Calculate expected packet count
            expected_packets = int(duration * pps)
            seq_num = 0
            while time.monotonic() < deadline:
                # Prepend sequence number (4 bytes) and expected total (4 bytes) to payload
                import struct
                packet = struct.pack('!II', seq_num, expected_packets) + payload
                transport.sendto(packet)
                packets_sent += 1
                seq_num += 1
                remaining = deadline - time.monotonic()
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
            packets_sent = 1
            timestamp = _format_timestamp()
            print(f"[{timestamp}] [{conn_id:>6}] UDP sent       | latency={latency_ms:.1f}ms | {len(packet)}B")
            if duration > 0:
                await asyncio.sleep(duration)

        result = ConnectionResult(conn_id=conn_id, success=True,
                                  latency_ms=latency_ms, packets_sent=packets_sent)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        timestamp = _format_timestamp()
        print(f"[{timestamp}] [{conn_id:>6}] UDP FAILED     | target={host}:{port} | {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, error=str(e))
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
        print(f"  Keepalive: ON (idle={_KA_IDLE}s, intvl={_KA_INTERVAL}s, cnt={_KA_COUNT})")
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
                                             file_header, file_data, file_size, stats)
                elif args.protocol == "tcp":
                    coro = tcp_connection(conn_id, args.host, args.port, payload,
                                          args.duration, stats, args)
                else:
                    coro = udp_connection(conn_id, args.host, args.port, payload,
                                          args.duration, stats, args)

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
        print(stats.summary())


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
  -F, --file PATH       File to send over each TCP connection (max 128 MiB);
                        server verifies SHA-256 checksum per connection
  --output PATH         Optional file path to log the output results
  -h, --help            Show this help message and exit

NOTES
-----
  * TCP keepalive is always enabled (idle=10s, interval=10s, count=5).
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
    parser.add_argument("-F", "--file", type=str, default=None,
                        help="File to send over each TCP connection (max 128 MiB); "
                             "server verifies SHA-256 checksum per connection")
    parser.add_argument("--output", type=str, default=None,
                        help="Optional file path to log the output results")
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
