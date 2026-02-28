#!/usr/bin/env python3
"""
Traffic Generator Server
Receives TCP or UDP connections and collects per-connection statistics.
"""

import asyncio
import argparse
import atexit
import hashlib
import platform
import signal
import socket
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

# Must match client.py constants
_FILE_HEADER_PREFIX = b"TGEN_FILE:"
_FILE_HEADER_LEN = len(_FILE_HEADER_PREFIX) + 64 + 1 + 10 + 1  # 86 bytes
_SERVER_MAX_FILE_SIZE = 128 * 1024 * 1024  # 128 MiB — matches client limit


class Tee:
    def __init__(self, filename):
        self.stdout = sys.stdout
        self.file = open(filename, "a")
        atexit.register(self.file.close)

    def write(self, data):
        self.stdout.write(data)
        self.file.write(data)

    def flush(self):
        self.stdout.flush()
        self.file.flush()


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

    def row(self) -> str:
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
        return (
            f"  {self.conn_id:>6} | {self.client_addr:<22} | "
            f"{dur:>10} | {self.bytes_received:>8}B | {self.messages_received:>5} msg | {pps:>8} pkt/s"
            f"{checksum_col}"
        )


class ServerStats:
    def __init__(self):
        self.records: List[ConnectionRecord] = []
        self.start_time = time.monotonic()
        self._lock = asyncio.Lock()
        self._next_id = 1

    async def new_connection(self, client_addr: str) -> ConnectionRecord:
        async with self._lock:
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
            checksum_hdr = " | Checksum" if has_ft else ""
            lines += [
                "",
                f"  {'ID':>6} | {'Client':<22} | {'Duration':>10} | {'Bytes':>9} | Messages | PPS (recv){checksum_hdr}",
                "  " + "-" * (80 + (10 if has_ft else 0)),
            ]
            for rec in self.records:
                lines.append(rec.row())

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

    # We already have the first chunk; read the rest of the header if needed
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

    # Read exactly expected_size bytes
    file_buf = bytearray(leftover)
    while len(file_buf) < expected_size:
        chunk = await reader.read(min(65536, expected_size - len(file_buf)))
        if not chunk:
            break
        file_buf += chunk

    rec.bytes_received += len(file_buf)
    rec.messages_received = 1

    if len(file_buf) != expected_size:
        reason = f"size mismatch: got {len(file_buf)} expected {expected_size}"
        writer.write(f"FAIL:{reason}\n".encode())
        await writer.drain()
        rec.checksum_ok = False
        print(f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | FAIL {reason}")
        return

    actual_sha256 = hashlib.sha256(file_buf).hexdigest()
    if actual_sha256 == expected_sha256:
        writer.write(b"OK\n")
        await writer.drain()
        rec.checksum_ok = True
        print(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | "
            f"{len(file_buf)}B | checksum=PASS"
        )
    else:
        reason = f"sha256 got={actual_sha256[:16]}... expected={expected_sha256[:16]}..."
        writer.write(f"FAIL:{reason}\n".encode())
        await writer.drain()
        rec.checksum_ok = False
        print(
            f"[{rec.conn_id:>6}] TCP file recv  | {rec.client_addr} | "
            f"{len(file_buf)}B | checksum=FAIL"
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
    print(f"[{rec.conn_id:>6}] TCP connect    | {addr_str} ka=on")

    try:
        # Peek at the first bytes to detect file-transfer mode
        first_chunk = await reader.read(len(_FILE_HEADER_PREFIX))
        if first_chunk == _FILE_HEADER_PREFIX:
            # File-transfer mode: read the rest of the header + file data
            remaining_header = await reader.read(_FILE_HEADER_LEN - len(_FILE_HEADER_PREFIX))
            await _handle_file_transfer(reader, writer, rec, first_chunk + remaining_header)
        elif first_chunk:
            # Normal traffic mode: count this chunk then keep reading
            rec.bytes_received += len(first_chunk)
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
        rec.disconnect_time = time.monotonic()
        dur = rec.duration if rec.duration is not None else 0.0
        print(
            f"[{rec.conn_id:>6}] TCP disconnect | {addr_str} | "
            f"dur={dur:.3f}s | {rec.bytes_received}B"
        )
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_tcp_server(host: str, port: int, stats: ServerStats):
    async def handler(r: asyncio.StreamReader, w: asyncio.StreamWriter):
        await handle_tcp_client(r, w, stats)

    server = await asyncio.start_server(
        handler,
        host, port
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
            print(f"[{rec.conn_id:>6}] UDP new sender | {addr_str}")

        rec = self._sessions[addr]
        rec.bytes_received += len(data)
        rec.messages_received += 1

        # Reset inactivity timer
        if addr in self._timers:
            self._timers[addr].cancel()
        if self._loop is not None:
            self._timers[addr] = self._loop.call_later(
                self.timeout, self._expire_session, addr
            )

    def _expire_session(self, addr: tuple):
        rec = self._sessions.pop(addr, None)
        self._timers.pop(addr, None)
        if rec:
            rec.disconnect_time = time.monotonic()
            print(
                f"[{rec.conn_id:>6}] UDP expired    | {rec.client_addr} | "
                f"dur={rec.duration:.3f}s | {rec.bytes_received}B"
            )

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

    def shutdown(*args):
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
    return parser.parse_args()


def main():
    args = parse_args()
    
    if args.output:
        try:
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
