#!/usr/bin/env python3
"""
Traffic Generator Server
Receives TCP or UDP connections and collects per-connection statistics.
"""

import asyncio
import argparse
import platform
import signal
import socket
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional


class Tee:
    def __init__(self, filename):
        self.stdout = sys.stdout
        self.file = open(filename, "a")

    def write(self, data):
        self.stdout.write(data)
        self.file.write(data)

    def flush(self):
        self.stdout.flush()


@dataclass
class ConnectionRecord:
    conn_id: int
    client_addr: str
    connect_time: float
    disconnect_time: Optional[float] = None
    bytes_received: int = 0
    messages_received: int = 0

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
        return (
            f"  {self.conn_id:>6} | {self.client_addr:<22} | "
            f"{dur:>10} | {self.bytes_received:>8}B | {self.messages_received:>5} msg | {pps:>8} pkt/s"
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

    def summary(self) -> str:
        elapsed = time.monotonic() - self.start_time
        completed = [r for r in self.records if r.duration is not None]
        total = len(self.records)
        rate = total / elapsed if elapsed > 0 else 0
        total_pkts = sum(r.messages_received for r in self.records)
        total_bytes = sum(r.bytes_received for r in self.records)

        lines = [
            "\n" + "=" * 85,
            "  SERVER SUMMARY",
            "=" * 85,
            f"  Elapsed time        : {elapsed:.2f}s",
            f"  Total connections   : {total}",
            f"  Completed           : {len(completed)}",
            f"  Still open          : {total - len(completed)}",
            f"  Observed rate       : {rate:.2f} conn/s",
            f"  Total bytes received: {total_bytes}",
            f"  Total packets recv  : {total_pkts}",
        ]

        if completed:
            # Filter out any None durations defensively (race at shutdown)
            durations: List[float] = [r.duration for r in completed if r.duration is not None]  # type: ignore[misc]
            if durations:
                avg_dur = sum(durations) / len(durations)
                pps_vals: List[float] = []
                for _r in completed:
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
            lines += [
                "",
                f"  {'ID':>6} | {'Client':<22} | {'Duration':>10} | {'Bytes':>9} | Messages | PPS (recv)",
                "  " + "-" * 80,
            ]
            for rec in self.records:
                lines.append(rec.row())

        lines.append("=" * 85)
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
        print(
            f"[{rec.conn_id:>6}] TCP disconnect | {addr_str} | "
            f"dur={rec.duration:.3f}s | {rec.bytes_received}B"
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
            # New "connection"
            rec = ConnectionRecord(
                conn_id=self.stats._next_id,
                client_addr=addr_str,
                connect_time=time.monotonic(),
            )
            self.stats._next_id += 1
            self.stats.records.append(rec)
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

    loop = asyncio.get_event_loop()

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
    parser = argparse.ArgumentParser(
        description="Traffic Generator Server — receives TCP/UDP connections and collects stats.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, required=True, help="Bind port")
    parser.add_argument("--protocol", choices=["tcp", "udp"], default="tcp",
                        help="Protocol to use")
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
