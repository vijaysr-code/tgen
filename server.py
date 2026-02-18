#!/usr/bin/env python3
"""
Traffic Generator Server
Receives TCP or UDP connections and collects per-connection statistics.
"""

import asyncio
import argparse
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


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
        if self.disconnect_time is not None:
            return self.disconnect_time - self.connect_time
        return None

    def row(self) -> str:
        dur = f"{self.duration:.3f}s" if self.duration is not None else "open"
        return (
            f"  {self.conn_id:>6} | {self.client_addr:<22} | "
            f"{dur:>10} | {self.bytes_received:>8}B | {self.messages_received:>5} msg"
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

        lines = [
            "\n" + "=" * 75,
            "  SERVER SUMMARY",
            "=" * 75,
            f"  Elapsed time        : {elapsed:.2f}s",
            f"  Total connections   : {total}",
            f"  Completed           : {len(completed)}",
            f"  Still open          : {total - len(completed)}",
            f"  Observed rate       : {rate:.2f} conn/s",
            f"  Total bytes received: {sum(r.bytes_received for r in self.records)}",
        ]

        if completed:
            durations = [r.duration for r in completed]
            avg = sum(durations) / len(durations)
            lines += [
                f"  Duration avg        : {avg:.3f}s",
                f"  Duration min        : {min(durations):.3f}s",
                f"  Duration max        : {max(durations):.3f}s",
            ]

        if self.records:
            lines += [
                "",
                f"  {'ID':>6} | {'Client':<22} | {'Duration':>10} | {'Bytes':>9} | Messages",
                "  " + "-" * 70,
            ]
            for rec in self.records:
                lines.append(rec.row())

        lines.append("=" * 75)
        return "\n".join(lines)


# ─── TCP ─────────────────────────────────────────────────────────────────────

async def handle_tcp_client(reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter,
                             stats: ServerStats):
    addr = writer.get_extra_info("peername")
    addr_str = f"{addr[0]}:{addr[1]}" if addr else "unknown"
    rec = await stats.new_connection(addr_str)
    print(f"[{rec.conn_id:>6}] TCP connect    | {addr_str}")

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
    server = await asyncio.start_server(
        lambda r, w: handle_tcp_client(r, w, stats),
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
        self._loop = None

    def connection_made(self, transport):
        self._loop = asyncio.get_event_loop()
        self.transport = transport

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
    loop = asyncio.get_event_loop()
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

    print(f"Starting traffic receiver")
    print(f"  Protocol : {args.protocol.upper()}")
    print(f"  Bind     : {args.host}:{args.port}")
    print("-" * 55)

    loop = asyncio.get_event_loop()

    def shutdown():
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
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        asyncio.run(run_server(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
