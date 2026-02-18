#!/usr/bin/env python3
"""
Traffic Generator Client
Generates TCP or UDP traffic with configurable rate and connection duration.
"""

import asyncio
import argparse
import os
import random
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ConnectionResult:
    conn_id: int
    success: bool
    latency_ms: float
    error: Optional[str] = None


@dataclass
class Stats:
    total: int = 0
    success: int = 0
    failed: int = 0
    latencies: list = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)

    def record(self, result: ConnectionResult):
        self.total += 1
        if result.success:
            self.success += 1
            self.latencies.append(result.latency_ms)
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
        ]
        if self.latencies:
            avg = sum(self.latencies) / len(self.latencies)
            lines += [
                f"  Latency avg       : {avg:.2f}ms",
                f"  Latency min       : {min(self.latencies):.2f}ms",
                f"  Latency max       : {max(self.latencies):.2f}ms",
            ]
        lines.append("=" * 55)
        return "\n".join(lines)


def make_payload(args) -> bytes:
    if args.payload_size:
        return os.urandom(args.payload_size)
    return args.payload.encode()


async def tcp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats):
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=10
        )
        latency_ms = (time.monotonic() - t0) * 1000
        print(f"[{conn_id:>6}] TCP connected  | latency={latency_ms:.1f}ms")

        writer.write(payload)
        await writer.drain()

        if duration > 0:
            await asyncio.sleep(duration)

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        result = ConnectionResult(conn_id=conn_id, success=True, latency_ms=latency_ms)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        print(f"[{conn_id:>6}] TCP FAILED     | {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, error=str(e))
    stats.record(result)


async def udp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats):
    t0 = time.monotonic()
    loop = asyncio.get_event_loop()
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        await loop.sock_sendto(sock, payload, (host, port))
        latency_ms = (time.monotonic() - t0) * 1000
        print(f"[{conn_id:>6}] UDP sent       | latency={latency_ms:.1f}ms | {len(payload)}B")

        if duration > 0:
            await asyncio.sleep(duration)

        sock.close()
        result = ConnectionResult(conn_id=conn_id, success=True, latency_ms=latency_ms)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        print(f"[{conn_id:>6}] UDP FAILED     | {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, error=str(e))
    stats.record(result)


async def run_client(args):
    stats = Stats()
    payload = make_payload(args)
    interval = 1.0 / args.rate
    conn_id = 0
    tasks = set()

    print(f"Starting traffic generator")
    print(f"  Target   : {args.protocol.upper()} {args.host}:{args.port}")
    print(f"  Rate     : {args.rate} conn/s  (interval={interval*1000:.1f}ms)")
    print(f"  Duration : {'long-lived (' + str(args.duration) + 's)' if args.duration > 0 else 'short-lived'}")
    print(f"  Total    : {'infinite' if args.total == 0 else args.total}")
    print(f"  Payload  : {len(payload)} bytes")
    print("-" * 55)

    try:
        while args.total == 0 or conn_id < args.total:
            conn_id += 1
            if args.protocol == "tcp":
                coro = tcp_connection(conn_id, args.host, args.port, payload,
                                      args.duration, stats)
            else:
                coro = udp_connection(conn_id, args.host, args.port, payload,
                                      args.duration, stats)

            task = asyncio.create_task(coro)
            tasks.add(task)
            task.add_done_callback(tasks.discard)

            await asyncio.sleep(interval)

        # Wait for all in-flight connections to finish
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        # Cancel remaining tasks
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        print(stats.summary())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Traffic Generator Client — sends TCP/UDP traffic at a configurable rate.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="Target host")
    parser.add_argument("--port", type=int, required=True, help="Target port")
    parser.add_argument("--protocol", choices=["tcp", "udp"], default="tcp",
                        help="Protocol to use")
    parser.add_argument("--rate", type=float, default=1.0,
                        help="Connections per second")
    parser.add_argument("--total", type=int, default=0,
                        help="Total connections to make (0 = infinite)")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="Seconds to hold each connection open (0 = short-lived)")
    parser.add_argument("--payload", default="PING",
                        help="Payload string to send")
    parser.add_argument("--payload-size", type=int, default=0,
                        help="Random payload size in bytes (overrides --payload)")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.rate <= 0:
        print("Error: --rate must be > 0", file=sys.stderr)
        sys.exit(1)

    try:
        asyncio.run(run_client(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
