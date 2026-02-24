#!/usr/bin/env python3
"""
Traffic Generator Client
Generates TCP or UDP traffic with configurable rate and connection duration.
"""

import asyncio
import argparse
import os
import platform
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Optional


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
class ConnectionResult:
    conn_id: int
    success: bool
    latency_ms: float
    packets_sent: int = 0
    error: Optional[str] = None


@dataclass
class Stats:
    total: int = 0
    success: int = 0
    failed: int = 0
    total_packets: int = 0
    latencies: list = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)

    def record(self, result: ConnectionResult):
        self.total += 1
        if result.success:
            self.success += 1
            self.latencies.append(result.latency_ms)
            self.total_packets += result.packets_sent
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
        lines.append("=" * 55)
        return "\n".join(lines)


def make_payload(args) -> bytes:
    if args.payload_size > 0:
        return os.urandom(args.payload_size)
    if not args.payload:
        return b"PING"
    return args.payload.encode()


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


async def tcp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats, args=None):
    t0 = time.monotonic()
    packets_sent = 0
    pps = args.pps if args else 0
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=10
        )

        # Apply TCP keepalive always
        sock = writer.get_extra_info("socket")
        if sock is not None:
            apply_keepalive(sock)

        latency_ms = (time.monotonic() - t0) * 1000
        print(f"[{conn_id:>6}] TCP connected  | latency={latency_ms:.1f}ms ka=on")

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

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        result = ConnectionResult(conn_id=conn_id, success=True,
                                  latency_ms=latency_ms, packets_sent=packets_sent)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        print(f"[{conn_id:>6}] TCP FAILED     | {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, error=str(e))
    stats.record(result)


async def udp_connection(conn_id: int, host: str, port: int, payload: bytes,
                         duration: float, stats: Stats, args=None):
    t0 = time.monotonic()
    loop = asyncio.get_running_loop()  # get_event_loop() is deprecated in async context
    packets_sent = 0
    pps = args.pps if args else 0
    transport = None
    try:
        transport, protocol = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol,
            remote_addr=(host, port)
        )
        if pps > 0 and duration > 0:
            interval = 1.0 / pps
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline:
                transport.sendto(payload)
                packets_sent += 1
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(interval, remaining))
            latency_ms = (time.monotonic() - t0) * 1000
            print(f"[{conn_id:>6}] UDP sent       | {packets_sent} pkts | {len(payload)}B each")
        else:
            transport.sendto(payload)
            packets_sent = 1
            latency_ms = (time.monotonic() - t0) * 1000
            print(f"[{conn_id:>6}] UDP sent       | latency={latency_ms:.1f}ms | {len(payload)}B")
            if duration > 0:
                await asyncio.sleep(duration)

        result = ConnectionResult(conn_id=conn_id, success=True,
                                  latency_ms=latency_ms, packets_sent=packets_sent)
    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        print(f"[{conn_id:>6}] UDP FAILED     | {e}")
        result = ConnectionResult(conn_id=conn_id, success=False,
                                  latency_ms=latency_ms, error=str(e))
    finally:
        if transport is not None:
            transport.close()  # always close, even on exception
    stats.record(result)


async def run_client(args):
    stats = Stats()
    payload = make_payload(args)
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    conn_id = 0
    tasks = set()

    print("Starting traffic generator")
    print(f"  Target   : {args.protocol.upper()} {args.host}:{args.port}")
    print(f"  Rate     : {args.rate} conn/s  (interval={interval*1000:.1f}ms)")
    print(f"  Duration : {'long-lived (' + str(args.duration) + 's)' if args.duration > 0 else 'short-lived'}")
    print(f"  PPS      : {'%g pkt/s per connection' % args.pps if args.pps > 0 else 'one-shot (no PPS)'}")
    print(f"  Total    : {'infinite' if args.total == 0 else args.total}")
    print(f"  Payload  : {len(payload)} bytes")
    if args.protocol == "tcp":
        print(f"  Keepalive: ON (idle={_KA_IDLE}s, intvl={_KA_INTERVAL}s, cnt={_KA_COUNT})")
    print("-" * 55)

    try:
        while args.total == 0 or conn_id < args.total:
            conn_id += 1
            if args.protocol == "tcp":
                coro = tcp_connection(conn_id, args.host, args.port, payload,
                                      args.duration, stats, args)
            else:
                coro = udp_connection(conn_id, args.host, args.port, payload,
                                      args.duration, stats, args)

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
    parser.add_argument("--pps", type=float, default=0.0,
                        help="Packets per second to send within each connection "
                             "(requires --duration > 0; 0 = send once and hold)")
    parser.add_argument("--output", type=str, default=None,
                        help="Optional file path to log the output results")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.rate <= 0:
        print("Error: --rate must be > 0", file=sys.stderr)
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
