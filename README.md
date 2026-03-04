# Traffic Generator

A Python CLI tool for generating and receiving TCP/UDP traffic with configurable rate and connection duration.

## Features

- **TCP/UDP Traffic Generation**: Configurable connection rate, duration, and payload
- **Per-Connection Statistics**: Detailed metrics for every connection
- **TCP Statistics (Linux)**: Retransmits, RTT, packet loss, congestion window
- **File Transfer Mode**: SHA-256 verified file transfers over TCP
- **Keepalive Support**: Automatic TCP keepalive configuration
- **High Performance**: Supports thousands of connections per second

## Files

| File | Description |
|---|---|
| `client.py` | Traffic generator — sends connections at a configurable rate |
| `server.py` | Traffic receiver — collects per-connection statistics |
| `TCP_STATISTICS.md` | Detailed documentation on TCP statistics collection |
| `test_tcp_stats.sh` | Test script for verifying TCP statistics functionality |

---

## Client Usage

```
python client.py --port PORT [options]
```

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Target host |
| `--port` | *(required)* | Target port |
| `--protocol` | `tcp` | Protocol: `tcp` or `udp` |
| `--cps` | `1.0` | Connections per second |
| `--total` | `0` | Total connections (0 = infinite) |
| `--duration` | `0.0` | Seconds to hold each connection open (0 = short-lived) |
| `--payload` | `PING` | Payload string to send |
| `--payload-size` | — | Random payload size in bytes (overrides `--payload`) |
| `--pps` | `0.0` | Packets per second to send within each connection (requires `--duration > 0`) |
| `-F`, `--file` | — | File to send over each TCP connection (max 128 MiB); server verifies SHA-256 checksum per connection |
| `--output` | — | Optional file path to log the output results |

> **Note**: TCP keepalive is enabled by default for all TCP connections with a 10s idle time and 10s interval.

> **Note**: `-F/--file` is TCP-only. When specified, `--duration`, `--pps`, and `--payload` are ignored. The client sends a fixed 86-byte header (`TGEN_FILE:<sha256>:<size>`) followed by the raw file bytes. The server verifies the SHA-256 checksum and responds `OK` or `FAIL:<reason>` per connection.

### Examples

```bash
# 10 short-lived TCP connections at 5/sec
python client.py --port 9000 --cps 5 --total 10

# Long-lived UDP connections (2s each) at 2/sec, infinite
python client.py --port 9001 --protocol udp --cps 2 --duration 2

# Constant traffic of 100 packets/s per connection for 5 seconds
python client.py --port 9000 --cps 2 --duration 5 --pps 100

# 1KB random payload, 20 connections at 10/sec
python client.py --port 9000 --cps 10 --total 20 --payload-size 1024

# Send a file over 5 TCP connections at 2/sec, server verifies checksum each time
python client.py --port 9000 --cps 2 --total 5 -F /path/to/file.bin
```

---

## Server Usage

```
python server.py --port PORT [options]
```

| Flag | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Bind host |
| `--port` | *(required)* | Bind port |
| `--protocol` | `tcp` | Protocol: `tcp` or `udp` |
| `--output` | — | Optional file path to log the output results |

> **Note**: TCP keepalive is automatically enabled for accepted TCP client connections.

Press **Ctrl+C** to stop the server and print the full statistics summary.

### Examples

```bash
# TCP server on port 9000
python server.py --port 9000

# UDP server on port 9001
python server.py --port 9001 --protocol udp
```

---

## Quick Test

**Terminal 1 — Start server:**
```bash
python server.py --port 9000 --protocol tcp
```

**Terminal 2 — Run client:**
```bash
python client.py --port 9000 --cps 5 --total 20 --duration 2
```

Press Ctrl+C on the server to see the full per-connection statistics table.

---

## TCP Statistics (Linux Only)

The traffic generator collects detailed TCP socket statistics on Linux systems, including:

- **Retransmits**: Number of TCP segment retransmissions
- **RTT**: Round-trip time in milliseconds
- **Lost Packets**: Number of packets marked as lost
- **Congestion Window**: TCP send congestion window size

These statistics are displayed in both real-time connection logs and the final summary.

**Example output:**
```
[     1] TCP disconnect | 127.0.0.1:54321 | dur=2.345s | 1024B | retx=0 rtt=0.5ms lost=0

TCP Statistics Summary:
  Total retransmits : 5
  Total lost packets: 0
  RTT avg           : 0.85ms
  RTT min           : 0.45ms
  RTT max           : 1.50ms
```

For detailed information, see [`TCP_STATISTICS.md`](TCP_STATISTICS.md).

**Test TCP statistics:**
```bash
./test_tcp_stats.sh
```
