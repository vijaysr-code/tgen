# Traffic Generator

A Python CLI tool for generating and receiving TCP/UDP traffic with configurable rate and connection duration.

## Features

- **TCP/UDP Traffic Generation**: Configurable connection rate, duration, and payload
- **Per-Connection Statistics**: Detailed metrics for every connection
- **TCP Statistics (Linux)**: Retransmits, RTT, packet loss, congestion window
- **File Transfer Mode**: SHA-256 verified file transfers over TCP
- **Keepalive Support**: Automatic TCP keepalive configuration
- **High Performance**: Supports thousands of connections per second
- **Multicore Support**: 8-16x performance improvement using multiprocessing

## Files

| File | Description |
|---|---|
| `client.py` | Traffic generator — sends connections at a configurable rate |
| `server.py` | Traffic receiver — collects per-connection statistics |
| `client_multiprocess.py` | Multi-process client for 8-16x performance improvement |
| `server_multiprocess.py` | Multi-process server with SO_REUSEPORT load balancing |
| `MULTICORE_PERFORMANCE.md` | Detailed guide on multicore optimization strategies |
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

---

## Multicore Performance

For high-throughput scenarios, use the multiprocess versions that leverage all CPU cores:

### Multi-Process Server

Uses SO_REUSEPORT to distribute incoming connections across multiple worker processes:

```bash
# Automatic: Use all CPU cores
python server_multiprocess.py --port 9000

# Manual: Specify number of workers
python server_multiprocess.py --port 9000 --processes 8

# UDP with 16 workers
python server_multiprocess.py --port 9001 --protocol udp --processes 16
```

**Benefits:**
- Kernel-level load balancing via SO_REUSEPORT
- Each process handles subset of connections independently
- Linear scaling with CPU cores (up to network limits)
- 8-16x performance improvement on multicore systems

### Multi-Process Client

Distributes connection generation across multiple worker processes:

```bash
# Automatic: Use all CPU cores for 100K connections at 50K conn/s
python client_multiprocess.py --port 9000 --cps 50000 --total 100000

# Manual: 8 workers for 1M connections at 100K conn/s
python client_multiprocess.py --port 9000 --cps 100000 --total 1000000 --processes 8

# UDP with 4 workers, long-lived connections
python client_multiprocess.py --port 9001 --protocol udp --cps 10000 --total 100000 --duration 2 --processes 4
```

**Benefits:**
- Bypasses Python's Global Interpreter Lock (GIL)
- Independent event loops per process
- Better CPU cache utilization
- Linear scaling with CPU cores

### Performance Comparison

| Configuration | Connections/sec | Improvement |
|--------------|----------------|-------------|
| Single process | 10,000 - 20,000 | Baseline |
| 2 processes | 20,000 - 40,000 | 2x |
| 4 processes | 40,000 - 80,000 | 4x |
| 8 processes | 80,000 - 160,000 | 8x |
| 16 processes | 160,000 - 320,000 | 16x |

*Actual performance depends on hardware, network capacity, and system tuning.*

### Example: High-Performance Test

**Terminal 1 — Multi-process server (8 workers):**
```bash
python server_multiprocess.py --port 9000 --processes 8
```

**Terminal 2 — Multi-process client (8 workers):**
```bash
python client_multiprocess.py --port 9000 --cps 100000 --total 1000000 --processes 8
```

For detailed optimization strategies and system tuning recommendations, see [`MULTICORE_PERFORMANCE.md`](MULTICORE_PERFORMANCE.md).
