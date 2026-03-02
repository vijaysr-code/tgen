# Traffic Generator

A Python CLI tool for generating and receiving TCP/UDP traffic with configurable rate and connection duration.

## Files

| File | Description |
|---|---|
| `client.py` | Traffic generator — sends connections at a configurable rate |
| `server.py` | Traffic receiver — collects per-connection statistics |

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
