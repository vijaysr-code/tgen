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
| `--rate` | `1.0` | Connections per second |
| `--total` | `0` | Total connections (0 = infinite) |
| `--duration` | `0.0` | Seconds to hold each connection open (0 = short-lived) |
| `--payload` | `PING` | Payload string to send |
| `--payload-size` | — | Random payload size in bytes (overrides `--payload`) |

### Examples

```bash
# 10 short-lived TCP connections at 5/sec
python client.py --port 9000 --rate 5 --total 10

# Long-lived UDP connections (2s each) at 2/sec, infinite
python client.py --port 9001 --protocol udp --rate 2 --duration 2

# 1KB random payload, 20 connections at 10/sec
python client.py --port 9000 --rate 10 --total 20 --payload-size 1024
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
python client.py --port 9000 --rate 5 --total 20 --duration 2
```

Press Ctrl+C on the server to see the full per-connection statistics table.
