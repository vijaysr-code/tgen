# Traffic Generator (tgen) - Architecture Documentation

## Overview

The `tgen` directory contains a Python-based **traffic generation and testing tool** for network performance testing. It consists of two main components that work together to generate and measure TCP/UDP traffic.

## Architecture Diagram

```
┌─────────────────┐                    ┌─────────────────┐
│   client.py     │                    │   server.py     │
│                 │                    │                 │
│ Traffic         │  TCP/UDP Traffic   │ Traffic         │
│ Generator       │ ──────────────────>│ Receiver        │
│                 │                    │                 │
│ - Rate Control  │                    │ - Connection    │
│ - PPS Mode      │                    │   Tracking      │
│ - Keepalive     │                    │ - Statistics    │
│ - Statistics    │                    │ - Per-conn Data │
└─────────────────┘                    └─────────────────┘
        │                                      │
        │                                      │
        v                                      v
┌─────────────────┐                    ┌─────────────────┐
│ Client Summary  │                    │ Server Summary  │
│                 │                    │                 │
│ - Total conns   │                    │ - Per-conn      │
│ - Success/Fail  │                    │   details       │
│ - Latency stats │                    │ - Duration      │
│ - Packets sent  │                    │ - Bytes/msgs    │
└─────────────────┘                    │ - PPS observed  │
                                       └─────────────────┘
```

## Component Breakdown

### 1. `client.py` - Traffic Generator

**Purpose**: Generates TCP or UDP connections at a configurable rate to test network capacity and behavior.

#### Key Classes

##### `Tee` (lines 18-28)
Utility class that duplicates output to both stdout and a file.
- Enables logging results while displaying them in real-time
- Used when `--output` flag is specified

##### `ConnectionResult` (lines 31-37)
Data class tracking individual connection outcomes:
- `conn_id`: Connection identifier
- `success`: Whether connection succeeded
- `latency_ms`: Connection establishment latency
- `packets_sent`: Number of packets transmitted
- `error`: Error message if failed

##### `Stats` (lines 40-80)
Aggregates statistics across all connections:
- Tracks success/failure counts, latencies, packet counts
- Calculates averages, min/max latencies
- Provides formatted summary output
- Records start time for rate calculation

#### Key Functions

##### `make_payload()` (lines 83-88)
Creates the payload to send:
- Random bytes if `--payload-size` specified
- Custom string from `--payload` flag
- Default: "PING"

##### `apply_keepalive()` (lines 97-116)
Configures TCP keepalive settings:
- Sets 10-second idle time before first probe
- 10-second interval between probes
- 5 unacked probes before dropping connection
- Platform-specific implementation (Linux/macOS)

##### `tcp_connection()` (lines 118-168)
Handles individual TCP connection lifecycle:
- Establishes connection with 10-second timeout
- Applies keepalive settings automatically
- Supports two modes:
  - **Single-shot**: Send payload once, optionally hold connection
  - **PPS mode**: Send packets at specified rate (`--pps`) for duration
- Records latency and packet statistics
- Graceful connection closure

##### `udp_connection()` (lines 171-213)
Handles UDP datagram transmission:
- Creates datagram endpoint
- Supports same two modes as TCP
- No connection establishment latency (connectionless)
- Automatic transport cleanup

##### `run_client()` (lines 216-264)
Main client orchestration:
- Creates connections at specified rate using asyncio
- Manages concurrent connection tasks
- Handles graceful shutdown on Ctrl+C
- Prints summary statistics on completion
- Waits for all in-flight connections to finish

#### Command-Line Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Target host |
| `--port` | *(required)* | Target port |
| `--protocol` | `tcp` | Protocol: `tcp` or `udp` |
| `--cps` | `1.0` | Connections per second |
| `--total` | `0` | Total connections (0 = infinite) |
| `--duration` | `0.0` | Seconds to hold each connection open |
| `--payload` | `PING` | Payload string to send |
| `--payload-size` | — | Random payload size in bytes |
| `--pps` | `0.0` | Packets per second within each connection |
| `--output` | — | Optional file path to log results |

#### Key Features

1. **CPS Control**: Connections created at precise intervals (e.g., `--cps 5` = 200ms intervals)
2. **Packet-Per-Second (PPS) Mode**: Send continuous packets within long-lived connections
3. **Flexible Payloads**: Custom strings or random bytes of specified size
4. **TCP Keepalive**: Always enabled with 10s idle/interval for connection health monitoring
5. **Async Architecture**: Uses `asyncio` for efficient concurrent connection handling
6. **Graceful Shutdown**: Properly cancels tasks and waits for completion

---

### 2. `server.py` - Traffic Receiver

**Purpose**: Receives and measures incoming TCP/UDP traffic, collecting detailed per-connection statistics.

#### Key Classes

##### `ConnectionRecord` (lines 31-60)
Tracks individual connection metrics:
- Connection ID, client address, timestamps
- Bytes/messages received
- Calculated properties:
  - `duration`: Connection lifetime in seconds
  - `pps_observed`: Packets per second received
- Formatted row output for statistics table

##### `ServerStats` (lines 63-134)
Manages all connection records:
- Thread-safe connection tracking with asyncio locks
- Auto-incrementing connection IDs
- Generates comprehensive summary with:
  - Total/completed/open connections
  - Observed connection rate
  - Duration statistics (avg/min/max)
  - PPS statistics for received traffic
  - Detailed per-connection table

##### `UDPServerProtocol` (lines 217-273)
UDP connection tracking protocol:
- **Connectionless tracking**: Treats each unique (host, port) as a "connection"
- **Inactivity timeout**: Closes "connection" after 5 seconds of no packets
- Uses timers to detect session expiration
- Tracks same metrics as TCP (bytes, messages, duration)
- Automatic timer cancellation and reset on new packets

#### Key Functions

##### `apply_keepalive()` (lines 145-163)
Same TCP keepalive configuration as client:
- Applied to all accepted TCP connections
- Platform-specific implementation

##### `handle_tcp_client()` (lines 165-198)
Handles individual TCP client:
- Applies keepalive on accepted socket
- Reads data in 64KB chunks until connection closes
- Tracks bytes and message count
- Records disconnect time and duration
- Graceful connection cleanup

##### `run_tcp_server()` (lines 201-212)
TCP server setup:
- Creates asyncio stream server
- Binds to specified host:port
- Runs indefinitely until shutdown
- Displays listening address

##### `run_udp_server()` (lines 276-286)
UDP server setup:
- Creates datagram endpoint with custom protocol
- Binds to specified host:port
- Runs indefinitely until shutdown

##### `run_server()` (lines 291-318)
Main server orchestration:
- Sets up signal handlers (SIGINT/SIGTERM) for graceful shutdown
- Prints summary statistics on shutdown
- Cancels all tasks cleanly
- Displays server configuration on startup

#### Command-Line Arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind host |
| `--port` | *(required)* | Bind port |
| `--protocol` | `tcp` | Protocol: `tcp` or `udp` |
| `--output` | — | Optional file path to log results |

---

## Usage Flow

```
┌──────┐                ┌────────┐                ┌────────┐
│ User │                │ Client │                │ Server │
└──┬───┘                └───┬────┘                └───┬────┘
   │                        │                         │
   │ Start server           │                         │
   │ (port 9000)            │                         │
   ├────────────────────────┼────────────────────────>│
   │                        │                         │
   │                        │      Listen for         │
   │                        │      connections        │
   │                        │<────────────────────────┤
   │                        │                         │
   │ Start client           │                         │
   │ (cps=5, total=20)      │                         │
   ├───────────────────────>│                         │
   │                        │                         │
   │                        │ Establish connection    │
   │                        ├────────────────────────>│
   │                        │                         │
   │                        │                         │ Record start
   │                        │                         ├──────────────┐
   │                        │                         │              │
   │                        │                         │<─────────────┘
   │                        │                         │
   │                        │ Send payload            │
   │                        ├────────────────────────>│
   │                        │                         │
   │                        │                         │ Track bytes/msgs
   │                        │                         ├──────────────┐
   │                        │                         │              │
   │                        │                         │<─────────────┘
   │                        │                         │
   │                        │ Hold connection         │
   │                        │ (if duration > 0)       │
   │                        ├────────────────────────>│
   │                        │                         │
   │                        │ Close connection        │
   │                        ├────────────────────────>│
   │                        │                         │
   │                        │                         │ Record disconnect
   │                        │                         ├──────────────┐
   │                        │                         │              │
   │                        │                         │<─────────────┘
   │                        │                         │
   │                        │ (Repeat at rate)        │
   │                        │                         │
   │                        │                         │
   │ Display client summary │                         │
   │<───────────────────────┤                         │
   │                        │                         │
   │ Ctrl+C to stop         │                         │
   ├────────────────────────┼────────────────────────>│
   │                        │                         │
   │                        │                         │ Display stats
   │ Display server summary │                         │
   │<────────────────────────────────────────────────┤
   │                        │                         │
```

## Key Design Patterns

### 1. Async/Await Pattern
Both client and server use `asyncio` for efficient concurrent I/O:
- Non-blocking connection handling
- Efficient task scheduling
- Proper resource cleanup with context managers

### 2. Dataclasses
Clean data structures with automatic `__init__` and type hints:
- `ConnectionResult`: Client-side connection tracking
- `ConnectionRecord`: Server-side connection tracking
- `Stats`: Aggregate statistics

### 3. Context Managers
Proper resource cleanup:
- `async with server:` for TCP server
- `try/finally` blocks for transport cleanup
- Automatic connection closure

### 4. Signal Handling
Graceful shutdown on SIGINT/SIGTERM:
- Prints summary statistics before exit
- Cancels all running tasks
- Prevents data loss

### 5. Platform Abstraction
OS-specific TCP keepalive configuration:
- Linux: `TCP_KEEPIDLE`, `TCP_KEEPINTVL`, `TCP_KEEPCNT`
- macOS: `TCP_KEEPALIVE` (equivalent to `TCP_KEEPIDLE`)

### 6. Observer Pattern
Statistics collection:
- `Stats.record()` observes connection results
- `ServerStats` observes connection lifecycle events
- Decoupled data collection from business logic

## Statistics Collected

### Client Side
- **Total connections**: Count of all connection attempts
- **Successful connections**: Count of successful connections
- **Failed connections**: Count of failed connections
- **Connection latency**: avg/min/max in milliseconds
- **Observed connection rate**: Actual connections per second
- **Total packets sent**: Aggregate packet count

### Server Side
- **Per-connection details**:
  - Connection ID
  - Client address (host:port)
  - Duration (seconds)
  - Bytes received
  - Messages received
  - PPS observed (packets per second)
- **Aggregate statistics**:
  - Total connections
  - Completed connections
  - Still open connections
  - Observed connection rate
  - Total bytes received
  - Total packets received
- **Duration statistics**: avg/min/max
- **PPS statistics**: avg/min/max for received traffic

## Use Cases

### 1. Load Testing
Generate sustained connection load to test server capacity:
```bash
python client.py --port 9000 --cps 100 --total 10000
```

### 2. Network Performance
Measure latency and throughput under various conditions:
```bash
python client.py --port 9000 --cps 10 --duration 5 --payload-size 1024
```

### 3. Connection Behavior
Test long-lived vs short-lived connection handling:
```bash
# Short-lived
python client.py --port 9000 --cps 5 --total 100

# Long-lived
python client.py --port 9000 --cps 2 --duration 30 --total 100
```

### 4. Rate Limiting
Verify rate limiting and connection throttling mechanisms:
```bash
python client.py --port 9000 --cps 1000 --total 5000
```

### 5. Keepalive Testing
Validate TCP keepalive behavior and connection health monitoring:
```bash
python client.py --port 9000 --cps 1 --duration 60 --total 10
```

### 6. Packet Rate Testing
Test sustained packet transmission within connections:
```bash
python client.py --port 9000 --cps 2 --duration 10 --pps 100
```

## Technical Details

### TCP Keepalive Configuration
- **Idle time**: 10 seconds before first probe
- **Interval**: 10 seconds between probes
- **Count**: 5 unacked probes before dropping connection
- **Total timeout**: ~60 seconds (10 + 5×10)

### UDP Session Tracking
- **Session identification**: Unique (host, port) tuple
- **Inactivity timeout**: 5 seconds
- **Timer management**: Automatic reset on new packets
- **Session expiration**: Logged with final statistics

### Async Task Management
- **Task tracking**: Set-based task collection
- **Callback cleanup**: `task.add_done_callback(tasks.discard)`
- **Graceful cancellation**: `task.cancel()` + `gather(..., return_exceptions=True)`
- **Timeout handling**: `asyncio.wait_for()` with 10-second timeout

### Error Handling
- **Connection failures**: Caught and logged with error message
- **Incomplete reads**: Handled gracefully (TCP)
- **Connection resets**: Caught and logged
- **Keyboard interrupts**: Graceful shutdown with statistics

## Performance Considerations

1. **Concurrent connections**: Limited by system file descriptor limits
2. **Memory usage**: Scales with number of concurrent connections
3. **CPU usage**: Minimal due to async I/O
4. **Network bandwidth**: Limited by payload size and rate
5. **Timer overhead**: UDP session tracking uses one timer per active session

## Future Enhancements

Potential improvements:
- TLS/SSL support
- HTTP/HTTPS protocol support
- Configurable keepalive parameters
- Real-time statistics dashboard
- Connection pooling
- Bandwidth throttling
- Latency injection
- Packet loss simulation
- Connection retry logic
- Prometheus metrics export