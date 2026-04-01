# Real-Time Dashboard for Traffic Generator

A web-based dashboard that displays live metrics from the traffic generator server and client programs.

## Features

- **Real-time Updates**: WebSocket-based live updates with no page refresh
- **Server Metrics**: Connection counts, throughput, bytes transferred
- **Client Metrics**: Success/failure rates, latency statistics, connection rate
- **Interactive Charts**: Live graphs showing connection rates and latency over time
- **Multi-process Support**: Aggregates metrics from multiprocess deployments
- **Modern UI**: Clean, dark-themed interface with responsive design

## Architecture

```
┌─────────────────┐
│   Dashboard     │
│   Web Server    │◄─── Browser (http://localhost:8080)
│   (port 8080)   │
└────────┬────────┘
         │ WebSocket
         │
┌────────▼────────┐
│  Metrics API    │
│  (port 8081)    │
└────────┬────────┘
         │ HTTP POST
    ┌────┴────┐
    │         │
┌───▼──┐  ┌──▼───┐
│Server│  │Client│
└──────┘  └──────┘
```

## Installation

### Prerequisites

Install the required dependency:

```bash
pip install aiohttp
```

### Quick Start

1. **Start the Dashboard**:
   ```bash
   python3 dashboard.py
   ```
   
   The dashboard will be available at:
   - Web UI: http://localhost:8080
   - Metrics API: http://localhost:8081

2. **Run Server with Dashboard Integration**:
   ```bash
   python3 server_multiprocess.py --port 9000 --processes 2 --dashboard http://localhost:8081
   ```

3. **Run Client with Dashboard Integration**:
   ```bash
   python3 client_multiprocess.py --port 9000 --cps 1000 --total 10000 --processes 4 --dashboard http://localhost:8081
   ```

4. **Open Dashboard in Browser**:
   Navigate to http://localhost:8080 to see real-time metrics

## Usage

### Dashboard Server

```bash
python3 dashboard.py [OPTIONS]
```

**Options:**
- `--host HOST`: Dashboard host (default: 0.0.0.0)
- `--port PORT`: Dashboard web UI port (default: 8080)
- `--metrics-port PORT`: Metrics API port (default: 8081)

**Example:**
```bash
# Start dashboard on custom ports
python3 dashboard.py --port 9090 --metrics-port 9091
```

### Server Integration

Add the `--dashboard` flag to any server command:

```bash
# Single-process server
python3 server.py --port 9000 --dashboard http://localhost:8081

# Multi-process server
python3 server_multiprocess.py --port 9000 --processes 4 --dashboard http://localhost:8081
```

### Client Integration

Add the `--dashboard` flag to any client command:

```bash
# Single-process client
python3 client.py --port 9000 --cps 100 --total 1000 --dashboard http://localhost:8081

# Multi-process client
python3 client_multiprocess.py --port 9000 --cps 5000 --total 50000 --processes 8 --dashboard http://localhost:8081
```

## Dashboard Metrics

### Server Metrics

| Metric | Description |
|--------|-------------|
| Status | Active/Inactive indicator |
| Protocol | TCP or UDP |
| Port | Server listening port |
| Processes | Number of worker processes |
| Total Connections | Cumulative connection count |
| Active Connections | Currently open connections |
| Conn/sec | Connection rate (connections per second) |
| Bytes Received | Total bytes received from clients |
| Bytes Sent | Total bytes sent to clients |

### Client Metrics

| Metric | Description |
|--------|-------------|
| Status | Active/Inactive indicator |
| Protocol | TCP or UDP |
| Processes | Number of worker processes |
| Total Connections | Cumulative connection attempts |
| Successful | Successfully completed connections |
| Failed | Failed connection attempts |
| Conn/sec | Connection rate (connections per second) |
| Avg Latency | Average connection latency in milliseconds |
| Latency Range | Min-Max latency range |
| Packets Sent | Total packets transmitted |

### Charts

1. **Connection Rate Chart**: Shows server and client connection rates over time
2. **Latency Chart**: Displays average client latency over time

## API Reference

### Metrics Collection API

The dashboard exposes a REST API for receiving metrics:

#### POST /metrics/server

Submit server metrics:

```json
{
  "timestamp": 1234567890.123,
  "total_connections": 1000,
  "active_connections": 50,
  "total_bytes_received": 1048576,
  "total_bytes_sent": 524288,
  "connections_per_sec": 100.5,
  "protocol": "tcp",
  "port": 9000,
  "processes": 2
}
```

#### POST /metrics/client

Submit client metrics:

```json
{
  "timestamp": 1234567890.123,
  "total_connections": 1000,
  "successful": 995,
  "failed": 5,
  "connections_per_sec": 100.5,
  "avg_latency_ms": 1.23,
  "min_latency_ms": 0.5,
  "max_latency_ms": 5.0,
  "total_packets_sent": 1000,
  "protocol": "tcp",
  "processes": 4
}
```

#### GET /api/metrics

Retrieve current metrics (REST endpoint):

```bash
curl http://localhost:8080/api/metrics
```

Response:
```json
{
  "server": { /* latest server metrics */ },
  "client": { /* latest client metrics */ },
  "server_history": [ /* last 20 server metrics */ ],
  "client_history": [ /* last 20 client metrics */ ]
}
```

## Integration Guide

### Using metrics_reporter Module

For custom integration, use the `metrics_reporter` module:

```python
from metrics_reporter import (
    MetricsReporter, 
    MetricsConfig,
    ServerMetricsTracker,
    start_metrics_reporting
)

# Configure metrics
config = MetricsConfig(
    dashboard_url="http://localhost:8081",
    report_interval=1.0,
    enabled=True
)

# Create reporter and tracker
reporter = MetricsReporter(config, 'server')
tracker = ServerMetricsTracker(protocol='tcp', port=9000, processes=2)

# Start reporting in background
asyncio.create_task(start_metrics_reporting(reporter, tracker, interval=1.0))

# Record metrics
tracker.record_connection()
tracker.record_bytes(received=1024, sent=512)
tracker.record_disconnection()
```

## Examples

### Example 1: Basic Usage

```bash
# Terminal 1: Start dashboard
python3 dashboard.py

# Terminal 2: Start server
python3 server_multiprocess.py --port 9000 --processes 2 --dashboard http://localhost:8081

# Terminal 3: Start client
python3 client_multiprocess.py --port 9000 --cps 1000 --total 10000 --processes 4 --dashboard http://localhost:8081

# Browser: Open http://localhost:8080
```

### Example 2: High-Performance Test

```bash
# Dashboard
python3 dashboard.py

# Server with 8 processes
python3 server_multiprocess.py --port 9000 --processes 8 --dashboard http://localhost:8081

# Client with 16 processes, 50K conn/s
python3 client_multiprocess.py --port 9000 --cps 50000 --total 500000 --processes 16 --dashboard http://localhost:8081
```

### Example 3: Long-Running Test

```bash
# Dashboard
python3 dashboard.py

# Server
python3 server_multiprocess.py --port 9000 --processes 4 --dashboard http://localhost:8081

# Client with long-lived connections
python3 client_multiprocess.py --port 9000 --cps 100 --total 10000 --duration 30 --pps 10 --processes 4 --dashboard http://localhost:8081
```

## Troubleshooting

### Dashboard Not Showing Data

1. **Check if aiohttp is installed**:
   ```bash
   pip install aiohttp
   ```

2. **Verify dashboard is running**:
   ```bash
   curl http://localhost:8080/api/metrics
   ```

3. **Check server/client are using --dashboard flag**:
   ```bash
   python3 server.py --port 9000 --dashboard http://localhost:8081
   ```

4. **Check metrics API is accessible**:
   ```bash
   curl -X POST http://localhost:8081/metrics/server -H "Content-Type: application/json" -d '{"timestamp":1234567890,"total_connections":0,"active_connections":0,"total_bytes_received":0,"total_bytes_sent":0,"connections_per_sec":0,"protocol":"tcp","port":9000,"processes":1}'
   ```

### WebSocket Connection Issues

- Ensure no firewall is blocking ports 8080 and 8081
- Check browser console for WebSocket errors
- Try refreshing the browser page

### Performance Impact

The dashboard has minimal performance impact:
- Metrics are reported every 1 second by default
- HTTP POST requests are non-blocking
- Failed reports are silently ignored (won't crash your app)

## Advanced Configuration

### Custom Reporting Interval

Modify the reporting interval in your integration:

```python
config = MetricsConfig(
    dashboard_url="http://localhost:8081",
    report_interval=0.5,  # Report every 500ms
    enabled=True
)
```

### Disable Metrics Reporting

```python
config = MetricsConfig(
    enabled=False  # Disable metrics reporting
)
```

Or simply omit the `--dashboard` flag when running server/client.

### Remote Dashboard

Run dashboard on a separate machine:

```bash
# On monitoring machine (192.168.1.100)
python3 dashboard.py --host 0.0.0.0 --port 8080 --metrics-port 8081

# On server machine
python3 server.py --port 9000 --dashboard http://192.168.1.100:8081

# On client machine
python3 client.py --port 9000 --dashboard http://192.168.1.100:8081
```

## Security Considerations

- The dashboard has no authentication by default
- Use firewall rules to restrict access to ports 8080 and 8081
- For production use, consider adding authentication
- Run dashboard on a private network or use SSH tunneling

## Browser Compatibility

Tested and working on:
- Chrome/Chromium 90+
- Firefox 88+
- Safari 14+
- Edge 90+

Requires WebSocket support (all modern browsers).

## License

Same as the main traffic generator project.

## Support

For issues or questions:
1. Check this documentation
2. Review the example scripts
3. Check the main README.md
4. Open an issue on the project repository