#!/usr/bin/env python3
"""
Real-time Dashboard for Traffic Generator
Displays live metrics from server and client programs
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from collections import deque
import argparse

try:
    from aiohttp import web
    import aiohttp
except ImportError:
    print("Error: aiohttp is required. Install with: pip install aiohttp")
    exit(1)


@dataclass
class ServerMetrics:
    """Metrics from server"""
    timestamp: float
    total_connections: int
    active_connections: int
    total_bytes_received: int
    total_bytes_sent: int
    connections_per_sec: float
    protocol: str
    port: int
    processes: int = 1


@dataclass
class ClientMetrics:
    """Metrics from client"""
    timestamp: float
    total_connections: int
    successful: int
    failed: int
    connections_per_sec: float
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    total_packets_sent: int
    protocol: str
    processes: int = 1


class MetricsCollector:
    """Collects and stores metrics from server and client"""
    
    def __init__(self, history_size: int = 100):
        self.server_metrics: deque = deque(maxlen=history_size)
        self.client_metrics: deque = deque(maxlen=history_size)
        self.websockets: List[web.WebSocketResponse] = []
        self.lock = asyncio.Lock()
    
    async def add_server_metrics(self, metrics: ServerMetrics):
        """Add server metrics and broadcast to connected clients"""
        async with self.lock:
            self.server_metrics.append(metrics)
            await self._broadcast({
                'type': 'server_metrics',
                'data': asdict(metrics)
            })
    
    async def add_client_metrics(self, metrics: ClientMetrics):
        """Add client metrics and broadcast to connected clients"""
        async with self.lock:
            self.client_metrics.append(metrics)
            await self._broadcast({
                'type': 'client_metrics',
                'data': asdict(metrics)
            })
    
    async def _broadcast(self, message: dict):
        """Broadcast message to all connected WebSocket clients"""
        if not self.websockets:
            return
        
        message_str = json.dumps(message)
        dead_sockets = []
        
        for ws in self.websockets:
            try:
                await ws.send_str(message_str)
            except Exception:
                dead_sockets.append(ws)
        
        # Remove dead connections
        for ws in dead_sockets:
            self.websockets.remove(ws)
    
    def get_latest_metrics(self) -> dict:
        """Get latest metrics for initial page load"""
        return {
            'server': asdict(self.server_metrics[-1]) if self.server_metrics else None,
            'client': asdict(self.client_metrics[-1]) if self.client_metrics else None,
            'server_history': [asdict(m) for m in list(self.server_metrics)[-20:]],
            'client_history': [asdict(m) for m in list(self.client_metrics)[-20:]]
        }


class DashboardServer:
    """Web server for the dashboard"""
    
    def __init__(self, host: str = '0.0.0.0', port: int = 8080, 
                 metrics_port: int = 8081):
        self.host = host
        self.port = port
        self.metrics_port = metrics_port
        self.collector = MetricsCollector()
        self.app = web.Application()
        self.metrics_app = web.Application()
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup HTTP routes"""
        # Dashboard web interface
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_get('/ws', self.handle_websocket)
        self.app.router.add_get('/api/metrics', self.handle_api_metrics)
        
        # Metrics collection API (for server/client to report to)
        self.metrics_app.router.add_post('/metrics/server', self.handle_server_metrics)
        self.metrics_app.router.add_post('/metrics/client', self.handle_client_metrics)
    
    async def handle_index(self, request):
        """Serve the dashboard HTML page"""
        html = self._get_dashboard_html()
        return web.Response(text=html, content_type='text/html')
    
    async def handle_websocket(self, request):
        """Handle WebSocket connections for real-time updates"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.collector.websockets.append(ws)
        
        # Send initial data
        initial_data = self.collector.get_latest_metrics()
        await ws.send_str(json.dumps({
            'type': 'initial',
            'data': initial_data
        }))
        
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if msg.data == 'close':
                        await ws.close()
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    print(f'WebSocket error: {ws.exception()}')
        finally:
            if ws in self.collector.websockets:
                self.collector.websockets.remove(ws)
        
        return ws
    
    async def handle_api_metrics(self, request):
        """REST API endpoint for getting current metrics"""
        metrics = self.collector.get_latest_metrics()
        return web.json_response(metrics)
    
    async def handle_server_metrics(self, request):
        """Receive metrics from server"""
        try:
            data = await request.json()
            metrics = ServerMetrics(**data)
            await self.collector.add_server_metrics(metrics)
            return web.json_response({'status': 'ok'})
        except Exception as e:
            return web.json_response({'status': 'error', 'message': str(e)}, status=400)
    
    async def handle_client_metrics(self, request):
        """Receive metrics from client"""
        try:
            data = await request.json()
            metrics = ClientMetrics(**data)
            await self.collector.add_client_metrics(metrics)
            return web.json_response({'status': 'ok'})
        except Exception as e:
            return web.json_response({'status': 'error', 'message': str(e)}, status=400)
    
    def _get_dashboard_html(self) -> str:
        """Generate the dashboard HTML"""
        return """<!DOCTYPE html>
<html>
<head>
    <title>Traffic Generator Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            padding: 20px;
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
        }
        .header h1 { font-size: 2em; margin-bottom: 10px; }
        .header p { opacity: 0.9; }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .card {
            background: #1e293b;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        }
        .card h2 {
            font-size: 1.2em;
            margin-bottom: 15px;
            color: #60a5fa;
            border-bottom: 2px solid #334155;
            padding-bottom: 10px;
        }
        .metric {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid #334155;
        }
        .metric:last-child { border-bottom: none; }
        .metric-label {
            color: #94a3b8;
            font-size: 0.9em;
        }
        .metric-value {
            font-weight: bold;
            font-size: 1.1em;
            color: #10b981;
        }
        .metric-value.warning { color: #f59e0b; }
        .metric-value.error { color: #ef4444; }
        .status {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }
        .status.active {
            background: #10b981;
            color: white;
        }
        .status.inactive {
            background: #64748b;
            color: white;
        }
        .chart-container {
            height: 200px;
            margin-top: 15px;
            position: relative;
        }
        canvas {
            width: 100% !important;
            height: 100% !important;
        }
        .no-data {
            text-align: center;
            padding: 40px;
            color: #64748b;
            font-style: italic;
        }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Traffic Generator Dashboard</h1>
            <p>Real-time monitoring of server and client metrics</p>
        </div>

        <div class="grid">
            <!-- Server Metrics -->
            <div class="card">
                <h2>📊 Server Metrics</h2>
                <div id="server-status" class="no-data">Waiting for server data...</div>
                <div id="server-metrics" style="display:none;">
                    <div class="metric">
                        <span class="metric-label">Status</span>
                        <span class="metric-value"><span id="server-status-badge" class="status inactive">Inactive</span></span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Protocol</span>
                        <span class="metric-value" id="server-protocol">-</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Port</span>
                        <span class="metric-value" id="server-port">-</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Processes</span>
                        <span class="metric-value" id="server-processes">-</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Total Connections</span>
                        <span class="metric-value" id="server-total-conn">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Active Connections</span>
                        <span class="metric-value" id="server-active-conn">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Conn/sec</span>
                        <span class="metric-value" id="server-cps">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Bytes Received</span>
                        <span class="metric-value" id="server-bytes-rx">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Bytes Sent</span>
                        <span class="metric-value" id="server-bytes-tx">0</span>
                    </div>
                </div>
            </div>

            <!-- Client Metrics -->
            <div class="card">
                <h2>📈 Client Metrics</h2>
                <div id="client-status" class="no-data">Waiting for client data...</div>
                <div id="client-metrics" style="display:none;">
                    <div class="metric">
                        <span class="metric-label">Status</span>
                        <span class="metric-value"><span id="client-status-badge" class="status inactive">Inactive</span></span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Protocol</span>
                        <span class="metric-value" id="client-protocol">-</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Processes</span>
                        <span class="metric-value" id="client-processes">-</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Total Connections</span>
                        <span class="metric-value" id="client-total-conn">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Successful</span>
                        <span class="metric-value" id="client-success">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Failed</span>
                        <span class="metric-value error" id="client-failed">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Conn/sec</span>
                        <span class="metric-value" id="client-cps">0</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Avg Latency</span>
                        <span class="metric-value" id="client-latency-avg">0 ms</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Latency Range</span>
                        <span class="metric-value" id="client-latency-range">0 - 0 ms</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Packets Sent</span>
                        <span class="metric-value" id="client-packets">0</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Charts -->
        <div class="grid">
            <div class="card">
                <h2>📉 Connection Rate (conn/s)</h2>
                <div class="chart-container">
                    <canvas id="cps-chart"></canvas>
                </div>
            </div>
            <div class="card">
                <h2>⏱️ Latency (ms)</h2>
                <div class="chart-container">
                    <canvas id="latency-chart"></canvas>
                </div>
            </div>
        </div>
    </div>

    <script>
        // WebSocket connection
        const ws = new WebSocket(`ws://${window.location.host}/ws`);
        
        // Chart data
        const maxDataPoints = 20;
        const cpsData = {
            labels: [],
            datasets: [{
                label: 'Server',
                data: [],
                borderColor: '#60a5fa',
                backgroundColor: 'rgba(96, 165, 250, 0.1)',
                tension: 0.4
            }, {
                label: 'Client',
                data: [],
                borderColor: '#10b981',
                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                tension: 0.4
            }]
        };
        
        const latencyData = {
            labels: [],
            datasets: [{
                label: 'Avg Latency',
                data: [],
                borderColor: '#f59e0b',
                backgroundColor: 'rgba(245, 158, 11, 0.1)',
                tension: 0.4
            }]
        };
        
        // Initialize charts
        const cpsChart = new Chart(document.getElementById('cps-chart'), {
            type: 'line',
            data: cpsData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: '#e2e8f0' } }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: '#94a3b8' },
                        grid: { color: '#334155' }
                    },
                    x: {
                        ticks: { color: '#94a3b8' },
                        grid: { color: '#334155' }
                    }
                }
            }
        });
        
        const latencyChart = new Chart(document.getElementById('latency-chart'), {
            type: 'line',
            data: latencyData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { labels: { color: '#e2e8f0' } }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: '#94a3b8' },
                        grid: { color: '#334155' }
                    },
                    x: {
                        ticks: { color: '#94a3b8' },
                        grid: { color: '#334155' }
                    }
                }
            }
        });
        
        // Helper functions
        function formatBytes(bytes) {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
        }
        
        function formatNumber(num) {
            return num.toLocaleString();
        }
        
        function getTimeLabel() {
            const now = new Date();
            return now.toLocaleTimeString();
        }
        
        function updateServerMetrics(data) {
            document.getElementById('server-status').style.display = 'none';
            document.getElementById('server-metrics').style.display = 'block';
            
            const badge = document.getElementById('server-status-badge');
            badge.textContent = 'Active';
            badge.className = 'status active';
            
            document.getElementById('server-protocol').textContent = data.protocol.toUpperCase();
            document.getElementById('server-port').textContent = data.port;
            document.getElementById('server-processes').textContent = data.processes;
            document.getElementById('server-total-conn').textContent = formatNumber(data.total_connections);
            document.getElementById('server-active-conn').textContent = formatNumber(data.active_connections);
            document.getElementById('server-cps').textContent = data.connections_per_sec.toFixed(2);
            document.getElementById('server-bytes-rx').textContent = formatBytes(data.total_bytes_received);
            document.getElementById('server-bytes-tx').textContent = formatBytes(data.total_bytes_sent);
            
            // Update chart
            const label = getTimeLabel();
            if (cpsData.labels.length >= maxDataPoints) {
                cpsData.labels.shift();
                cpsData.datasets[0].data.shift();
            }
            cpsData.labels.push(label);
            cpsData.datasets[0].data.push(data.connections_per_sec);
            cpsChart.update();
        }
        
        function updateClientMetrics(data) {
            document.getElementById('client-status').style.display = 'none';
            document.getElementById('client-metrics').style.display = 'block';
            
            const badge = document.getElementById('client-status-badge');
            badge.textContent = 'Active';
            badge.className = 'status active';
            
            document.getElementById('client-protocol').textContent = data.protocol.toUpperCase();
            document.getElementById('client-processes').textContent = data.processes;
            document.getElementById('client-total-conn').textContent = formatNumber(data.total_connections);
            document.getElementById('client-success').textContent = formatNumber(data.successful);
            document.getElementById('client-failed').textContent = formatNumber(data.failed);
            document.getElementById('client-cps').textContent = data.connections_per_sec.toFixed(2);
            document.getElementById('client-latency-avg').textContent = data.avg_latency_ms.toFixed(2) + ' ms';
            document.getElementById('client-latency-range').textContent = 
                data.min_latency_ms.toFixed(2) + ' - ' + data.max_latency_ms.toFixed(2) + ' ms';
            document.getElementById('client-packets').textContent = formatNumber(data.total_packets_sent);
            
            // Update charts
            const label = getTimeLabel();
            if (cpsData.labels.length >= maxDataPoints) {
                cpsData.datasets[1].data.shift();
            }
            cpsData.datasets[1].data.push(data.connections_per_sec);
            
            if (latencyData.labels.length >= maxDataPoints) {
                latencyData.labels.shift();
                latencyData.datasets[0].data.shift();
            }
            latencyData.labels.push(label);
            latencyData.datasets[0].data.push(data.avg_latency_ms);
            
            cpsChart.update();
            latencyChart.update();
        }
        
        // WebSocket message handler
        ws.onmessage = function(event) {
            const message = JSON.parse(event.data);
            
            if (message.type === 'initial') {
                // Load initial data
                if (message.data.server) {
                    updateServerMetrics(message.data.server);
                }
                if (message.data.client) {
                    updateClientMetrics(message.data.client);
                }
            } else if (message.type === 'server_metrics') {
                updateServerMetrics(message.data);
            } else if (message.type === 'client_metrics') {
                updateClientMetrics(message.data);
            }
        };
        
        ws.onerror = function(error) {
            console.error('WebSocket error:', error);
        };
        
        ws.onclose = function() {
            console.log('WebSocket connection closed');
            setTimeout(() => {
                location.reload();
            }, 5000);
        };
    </script>
</body>
</html>"""
    
    async def start(self):
        """Start both web servers"""
        # Start dashboard web server
        runner1 = web.AppRunner(self.app)
        await runner1.setup()
        site1 = web.TCPSite(runner1, self.host, self.port)
        await site1.start()
        
        # Start metrics collection API server
        runner2 = web.AppRunner(self.metrics_app)
        await runner2.setup()
        site2 = web.TCPSite(runner2, self.host, self.metrics_port)
        await site2.start()
        
        print("=" * 70)
        print("  TRAFFIC GENERATOR DASHBOARD")
        print("=" * 70)
        print(f"  Dashboard URL    : http://{self.host}:{self.port}")
        print(f"  Metrics API      : http://{self.host}:{self.metrics_port}")
        print("=" * 70)
        print()
        print("Configure your server/client to report metrics to:")
        print(f"  Server metrics: POST http://localhost:{self.metrics_port}/metrics/server")
        print(f"  Client metrics: POST http://localhost:{self.metrics_port}/metrics/client")
        print()
        print("Press Ctrl+C to stop")
        print("=" * 70)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Real-time dashboard for traffic generator metrics"
    )
    parser.add_argument('--host', default='0.0.0.0',
                        help='Dashboard host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8080,
                        help='Dashboard port (default: 8080)')
    parser.add_argument('--metrics-port', type=int, default=8081,
                        help='Metrics API port (default: 8081)')
    return parser.parse_args()


async def main():
    args = parse_args()
    
    dashboard = DashboardServer(
        host=args.host,
        port=args.port,
        metrics_port=args.metrics_port
    )
    
    await dashboard.start()
    
    # Keep running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down dashboard...")


if __name__ == '__main__':
    asyncio.run(main())

# Made with Bob
