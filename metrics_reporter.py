#!/usr/bin/env python3
"""
Metrics Reporter for Dashboard Integration
Sends metrics from server/client to the dashboard
"""

import asyncio
import json
import time
from typing import Optional
from dataclasses import dataclass, asdict

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


@dataclass
class MetricsConfig:
    """Configuration for metrics reporting"""
    dashboard_url: str = "http://localhost:8081"
    report_interval: float = 1.0  # seconds
    enabled: bool = True


class MetricsReporter:
    """Reports metrics to the dashboard"""
    
    def __init__(self, config: MetricsConfig, metric_type: str):
        """
        Args:
            config: Metrics configuration
            metric_type: Either 'server' or 'client'
        """
        self.config = config
        self.metric_type = metric_type
        self.session: Optional[aiohttp.ClientSession] = None
        self.running = False
        self.task: Optional[asyncio.Task] = None
        
        if not AIOHTTP_AVAILABLE:
            print("Warning: aiohttp not available, metrics reporting disabled")
            self.config.enabled = False
    
    async def start(self):
        """Start the metrics reporter"""
        if not self.config.enabled or not AIOHTTP_AVAILABLE:
            return
        
        self.session = aiohttp.ClientSession()
        self.running = True
        print(f"Metrics reporting enabled: {self.config.dashboard_url}/metrics/{self.metric_type}")
    
    async def stop(self):
        """Stop the metrics reporter"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        if self.session:
            await self.session.close()
    
    async def report(self, metrics: dict):
        """Report metrics to dashboard"""
        if not self.config.enabled or not self.session:
            return
        
        try:
            url = f"{self.config.dashboard_url}/metrics/{self.metric_type}"
            async with self.session.post(url, json=metrics, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                if resp.status != 200:
                    print(f"DEBUG: Metrics report failed: HTTP {resp.status} for {self.metric_type}")
        except Exception as e:
            print(f"DEBUG: Metrics report exception for {self.metric_type}: {e}")


class ServerMetricsTracker:
    """Tracks server metrics for reporting"""
    
    def __init__(self, protocol: str, port: int, processes: int = 1):
        self.protocol = protocol
        self.port = port
        self.processes = processes
        self.total_connections = 0
        self.active_connections = 0
        self.total_bytes_received = 0
        self.total_bytes_sent = 0
        self.start_time = time.monotonic()
        self.last_report_time = self.start_time
        self.last_total_connections = 0
    
    def record_connection(self):
        """Record a new connection"""
        self.total_connections += 1
        self.active_connections += 1
    
    def record_disconnection(self):
        """Record a disconnection"""
        self.active_connections = max(0, self.active_connections - 1)
    
    def record_bytes(self, received: int, sent: int):
        """Record bytes transferred"""
        self.total_bytes_received += received
        self.total_bytes_sent += sent
    
    def get_metrics(self) -> dict:
        """Get current metrics"""
        now = time.monotonic()
        elapsed = now - self.last_report_time
        
        # Calculate connections per second
        new_connections = self.total_connections - self.last_total_connections
        cps = new_connections / elapsed if elapsed > 0 else 0
        
        # Update for next calculation
        self.last_report_time = now
        self.last_total_connections = self.total_connections
        
        return {
            'timestamp': now,
            'total_connections': self.total_connections,
            'active_connections': self.active_connections,
            'total_bytes_received': self.total_bytes_received,
            'total_bytes_sent': self.total_bytes_sent,
            'connections_per_sec': cps,
            'protocol': self.protocol,
            'port': self.port,
            'processes': self.processes
        }


class ClientMetricsTracker:
    """Tracks client metrics for reporting"""
    
    def __init__(self, protocol: str, processes: int = 1):
        self.protocol = protocol
        self.processes = processes
        self.total_connections = 0
        self.successful = 0
        self.failed = 0
        self.total_packets_sent = 0
        self.latencies = []
        self.start_time = time.monotonic()
        self.last_report_time = self.start_time
        self.last_total_connections = 0
    
    def record_connection(self, success: bool, latency_ms: float = 0, packets: int = 1):
        """Record a connection attempt"""
        self.total_connections += 1
        if success:
            self.successful += 1
            if latency_ms > 0:
                self.latencies.append(latency_ms)
        else:
            self.failed += 1
        self.total_packets_sent += packets
    
    def get_metrics(self) -> dict:
        """Get current metrics"""
        now = time.monotonic()
        elapsed = now - self.last_report_time
        
        # Calculate connections per second
        new_connections = self.total_connections - self.last_total_connections
        cps = new_connections / elapsed if elapsed > 0 else 0
        
        # Calculate latency stats
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        min_latency = min(self.latencies) if self.latencies else 0
        max_latency = max(self.latencies) if self.latencies else 0
        
        # Update for next calculation
        self.last_report_time = now
        self.last_total_connections = self.total_connections
        
        return {
            'timestamp': now,
            'total_connections': self.total_connections,
            'successful': self.successful,
            'failed': self.failed,
            'connections_per_sec': cps,
            'avg_latency_ms': avg_latency,
            'min_latency_ms': min_latency,
            'max_latency_ms': max_latency,
            'total_packets_sent': self.total_packets_sent,
            'protocol': self.protocol,
            'processes': self.processes
        }


async def start_metrics_reporting(reporter: MetricsReporter, tracker, interval: float = 1.0):
    """
    Background task to periodically report metrics
    
    Args:
        reporter: MetricsReporter instance
        tracker: ServerMetricsTracker or ClientMetricsTracker instance
        interval: Reporting interval in seconds
    """
    await reporter.start()
    
    try:
        while reporter.running:
            metrics = tracker.get_metrics()
            await reporter.report(metrics)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        await reporter.stop()

# Made with Bob
