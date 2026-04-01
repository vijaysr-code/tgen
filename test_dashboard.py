#!/usr/bin/env python3
"""
Test script to demonstrate dashboard with live traffic
"""

import asyncio
import json
import time
import subprocess
import sys

try:
    import aiohttp
except ImportError:
    print("Error: aiohttp is required. Install with: pip install aiohttp")
    sys.exit(1)


async def send_server_metrics(session, port=8081):
    """Send sample server metrics to dashboard"""
    url = f"http://localhost:{port}/metrics/server"
    
    for i in range(10):
        metrics = {
            "timestamp": time.time(),
            "total_connections": i * 100,
            "active_connections": 10 + i,
            "total_bytes_received": i * 1024 * 100,
            "total_bytes_sent": i * 1024 * 50,
            "connections_per_sec": 100.0 + i * 10,
            "protocol": "tcp",
            "port": 9000,
            "processes": 2
        }
        
        try:
            async with session.post(url, json=metrics) as resp:
                if resp.status == 200:
                    print(f"✓ Sent server metrics (iteration {i+1})")
                else:
                    print(f"✗ Failed to send server metrics: {resp.status}")
        except Exception as e:
            print(f"✗ Error sending server metrics: {e}")
        
        await asyncio.sleep(1)


async def send_client_metrics(session, port=8081):
    """Send sample client metrics to dashboard"""
    url = f"http://localhost:{port}/metrics/client"
    
    for i in range(10):
        metrics = {
            "timestamp": time.time(),
            "total_connections": i * 100,
            "successful": i * 95,
            "failed": i * 5,
            "connections_per_sec": 95.0 + i * 10,
            "avg_latency_ms": 1.5 + i * 0.1,
            "min_latency_ms": 0.5,
            "max_latency_ms": 5.0 + i * 0.5,
            "total_packets_sent": i * 100,
            "protocol": "tcp",
            "processes": 2
        }
        
        try:
            async with session.post(url, json=metrics) as resp:
                if resp.status == 200:
                    print(f"✓ Sent client metrics (iteration {i+1})")
                else:
                    print(f"✗ Failed to send client metrics: {resp.status}")
        except Exception as e:
            print(f"✗ Error sending client metrics: {e}")
        
        await asyncio.sleep(1)


async def test_dashboard():
    """Test dashboard with simulated metrics"""
    print("=" * 70)
    print("  DASHBOARD TEST")
    print("=" * 70)
    print()
    print("This test will:")
    print("  1. Send simulated server metrics to dashboard")
    print("  2. Send simulated client metrics to dashboard")
    print("  3. You can view real-time updates at http://localhost:8080")
    print()
    print("Starting in 3 seconds...")
    await asyncio.sleep(3)
    
    async with aiohttp.ClientSession() as session:
        # Send metrics concurrently
        await asyncio.gather(
            send_server_metrics(session),
            send_client_metrics(session)
        )
    
    print()
    print("=" * 70)
    print("  TEST COMPLETE")
    print("=" * 70)
    print()
    print("Dashboard is still running at http://localhost:8080")
    print("You can now run actual traffic tests:")
    print()
    print("  # Terminal 1: Start server")
    print("  python3 server.py --port 9000")
    print()
    print("  # Terminal 2: Run client")
    print("  python3 client.py --port 9000 --cps 100 --total 1000")
    print()


async def run_actual_traffic_test():
    """Run actual traffic test (TCP and UDP)"""
    print("=" * 70)
    print("  ACTUAL TRAFFIC TEST")
    print("=" * 70)
    print()
    
    # Test TCP
    print("Testing TCP traffic...")
    print("  Server: localhost:9000")
    print("  Client: 100 connections")
    print()
    
    proc = subprocess.Popen(
        ['python3', 'client.py', '--port', '9000', '--cps', '50', '--total', '100'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    stdout, stderr = proc.communicate()
    print(stdout.decode())
    
    if proc.returncode == 0:
        print("✓ TCP test completed successfully")
    else:
        print(f"✗ TCP test failed with code {proc.returncode}")
        if stderr:
            print(stderr.decode())
    
    print()
    
    # Test UDP
    print("Testing UDP traffic...")
    print("  Server: localhost:9001")
    print("  Client: 50 connections")
    print()
    
    proc = subprocess.Popen(
        ['python3', 'client.py', '--port', '9001', '--protocol', 'udp', '--cps', '25', '--total', '50'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    stdout, stderr = proc.communicate()
    print(stdout.decode())
    
    if proc.returncode == 0:
        print("✓ UDP test completed successfully")
    else:
        print(f"✗ UDP test failed with code {proc.returncode}")
        if stderr:
            print(stderr.decode())
    
    print()
    print("=" * 70)
    print("  TRAFFIC TEST COMPLETE")
    print("=" * 70)


async def main():
    """Main test function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test dashboard with traffic")
    parser.add_argument('--mode', choices=['simulated', 'actual', 'both'], default='both',
                        help='Test mode: simulated metrics, actual traffic, or both')
    args = parser.parse_args()
    
    if args.mode in ['simulated', 'both']:
        await test_dashboard()
    
    if args.mode in ['actual', 'both']:
        if args.mode == 'both':
            print("\nWaiting 5 seconds before running actual traffic...")
            await asyncio.sleep(5)
        await run_actual_traffic_test()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTest interrupted")

# Made with Bob
