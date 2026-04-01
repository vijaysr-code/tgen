#!/usr/bin/env python3
"""
Example: Running server and client with dashboard integration
This demonstrates how to use the real-time dashboard
"""

import asyncio
import subprocess
import time
import sys

def print_banner(text):
    """Print a formatted banner"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")

async def main():
    print_banner("TRAFFIC GENERATOR WITH DASHBOARD")
    
    print("This example will:")
    print("  1. Start the dashboard server")
    print("  2. Start a multiprocess server")
    print("  3. Start a multiprocess client")
    print("  4. Display real-time metrics in the dashboard")
    print()
    print("Prerequisites:")
    print("  - Install aiohttp: pip install aiohttp")
    print()
    
    input("Press Enter to continue...")
    
    processes = []
    
    try:
        # Start dashboard
        print_banner("Starting Dashboard")
        dashboard_proc = subprocess.Popen(
            ['python3', 'dashboard.py', '--port', '8080', '--metrics-port', '8081'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(dashboard_proc)
        print("Dashboard starting...")
        time.sleep(2)
        
        print("\n✓ Dashboard is running!")
        print("  Open in browser: http://localhost:8080")
        print()
        
        # Start server with dashboard integration
        print_banner("Starting Server")
        print("Starting multiprocess server on port 9000...")
        server_proc = subprocess.Popen(
            ['python3', 'server_multiprocess.py', 
             '--port', '9000', 
             '--processes', '2',
             '--dashboard', 'http://localhost:8081'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(server_proc)
        time.sleep(1)
        print("✓ Server is running")
        print()
        
        # Start client with dashboard integration
        print_banner("Starting Client")
        print("Starting multiprocess client...")
        print("  Target: localhost:9000")
        print("  Rate: 1000 conn/s")
        print("  Total: 10000 connections")
        print("  Processes: 4")
        print()
        
        client_proc = subprocess.Popen(
            ['python3', 'client_multiprocess.py',
             '--port', '9000',
             '--cps', '1000',
             '--total', '10000',
             '--processes', '4',
             '--dashboard', 'http://localhost:8081'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        processes.append(client_proc)
        
        print("✓ Client is running")
        print()
        print_banner("DASHBOARD ACTIVE")
        print("View real-time metrics at: http://localhost:8080")
        print()
        print("The dashboard shows:")
        print("  • Server metrics (connections, throughput, bytes)")
        print("  • Client metrics (success rate, latency, conn/s)")
        print("  • Real-time charts")
        print()
        print("Press Ctrl+C to stop all processes")
        print("=" * 70)
        
        # Wait for client to finish
        client_proc.wait()
        
        print("\n✓ Client completed")
        print("\nDashboard will continue running. Press Ctrl+C to stop.")
        
        # Keep dashboard and server running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\nStopping all processes...")
    finally:
        for proc in processes:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        print("✓ All processes stopped")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")

# Made with Bob
