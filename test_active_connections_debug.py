#!/usr/bin/env python3
"""
Debug version of test script with verbose output from server/client
"""

import asyncio
import json
import subprocess
import sys
import time
from typing import Optional

try:
    import aiohttp
except ImportError:
    print("Error: aiohttp is required. Install with: pip install aiohttp")
    sys.exit(1)


class TestRunner:
    """Manages test execution and verification"""
    
    def __init__(self):
        self.dashboard_proc: Optional[subprocess.Popen] = None
        self.server_proc: Optional[subprocess.Popen] = None
        self.client_proc: Optional[subprocess.Popen] = None
        self.dashboard_url = "http://localhost:8080"
        self.metrics_url = "http://localhost:8081"
        self.server_port = 9000
        
    async def start_dashboard(self):
        """Start the dashboard server"""
        print("Starting dashboard...")
        self.dashboard_proc = subprocess.Popen(
            ["python3", "dashboard.py", "--port", "8080", "--metrics-port", "8081"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Wait for dashboard to be ready
        await asyncio.sleep(2)
        
        # Verify dashboard is running
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.dashboard_url}/api/metrics", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        print("✓ Dashboard is running")
                        return True
        except Exception as e:
            print(f"✗ Dashboard failed to start: {e}")
            return False
        
        return False
    
    async def start_server(self):
        """Start the server with dashboard reporting"""
        print(f"Starting server on port {self.server_port}...")
        self.server_proc = subprocess.Popen(
            [
                "python3", "server.py",
                "--port", str(self.server_port),
                "--protocol", "tcp",
                "--dashboard", self.metrics_url
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Wait for server to be ready and print output
        await asyncio.sleep(2)
        
        # Read some output
        if self.server_proc.stdout:
            try:
                line = self.server_proc.stdout.readline()
                if line:
                    print(f"  Server: {line.strip()}")
            except:
                pass
        
        print("✓ Server started")
        return True
    
    async def start_client(self, duration: int = 10):
        """Start the client with long-lived connections"""
        print(f"Starting client with {duration}s connection duration...")
        self.client_proc = subprocess.Popen(
            [
                "python3", "client.py",
                "--host", "localhost",
                "--port", str(self.server_port),
                "--protocol", "tcp",
                "--cps", "5",
                "--total", "50",
                "--duration", str(duration),
                "--dashboard", self.metrics_url
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Wait for client to start and print output
        await asyncio.sleep(3)
        
        # Read some output
        if self.client_proc.stdout:
            try:
                for _ in range(5):
                    line = self.client_proc.stdout.readline()
                    if line:
                        print(f"  Client: {line.strip()}")
            except:
                pass
        
        print("✓ Client started")
        return True
    
    async def check_active_connections(self) -> dict:
        """Query dashboard API and check for active connections"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.dashboard_url}/api/metrics",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data
                    else:
                        print(f"✗ Failed to get metrics: HTTP {resp.status}")
                        return {}
        except Exception as e:
            print(f"✗ Error querying dashboard: {e}")
            return {}
    
    def print_process_output(self, proc, name, lines=10):
        """Print recent output from a process"""
        if proc and proc.stdout:
            print(f"\n{name} output:")
            try:
                for _ in range(lines):
                    line = proc.stdout.readline()
                    if line:
                        print(f"  {line.strip()}")
                    else:
                        break
            except:
                pass
    
    def cleanup(self):
        """Stop all processes"""
        print("\nCleaning up...")
        
        # Print final output before cleanup
        self.print_process_output(self.server_proc, "Server", 20)
        self.print_process_output(self.client_proc, "Client", 20)
        
        if self.client_proc:
            self.client_proc.terminate()
            try:
                self.client_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.client_proc.kill()
            print("✓ Client stopped")
        
        if self.server_proc:
            self.server_proc.terminate()
            try:
                self.server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_proc.kill()
            print("✓ Server stopped")
        
        if self.dashboard_proc:
            self.dashboard_proc.terminate()
            try:
                self.dashboard_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.dashboard_proc.kill()
            print("✓ Dashboard stopped")
    
    async def run_test(self):
        """Run the complete test"""
        print("=" * 70)
        print("Testing Dashboard Active Connection Display (DEBUG)")
        print("=" * 70)
        print()
        
        try:
            # Start dashboard
            if not await self.start_dashboard():
                print("\n✗ TEST FAILED: Dashboard did not start")
                return False
            
            # Start server
            if not await self.start_server():
                print("\n✗ TEST FAILED: Server did not start")
                return False
            
            # Start client
            if not await self.start_client(duration=10):
                print("\n✗ TEST FAILED: Client did not start")
                return False
            
            print("\n" + "=" * 70)
            print("Monitoring Active Connections")
            print("=" * 70)
            
            # Monitor for shorter time with more frequent checks
            max_active_seen = 0
            checks_with_active = 0
            total_checks = 0
            
            for i in range(6):  # 6 checks over ~12 seconds
                await asyncio.sleep(2)
                total_checks += 1
                
                metrics = await self.check_active_connections()
                
                if metrics and 'server' in metrics and metrics['server']:
                    server_data = metrics['server']
                    active = server_data.get('active_connections', 0)
                    total = server_data.get('total_connections', 0)
                    cps = server_data.get('connections_per_sec', 0)
                    
                    print(f"\nCheck {i+1}:")
                    print(f"  Total Connections: {total}")
                    print(f"  Active Connections: {active}")
                    print(f"  Connections/sec: {cps:.2f}")
                    
                    if active > 0:
                        checks_with_active += 1
                        max_active_seen = max(max_active_seen, active)
                        print(f"  ✓ Active connections detected!")
                    else:
                        print(f"  ⚠ No active connections yet")
                else:
                    print(f"\nCheck {i+1}: ⚠ No server metrics available yet")
                
                if metrics and 'client' in metrics and metrics['client']:
                    client_data = metrics['client']
                    client_total = client_data.get('total_connections', 0)
                    client_success = client_data.get('successful', 0)
                    print(f"  Client: {client_success}/{client_total} successful")
            
            print("\n" + "=" * 70)
            print("Test Results")
            print("=" * 70)
            
            success = max_active_seen > 0
            
            if success:
                print(f"✓ Active connections were displayed (max: {max_active_seen})")
                print(f"✓ Active connections shown in {checks_with_active}/{total_checks} checks")
            else:
                print("✗ No active connections were ever displayed")
            
            return success
            
        except Exception as e:
            print(f"\n✗ TEST FAILED with exception: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.cleanup()


async def main():
    """Main entry point"""
    runner = TestRunner()
    success = await runner.run_test()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    asyncio.run(main())

# Made with Bob
