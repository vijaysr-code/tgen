# Multicore Performance Optimization Guide

## Overview

The current `client.py` and `server.py` use Python's `asyncio` which runs on a single thread. To leverage multicore systems for higher performance, we need to use multiprocessing or threading strategies.

## Current Architecture Limitations

### Single-Threaded Asyncio
- **Current:** Single event loop on one CPU core
- **GIL Impact:** Python's Global Interpreter Lock limits CPU-bound operations
- **Bottleneck:** Network I/O operations compete for single event loop time
- **Max Performance:** Limited by single core's capacity (~10K-50K conn/s depending on hardware)

## Multicore Optimization Strategies

### Strategy 1: Process-Based Parallelism (Recommended)

Use multiple processes, each with its own event loop, to bypass the GIL.

#### Implementation: Multi-Process Client

```python
#!/usr/bin/env python3
"""
Multi-process traffic generator client
Distributes load across multiple CPU cores
"""

import asyncio
import argparse
import multiprocessing as mp
import os
import sys
from dataclasses import dataclass
from typing import List

# Import existing client functions
from client import run_client, parse_args, Stats


@dataclass
class ProcessStats:
    """Statistics from a single worker process"""
    process_id: int
    total: int
    success: int
    failed: int
    total_packets: int
    latencies: List[float]
    elapsed: float


def worker_process(process_id: int, args, connections_per_process: int, 
                   result_queue: mp.Queue):
    """
    Worker process that runs its own event loop
    Each process handles a portion of the total connections
    """
    # Modify args for this worker
    worker_args = argparse.Namespace(**vars(args))
    worker_args.total = connections_per_process
    
    # Run the client in this process
    stats = Stats()
    try:
        asyncio.run(run_client(worker_args))
        # Collect stats and send back to main process
        result = ProcessStats(
            process_id=process_id,
            total=stats.total,
            success=stats.success,
            failed=stats.failed,
            total_packets=stats.total_packets,
            latencies=stats.latencies,
            elapsed=time.monotonic() - stats.start_time
        )
        result_queue.put(result)
    except Exception as e:
        print(f"Worker {process_id} error: {e}", file=sys.stderr)
        result_queue.put(None)


def run_multiprocess_client(args, num_processes: int = None):
    """
    Main function to coordinate multiple worker processes
    
    Args:
        args: Parsed command-line arguments
        num_processes: Number of processes to spawn (default: CPU count)
    """
    if num_processes is None:
        num_processes = mp.cpu_count()
    
    print(f"Starting {num_processes} worker processes on {mp.cpu_count()} CPU cores")
    
    # Calculate connections per process
    if args.total > 0:
        connections_per_process = args.total // num_processes
        remainder = args.total % num_processes
    else:
        # Infinite mode: each process runs indefinitely
        connections_per_process = 0
        remainder = 0
    
    # Create result queue for collecting stats
    result_queue = mp.Queue()
    
    # Spawn worker processes
    processes = []
    for i in range(num_processes):
        # Distribute remainder connections to first few processes
        worker_connections = connections_per_process + (1 if i < remainder else 0)
        
        p = mp.Process(
            target=worker_process,
            args=(i, args, worker_connections, result_queue)
        )
        p.start()
        processes.append(p)
        print(f"Started worker process {i} (PID: {p.pid}) - {worker_connections} connections")
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    # Collect and aggregate results
    all_stats = []
    while not result_queue.empty():
        stat = result_queue.get()
        if stat:
            all_stats.append(stat)
    
    # Print aggregated summary
    print_aggregated_summary(all_stats, num_processes)


def print_aggregated_summary(stats_list: List[ProcessStats], num_processes: int):
    """Print aggregated statistics from all worker processes"""
    total_connections = sum(s.total for s in stats_list)
    total_success = sum(s.success for s in stats_list)
    total_failed = sum(s.failed for s in stats_list)
    total_packets = sum(s.total_packets for s in stats_list)
    
    all_latencies = []
    for s in stats_list:
        all_latencies.extend(s.latencies)
    
    max_elapsed = max(s.elapsed for s in stats_list) if stats_list else 0
    aggregate_rate = total_connections / max_elapsed if max_elapsed > 0 else 0
    
    print("\n" + "=" * 70)
    print("  MULTI-PROCESS CLIENT SUMMARY")
    print("=" * 70)
    print(f"  Worker processes  : {num_processes}")
    print(f"  CPU cores         : {mp.cpu_count()}")
    print(f"  Elapsed time      : {max_elapsed:.2f}s")
    print(f"  Total connections : {total_connections}")
    print(f"  Successful        : {total_success}")
    print(f"  Failed            : {total_failed}")
    print(f"  Aggregate rate    : {aggregate_rate:.2f} conn/s")
    print(f"  Total packets sent: {total_packets}")
    
    if all_latencies:
        avg_latency = sum(all_latencies) / len(all_latencies)
        print(f"  Latency avg       : {avg_latency:.2f}ms")
        print(f"  Latency min       : {min(all_latencies):.2f}ms")
        print(f"  Latency max       : {max(all_latencies):.2f}ms")
    
    print("\n  Per-Process Breakdown:")
    print("  " + "-" * 66)
    for s in stats_list:
        rate = s.total / s.elapsed if s.elapsed > 0 else 0
        print(f"  Process {s.process_id:2d}: {s.success:6d} success, "
              f"{s.failed:4d} failed, {rate:8.2f} conn/s")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Multi-process traffic generator client"
    )
    parser.add_argument("--processes", type=int, default=None,
                       help="Number of worker processes (default: CPU count)")
    # Add all other arguments from original client
    # ... (include all arguments from client.py)
    
    args = parser.parse_args()
    
    # Validate and run
    run_multiprocess_client(args, args.processes)


if __name__ == "__main__":
    main()
```

### Strategy 2: Multi-Process Server

```python
#!/usr/bin/env python3
"""
Multi-process traffic generator server
Uses SO_REUSEPORT to distribute connections across processes
"""

import asyncio
import multiprocessing as mp
import socket
from server import run_server, parse_args


def worker_server(process_id: int, args):
    """
    Worker process running its own server instance
    SO_REUSEPORT allows multiple processes to bind to same port
    """
    print(f"Server worker {process_id} starting (PID: {os.getpid()})")
    
    # Each process runs its own event loop
    asyncio.run(run_server(args))


def run_multiprocess_server(args, num_processes: int = None):
    """
    Start multiple server processes, all listening on the same port
    Kernel load-balances incoming connections across processes
    """
    if num_processes is None:
        num_processes = mp.cpu_count()
    
    print(f"Starting {num_processes} server processes on port {args.port}")
    
    # Spawn worker processes
    processes = []
    for i in range(num_processes):
        p = mp.Process(target=worker_server, args=(i, args))
        p.start()
        processes.append(p)
        print(f"Started server worker {i} (PID: {p.pid})")
    
    # Wait for all processes
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nShutting down all server workers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()


# Modify server.py to enable SO_REUSEPORT:
def create_server_socket(host: str, port: int, protocol: str) -> socket.socket:
    """Create server socket with SO_REUSEPORT for multi-process support"""
    if protocol == "tcp":
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Enable SO_REUSEPORT for multi-process load balancing
        if hasattr(socket, 'SO_REUSEPORT'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((host, port))
        sock.listen(1024)
    else:  # UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, 'SO_REUSEPORT'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((host, port))
    
    return sock
```

### Strategy 3: Thread Pool for I/O Operations

For specific I/O-bound operations, use thread pools:

```python
import concurrent.futures
import asyncio


async def run_with_thread_pool(args):
    """
    Use thread pool for blocking I/O operations
    Useful for file operations, DNS lookups, etc.
    """
    loop = asyncio.get_running_loop()
    
    # Create thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Example: Parallel DNS resolution
        hosts = ['host1.example.com', 'host2.example.com', 'host3.example.com']
        
        # Run blocking operations in thread pool
        tasks = [
            loop.run_in_executor(executor, socket.gethostbyname, host)
            for host in hosts
        ]
        
        results = await asyncio.gather(*tasks)
        return results
```

## Performance Comparison

### Single Process (Current)
```bash
# Single core, ~10K-20K conn/s
python3 client.py --port 9000 --cps 10000 --total 100000
```

### Multi-Process (Optimized)
```bash
# 8 cores, ~80K-160K conn/s (8x improvement)
python3 client_multiprocess.py --port 9000 --cps 10000 --total 100000 --processes 8
```

## Implementation Recommendations

### 1. Client-Side Optimization

**Best Approach:** Multi-process client with process pool

```bash
# Automatic: Use all CPU cores
python3 client_multiprocess.py --port 9000 --cps 50000 --total 1000000

# Manual: Specify process count
python3 client_multiprocess.py --port 9000 --cps 50000 --total 1000000 --processes 16
```

**Benefits:**
- Linear scaling with CPU cores (up to network limits)
- No GIL contention
- Independent event loops per process
- Better CPU cache utilization

### 2. Server-Side Optimization

**Best Approach:** Multi-process server with SO_REUSEPORT

```bash
# Start server with 8 worker processes
python3 server_multiprocess.py --port 9000 --processes 8
```

**Benefits:**
- Kernel-level load balancing
- Each process handles subset of connections
- Better CPU utilization
- Scales to 100K+ concurrent connections

### 3. Hybrid Approach

Combine both for maximum performance:

```bash
# Terminal 1: Multi-process server (8 workers)
python3 server_multiprocess.py --port 9000 --processes 8

# Terminal 2: Multi-process client (8 workers)
python3 client_multiprocess.py --port 9000 --cps 100000 --total 10000000 --processes 8
```

## System Tuning for High Performance

### 1. Increase File Descriptor Limits

```bash
# Temporary (current session)
ulimit -n 1048576

# Permanent (add to /etc/security/limits.conf)
* soft nofile 1048576
* hard nofile 1048576
```

### 2. Kernel Network Tuning

```bash
# Increase connection tracking
sudo sysctl -w net.netfilter.nf_conntrack_max=2000000

# Increase socket buffers
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728

# Enable TCP fast open
sudo sysctl -w net.ipv4.tcp_fastopen=3

# Increase port range
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"

# Enable SO_REUSEPORT
sudo sysctl -w net.core.somaxconn=65535
```

### 3. CPU Affinity (Optional)

Pin processes to specific CPU cores:

```python
import os

def set_cpu_affinity(process_id: int, num_processes: int):
    """Pin process to specific CPU core"""
    cpu_count = os.cpu_count()
    cpu_id = process_id % cpu_count
    os.sched_setaffinity(0, {cpu_id})
```

## Performance Benchmarks

### Expected Performance Gains

| Configuration | Connections/sec | Improvement |
|--------------|----------------|-------------|
| Single process | 10,000 - 20,000 | Baseline |
| 2 processes | 20,000 - 40,000 | 2x |
| 4 processes | 40,000 - 80,000 | 4x |
| 8 processes | 80,000 - 160,000 | 8x |
| 16 processes | 160,000 - 320,000 | 16x |

*Note: Actual performance depends on hardware, network capacity, and system tuning*

### Bottleneck Analysis

1. **CPU-bound:** Use more processes (up to 2x CPU cores)
2. **Network-bound:** Optimize network stack, use faster NICs
3. **Memory-bound:** Reduce payload sizes, optimize data structures
4. **Disk I/O-bound:** Use faster storage, reduce logging

## Monitoring and Profiling

### CPU Usage Monitoring

```bash
# Monitor per-process CPU usage
top -H -p $(pgrep -d',' -f client_multiprocess)

# Detailed CPU profiling
python3 -m cProfile -o profile.stats client_multiprocess.py --port 9000 --total 10000
python3 -m pstats profile.stats
```

### Network Monitoring

```bash
# Monitor network throughput
iftop -i eth0

# Monitor connection states
ss -s

# Monitor packet rates
nload eth0
```

## Best Practices

1. **Process Count:** Start with CPU core count, tune based on workload
2. **Connection Distribution:** Ensure even distribution across processes
3. **Resource Limits:** Set appropriate ulimits before scaling
4. **Monitoring:** Track CPU, memory, and network utilization
5. **Graceful Shutdown:** Handle SIGTERM/SIGINT in all processes
6. **Error Handling:** Implement per-process error recovery
7. **Statistics Aggregation:** Collect and merge stats from all processes

## Conclusion

Multi-process architecture can provide **8-16x performance improvement** on modern multicore systems. The key is to:

1. Use separate processes (not threads) to bypass GIL
2. Enable SO_REUSEPORT for server load balancing
3. Tune system limits and kernel parameters
4. Monitor and profile to identify bottlenecks
5. Scale horizontally across multiple machines if needed

For production deployments handling millions of connections, combine multi-process architecture with proper system tuning and monitoring.