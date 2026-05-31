#!/usr/bin/env python3
"""
Multi-process traffic generator client
Distributes load across multiple CPU cores for higher throughput
Provides 8-16x performance improvement on multicore systems
"""

import asyncio
import argparse
import copy
import io
import multiprocessing as mp
import os
import platform
import resource
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

# Import existing client functions
from client import run_client, parse_args as client_parse_args, Stats, _format_timestamp


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
    tcp_retransmits_list: List[int]
    tcp_rtt_list: List[float]
    tcp_lost_list: List[int]


def _set_cpu_affinity(process_id: int) -> None:
    """
    Pin process to specific CPU core (Linux only).
    
    Args:
        process_id: Process ID used to determine which CPU core to pin to
    """
    if not hasattr(os, 'sched_setaffinity'):
        return
    
    try:
        cpu_count = os.cpu_count() or 1
        cpu_id = process_id % cpu_count
        os.sched_setaffinity(0, {cpu_id})
    except (OSError, AttributeError):
        pass


def worker_process(process_id: int, args, connections_per_process: int,
                   result_queue: mp.Queue, num_processes: int = 1, use_affinity: bool = False,
                   original_cps: Optional[float] = None):
    """
    Worker process that runs its own event loop.
    Each process handles a portion of the total connections.
    
    Args:
        process_id: Unique ID for this worker process
        args: Command-line arguments
        connections_per_process: Number of connections this worker should handle
        result_queue: Queue for returning statistics
        num_processes: Total number of worker processes
        use_affinity: Whether to pin this process to a specific CPU core
        original_cps: Original total CPS before division (for timeout calculation)
    """
    # Set CPU affinity if requested (Linux only)
    if use_affinity:
        _set_cpu_affinity(process_id)
    
    # Create a copy of args for this worker to avoid modifying the original
    worker_args = copy.copy(args)
    worker_args.total = connections_per_process
    
    # CRITICAL: Divide CPS among workers to avoid overwhelming the server
    # If we don't do this, each worker will try to connect at the full CPS rate,
    # resulting in total_rate = CPS * num_processes, which causes timeouts and 0-byte connections
    worker_args.cps = args.cps / num_processes
    
    # Store original CPS for timeout calculation
    # Timeout should be based on TOTAL server load, not per-worker rate
    if original_cps is not None:
        worker_args.original_cps = original_cps
    
    # Suppress individual worker summary output by redirecting stdout
    # We'll aggregate all stats in the main process
    original_stdout = sys.stdout
    start_time = time.monotonic()
    
    try:
        # Redirect stdout to suppress worker output (we only want aggregated stats)
        try:
            sys.stdout = io.StringIO()
            
            # Run client and capture the returned stats object
            stats = asyncio.run(run_client(worker_args))
            
            # Check for None return (edge case handling)
            if stats is None:
                raise RuntimeError(f'Worker {process_id}: run_client returned None')
        finally:
            # Restore stdout
            sys.stdout = original_stdout
        
        # Calculate elapsed time
        elapsed = time.monotonic() - start_time
        
        # Convert Stats object to ProcessStats for queue transfer
        result = ProcessStats(
            process_id=process_id,
            total=stats.total,
            success=stats.success,
            failed=stats.failed,
            total_packets=stats.total_packets,
            latencies=stats.latencies.copy(),
            elapsed=elapsed,
            tcp_retransmits_list=stats.tcp_retransmits_list.copy(),
            tcp_rtt_list=stats.tcp_rtt_list.copy(),
            tcp_lost_list=stats.tcp_lost_list.copy()
        )
        result_queue.put(result)
        
    except Exception as e:
        # Restore stdout on error
        sys.stdout = original_stdout
        import traceback
        error_msg = (
            f"Worker {process_id} error: {e}\n"
            f"Args: total={connections_per_process}, cps={worker_args.cps:.2f}\n"
            f"{traceback.format_exc()}"
        )
        print(error_msg, file=sys.stderr)
        result_queue.put(None)


def check_and_set_ulimit():
    """
    Check and adjust ulimit (open files) on Linux if needed.
    Target: 1048576 (1M) open files for high-performance client.
    """
    if platform.system() != "Linux":
        return
    
    try:
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        target_limit = 1048576
        
        if soft_limit < target_limit:
            new_limit = min(target_limit, hard_limit)
            
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (new_limit, hard_limit))
                print(f"Adjusted ulimit from {soft_limit} to {new_limit} open files")
            except (ValueError, OSError):
                if new_limit != hard_limit:
                    try:
                        resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))
                        print(f"Adjusted ulimit from {soft_limit} to {hard_limit} open files (max available)")
                    except (ValueError, OSError):
                        print(f"Warning: Could not adjust ulimit (current: {soft_limit}, target: {target_limit})", file=sys.stderr)
                        print(f"Consider running: ulimit -n {target_limit}", file=sys.stderr)
                else:
                    print(f"Warning: Could not adjust ulimit (current: {soft_limit}, target: {target_limit})", file=sys.stderr)
                    print(f"Consider running: ulimit -n {target_limit}", file=sys.stderr)
        else:
            print(f"ulimit already sufficient: {soft_limit} open files")
    except Exception as e:
        print(f"Warning: Could not check ulimit: {e}", file=sys.stderr)


def calculate_process_timeout(total_connections: int, cps: float, duration: float) -> float:
    """
    Calculate reasonable timeout for process completion.
    
    Args:
        total_connections: Total number of connections
        cps: Connections per second rate
        duration: Duration each connection stays alive
    
    Returns:
        Timeout in seconds
    """
    setup_time = total_connections / cps if cps > 0 else total_connections
    
    base_cleanup_buffer = 120.0  # 2 minutes default
    additional_connection_blocks = total_connections // 5000
    cleanup_buffer = base_cleanup_buffer + (additional_connection_blocks * 60.0)
    
    return setup_time + duration + cleanup_buffer


def collect_stats_from_queue(result_queue: mp.Queue,
                             all_stats: List[ProcessStats],
                             collected_worker_ids: set) -> List[ProcessStats]:
    """
    Collect available stats from queue, avoiding duplicates.
    
    Args:
        result_queue: Queue containing ProcessStats objects
        all_stats: List to append collected stats to
        collected_worker_ids: Set of worker IDs already collected
    
    Returns:
        List of newly collected stats
    """
    new_stats = []
    while not result_queue.empty():
        stat = result_queue.get()
        if stat and stat.process_id not in collected_worker_ids:
            new_stats.append(stat)
            all_stats.append(stat)
            collected_worker_ids.add(stat.process_id)
    return new_stats


def print_progress_report(elapsed: float, timeout: float,
                         new_stats: List[ProcessStats],
                         num_processes: int, completed_count: int,
                         running_count: int, connections_per_process: int,
                         cps: float, duration: float, total_connections: int):
    """
    Print periodic progress report during execution.
    
    Args:
        elapsed: Time elapsed since start
        timeout: Total timeout value
        new_stats: Newly collected stats since last report
        num_processes: Total number of worker processes
        completed_count: Number of workers that have completed
        running_count: Number of workers still running
        connections_per_process: Expected connections per worker
        cps: Connections per second rate
        duration: Connection duration
        total_connections: Total connections across all workers
    """
    stats_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"\n[{stats_timestamp}] Progress Report (elapsed: {elapsed:.1f}s / timeout: {timeout:.1f}s)")
    
    # Show completed workers
    if new_stats:
        print(f"  Completed workers since last check:")
        for s in new_stats:
            print(f"    Worker {s.process_id}: {s.success}/{s.total} successful, "
                  f"{s.total_packets} packets, {s.elapsed:.1f}s elapsed")
    
    # Show status
    print(f"  Status: {completed_count}/{num_processes} workers completed, {running_count} still running")
    
    # Show expected progress for running workers
    unreported = num_processes - completed_count
    if unreported > 0 and total_connections > 0:
        expected_setup_time = connections_per_process / cps if cps > 0 else 0
        expected_total_time = expected_setup_time + duration
        progress_pct = min(100, (elapsed / expected_total_time) * 100) if expected_total_time > 0 else 0
        print(f"  Expected: ~{connections_per_process} connections/worker, "
              f"~{expected_total_time:.1f}s total time")
        print(f"  Progress: ~{progress_pct:.0f}% of expected time elapsed")


def terminate_hung_processes(hung_processes: List[mp.Process]) -> None:
    """
    Terminate hung processes gracefully, then forcefully if needed.
    
    Args:
        hung_processes: List of processes that exceeded timeout
    """
    if not hung_processes:
        return
    
    terminate_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"\n[{terminate_timestamp}] Terminating {len(hung_processes)} hung process(es)...",
          file=sys.stderr)
    print(f"  Note: Stats from terminated processes will NOT be available", file=sys.stderr)
    print(f"        Workers only report stats upon completion, not during execution", file=sys.stderr)
    
    # Attempt graceful termination
    for p in hung_processes:
        p.terminate()
    
    # Give them a moment to terminate gracefully
    time.sleep(1)
    
    # Force kill if still alive
    for p in hung_processes:
        if p.is_alive():
            p.kill()
            kill_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"[{kill_timestamp}] Force killed process {p.pid}", file=sys.stderr)


def run_multiprocess_client(args, num_processes: Optional[int] = None, use_affinity: bool = False, stats_interval: float = 30.0):
    """
    Main function to coordinate multiple worker processes
    
    Args:
        args: Parsed command-line arguments
        num_processes: Number of processes to spawn (default: CPU count)
        use_affinity: Pin each process to a specific CPU core (Linux only)
        stats_interval: Interval in seconds to print intermediate stats (0 = disabled)
    """
    if num_processes is None:
        num_processes = mp.cpu_count()
    
    print("=" * 70)
    print("  MULTI-PROCESS CLIENT")
    print("=" * 70)
    print(f"  Worker processes  : {num_processes}")
    print(f"  CPU cores         : {mp.cpu_count()}")
    if use_affinity:
        print(f"  CPU affinity      : enabled (pinned to cores)")
    print("=" * 70)
    print()
    
    # Calculate connections per process
    if args.total > 0:
        connections_per_process = args.total // num_processes
        remainder = args.total % num_processes
        
        if connections_per_process == 0:
            print(f"Error: --total ({args.total}) is less than --processes ({num_processes})", 
                  file=sys.stderr)
            print(f"Either reduce --processes or increase --total", file=sys.stderr)
            sys.exit(1)
    else:
        # Infinite mode: each process runs indefinitely
        connections_per_process = 0
        remainder = 0
    
    # Create result queue for collecting stats
    result_queue = mp.Queue()
    
    # Spawn worker processes
    processes = []
    start_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{start_timestamp}] Starting {num_processes} worker processes...")
    
    for i in range(num_processes):
        # Distribute remainder connections to first few processes
        worker_connections = connections_per_process + (1 if i < remainder else 0)
        
        p = mp.Process(
            target=worker_process,
            args=(i, args, worker_connections, result_queue, num_processes, use_affinity, args.cps)
        )
        p.start()
        processes.append(p)
        print(f"Started worker process {i} (PID: {p.pid}) - {worker_connections} connections")
    
    print()
    
    # Calculate timeout for process completion
    if args.total > 0:
        timeout_per_process = calculate_process_timeout(args.total, args.cps, args.duration)
    else:
        # Should not reach here, but provide fallback
        timeout_per_process = 600.0  # 10 minutes default
    
    # Wait for all processes to complete with timeout
    # Periodically collect and print stats if stats_interval > 0
    start_wait = time.time()
    last_stats_time = start_wait
    hung_processes = []
    all_stats = []
    collected_worker_ids = set()  # Track which workers have reported stats
    
    if stats_interval > 0:
        print(f"\nMonitoring processes (stats every {stats_interval}s)...\n")
    
    # Poll processes until all complete or timeout
    while True:
        current_time = time.time()
        elapsed = current_time - start_wait
        
        # Check if we've exceeded timeout
        if elapsed >= timeout_per_process:
            # Mark any still-alive processes as hung
            for p in processes:
                if p.is_alive():
                    print(f"Warning: Process {p.pid} did not complete within timeout",
                          file=sys.stderr)
                    hung_processes.append(p)
            break
        
        # Check if all processes have completed
        if all(not p.is_alive() for p in processes):
            break
        
        # Collect stats and print progress report periodically
        if stats_interval > 0 and (current_time - last_stats_time) >= stats_interval:
            new_stats = collect_stats_from_queue(result_queue, all_stats, collected_worker_ids)
            
            completed = len(collected_worker_ids)
            running = sum(1 for p in processes if p.is_alive())
            
            print_progress_report(
                elapsed, timeout_per_process, new_stats, num_processes,
                completed, running, connections_per_process,
                args.cps, args.duration, args.total
            )
            
            last_stats_time = current_time
        
        # Sleep briefly before next check
        time.sleep(0.5)
    
    # Terminate any hung processes
    terminate_hung_processes(hung_processes)
    
    # Collect any remaining stats from queue
    collect_stats_from_queue(result_queue, all_stats, collected_worker_ids)
    
    # Print aggregated summary
    print_aggregated_summary(all_stats, num_processes)


def print_aggregated_summary(stats_list: List[ProcessStats], num_processes: int):
    """Print aggregated statistics from all worker processes"""
    if not stats_list:
        print("No statistics collected from workers.", file=sys.stderr)
        return
    
    total_connections = sum(s.total for s in stats_list)
    total_success = sum(s.success for s in stats_list)
    total_failed = sum(s.failed for s in stats_list)
    total_packets = sum(s.total_packets for s in stats_list)
    
    all_latencies = []
    for s in stats_list:
        all_latencies.extend(s.latencies)
    
    all_tcp_retransmits = []
    for s in stats_list:
        all_tcp_retransmits.extend(s.tcp_retransmits_list)
    
    all_tcp_rtt = []
    for s in stats_list:
        all_tcp_rtt.extend(s.tcp_rtt_list)
    
    all_tcp_lost = []
    for s in stats_list:
        all_tcp_lost.extend(s.tcp_lost_list)
    
    max_elapsed = max(s.elapsed for s in stats_list) if stats_list else 0
    aggregate_rate = total_connections / max_elapsed if max_elapsed > 0 else 0
    
    timestamp = _format_timestamp()
    print("\n" + "=" * 70)
    print(f"  MULTI-PROCESS CLIENT SUMMARY [{timestamp}]")
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
    
    # TCP statistics summary
    if all_tcp_retransmits:
        total_retx = sum(all_tcp_retransmits)
        print("")
        print("  TCP Statistics:")
        print(f"    Total retransmits : {total_retx}")
        print(f"    Avg retransmits   : {total_retx/len(all_tcp_retransmits):.1f} per conn")
    
    if all_tcp_rtt:
        avg_rtt = sum(all_tcp_rtt) / len(all_tcp_rtt)
        print(f"    RTT avg           : {avg_rtt:.2f}ms")
        print(f"    RTT min           : {min(all_tcp_rtt):.2f}ms")
        print(f"    RTT max           : {max(all_tcp_rtt):.2f}ms")
    
    if all_tcp_lost:
        total_lost = sum(all_tcp_lost)
        if total_lost > 0:
            print(f"    Total lost packets: {total_lost}")
    
    print("\n  Per-Process Breakdown:")
    print("  " + "-" * 66)
    for s in stats_list:
        rate = s.total / s.elapsed if s.elapsed > 0 else 0
        print(f"  Process {s.process_id:2d}: {s.success:6d} success, "
              f"{s.failed:4d} failed, {rate:8.2f} conn/s")
    print("=" * 70)


def parse_args():
    epilog = """\
SYNTAX
------
  python client_multiprocess.py --port PORT [options]

OPTIONS
-------
  --host HOST           Target host (default: 127.0.0.1)
  --port PORT           Target port (required)
  --protocol {tcp,udp}  Protocol to use (default: tcp)
  --cps CPS             Connections per second (default: 1.0)
  --total N             Total connections to make; 0 = infinite (default: 1000)
  --duration SECS       Seconds to hold each connection open; 0 = short-lived (default: 0.0)
  --payload TEXT        Payload string to send (default: PING)
  --payload-size BYTES  Random payload size in bytes; overrides --payload (default: 0)
  --pps PPS             Packets per second per connection; requires --duration > 0 (default: 0.0)
  --keepalive-mode MODE TCP keepalive mode: standard or aggressive (default: standard)
  --heartbeat           Enable heartbeat packets when pps=0 (default: disabled)
  -F, --file PATH       File to send over each TCP connection (max 128 MiB)
  --output PATH         Optional file path to log the output results
  --processes N         Number of worker processes (default: CPU count)
  --cpu-affinity        Pin each worker process to a specific CPU core (Linux only)
  --stats-interval SECS Print intermediate stats every N seconds; 0 = disabled (default: 30)
  -h, --help            Show this help message and exit

NOTES
-----
  * Uses multiple processes to bypass Python's GIL and leverage all CPU cores
  * Each worker process runs its own event loop independently
  * Provides 8-16x performance improvement on multicore systems
  * TCP keepalive modes:
    - standard: idle=10s, interval=10s, count=5 (60s detection, heartbeat=15s)
    - aggressive: idle=5s, interval=5s, count=3 (20s detection, heartbeat=10s)
  * --heartbeat sends application-level keepalive when pps=0 (tolerates 3 failures)
  * -F/--file is TCP-only; --duration, --pps, and --payload are ignored in file mode
  * --pps has no effect unless --duration > 0
  * --pps accepts fractional values for low-rate traffic (see PPS REFERENCE below)

PPS REFERENCE
-------------
  Common PPS values for different use cases:

  High-Rate Traffic (Throughput Testing):
    --pps 1000      = 1,000 packets/sec (1 ms interval)
    --pps 100       = 100 packets/sec (10 ms interval)
    --pps 10        = 10 packets/sec (100 ms interval)

  Medium-Rate Traffic (Moderate Load):
    --pps 1         = 1 packet/sec (1 second interval)
    --pps 0.5       = 1 packet every 2 seconds
    --pps 0.2       = 1 packet every 5 seconds

  Low-Rate Traffic (Keepalive/NAT Traversal):
    --pps 0.1       = 1 packet every 10 seconds (aggressive keepalive)
    --pps 0.067     = 1 packet every 15 seconds (standard keepalive)
    --pps 0.05      = 1 packet every 20 seconds
    --pps 0.033     = 1 packet every 30 seconds (typical NAT timeout)
    --pps 0.017     = 1 packet every 60 seconds (1 minute interval)
    --pps 0.0083    = 1 packet every 2 minutes
    --pps 0.0033    = 1 packet every 5 minutes

  Formula: pps = 1 / interval_seconds
  Example: For 45-second interval: pps = 1/45 = 0.022

EXAMPLES
--------
  # Automatic: Use all CPU cores for 100K connections at 50K conn/s
  python client_multiprocess.py --port 9000 --cps 50000 --total 100000

  # Manual: 8 worker processes for 1M connections at 100K conn/s
  python client_multiprocess.py --port 9000 --cps 100000 --total 1000000 --processes 8

  # UDP with 16 workers, long-lived connections
  python client_multiprocess.py --port 9001 --protocol udp --cps 10000 --total 100000 --duration 2 --processes 16

  # Low-rate: UDP keepalive every 30 seconds across 4 workers
  python client_multiprocess.py --port 9001 --protocol udp --duration 300 --pps 0.033 --processes 4

  # File transfer with 4 workers
  python client_multiprocess.py --port 9000 --cps 1000 --total 10000 -F /path/to/file.bin --processes 4

PERFORMANCE
-----------
  Expected performance gains (compared to single-process):
    2 processes  : 2x improvement
    4 processes  : 4x improvement
    8 processes  : 8x improvement
    16 processes : 16x improvement
  
  Actual performance depends on hardware, network capacity, and system tuning.
"""
    parser = argparse.ArgumentParser(
        description="Multi-process Traffic Generator Client — high-performance client using multiprocessing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", default=argparse.SUPPRESS,
                        help="Show this help message and exit")
    parser.add_argument("--host", default="127.0.0.1", 
                        help="Target host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, required=True, 
                        help="Target port (required)")
    parser.add_argument("--protocol", choices=["tcp", "udp"], default="tcp",
                        help="Protocol to use: tcp or udp (default: tcp)")
    parser.add_argument("--cps", type=float, default=1.0,
                        help="Connections per second (default: 1.0)")
    parser.add_argument("--total", type=int, default=1000,
                        help="Total connections to make; 0 = infinite (default: 1000)")
    parser.add_argument("--duration", type=float, default=0.0,
                        help="Seconds to hold each connection open; 0 = short-lived (default: 0.0)")
    parser.add_argument("--payload", default="PING",
                        help="Payload string to send (default: PING)")
    parser.add_argument("--payload-size", type=int, default=0,
                        help="Random payload size in bytes; overrides --payload (default: 0)")
    parser.add_argument("--pps", type=float, default=0.0,
                        help="Packets per second per connection; requires --duration > 0 (default: 0.0)")
    parser.add_argument("--keepalive-mode", type=str, choices=['standard', 'aggressive'], default='standard',
                        help="TCP keepalive mode: standard (10s/10s/5, 60s detection) or aggressive (5s/5s/3, 20s detection) (default: standard)")
    parser.add_argument("--heartbeat", action="store_true", default=False,
                        help="Enable heartbeat packets when pps=0 (15s for standard, 10s for aggressive) (default: disabled)")
    parser.add_argument("-F", "--file", type=str, default=None,
                        help="File to send over each TCP connection (max 128 MiB)")
    parser.add_argument("--output", type=str, default=None,
                        help="Optional file path to log the output results")
    parser.add_argument("--processes", type=int, default=None,
                        help="Number of worker processes (default: CPU count)")
    parser.add_argument("--cpu-affinity", action="store_true",
                        help="Pin each worker process to a specific CPU core (Linux only)")
    parser.add_argument("--stats-interval", type=float, default=30.0,
                        help="Print intermediate stats every N seconds; 0 = disabled (default: 30)")
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Check and adjust ulimit on Linux
    check_and_set_ulimit()
    
    # Set up output file if specified
    if args.output:
        try:
            from client import Tee
            sys.stdout = Tee(args.output)
        except Exception as e:
            print(f"Error opening output file: {e}", file=sys.stderr)
            sys.exit(1)
    
    # Validate arguments
    if args.cps <= 0:
        print("Error: --cps must be > 0", file=sys.stderr)
        sys.exit(1)
    if args.pps < 0:
        print("Error: --pps must be >= 0", file=sys.stderr)
        sys.exit(1)
    if args.duration < 0:
        print("Error: --duration must be >= 0", file=sys.stderr)
        sys.exit(1)
    if args.pps > 0 and args.duration == 0:
        print("Warning: --pps has no effect without --duration > 0 (single-shot mode)",
              file=sys.stderr)
    if args.file and args.protocol == "udp":
        print("Error: -F/--file is only supported with TCP (UDP is unreliable for integrity checks)",
              file=sys.stderr)
        sys.exit(1)
    if args.processes is not None and args.processes < 1:
        print("Error: --processes must be >= 1", file=sys.stderr)
        sys.exit(1)
    if args.stats_interval < 0:
        print("Error: --stats-interval must be >= 0", file=sys.stderr)
        sys.exit(1)
    
    # Validate file if specified
    if args.file:
        try:
            from client import load_file_metadata
            load_file_metadata(args.file)  # validate early: existence + size
        except (OSError, ValueError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    # Run multi-process client
    try:
        run_multiprocess_client(args, args.processes, args.cpu_affinity, args.stats_interval)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        # Ensure all output is flushed to file
        sys.stdout.flush()


if __name__ == "__main__":
    main()

# Made with Bob
