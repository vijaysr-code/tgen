# Scaling to 64K Connections Per Client

## Current Limitations

### 1. **Python asyncio Event Loop Bottleneck**
- **Current Performance**: ~500-1000 concurrent connections with acceptable performance
- **Issue**: Single-threaded event loop becomes saturated with 64K concurrent tasks
- **Impact**: Severe performance degradation, high latency, reduced throughput

### 2. **Operating System Limits**
- **File Descriptors**: Default limit typically 1024-4096 per process
- **Ephemeral Ports**: Client-side port exhaustion (32K-60K available ports)
- **Memory**: Each connection consumes ~4-8KB (256-512 MB for 64K connections)
- **TCP Connection Tracking**: Kernel connection table limits

### 3. **Network Stack Limitations**
- **TCP TIME_WAIT**: Closed connections remain in TIME_WAIT state (60-120s)
- **Socket Buffers**: Kernel memory for send/receive buffers per connection
- **Connection Tracking**: iptables/netfilter conntrack table size

## Recommended Architecture Changes

### Option 1: Multi-Process Architecture (Recommended for Python)

**Approach**: Distribute connections across multiple client processes

```python
# Pseudo-code structure
def run_client_worker(worker_id, connections_per_worker, shared_stats):
    """Each worker handles a subset of connections"""
    # Worker handles connections_per_worker connections
    # Example: 64 workers × 1000 connections = 64K total
    pass

def main():
    num_workers = 64  # Configurable
    connections_per_worker = 1000
    
    # Spawn worker processes
    with multiprocessing.Pool(num_workers) as pool:
        results = pool.starmap(run_client_worker, 
                              [(i, connections_per_worker, stats) 
                               for i in range(num_workers)])
```

**Benefits**:
- Bypasses single event loop limitation
- Each process has independent file descriptor limits
- Better CPU utilization across cores
- Easier to implement in existing Python codebase

**Drawbacks**:
- Higher memory overhead (multiple Python interpreters)
- More complex coordination and statistics aggregation
- Still limited by Python's GIL per process

### Option 2: Rewrite in Go/Rust (Best Performance)

**Go Implementation**:
```go
// Goroutines are extremely lightweight (~2KB stack)
// Can easily handle 100K+ concurrent connections

func main() {
    var wg sync.WaitGroup
    for i := 0; i < 64000; i++ {
        wg.Add(1)
        go func(connID int) {
            defer wg.Done()
            // Handle connection
        }(i)
    }
    wg.Wait()
}
```

**Rust Implementation**:
```rust
// Using tokio async runtime
#[tokio::main]
async fn main() {
    let mut tasks = Vec::new();
    for i in 0..64000 {
        tasks.push(tokio::spawn(async move {
            // Handle connection
        }));
    }
    futures::future::join_all(tasks).await;
}
```

**Benefits**:
- Native async/await with minimal overhead
- Much lower memory per connection (~2-4KB vs 8-16KB in Python)
- Better performance (10-100x faster)
- No GIL limitations

**Drawbacks**:
- Complete rewrite required
- Different ecosystem and tooling

### Option 3: Hybrid Approach

**Approach**: Keep Python for orchestration, use compiled extension for connection handling

```python
# Use Cython or Rust PyO3 for performance-critical parts
import fast_connections  # Compiled extension

def run_client(args):
    # Python handles CLI, stats, coordination
    # Compiled code handles actual connections
    fast_connections.create_connections(
        count=64000,
        target=args.host,
        port=args.port,
        # ...
    )
```

## Required System Configuration Changes

### 1. **Increase File Descriptor Limits**

```bash
# /etc/security/limits.conf
* soft nofile 100000
* hard nofile 100000

# Verify
ulimit -n 100000
```

### 2. **Increase Ephemeral Port Range**

```bash
# /etc/sysctl.conf or /etc/sysctl.d/99-tgen.conf
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 30

# Apply
sudo sysctl -p
```

### 3. **Increase Connection Tracking**

```bash
# /etc/sysctl.conf
net.netfilter.nf_conntrack_max = 200000
net.nf_conntrack_max = 200000

# Increase hash table size
net.netfilter.nf_conntrack_buckets = 50000
```

### 4. **Optimize TCP Stack**

```bash
# /etc/sysctl.conf
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 65535
net.ipv4.tcp_max_syn_backlog = 65535

# Socket buffer sizes
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# Reduce TIME_WAIT
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
```

### 5. **Use Multiple IP Addresses**

To bypass the ~64K ephemeral port limit per IP:

```bash
# Add virtual IPs
sudo ip addr add 192.168.1.101/24 dev eth0
sudo ip addr add 192.168.1.102/24 dev eth0
# ... up to 192.168.1.164 for 64 IPs × 1000 conns = 64K
```

## Implementation Recommendations

### Phase 1: Multi-Process Python (Quick Win)

**Estimated Effort**: 2-3 days

1. **Add `--workers` parameter**:
```python
parser.add_argument('--workers', type=int, default=1,
                   help='Number of worker processes (for scaling)')
```

2. **Implement worker pool**:
```python
def run_worker(worker_id, connections, args, result_queue):
    """Each worker handles a subset of connections"""
    stats = ClientStats()
    # Run connections
    result_queue.put(stats.to_dict())

def run_client_multiprocess(args):
    if args.workers == 1:
        return run_client(args)  # Existing single-process code
    
    connections_per_worker = args.total // args.workers
    result_queue = multiprocessing.Queue()
    
    processes = []
    for i in range(args.workers):
        p = multiprocessing.Process(
            target=run_worker,
            args=(i, connections_per_worker, args, result_queue)
        )
        processes.append(p)
        p.start()
    
    # Aggregate results
    for p in processes:
        p.join()
```

3. **Test scaling**:
```bash
# Test with 10K connections across 10 workers
python3 client.py --workers 10 --total 10000 --cps 1000

# Test with 64K connections across 64 workers
python3 client.py --workers 64 --total 64000 --cps 1000
```

**Expected Performance**:
- 10K connections: Good performance
- 64K connections: Acceptable for short-lived connections
- Long-lived with PPS: May struggle due to aggregate load

### Phase 2: Optimize Python Implementation (Medium Term)

**Estimated Effort**: 1 week

1. **Use uvloop** (faster event loop):
```python
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
```

2. **Connection pooling** for long-lived connections:
```python
# Reuse connections instead of creating new ones
class ConnectionPool:
    def __init__(self, size):
        self.pool = asyncio.Queue(maxsize=size)
```

3. **Batch operations**:
```python
# Send packets in batches instead of one-by-one
async def send_batch(writer, packets):
    writer.writelines(packets)
    await writer.drain()
```

### Phase 3: Rewrite in Go/Rust (Long Term)

**Estimated Effort**: 3-4 weeks

**Recommended**: Go for faster development, Rust for maximum performance

**Go Advantages**:
- Simpler than Rust
- Excellent networking libraries
- Built-in concurrency (goroutines)
- Fast compilation

**Rust Advantages**:
- Maximum performance
- Memory safety guarantees
- Tokio async runtime is mature
- Can create Python bindings (PyO3)

## Testing Strategy

### 1. **Incremental Load Testing**

```bash
# Test progression
python3 client.py --total 1000 --cps 100    # Baseline
python3 client.py --total 5000 --cps 500    # 5K
python3 client.py --total 10000 --cps 1000  # 10K
python3 client.py --total 20000 --cps 2000  # 20K
python3 client.py --total 40000 --cps 4000  # 40K
python3 client.py --total 64000 --cps 6400  # 64K
```

### 2. **Monitor System Resources**

```bash
# Watch file descriptors
watch -n 1 'lsof -p $(pgrep -f client.py) | wc -l'

# Watch connections
watch -n 1 'ss -s'

# Watch memory
watch -n 1 'ps aux | grep client.py'
```

### 3. **Measure Performance Metrics**

- Connection establishment rate (actual vs target CPS)
- Memory usage per connection
- CPU utilization
- Latency distribution
- Packet loss rate
- Connection failure rate

## Cost-Benefit Analysis

| Approach | Effort | Performance | Scalability | Maintenance |
|----------|--------|-------------|-------------|-------------|
| Multi-Process Python | Low (2-3 days) | Medium (10-20K) | Medium | Easy |
| Optimized Python | Medium (1 week) | Medium (20-30K) | Medium | Easy |
| Go Rewrite | High (3-4 weeks) | High (100K+) | Excellent | Medium |
| Rust Rewrite | Very High (4-6 weeks) | Excellent (200K+) | Excellent | Hard |

## Recommendation

**For 64K connections:**

1. **Immediate (Week 1)**: Implement multi-process architecture
   - Quick to implement
   - Reuses existing code
   - Can handle 64K short-lived connections
   - Good enough for most use cases

2. **Short-term (Month 1)**: Add system tuning guide
   - Document all sysctl settings
   - Create setup scripts
   - Add monitoring tools

3. **Long-term (Quarter 1)**: Consider Go rewrite if:
   - Need sustained 64K+ concurrent connections
   - Need long-lived connections with high PPS
   - Performance is critical
   - Have development resources

## Example Multi-Process Implementation

See `client_multiprocess.py` (to be created) for a working example of the multi-process architecture that can scale to 64K connections.

## Conclusion

**For 64K connections, the multi-process Python approach is the most pragmatic solution:**
- ✅ Achievable in 2-3 days
- ✅ Reuses 95% of existing code
- ✅ Can handle 64K short-lived connections
- ✅ Easy to maintain and debug
- ⚠️ May struggle with 64K long-lived + high PPS (consider Go rewrite)

**System tuning is mandatory regardless of approach.**