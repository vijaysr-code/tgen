# UDP Statistics Fix - Simple Packet Loss Calculation

## Problem

The UDP per-connection statistics implementation was using complex sequence number tracking that could incorrectly report lost packets due to packet reordering, which is common in UDP networks.

## Solution

Implemented a simple packet loss calculation: **packet loss = expected packets - received packets**

### How It Works

1. **Client sends expected packet count**: Each UDP packet now includes:
   - Sequence number (4 bytes)
   - Expected total packets (4 bytes)
   - Payload data

2. **Server tracks expected vs received**:
   - Extracts expected packet count from the first packet
   - Counts all received packets
   - At connection end: `lost_packets = expected_packets - messages_received`

3. **Loss percentage**: `loss% = (lost_packets / expected_packets) * 100`

### Changes Made

**Client (`client.py`):**
- Modified UDP packet format to include both sequence number and expected total
- Packet format: `[seq_num:4 bytes][expected_total:4 bytes][payload]`
- Calculates expected packets as `int(duration * pps)` for multi-packet connections

**Server (`server.py`):**
- Simplified `ConnectionRecord` to track only `udp_expected_packets` and `udp_lost_packets`
- Removed complex sequence tracking (sets, max_seq, out-of-order, duplicates)
- Simple calculation at connection end: `lost = expected - received`
- Updated statistics display to show: expected, received, lost, and loss percentage

### Benefits

1. **Simple and accurate**: No false positives from packet reordering
2. **Clear semantics**: Loss is simply the difference between expected and received
3. **Low overhead**: No memory-intensive set tracking
4. **Easy to understand**: Straightforward calculation that matches user expectations

### Example

```
Client sends 100 packets (duration=2s, pps=50)
Server receives 98 packets

Result:
  Expected: 100
  Received: 98
  Lost: 2 (2.0%)
```

### Testing

Run the test script to verify:

```bash
cd tgen
./test_udp_stats.sh
```

Expected results on localhost:
- Loss should be 0% or very low (<1%)
- Statistics clearly show expected vs received counts

## Limitations

1. **Client must know packet count**: The client calculates expected packets as `duration * pps`, which may not be exact due to timing variations
2. **No reordering detection**: The simple approach doesn't distinguish between lost and reordered packets
3. **No duplicate detection**: Duplicate packets are counted as received (though UDP typically doesn't have duplicates)

These limitations are acceptable for a traffic generator where the goal is to measure overall packet delivery success rate.