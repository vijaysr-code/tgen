# UDP Statistics Fix - Packet Loss Tracking

## Problem

The UDP per-connection statistics implementation was incorrectly reporting a high number of lost packets in good network conditions. The issue was caused by the original sequence number tracking logic that counted **out-of-order packets as lost**.

### Original Implementation Issues

1. **Immediate Loss Detection**: When a gap in sequence numbers was detected (e.g., receiving seq 3 after seq 1), the code immediately counted the missing packets (seq 2) as lost.

2. **No Correction for Reordering**: When the "lost" packet arrived later (out of order), it was only marked as out-of-order, but the lost packet count was never decremented.

3. **Example Scenario**:
   ```
   Client sends: seq 0, 1, 2, 3, 4, 5...
   Server receives (due to network reordering): 0, 1, 3, 2, 4, 5...
   
   Original behavior:
   - Receive seq 0: expected_seq = 1
   - Receive seq 1: expected_seq = 2
   - Receive seq 3: GAP! lost_packets += 1 (seq 2 counted as lost), expected_seq = 4
   - Receive seq 2: out_of_order += 1 (but lost_packets still = 1!)
   
   Result: 1 packet falsely reported as lost
   ```

## Solution

The fix implements a **set-based tracking approach** that properly handles packet reordering:

### Key Changes

1. **Track Received Sequence Numbers**: Use a set (`udp_received_seqs`) to remember all sequence numbers we've seen.

2. **Track Maximum Sequence**: Keep track of the highest sequence number received (`udp_max_seq`).

3. **Deferred Loss Calculation**: Only calculate final loss statistics when the connection expires, based on:
   - Expected total packets = `max_seq + 1` (since sequences start at 0)
   - Actual received = count of unique packets
   - Lost = expected - received

4. **Proper Classification**:
   - **Duplicate**: Same sequence number received twice
   - **Out-of-order**: Sequence number lower than current max (but not a duplicate)
   - **Lost**: Sequence numbers in range [0, max_seq] that were never received

### New Implementation

```python
# Track using a set of received sequence numbers
if seq_num in rec.udp_received_seqs:
    # Duplicate packet
    rec.udp_duplicates += 1
else:
    # New packet
    rec.udp_received_seqs.add(seq_num)
    rec.messages_received += 1
    
    if seq_num > rec.udp_max_seq:
        # New highest sequence number
        rec.udp_max_seq = seq_num
    elif seq_num < rec.udp_max_seq:
        # Out of order packet
        rec.udp_out_of_order += 1

# At connection end:
expected_total = rec.udp_max_seq + 1
actual_received = rec.messages_received
rec.udp_lost_packets = expected_total - actual_received
```

## Benefits

1. **Accurate Loss Detection**: Reordered packets are no longer counted as lost
2. **Proper Statistics**: Loss percentage is calculated correctly as `lost / expected_total`
3. **Memory Efficient**: Set-based tracking is efficient for typical connection sizes
4. **Clear Semantics**: Each packet is classified exactly once (received, duplicate, or lost)

## Testing

Run the test script to verify the fix:

```bash
cd tgen
./test_udp_stats.sh
```

Expected results on localhost:
- Loss should be 0% or very low (<1%)
- Out-of-order packets may occur but should be minimal
- No false positives for lost packets due to reordering

## Performance Considerations

The set-based approach uses O(n) memory where n is the number of unique packets received. For typical use cases:
- 1000 packets = ~8KB memory per connection
- 10000 packets = ~80KB memory per connection

This is acceptable for the traffic generator's use case and provides accurate statistics.

## Limitations

1. **Memory Usage**: For very long-lived connections with millions of packets, memory usage could become significant. Consider implementing a sliding window approach if this becomes an issue.

2. **Sequence Number Wraparound**: The current implementation doesn't handle uint32 wraparound (after 4 billion packets). This is unlikely to be an issue for typical test scenarios.

3. **Final Loss Calculation**: Loss is only calculated accurately at connection end. During the connection, the `udp_lost_packets` field may not reflect the true loss count.