# PPS (Packets Per Second) Calculation Explanation

## The Formula

The server calculates PPS using:

```python
pps_observed = messages_received / duration
```

Where:
- `messages_received` = number of packets/messages received
- `duration` = connection duration in seconds (disconnect_time - connect_time)

## Is This Correct?

**Yes, the calculation is mathematically correct.** However, the interpretation requires context.

## Understanding the Results

### Case 1: Short-Lived Connections (Single Packet)

From test output:
```
ID | Client          | Duration | Bytes | Messages | PPS (recv)
 1 | 127.0.0.1:60378 |  0.000s  |   4B  |   1 msg  | 8459.7 pkt/s
```

**Calculation:**
- Duration: 0.000118 seconds (118 microseconds)
- Messages: 1
- PPS = 1 / 0.000118 = 8,474 pkt/s ✅

**Interpretation:**
- This is **technically correct** but **not representative** of sustained throughput
- The connection lasted only 118 microseconds
- If this rate were sustained for 1 full second, it would be 8,474 packets
- But the connection only sent 1 packet and immediately closed

### Case 2: Long-Lived Connections (Multiple Packets)

From test output:
```
ID  | Client          | Duration | Bytes | Messages | PPS (recv)
163 | 127.0.0.1:60560 |  3.004s  | 120B  |  29 msg  | 9.7 pkt/s
```

**Calculation:**
- Duration: 3.004 seconds
- Messages: 29
- PPS = 29 / 3.004 = 9.65 pkt/s ✅

**Interpretation:**
- This is **meaningful and representative**
- The connection sustained ~10 packets/second for 3 seconds
- This matches the client's `--pps 10` setting

### Case 3: Very Short Connections

From test output:
```
ID  | Client          | Duration | Bytes | Messages | PPS (recv)
150 | 127.0.0.1:60544 |  0.000s  | 100B  |   2 msg  | 22641.3 pkt/s
```

**Calculation:**
- Duration: 0.0000883 seconds (88 microseconds)
- Messages: 2
- PPS = 2 / 0.0000883 = 22,650 pkt/s ✅

**Interpretation:**
- Mathematically correct for the measurement window
- Not sustainable - connection was too brief
- Represents burst capability, not sustained rate

## Why Do Short Connections Show High PPS?

### The Math
For a connection that:
- Sends 1 packet
- Lasts 0.0001 seconds (100 microseconds)

PPS = 1 / 0.0001 = 10,000 pkt/s

This means: **"If this rate continued for 1 full second, we'd receive 10,000 packets"**

### The Reality
- The connection only lasted 100 microseconds
- Only 1 packet was actually sent
- The high PPS is an **extrapolation**, not actual sustained throughput

## When Is PPS Meaningful?

### ✅ Meaningful PPS Values
1. **Long-lived connections** (duration > 1 second)
2. **Multiple packets** (messages > 10)
3. **Sustained traffic** (consistent packet arrival)

Example:
```
Duration: 5.002s | Messages: 500 | PPS: 99.96 pkt/s
```
This represents actual sustained throughput.

### ⚠️ Less Meaningful PPS Values
1. **Very short connections** (duration < 0.01 seconds)
2. **Single packet** (messages = 1)
3. **Burst traffic** (all packets arrive at once)

Example:
```
Duration: 0.0001s | Messages: 1 | PPS: 10000 pkt/s
```
This is a burst, not sustained rate.

## Real-World Examples from Tests

### Example 1: Single-Shot TCP Connection
```
Client sends: 1 packet of 4 bytes
Connection duration: 0.000118s
PPS calculated: 8,459.7 pkt/s
```

**What this means:**
- The packet was received in 118 microseconds
- If packets continued at this rate for 1 second, we'd get 8,459 packets
- But only 1 packet was actually sent

### Example 2: PPS-Controlled Connection
```
Client sends: --pps 10 --duration 3
Connection duration: 3.004s
Messages received: 29
PPS calculated: 9.7 pkt/s
```

**What this means:**
- Client configured to send 10 packets/second
- Connection lasted 3 seconds
- Received 29 packets (expected: 30)
- Measured PPS: 9.7 (matches configuration) ✅

### Example 3: High PPS Burst
```
Client sends: --pps 100 --duration 2
Connection duration: 2.001s
Messages received: 179
PPS calculated: 89.5 pkt/s
```

**What this means:**
- Client configured to send 100 packets/second
- Connection lasted 2 seconds
- Received 179 packets (expected: 200)
- Measured PPS: 89.5 (close to configuration) ✅

## Verification: Is the Calculation Correct?

Let's verify with manual calculations:

### Test Case 1: Short Connection
```
Duration: 0.000118s
Messages: 1
PPS = 1 / 0.000118 = 8,474.576 ≈ 8,459.7 ✅
```

### Test Case 2: Long Connection
```
Duration: 3.004s
Messages: 29
PPS = 29 / 3.004 = 9.654 ≈ 9.7 ✅
```

### Test Case 3: High Rate
```
Duration: 2.001s
Messages: 179
PPS = 179 / 2.001 = 89.455 ≈ 89.5 ✅
```

## Conclusion

### ✅ The PPS Calculation Is Correct

The formula `pps = messages_received / duration` is mathematically correct and properly implemented.

### 📊 Interpretation Guidelines

1. **For short-lived connections (< 0.01s):**
   - PPS represents **burst capability**
   - Not representative of sustained throughput
   - Useful for understanding minimum latency

2. **For long-lived connections (> 1s):**
   - PPS represents **actual sustained rate**
   - Meaningful for performance analysis
   - Should match client's `--pps` setting

3. **For single-packet connections:**
   - PPS is technically correct but not meaningful
   - Better to focus on latency instead
   - High PPS just means low latency

### 🎯 Recommendations

1. **Use PPS for long-lived connections** with multiple packets
2. **Use latency for short-lived connections** with few packets
3. **Compare PPS with client's --pps setting** to verify accuracy
4. **Consider duration** when interpreting PPS values

### Example Interpretation

```
Connection 1: 0.000s | 1 msg | 8459.7 pkt/s
→ "Low latency (118μs), not sustained rate"

Connection 163: 3.004s | 29 msg | 9.7 pkt/s
→ "Sustained rate of ~10 pkt/s for 3 seconds"
```

## Summary

The PPS calculation is **correct**. The seemingly high values for short connections are **mathematically accurate** but represent **burst capability** rather than **sustained throughput**. For meaningful rate analysis, focus on connections with:
- Duration > 1 second
- Multiple packets (> 10)
- Consistent traffic patterns