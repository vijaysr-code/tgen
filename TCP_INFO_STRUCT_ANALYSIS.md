# TCP_INFO Structure Analysis and Correction

## Issue Identified

The TCP retransmission statistics may not be reading from the correct offsets in the `struct tcp_info`. The struct layout can vary by:
1. Linux kernel version
2. Architecture (32-bit vs 64-bit)
3. Compiler padding

## Linux tcp_info Structure (from linux/tcp.h)

### Kernel 4.x+ Structure (64-bit x86_64)

```c
struct tcp_info {
    __u8    tcpi_state;           // offset 0
    __u8    tcpi_ca_state;        // offset 1
    __u8    tcpi_retransmits;     // offset 2  ← CORRECT OFFSET
    __u8    tcpi_probes;          // offset 3
    __u8    tcpi_backoff;         // offset 4
    __u8    tcpi_options;         // offset 5
    __u8    tcpi_snd_wscale : 4,  // offset 6
            tcpi_rcv_wscale : 4;
    __u8    tcpi_delivery_rate_app_limited:1; // offset 7
    
    __u32   tcpi_rto;             // offset 8
    __u32   tcpi_ato;             // offset 12
    __u32   tcpi_snd_mss;         // offset 16
    __u32   tcpi_rcv_mss;         // offset 20
    
    __u32   tcpi_unacked;         // offset 24
    __u32   tcpi_sacked;          // offset 28
    __u32   tcpi_lost;            // offset 32  ← LOST PACKETS
    __u32   tcpi_retrans;         // offset 36  ← RETRANSMITTED SEGMENTS
    __u32   tcpi_fackets;         // offset 40
    
    /* Times */
    __u32   tcpi_last_data_sent;  // offset 44
    __u32   tcpi_last_ack_sent;   // offset 48
    __u32   tcpi_last_data_recv;  // offset 52
    __u32   tcpi_last_ack_recv;   // offset 56
    
    /* Metrics */
    __u32   tcpi_pmtu;            // offset 60
    __u32   tcpi_rcv_ssthresh;    // offset 64
    __u32   tcpi_rtt;             // offset 68  ← RTT (microseconds)
    __u32   tcpi_rttvar;          // offset 72  ← RTT variance
    __u32   tcpi_snd_ssthresh;    // offset 76
    __u32   tcpi_snd_cwnd;        // offset 80  ← Congestion window
    __u32   tcpi_advmss;          // offset 84
    __u32   tcpi_reordering;      // offset 88  ← Reordering
    
    // ... more fields follow
};
```

## Current Code Issues

### Issue 1: Wrong Offset for Retransmits
```python
# WRONG - offset 5 is tcpi_options, not tcpi_retransmits
retransmits = struct.unpack_from('B', tcp_info, 5)[0]

# CORRECT - offset 2 is tcpi_retransmits
retransmits = struct.unpack_from('B', tcp_info, 2)[0]
```

### Issue 2: Wrong Offset for RTT
```python
# WRONG - offset 32 is tcpi_lost, not tcpi_rtt
rtt_us = struct.unpack_from('I', tcp_info, 32)[0]

# CORRECT - offset 68 is tcpi_rtt
rtt_us = struct.unpack_from('I', tcp_info, 68)[0]
```

### Issue 3: Correct Offset for Lost (by accident)
```python
# CORRECT - offset 32 is actually tcpi_lost
lost = struct.unpack_from('I', tcp_info, 32)[0]
```

### Issue 4: Wrong Offset for RTT Variance
```python
# WRONG - offset 36 is tcpi_retrans, not tcpi_rttvar
rtt_var_us = struct.unpack_from('I', tcp_info, 36)[0]

# CORRECT - offset 72 is tcpi_rttvar
rtt_var_us = struct.unpack_from('I', tcp_info, 72)[0]
```

### Issue 5: Wrong Offset for Congestion Window
```python
# WRONG - offset 52 is tcpi_last_data_recv, not tcpi_snd_cwnd
snd_cwnd = struct.unpack_from('I', tcp_info, 52)[0]

# CORRECT - offset 80 is tcpi_snd_cwnd
snd_cwnd = struct.unpack_from('I', tcp_info, 80)[0]
```

### Issue 6: Correct Offset for Reordering (by accident)
```python
# CORRECT - offset 88 is tcpi_reordering
reordering = struct.unpack_from('I', tcp_info, 88)[0]
```

## Corrected Implementation

### For client.py

```python
def get_tcp_info(sock: socket.socket) -> Tuple[Optional[int], Optional[float], Optional[int]]:
    """
    Retrieve TCP socket statistics (Linux only).
    Returns: (retransmits, rtt_ms, lost_packets)
    """
    system = platform.system()
    if system != "Linux":
        return (None, None, None)
    
    try:
        # TCP_INFO socket option (Linux-specific)
        TCP_INFO = 11
        
        # Get TCP_INFO structure
        tcp_info = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 256)
        
        # Parse relevant fields from tcp_info structure
        import struct
        
        # CORRECTED OFFSETS based on struct tcp_info (kernel 4.x+, x86_64)
        retransmits = struct.unpack_from('B', tcp_info, 2)[0]   # tcpi_retransmits (u8)
        rtt_us = struct.unpack_from('I', tcp_info, 68)[0]       # tcpi_rtt (u32, microseconds)
        lost = struct.unpack_from('I', tcp_info, 32)[0]         # tcpi_lost (u32)
        
        # Convert microseconds to milliseconds
        rtt_ms = rtt_us / 1000.0 if rtt_us > 0 else None
        
        return (retransmits, rtt_ms, lost)
    except Exception:
        return (None, None, None)
```

### For server.py

```python
def get_tcp_info(sock: socket.socket) -> Tuple[Optional[int], Optional[float], Optional[float],
                                                 Optional[int], Optional[int], Optional[int]]:
    """
    Retrieve TCP socket statistics (Linux only).
    Returns: (retransmits, rtt_ms, rtt_var_ms, snd_cwnd, lost_packets, reordering)
    """
    system = platform.system()
    if system != "Linux":
        return (None, None, None, None, None, None)
    
    try:
        # TCP_INFO socket option (Linux-specific)
        TCP_INFO = 11
        
        # Get TCP_INFO structure (size varies by kernel version, but we only need first ~200 bytes)
        tcp_info = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 256)
        
        # Parse relevant fields from tcp_info structure
        import struct
        
        # CORRECTED OFFSETS based on struct tcp_info (kernel 4.x+, x86_64)
        retransmits = struct.unpack_from('B', tcp_info, 2)[0]   # tcpi_retransmits (u8)
        rtt_us = struct.unpack_from('I', tcp_info, 68)[0]       # tcpi_rtt (u32, microseconds)
        rtt_var_us = struct.unpack_from('I', tcp_info, 72)[0]   # tcpi_rttvar (u32, microseconds)
        snd_cwnd = struct.unpack_from('I', tcp_info, 80)[0]     # tcpi_snd_cwnd (u32)
        lost = struct.unpack_from('I', tcp_info, 32)[0]         # tcpi_lost (u32)
        reordering = struct.unpack_from('I', tcp_info, 88)[0]   # tcpi_reordering (u32)
        
        # Convert microseconds to milliseconds
        rtt_ms = rtt_us / 1000.0 if rtt_us > 0 else None
        rtt_var_ms = rtt_var_us / 1000.0 if rtt_var_us > 0 else None
        
        return (retransmits, rtt_ms, rtt_var_ms, snd_cwnd, lost, reordering)
    except Exception:
        return (None, None, None, None, None, None)
```

## Additional Improvements

### 1. Add Struct Offset Constants

```python
# TCP_INFO struct offsets (Linux kernel 4.x+, x86_64)
_TCPI_STATE = 0
_TCPI_RETRANSMITS = 2
_TCPI_LOST = 32
_TCPI_RETRANS = 36
_TCPI_RTT = 68
_TCPI_RTTVAR = 72
_TCPI_SND_CWND = 80
_TCPI_REORDERING = 88
```

### 2. Add Kernel Version Detection

```python
def get_kernel_version():
    """Get Linux kernel version for struct compatibility check."""
    try:
        import platform
        version = platform.release()
        major = int(version.split('.')[0])
        return major
    except:
        return None
```

### 3. Add Validation

```python
def validate_tcp_info_size(tcp_info: bytes) -> bool:
    """Validate that tcp_info buffer is large enough."""
    # Minimum size for fields we need (up to offset 88 + 4 bytes)
    return len(tcp_info) >= 92
```

## Testing on Linux

To verify the corrections work on Linux:

```bash
# On a Linux system
python3 << 'EOF'
import socket
import struct

# Create a test connection
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('google.com', 80))

# Get TCP_INFO
TCP_INFO = 11
tcp_info = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 256)

print(f"TCP_INFO buffer size: {len(tcp_info)} bytes")
print(f"\nField values:")
print(f"  tcpi_state (offset 0): {struct.unpack_from('B', tcp_info, 0)[0]}")
print(f"  tcpi_retransmits (offset 2): {struct.unpack_from('B', tcp_info, 2)[0]}")
print(f"  tcpi_lost (offset 32): {struct.unpack_from('I', tcp_info, 32)[0]}")
print(f"  tcpi_rtt (offset 68): {struct.unpack_from('I', tcp_info, 68)[0]} us")
print(f"  tcpi_rttvar (offset 72): {struct.unpack_from('I', tcp_info, 72)[0]} us")
print(f"  tcpi_snd_cwnd (offset 80): {struct.unpack_from('I', tcp_info, 80)[0]}")
print(f"  tcpi_reordering (offset 88): {struct.unpack_from('I', tcp_info, 88)[0]}")

sock.close()
EOF
```

## Summary of Changes Needed

| Field | Old Offset | Correct Offset | Type | Notes |
|-------|-----------|----------------|------|-------|
| tcpi_retransmits | 5 ❌ | 2 ✅ | u8 | Was reading tcpi_options |
| tcpi_rtt | 32 ❌ | 68 ✅ | u32 | Was reading tcpi_lost |
| tcpi_rttvar | 36 ❌ | 72 ✅ | u32 | Was reading tcpi_retrans |
| tcpi_snd_cwnd | 52 ❌ | 80 ✅ | u32 | Was reading tcpi_last_data_recv |
| tcpi_lost | 32 ✅ | 32 ✅ | u32 | Correct by accident |
| tcpi_reordering | 88 ✅ | 88 ✅ | u32 | Correct |

## Impact

**Before Fix:**
- Retransmits: Reading wrong field (tcpi_options instead of tcpi_retransmits)
- RTT: Reading wrong field (tcpi_lost instead of tcpi_rtt)
- RTT Variance: Reading wrong field (tcpi_retrans instead of tcpi_rttvar)
- Congestion Window: Reading wrong field (tcpi_last_data_recv instead of tcpi_snd_cwnd)

**After Fix:**
- All fields read from correct offsets
- Statistics will be accurate
- Values will make sense

## Recommendation

**URGENT:** Apply the corrected offsets immediately. The current implementation is reading incorrect fields, which means:
1. Retransmit counts are wrong
2. RTT measurements are wrong
3. Other statistics are unreliable

The fix is straightforward - just update the offset values in both `client.py` and `server.py`.