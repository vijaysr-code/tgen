# Network Statistics Enhancement

## Overview

The traffic generator now collects and displays detailed network statistics for both TCP and UDP connections.

### TCP Statistics (Linux Only)

- **Retransmits**: Number of TCP segment retransmissions
- **RTT (Round-Trip Time)**: Current smoothed round-trip time in milliseconds
- **RTT Variance**: Variance in round-trip time measurements
- **Lost Packets**: Number of packets marked as lost by TCP
- **Congestion Window**: Current TCP send congestion window size
- **Reordering**: TCP reordering metric

### UDP Statistics (All Platforms)

- **Lost Packets**: Detected via sequence number gaps
- **Out of Order**: Packets received out of sequence
- **Duplicates**: Duplicate packets received
- **Loss Percentage**: Calculated packet loss rate

## Platform Support

**TCP Statistics - Linux Only**: TCP statistics collection uses the `TCP_INFO` socket option, which is Linux-specific. On macOS and other platforms, TCP statistics will show as `N/A` but the tool will continue to function normally.

**UDP Statistics - All Platforms**: UDP packet loss detection works on all platforms by embedding sequence numbers in the payload (first 4 bytes).

## Server Statistics

### Per-Connection Display

The server displays network statistics for each connection in the summary table:

**TCP Connections:**
```
  ID | Client                 | Duration   | Bytes     | Messages | PPS (recv) | TCP Stats (retx/rtt/lost)
  ------------------------------------------------------------------------------------------------
     1 | 127.0.0.1:54321       |     2.345s |    1024B |     5 msg |     2.1 pkt/s | retx=  0 rtt=  0.5ms lost=  0
     2 | 127.0.0.1:54322       |     3.120s |    2048B |    10 msg |     3.2 pkt/s | retx=  2 rtt=  1.2ms lost=  0
```

**UDP Connections:**
```
  ID | Client                 | Duration   | Bytes     | Messages | PPS (recv) | UDP Stats (lost/ooo/dup)
  ------------------------------------------------------------------------------------------------
     1 | 127.0.0.1:54321       |     5.123s |    5120B |   100 msg |    19.5 pkt/s | lost=  2 ( 2.0%) ooo=  1
     2 | 127.0.0.1:54322       |     5.089s |    5120B |    98 msg |    19.3 pkt/s | lost=  4 ( 3.9%)
```

### Disconnect Messages

TCP statistics are included in disconnect log messages:

```
[2024-03-04 01:23:45.123] [     1] TCP disconnect | 127.0.0.1:54321 | dur=2.345s | 1024B | retx=0 rtt=0.5ms lost=0
```

### Summary Statistics

The server summary includes aggregate network statistics:

**TCP Summary:**
```
TCP Statistics Summary:
  Total retransmits : 5
  Total lost packets: 0
  RTT avg           : 0.85ms
  RTT min           : 0.45ms
  RTT max           : 1.50ms
```

**UDP Summary:**
```
UDP Statistics Summary:
  Total lost packets: 12 (2.35%)
  Out of order      : 3
  Duplicates        : 0
```

## Client Statistics

### Per-Connection Display

File transfer operations now show TCP statistics:

```
[     1] TCP file xfer  | latency=1.2ms total=150ms 1048576B | checksum=PASS | retx=0 rtt=0.8ms
```

### Summary Statistics

The client summary includes TCP statistics across all connections:

```
CLIENT SUMMARY
=======================================================
  Elapsed time      : 10.50s
  Total connections : 100
  Successful        : 100
  Failed            : 0
  Observed rate     : 9.52 conn/s
  Total packets sent: 500
  Latency avg       : 1.25ms
  Latency min       : 0.80ms
  Latency max       : 2.50ms

  TCP Statistics:
    Total retransmits : 12
    Avg retransmits   : 0.1 per conn
    RTT avg           : 1.15ms
    RTT min           : 0.75ms
    RTT max           : 2.20ms
    Total lost packets: 0
=======================================================
```

## Implementation Details

### TCP_INFO Structure (Linux)

The implementation reads the Linux kernel's `tcp_info` structure using the `TCP_INFO` socket option. Key fields extracted:

- **Offset 5**: `tcpi_retransmits` (u8) - Number of retransmissions
- **Offset 32**: `tcpi_rtt` (u32) - Smoothed RTT in microseconds
- **Offset 36**: `tcpi_rttvar` (u32) - RTT variance in microseconds
- **Offset 52**: `tcpi_snd_cwnd` (u32) - Send congestion window
- **Offset 72**: `tcpi_lost` (u32) - Lost packets
- **Offset 88**: `tcpi_reordering` (u32) - Reordering metric

### UDP Sequence Number Protocol

UDP packets include a 4-byte sequence number header (network byte order, uint32):

```
[4-byte seq num][original payload]
```

The server tracks:
- **Expected sequence**: Next expected sequence number
- **Lost packets**: Detected when seq_num > expected (gap in sequence)
- **Out of order**: Packets arriving with seq_num < expected (within reasonable window)
- **Duplicates**: Very old sequence numbers (outside reasonable window)

### Data Collection Timing

Statistics are collected:
- **TCP Server**: Just before closing each connection (in the `finally` block)
- **TCP Client**: Just before closing each connection, after all data transmission
- **UDP Server**: On each datagram received, with final stats on session expiry
- **UDP Client**: Sequence numbers embedded in each packet sent

This ensures we capture the final state of connections including any retransmissions or packet loss that occurred during the connection lifetime.

## Usage Examples

### Basic TCP Connection Test

```bash
# Server
python3 server.py --port 9000

# Client
python3 client.py --port 9000 --cps 10 --total 100
```

### Long-lived Connections with Traffic

```bash
# Server
python3 server.py --port 9000

# Client - sustained traffic to observe TCP behavior
python3 client.py --port 9000 --cps 5 --duration 10 --pps 100 --total 20
```

### File Transfer with Statistics

```bash
# Server
python3 server.py --port 9000

# Client - transfer large file to observe retransmissions
python3 client.py --port 9000 --cps 2 --total 10 -F /path/to/large/file.bin
```

### UDP with Packet Loss Detection

```bash
# Server
python3 server.py --port 9001 --protocol udp

# Client - sustained UDP traffic to observe packet loss
python3 client.py --port 9001 --protocol udp --cps 5 --duration 10 --pps 100 --total 20
```

### Automated Testing

Use the provided test script:

```bash
cd tgen
./test_tcp_stats.sh
```

This script runs multiple test scenarios and displays the TCP statistics collected.

## Interpreting the Statistics

### Retransmits

- **0 retransmits**: Ideal - no packet loss or reordering
- **1-5 retransmits**: Normal for longer connections or congested networks
- **>10 retransmits**: May indicate network issues, congestion, or packet loss

### RTT (Round-Trip Time)

- **<1ms**: Excellent - local network or same datacenter
- **1-10ms**: Good - regional network
- **10-50ms**: Acceptable - cross-region
- **>50ms**: High latency - may impact performance

### Lost Packets

- **0 lost**: Ideal
- **>0 lost**: Indicates packet loss in the network path

### Congestion Window

- Higher values indicate better throughput potential
- Low values may indicate congestion or slow start phase

## Limitations

1. **Linux Only**: TCP_INFO is not available on macOS, Windows, or other platforms
2. **Snapshot in Time**: Statistics represent the state at connection close
3. **Kernel Version**: Field offsets may vary slightly between kernel versions (tested on modern kernels)

## Troubleshooting

### Statistics Show as N/A

- **Cause**: Running on non-Linux platform
- **Solution**: Run on Linux for TCP statistics, or accept N/A values

### All Statistics are Zero

- **Cause**: Very short-lived connections or local loopback
- **Solution**: Use longer duration connections or test over real network

### Permission Errors

- **Cause**: Some TCP_INFO fields may require elevated privileges
- **Solution**: Run with appropriate permissions or accept partial statistics

## Future Enhancements

Potential additions:
- UDP packet loss estimation (via sequence numbers)
- Bandwidth utilization metrics
- Jitter measurements
- Per-packet timing histograms
- Export to JSON/CSV for analysis