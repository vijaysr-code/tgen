#!/bin/bash
# Test script to verify enhanced TCP and UDP statistics collection

set -e

echo "=========================================="
echo "Testing Enhanced Network Statistics"
echo "=========================================="
echo ""

# Check if running on Linux (TCP_INFO is Linux-specific)
if [[ "$(uname)" != "Linux" ]]; then
    echo "WARNING: TCP statistics are only available on Linux"
    echo "UDP statistics work on all platforms"
    echo ""
fi

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $TCP_SERVER_PID 2>/dev/null || true
    kill $UDP_SERVER_PID 2>/dev/null || true
    wait $TCP_SERVER_PID 2>/dev/null || true
    wait $UDP_SERVER_PID 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

echo "=========================================="
echo "TCP TESTS"
echo "=========================================="
echo ""

# Start TCP server in background
echo "Starting TCP server on port 9999..."
python3 server.py --port 9999 --protocol tcp --output test_tcp_server.log &
TCP_SERVER_PID=$!
sleep 2
echo "TCP Server started (PID: $TCP_SERVER_PID)"
echo ""

# Test 1: Short-lived TCP connections
echo "Test 1: Short-lived TCP connections (10 connections at 5/sec)"
echo "--------------------------------------------------------------"
python3 client.py --host 127.0.0.1 --port 9999 --protocol tcp --cps 5 --total 10 --output test_tcp_client1.log
echo ""
sleep 1

# Test 2: Long-lived TCP connections with traffic
echo "Test 2: Long-lived TCP connections with sustained traffic"
echo "-----------------------------------------------------------"
python3 client.py --host 127.0.0.1 --port 9999 --protocol tcp --cps 2 --total 5 --duration 3 --pps 50 --output test_tcp_client2.log
echo ""
sleep 1

# Test 3: Large payload TCP connections
echo "Test 3: Large payload TCP connections (1MB payload)"
echo "----------------------------------------------------"
python3 client.py --host 127.0.0.1 --port 9999 --protocol tcp --cps 1 --total 3 --payload-size 1048576 --output test_tcp_client3.log
echo ""
sleep 1

# Stop TCP server gracefully
echo "Stopping TCP server..."
kill -INT $TCP_SERVER_PID
wait $TCP_SERVER_PID 2>/dev/null || true
echo ""

echo "=========================================="
echo "UDP TESTS"
echo "=========================================="
echo ""

# Start UDP server in background
echo "Starting UDP server on port 9998..."
python3 server.py --port 9998 --protocol udp --output test_udp_server.log &
UDP_SERVER_PID=$!
sleep 2
echo "UDP Server started (PID: $UDP_SERVER_PID)"
echo ""

# Test 4: Short-lived UDP connections
echo "Test 4: Short-lived UDP connections (10 connections at 5/sec)"
echo "--------------------------------------------------------------"
python3 client.py --host 127.0.0.1 --port 9998 --protocol udp --cps 5 --total 10 --output test_udp_client1.log
echo ""
sleep 6  # Wait for UDP sessions to expire (5s timeout + 1s buffer)

# Test 5: Long-lived UDP connections with sustained traffic
echo "Test 5: Long-lived UDP connections with sustained traffic"
echo "-----------------------------------------------------------"
python3 client.py --host 127.0.0.1 --port 9998 --protocol udp --cps 2 --total 5 --duration 3 --pps 100 --output test_udp_client2.log
echo ""
sleep 6  # Wait for UDP sessions to expire

# Test 6: High-rate UDP to observe packet loss
echo "Test 6: High-rate UDP traffic (500 pkt/s for 2s)"
echo "-------------------------------------------------"
python3 client.py --host 127.0.0.1 --port 9998 --protocol udp --cps 1 --total 3 --duration 2 --pps 500 --output test_udp_client3.log
echo ""
sleep 6  # Wait for UDP sessions to expire

# Stop UDP server gracefully
echo "Stopping UDP server..."
kill -INT $UDP_SERVER_PID
wait $UDP_SERVER_PID 2>/dev/null || true
echo ""

echo "=========================================="
echo "TEST RESULTS"
echo "=========================================="
echo ""
echo "TCP Server Summary (last 60 lines):"
echo "------------------------------------"
tail -60 test_tcp_server.log
echo ""
echo "UDP Server Summary (last 60 lines):"
echo "------------------------------------"
tail -60 test_udp_server.log
echo ""
echo "TCP Client Test 1 Summary:"
echo "--------------------------"
tail -20 test_tcp_client1.log
echo ""
echo "TCP Client Test 2 Summary:"
echo "--------------------------"
tail -20 test_tcp_client2.log
echo ""
echo "UDP Client Test 2 Summary:"
echo "--------------------------"
tail -20 test_udp_client2.log
echo ""
echo "UDP Client Test 3 Summary:"
echo "--------------------------"
tail -20 test_udp_client3.log
echo ""

if [[ "$(uname)" == "Linux" ]]; then
    echo "✓ All tests completed successfully!"
    echo "  - TCP statistics: retransmits, RTT, lost packets"
    echo "  - UDP statistics: lost packets, out-of-order, duplicates"
else
    echo "✓ All tests completed successfully!"
    echo "  - TCP statistics: N/A (Linux only)"
    echo "  - UDP statistics: lost packets, out-of-order, duplicates"
fi
echo ""
echo "Log files created:"
echo "  TCP: test_tcp_server.log, test_tcp_client1.log, test_tcp_client2.log, test_tcp_client3.log"
echo "  UDP: test_udp_server.log, test_udp_client1.log, test_udp_client2.log, test_udp_client3.log"

# Made with Bob
