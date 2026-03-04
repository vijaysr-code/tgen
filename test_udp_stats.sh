#!/bin/bash
# Test script to verify UDP statistics are correctly calculated

echo "Testing UDP per-connection statistics..."
echo "========================================="
echo ""

# Start server in background
echo "Starting UDP server on port 9001..."
python3 server.py --port 9001 --protocol udp --output server_udp_test.log &
SERVER_PID=$!
sleep 2

# Test 1: Send 100 packets at 50 pps for 2 seconds
echo "Test 1: Sending 100 packets at 50 pps (should have 0% loss in good network)"
python3 client.py --host 127.0.0.1 --port 9001 --protocol udp --cps 1 --total 1 --duration 2 --pps 50 --output client_udp_test.log

sleep 3

# Test 2: Multiple connections
echo ""
echo "Test 2: Multiple connections (5 connections, 20 packets each at 10 pps)"
python3 client.py --host 127.0.0.1 --port 9001 --protocol udp --cps 2 --total 5 --duration 2 --pps 10

sleep 3

# Stop server
echo ""
echo "Stopping server..."
kill -SIGINT $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo ""
echo "Test complete. Check server_udp_test.log for detailed results."
echo ""
echo "Expected results:"
echo "  - Loss should be 0% or very low (<1%) on localhost"
echo "  - Out-of-order packets may occur but should be minimal"
echo "  - No false positives for lost packets due to reordering"

# Made with Bob
