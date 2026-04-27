#!/bin/bash
# Debug script to check why client metrics aren't showing

set -e

echo "Killing any existing processes..."
pkill -9 -f "dashboard.py" 2>/dev/null || true
pkill -9 -f "server.py" 2>/dev/null || true
pkill -9 -f "client.py" 2>/dev/null || true
sleep 2

echo "Starting dashboard..."
python3 dashboard.py 2>&1 | tee /tmp/dash.log &
DASH_PID=$!
sleep 3

echo "Starting server..."
python3 server.py --port 9000 --dashboard http://localhost:8081 2>&1 | tee /tmp/srv.log &
SRV_PID=$!
sleep 2

echo "Starting client..."
python3 client.py --host localhost --port 9000 --cps 5 --total 10 --duration 3 --dashboard http://localhost:8081 2>&1 | tee /tmp/cli.log &
CLI_PID=$!

echo "Waiting 8 seconds for connections..."
sleep 8

echo ""
echo "=== Checking Dashboard Logs ==="
grep -i "debug\|received\|client" /tmp/dash.log | tail -20

echo ""
echo "=== Checking Server Logs ==="
grep -i "debug\|metrics" /tmp/srv.log | tail -10

echo ""
echo "=== Checking Client Logs ==="
grep -i "debug\|metrics" /tmp/cli.log | tail -10

echo ""
echo "=== Querying Dashboard API ==="
curl -s http://localhost:8080/api/metrics | python3 -m json.tool | grep -A 10 "client"

echo ""
echo "Cleaning up..."
kill $DASH_PID $SRV_PID $CLI_PID 2>/dev/null || true
sleep 1
pkill -9 -f "dashboard.py\|server.py\|client.py" 2>/dev/null || true

echo "Done!"

# Made with Bob
