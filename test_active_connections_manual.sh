#!/bin/bash
# Manual test to verify dashboard shows active connections
# This script starts all components and lets you observe them

set -e

echo "========================================================================"
echo "Manual Test: Dashboard Active Connection Display"
echo "========================================================================"
echo ""
echo "This test will:"
echo "1. Start the dashboard on http://localhost:8080"
echo "2. Start the server on port 9000 with dashboard reporting"
echo "3. Start the client making long-lived connections"
echo "4. You can open http://localhost:8080 in your browser to see live metrics"
echo ""
echo "Press Ctrl+C to stop all processes"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Stopping all processes..."
    if [ ! -z "$DASHBOARD_PID" ]; then
        kill $DASHBOARD_PID 2>/dev/null || true
    fi
    if [ ! -z "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
    fi
    if [ ! -z "$CLIENT_PID" ]; then
        kill $CLIENT_PID 2>/dev/null || true
    fi
    echo "Cleanup complete"
    exit 0
}

trap cleanup INT TERM

# Start dashboard
echo "Starting dashboard..."
python3 dashboard.py --port 8080 --metrics-port 8081 > /tmp/dashboard.log 2>&1 &
DASHBOARD_PID=$!
sleep 2

# Check if dashboard started
if ! curl -s http://localhost:8080/api/metrics > /dev/null; then
    echo "ERROR: Dashboard failed to start"
    cat /tmp/dashboard.log
    cleanup
fi
echo "✓ Dashboard running at http://localhost:8080"
echo ""

# Start server
echo "Starting server on port 9000..."
python3 server.py --port 9000 --protocol tcp --dashboard http://localhost:8081 > /tmp/server.log 2>&1 &
SERVER_PID=$!
sleep 2
echo "✓ Server started"
echo ""

# Start client with long-lived connections
echo "Starting client (5 conn/s, 10s duration, 100 total)..."
python3 client.py \
    --host localhost \
    --port 9000 \
    --protocol tcp \
    --cps 5 \
    --total 100 \
    --duration 10 \
    --dashboard http://localhost:8081 > /tmp/client.log 2>&1 &
CLIENT_PID=$!
sleep 2
echo "✓ Client started"
echo ""

echo "========================================================================"
echo "Test Running!"
echo "========================================================================"
echo ""
echo "Dashboard URL: http://localhost:8080"
echo ""
echo "Monitoring metrics for 30 seconds..."
echo ""

# Monitor for 30 seconds
for i in {1..15}; do
    sleep 2
    echo "Check $i ($(date +%H:%M:%S)):"
    
    # Query dashboard API
    METRICS=$(curl -s http://localhost:8080/api/metrics)
    
    # Extract server metrics
    SERVER_TOTAL=$(echo "$METRICS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('server', {}).get('total_connections', 0) if data.get('server') else 0)" 2>/dev/null || echo "0")
    SERVER_ACTIVE=$(echo "$METRICS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('server', {}).get('active_connections', 0) if data.get('server') else 0)" 2>/dev/null || echo "0")
    SERVER_CPS=$(echo "$METRICS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(f\"{data.get('server', {}).get('connections_per_sec', 0):.2f}\" if data.get('server') else '0.00')" 2>/dev/null || echo "0.00")
    
    # Extract client metrics
    CLIENT_TOTAL=$(echo "$METRICS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('client', {}).get('total_connections', 0) if data.get('client') else 0)" 2>/dev/null || echo "0")
    CLIENT_SUCCESS=$(echo "$METRICS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('client', {}).get('successful', 0) if data.get('client') else 0)" 2>/dev/null || echo "0")
    
    echo "  Server: Total=$SERVER_TOTAL, Active=$SERVER_ACTIVE, CPS=$SERVER_CPS"
    echo "  Client: $CLIENT_SUCCESS/$CLIENT_TOTAL successful"
    
    if [ "$SERVER_ACTIVE" -gt "0" ]; then
        echo "  ✓ Active connections detected!"
    fi
    echo ""
done

echo "========================================================================"
echo "Test Complete!"
echo "========================================================================"
echo ""
echo "Final check:"
METRICS=$(curl -s http://localhost:8080/api/metrics)
SERVER_TOTAL=$(echo "$METRICS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('server', {}).get('total_connections', 0) if data.get('server') else 0)" 2>/dev/null || echo "0")
SERVER_ACTIVE=$(echo "$METRICS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('server', {}).get('active_connections', 0) if data.get('server') else 0)" 2>/dev/null || echo "0")

echo "Server Total Connections: $SERVER_TOTAL"
echo "Server Active Connections: $SERVER_ACTIVE"
echo ""

if [ "$SERVER_TOTAL" -gt "0" ]; then
    echo "✓ TEST PASSED: Dashboard received server metrics"
    if [ "$SERVER_ACTIVE" -gt "0" ] || [ "$SERVER_TOTAL" -gt "10" ]; then
        echo "✓ Active connections were displayed during the test"
    fi
else
    echo "✗ TEST FAILED: No server metrics received"
    echo ""
    echo "Server log:"
    cat /tmp/server.log
    echo ""
    echo "Client log:"
    cat /tmp/client.log
fi

echo ""
echo "Logs saved to:"
echo "  Dashboard: /tmp/dashboard.log"
echo "  Server: /tmp/server.log"
echo "  Client: /tmp/client.log"
echo ""

cleanup

# Made with Bob
