#!/bin/bash
# Test script to verify dashboard integration

echo "=========================================="
echo "Testing Dashboard Integration"
echo "=========================================="
echo ""

# Check if aiohttp is installed
echo "1. Checking dependencies..."
python3 -c "import aiohttp" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "   ✓ aiohttp is installed"
else
    echo "   ✗ aiohttp is NOT installed"
    echo "   Install with: pip install aiohttp"
    exit 1
fi

# Check if metrics_reporter exists
if [ -f "metrics_reporter.py" ]; then
    echo "   ✓ metrics_reporter.py exists"
else
    echo "   ✗ metrics_reporter.py NOT found"
    exit 1
fi

# Check if dashboard.py exists
if [ -f "dashboard.py" ]; then
    echo "   ✓ dashboard.py exists"
else
    echo "   ✗ dashboard.py NOT found"
    exit 1
fi

echo ""
echo "2. Checking client.py for dashboard integration..."
if grep -q "from metrics_reporter import" client.py; then
    echo "   ✓ Client has metrics_reporter import"
else
    echo "   ✗ Client missing metrics_reporter import"
fi

if grep -q "\-\-dashboard" client.py; then
    echo "   ✓ Client has --dashboard argument"
else
    echo "   ✗ Client missing --dashboard argument"
fi

if grep -q "ClientMetricsTracker" client.py; then
    echo "   ✓ Client has ClientMetricsTracker"
else
    echo "   ✗ Client missing ClientMetricsTracker"
fi

echo ""
echo "3. Checking server.py for dashboard integration..."
if grep -q "from metrics_reporter import" server.py; then
    echo "   ✓ Server has metrics_reporter import"
else
    echo "   ✗ Server missing metrics_reporter import"
fi

if grep -q "\-\-dashboard" server.py; then
    echo "   ✓ Server has --dashboard argument"
else
    echo "   ✗ Server missing --dashboard argument"
fi

if grep -q "ServerMetricsTracker" server.py; then
    echo "   ✓ Server has ServerMetricsTracker"
else
    echo "   ✗ Server missing ServerMetricsTracker"
fi

echo ""
echo "4. Testing command-line help..."
echo "   Client help:"
python3 client.py --help 2>&1 | grep -q "dashboard"
if [ $? -eq 0 ]; then
    echo "   ✓ Client --help shows dashboard option"
else
    echo "   ✗ Client --help missing dashboard option"
fi

echo "   Server help:"
python3 server.py --help 2>&1 | grep -q "dashboard"
if [ $? -eq 0 ]; then
    echo "   ✓ Server --help shows dashboard option"
else
    echo "   ✗ Server --help missing dashboard option"
fi

echo ""
echo "=========================================="
echo "Integration Check Complete!"
echo "=========================================="
echo ""
echo "To test the dashboard:"
echo "1. Start dashboard:  python3 dashboard.py"
echo "2. Start server:     python3 server.py --port 9000 --dashboard http://localhost:8081"
echo "3. Start client:     python3 client.py --port 9000 --cps 10 --total 100 --dashboard http://localhost:8081"
echo "4. Open browser:     http://localhost:8080"

# Made with Bob
