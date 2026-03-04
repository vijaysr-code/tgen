#!/bin/bash
# Comprehensive test script for client.py - tests all available options

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test counter
TEST_NUM=0

# Function to print test header
print_test() {
    TEST_NUM=$((TEST_NUM + 1))
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}TEST $TEST_NUM: $1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Function to run test
run_test() {
    echo -e "${GREEN}Running: $@${NC}"
    "$@"
    echo -e "${GREEN}✓ Test completed${NC}"
    sleep 1
}

# Create test directory
TEST_DIR="test_output"
mkdir -p "$TEST_DIR"

# Create a small test file for file transfer tests
TEST_FILE="$TEST_DIR/test_file.bin"
dd if=/dev/urandom of="$TEST_FILE" bs=1024 count=10 2>/dev/null
echo "Created test file: $TEST_FILE (10 KB)"

# Start server in background
echo -e "\n${BLUE}Starting server on port 9000...${NC}"
python3 server.py --port 9000 --protocol tcp --output "$TEST_DIR/server_tcp.log" &
SERVER_TCP_PID=$!
sleep 2

echo -e "${BLUE}Starting server on port 9001 for UDP...${NC}"
python3 server.py --port 9001 --protocol udp --output "$TEST_DIR/server_udp.log" &
SERVER_UDP_PID=$!
sleep 2

# Trap to cleanup on exit
cleanup() {
    echo -e "\n${BLUE}Cleaning up...${NC}"
    kill $SERVER_TCP_PID 2>/dev/null || true
    kill $SERVER_UDP_PID 2>/dev/null || true
    wait 2>/dev/null || true
    echo -e "${GREEN}Cleanup complete${NC}"
}
trap cleanup EXIT INT TERM

# ============================================================================
# BASIC TESTS
# ============================================================================

print_test "Basic TCP connection (default settings)"
run_test python3 client.py --port 9000 --total 5

print_test "Basic UDP connection"
run_test python3 client.py --port 9001 --protocol udp --total 5

# ============================================================================
# HOST AND PORT TESTS
# ============================================================================

print_test "Custom host (localhost)"
run_test python3 client.py --host localhost --port 9000 --total 3

print_test "Custom host (127.0.0.1)"
run_test python3 client.py --host 127.0.0.1 --port 9000 --total 3

# ============================================================================
# CPS (CONNECTIONS PER SECOND) TESTS
# ============================================================================

print_test "High CPS (10 connections/sec)"
run_test python3 client.py --port 9000 --cps 10 --total 20

print_test "Low CPS (0.5 connections/sec)"
run_test python3 client.py --port 9000 --cps 0.5 --total 3

print_test "Very high CPS (50 connections/sec)"
run_test python3 client.py --port 9000 --cps 50 --total 100

# ============================================================================
# DURATION TESTS
# ============================================================================

print_test "Long-lived connections (2 seconds)"
run_test python3 client.py --port 9000 --duration 2 --total 3

print_test "Long-lived connections (5 seconds)"
run_test python3 client.py --port 9000 --duration 5 --total 2

print_test "Short-lived connections (duration=0, default)"
run_test python3 client.py --port 9000 --duration 0 --total 5

# ============================================================================
# PAYLOAD TESTS
# ============================================================================

print_test "Custom payload string"
run_test python3 client.py --port 9000 --payload "Hello World" --total 5

print_test "Small random payload (100 bytes)"
run_test python3 client.py --port 9000 --payload-size 100 --total 5

print_test "Medium random payload (1 KB)"
run_test python3 client.py --port 9000 --payload-size 1024 --total 5

print_test "Large random payload (10 KB)"
run_test python3 client.py --port 9000 --payload-size 10240 --total 3

# ============================================================================
# PPS (PACKETS PER SECOND) TESTS
# ============================================================================

print_test "PPS with duration (10 pps for 3 seconds)"
run_test python3 client.py --port 9000 --duration 3 --pps 10 --total 2

print_test "High PPS (100 pps for 2 seconds)"
run_test python3 client.py --port 9000 --duration 2 --pps 100 --total 2

print_test "UDP with PPS (50 pps for 2 seconds)"
run_test python3 client.py --port 9001 --protocol udp --duration 2 --pps 50 --total 2

# ============================================================================
# FILE TRANSFER TESTS
# ============================================================================

print_test "File transfer (TCP only)"
run_test python3 client.py --port 9000 --file "$TEST_FILE" --total 3

print_test "File transfer with custom CPS"
run_test python3 client.py --port 9000 --file "$TEST_FILE" --cps 2 --total 5

# ============================================================================
# OUTPUT LOGGING TESTS
# ============================================================================

print_test "Output to file"
run_test python3 client.py --port 9000 --total 5 --output "$TEST_DIR/client_output.log"

print_test "Output to file with UDP"
run_test python3 client.py --port 9001 --protocol udp --total 5 --output "$TEST_DIR/client_udp_output.log"

# ============================================================================
# COMBINED OPTIONS TESTS
# ============================================================================

print_test "Combined: TCP + CPS + Duration + Payload"
run_test python3 client.py --port 9000 --cps 5 --duration 2 --payload "TEST" --total 10

print_test "Combined: UDP + CPS + Duration + Payload-size"
run_test python3 client.py --port 9001 --protocol udp --cps 3 --duration 1 --payload-size 512 --total 6

print_test "Combined: TCP + Duration + PPS + Payload-size"
run_test python3 client.py --port 9000 --duration 3 --pps 20 --payload-size 256 --total 3

print_test "Combined: TCP + High CPS + Custom payload + Output"
run_test python3 client.py --port 9000 --cps 20 --payload "STRESS_TEST" --total 50 --output "$TEST_DIR/stress_test.log"

# ============================================================================
# EDGE CASES AND LIMITS
# ============================================================================

print_test "Single connection (total=1)"
run_test python3 client.py --port 9000 --total 1

print_test "Very small payload (1 byte)"
run_test python3 client.py --port 9000 --payload-size 1 --total 3

print_test "Empty payload string"
run_test python3 client.py --port 9000 --payload "" --total 3

print_test "Very short duration (0.1 seconds)"
run_test python3 client.py --port 9000 --duration 0.1 --total 3

print_test "Low PPS (1 pps for 3 seconds)"
run_test python3 client.py --port 9000 --duration 3 --pps 1 --total 2

# ============================================================================
# PROTOCOL-SPECIFIC TESTS
# ============================================================================

print_test "TCP with all keepalive features"
run_test python3 client.py --port 9000 --protocol tcp --duration 5 --total 2

print_test "UDP burst test (high CPS)"
run_test python3 client.py --port 9001 --protocol udp --cps 100 --total 200

# ============================================================================
# SUMMARY
# ============================================================================

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}ALL TESTS COMPLETED SUCCESSFULLY!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Total tests run: $TEST_NUM"
echo -e "\nTest outputs saved in: $TEST_DIR/"
echo -e "  - server_tcp.log: TCP server log"
echo -e "  - server_udp.log: UDP server log"
echo -e "  - client_output.log: Client output test"
echo -e "  - client_udp_output.log: Client UDP output test"
echo -e "  - stress_test.log: Stress test output"
echo -e "  - test_file.bin: Test file for transfers"

# Verify log files were created
echo -e "\n${BLUE}Verifying output files...${NC}"
for logfile in "$TEST_DIR"/*.log; do
    if [ -f "$logfile" ]; then
        size=$(wc -l < "$logfile")
        echo -e "${GREEN}✓${NC} $logfile ($size lines)"
    fi
done

echo -e "\n${GREEN}Test suite complete!${NC}"

# Made with Bob
