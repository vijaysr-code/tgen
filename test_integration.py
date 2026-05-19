#!/usr/bin/env python3
"""
Quick integration test for dashboard functionality
"""
import subprocess
import time
import sys

def test_basic_functionality():
    """Test basic client/server without dashboard"""
    print("=" * 60)
    print("TEST 1: Basic Client/Server (no dashboard)")
    print("=" * 60)
    
    # Start server
    print("Starting server on port 9999...")
    server_proc = subprocess.Popen(
        ['python3', 'server.py', '--port', '9999', '--quiet'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(1)
    
    # Start client
    print("Starting client (10 connections at 5/sec)...")
    client_proc = subprocess.Popen(
        ['python3', 'client.py', '--port', '9999', '--cps', '5', '--total', '10'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait for client to finish
    try:
        stdout, stderr = client_proc.communicate(timeout=10)
        print("✓ Client completed successfully")
        
        # Check output
        output = stdout.decode()
        if 'Successful' in output and 'CLIENT SUMMARY' in output:
            print("✓ Client output looks good")
            return True
        else:
            print("✗ Client output unexpected")
            print(output[:500])
            return False
    except subprocess.TimeoutExpired:
        print("✗ Client timed out")
        client_proc.kill()
        return False
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=2)
        except:
            server_proc.kill()
    
    print()

def test_help_commands():
    """Test that help commands show dashboard option"""
    print("=" * 60)
    print("TEST 2: Help Commands")
    print("=" * 60)
    
    # Test client help
    print("Testing client --help...")
    result = subprocess.run(
        ['python3', 'client.py', '--help'],
        capture_output=True,
        text=True,
        timeout=5
    )
    if 'dashboard' in result.stdout.lower():
        print("✓ Client help shows dashboard option")
    else:
        print("✗ Client help missing dashboard option")
        return False
    
    # Test server help
    print("Testing server --help...")
    result = subprocess.run(
        ['python3', 'server.py', '--help'],
        capture_output=True,
        text=True,
        timeout=5
    )
    if 'dashboard' in result.stdout.lower():
        print("✓ Server help shows dashboard option")
    else:
        print("✗ Server help missing dashboard option")
        return False
    
    print()
    return True

def main():
    print("🚀 Testing Dashboard Integration")
    print()

    try:
        success = True
        success = test_help_commands() and success
        success = test_basic_functionality() and success
        
        print("=" * 60)
        if success:
            print("✅ ALL TESTS PASSED")
        else:
            print("❌ SOME TESTS FAILED")
        print("=" * 60)
        print()
        print("To manually test the dashboard:")
        print("1. python3 dashboard.py")
        print("2. python3 server.py --port 9000 --dashboard http://localhost:8081")
        print("3. python3 client.py --port 9000 --cps 10 --total 100 --dashboard http://localhost:8081")
        print("4. Open http://localhost:8080 in your browser")
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        return 1
    except Exception as e:
        print(f"Error during tests: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())

# Made with Bob
