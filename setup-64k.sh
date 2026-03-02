#!/bin/bash
# Setup script for 64K connection scaling
# Run with sudo: sudo ./setup-64k.sh

set -e

echo "=========================================="
echo "Setting up system for 64K connections"
echo "=========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "ERROR: Please run as root (use sudo)"
    exit 1
fi

# Backup existing configuration
BACKUP_DIR="/etc/tgen-backup-$(date +%Y%m%d-%H%M%S)"
echo "Creating backup directory: $BACKUP_DIR"
mkdir -p "$BACKUP_DIR"

# ============================================
# 1. File Descriptor Limits
# ============================================
echo ""
echo "1. Configuring file descriptor limits..."

if [ -f /etc/security/limits.conf ]; then
    cp /etc/security/limits.conf "$BACKUP_DIR/"
fi

# Remove old tgen entries if they exist
sed -i '/# tgen 64K setup/d' /etc/security/limits.conf
sed -i '/\* soft nofile 100000/d' /etc/security/limits.conf
sed -i '/\* hard nofile 100000/d' /etc/security/limits.conf

# Add new entries
cat >> /etc/security/limits.conf << 'EOF'
# tgen 64K setup
* soft nofile 100000
* hard nofile 100000
EOF

echo "   ✓ File descriptor limits set to 100000"

# ============================================
# 2. System-wide file descriptor limit
# ============================================
echo ""
echo "2. Configuring system-wide file descriptor limit..."

if [ -f /etc/sysctl.conf ]; then
    cp /etc/sysctl.conf "$BACKUP_DIR/"
fi

# Set system-wide limit
if ! grep -q "fs.file-max" /etc/sysctl.conf; then
    echo "fs.file-max = 200000" >> /etc/sysctl.conf
fi

echo "   ✓ System-wide file descriptor limit set"

# ============================================
# 3. Network Configuration
# ============================================
echo ""
echo "3. Applying network configuration..."

# Copy sysctl configuration
SYSCTL_FILE="/etc/sysctl.d/99-tgen-64k.conf"
if [ -f "$SYSCTL_FILE" ]; then
    cp "$SYSCTL_FILE" "$BACKUP_DIR/"
fi

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -f "$SCRIPT_DIR/sysctl-64k.conf" ]; then
    cp "$SCRIPT_DIR/sysctl-64k.conf" "$SYSCTL_FILE"
    echo "   ✓ Copied sysctl configuration to $SYSCTL_FILE"
else
    echo "   ⚠ Warning: sysctl-64k.conf not found in $SCRIPT_DIR"
    echo "   Please ensure sysctl-64k.conf is in the same directory as this script"
fi

# Apply sysctl settings
echo ""
echo "4. Applying sysctl settings..."
sysctl -p "$SYSCTL_FILE" 2>&1 | grep -v "cannot stat" || true
echo "   ✓ Sysctl settings applied"

# ============================================
# 5. Load nf_conntrack module (if needed)
# ============================================
echo ""
echo "5. Checking connection tracking module..."

if lsmod | grep -q nf_conntrack; then
    echo "   ✓ nf_conntrack module already loaded"
else
    echo "   Loading nf_conntrack module..."
    modprobe nf_conntrack || echo "   ⚠ Could not load nf_conntrack (may not be needed)"
fi

# ============================================
# 6. Verify Configuration
# ============================================
echo ""
echo "=========================================="
echo "Verifying Configuration"
echo "=========================================="
echo ""

echo "File Descriptors:"
echo "  System limit: $(cat /proc/sys/fs/file-max)"
echo "  User soft limit: $(ulimit -Sn)"
echo "  User hard limit: $(ulimit -Hn)"
echo ""

echo "Ephemeral Port Range:"
echo "  $(cat /proc/sys/net/ipv4/ip_local_port_range)"
echo ""

echo "Connection Tracking:"
if [ -f /proc/sys/net/netfilter/nf_conntrack_max ]; then
    echo "  Max connections: $(cat /proc/sys/net/netfilter/nf_conntrack_max)"
else
    echo "  Connection tracking not available (not needed for basic testing)"
fi
echo ""

echo "TCP Settings:"
echo "  tcp_tw_reuse: $(cat /proc/sys/net/ipv4/tcp_tw_reuse)"
echo "  tcp_fin_timeout: $(cat /proc/sys/net/ipv4/tcp_fin_timeout)"
echo "  tcp_max_syn_backlog: $(cat /proc/sys/net/ipv4/tcp_max_syn_backlog)"
echo ""

# ============================================
# 7. Additional Recommendations
# ============================================
echo "=========================================="
echo "Additional Recommendations"
echo "=========================================="
echo ""
echo "1. REBOOT REQUIRED for file descriptor limits to take effect"
echo "   Or log out and log back in"
echo ""
echo "2. For multiple IP addresses (to bypass port exhaustion):"
echo "   sudo ip addr add 192.168.1.101/24 dev eth0"
echo "   sudo ip addr add 192.168.1.102/24 dev eth0"
echo "   (Repeat for as many IPs as needed)"
echo ""
echo "3. Monitor system during testing:"
echo "   watch -n 1 'ss -s'"
echo "   watch -n 1 'lsof -p \$(pgrep -f client.py) | wc -l'"
echo ""
echo "4. Backup created at: $BACKUP_DIR"
echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="

# Made with Bob
