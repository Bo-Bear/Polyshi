#!/usr/bin/env bash
set -euo pipefail

# ── Polyshi VPS Setup ──────────────────────────────────────────────
# Run this ON the EC2 instance after cloning the repo.
#
# Usage:
#   ssh ubuntu@<your-ec2-ip>
#   git clone <repo-url> ~/Polyshi
#   cd ~/Polyshi
#   bash setup-vps.sh
#
# After setup, copy your secrets:
#   scp .env ubuntu@<your-ec2-ip>:~/Polyshi/.env
#   scp kalshi-key.pem ubuntu@<your-ec2-ip>:~/Polyshi/kalshi-key.pem
#
# Then run the bot the same way as locally:
#   cd ~/Polyshi && source venv/bin/activate && python main.py
# ───────────────────────────────────────────────────────────────────

echo "==> Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-pip python3-venv git tmux

echo "==> Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo "==> Installing Python dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo "==> Creating logs directory..."
mkdir -p logs

# Check if .env exists
if [ ! -f .env ]; then
    echo ""
    echo "==> No .env file found."
    echo "    Copy your local .env and Kalshi .pem key to this machine:"
    echo ""
    echo "    scp .env ubuntu@\$(curl -s ifconfig.me):~/Polyshi/.env"
    echo "    scp <kalshi-key>.pem ubuntu@\$(curl -s ifconfig.me):~/Polyshi/"
    echo ""
    echo "    Or copy .env.example and fill it in:"
    echo "    cp .env.example .env && nano .env"
else
    echo "==> .env found"
fi

echo ""
echo "==> Setup complete. To run:"
echo "    cd ~/Polyshi"
echo "    ./start.sh              # launches in tmux (recommended)"
echo ""
echo "    Or without tmux:"
echo "    source venv/bin/activate"
echo "    python main.py"
