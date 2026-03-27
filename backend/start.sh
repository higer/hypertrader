#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

echo "========================================"
echo "  HyperTrader — Hyperliquid Perps"
echo "========================================"

# Check Python
if ! command -v python3 &>/dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

# Install deps if needed
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo "Installing dependencies..."
    pip install -r requirements.txt
fi

# Check .env
if [ ! -f .env ]; then
    echo ""
    echo "⚠  No .env file found."
    echo "   Copy .env.example to .env and fill in your Telegram bot credentials."
    echo "   cp .env.example .env"
    echo ""
    echo "   Starting without Telegram notifications..."
    echo ""
fi

echo ""
echo "Starting server on http://0.0.0.0:8080"
echo "Open the dashboard in your browser."
echo "Mode: DRY RUN (paper trading — no real orders)"
echo ""

python3 main.py
