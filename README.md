# HyperTrader

Autonomous cryptocurrency trading system for Hyperliquid perpetual futures.

## Features

- **4 trading strategies**: Momentum (EMA crossover), Mean Reversion (Bollinger Bands), Breakout (Donchian Channel), Funding Arbitrage
- **Risk management**: Kelly-lite sizing, drawdown circuit breaker, trailing stops, daily loss limits, per-coin cooldowns
- **Real-time dashboard**: Dark-themed trading terminal with live WebSocket updates
- **Market scanner**: Proximity analysis showing how close each coin is to triggering signals
- **Telegram alerts**: Rich HTML-formatted notifications for signals, entries, exits, and stats
- **Dry-run mode**: Paper trading with real market data (default)
- **Hot-reload config**: All strategy parameters configurable via dashboard without restart

## Quick Start

```bash
cd backend
pip install -r requirements.txt
cp .env.example .env   # Optional: add Telegram credentials
python3 main.py
```

Open `http://localhost:8080` in your browser.

## Architecture

```
backend/
  main.py              # FastAPI server + trading loop
  config.py            # Pydantic config schemas
  data/collector.py    # Hyperliquid API data collection
  strategy/
    base.py            # Indicators (EMA, RSI, ATR, BB, Donchian)
    engine.py          # Strategy orchestrator + market scanner
    momentum.py        # Trend-following strategy
    mean_reversion.py  # Bollinger Band reversion
    breakout.py        # Donchian channel breakout
    funding_arb.py     # Funding rate arbitrage
    risk_manager.py    # Position sizing + risk controls
  execution/
    openclaw_agent.py  # OpenClaw Agent integration (deferred)
  alerts/
    notifier.py        # Telegram + webhook notifications
frontend/
  index.html           # Single-page trading dashboard
```

## Deployment

```bash
# Docker
docker compose up -d

# PM2
pm2 start backend/main.py --name hypertrader --interpreter python3

# systemd
sudo cp hypertrader.service /etc/systemd/system/
sudo systemctl enable --now hypertrader
```

## Configuration

All parameters are configurable via the dashboard Configuration page or by editing `system_config.json`.

| Parameter | Default | Description |
|---|---|---|
| dry_run | true | Paper trading mode |
| top_n | 30 | Number of coins to monitor |
| candle_interval | 15m | Candle timeframe |
| poll_interval_sec | 15 | Trading loop interval |
| leverage | 3x | Default leverage |
| max_open_positions | 6 | Concurrent position limit |
| max_drawdown_pct | 8% | Drawdown circuit breaker |
