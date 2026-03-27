"""
Main entry-point – FastAPI server.
Exposes REST API for the frontend dashboard and runs the autonomous
trading loop in the background.

Dry-run mode (default):
  - Fetches real market data from Hyperliquid
  - Runs all strategies and generates real signals
  - Simulates positions in-memory (paper trading)
  - Pushes signals + entries/exits to Telegram
  - Does NOT send orders to OpenClaw agent
"""

import asyncio, time, json, os, sys, traceback
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import SystemConfig
from data.collector import DataCollector
from strategy.engine import StrategyEngine
from strategy.risk_manager import RiskManager
from execution.openclaw_agent import OpenClawExecutor
from alerts.notifier import Notifier

# ---------------------------------------------------------------------------
CONFIG_PATH = "system_config.json"
state: dict = {}

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = SystemConfig.load(CONFIG_PATH)
    collector = DataCollector(cfg)
    risk = RiskManager(cfg.risk)
    engine = StrategyEngine(cfg, collector, risk)
    executor = OpenClawExecutor(cfg.openclaw)
    notifier = Notifier(cfg.alert)

    state["cfg"] = cfg
    state["collector"] = collector
    state["risk"] = risk
    state["engine"] = engine
    state["executor"] = executor
    state["notifier"] = notifier
    state["running"] = False
    state["last_tick"] = None
    state["tick_history"] = []
    state["ws_clients"] = set()
    state["signal_log"] = []       # all signals ever found
    state["tick_count"] = 0
    state["start_time"] = time.time()

    await collector.init_session()

    # -- Initial data fetch so the frontend has data immediately --
    logger.info("Performing initial data fetch from Hyperliquid...")
    try:
        await collector.refresh()
        logger.info(f"Initial fetch OK — {len(collector.universe)} coins loaded")
    except Exception as e:
        logger.warning(f"Initial fetch failed (will retry when started): {e}")

    mode = "DRY RUN (paper)" if cfg.dry_run else "LIVE (agent execution)"
    await notifier.on_system(f"HyperTrader started — mode: {mode}")
    logger.info(f"System ready — dry_run={cfg.dry_run}")

    yield

    state["running"] = False
    await collector.close()
    await executor.close()
    await notifier.close()


app = FastAPI(title="Hyperliquid Perps Trading System", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# ---------------------------------------------------------------------------
# WebSocket broadcast
# ---------------------------------------------------------------------------

async def broadcast(data: dict):
    dead = set()
    for ws in state.get("ws_clients", set()):
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    state["ws_clients"] -= dead

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    state["ws_clients"].add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        state["ws_clients"].discard(ws)

# ---------------------------------------------------------------------------
# Trading loop
# ---------------------------------------------------------------------------

async def trading_loop():
    engine: StrategyEngine = state["engine"]
    executor: OpenClawExecutor = state["executor"]
    notifier: Notifier = state["notifier"]
    cfg: SystemConfig = state["cfg"]

    while state.get("running"):
        try:
            result = await engine.tick()
            state["tick_count"] += 1
            state["last_tick"] = {**result, "ts": time.time()}
            state["tick_history"].append(state["last_tick"])
            if len(state["tick_history"]) > 500:
                state["tick_history"] = state["tick_history"][-500:]

            # Record all signals found
            for sig_data in result.get("all_signals", []):
                state["signal_log"].append({**sig_data, "ts": time.time()})
            if len(state["signal_log"]) > 2000:
                state["signal_log"] = state["signal_log"][-2000:]

            # -- Process entries --
            for cmd in result["entries"]:
                await notifier.on_entry(cmd, dry_run=cfg.dry_run)

            # -- Process exits --
            for cmd in result["exits"]:
                await notifier.on_exit(cmd, dry_run=cfg.dry_run)

            # -- If NOT dry_run, also send to Agent --
            if not cfg.dry_run:
                all_cmds = result["entries"] + result["exits"]
                if all_cmds:
                    exec_results = await executor.execute_commands(all_cmds)
                    for er in exec_results:
                        await notifier.on_order(er["cmd"], er["result"])

            # -- Broadcast to frontend --
            await broadcast({"type": "tick", "data": state["last_tick"]})

            # -- Periodic stats push (every 20 ticks) --
            if state["tick_count"] % 20 == 0:
                await notifier.on_stats_summary(result["stats"])

            logger.info(
                f"Tick #{state['tick_count']}: "
                f"universe={len(engine.collector.universe)} "
                f"signals={result['signals_found']} "
                f"entries={len(result['entries'])} "
                f"exits={len(result['exits'])} "
                f"equity=${result['stats'].get('equity', 0):,.0f}"
            )

        except Exception as e:
            logger.error(f"Trading loop error: {e}\n{traceback.format_exc()}")
            await notifier.on_system(f"Loop error: {e}")

        await asyncio.sleep(cfg.poll_interval_sec)


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@app.get("/")
@app.head("/")
async def index():
    fp = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(fp):
        return FileResponse(fp)
    return HTMLResponse("<h1>Hyperliquid Trading System API</h1>")


@app.get("/api/status")
async def get_status():
    return {
        "running": state.get("running", False),
        "dry_run": state["cfg"].dry_run,
        "stats": state["risk"].stats() if "risk" in state else {},
        "universe": state["collector"].universe if "collector" in state else [],
        "universe_meta": state["collector"].meta if "collector" in state else [],
        "last_tick": state.get("last_tick"),
        "tick_count": state.get("tick_count", 0),
        "uptime_sec": round(time.time() - state.get("start_time", time.time()), 1),
    }


@app.get("/api/agent/status")
async def agent_status():
    if state["cfg"].dry_run:
        return {"status": "dry_run", "message": "Agent disabled in dry-run mode", "connected": False}
    executor: OpenClawExecutor = state["executor"]
    return await executor.get_agent_status()


@app.post("/api/start")
async def start_trading():
    if state.get("running"):
        return {"message": "Already running"}
    state["running"] = True
    asyncio.create_task(trading_loop())
    mode = "DRY RUN" if state["cfg"].dry_run else "LIVE"
    await state["notifier"].on_system(f"Trading STARTED ({mode})")
    return {"message": f"Trading started ({mode})"}


@app.post("/api/stop")
async def stop_trading():
    state["running"] = False
    await state["notifier"].on_system("Trading STOPPED")
    return {"message": "Trading stopped"}


@app.get("/api/config")
async def get_config():
    return state["cfg"].model_dump()


class ConfigUpdate(BaseModel):
    config: dict

@app.post("/api/config")
async def update_config(body: ConfigUpdate):
    try:
        new_cfg = SystemConfig.model_validate(body.config)
        new_cfg.save(CONFIG_PATH)

        state["cfg"] = new_cfg
        state["collector"].cfg = new_cfg
        state["collector"].market_filter.top_n = new_cfg.top_n
        state["collector"].candle_store.interval = new_cfg.candle_interval
        state["collector"].candle_store.lookback = new_cfg.data_lookback_bars
        state["risk"].cfg = new_cfg.risk
        state["engine"] = StrategyEngine(new_cfg, state["collector"], state["risk"])
        state["executor"].cfg = new_cfg.openclaw
        state["executor"].base = new_cfg.openclaw.agent_endpoint.rstrip("/")
        # Update notifier config
        state["notifier"].cfg = new_cfg.alert
        state["notifier"]._tg_configured = bool(new_cfg.alert.telegram_bot_token and new_cfg.alert.telegram_chat_id)

        await state["notifier"].on_system("Config updated & hot-reloaded")
        return {"message": "Config updated", "config": new_cfg.model_dump()}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/positions")
async def get_positions():
    return {"positions": state["risk"].stats().get("positions", {})}


@app.get("/api/trades")
async def get_trades():
    return {"trades": state["risk"].closed_trades}


@app.get("/api/signals")
async def get_signals():
    """Run a scan without executing – preview mode."""
    engine: StrategyEngine = state["engine"]
    notifier: Notifier = state["notifier"]
    await state["collector"].refresh()
    signals = engine.scan()

    # Push batch to Telegram
    if signals:
        await notifier.on_signals_batch(signals, dry_run=state["cfg"].dry_run)

    return {"signals": [
        {"coin": s.coin, "direction": s.direction, "strategy": s.strategy,
         "strength": s.strength, "entry": s.entry_price,
         "sl": s.stop_loss, "tp": s.take_profit, "meta": s.meta}
        for s in signals
    ]}


@app.get("/api/market")
async def get_market():
    collector = state["collector"]
    # If no data yet, try a quick refresh
    if not collector.meta:
        try:
            await collector.market_filter.refresh(collector._session)
        except Exception:
            pass
    meta = collector.meta
    # Merge funding data
    for item in meta:
        coin = item.get("coin", "")
        fr = collector.get_funding(coin)
        item["funding"] = fr.get("funding", 0) if fr else 0
    return {"coins": meta}


@app.get("/api/candles/{coin}")
async def get_candles(coin: str):
    df = state["collector"].get_df(coin)
    if df.empty:
        return {"candles": []}
    records = df.reset_index().to_dict(orient="records")
    for r in records:
        r["ts"] = str(r["ts"])
    return {"candles": records}


@app.get("/api/funding")
async def get_funding():
    return {"rates": state["collector"].funding.rates}


@app.get("/api/order-log")
async def get_order_log():
    return {"orders": state["executor"].order_log[-100:]}


@app.get("/api/tick-history")
async def get_tick_history():
    return {"ticks": state.get("tick_history", [])[-100:]}


@app.get("/api/signal-log")
async def get_signal_log():
    return {"signals": state.get("signal_log", [])[-200:]}


@app.get("/api/scanner")
async def get_scanner():
    """Market proximity scanner — shows how close each coin is to triggering."""
    engine: StrategyEngine = state["engine"]
    return {"coins": engine.market_scan()}


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=False)
