"""
Hyperliquid market-data collector.
- REST polling for snapshots (meta, candles, funding, open-interest)
- WebSocket streaming for real-time trades / L2 book
Filters for top-N market-cap / volume perpetual contracts.
"""

import asyncio, time, json
from typing import Dict, List, Optional
import aiohttp
import numpy as np
import pandas as pd
from loguru import logger

from config import (
    HYPERLIQUID_REST, HYPERLIQUID_WS, HYPERLIQUID_INFO,
    SystemConfig, DEFAULT_TOP_N, MIN_24H_VOLUME_USD,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _post(session: aiohttp.ClientSession, payload: dict) -> dict:
    async with session.post(HYPERLIQUID_INFO, json=payload) as r:
        r.raise_for_status()
        return await r.json()


# ---------------------------------------------------------------------------
# Market universe filter
# ---------------------------------------------------------------------------

class MarketFilter:
    """Select the top-N biggest perps by 24h volume × OI."""

    def __init__(self, top_n: int = DEFAULT_TOP_N):
        self.top_n = top_n
        self.universe: List[str] = []
        self._meta: List[dict] = []

    async def refresh(self, session: aiohttp.ClientSession):
        meta = await _post(session, {"type": "meta"})
        ctx  = await _post(session, {"type": "metaAndAssetCtxs"})

        assets = meta.get("universe", [])
        ctxs   = ctx[1] if isinstance(ctx, list) and len(ctx) > 1 else []

        scored = []
        for i, asset in enumerate(assets):
            if i >= len(ctxs):
                break
            c = ctxs[i]
            vol24  = float(c.get("dayNtlVlm", 0))
            oi     = float(c.get("openInterest", 0))
            mark   = float(c.get("markPx", 0))
            if vol24 < MIN_24H_VOLUME_USD:
                continue
            scored.append({
                "coin": asset["name"],
                "vol24": vol24,
                "oi": oi,
                "mark": mark,
                "score": vol24 * 0.6 + oi * mark * 0.4,
                "maxLeverage": asset.get("maxLeverage", 20),
            })

        scored.sort(key=lambda x: x["score"], reverse=True)
        self.universe = [s["coin"] for s in scored[: self.top_n]]
        self._meta = scored[: self.top_n]
        logger.info(f"Universe refreshed: {len(self.universe)} coins – {self.universe[:10]}…")
        return self._meta


# ---------------------------------------------------------------------------
# Candle / OHLCV data
# ---------------------------------------------------------------------------

INTERVAL_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}

class CandleStore:
    """Maintains rolling OHLCV DataFrames keyed by coin."""

    def __init__(self, interval: str = "15m", lookback: int = 200):
        self.interval = interval
        self.lookback = lookback
        self.candles: Dict[str, pd.DataFrame] = {}

    async def fetch(self, session: aiohttp.ClientSession, coin: str) -> pd.DataFrame:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - self.lookback * INTERVAL_MS[self.interval]
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": self.interval,
                    "startTime": start_ms, "endTime": end_ms},
        }
        raw = await _post(session, payload)
        if not raw:
            return pd.DataFrame()

        rows = []
        for c in raw:
            rows.append({
                "ts": int(c["t"]),
                "open": float(c["o"]),
                "high": float(c["h"]),
                "low": float(c["l"]),
                "close": float(c["c"]),
                "volume": float(c["v"]),
            })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        df.set_index("ts", inplace=True)
        df.sort_index(inplace=True)
        self.candles[coin] = df
        return df

    async def refresh_all(self, session: aiohttp.ClientSession, coins: List[str]):
        tasks = [self.fetch(session, c) for c in coins]
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Candles refreshed for {len(coins)} coins")


# ---------------------------------------------------------------------------
# Funding rate snapshot
# ---------------------------------------------------------------------------

class FundingCollector:
    """Collect predicted and current funding rates."""

    def __init__(self):
        self.rates: Dict[str, dict] = {}

    async def refresh(self, session: aiohttp.ClientSession, coins: List[str]):
        ctx = await _post(session, {"type": "metaAndAssetCtxs"})
        meta_universe = (await _post(session, {"type": "meta"})).get("universe", [])
        ctxs = ctx[1] if isinstance(ctx, list) and len(ctx) > 1 else []
        name_map = {a["name"]: i for i, a in enumerate(meta_universe)}
        for coin in coins:
            idx = name_map.get(coin)
            if idx is not None and idx < len(ctxs):
                c = ctxs[idx]
                self.rates[coin] = {
                    "funding": float(c.get("funding", 0)),
                    "premium": float(c.get("premium", 0)),
                    "openInterest": float(c.get("openInterest", 0)),
                    "markPx": float(c.get("markPx", 0)),
                }
        logger.debug(f"Funding rates refreshed for {len(self.rates)} coins")


# ---------------------------------------------------------------------------
# Real-time WebSocket stream (trades + L2)
# ---------------------------------------------------------------------------

class RealtimeStream:
    """WebSocket subscription manager for live data."""

    def __init__(self):
        self._ws = None
        self._running = False
        self.latest_trades: Dict[str, list] = {}
        self.latest_l2: Dict[str, dict] = {}

    async def connect(self, coins: List[str]):
        import websockets
        self._running = True
        try:
            async with websockets.connect(HYPERLIQUID_WS) as ws:
                self._ws = ws
                # subscribe trades
                for coin in coins[:15]:  # limit subscriptions
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": coin}
                    }))
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": coin}
                    }))
                logger.info(f"WS subscribed to {min(len(coins),15)} coins")
                async for msg in ws:
                    if not self._running:
                        break
                    data = json.loads(msg)
                    ch = data.get("channel")
                    payload = data.get("data", {})
                    if ch == "trades":
                        for t in payload:
                            coin = t.get("coin", "")
                            self.latest_trades.setdefault(coin, []).append(t)
                            # keep last 200
                            self.latest_trades[coin] = self.latest_trades[coin][-200:]
                    elif ch == "l2Book":
                        coin = payload.get("coin", "")
                        self.latest_l2[coin] = payload
        except Exception as e:
            logger.error(f"WS error: {e}")

    def stop(self):
        self._running = False


# ---------------------------------------------------------------------------
# Aggregated collector façade
# ---------------------------------------------------------------------------

class DataCollector:
    """Unified data layer used by the strategy engine."""

    def __init__(self, cfg: SystemConfig):
        self.cfg = cfg
        self.market_filter = MarketFilter(cfg.top_n)
        self.candle_store = CandleStore(cfg.candle_interval, cfg.data_lookback_bars)
        self.funding = FundingCollector()
        self.stream = RealtimeStream()
        self._session: Optional[aiohttp.ClientSession] = None

    async def init_session(self):
        self._session = aiohttp.ClientSession()

    async def close(self):
        if self._session:
            await self._session.close()

    @property
    def universe(self) -> List[str]:
        return self.market_filter.universe

    @property
    def meta(self) -> List[dict]:
        return self.market_filter._meta

    async def refresh(self):
        """Full data refresh cycle."""
        if not self._session:
            await self.init_session()
        await self.market_filter.refresh(self._session)
        coins = self.universe
        await asyncio.gather(
            self.candle_store.refresh_all(self._session, coins),
            self.funding.refresh(self._session, coins),
            return_exceptions=True,
        )
        logger.info("Full data refresh complete")

    def get_df(self, coin: str) -> pd.DataFrame:
        return self.candle_store.candles.get(coin, pd.DataFrame())

    def get_funding(self, coin: str) -> dict:
        return self.funding.rates.get(coin, {})
