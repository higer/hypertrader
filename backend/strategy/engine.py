"""
Strategy engine – orchestrates all strategies, collects signals,
passes them through risk management, and emits execution commands.
"""

import asyncio
import pandas as pd
from typing import List, Dict, Optional
from loguru import logger

from config import SystemConfig
from data.collector import DataCollector
from strategy.base import Signal, ema, rsi, atr, bollinger, donchian, volume_sma
from strategy.momentum import MomentumStrategy
from strategy.mean_reversion import MeanReversionStrategy
from strategy.breakout import BreakoutStrategy
from strategy.funding_arb import FundingArbStrategy
from strategy.risk_manager import RiskManager


class StrategyEngine:

    def __init__(self, cfg: SystemConfig, collector: DataCollector,
                 risk: RiskManager):
        self.cfg = cfg
        self.collector = collector
        self.risk = risk

        self.strategies = []
        if cfg.momentum.enabled:
            self.strategies.append(MomentumStrategy(cfg.momentum))
        if cfg.mean_reversion.enabled:
            self.strategies.append(MeanReversionStrategy(cfg.mean_reversion))
        if cfg.breakout.enabled:
            self.strategies.append(BreakoutStrategy(cfg.breakout))
        if cfg.funding_arb.enabled:
            self.strategies.append(FundingArbStrategy(cfg.funding_arb))

        logger.info(f"Engine initialised with {len(self.strategies)} strategies: "
                     f"{[s.name for s in self.strategies]}")

    def scan(self) -> List[Signal]:
        """Run all strategies over the full universe; return merged signals."""
        signals: List[Signal] = []
        for coin in self.collector.universe:
            df = self.collector.get_df(coin)
            funding = self.collector.get_funding(coin)
            if df.empty:
                continue
            for strat in self.strategies:
                try:
                    sig = strat.evaluate(coin, df, funding)
                    if sig:
                        signals.append(sig)
                except Exception as e:
                    logger.error(f"Strategy {strat.name} error on {coin}: {e}")
        # Rank by strength descending
        signals.sort(key=lambda s: s.strength, reverse=True)
        return signals

    def process_signals(self, signals: List[Signal]) -> List[dict]:
        """Filter through risk manager, return execution commands."""
        commands = []
        for sig in signals:
            pos = self.risk.open_position(sig)
            if pos:
                commands.append({
                    "action": "open",
                    "coin": pos.coin,
                    "direction": pos.direction,
                    "size_usd": pos.size_usd,
                    "leverage": self.cfg.risk.leverage,
                    "entry_price": pos.entry_price,
                    "stop_loss": pos.stop_loss,
                    "take_profit": pos.take_profit,
                    "strategy": pos.strategy,
                })
        return commands

    def check_exits(self) -> List[dict]:
        """Check current prices against open positions."""
        prices = {}
        for coin in self.collector.universe:
            df = self.collector.get_df(coin)
            if not df.empty:
                prices[coin] = df["close"].iloc[-1]
        exits = self.risk.check_exits(prices)
        commands = []
        for ex in exits:
            commands.append({
                "action": "close",
                "coin": ex["coin"],
                "direction": ex["direction"],
                "reason": ex["reason"],
                "pnl_pct": ex["pnl_pct"],
                "pnl_usd": ex["pnl_usd"],
                # Preserve position data for rollback if ACP fails
                "entry": ex.get("entry", 0),
                "size_usd": ex.get("size_usd", 0),
                "stop_loss": ex.get("stop_loss", 0),
                "take_profit": ex.get("take_profit", 0),
                "strategy": ex.get("strategy", ""),
            })
        return commands

    async def tick(self) -> Dict:
        """One full evaluation cycle."""
        await self.collector.refresh()
        # Check exits first
        exit_cmds = self.check_exits()
        # Then scan for new entries
        signals = self.scan()
        entry_cmds = self.process_signals(signals)

        # Serialise raw signals for logging
        all_signals = [
            {"coin": s.coin, "direction": s.direction, "strategy": s.strategy,
             "strength": s.strength, "entry": s.entry_price,
             "sl": s.stop_loss, "tp": s.take_profit, "meta": s.meta}
            for s in signals
        ]

        return {
            "signals_found": len(signals),
            "all_signals": all_signals,
            "entries": entry_cmds,
            "exits": exit_cmds,
            "stats": self.risk.stats(),
        }

    def market_scan(self) -> List[dict]:
        """Compute indicator proximity for every coin — how close to triggering."""
        results = []
        for coin in self.collector.universe:
            df = self.collector.get_df(coin)
            funding_data = self.collector.get_funding(coin)
            if df.empty or len(df) < 30:
                continue

            close = df["close"]
            price = close.iloc[-1]
            _rsi_val = rsi(close, 14).iloc[-1]
            _atr_val = atr(df, 14).iloc[-1]
            fast_e = ema(close, 8).iloc[-1]
            slow_e = ema(close, 21).iloc[-1]
            mid, upper, lower = bollinger(close, 20, self.cfg.mean_reversion.bb_std)
            vol_avg = volume_sma(df, 20).iloc[-1]
            cur_vol = df["volume"].iloc[-1]
            vol_ratio = cur_vol / vol_avg if vol_avg > 0 else 0

            # EMA spread
            ema_spread_pct = (fast_e - slow_e) / price * 100 if price > 0 else 0

            # BB position (0 = at lower, 1 = at upper)
            bb_w = upper.iloc[-1] - lower.iloc[-1]
            bb_pos = (price - lower.iloc[-1]) / bb_w if bb_w > 0 else 0.5

            # Donchian proximity
            don_u, don_l = donchian(df, self.cfg.breakout.lookback)
            don_upper_v = don_u.iloc[-2] if len(don_u) > 1 else price
            don_lower_v = don_l.iloc[-2] if len(don_l) > 1 else price
            don_width = don_upper_v - don_lower_v
            don_pos = (price - don_lower_v) / don_width if don_width > 0 else 0.5

            # Funding rate
            fr = funding_data.get("funding", 0) if funding_data else 0

            # Compute proximity scores for each strategy
            # Momentum: how close to EMA crossover + RSI + volume
            mom_score = 0
            if fast_e > slow_e and _rsi_val > 50:
                mom_score = min(1.0, 0.3 + abs(ema_spread_pct) * 0.1 + (vol_ratio - 1) * 0.2)
            elif fast_e < slow_e and _rsi_val < 50:
                mom_score = min(1.0, 0.3 + abs(ema_spread_pct) * 0.1 + (vol_ratio - 1) * 0.2)

            # Mean reversion: how close to band touch + RSI extreme
            mr_score = 0
            if bb_pos < 0.2 and _rsi_val < 40:
                mr_score = min(1.0, (0.2 - bb_pos) * 3 + (40 - _rsi_val) / 40)
            elif bb_pos > 0.8 and _rsi_val > 60:
                mr_score = min(1.0, (bb_pos - 0.8) * 3 + (_rsi_val - 60) / 40)

            # Determine bias
            bias = "neutral"
            if _rsi_val > 55 and fast_e > slow_e:
                bias = "bullish"
            elif _rsi_val < 45 and fast_e < slow_e:
                bias = "bearish"

            results.append({
                "coin": coin,
                "price": round(price, 6),
                "rsi": round(float(_rsi_val), 1) if not pd.isna(_rsi_val) else 50,
                "ema_spread": round(ema_spread_pct, 3),
                "bb_position": round(float(bb_pos), 3),
                "donchian_position": round(float(don_pos), 3),
                "vol_ratio": round(float(vol_ratio), 2),
                "funding_rate": round(float(fr) * 100, 4) if fr else 0,
                "atr_pct": round(float(_atr_val) / price * 100, 3) if price > 0 and not pd.isna(_atr_val) else 0,
                "momentum_proximity": round(max(0, mom_score), 2),
                "mean_rev_proximity": round(max(0, mr_score), 2),
                "bias": bias,
            })

        results.sort(key=lambda x: max(x["momentum_proximity"], x["mean_rev_proximity"]), reverse=True)
        return results
