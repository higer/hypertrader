"""
Momentum / Trend-following strategy.
Entry : Fast EMA crosses above Slow EMA + RSI confirms + volume surge.
Exit  : ATR-based trailing stop / take-profit, or RSI exhaustion.
"""

from typing import Optional
import pandas as pd
from .base import BaseStrategy, Signal, ema, rsi, atr, volume_sma
from config import MomentumConfig


class MomentumStrategy(BaseStrategy):
    name = "momentum"

    def __init__(self, cfg: MomentumConfig):
        self.c = cfg

    def evaluate(self, coin: str, df: pd.DataFrame, funding: dict) -> Optional[Signal]:
        if not self.c.enabled or len(df) < max(self.c.slow_ema, self.c.atr_period) + 5:
            return None

        close = df["close"]
        fast = ema(close, self.c.fast_ema)
        slow = ema(close, self.c.slow_ema)
        _rsi = rsi(close, self.c.rsi_period)
        _atr = atr(df, self.c.atr_period)
        vol_avg = volume_sma(df, 20)

        cur_fast, prev_fast = fast.iloc[-1], fast.iloc[-2]
        cur_slow, prev_slow = slow.iloc[-1], slow.iloc[-2]
        cur_rsi = _rsi.iloc[-1]
        cur_atr = _atr.iloc[-1]
        cur_vol = df["volume"].iloc[-1]
        avg_vol = vol_avg.iloc[-1]
        price = close.iloc[-1]

        if pd.isna(cur_atr) or cur_atr == 0:
            return None

        vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0
        vol_surge = vol_ratio > self.c.volume_surge_mult

        # EMA spread as % of price
        ema_spread = (cur_fast - cur_slow) / price if price > 0 else 0

        # --- LONG ---
        # Mode 1: Fresh crossover (classic)
        fresh_cross_long = (prev_fast <= prev_slow and cur_fast > cur_slow)
        # Mode 2: Trend continuation — fast above slow + RSI accelerating + volume
        trend_long = (cur_fast > cur_slow and ema_spread > 0.001
                      and cur_rsi > _rsi.iloc[-2] and cur_rsi > 52)

        if ((fresh_cross_long or trend_long)
                and cur_rsi > 50 and cur_rsi < self.c.rsi_ob and vol_surge):
            sl = price - self.c.atr_sl_mult * cur_atr
            tp = price + self.c.atr_tp_mult * cur_atr
            base = 0.35 if fresh_cross_long else 0.2
            strength = min(1.0, base + (cur_rsi - 50) / 30 * 0.3 + (vol_ratio - 1) * 0.15 + abs(ema_spread) * 10)
            return Signal(
                coin=coin, direction="long", strategy=self.name,
                strength=round(strength, 3),
                entry_price=price, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                meta={"rsi": round(cur_rsi, 2), "atr": round(cur_atr, 6),
                       "vol_ratio": round(vol_ratio, 2),
                       "ema_spread": round(ema_spread * 100, 3),
                       "mode": "crossover" if fresh_cross_long else "trend"},
            )

        # --- SHORT ---
        fresh_cross_short = (prev_fast >= prev_slow and cur_fast < cur_slow)
        trend_short = (cur_fast < cur_slow and ema_spread < -0.001
                       and cur_rsi < _rsi.iloc[-2] and cur_rsi < 48)

        if ((fresh_cross_short or trend_short)
                and cur_rsi < 50 and cur_rsi > self.c.rsi_os and vol_surge):
            sl = price + self.c.atr_sl_mult * cur_atr
            tp = price - self.c.atr_tp_mult * cur_atr
            base = 0.35 if fresh_cross_short else 0.2
            strength = min(1.0, base + (50 - cur_rsi) / 30 * 0.3 + (vol_ratio - 1) * 0.15 + abs(ema_spread) * 10)
            return Signal(
                coin=coin, direction="short", strategy=self.name,
                strength=round(strength, 3),
                entry_price=price, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                meta={"rsi": round(cur_rsi, 2), "atr": round(cur_atr, 6),
                       "vol_ratio": round(vol_ratio, 2),
                       "ema_spread": round(ema_spread * 100, 3),
                       "mode": "crossover" if fresh_cross_short else "trend"},
            )

        return None
