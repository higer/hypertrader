"""
Mean-Reversion strategy.
Entry : Price touches Bollinger Band + RSI extreme + no strong trend.
Exit  : Return to mid-band or time-based exit.
"""

from typing import Optional
import pandas as pd
from .base import BaseStrategy, Signal, rsi, atr, bollinger, ema
from config import MeanReversionConfig


class MeanReversionStrategy(BaseStrategy):
    name = "mean_reversion"

    def __init__(self, cfg: MeanReversionConfig):
        self.c = cfg

    def evaluate(self, coin: str, df: pd.DataFrame, funding: dict) -> Optional[Signal]:
        if not self.c.enabled or len(df) < self.c.bb_period + 5:
            return None

        close = df["close"]
        _rsi = rsi(close, self.c.rsi_period)
        _atr = atr(df, self.c.atr_period)
        mid, upper, lower = bollinger(close, self.c.bb_period, self.c.bb_std)

        price = close.iloc[-1]
        cur_rsi = _rsi.iloc[-1]
        cur_atr = _atr.iloc[-1]
        cur_mid = mid.iloc[-1]
        cur_upper = upper.iloc[-1]
        cur_lower = lower.iloc[-1]

        if pd.isna(cur_atr) or cur_atr == 0:
            return None

        # Reject if strong trend (EMA slope)
        slow_ema = ema(close, 50)
        if len(slow_ema) > 5:
            slope = (slow_ema.iloc[-1] - slow_ema.iloc[-5]) / slow_ema.iloc[-5]
            if abs(slope) > 0.04:  # allow moderate trends (was 0.02)
                return None

        # How far price is from band (proximity factor for smoother entries)
        bb_width = cur_upper - cur_lower
        lower_prox = (price - cur_lower) / bb_width if bb_width > 0 else 0.5
        upper_prox = (cur_upper - price) / bb_width if bb_width > 0 else 0.5

        # --- LONG (price near lower band + oversold RSI) ---
        # Allow entry when price is within 20% of lower band range
        near_lower = price <= cur_lower or (lower_prox < 0.15 and cur_rsi < self.c.rsi_entry_low + 5)
        if near_lower and cur_rsi < self.c.rsi_entry_low:
            sl = price - self.c.atr_sl_mult * cur_atr
            tp = cur_mid  # target mid-band
            strength = min(1.0, (self.c.rsi_entry_low - cur_rsi) / 25 * 0.6 + 0.3)
            return Signal(
                coin=coin, direction="long", strategy=self.name,
                strength=round(strength, 3),
                entry_price=price, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                meta={"rsi": round(cur_rsi, 2), "bb_lower": round(cur_lower, 6),
                       "bb_mid": round(cur_mid, 6)},
            )

        # --- SHORT (price near upper band + overbought RSI) ---
        near_upper = price >= cur_upper or (upper_prox < 0.15 and cur_rsi > self.c.rsi_entry_high - 5)
        if near_upper and cur_rsi > self.c.rsi_entry_high:
            sl = price + self.c.atr_sl_mult * cur_atr
            tp = cur_mid
            strength = min(1.0, (cur_rsi - self.c.rsi_entry_high) / 25 * 0.6 + 0.3)
            return Signal(
                coin=coin, direction="short", strategy=self.name,
                strength=round(strength, 3),
                entry_price=price, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                meta={"rsi": round(cur_rsi, 2), "bb_upper": round(cur_upper, 6),
                       "bb_mid": round(cur_mid, 6)},
            )

        return None
