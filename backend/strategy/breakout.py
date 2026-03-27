"""
Breakout strategy.
Entry : Donchian channel breakout + volume confirmation + consolidation check.
"""

from typing import Optional
import pandas as pd
import numpy as np
from .base import BaseStrategy, Signal, atr, donchian, volume_sma
from config import BreakoutConfig


class BreakoutStrategy(BaseStrategy):
    name = "breakout"

    def __init__(self, cfg: BreakoutConfig):
        self.c = cfg

    def _is_consolidated(self, df: pd.DataFrame) -> bool:
        """Check if price was in a tight range before breakout."""
        if len(df) < self.c.min_consolidation_bars + 2:
            return False
        recent = df.iloc[-(self.c.min_consolidation_bars + 1):-1]
        rng = (recent["high"].max() - recent["low"].min()) / recent["close"].mean()
        return rng < 0.10  # < 10% range = consolidation (was 6%)

    def evaluate(self, coin: str, df: pd.DataFrame, funding: dict) -> Optional[Signal]:
        if not self.c.enabled or len(df) < self.c.lookback + 5:
            return None

        close = df["close"]
        price = close.iloc[-1]
        prev_price = close.iloc[-2]
        _atr = atr(df, self.c.atr_period)
        cur_atr = _atr.iloc[-1]
        vol_avg = volume_sma(df, 20)
        cur_vol = df["volume"].iloc[-1]
        avg_vol = vol_avg.iloc[-1]

        if pd.isna(cur_atr) or cur_atr == 0:
            return None

        upper, lower = donchian(df, self.c.lookback)
        cur_upper = upper.iloc[-2]  # use previous bar's channel (avoid look-ahead)
        cur_lower = lower.iloc[-2]

        vol_confirm = cur_vol > avg_vol * self.c.volume_confirm_mult if avg_vol > 0 else False
        consolidated = self._is_consolidated(df)

        # --- LONG breakout ---
        if price > cur_upper and prev_price <= cur_upper and vol_confirm and consolidated:
            sl = price - self.c.atr_sl_mult * cur_atr
            tp = price + self.c.atr_tp_mult * cur_atr
            strength = min(1.0, 0.5 + (cur_vol / avg_vol - 1) * 0.15 + 0.2)
            return Signal(
                coin=coin, direction="long", strategy=self.name,
                strength=round(strength, 3),
                entry_price=price, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                meta={"donchian_upper": round(cur_upper, 6),
                       "vol_ratio": round(cur_vol / avg_vol, 2) if avg_vol else 0},
            )

        # --- SHORT breakdown ---
        if price < cur_lower and prev_price >= cur_lower and vol_confirm and consolidated:
            sl = price + self.c.atr_sl_mult * cur_atr
            tp = price - self.c.atr_tp_mult * cur_atr
            strength = min(1.0, 0.5 + (cur_vol / avg_vol - 1) * 0.15 + 0.2)
            return Signal(
                coin=coin, direction="short", strategy=self.name,
                strength=round(strength, 3),
                entry_price=price, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                meta={"donchian_lower": round(cur_lower, 6),
                       "vol_ratio": round(cur_vol / avg_vol, 2) if avg_vol else 0},
            )

        return None
