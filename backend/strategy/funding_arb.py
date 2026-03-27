"""
Funding-rate arbitrage strategy.
When funding is significantly positive → short (earn funding).
When funding is significantly negative → long (earn funding).
Typically combined with a hedge on another venue, but here used as
a standalone short-term directional bias with tight risk.
"""

from typing import Optional
import pandas as pd
from .base import BaseStrategy, Signal, atr
from config import FundingArbConfig


class FundingArbStrategy(BaseStrategy):
    name = "funding_arb"

    def __init__(self, cfg: FundingArbConfig):
        self.c = cfg

    def evaluate(self, coin: str, df: pd.DataFrame, funding: dict) -> Optional[Signal]:
        if not self.c.enabled or not funding:
            return None

        rate = funding.get("funding", 0)
        mark = funding.get("markPx", 0)
        if mark == 0:
            return None

        _atr = atr(df, 14)
        cur_atr = _atr.iloc[-1] if len(_atr) > 0 and not pd.isna(_atr.iloc[-1]) else 0
        if cur_atr == 0:
            return None

        # Positive funding → shorts pay longs → we go short to earn
        if rate > self.c.funding_threshold:
            sl = mark + 2 * cur_atr
            tp = mark - 1.5 * cur_atr
            strength = min(1.0, rate / (self.c.funding_threshold * 4))
            return Signal(
                coin=coin, direction="short", strategy=self.name,
                strength=round(strength, 3),
                entry_price=mark, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                size_hint_pct=self.c.max_position_pct,
                meta={"funding_rate": rate},
            )

        # Negative funding → longs pay shorts → we go long to earn
        if rate < -self.c.funding_threshold:
            sl = mark - 2 * cur_atr
            tp = mark + 1.5 * cur_atr
            strength = min(1.0, abs(rate) / (self.c.funding_threshold * 4))
            return Signal(
                coin=coin, direction="long", strategy=self.name,
                strength=round(strength, 3),
                entry_price=mark, stop_loss=round(sl, 6), take_profit=round(tp, 6),
                size_hint_pct=self.c.max_position_pct,
                meta={"funding_rate": rate},
            )

        return None
