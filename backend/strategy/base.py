"""
Technical indicator helpers and base strategy class.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, List
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Indicator library
# ---------------------------------------------------------------------------

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"].shift(1)
    tr = pd.concat([h - l, (h - c).abs(), (l - c).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def bollinger(series: pd.Series, period: int = 20, std: float = 2.0):
    mid = sma(series, period)
    s = series.rolling(period).std()
    return mid, mid + std * s, mid - std * s

def volume_sma(df: pd.DataFrame, period: int = 20) -> pd.Series:
    return sma(df["volume"], period)

def donchian(df: pd.DataFrame, period: int = 48):
    upper = df["high"].rolling(period).max()
    lower = df["low"].rolling(period).min()
    return upper, lower


# ---------------------------------------------------------------------------
# Signal dataclass
# ---------------------------------------------------------------------------

@dataclass
class Signal:
    coin: str
    direction: str          # "long" | "short"
    strategy: str           # name of the emitting strategy
    strength: float = 0.0   # 0-1 confidence score
    entry_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    size_hint_pct: float = 0.0   # suggested position size as % equity
    meta: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Base strategy interface
# ---------------------------------------------------------------------------

class BaseStrategy(ABC):
    name: str = "base"

    @abstractmethod
    def evaluate(self, coin: str, df: pd.DataFrame, funding: dict) -> Optional[Signal]:
        """Return a Signal if entry conditions are met, else None."""
        ...
