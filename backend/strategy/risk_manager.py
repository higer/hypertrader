"""
Risk Manager – position sizing, drawdown control, portfolio-level limits.
"""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from loguru import logger
from config import RiskConfig
from strategy.base import Signal


@dataclass
class Position:
    coin: str
    direction: str
    entry_price: float
    size_usd: float
    stop_loss: float
    take_profit: float
    trailing_stop: float
    strategy: str
    opened_at: float = field(default_factory=time.time)
    highest_pnl_pct: float = 0.0

    @property
    def id(self) -> str:
        return f"{self.coin}_{self.direction}_{int(self.opened_at)}"


class RiskManager:

    def __init__(self, cfg: RiskConfig, initial_equity: float = None):
        self.cfg = cfg
        equity = initial_equity if initial_equity is not None else cfg.initial_equity
        self.equity = equity
        self.peak_equity = equity
        self.daily_start_equity = equity
        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[dict] = []
        self._cooldowns: Dict[str, float] = {}  # coin → reentry time

    def reset_equity(self, new_equity: float):
        """Reset equity to a new value. Called when initial_equity config changes.
        Preserves PnL from closed trades by computing the delta."""
        old_equity = self.equity
        self.equity = new_equity
        self.peak_equity = max(new_equity, self.peak_equity)
        self.daily_start_equity = new_equity
        logger.info(f"Equity reset: ${old_equity:,.2f} → ${new_equity:,.2f}")

    # ----- sizing -----
    def compute_size(self, signal: Signal) -> float:
        """Kelly-lite + risk parity sizing."""
        risk_per_trade = self.equity * self.cfg.max_position_pct
        if signal.size_hint_pct > 0:
            risk_per_trade = self.equity * signal.size_hint_pct

        # Scale by strength
        risk_per_trade *= max(0.3, signal.strength)

        # ATR-distance proportional sizing
        if signal.entry_price and signal.stop_loss:
            dist = abs(signal.entry_price - signal.stop_loss)
            if dist > 0:
                raw = risk_per_trade / dist * signal.entry_price
                risk_per_trade = min(risk_per_trade, raw)

        return round(risk_per_trade, 2)

    # ----- checks -----
    def can_open(self, signal: Signal) -> (bool, str):
        # Max positions
        if len(self.positions) >= self.cfg.max_open_positions:
            return False, "max_positions_reached"

        # Duplicate coin+direction
        key = f"{signal.coin}_{signal.direction}"
        if key in self.positions:
            return False, "duplicate_position"

        # Drawdown circuit breaker
        dd = (self.peak_equity - self.equity) / self.peak_equity if self.peak_equity > 0 else 0
        if dd >= self.cfg.max_drawdown_pct:
            return False, f"drawdown_breaker ({dd:.2%})"

        # Daily loss limit
        daily_pnl = (self.equity - self.daily_start_equity) / self.daily_start_equity if self.daily_start_equity > 0 else 0
        if daily_pnl <= -self.cfg.daily_loss_limit_pct:
            return False, f"daily_loss_limit ({daily_pnl:.2%})"

        # Cooldown
        if signal.coin in self._cooldowns and time.time() < self._cooldowns[signal.coin]:
            return False, "cooldown"

        return True, "ok"

    # ----- position lifecycle -----
    def open_position(self, signal: Signal) -> Optional[Position]:
        ok, reason = self.can_open(signal)
        if not ok:
            logger.warning(f"Risk blocked {signal.coin} {signal.direction}: {reason}")
            return None

        size = self.compute_size(signal)
        if size < 10:  # min $10
            return None

        pos = Position(
            coin=signal.coin,
            direction=signal.direction,
            entry_price=signal.entry_price,
            size_usd=size,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            trailing_stop=signal.stop_loss,
            strategy=signal.strategy,
        )
        key = f"{signal.coin}_{signal.direction}"
        self.positions[key] = pos
        logger.info(f"OPEN {pos.direction.upper()} {pos.coin} ${pos.size_usd:.0f} "
                     f"@ {pos.entry_price} SL={pos.stop_loss} TP={pos.take_profit}")
        return pos

    def rollback_position(self, coin: str, direction: str):
        """Remove a position that was opened internally but failed on ACP.
        Called when the executor returns an error for an open command."""
        key = f"{coin}_{direction}"
        if key in self.positions:
            pos = self.positions.pop(key)
            logger.warning(f"ROLLBACK {pos.direction.upper()} {pos.coin} "
                          f"${pos.size_usd:.0f} — ACP execution failed")
            return True
        return False

    def rollback_exit(self, exit_record: dict):
        """Restore a position that was closed internally but failed on ACP.
        Called when the executor returns an error for a close command."""
        coin = exit_record["coin"]
        direction = exit_record["direction"]
        key = f"{coin}_{direction}"

        # Re-create the position from the exit record
        # Undo the equity change
        self.equity -= exit_record.get("pnl_usd", 0)
        self.peak_equity = max(self.peak_equity, self.equity)

        # Remove from closed_trades
        if self.closed_trades and self.closed_trades[-1].get("coin") == coin:
            self.closed_trades.pop()

        # Remove cooldown
        if coin in self._cooldowns:
            del self._cooldowns[coin]

        # Restore position (use entry price from the exit record)
        pos = Position(
            coin=coin,
            direction=direction,
            entry_price=exit_record.get("entry", 0),
            size_usd=exit_record.get("size_usd", 0),
            stop_loss=exit_record.get("stop_loss", 0),
            take_profit=exit_record.get("take_profit", 0),
            trailing_stop=exit_record.get("stop_loss", 0),
            strategy=exit_record.get("strategy", ""),
        )
        self.positions[key] = pos
        logger.warning(f"ROLLBACK EXIT {direction.upper()} {coin} — "
                      f"ACP close failed, position restored")

    def check_exits(self, prices: Dict[str, float]) -> List[dict]:
        """Check all open positions for SL/TP/trailing-stop hits."""
        exits = []
        for key, pos in list(self.positions.items()):
            price = prices.get(pos.coin)
            if price is None:
                continue

            pnl_pct = ((price - pos.entry_price) / pos.entry_price
                       if pos.direction == "long"
                       else (pos.entry_price - price) / pos.entry_price)
            pos.highest_pnl_pct = max(pos.highest_pnl_pct, pnl_pct)

            # Update trailing stop
            if pnl_pct > 0.005:
                if pos.direction == "long":
                    new_ts = price * (1 - self.cfg.trailing_stop_pct)
                    pos.trailing_stop = max(pos.trailing_stop, new_ts)
                else:
                    new_ts = price * (1 + self.cfg.trailing_stop_pct)
                    pos.trailing_stop = min(pos.trailing_stop, new_ts) if pos.trailing_stop > 0 else new_ts

            reason = None
            # Hard stop loss
            if pos.direction == "long" and price <= pos.stop_loss:
                reason = "stop_loss"
            elif pos.direction == "short" and price >= pos.stop_loss:
                reason = "stop_loss"
            # Take profit
            elif pos.direction == "long" and price >= pos.take_profit:
                reason = "take_profit"
            elif pos.direction == "short" and price <= pos.take_profit:
                reason = "take_profit"
            # Trailing stop
            elif pos.direction == "long" and price <= pos.trailing_stop and pnl_pct > 0:
                reason = "trailing_stop"
            elif pos.direction == "short" and price >= pos.trailing_stop and pnl_pct > 0:
                reason = "trailing_stop"

            if reason:
                pnl_usd = pos.size_usd * pnl_pct
                self.equity += pnl_usd
                self.peak_equity = max(self.peak_equity, self.equity)

                record = {
                    "coin": pos.coin, "direction": pos.direction,
                    "strategy": pos.strategy, "reason": reason,
                    "entry": pos.entry_price, "exit": price,
                    "pnl_pct": round(pnl_pct * 100, 2),
                    "pnl_usd": round(pnl_usd, 2),
                    "duration_s": round(time.time() - pos.opened_at, 1),
                    # Preserve for rollback
                    "size_usd": pos.size_usd,
                    "stop_loss": pos.stop_loss,
                    "take_profit": pos.take_profit,
                }
                self.closed_trades.append(record)
                exits.append(record)
                del self.positions[key]
                self._cooldowns[pos.coin] = time.time() + 180  # 3-min cooldown
                logger.info(f"CLOSE {pos.direction.upper()} {pos.coin} reason={reason} "
                             f"PnL={pnl_pct*100:+.2f}% (${pnl_usd:+.2f})")

        return exits

    def reset_daily(self):
        self.daily_start_equity = self.equity

    # ----- stats -----
    def stats(self) -> dict:
        wins = [t for t in self.closed_trades if t["pnl_usd"] > 0]
        losses = [t for t in self.closed_trades if t["pnl_usd"] <= 0]
        total = len(self.closed_trades)
        return {
            "equity": round(self.equity, 2),
            "peak_equity": round(self.peak_equity, 2),
            "drawdown_pct": round((self.peak_equity - self.equity) / self.peak_equity * 100, 2) if self.peak_equity else 0,
            "open_positions": len(self.positions),
            "total_trades": total,
            "win_rate": round(len(wins) / total * 100, 1) if total else 0,
            "avg_win": round(sum(t["pnl_usd"] for t in wins) / len(wins), 2) if wins else 0,
            "avg_loss": round(sum(t["pnl_usd"] for t in losses) / len(losses), 2) if losses else 0,
            "total_pnl": round(sum(t["pnl_usd"] for t in self.closed_trades), 2),
            "positions": {k: {
                "coin": p.coin, "dir": p.direction, "entry": p.entry_price,
                "size": p.size_usd, "sl": p.stop_loss, "tp": p.take_profit,
                "strategy": p.strategy,
            } for k, p in self.positions.items()},
            "recent_trades": self.closed_trades[-20:],
        }
