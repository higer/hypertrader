"""
OpenClaw Agent integration.
Sends trade execution commands to the OpenClaw Agent REST API.
All orders (open / close / modify) are serialised as JSON and dispatched
to the agent, which handles the actual Hyperliquid on-chain execution.
"""

import time
import aiohttp
from typing import Optional, Dict, List
from loguru import logger

from config import OpenClawConfig


class OpenClawExecutor:

    def __init__(self, cfg: OpenClawConfig):
        self.cfg = cfg
        self.base = cfg.agent_endpoint.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self.order_log: List[dict] = []

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            headers = {"Content-Type": "application/json"}
            if self.cfg.api_key:
                headers["Authorization"] = f"Bearer {self.cfg.api_key}"
            self._session = aiohttp.ClientSession(headers=headers)

    async def close(self):
        if self._session:
            await self._session.close()

    # ------------------------------------------------------------------ API
    # ------------------------------------------------------------------

    async def _post(self, path: str, payload: dict) -> dict:
        await self._ensure_session()
        url = f"{self.base}{path}"
        try:
            async with self._session.post(url, json=payload,
                                           timeout=aiohttp.ClientTimeout(total=self.cfg.timeout)) as r:
                body = await r.json()
                status = r.status
                record = {"ts": time.time(), "url": url, "payload": payload,
                          "status": status, "response": body}
                self.order_log.append(record)
                if status >= 400:
                    logger.error(f"OpenClaw error {status}: {body}")
                return body
        except Exception as e:
            logger.error(f"OpenClaw request failed: {e}")
            self.order_log.append({"ts": time.time(), "url": url,
                                    "payload": payload, "error": str(e)})
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # High-level commands
    # ------------------------------------------------------------------

    async def place_order(self, coin: str, direction: str, size_usd: float,
                          leverage: int, entry_price: float,
                          stop_loss: float, take_profit: float,
                          strategy: str = "") -> dict:
        """Send an open-position command to the agent."""
        payload = {
            "action": "place_order",
            "params": {
                "coin": coin,
                "is_buy": direction == "long",
                "sz_usd": size_usd,
                "leverage": leverage,
                "limit_px": entry_price,
                "order_type": "market",
                "reduce_only": False,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "meta": {"strategy": strategy},
            }
        }
        logger.info(f"→ OpenClaw OPEN {direction.upper()} {coin} ${size_usd:.0f} lev={leverage}")
        return await self._post("/api/v1/execute", payload)

    async def close_position(self, coin: str, direction: str,
                             reason: str = "") -> dict:
        """Send a close-position command to the agent."""
        payload = {
            "action": "close_position",
            "params": {
                "coin": coin,
                "is_buy": direction == "short",  # close short = buy
                "reduce_only": True,
                "order_type": "market",
                "meta": {"reason": reason},
            }
        }
        logger.info(f"→ OpenClaw CLOSE {direction.upper()} {coin} reason={reason}")
        return await self._post("/api/v1/execute", payload)

    async def modify_sl_tp(self, coin: str, direction: str,
                           stop_loss: float, take_profit: float) -> dict:
        payload = {
            "action": "modify_sl_tp",
            "params": {
                "coin": coin,
                "is_buy": direction == "long",
                "stop_loss": stop_loss,
                "take_profit": take_profit,
            }
        }
        return await self._post("/api/v1/execute", payload)

    async def get_agent_status(self) -> dict:
        """Ping the agent for health / balance / positions."""
        await self._ensure_session()
        try:
            async with self._session.get(f"{self.base}/api/v1/status",
                                          timeout=aiohttp.ClientTimeout(total=10)) as r:
                return await r.json()
        except Exception as e:
            return {"error": str(e), "connected": False}

    # ------------------------------------------------------------------
    # Batch execution
    # ------------------------------------------------------------------

    async def execute_commands(self, commands: List[dict]) -> List[dict]:
        """Execute a batch of entry/exit commands from the strategy engine."""
        results = []
        for cmd in commands:
            if cmd["action"] == "open":
                res = await self.place_order(
                    coin=cmd["coin"],
                    direction=cmd["direction"],
                    size_usd=cmd["size_usd"],
                    leverage=cmd.get("leverage", 3),
                    entry_price=cmd["entry_price"],
                    stop_loss=cmd["stop_loss"],
                    take_profit=cmd["take_profit"],
                    strategy=cmd.get("strategy", ""),
                )
                results.append({"cmd": cmd, "result": res})
            elif cmd["action"] == "close":
                res = await self.close_position(
                    coin=cmd["coin"],
                    direction=cmd["direction"],
                    reason=cmd.get("reason", ""),
                )
                results.append({"cmd": cmd, "result": res})
        return results
