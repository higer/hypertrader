"""
Degenerate Claw (dgclaw) direct executor — bypasses OpenClaw AI processing
and sends structured ACP job commands directly to the Degen Claw Trader agent.

This eliminates AI thinking overhead and endpoint routing issues, enabling
high-frequency trading execution.

Flow:
  1. Convert structured command → ACP perp_trade JSON payload
  2. Create ACP job via `acp job create` subprocess
  3. Auto-approve payment in NEGOTIATION phase
  4. Poll job status until COMPLETED / REJECTED / EXPIRED
  5. Return structured result

Constants:
  - Degen Claw Trader wallet: 0xd478a8B40372db16cA8045F28C6FE07228F3781A
  - Trading resource API: https://dgclaw-trader.virtuals.io
  - ACP service fee: ~$0.01 per job
"""

import asyncio
import json
import time
from typing import Optional, Dict, List

import aiohttp
from loguru import logger

from config import DGClawConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DGCLAW_TRADER_WALLET = "0xd478a8B40372db16cA8045F28C6FE07228F3781A"
DGCLAW_RESOURCE_BASE = "https://dgclaw-trader.virtuals.io"


# ---------------------------------------------------------------------------
# ACP subprocess helpers (async, non-blocking)
# ---------------------------------------------------------------------------

async def _run_acp(*args, timeout: float = 30) -> dict:
    """Run an `acp` CLI command and return parsed JSON output."""
    cmd = ["acp"] + list(args) + ["--json"]
    logger.debug(f"ACP cmd: {' '.join(cmd)}")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )

        out_text = stdout.decode().strip()
        err_text = stderr.decode().strip()

        if proc.returncode != 0:
            logger.error(f"ACP error (rc={proc.returncode}): {err_text}")
            return {"error": f"ACP exit code {proc.returncode}: {err_text}"}

        if not out_text:
            return {"error": "ACP returned empty output"}

        # Parse JSON — acp --json outputs JSON
        try:
            return json.loads(out_text)
        except json.JSONDecodeError:
            # Sometimes output has non-JSON preamble; try last line
            for line in reversed(out_text.splitlines()):
                line = line.strip()
                if line.startswith("{") or line.startswith("["):
                    return json.loads(line)
            return {"raw": out_text}

    except asyncio.TimeoutError:
        return {"error": f"ACP command timed out after {timeout}s"}
    except FileNotFoundError:
        return {"error": "acp CLI not found — install openclaw-acp first"}
    except Exception as e:
        return {"error": f"ACP subprocess error: {e}"}


# ---------------------------------------------------------------------------
# Executor class
# ---------------------------------------------------------------------------

class DGClawExecutor:

    def __init__(self, cfg: DGClawConfig):
        self.cfg = cfg
        self.order_log: List[dict] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._connected: Optional[bool] = None
        self._wallet: Optional[str] = None  # our wallet, discovered via acp whoami

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            headers = {}
            if self.cfg.api_key:
                headers["Authorization"] = f"Bearer {self.cfg.api_key}"
            self._session = aiohttp.ClientSession(headers=headers)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_wallet(self) -> Optional[str]:
        """Get our wallet address from acp whoami (cached)."""
        if self._wallet:
            return self._wallet
        result = await _run_acp("whoami")
        if "error" not in result:
            self._wallet = result.get("walletAddress") or result.get("address")
            if self._wallet:
                logger.info(f"ACP wallet: {self._wallet}")
        return self._wallet

    # ------------------------------------------------------------------
    # ACP job lifecycle
    # ------------------------------------------------------------------

    async def _create_job(self, operation: str, requirements: dict) -> dict:
        """Create an ACP job targeting the Degen Claw Trader."""
        req_json = json.dumps(requirements, separators=(",", ":"))
        result = await _run_acp(
            "job", "create",
            DGCLAW_TRADER_WALLET,
            operation,
            "--requirements", req_json,
            timeout=self.cfg.job_create_timeout,
        )
        return result

    async def _poll_job(self, job_id: str) -> dict:
        """
        Poll an ACP job until terminal state.
        Auto-approves payment in NEGOTIATION phase.
        """
        for attempt in range(self.cfg.max_poll_attempts):
            await asyncio.sleep(self.cfg.poll_interval)

            status = await _run_acp("job", "status", str(job_id))
            if "error" in status:
                logger.warning(f"Poll error (attempt {attempt+1}): {status['error']}")
                continue

            phase = self._get_phase(status)
            logger.debug(f"Job {job_id} phase: {phase} (attempt {attempt+1})")

            if phase == "COMPLETED":
                deliverable = status.get("deliverable", "")
                logger.info(f"Job {job_id} COMPLETED: {str(deliverable)[:200]}")
                return {
                    "status": "completed",
                    "job_id": job_id,
                    "deliverable": deliverable,
                    "raw": status,
                }

            elif phase in ("REJECTED", "EXPIRED"):
                reason = self._get_rejection_reason(status)
                logger.error(f"Job {job_id} {phase}: {reason}")
                return {
                    "status": phase.lower(),
                    "job_id": job_id,
                    "error": f"Job {phase}: {reason}",
                    "raw": status,
                }

            elif phase == "NEGOTIATION":
                # Auto-approve payment (typically $0.01 ACP fee)
                payment = status.get("paymentRequestData", {})
                amount = payment.get("amountUsd", 0)
                if amount > self.cfg.max_auto_pay_usd:
                    logger.error(
                        f"Job {job_id} payment ${amount} exceeds "
                        f"max_auto_pay ${self.cfg.max_auto_pay_usd}"
                    )
                    return {
                        "status": "rejected",
                        "job_id": job_id,
                        "error": f"Payment ${amount} exceeds limit",
                    }

                logger.info(f"Job {job_id} auto-paying ${amount}")
                pay_result = await _run_acp(
                    "job", "pay", str(job_id), "--accept", "true",
                    timeout=30,
                )
                if "error" in pay_result:
                    logger.error(f"Payment failed: {pay_result['error']}")

            # TRANSACTION, EVALUATION, REQUEST — keep polling

        return {
            "status": "timeout",
            "job_id": job_id,
            "error": f"Job {job_id} timed out after {self.cfg.max_poll_attempts} polls",
        }

    @staticmethod
    def _get_phase(status: dict) -> str:
        """Extract the current phase, preferring memoHistory over top-level."""
        memo = status.get("memoHistory", [])
        if memo:
            # Latest memo entry has the most recent phase
            last = memo[-1] if isinstance(memo, list) else memo
            if isinstance(last, dict):
                next_phase = last.get("nextPhase", "")
                if next_phase:
                    return next_phase
        return status.get("phase", "UNKNOWN")

    @staticmethod
    def _get_rejection_reason(status: dict) -> str:
        memo = status.get("memoHistory", [])
        if memo and isinstance(memo, list):
            for entry in reversed(memo):
                content = entry.get("content", "")
                if content:
                    return str(content)[:300]
        return status.get("deliverable", "unknown reason")

    # ------------------------------------------------------------------
    # Build trade payloads
    # ------------------------------------------------------------------

    @staticmethod
    def _build_open_payload(cmd: dict) -> dict:
        """Convert our internal command format → dgclaw perp_trade JSON."""
        payload = {
            "action": "open",
            "pair": cmd["coin"],
            "side": cmd["direction"],          # "long" or "short"
            "size": str(int(cmd["size_usd"])),  # string, USD notional
            "orderType": cmd.get("order_type", "market"),
        }
        if cmd.get("leverage"):
            payload["leverage"] = int(cmd["leverage"])  # number, not string
        if cmd.get("stop_loss"):
            payload["stopLoss"] = str(cmd["stop_loss"])
        if cmd.get("take_profit"):
            payload["takeProfit"] = str(cmd["take_profit"])
        if cmd.get("limit_price") and payload["orderType"] == "limit":
            payload["limitPrice"] = str(cmd["limit_price"])
        return payload

    @staticmethod
    def _build_close_payload(cmd: dict) -> dict:
        return {
            "action": "close",
            "pair": cmd["coin"],
        }

    @staticmethod
    def _build_modify_payload(cmd: dict) -> dict:
        payload = {"pair": cmd["coin"]}
        if cmd.get("leverage"):
            payload["leverage"] = int(cmd["leverage"])
        if cmd.get("stop_loss"):
            payload["stopLoss"] = str(cmd["stop_loss"])
        if cmd.get("take_profit"):
            payload["takeProfit"] = str(cmd["take_profit"])
        return payload

    # ------------------------------------------------------------------
    # High-level trading commands
    # ------------------------------------------------------------------

    async def place_order(self, coin: str, direction: str, size_usd: float,
                          leverage: int, entry_price: float,
                          stop_loss: float, take_profit: float,
                          strategy: str = "") -> dict:
        """Open a position via ACP perp_trade job."""
        cmd = {
            "coin": coin, "direction": direction,
            "size_usd": size_usd, "leverage": leverage,
            "stop_loss": stop_loss, "take_profit": take_profit,
        }
        payload = self._build_open_payload(cmd)

        logger.info(
            f"→ DGCLAW OPEN {direction.upper()} {coin} "
            f"${size_usd:.0f} lev={leverage} SL={stop_loss} TP={take_profit}"
        )

        record = {
            "ts": time.time(),
            "action": "open",
            "coin": coin,
            "direction": direction,
            "payload": payload,
        }

        result = await self._create_job("perp_trade", payload)

        if "error" in result:
            record["error"] = result["error"]
            self._append_log(record)
            return result

        job_id = result.get("jobId") or result.get("id") or result.get("job_id")
        if not job_id:
            record["error"] = "No job ID returned"
            self._append_log(record)
            return {"error": "No job ID in ACP response", "raw": result}

        record["job_id"] = job_id
        logger.info(f"ACP job created: {job_id}")

        poll_result = await self._poll_job(job_id)
        record["result"] = poll_result
        self._append_log(record)
        return poll_result

    async def close_position(self, coin: str, direction: str,
                             reason: str = "") -> dict:
        """Close a position via ACP perp_trade job."""
        payload = self._build_close_payload({"coin": coin})

        logger.info(f"→ DGCLAW CLOSE {direction.upper()} {coin} reason={reason}")

        record = {
            "ts": time.time(),
            "action": "close",
            "coin": coin,
            "direction": direction,
            "reason": reason,
            "payload": payload,
        }

        result = await self._create_job("perp_trade", payload)

        if "error" in result:
            record["error"] = result["error"]
            self._append_log(record)
            return result

        job_id = result.get("jobId") or result.get("id") or result.get("job_id")
        if not job_id:
            record["error"] = "No job ID returned"
            self._append_log(record)
            return {"error": "No job ID in ACP response", "raw": result}

        record["job_id"] = job_id
        poll_result = await self._poll_job(job_id)
        record["result"] = poll_result
        self._append_log(record)
        return poll_result

    async def modify_position(self, coin: str, leverage: int = None,
                              stop_loss: float = None,
                              take_profit: float = None) -> dict:
        """Modify an open position's SL/TP/leverage via ACP perp_modify."""
        cmd = {"coin": coin}
        if leverage is not None:
            cmd["leverage"] = leverage
        if stop_loss is not None:
            cmd["stop_loss"] = stop_loss
        if take_profit is not None:
            cmd["take_profit"] = take_profit

        payload = self._build_modify_payload(cmd)
        logger.info(f"→ DGCLAW MODIFY {coin} {payload}")

        result = await self._create_job("perp_modify", payload)
        if "error" in result:
            return result

        job_id = result.get("jobId") or result.get("id")
        if not job_id:
            return {"error": "No job ID in ACP response", "raw": result}

        return await self._poll_job(job_id)

    # ------------------------------------------------------------------
    # Resource queries (direct HTTP — fast, no ACP overhead)
    # ------------------------------------------------------------------

    async def get_positions(self) -> dict:
        """Query live open positions from dgclaw-trader API."""
        wallet = await self._get_wallet()
        if not wallet:
            return {"error": "Wallet address unknown — run `acp whoami`"}
        return await self._resource_get(f"/users/{wallet}/positions")

    async def get_account(self) -> dict:
        """Query account balance."""
        wallet = await self._get_wallet()
        if not wallet:
            return {"error": "Wallet address unknown"}
        return await self._resource_get(f"/users/{wallet}/account")

    async def get_trade_history(self, pair: str = None, limit: int = 50) -> dict:
        """Query trade history."""
        wallet = await self._get_wallet()
        if not wallet:
            return {"error": "Wallet address unknown"}
        params = f"?limit={limit}"
        if pair:
            params += f"&pair={pair}"
        return await self._resource_get(f"/users/{wallet}/perp-trades{params}")

    async def get_tickers(self) -> dict:
        """Query all supported tickers (mark price, funding, OI)."""
        return await self._resource_get("/tickers")

    async def _resource_get(self, path: str) -> dict:
        """GET from dgclaw-trader.virtuals.io resource API."""
        await self._ensure_session()
        url = f"{DGCLAW_RESOURCE_BASE}{path}"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                if r.status >= 400:
                    body = await r.text()
                    return {"error": f"HTTP {r.status}: {body[:200]}"}
                return await r.json()
        except Exception as e:
            return {"error": f"Resource query failed: {e}"}

    # ------------------------------------------------------------------
    # Agent status
    # ------------------------------------------------------------------

    async def get_agent_status(self) -> dict:
        """Check ACP connectivity and account status."""
        # 1. Check acp whoami
        whoami = await _run_acp("whoami")
        if "error" in whoami:
            self._connected = False
            return {
                "status": "disconnected",
                "connected": False,
                "error": whoami["error"],
                "message": "ACP CLI not available or not configured",
            }

        wallet = whoami.get("walletAddress") or whoami.get("address", "unknown")
        self._wallet = wallet
        self._connected = True

        # 2. Try to get account balance
        account = await self.get_account()
        positions = await self.get_positions()

        return {
            "status": "connected",
            "connected": True,
            "executor": "dgclaw",
            "wallet": wallet,
            "trader_wallet": DGCLAW_TRADER_WALLET,
            "account": account if "error" not in account else None,
            "positions": positions if "error" not in positions else None,
            "resource_api": DGCLAW_RESOURCE_BASE,
        }

    # ------------------------------------------------------------------
    # Batch execution (called by trading loop)
    # ------------------------------------------------------------------

    async def execute_commands(self, commands: List[dict]) -> List[dict]:
        """Execute a batch of entry/exit commands via ACP jobs."""
        results = []
        for cmd in commands:
            try:
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
            except Exception as e:
                logger.error(f"DGClaw execution error for {cmd}: {e}")
                results.append({"cmd": cmd, "result": {"error": str(e)}})
        return results

    # ------------------------------------------------------------------
    # Order log management
    # ------------------------------------------------------------------

    def _append_log(self, record: dict):
        self.order_log.append(record)
        if len(self.order_log) > 500:
            self.order_log = self.order_log[-500:]
