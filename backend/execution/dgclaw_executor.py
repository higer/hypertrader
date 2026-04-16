"""
Degenerate Claw (dgclaw) direct executor — bypasses OpenClaw AI processing
and sends structured ACP job commands directly to the Degen Claw Trader agent
via the ACP REST API (pure HTTP, no CLI subprocess dependency).

This eliminates AI thinking overhead, endpoint routing issues, and the
`npx tsx` dependency, enabling high-frequency trading execution.

Flow (ACP v1.0):
  1. Convert structured command → ACP perp_trade JSON payload
  2. POST to ACP API to create job (isAutomated=True for auto-pay)
  3. Poll job status through phases:
     REQUEST → NEGOTIATION → TRANSACTION → EVALUATION → COMPLETED/REJECTED/EXPIRED
  4. Return structured result on terminal state

API:
  - ACP API: https://claw-api.virtuals.io
  - Auth: x-api-key (LITE_AGENT_API_KEY from `acp setup`)
  - Degen Claw Trader wallet: 0xd478a8B40372db16cA8045F28C6FE07228F3781A
  - Trading resource API: https://dgclaw-trader.virtuals.io
  - ACP service fee: ~$0.01 per job
"""

import asyncio
import json
import os
import time
from typing import Optional, List

import aiohttp
from loguru import logger

from config import DGClawConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DGCLAW_TRADER_WALLET = "0xd478a8B40372db16cA8045F28C6FE07228F3781A"
DGCLAW_RESOURCE_BASE = "https://dgclaw-trader.virtuals.io"
ACP_API_BASE = "https://claw-api.virtuals.io"


# ---------------------------------------------------------------------------
# Helper: load ACP API key from openclaw-acp config.json
# ---------------------------------------------------------------------------

def _load_acp_api_key() -> Optional[str]:
    """
    Load LITE_AGENT_API_KEY from environment or openclaw-acp config.json.
    The `acp setup` command stores the key in <openclaw-acp>/config.json.
    """
    # 1. Environment variable (highest priority)
    key = os.getenv("LITE_AGENT_API_KEY", "").strip()
    if key:
        return key

    # 2. Try common config.json locations
    candidates = [
        os.path.expanduser("~/openclaw-acp/config.json"),
        os.path.expanduser("~/.openclaw/config.json"),
        "/workspace/openclaw-acp/config.json",
    ]
    for path in candidates:
        try:
            with open(path) as f:
                cfg = json.load(f)
            key = cfg.get("LITE_AGENT_API_KEY", "").strip()
            if key:
                logger.info(f"ACP API key loaded from {path}")
                return key
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            continue

    return None


def _load_acp_builder_code() -> Optional[str]:
    """Load ACP_BUILDER_CODE from environment or config."""
    code = os.getenv("ACP_BUILDER_CODE", "").strip()
    if code:
        return code
    candidates = [
        os.path.expanduser("~/openclaw-acp/config.json"),
        os.path.expanduser("~/.openclaw/config.json"),
        "/workspace/openclaw-acp/config.json",
    ]
    for path in candidates:
        try:
            with open(path) as f:
                cfg = json.load(f)
            code = cfg.get("ACP_BUILDER_CODE", "").strip()
            if code:
                return code
        except (FileNotFoundError, json.JSONDecodeError):
            continue
    return None


# ---------------------------------------------------------------------------
# Executor class
# ---------------------------------------------------------------------------

class DGClawExecutor:

    def __init__(self, cfg: DGClawConfig):
        self.cfg = cfg
        self.order_log: List[dict] = []
        self._acp_session: Optional[aiohttp.ClientSession] = None  # ACP API
        self._res_session: Optional[aiohttp.ClientSession] = None  # resource API
        self._connected: Optional[bool] = None
        self._wallet: Optional[str] = None
        self._acp_api_key: Optional[str] = None
        self._acp_builder_code: Optional[str] = None

    async def _ensure_acp_session(self):
        """Create/reuse the ACP API HTTP session with auth headers."""
        if self._acp_session and not self._acp_session.closed:
            return

        # Load API key
        if not self._acp_api_key:
            self._acp_api_key = (
                self.cfg.acp_api_key
                or _load_acp_api_key()
            )
        if not self._acp_builder_code:
            self._acp_builder_code = (
                self.cfg.acp_builder_code
                or _load_acp_builder_code()
            )

        if not self._acp_api_key:
            raise RuntimeError(
                "ACP API key not found. Set LITE_AGENT_API_KEY env var "
                "or run `acp setup` in openclaw-acp."
            )

        headers = {
            "Content-Type": "application/json",
            "x-api-key": self._acp_api_key,
        }
        if self._acp_builder_code:
            headers["x-builder-code"] = self._acp_builder_code

        base = os.getenv("ACP_API_URL", ACP_API_BASE).rstrip("/")
        self._acp_session = aiohttp.ClientSession(
            base_url=base,
            headers=headers,
        )

    async def _ensure_res_session(self):
        """Create/reuse the resource query HTTP session."""
        if self._res_session and not self._res_session.closed:
            return
        headers = {}
        if self.cfg.api_key:
            headers["Authorization"] = f"Bearer {self.cfg.api_key}"
        self._res_session = aiohttp.ClientSession(headers=headers)

    async def close(self):
        for s in (self._acp_session, self._res_session):
            if s and not s.closed:
                await s.close()

    # ------------------------------------------------------------------
    # ACP REST helpers
    # ------------------------------------------------------------------

    async def _acp_post(self, path: str, payload: dict,
                        timeout: float = 30) -> dict:
        """POST to ACP API and return parsed response."""
        await self._ensure_acp_session()
        try:
            async with self._acp_session.post(
                path, json=payload,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                body = await r.json()
                if r.status >= 400:
                    error_msg = json.dumps(body) if isinstance(body, dict) else str(body)
                    logger.error(f"ACP POST {path} → {r.status}: {error_msg[:300]}")
                    return {"error": f"HTTP {r.status}: {error_msg[:300]}"}
                return body
        except aiohttp.ClientError as e:
            return {"error": f"ACP connection error: {e}"}
        except asyncio.TimeoutError:
            return {"error": f"ACP request to {path} timed out"}
        except Exception as e:
            return {"error": f"ACP request error: {e}"}

    async def _acp_get(self, path: str, timeout: float = 15) -> dict:
        """GET from ACP API."""
        await self._ensure_acp_session()
        try:
            async with self._acp_session.get(
                path, timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                body = await r.json()
                if r.status >= 400:
                    error_msg = json.dumps(body) if isinstance(body, dict) else str(body)
                    return {"error": f"HTTP {r.status}: {error_msg[:300]}"}
                return body
        except aiohttp.ClientError as e:
            return {"error": f"ACP connection error: {e}"}
        except asyncio.TimeoutError:
            return {"error": f"ACP GET {path} timed out"}
        except Exception as e:
            return {"error": f"ACP request error: {e}"}

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    async def _get_wallet(self) -> Optional[str]:
        """Get our wallet address via GET /acp/me (cached)."""
        if self._wallet:
            return self._wallet
        result = await self._acp_get("/acp/me")
        if "error" not in result:
            data = result.get("data", result)
            self._wallet = data.get("walletAddress") or data.get("address")
            if self._wallet:
                logger.info(f"ACP wallet: {self._wallet}")
        return self._wallet

    # ------------------------------------------------------------------
    # ACP job lifecycle (pure HTTP)
    # ------------------------------------------------------------------

    async def _create_job(self, operation: str, requirements: dict) -> dict:
        """
        Create an ACP job targeting the Degen Claw Trader.
        POST /acp/jobs

        ACP v1.0: uses providerWalletAddress, jobOfferingName,
        serviceRequirements, isAutomated.
        """
        payload = {
            "providerWalletAddress": DGCLAW_TRADER_WALLET,
            "jobOfferingName": operation,
            "serviceRequirements": requirements,
            "isAutomated": True,  # ACP v1.0: auto-approve payment flow
        }
        logger.debug(f"ACP create job: {operation} → {json.dumps(requirements)}")
        result = await self._acp_post(
            "/acp/jobs", payload, timeout=self.cfg.job_create_timeout
        )

        # Extract jobId from response: { data: { jobId: N } }
        if "error" not in result:
            data = result.get("data", result)
            job_id = data.get("jobId") or data.get("id")
            if job_id:
                return {"jobId": job_id}
            return {"error": "No jobId in response", "raw": result}
        return result

    async def _get_job_status(self, job_id) -> dict:
        """GET /acp/jobs/{jobId}"""
        result = await self._acp_get(f"/acp/jobs/{job_id}")
        if "error" not in result:
            return result.get("data", result)
        return result

    async def _approve_payment(self, job_id) -> dict:
        """POST /acp/providers/jobs/{jobId}/negotiation — ACP v1.0 format."""
        return await self._acp_post(
            f"/acp/providers/jobs/{job_id}/negotiation",
            {"accept": True, "content": "auto-approved by HyperTrader"},
            timeout=30,
        )

    async def _poll_job(self, job_id) -> dict:
        """
        Poll an ACP job until terminal state.
        Auto-approves payment in NEGOTIATION phase (fallback if isAutomated
        didn't handle it).

        ACP v1.0 phases:
          REQUEST → NEGOTIATION → TRANSACTION → EVALUATION → COMPLETED
                                                           → REJECTED
                                                           → EXPIRED
        """
        for attempt in range(self.cfg.max_poll_attempts):
            await asyncio.sleep(self.cfg.poll_interval)

            status = await self._get_job_status(job_id)
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
                # With isAutomated=True this shouldn't trigger, but handle
                # as fallback — auto-approve if within fee limit.
                payment = status.get("paymentRequestData", {})
                amount = self._extract_payment_amount(payment)

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
                pay_result = await self._approve_payment(job_id)
                if "error" in pay_result:
                    logger.error(f"Payment failed: {pay_result['error']}")

            # REQUEST, TRANSACTION, EVALUATION — keep polling

        return {
            "status": "timeout",
            "job_id": job_id,
            "error": f"Job {job_id} timed out after {self.cfg.max_poll_attempts} polls",
        }

    @staticmethod
    def _extract_payment_amount(payment: dict) -> float:
        """Extract USD payment amount from ACP v1.0 paymentRequestData."""
        if not isinstance(payment, dict):
            return 0
        # Try direct usdValue or amountUsd
        amount = payment.get("amountUsd") or payment.get("usdValue") or 0
        if amount:
            return float(amount)
        # Try nested budget object (ACP v1.0 structure)
        budget = payment.get("budget", {})
        if isinstance(budget, dict):
            amount = budget.get("usdValue") or budget.get("amount") or 0
            return float(amount)
        return 0

    @staticmethod
    def _get_phase(status: dict) -> str:
        """Extract current phase from ACP v1.0 job status.
        Checks memoHistory last entry first, falls back to top-level phase."""
        # ACP v1.0: memoHistory contains phase transitions
        memos = status.get("memoHistory", status.get("memos", []))
        if memos and isinstance(memos, list):
            last = memos[-1]
            if isinstance(last, dict):
                next_phase = last.get("nextPhase") or last.get("status", "")
                if next_phase:
                    return next_phase.upper()
        # Fallback to top-level phase
        phase = status.get("phase", status.get("status", "UNKNOWN"))
        return phase.upper() if isinstance(phase, str) else "UNKNOWN"

    @staticmethod
    def _get_rejection_reason(status: dict) -> str:
        memos = status.get("memos", status.get("memoHistory", []))
        if memos and isinstance(memos, list):
            for entry in reversed(memos):
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

        job_id = result.get("jobId")
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

        job_id = result.get("jobId")
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

        job_id = result.get("jobId")
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
            return {"error": "Wallet address unknown — set LITE_AGENT_API_KEY"}
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
        await self._ensure_res_session()
        url = f"{DGCLAW_RESOURCE_BASE}{path}"
        try:
            async with self._res_session.get(
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
        """Check ACP connectivity and account status via REST API."""
        try:
            result = await self._acp_get("/acp/me")
        except RuntimeError as e:
            # API key not configured
            self._connected = False
            return {
                "status": "disconnected",
                "connected": False,
                "error": str(e),
                "message": "ACP API key not configured",
            }

        if "error" in result:
            self._connected = False
            return {
                "status": "disconnected",
                "connected": False,
                "error": result["error"],
                "message": "Cannot reach ACP API",
            }

        data = result.get("data", result)
        wallet = data.get("walletAddress") or data.get("address", "unknown")
        self._wallet = wallet
        self._connected = True

        # Get account balance and positions
        account = await self.get_account()
        positions = await self.get_positions()

        return {
            "status": "connected",
            "connected": True,
            "executor": "dgclaw",
            "wallet": wallet,
            "agent_name": data.get("name", ""),
            "trader_wallet": DGCLAW_TRADER_WALLET,
            "account": account if "error" not in account else None,
            "positions": positions if "error" not in positions else None,
            "resource_api": DGCLAW_RESOURCE_BASE,
            "acp_api": ACP_API_BASE,
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
