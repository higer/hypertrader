"""
OpenClaw Agent integration — sends natural-language trading instructions
to a locally-running OpenClaw Gateway that has the HyperLiquid-Claw skill
(or equivalent) installed.

Flow:
  1. Convert structured command → clear natural-language prompt
  2. POST prompt to OpenClaw Gateway
  3. Poll for completion (async job model) if needed
  4. Parse and return result

The Gateway default address is http://127.0.0.1:18789.
"""

import asyncio, time, json
import aiohttp
from typing import Optional, Dict, List
from loguru import logger

from config import OpenClawConfig


# ---------------------------------------------------------------------------
# Prompt templates — designed for HyperLiquid-Claw MCP tools
# ---------------------------------------------------------------------------

def _open_prompt(cmd: dict) -> str:
    """Build a natural-language open-position instruction."""
    side = "buy" if cmd["direction"] == "long" else "sell"
    coin = cmd["coin"]
    size_usd = cmd["size_usd"]
    leverage = cmd.get("leverage", 3)
    sl = cmd["stop_loss"]
    tp = cmd["take_profit"]
    strategy = cmd.get("strategy", "")

    return (
        f"Execute the following Hyperliquid perpetual futures trade:\n"
        f"- Action: Market {side} (open {cmd['direction']} position)\n"
        f"- Coin: {coin}\n"
        f"- Size: approximately ${size_usd:.0f} USD notional\n"
        f"- Leverage: {leverage}x\n"
        f"- Stop loss: {sl}\n"
        f"- Take profit: {tp}\n"
        f"- Strategy: {strategy}\n\n"
        f"First set leverage to {leverage}x for {coin}, "
        f"then place a market {side} order for {coin} with size ~${size_usd:.0f}. "
        f"This is an automated signal — execute immediately."
    )


def _close_prompt(cmd: dict) -> str:
    """Build a natural-language close-position instruction."""
    side = "sell" if cmd["direction"] == "long" else "buy"
    coin = cmd["coin"]
    reason = cmd.get("reason", "signal")

    return (
        f"Close my {cmd['direction']} position on {coin} (Hyperliquid perps).\n"
        f"- Action: Market {side} to close (reduce_only)\n"
        f"- Coin: {coin}\n"
        f"- Reason: {reason}\n\n"
        f"Close the entire {coin} {cmd['direction']} position at market price immediately."
    )


def _status_prompt() -> str:
    return (
        "Show my current Hyperliquid account status: "
        "wallet balance, open positions, and recent fills."
    )


# ---------------------------------------------------------------------------
# Executor class
# ---------------------------------------------------------------------------

class OpenClawExecutor:

    def __init__(self, cfg: OpenClawConfig):
        self.cfg = cfg
        self.base = cfg.agent_endpoint.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self.order_log: List[dict] = []
        self._connected: Optional[bool] = None
        self._working_endpoint: Optional[str] = None  # cache the working path

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            headers = {"Content-Type": "application/json"}
            if self.cfg.api_key:
                headers["Authorization"] = f"Bearer {self.cfg.api_key}"
            self._session = aiohttp.ClientSession(headers=headers)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Core: send prompt to OpenClaw Gateway
    # ------------------------------------------------------------------

    async def _send_prompt(self, prompt: str) -> dict:
        """
        Send a natural-language prompt to the OpenClaw Gateway.
        Tries multiple common endpoint patterns to find the working one.
        """
        await self._ensure_session()

        # If we already found a working endpoint, use it directly
        if self._working_endpoint:
            return await self._try_send(self._working_endpoint, prompt)

        # OpenClaw Gateway endpoints — ordered by priority.
        # /v1/chat/completions requires chatCompletions.enabled: true in config
        # /v1/responses requires responses.enabled: true in config
        endpoint_candidates = [
            "/v1/chat/completions",  # OpenAI-compatible (most reliable)
            "/v1/responses",         # OpenResponses-compatible
            "/tools/invoke",         # Direct tool invocation (always available)
        ]

        last_error = None
        for endpoint in endpoint_candidates:
            result = await self._try_send(endpoint, prompt)
            if "error" not in result or "404" not in str(result.get("error", "")):
                # Found a working endpoint — cache it
                if "error" not in result:
                    self._working_endpoint = endpoint
                    self._connected = True
                return result
            last_error = result.get("error")

        error_msg = f"All OpenClaw endpoints failed. Last error: {last_error}"
        logger.error(error_msg)
        self._connected = False
        self.order_log.append({
            "ts": time.time(), "url": self.base,
            "prompt": prompt[:200], "error": error_msg
        })
        return {"error": error_msg}

    async def _try_send(self, endpoint: str, prompt: str) -> dict:
        """Try sending a prompt to a specific endpoint with the correct payload format."""
        url = f"{self.base}{endpoint}"

        # Build the correct payload based on the endpoint type
        if endpoint == "/v1/chat/completions":
            payload = {
                "model": "openclaw",
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            }
        elif endpoint == "/v1/responses":
            payload = {
                "model": "openclaw",
                "input": prompt,
            }
        elif endpoint == "/tools/invoke":
            # Use tools/invoke to send as a session message
            # This wraps the prompt into a tool call that the agent processes
            payload = {
                "tool": "computer",
                "action": "json",
                "args": {"command": prompt},
            }
        else:
            payload = {
                "model": "openclaw",
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            }

        try:
            async with self._session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=self.cfg.timeout)
            ) as r:
                if r.status == 404:
                    return {"error": f"404 at {url}"}
                if r.status == 403:
                    return {"error": f"403 Forbidden at {url} — check API key/token"}

                try:
                    body = await r.json()
                except Exception:
                    body = {"raw": await r.text()}

                record = {
                    "ts": time.time(),
                    "url": url,
                    "prompt": prompt[:200],
                    "status": r.status,
                    "response": body,
                }
                self.order_log.append(record)
                if len(self.order_log) > 500:
                    self.order_log = self.order_log[-500:]

                if r.status >= 400:
                    logger.error(f"OpenClaw error {r.status}: {body}")
                    return {"error": f"HTTP {r.status}", "details": body}

                # Extract the assistant's response text
                result = self._extract_response(body, endpoint)
                logger.info(f"OpenClaw prompt sent OK via {endpoint}")
                return result

        except asyncio.TimeoutError:
            return {"error": f"Timeout sending to {url}"}
        except aiohttp.ClientError as e:
            return {"error": f"Connection error: {e}"}
        except Exception as e:
            return {"error": f"Unexpected error: {e}"}

    def _extract_response(self, body: dict, endpoint: str) -> dict:
        """Extract the useful response from different endpoint formats."""
        if endpoint == "/v1/chat/completions":
            # OpenAI format: body.choices[0].message.content
            choices = body.get("choices", [])
            if choices:
                msg = choices[0].get("message", {})
                return {
                    "response": msg.get("content", ""),
                    "role": msg.get("role", "assistant"),
                    "raw": body,
                }
        elif endpoint == "/v1/responses":
            # OpenResponses format
            output = body.get("output", body.get("response", ""))
            return {"response": output, "raw": body}

        # Fallback — return body as-is
        return body

    async def _send_and_poll(self, prompt: str) -> dict:
        """Send prompt, and if the response includes a job ID, poll until done."""
        result = await self._send_prompt(prompt)

        if "error" in result:
            return result

        # Check if this is an async job response that needs polling
        job_id = result.get("jobId") or result.get("job_id") or result.get("id")
        if job_id and result.get("status") in ("pending", "processing", "queued"):
            return await self._poll_job(job_id)

        return result

    async def _poll_job(self, job_id: str) -> dict:
        """Poll an async job until completion."""
        await self._ensure_session()
        poll_urls = [
            f"{self.base}/api/v1/job/{job_id}",
            f"{self.base}/api/job/{job_id}",
        ]

        for attempt in range(self.cfg.max_poll_attempts):
            await asyncio.sleep(self.cfg.poll_interval)
            for url in poll_urls:
                try:
                    async with self._session.get(
                        url, timeout=aiohttp.ClientTimeout(total=15)
                    ) as r:
                        if r.status == 404:
                            continue
                        body = await r.json()
                        status = body.get("status", "")
                        if status in ("completed", "done", "success"):
                            logger.info(f"Job {job_id} completed")
                            return body
                        elif status in ("failed", "error"):
                            logger.error(f"Job {job_id} failed: {body}")
                            return body
                        break  # still processing, continue polling
                except Exception:
                    continue

        return {"error": f"Job {job_id} timed out after {self.cfg.max_poll_attempts} polls"}

    # ------------------------------------------------------------------
    # High-level trading commands
    # ------------------------------------------------------------------

    async def place_order(self, coin: str, direction: str, size_usd: float,
                          leverage: int, entry_price: float,
                          stop_loss: float, take_profit: float,
                          strategy: str = "") -> dict:
        """Send an open-position command to the agent."""
        cmd = {
            "action": "open", "coin": coin, "direction": direction,
            "size_usd": size_usd, "leverage": leverage,
            "entry_price": entry_price, "stop_loss": stop_loss,
            "take_profit": take_profit, "strategy": strategy,
        }
        prompt = _open_prompt(cmd)
        logger.info(f"→ Agent OPEN {direction.upper()} {coin} "
                     f"${size_usd:.0f} lev={leverage}")
        return await self._send_and_poll(prompt)

    async def close_position(self, coin: str, direction: str,
                             reason: str = "") -> dict:
        """Send a close-position command to the agent."""
        cmd = {"action": "close", "coin": coin, "direction": direction,
               "reason": reason}
        prompt = _close_prompt(cmd)
        logger.info(f"→ Agent CLOSE {direction.upper()} {coin} reason={reason}")
        return await self._send_and_poll(prompt)

    async def get_agent_status(self) -> dict:
        """Query agent for health / balance / positions."""
        await self._ensure_session()

        # Try health endpoints (always available on OpenClaw Gateway)
        health_urls = [
            f"{self.base}/health",
            f"{self.base}/healthz",
            f"{self.base}/ready",
        ]
        for url in health_urls:
            try:
                async with self._session.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status == 200:
                        body = await r.json()
                        self._connected = True
                        return {
                            "status": "connected",
                            "connected": True,
                            "endpoint": self.base,
                            "details": body,
                        }
            except Exception:
                continue

        # If health endpoints fail, try sending a status prompt
        try:
            result = await self._send_prompt(_status_prompt())
            if "error" not in result:
                return {
                    "status": "connected",
                    "connected": True,
                    "endpoint": self.base,
                    "details": result,
                }
        except Exception:
            pass

        self._connected = False
        return {
            "status": "disconnected",
            "connected": False,
            "endpoint": self.base,
            "message": "Cannot reach OpenClaw Gateway",
        }

    # ------------------------------------------------------------------
    # Batch execution (called by trading loop)
    # ------------------------------------------------------------------

    async def execute_commands(self, commands: List[dict]) -> List[dict]:
        """Execute a batch of entry/exit commands from the strategy engine."""
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
                logger.error(f"Agent execution error for {cmd}: {e}")
                results.append({"cmd": cmd, "result": {"error": str(e)}})

        return results
