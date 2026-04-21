"""
Degenerate Claw (dgclaw) executor — v4.0 architecture.

Trades directly on Hyperliquid via API wallet (EIP-712 signed orders),
bypassing the ACP job layer entirely. ACP is only used for USDC deposits.

Flow:
  1. Sign order with HL_API_WALLET_KEY (API wallet private key)
  2. POST signed action to https://api.hyperliquid.xyz/exchange
  3. Set TP/SL as trigger orders
  4. Query positions/balance via /info endpoint

Requirements:
  - HL_API_WALLET_KEY: API wallet private key (from add-api-wallet.ts)
  - HL_MASTER_ADDRESS: Master wallet address (ACP agent wallet)

References:
  - dgclaw-skill v4.0: https://github.com/Virtual-Protocol/dgclaw-skill
  - Hyperliquid API: https://hyperliquid.gitbook.io/hyperliquid-docs
"""

import asyncio
import json
import math
import os
import time
from typing import Optional, List, Dict, Any

import aiohttp
from loguru import logger

from config import DGClawConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HL_API_BASE = "https://api.hyperliquid.xyz"
HL_INFO_URL = f"{HL_API_BASE}/info"
HL_EXCHANGE_URL = f"{HL_API_BASE}/exchange"

DGCLAW_TRADER_WALLET = "0xd478a8B40372db16cA8045F28C6FE07228F3781A"
DGCLAW_FORUM_BASE = "https://degen.virtuals.io"


# ---------------------------------------------------------------------------
# Hyperliquid EIP-712 signing helpers
# ---------------------------------------------------------------------------

def _resolve_key(cfg: DGClawConfig) -> str:
    """
    Resolve HL_API_WALLET_KEY from multiple sources (priority order):
      1. cfg.hl_api_wallet_key  (from system_config.json or in-memory config)
      2. os.getenv("HL_API_WALLET_KEY")  (from .env / shell environment)
    Pydantic's default_factory is skipped when JSON has explicit null,
    so we must always check env as fallback.
    """
    key = (cfg.hl_api_wallet_key or "").strip()
    if not key:
        key = os.getenv("HL_API_WALLET_KEY", "").strip()
    return key


def _resolve_master(cfg: DGClawConfig) -> str:
    """
    Resolve HL_MASTER_ADDRESS from config or env.
    Also checks HL_API_WALLET_ADDRESS as a common alternative name.
    """
    addr = (cfg.hl_master_address or "").strip()
    if not addr:
        addr = os.getenv("HL_MASTER_ADDRESS", "").strip()
    if not addr:
        # Common alternative name users might use
        addr = os.getenv("HL_API_WALLET_ADDRESS", "").strip()
    return addr


def _load_wallet(cfg: DGClawConfig):
    """Load API wallet from config or env."""
    key = _resolve_key(cfg)
    if not key:
        return None, None, "HL_API_WALLET_KEY is empty — set it in .env or config"
    try:
        from eth_account import Account
        if not key.startswith("0x"):
            key = "0x" + key
        account = Account.from_key(key)
        return account, key, None
    except Exception as e:
        logger.error(f"Failed to load API wallet: {e}")
        return None, None, f"HL_API_WALLET_KEY is set but invalid: {e}"


def _get_master_address(cfg: DGClawConfig) -> Optional[str]:
    """Get master wallet address."""
    addr = _resolve_master(cfg)
    return addr if addr else None


# ---------------------------------------------------------------------------
# Executor class
# ---------------------------------------------------------------------------

class DGClawExecutor:
    """
    Direct Hyperliquid executor for dgclaw-skill v4.0.
    Trades via API wallet signing, queries via info endpoint.
    """

    def __init__(self, cfg: DGClawConfig):
        self.cfg = cfg
        self.order_log: List[dict] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._connected: Optional[bool] = None
        self._account = None
        self._api_key = None
        self._wallet_error: Optional[str] = None
        self._master_address: Optional[str] = None
        self._asset_meta: Dict[str, dict] = {}  # coin → {index, szDecimals, maxLeverage}
        self._meta_ts: float = 0  # last meta refresh time

        # Load wallet
        self._reload_wallet()

    def _reload_wallet(self):
        """(Re-)load wallet credentials from config + env vars."""
        self._account, self._api_key, self._wallet_error = _load_wallet(self.cfg)
        self._master_address = _get_master_address(self.cfg)

        if self._account:
            logger.info(f"DGClaw executor: API wallet {self._account.address}")
        else:
            logger.warning(f"DGClaw executor: {self._wallet_error or 'wallet not loaded'}")
        if self._master_address:
            logger.info(f"DGClaw executor: master address {self._master_address}")
        else:
            logger.warning("DGClaw executor: HL_MASTER_ADDRESS not set — queries disabled")

    async def _ensure_session(self):
        if self._session and not self._session.closed:
            return
        self._session = aiohttp.ClientSession(
            headers={"Content-Type": "application/json"}
        )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Hyperliquid Info API (queries — no signing needed)
    # ------------------------------------------------------------------

    async def _info_post(self, payload: dict) -> dict:
        """POST to /info endpoint."""
        await self._ensure_session()
        try:
            async with self._session.post(
                HL_INFO_URL, json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                body = await r.json()
                if r.status >= 400:
                    return {"error": f"HTTP {r.status}: {json.dumps(body)[:300]}"}
                return body if isinstance(body, dict) else {"data": body}
        except Exception as e:
            return {"error": f"Info request failed: {e}"}

    async def _refresh_meta(self):
        """Refresh asset metadata (cached for 5 min)."""
        if time.time() - self._meta_ts < 300 and self._asset_meta:
            return
        result = await self._info_post({"type": "meta"})
        if "error" in result:
            logger.warning(f"Meta refresh failed: {result['error']}")
            return
        universe = result.get("universe", [])
        if not universe and isinstance(result.get("data"), dict):
            universe = result["data"].get("universe", [])
        for i, asset in enumerate(universe):
            name = asset.get("name", "").upper()
            self._asset_meta[name] = {
                "index": i,
                "szDecimals": asset.get("szDecimals", 2),
                "maxLeverage": asset.get("maxLeverage", 50),
            }
        self._meta_ts = time.time()
        logger.debug(f"HL meta refreshed: {len(self._asset_meta)} assets")

    def _get_asset(self, coin: str) -> Optional[dict]:
        return self._asset_meta.get(coin.upper())

    async def _get_all_mids(self) -> Dict[str, float]:
        """Get mid prices for all assets."""
        result = await self._info_post({"type": "allMids"})
        if "error" in result:
            return {}
        # Response is {coin: midPrice, ...}
        mids = {}
        data = result.get("data", result)
        if isinstance(data, dict):
            for k, v in data.items():
                try:
                    mids[k.upper()] = float(v)
                except (ValueError, TypeError):
                    pass
        return mids

    async def get_positions(self) -> dict:
        """Query open positions from Hyperliquid."""
        if not self._master_address:
            return {"error": "HL_MASTER_ADDRESS not set"}
        result = await self._info_post({
            "type": "clearinghouseState",
            "user": self._master_address,
        })
        if "error" in result:
            return result
        positions = []
        for p in result.get("assetPositions", []):
            pos = p.get("position", {})
            szi = float(pos.get("szi", 0))
            if szi == 0:
                continue
            positions.append({
                "coin": pos.get("coin", ""),
                "side": "long" if szi > 0 else "short",
                "size": abs(szi),
                "entryPrice": pos.get("entryPx", "0"),
                "markPrice": pos.get("positionValue", "0"),
                "unrealizedPnl": pos.get("unrealizedPnl", "0"),
                "leverage": pos.get("leverage", {}).get("value", 1),
                "liquidationPrice": pos.get("liquidationPx"),
            })
        return {"positions": positions}

    async def get_account(self) -> dict:
        """Query account balance."""
        if not self._master_address:
            return {"error": "HL_MASTER_ADDRESS not set"}
        # Get both spot and perp state
        perp = await self._info_post({
            "type": "clearinghouseState",
            "user": self._master_address,
        })
        spot = await self._info_post({
            "type": "spotClearinghouseState",
            "user": self._master_address,
        })
        result = {}
        if "error" not in perp:
            ms = perp.get("marginSummary", {})
            result["perp"] = {
                "accountValue": ms.get("accountValue", "0"),
                "totalMarginUsed": ms.get("totalMarginUsed", "0"),
                "withdrawable": perp.get("withdrawable", "0"),
            }
        if "error" not in spot:
            result["spot"] = {
                "balances": spot.get("balances", []),
            }
        return result if result else {"error": "Failed to query account"}

    async def get_trade_history(self, pair: str = None, limit: int = 50) -> dict:
        """Query trade fills from Hyperliquid."""
        if not self._master_address:
            return {"error": "HL_MASTER_ADDRESS not set"}
        result = await self._info_post({
            "type": "userFills",
            "user": self._master_address,
        })
        if "error" in result:
            return result
        fills = result if isinstance(result, list) else result.get("data", [])
        if pair:
            fills = [f for f in fills if f.get("coin", "").upper() == pair.upper()]
        return {"trades": fills[:limit]}

    async def get_tickers(self) -> dict:
        """Get all available trading pairs with mid prices."""
        await self._refresh_meta()
        mids = await self._get_all_mids()
        tickers = []
        for coin, meta in self._asset_meta.items():
            tickers.append({
                "symbol": coin,
                "midPrice": mids.get(coin, 0),
                "maxLeverage": meta["maxLeverage"],
                "szDecimals": meta["szDecimals"],
            })
        return {"tickers": tickers}

    # ------------------------------------------------------------------
    # Hyperliquid Exchange API (signed actions)
    # ------------------------------------------------------------------

    async def _exchange_post(self, action: dict, nonce: int = None,
                             vault_address: str = None) -> dict:
        """
        Sign and POST an action to /exchange.
        Uses the Hyperliquid Python SDK for proper EIP-712 signing.
        """
        if not self._account:
            # Try reloading — user may have updated .env since startup
            self._reload_wallet()
            if not self._account:
                err = self._wallet_error or "HL_API_WALLET_KEY not set"
                return {"error": f"{err} — cannot trade"}

        await self._ensure_session()

        if nonce is None:
            nonce = int(time.time() * 1000)

        try:
            from hyperliquid.utils.signing import sign_l1_action
            # SDK signature: sign_l1_action(wallet, action, active_pool, nonce, expires_after, is_mainnet)
            signature = sign_l1_action(
                self._account, action, vault_address, nonce, None, True
            )
            payload = {
                "action": action,
                "nonce": nonce,
                "signature": signature,
            }
            if vault_address:
                payload["vaultAddress"] = vault_address

            async with self._session.post(
                HL_EXCHANGE_URL, json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                body = await r.json()
                if r.status >= 400:
                    return {"error": f"HTTP {r.status}: {json.dumps(body)[:300]}"}
                return body
        except ImportError:
            return await self._exchange_post_raw(action, nonce, vault_address)
        except Exception as e:
            logger.error(f"Exchange POST failed: {e}")
            return {"error": f"Exchange request failed: {e}"}

    async def _exchange_post_raw(self, action: dict, nonce: int,
                                 vault_address: str = None) -> dict:
        """
        Fallback: Sign and POST using raw eth_account EIP-712.
        Replicates the exact signing logic from hyperliquid-python-sdk:
          1. msgpack(action) + nonce bytes + vault flag → keccak hash
          2. Construct phantom agent {source: "a", connectionId: hash}
          3. EIP-712 sign the phantom agent
        Requires: eth_account, msgpack, eth_utils
        """
        try:
            import msgpack
            from eth_account.messages import encode_typed_data
            from eth_utils import keccak, to_hex

            # Step 1: Hash the action (same as SDK's action_hash)
            data = msgpack.packb(action)
            data += nonce.to_bytes(8, "big")
            if vault_address is None:
                data += b"\x00"
            else:
                data += b"\x01"
                addr_hex = vault_address[2:] if vault_address.startswith("0x") else vault_address
                data += bytes.fromhex(addr_hex)
            # expires_after = None → no extra bytes
            action_hash = keccak(data)

            # Step 2: Construct phantom agent
            phantom_agent = {"source": "a", "connectionId": action_hash}

            # Step 3: Build EIP-712 typed data
            typed_data = {
                "domain": {
                    "chainId": 1337,
                    "name": "Exchange",
                    "verifyingContract": "0x0000000000000000000000000000000000000000",
                    "version": "1",
                },
                "types": {
                    "Agent": [
                        {"name": "source", "type": "string"},
                        {"name": "connectionId", "type": "bytes32"},
                    ],
                    "EIP712Domain": [
                        {"name": "name", "type": "string"},
                        {"name": "version", "type": "string"},
                        {"name": "chainId", "type": "uint256"},
                        {"name": "verifyingContract", "type": "address"},
                    ],
                },
                "primaryType": "Agent",
                "message": phantom_agent,
            }

            # Step 4: Sign
            structured_data = encode_typed_data(full_message=typed_data)
            signed = self._account.sign_message(structured_data)
            signature = {
                "r": to_hex(signed["r"]),
                "s": to_hex(signed["s"]),
                "v": signed["v"],
            }

            payload = {
                "action": action,
                "nonce": nonce,
                "signature": signature,
            }
            if vault_address:
                payload["vaultAddress"] = vault_address

            async with self._session.post(
                HL_EXCHANGE_URL, json=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                body = await r.json()
                if r.status >= 400:
                    return {"error": f"HTTP {r.status}: {json.dumps(body)[:300]}"}
                return body
        except ImportError as e:
            logger.error(f"Missing dependency for raw signing: {e}")
            return {
                "error": f"Missing package: {e}. Install with: "
                         f"pip install hyperliquid-python-sdk (recommended) "
                         f"or pip install msgpack eth-utils eth-account"
            }
        except Exception as e:
            logger.error(f"Raw exchange POST failed: {e}")
            return {"error": f"Signing/exchange failed: {e}"}

    # ------------------------------------------------------------------
    # Trading helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_price(price: float, sig_figs: int = 5) -> str:
        """Format price to significant figures."""
        if price == 0:
            return "0"
        return f"{price:.{sig_figs}g}"

    @staticmethod
    def _format_size(usd_size: float, price: float, sz_decimals: int) -> str:
        """Convert USD notional to asset size."""
        if price <= 0:
            return "0"
        raw = usd_size / price
        return f"{raw:.{sz_decimals}f}"

    # ------------------------------------------------------------------
    # High-level trading commands
    # ------------------------------------------------------------------

    async def place_order(self, coin: str, direction: str, size_usd: float,
                          leverage: int, entry_price: float,
                          stop_loss: float, take_profit: float,
                          strategy: str = "") -> dict:
        """Open a position on Hyperliquid via direct API."""
        await self._refresh_meta()
        asset = self._get_asset(coin)
        if not asset:
            return {"error": f"Unknown asset: {coin}. Run tickers to see available pairs."}

        asset_id = asset["index"]
        sz_decimals = asset["szDecimals"]
        is_buy = direction == "long"

        logger.info(
            f"→ DGCLAW OPEN {direction.upper()} {coin} "
            f"${size_usd:.0f} lev={leverage} SL={stop_loss} TP={take_profit}"
        )

        record = {
            "ts": time.time(), "action": "open",
            "coin": coin, "direction": direction,
        }

        # 1. Set leverage
        lev_result = await self._exchange_post({
            "type": "updateLeverage",
            "asset": asset_id,
            "isCross": True,
            "leverage": leverage,
        })
        if "error" in lev_result:
            logger.warning(f"Leverage set warning: {lev_result['error']}")
            # Continue anyway — leverage might already be set

        # 2. Get mid price for market order
        mids = await self._get_all_mids()
        mid_price = mids.get(coin.upper(), entry_price)
        if mid_price <= 0:
            record["error"] = f"No mid price for {coin}"
            self._append_log(record)
            return {"error": f"Could not get mid price for {coin}"}

        # Market order with slippage
        slippage = self.cfg.slippage_pct
        order_price = mid_price * (1 + slippage) if is_buy else mid_price * (1 - slippage)
        order_price_str = self._format_price(order_price)
        sz = self._format_size(size_usd, mid_price, sz_decimals)

        logger.info(f"  size={sz} price={order_price_str} mid={mid_price}")

        # 3. Place main order
        order_result = await self._exchange_post({
            "type": "order",
            "orders": [{
                "a": asset_id,
                "b": is_buy,
                "p": order_price_str,
                "s": sz,
                "r": False,
                "t": {"limit": {"tif": "Ioc"}},
            }],
            "grouping": "na",
        })

        record["order_result"] = order_result
        if "error" in order_result:
            record["error"] = order_result["error"]
            self._append_log(record)
            return order_result

        # Check if order was filled
        status = order_result.get("status", order_result.get("response", {}).get("type", ""))
        statuses = order_result.get("response", {}).get("data", {}).get("statuses", [])
        if statuses:
            first_status = statuses[0] if statuses else {}
            if isinstance(first_status, dict) and "error" in first_status:
                record["error"] = first_status["error"]
                self._append_log(record)
                return {"error": first_status["error"]}

        logger.info(f"Order placed: {json.dumps(order_result)[:200]}")

        # 4. Set TP trigger order
        if take_profit and take_profit > 0:
            tp_result = await self._exchange_post({
                "type": "order",
                "orders": [{
                    "a": asset_id,
                    "b": not is_buy,
                    "p": self._format_price(take_profit),
                    "s": sz,
                    "r": True,
                    "t": {"trigger": {
                        "triggerPx": self._format_price(take_profit),
                        "isMarket": True,
                        "tpsl": "tp",
                    }},
                }],
                "grouping": "na",
            })
            if "error" not in tp_result:
                logger.info(f"TP set at {take_profit}")
            else:
                logger.warning(f"TP set failed: {tp_result['error']}")

        # 5. Set SL trigger order
        if stop_loss and stop_loss > 0:
            sl_result = await self._exchange_post({
                "type": "order",
                "orders": [{
                    "a": asset_id,
                    "b": not is_buy,
                    "p": self._format_price(stop_loss),
                    "s": sz,
                    "r": True,
                    "t": {"trigger": {
                        "triggerPx": self._format_price(stop_loss),
                        "isMarket": True,
                        "tpsl": "sl",
                    }},
                }],
                "grouping": "na",
            })
            if "error" not in sl_result:
                logger.info(f"SL set at {stop_loss}")
            else:
                logger.warning(f"SL set failed: {sl_result['error']}")

        result = {"status": "completed", "order": order_result}
        record["result"] = result
        self._append_log(record)
        return result

    async def close_position(self, coin: str, direction: str,
                             reason: str = "") -> dict:
        """Close a position on Hyperliquid."""
        await self._refresh_meta()
        asset = self._get_asset(coin)
        if not asset:
            return {"error": f"Unknown asset: {coin}"}

        asset_id = asset["index"]

        logger.info(f"→ DGCLAW CLOSE {direction.upper()} {coin} reason={reason}")

        record = {
            "ts": time.time(), "action": "close",
            "coin": coin, "direction": direction, "reason": reason,
        }

        # Get current position size
        if not self._master_address:
            record["error"] = "HL_MASTER_ADDRESS not set"
            self._append_log(record)
            return {"error": "HL_MASTER_ADDRESS not set"}

        state = await self._info_post({
            "type": "clearinghouseState",
            "user": self._master_address,
        })
        if "error" in state:
            record["error"] = state["error"]
            self._append_log(record)
            return state

        # Find the position
        pos_data = None
        for p in state.get("assetPositions", []):
            pos = p.get("position", {})
            if pos.get("coin", "").upper() == coin.upper():
                szi = float(pos.get("szi", 0))
                if szi != 0:
                    pos_data = {"szi": szi, "coin": pos["coin"]}
                    break

        if not pos_data:
            record["error"] = f"No open position for {coin}"
            self._append_log(record)
            return {"error": f"No open position for {coin}"}

        szi = pos_data["szi"]
        is_buy = szi < 0  # Close short = buy, close long = sell
        sz = str(abs(szi))

        # Market close with slippage
        mids = await self._get_all_mids()
        mid_price = mids.get(coin.upper(), 0)
        if mid_price <= 0:
            record["error"] = f"No mid price for {coin}"
            self._append_log(record)
            return {"error": f"Could not get mid price for {coin}"}

        slippage = self.cfg.slippage_pct
        order_price = mid_price * (1 + slippage) if is_buy else mid_price * (1 - slippage)
        order_price_str = self._format_price(order_price)

        logger.info(f"  closing size={sz} price={order_price_str}")

        order_result = await self._exchange_post({
            "type": "order",
            "orders": [{
                "a": asset_id,
                "b": is_buy,
                "p": order_price_str,
                "s": sz,
                "r": True,  # reduce-only
                "t": {"limit": {"tif": "Ioc"}},
            }],
            "grouping": "na",
        })

        record["order_result"] = order_result
        if "error" in order_result:
            record["error"] = order_result["error"]
            self._append_log(record)
            return order_result

        result = {"status": "completed", "order": order_result}
        record["result"] = result
        self._append_log(record)
        return result

    async def modify_position(self, coin: str, leverage: int = None,
                              stop_loss: float = None,
                              take_profit: float = None) -> dict:
        """Modify an open position's SL/TP/leverage."""
        await self._refresh_meta()
        asset = self._get_asset(coin)
        if not asset:
            return {"error": f"Unknown asset: {coin}"}

        asset_id = asset["index"]
        results = {}

        # Update leverage
        if leverage is not None:
            lev_result = await self._exchange_post({
                "type": "updateLeverage",
                "asset": asset_id,
                "isCross": True,
                "leverage": leverage,
            })
            results["leverage"] = lev_result
            logger.info(f"Leverage updated to {leverage}x")

        # Get position to know direction and size
        if stop_loss or take_profit:
            if not self._master_address:
                return {"error": "HL_MASTER_ADDRESS not set"}

            state = await self._info_post({
                "type": "clearinghouseState",
                "user": self._master_address,
            })
            if "error" in state:
                return state

            pos_data = None
            for p in state.get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin", "").upper() == coin.upper():
                    szi = float(pos.get("szi", 0))
                    if szi != 0:
                        pos_data = {"szi": szi}
                        break

            if not pos_data:
                return {"error": f"No open position for {coin}"}

            is_long = pos_data["szi"] > 0
            sz = str(abs(pos_data["szi"]))

            # Cancel existing TP/SL orders
            open_orders = await self._info_post({
                "type": "openOrders",
                "user": self._master_address,
            })
            if "error" not in open_orders:
                orders_data = open_orders if isinstance(open_orders, list) else open_orders.get("data", [])
                for o in orders_data:
                    if (o.get("coin", "").upper() == coin.upper()
                            and "trigger" in str(o.get("orderType", "")).lower()):
                        try:
                            await self._exchange_post({
                                "type": "cancel",
                                "cancels": [{"a": asset_id, "o": o["oid"]}],
                            })
                        except Exception:
                            pass

            if take_profit:
                tp_result = await self._exchange_post({
                    "type": "order",
                    "orders": [{
                        "a": asset_id,
                        "b": not is_long,
                        "p": self._format_price(take_profit),
                        "s": sz,
                        "r": True,
                        "t": {"trigger": {
                            "triggerPx": self._format_price(take_profit),
                            "isMarket": True,
                            "tpsl": "tp",
                        }},
                    }],
                    "grouping": "na",
                })
                results["take_profit"] = tp_result

            if stop_loss:
                sl_result = await self._exchange_post({
                    "type": "order",
                    "orders": [{
                        "a": asset_id,
                        "b": not is_long,
                        "p": self._format_price(stop_loss),
                        "s": sz,
                        "r": True,
                        "t": {"trigger": {
                            "triggerPx": self._format_price(stop_loss),
                            "isMarket": True,
                            "tpsl": "sl",
                        }},
                    }],
                    "grouping": "na",
                })
                results["stop_loss"] = sl_result

        return {"status": "completed", "results": results}

    # ------------------------------------------------------------------
    # Agent status
    # ------------------------------------------------------------------

    async def get_agent_status(self) -> dict:
        """Check Hyperliquid connectivity and account status."""
        has_wallet = self._account is not None
        has_master = self._master_address is not None

        if not has_wallet and not has_master:
            self._connected = False
            return {
                "status": "disconnected",
                "connected": False,
                "error": "HL_API_WALLET_KEY and HL_MASTER_ADDRESS not set",
                "message": "Set API wallet key and master address in .env",
            }

        # Test connectivity
        account = await self.get_account()
        positions = await self.get_positions()

        self._connected = "error" not in account

        return {
            "status": "connected" if self._connected else "disconnected",
            "connected": self._connected,
            "executor": "dgclaw-v4",
            "mode": "direct-hl",
            "wallet": self._account.address if self._account else None,
            "master_address": self._master_address,
            "trader_wallet": DGCLAW_TRADER_WALLET,
            "account": account if "error" not in account else None,
            "positions": positions if "error" not in positions else None,
            "hl_api": HL_API_BASE,
            "forum": DGCLAW_FORUM_BASE,
        }

    # ------------------------------------------------------------------
    # Batch execution (called by trading loop)
    # ------------------------------------------------------------------

    async def execute_commands(self, commands: List[dict]) -> List[dict]:
        """Execute a batch of entry/exit commands via Hyperliquid direct API."""
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
