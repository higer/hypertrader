"""
Alert & logging system.
- Structured log files via Loguru
- Telegram bot rich notifications (primary channel)
- Webhook push (Slack / Discord / generic)
- DingTalk webhook
"""

import time, json
import aiohttp
from typing import Optional, List
from loguru import logger

from config import AlertConfig


class Notifier:

    def __init__(self, cfg: AlertConfig):
        self.cfg = cfg
        self._session: Optional[aiohttp.ClientSession] = None
        self._setup_loguru()
        self._tg_configured = bool(cfg.telegram_bot_token and cfg.telegram_chat_id)
        if self._tg_configured:
            logger.info(f"Telegram notifications enabled → chat_id={cfg.telegram_chat_id}")
        else:
            logger.warning("Telegram not configured — set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")

    def _setup_loguru(self):
        logger.add(
            self.cfg.log_file,
            rotation="10 MB",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
            level="DEBUG",
        )
        logger.info("Notifier initialised")

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()

    async def close(self):
        if self._session:
            await self._session.close()

    # ------------------------------------------------------------------
    # Push channels
    # ------------------------------------------------------------------

    async def _send_webhook(self, text: str):
        if not self.cfg.webhook_url:
            return
        await self._ensure_session()
        try:
            payload = {"text": text, "content": text}
            async with self._session.post(self.cfg.webhook_url, json=payload,
                                           timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status >= 400:
                    logger.warning(f"Webhook failed {r.status}")
        except Exception as e:
            logger.error(f"Webhook error: {e}")

    async def _send_telegram(self, text: str, parse_mode: str = "HTML"):
        if not self._tg_configured:
            return
        await self._ensure_session()
        url = f"https://api.telegram.org/bot{self.cfg.telegram_bot_token}/sendMessage"
        try:
            async with self._session.post(url, json={
                "chat_id": self.cfg.telegram_chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            }, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status >= 400:
                    body = await r.text()
                    logger.warning(f"Telegram API error {r.status}: {body}")
                else:
                    logger.debug("Telegram message sent")
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    async def _send_dingtalk(self, text: str):
        if not self.cfg.dingtalk_webhook:
            return
        await self._ensure_session()
        try:
            payload = {"msgtype": "text", "text": {"content": text}}
            async with self._session.post(self.cfg.dingtalk_webhook, json=payload,
                                           timeout=aiohttp.ClientTimeout(total=10)):
                pass
        except Exception as e:
            logger.error(f"DingTalk error: {e}")

    async def send(self, message: str, level: str = "info"):
        """Send plain text to all configured channels + log."""
        getattr(logger, level, logger.info)(f"[ALERT] {message}")
        await self._send_webhook(message)
        await self._send_telegram(message, parse_mode="HTML")
        await self._send_dingtalk(message)

    async def send_html(self, html: str, level: str = "info"):
        """Send HTML-formatted message to Telegram only."""
        logger.log(level.upper(), html[:200])
        await self._send_telegram(html, parse_mode="HTML")

    # ------------------------------------------------------------------
    # High-level alert helpers
    # ------------------------------------------------------------------

    async def on_signal(self, signal, dry_run: bool = True):
        """Rich Telegram notification for a trading signal."""
        direction_emoji = "🟢" if signal.direction == "long" else "🔴"
        mode_tag = "🔬 DRY RUN" if dry_run else "🔥 LIVE"

        rsi_val = signal.meta.get("rsi", "—")
        vol_ratio = signal.meta.get("vol_ratio", "—")
        atr_val = signal.meta.get("atr", "—")

        html = (
            f"<b>{direction_emoji} SIGNAL — {signal.coin} {signal.direction.upper()}</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"📊 Strategy: <b>{signal.strategy}</b>\n"
            f"💪 Strength: <b>{signal.strength:.0%}</b>\n"
            f"💰 Entry:    <code>{signal.entry_price}</code>\n"
            f"🛑 Stop:     <code>{signal.stop_loss}</code>\n"
            f"🎯 Target:   <code>{signal.take_profit}</code>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"RSI: {rsi_val} | Vol×: {vol_ratio} | ATR: {atr_val}\n"
            f"<i>{mode_tag}</i>"
        )
        await self.send_html(html)

    async def on_signals_batch(self, signals, dry_run: bool = True):
        """Summary of all signals found in one scan cycle."""
        if not signals:
            return
        mode_tag = "🔬 DRY RUN" if dry_run else "🔥 LIVE"
        lines = [f"<b>📡 Scan Complete — {len(signals)} signal(s) found</b>"]
        lines.append(f"<code>━━━━━━━━━━━━━━━━━━━━━</code>")
        for s in signals[:10]:  # max 10 in batch
            d_emoji = "🟢" if s.direction == "long" else "🔴"
            lines.append(
                f"{d_emoji} <b>{s.coin}</b> {s.direction.upper()} "
                f"| {s.strategy} | {s.strength:.0%} "
                f"| entry={s.entry_price}"
            )
        if len(signals) > 10:
            lines.append(f"<i>... and {len(signals)-10} more</i>")
        lines.append(f"\n<i>{mode_tag}</i>")
        await self.send_html("\n".join(lines))

    async def on_entry(self, cmd: dict, dry_run: bool = True):
        """Notification for simulated or real position entry."""
        direction_emoji = "🟢" if cmd.get("direction") == "long" else "🔴"
        mode_tag = "📝 SIMULATED" if dry_run else "✅ EXECUTED"
        html = (
            f"<b>{direction_emoji} OPEN {cmd.get('direction','?').upper()} {cmd.get('coin','?')}</b>\n"
            f"Size: <code>${cmd.get('size_usd',0):.0f}</code> | "
            f"Lev: <code>{cmd.get('leverage',0)}×</code>\n"
            f"Entry: <code>{cmd.get('entry_price','?')}</code> | "
            f"SL: <code>{cmd.get('stop_loss','?')}</code> | "
            f"TP: <code>{cmd.get('take_profit','?')}</code>\n"
            f"Strategy: {cmd.get('strategy','?')}\n"
            f"<i>{mode_tag}</i>"
        )
        await self.send_html(html)

    async def on_exit(self, cmd: dict, dry_run: bool = True):
        """Notification for simulated or real position exit."""
        pnl = cmd.get("pnl_pct", 0)
        pnl_emoji = "💚" if pnl > 0 else "💔"
        mode_tag = "📝 SIMULATED" if dry_run else "✅ EXECUTED"
        html = (
            f"<b>{pnl_emoji} CLOSE {cmd.get('direction','?').upper()} {cmd.get('coin','?')}</b>\n"
            f"Reason: <code>{cmd.get('reason','?')}</code>\n"
            f"PnL: <b>{pnl:+.2f}%</b> (${cmd.get('pnl_usd',0):+.2f})\n"
            f"<i>{mode_tag}</i>"
        )
        await self.send_html(html)

    async def on_order(self, cmd: dict, result: dict):
        """Notification when an order is sent to the OpenClaw Agent."""
        action = cmd.get("action", "?")
        coin = cmd.get("coin", "?")
        direction = cmd.get("direction", "?")
        error = result.get("error")

        if error:
            html = (
                f"<b>❌ AGENT ORDER FAILED</b>\n"
                f"<code>━━━━━━━━━━━━━━━━━━━━━</code>\n"
                f"Action: {action.upper()} {direction.upper()} {coin}\n"
            )
            if action == "open":
                html += f"Size: ${cmd.get('size_usd',0):.0f} | Lev: {cmd.get('leverage',0)}×\n"
            elif action == "close":
                html += f"Reason: {cmd.get('reason','')}\n"
            html += f"<b>Error:</b> <code>{str(error)[:200]}</code>"
            await self.send_html(html, "error")
        else:
            html = (
                f"<b>✅ AGENT ORDER SENT</b>\n"
                f"<code>━━━━━━━━━━━━━━━━━━━━━</code>\n"
                f"Action: {action.upper()} {direction.upper()} {coin}\n"
            )
            if action == "open":
                html += (
                    f"Size: <code>${cmd.get('size_usd',0):.0f}</code> | "
                    f"Lev: <code>{cmd.get('leverage',0)}×</code>\n"
                    f"SL: <code>{cmd.get('stop_loss','?')}</code> | "
                    f"TP: <code>{cmd.get('take_profit','?')}</code>\n"
                    f"Strategy: {cmd.get('strategy','?')}\n"
                )
            elif action == "close":
                html += f"Reason: {cmd.get('reason','')}\n"
            # Include agent response snippet
            resp_str = json.dumps(result, default=str)[:150]
            html += f"<i>Agent: {resp_str}</i>"
            await self.send_html(html)

    async def on_risk_event(self, event: str, details: str = ""):
        html = f"<b>⚠️ RISK — {event}</b>\n{details}"
        await self.send_html(html, "warning")

    async def on_system(self, event: str):
        html = f"<b>⚙️ SYSTEM</b>\n{event}"
        await self.send_html(html)

    async def on_stats_summary(self, stats: dict):
        """Periodic stats summary pushed to Telegram."""
        html = (
            f"<b>📈 Portfolio Summary</b>\n"
            f"<code>━━━━━━━━━━━━━━━━━━━━━</code>\n"
            f"💰 Equity:     <code>${stats.get('equity', 0):,.2f}</code>\n"
            f"📊 Total PnL:  <code>${stats.get('total_pnl', 0):+,.2f}</code>\n"
            f"🎯 Win Rate:   <code>{stats.get('win_rate', 0):.1f}%</code>\n"
            f"📉 Drawdown:   <code>{stats.get('drawdown_pct', 0):.1f}%</code>\n"
            f"📂 Open Pos:   <code>{stats.get('open_positions', 0)}</code>\n"
            f"📋 Total Trades: <code>{stats.get('total_trades', 0)}</code>\n"
        )
        await self.send_html(html)
