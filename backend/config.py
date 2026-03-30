"""
Global configuration for the Hyperliquid Perps Trading System.
All tuneable parameters are centralised here.
"""

from pydantic import BaseModel, Field
from typing import Optional
import json, os
from dotenv import load_dotenv

load_dotenv()  # load .env file if present

# ---------------------------------------------------------------------------
# Hyperliquid endpoints
# ---------------------------------------------------------------------------
HYPERLIQUID_REST = "https://api.hyperliquid.xyz"
HYPERLIQUID_WS = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO = f"{HYPERLIQUID_REST}/info"

# ---------------------------------------------------------------------------
# Market-cap filter: only trade coins whose 24h volume ranks in the top-N
# ---------------------------------------------------------------------------
DEFAULT_TOP_N = 30          # top-30 by open-interest / volume
MIN_24H_VOLUME_USD = 5_000_000  # additional floor

# ---------------------------------------------------------------------------
# Strategy parameter schemas
# ---------------------------------------------------------------------------

class RiskConfig(BaseModel):
    max_position_pct: float = Field(0.05, description="Max % of equity per position")
    max_drawdown_pct: float = Field(0.08, description="Hard drawdown stop (8%)")
    max_open_positions: int = Field(6, description="Max simultaneous positions")
    trailing_stop_pct: float = Field(0.03, description="Trailing stop 3%")
    daily_loss_limit_pct: float = Field(0.04, description="Daily loss cap 4%")
    leverage: int = Field(3, ge=1, le=20, description="Default leverage")

class MomentumConfig(BaseModel):
    enabled: bool = True
    fast_ema: int = 8
    slow_ema: int = 21
    rsi_period: int = 14
    rsi_ob: float = 75          # overbought
    rsi_os: float = 25          # oversold
    volume_surge_mult: float = 1.3  # lowered for more signals
    atr_period: int = 14
    atr_sl_mult: float = 1.5   # SL = atr_sl_mult * ATR
    atr_tp_mult: float = 3.0   # TP = atr_tp_mult * ATR
    cooldown_bars: int = 3      # bars after exit before re-entry

class MeanReversionConfig(BaseModel):
    enabled: bool = True
    bb_period: int = 20
    bb_std: float = 1.8        # tighter bands = more touches
    rsi_period: int = 14
    rsi_entry_low: float = 32  # loosened from 25
    rsi_entry_high: float = 68 # loosened from 75
    holding_bars_max: int = 12
    atr_period: int = 14
    atr_sl_mult: float = 1.2
    atr_tp_mult: float = 2.0

class BreakoutConfig(BaseModel):
    enabled: bool = True
    lookback: int = 24          # shorter lookback for more signals
    volume_confirm_mult: float = 1.4  # lowered from 1.8
    atr_period: int = 14
    atr_sl_mult: float = 1.5
    atr_tp_mult: float = 3.5
    min_consolidation_bars: int = 8

class FundingArbConfig(BaseModel):
    enabled: bool = True          # enabled by default
    funding_threshold: float = 0.0003  # 0.03% — more sensitive
    holding_hours: int = 8
    max_position_pct: float = 0.03

class AlertConfig(BaseModel):
    webhook_url: Optional[str] = None
    telegram_bot_token: Optional[str] = Field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN"))
    telegram_chat_id: Optional[str] = Field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID"))
    dingtalk_webhook: Optional[str] = None
    log_file: str = "trading.log"

class OpenClawConfig(BaseModel):
    agent_endpoint: str = Field(
        default_factory=lambda: os.getenv("OPENCLAW_ENDPOINT", "http://127.0.0.1:18789"),
        description="OpenClaw Gateway URL"
    )
    api_key: Optional[str] = Field(
        default_factory=lambda: os.getenv("OPENCLAW_API_KEY"),
        description="API key / bearer token for OpenClaw"
    )
    timeout: int = 60           # agent may need time to process
    poll_interval: float = 2.0  # seconds between job status polls
    max_poll_attempts: int = 30 # max polls before timeout
    confirm_before_execute: bool = True  # require confirmation for large trades

class DGClawConfig(BaseModel):
    enabled: bool = Field(True, description="Use DGClaw direct executor instead of OpenClaw")
    api_key: Optional[str] = Field(
        default_factory=lambda: os.getenv("DGCLAW_API_KEY"),
        description="DGClaw API key for leaderboard/forum"
    )
    acp_api_key: Optional[str] = Field(
        default_factory=lambda: os.getenv("LITE_AGENT_API_KEY"),
        description="ACP API key from `acp setup` (LITE_AGENT_API_KEY)"
    )
    acp_builder_code: Optional[str] = Field(
        default_factory=lambda: os.getenv("ACP_BUILDER_CODE"),
        description="ACP builder code (optional)"
    )
    poll_interval: float = Field(3.0, description="Seconds between ACP job status polls")
    max_poll_attempts: int = Field(60, description="Max polls before timeout (60×3s = 3min)")
    job_create_timeout: float = Field(30.0, description="Timeout for ACP job create request")
    max_auto_pay_usd: float = Field(0.05, description="Max ACP fee to auto-approve ($)")
    confirm_before_execute: bool = Field(False, description="Require confirmation (disabled for HFT)")

class SystemConfig(BaseModel):
    dry_run: bool = True                     # True = no real execution, signals only
    risk: RiskConfig = RiskConfig()
    momentum: MomentumConfig = MomentumConfig()
    mean_reversion: MeanReversionConfig = MeanReversionConfig()
    breakout: BreakoutConfig = BreakoutConfig()
    funding_arb: FundingArbConfig = FundingArbConfig()
    alert: AlertConfig = AlertConfig()
    openclaw: OpenClawConfig = OpenClawConfig()
    dgclaw: DGClawConfig = DGClawConfig()
    top_n: int = DEFAULT_TOP_N
    candle_interval: str = "15m"            # 1m,5m,15m,1h,4h,1d
    data_lookback_bars: int = 200
    poll_interval_sec: int = 15

    def save(self, path: str = "system_config.json"):
        with open(path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load(cls, path: str = "system_config.json") -> "SystemConfig":
        if os.path.exists(path):
            with open(path) as f:
                return cls.model_validate_json(f.read())
        return cls()
