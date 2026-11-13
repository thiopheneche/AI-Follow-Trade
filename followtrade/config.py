import argparse
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from probe import portfolio_id


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class AIConfig:
    base_url: str
    model: str
    token: str
    timeout: int
    profile: str


@dataclass(frozen=True)
class TelegramConfig:
    token: str
    chat_id: int
    push_fills: bool


@dataclass(frozen=True)
class Config:
    portfolio: str
    database: Path
    starting: Decimal
    poll_seconds: int
    max_age: int
    fixed_margin: Decimal
    fixed_leverage: int
    max_leverage: int
    symbol_limit: Decimal
    gross_limit: Decimal
    margin_limit: Decimal
    deviation: Decimal
    fee: Decimal
    slip: Decimal
    minimum: Decimal
    daily_loss: Decimal
    position_stop: Decimal
    ai: AIConfig = None
    telegram: TelegramConfig = None


def decimal_setting(raw, key, default, low, high):
    try:
        value = Decimal(str(raw.get(key, default)))
    except InvalidOperation:
        raise ConfigError(f"invalid number for {key}") from None
    if not value.is_finite() or not Decimal(low) <= value <= Decimal(high):
        raise ConfigError(f"{key} must be between {low} and {high}")
    return value


def integer_setting(raw, key, default, low, high):
    value = raw.get(key, default)
    if type(value) is not int or not low <= value <= high:
        raise ConfigError(f"{key} must be an integer between {low} and {high}")
    return value


def enabled(raw, key, default=False):
    value = raw.get(key, default)
    if type(value) is not bool:
        raise ConfigError(f"{key} must be true or false")
    return value


def load_config(path):
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ConfigError("configuration must be a JSON object")
    try:
        portfolio = portfolio_id(raw.get("portfolio_id", "5075281354358777856"))
    except (argparse.ArgumentTypeError, TypeError):
        raise ConfigError("invalid portfolio_id") from None
    ai, telegram = None, None
    ai_raw, tg_raw = raw.get("ai", {}), raw.get("telegram", {})
    if not isinstance(ai_raw, dict) or not isinstance(tg_raw, dict):
        raise ConfigError("ai and telegram must be JSON objects")
    if enabled(ai_raw, "enabled"):
        base_url, model, profile = (ai_raw.get(k, "") for k in ("base_url", "model", "trader_profile"))
        token = os.getenv("FOLLOWTRADE_AI_KEY", "")
        if not all(isinstance(value, str) and value.strip() for value in (base_url, model, profile, token)):
            raise ConfigError("AI requires base_url, model, trader_profile and FOLLOWTRADE_AI_KEY")
        url = urlsplit(base_url)
        if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ConfigError("ai.base_url must be an HTTPS API base URL")
        ai = AIConfig(base_url.rstrip("/"), model, token,
                      integer_setting(ai_raw, "timeout_seconds", 10, 1, 30), profile)
    chat = tg_raw.get("chat_id")
    if chat is not None:
        token = os.getenv("FOLLOWTRADE_TELEGRAM_TOKEN", "")
        if type(chat) is not int or chat <= 0 or not token:
            raise ConfigError("Telegram requires a positive private chat_id and FOLLOWTRADE_TELEGRAM_TOKEN")
        if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token):
            raise ConfigError("invalid Telegram token format")
        telegram = TelegramConfig(token, chat, enabled(tg_raw, "push_fills", True))
    database = Path(raw.get("database", "state/paper.sqlite3"))
    if not database.is_absolute():
        database = path.parent / database
    fixed = integer_setting(raw, "fixed_leverage", 3, 1, 125)
    ceiling = integer_setting(raw, "max_leverage", 10, 1, 125)
    if fixed > ceiling:
        raise ConfigError("fixed_leverage exceeds max_leverage")
    slip = decimal_setting(raw, "extra_slippage_fraction", "0.0005", "0", "0.1")
    return Config(
        portfolio, database, decimal_setting(raw, "starting_usdt", "1000", "1", "10000000"),
        integer_setting(raw, "poll_seconds", 45, 5, 300),
        integer_setting(raw, "max_signal_age_seconds", 300, 15, 300),
        decimal_setting(raw, "fixed_margin_fraction", "0.05", "0.0001", "1"), fixed, ceiling,
        decimal_setting(raw, "symbol_notional_fraction", "0.45", "0.0001", "20"),
        decimal_setting(raw, "gross_notional_fraction", "1.20", "0.0001", "20"),
        decimal_setting(raw, "margin_fraction", "0.40", "0.0001", "1"),
        decimal_setting(raw, "max_entry_deviation_fraction", "0.005", "0", "0.25"),
        decimal_setting(raw, "estimated_taker_fee_fraction", "0.0005", "0", "0.05"), slip,
        decimal_setting(raw, "minimum_notional_usdt", "10", "0.01", "100000"),
        decimal_setting(raw, "daily_loss_fraction", "0.05", "0.0001", "1"),
        decimal_setting(raw, "position_stop_fraction", "0.05", "0.0001", "0.99"), ai, telegram,
    )
