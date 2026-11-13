#!/usr/bin/env python3
"""Validate and save credentials for one Binance Futures environment."""

import argparse
from decimal import Decimal, InvalidOperation
import getpass
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from followtrade.binance import BASE_URLS, Futures, credentials_prefix
from followtrade.clients import ServiceError
from private_config import with_environment, write_private


SERVICE = ["systemctl", "--user"]
UNIT = "ai-follow-trade.service"


def secret(prompt):
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        value = getpass.getpass(prompt).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise SystemExit("请输入币安生成的 HMAC 凭据。")
    return value


def restart():
    subprocess.run([*SERVICE, "restart", UNIT], check=True)
    time.sleep(2)
    subprocess.run([*SERVICE, "is-active", "--quiet", UNIT], check=True)


def configure(mode):
    if not sys.stdin.isatty():
        raise SystemExit("请通过 ssh -t 在交互终端中运行。")
    root = Path.home() / ".config" / "ai-follow-trade"
    settings_path, env_path = root / "config.json", root / "secrets.env"
    original_settings = settings_path.read_text(encoding="utf-8-sig")
    settings = json.loads(original_settings)
    database = Path(settings.get("database", "state/paper.sqlite3"))
    if not database.is_absolute():
        database = root / database
    if database.exists():
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as db:
            route = db.execute("SELECT value FROM settings WHERE key='execution.mode'").fetchone()
            if route and route[0] == mode:
                raise SystemExit("请先在 Telegram 发送 /paper，或切换至另一个账户，再更新此账户密钥。")
            for table, extra in (("exchange_positions", ""), ("exchange_orders", " AND status='CHECKING'")):
                if (db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone()
                        and db.execute(f"SELECT 1 FROM {table} WHERE mode=?" + extra, (mode,)).fetchone()):
                    raise SystemExit("此账户仍有仓位或待核对委托，请先处理完毕再更新密钥。")
    print(f"配置币安 {mode} 账户。")
    key, password = secret("Binance API Key: "), secret("Binance API Secret: ")
    client = Futures(mode, key, password)
    client.check_mode()
    _, positions = client.account()
    if any(Decimal(row["positionAmt"]) for row in positions):
        raise SystemExit("初次接入请使用空仓的专用合约账户。")
    if client.signed("/fapi/v1/openOrders") or client.signed("/fapi/v1/openAlgoOrders"):
        raise SystemExit("请先处理专用账户已有的普通和条件委托。")
    client.test_order()
    print("账户读取和币安测试下单接口均通过。", flush=True)
    default = "ai" if settings.get("ai", {}).get("enabled") else "fixed"
    strategy = input(f"执行策略（ai/fixed，默认 {default}）: ").strip().lower() or default
    if strategy not in ("ai", "fixed") or (strategy == "ai" and default != "ai"):
        raise SystemExit("请填写已启用的 ai 或 fixed 策略。")
    route_config = {"strategy": strategy}
    if mode == "demo":
        try:
            budget = Decimal(input("Demo 策略预算（USDT）: ").strip())
            if not budget.is_finite() or not 1 <= budget <= 10_000_000:
                raise InvalidOperation
        except InvalidOperation:
            raise SystemExit("预算须为 1 到 10,000,000 USDT。") from None
        route_config["budget_usdt"] = str(budget)
    original_env = env_path.read_text(encoding="utf-8") if env_path.exists() else None
    prefix = credentials_prefix(mode)
    values = {prefix + "KEY": key, prefix + "SECRET": password}
    settings.setdefault("execution", {})[mode] = route_config
    try:
        write_private(env_path, with_environment(original_env, values))
        write_private(settings_path, json.dumps(settings, ensure_ascii=False, indent=2) + "\n")
        restart()
    except (OSError, subprocess.SubprocessError, KeyboardInterrupt):
        write_private(settings_path, original_settings)
        if original_env is None:
            env_path.unlink(missing_ok=True)
        else:
            write_private(env_path, original_env)
        restored = subprocess.run([*SERVICE, "restart", UNIT], check=False)
        raise SystemExit("已恢复原配置。" if restored.returncode == 0 else "已恢复原配置，请检查服务状态。") from None
    print(f"{mode} 已绑定。去 Telegram 发送 /{'demo' if mode == 'demo' else 'live'} 选择此账户。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=BASE_URLS)
    try:
        configure(parser.parse_args().mode)
    except ServiceError as error:
        raise SystemExit(f"币安验证失败。{error}") from None
    except (getpass.GetPassWarning, EOFError, KeyboardInterrupt):
        raise SystemExit("录入已中止，请使用 SSH 交互终端重试。") from None
