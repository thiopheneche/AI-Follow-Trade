#!/usr/bin/env python3
"""Configure the deployed systemd paper service from an SSH terminal."""

import getpass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import urlsplit
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from followtrade.clients import Model, ServiceError
from followtrade.config import AIConfig
from private_config import with_environment, write_private


SERVICE = "ai-follow-trade.service"
SYSTEMCTL = ["systemctl", "--user"]
KEY = "FOLLOWTRADE_AI_KEY"


def environment_text(original, key):
    return with_environment(original, {KEY: key})


def validate_model(base_url, model, key, timeout):
    test = AIConfig(base_url, model, key, timeout, "Synthetic connectivity check. There is no eligible signal.")
    Model(test).decide({"signal": None, "account_equity_usdt": "1000", "max_leverage": 3})


def restart_and_check_ai(local_paper_enabled):
    accounts = "fixed, ai" if local_paper_enabled else "fixed"
    subprocess.run([*SYSTEMCTL, "restart", SERVICE], check=True)
    for _ in range(20):
        pid = subprocess.check_output([*SYSTEMCTL, "show", "-p", "MainPID", "--value", SERVICE], text=True).strip()
        if pid.isdigit() and pid != "0":
            log = subprocess.check_output(
                ["journalctl", "--user", "-u", SERVICE, f"_PID={pid}", "-n", "20", "--no-pager", "-o", "cat"],
                text=True,
            )
            if f"Paper accounts {accounts} for portfolio " in log:
                subprocess.run([*SYSTEMCTL, "is-active", "--quiet", SERVICE], check=True)
                return
        time.sleep(0.5)
    raise RuntimeError("AI account did not start")


def configure():
    if not sys.stdin.isatty():
        raise SystemExit("请使用带 -t 的 SSH 命令，在交互终端中运行。")
    root = Path.home() / ".config" / "ai-follow-trade"
    settings_path, env_path = root / "config.json", root / "secrets.env"
    original_settings = settings_path.read_text(encoding="utf-8-sig")
    settings = json.loads(original_settings)
    ai = settings["ai"]
    base_url = input("AI 服务的 HTTPS Base URL（保留 /v1 等路径）: ").strip().rstrip("/")
    try:
        url = urlsplit(base_url)
        if (url.scheme != "https" or not url.hostname or url.username or url.password
                or url.query or url.fragment or url.path.endswith("/chat/completions")
                or not re.fullmatch(r"[!-~]+", base_url)):
            raise ValueError
    except ValueError:
        raise SystemExit("请填写 HTTPS API 根地址，去掉末尾的 /chat/completions。") from None
    model = input("模型的 API ID: ").strip()
    if not model or not ai.get("trader_profile", "").strip():
        raise SystemExit("模型 ID 和交易员画像均不能为空。")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        key = getpass.getpass("粘贴 AI API Key，输入不会显示: ").strip()
    if not re.fullmatch(r"[!-~]+", key):
        raise SystemExit("API Key 为空或含无效字符。")
    print("正在从 VPS 验证模型响应……", flush=True)
    validate_model(base_url, model, key, ai.get("timeout_seconds", 60))

    original_env = env_path.read_text(encoding="utf-8") if env_path.exists() else None
    ai.update(enabled=True, base_url=base_url, model=model)
    try:
        write_private(env_path, environment_text(original_env, key))
        write_private(settings_path, json.dumps(settings, ensure_ascii=False, indent=2) + "\n")
        restart_and_check_ai(ai.get("local_paper_enabled", True))
    except (OSError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt):
        write_private(settings_path, original_settings)
        if original_env is None:
            env_path.unlink(missing_ok=True)
        else:
            write_private(env_path, original_env)
        restored = subprocess.run([*SYSTEMCTL, "restart", SERVICE], check=False)
        if restored.returncode:
            raise SystemExit("已恢复原配置，但服务重启失败，请检查服务状态。") from None
        raise SystemExit("AI 账户启动失败，已恢复原配置并重新启动服务。") from None

    print("模型响应验证通过，服务已启动。可在 Telegram 发送 /status 查看当前账户。")


if __name__ == "__main__":
    try:
        configure()
    except ServiceError as error:
        raise SystemExit(f"模型验证失败，配置未写入。{error}") from None
    except (getpass.GetPassWarning, EOFError, KeyboardInterrupt):
        raise SystemExit("未完成输入，请重新在 SSH 交互终端运行。") from None
