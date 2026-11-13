from datetime import datetime, timezone
from decimal import Decimal
import logging
import sqlite3

from .clients import ServiceError, request_json
from .store import Store, now_ms


def format_fill(row):
    return (
        f"[模拟/{row['account']}] {row['symbol']} {row['leg']} {row['action']} "
        f"{Decimal(row['qty']):.8g} @ {Decimal(row['price']):.8g}\n"
        f"{row['leverage']}x · 手续费 {Decimal(row['fee']):.4f} USDT · "
        f"平仓盈亏 {Decimal(row['realized']):+.2f} USDT\n{row['reason']}"
    )


class Telegram:
    def __init__(self, config):
        self.config = config

    def call(self, method, body, timeout=8):
        result = request_json("Telegram", f"https://api.telegram.org/bot{self.config.token}/{method}",
                              body=body, timeout=timeout)
        if not isinstance(result, dict) or result.get("ok") is not True:
            raise ServiceError("Telegram: API rejected request")
        return result.get("result")

    def send(self, message):
        self.call("sendMessage", {"chat_id": self.config.chat_id, "text": message[:4000]})

    def serve(self, config, accounts, stop):
        store = Store(config.database, config.portfolio, config.starting, accounts)
        try:
            cursor = int(store.get("telegram.next_update", 0))
            last_fill = store.get("telegram.last_fill")
            if last_fill is None:
                with store.transaction():
                    last_fill = store.db.execute("SELECT COALESCE(MAX(id),0) FROM fills").fetchone()[0]
                    store.put("telegram.last_fill", last_fill)
            while not stop.is_set():
                try:
                    if self.config.push_fills:
                        pending = store.db.execute("SELECT * FROM fills WHERE id>? ORDER BY id LIMIT 12",
                                                   (int(last_fill),)).fetchall()
                        for row in pending:
                            if stop.is_set():
                                return
                            self.send(format_fill(row))
                            with store.transaction():
                                store.put("telegram.last_fill", row["id"])
                            last_fill = row["id"]
                    updates = self.call("getUpdates", {"offset": cursor, "timeout": 8, "limit": 20,
                                                       "allowed_updates": ["message"]}, timeout=12)
                    if not isinstance(updates, list):
                        raise ServiceError("Telegram: malformed updates")
                    for update in updates:
                        message = update.get("message", {})
                        chat = message.get("chat", {})
                        text = message.get("text")
                        if chat.get("type") == "private" and chat.get("id") == self.config.chat_id and isinstance(text, str):
                            command = text.split(maxsplit=1)[0].split("@")[0] if text.strip() else ""
                            self.send(self.reply(store, accounts, command))
                        cursor = update["update_id"] + 1
                        with store.transaction():
                            store.put("telegram.next_update", cursor)
                except (ServiceError, sqlite3.OperationalError):
                    logging.warning("Telegram temporarily unavailable")
                    stop.wait(12)
        finally:
            store.db.close()

    @staticmethod
    def reply(store, accounts, command):
        if command == "/status":
            source = store.get("feed_status", "等待轮询")
            quotes = store.get("quote_status", "等待行情")
            close = store.get("close_status", "等待轮询")
            lines = ["模拟跟单", f"信号源 {source}", f"全平 {close}", f"行情 {quotes}"]
            signal = store.db.execute("SELECT time, first_seen FROM signals WHERE action='increase' "
                                      "ORDER BY first_seen DESC, time DESC LIMIT 1").fetchone()
            if signal:
                age = (signal["first_seen"] - signal["time"]) // 1000
                lines.append(f"最近入场信号首次观测时距订单更新 {age} 秒（含冷启动）")
            with store.db:
                for account in accounts:
                    _, equity, gross, margin, positions = store.snapshot(account)
                    stale = any(now_ms() - p[4] > 30_000 for p in positions)
                    suffix = "（部分报价过期）" if stale else ""
                    lines.append(f"{account} 权益 {equity:.2f} | 敞口 {gross:.2f} | 保证金估计 {margin:.2f} USDT{suffix}")
            return "\n".join(lines)
        if command == "/positions":
            lines = ["模拟持仓"]
            for account in accounts:
                *_, positions = store.snapshot(account)
                if not positions:
                    lines.append(f"{account} 空仓")
                for position, quantity, entry, mark, observed, pnl in positions:
                    stamp = datetime.fromtimestamp(observed / 1000, timezone.utc).strftime("%m-%d %H:%M UTC") if observed else "暂无"
                    lines.append(f"{account} {position['symbol']} {position['leg']} {position['leverage']}x "
                                 f"数量 {quantity:.8g} 均价 {entry:.7g} 报价 {mark:.7g} "
                                 f"浮盈亏 {pnl:+.2f} USDT（{stamp}）")
            return "\n".join(lines)
        if command == "/trades":
            rows = store.db.execute("SELECT * FROM fills ORDER BY id DESC LIMIT 8").fetchall()
            return "\n\n".join(format_fill(r) for r in rows) or "暂无模拟成交"
        return "可用指令\n/status 运行状态和模拟权益\n/positions 模拟持仓\n/trades 最近模拟成交"
