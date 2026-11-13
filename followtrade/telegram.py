from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
import sqlite3
import time

from .clients import ServiceError, request_json
from .execution import choose_mode, exchange_positions, format_exchange_order, mode_status
from .store import Store, now_ms


UTC_PLUS_8 = timezone(timedelta(hours=8))
PUSH_BATCH_SIZE = 12
MODE_COMMANDS = {"/paper": "paper", "/demo": "demo", "/live": "mainnet"}
COMMAND_HELP = (
    "Bot 操作指令\n"
    "/func · /help · /start  查看指令列表\n"
    "/status  查看运行状态、行情和模拟账户权益\n"
    "/positions  查看模拟持仓和 Bot 已核对的币安持仓\n"
    "/trades  查看最近的本地模拟成交\n"
    "/orders  查看最近的币安委托和成交回报\n"
    "/mode  查看新开仓去向和已绑定的币安账户\n\n"
    "切换新开仓去向\n"
    "/paper  本地模拟\n"
    "/demo  币安 Demo（虚拟资金）\n"
    "/live  币安正式账户（真实资金）\n\n"
    "已提交委托和已有持仓继续在原账户处理。"
)


def format_fill(row):
    return (
        f"[模拟/{row['account']}] {row['symbol']} {row['leg']} {row['action']} "
        f"{Decimal(row['qty']):.8g} @ {Decimal(row['price']):.8g}\n"
        f"{row['leverage']}x · 手续费 {Decimal(row['fee']):.4f} USDT · "
        f"平仓盈亏 {Decimal(row['realized']):+.2f} USDT\n{row['reason']}"
    )


def format_lead_fill(row):
    direction = {"LONG": "多头", "SHORT": "空头"}[row["leg"]]
    action = {"increase": "开仓/加仓", "decrease": "减仓/平仓"}[row["action"]]
    updated, seen = (datetime.fromtimestamp(row[key] / 1000, UTC_PLUS_8).strftime("%m-%d %H:%M:%S")
                     for key in ("time", "first_seen"))
    return (
        f"[带单员成交] {row['symbol']} {direction}{action}\n"
        f"成交数量 {row['quantity']} · 均价 {row['price']}\n"
        f"订单更新 {updated} · 检测到 {seen}（UTC+8）"
    )


class Telegram:
    def __init__(self, config):
        self.config = config
        self.next_send = 0

    def call(self, method, body, timeout=8):
        result = request_json("Telegram", f"https://api.telegram.org/bot{self.config.token}/{method}",
                              body=body, timeout=timeout)
        if not isinstance(result, dict) or result.get("ok") is not True:
            raise ServiceError("Telegram: API rejected request")
        return result.get("result")

    def send(self, message):
        time.sleep(max(0, self.next_send - time.monotonic()))
        self.next_send = time.monotonic() + 1
        self.call("sendMessage", {"chat_id": self.config.chat_id, "text": message[:4000]})

    def push_lead_fills(self, store, stop):
        pending = store.db.execute(
            "SELECT s.* FROM telegram_lead_fills q JOIN signals s ON s.identity = q.signal "
            "ORDER BY q.rowid LIMIT ?", (PUSH_BATCH_SIZE,),
        ).fetchall()
        for row in pending:
            if stop.is_set():
                break
            self.send(format_lead_fill(row))
            with store.transaction():
                store.db.execute("DELETE FROM telegram_lead_fills WHERE signal = ?", (row["identity"],))
        return len(pending) == PUSH_BATCH_SIZE

    def push_exchange_orders(self, store, stop):
        pending = store.db.execute(
            "SELECT o.*, s.symbol FROM exchange_orders o JOIN signals s ON s.identity=o.signal "
            "WHERE o.notified=0 AND o.status<>'CHECKING' ORDER BY o.rowid LIMIT ?", (PUSH_BATCH_SIZE,),
        ).fetchall()
        for row in pending:
            if stop.is_set():
                return False
            self.send(format_exchange_order(row))
            with store.transaction():
                store.db.execute("UPDATE exchange_orders SET notified=1 WHERE client_id=?", (row["client_id"],))
        return len(pending) == PUSH_BATCH_SIZE

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
                    has_backlog = False
                    if self.config.push_fills:
                        has_backlog = self.push_lead_fills(store, stop)
                        has_backlog = self.push_exchange_orders(store, stop) or has_backlog
                        pending = store.db.execute("SELECT * FROM fills WHERE id>? ORDER BY id LIMIT ?",
                                                   (int(last_fill), PUSH_BATCH_SIZE)).fetchall()
                        for row in pending:
                            if stop.is_set():
                                return
                            self.send(format_fill(row))
                            with store.transaction():
                                store.put("telegram.last_fill", row["id"])
                            last_fill = row["id"]
                        has_backlog = has_backlog or len(pending) == PUSH_BATCH_SIZE
                    if stop.is_set():
                        break
                    updates = self.call("getUpdates", {"offset": cursor, "timeout": 0 if has_backlog else 8, "limit": 20,
                                                       "allowed_updates": ["message"]}, timeout=12)
                    if not isinstance(updates, list):
                        raise ServiceError("Telegram: malformed updates")
                    for update in updates:
                        message = update.get("message", {})
                        chat = message.get("chat", {})
                        text = message.get("text")
                        if chat.get("type") == "private" and chat.get("id") == self.config.chat_id and isinstance(text, str):
                            command = text.split(maxsplit=1)[0].split("@")[0] if text.strip() else ""
                            if command in MODE_COMMANDS and now_ms() - message.get("date", 0) * 1000 > 300_000:
                                self.send("切换指令已过期，请重新发送。")
                            else:
                                self.send(self.reply(store, accounts, command, config.execution))
                        cursor = update["update_id"] + 1
                        with store.transaction():
                            store.put("telegram.next_update", cursor)
                except (ServiceError, sqlite3.OperationalError):
                    logging.warning("Telegram temporarily unavailable")
                    stop.wait(12)
        finally:
            store.db.close()

    @staticmethod
    def reply(store, accounts, command, execution=()):
        if command in ("/func", "/help", "/start"):
            return COMMAND_HELP
        if command in MODE_COMMANDS:
            return choose_mode(store, execution, MODE_COMMANDS[command])
        if command == "/mode":
            return mode_status(store, execution) + "\n/paper 本地模拟 · /demo 币安 Demo · /live 币安正式账户"
        if command == "/orders":
            rows = store.db.execute("SELECT o.*,s.symbol FROM exchange_orders o JOIN signals s ON s.identity=o.signal "
                                    "ORDER BY o.rowid DESC LIMIT 8").fetchall()
            return "\n\n".join(format_exchange_order(row) for row in rows) or "暂无币安委托"
        if command == "/status":
            source = store.get("feed_status", "等待轮询")
            quotes = store.get("quote_status", "等待行情")
            close = store.get("close_status", "等待轮询")
            lines = [mode_status(store, execution), f"信号源 {source}", f"全平 {close}", f"行情 {quotes}"]
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
            tracked = exchange_positions(store)
            if tracked:
                lines.extend(["币安持仓（按已核对成交）", *tracked])
            return "\n".join(lines)
        if command == "/trades":
            rows = store.db.execute("SELECT * FROM fills ORDER BY id DESC LIMIT 8").fetchall()
            return "\n\n".join(format_fill(r) for r in rows) or "暂无模拟成交"
        return COMMAND_HELP
