from concurrent.futures import Future
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from followtrade.__main__ import OrderTask, Runner
from followtrade.clients import Quote, Signal
from followtrade.config import AIConfig, TelegramConfig, load_config
from followtrade.store import Store
from followtrade.telegram import Telegram
from probe import ProbeError


TARGET = "5231135285416047873"
NOW = 1_790_000_000_000
SYMBOL = "ETHUSDT"
D = Decimal


def signal(identity, *, action="increase", at=NOW, opened=0):
    return Signal(identity, at, SYMBOL, "LONG", action, D(10), D(100), opened)


class SwitchTraderTest(TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        settings = Path(__file__).resolve().parents[1] / "config.example.json"
        self.config = replace(
            load_config(settings), database=Path(temporary.name) / "trades.sqlite",
            ai=AIConfig("https://example.test/v1", "test", "fake", 60, "熬鹰资本画像"),
            telegram=TelegramConfig("123:fake", 456, True),
        )
        self.store = Store(self.config.database, self.config.portfolio, D(1000), ("fixed", "ai"))
        self.addCleanup(self.store.db.close)
        self.runner = Runner(self.config, self.store, None)
        self.bot = Telegram(self.config.telegram)
        self.old = self.config.portfolio

    def message(self, text, *, sent_at=NOW // 1000):
        with patch("followtrade.telegram.now_ms", return_value=NOW):
            return self.bot.handle_message(self.store, self.runner.accounts, (), text, sent_at)

    def test_telegram_accepts_prompt_reply_and_rejects_invalid_target(self):
        self.assertIn(self.old, self.message("/trader@Bot"))
        self.assertIn("请输入", self.message("https://example.test/wrong"))
        with patch("followtrade.telegram.BinanceFeed") as factory:
            feed = factory.return_value
            feed.active_name.return_value = "新交易员"
            feed.orders.side_effect = ProbeError("public feed unavailable")
            self.assertIn("币安暂无法验证", self.message(TARGET))
            self.assertIsNone(self.store.get("trader.pending"))
            feed.orders.side_effect = None
            feed.orders.return_value = []
            self.assertIn("已提交切换", self.message(f"/trader https://www.binance.com/en/copy-trading/lead-details/{TARGET}"))
            factory.assert_called_with(TARGET)
            self.assertIn(f"新交易员（{TARGET}）", self.message("/status"))
            self.assertIn("已取消", self.message("/trader cancel"))
            self.assertIsNone(self.store.get("trader.pending"))
            feed.orders.assert_called_with(NOW - 299_999, NOW)
            feed.closures.assert_called_once()
        self.assertIn("已过期", self.message(f"/trader {TARGET}", sent_at=NOW // 1000 - 301))

    def test_waits_for_positions_and_inflight_poll_and_suppresses_old_entries(self):
        opening = signal("old-open", at=NOW - 2000)
        self.store.ingest([opening], self.runner.accounts, NOW, source=self.old)
        quotes = {SYMBOL: Quote(D("100"), D("100.01"), NOW)}
        self.runner.engine.process("fixed", opening, quotes, (D(".15"), 3, "fixed"), NOW)
        self.store.request_trader(TARGET, "新交易员")
        self.runner._switch_trader()
        self.assertEqual(self.runner.feed.portfolio, self.old)
        self.assertEqual(self.store.pending("ai"), [])
        self.store.ingest([signal("old-next")], self.runner.accounts, NOW, source=self.old)
        self.assertEqual(self.store.pending("fixed"), [])

        ending = signal("old-close", action="close", at=NOW, opened=NOW - 3000)
        self.store.ingest([ending], self.runner.accounts, NOW, source=self.old)
        self.runner.engine.process("fixed", ending, quotes, None, NOW)
        self.assertEqual(self.store.positions("fixed"), [])
        with self.store.transaction():
            self.store.db.execute(
                "INSERT INTO exchange_orders(client_id,mode,signal,side,quantity,leverage,time,action) "
                "VALUES ('pending','demo','old-open','BUY','1',3,?,'increase')", (NOW,),
            )
        self.runner._switch_trader()
        self.assertEqual(self.runner.feed.portfolio, self.old)
        with self.store.transaction():
            self.store.db.execute("UPDATE exchange_orders SET status='REJECTED' WHERE client_id='pending'")

        old_poll = Future()
        self.runner.order_task = OrderTask(old_poll, NOW)
        self.runner._switch_trader()
        self.assertEqual(self.runner.feed.portfolio, self.old)
        old_poll.set_result(([signal("queued-before-switch")], NOW, NOW - 300_000, []))
        self.runner._read_orders(0)
        with patch("followtrade.store.now_ms", return_value=NOW + 100):
            self.runner._switch_trader()
        self.assertEqual(self.runner.feed.portfolio, TARGET)
        self.assertIn(TARGET, self.store.get("trader.notification"))
        self.assertFalse(self.store.pending("fixed"))

    def test_switch_persists_and_scopes_history_fills_and_ai_profile(self):
        old_close = signal("shared-id", action="close", opened=NOW - 2000, at=NOW + 100)
        self.store.ingest([old_close], self.runner.accounts, NOW, source=self.old,
                          progress={"orders.gap": "old gap", "orders.last_end": NOW - 5000})
        self.store.request_trader(TARGET, "新交易员")
        with patch("followtrade.store.now_ms", return_value=NOW):
            self.runner._switch_trader()
        self.assertIsNone(self.store.get("orders.gap"))
        self.assertEqual(self.store.get("orders.last_end"), str(NOW))
        stale = signal("before-switch", at=NOW - 1)
        fresh = signal("shared-id", at=NOW + 1)
        self.store.ingest([fresh], ("ai",), NOW + 1, source=self.old)
        self.store.ingest([stale, fresh], ("ai",), NOW + 1, source=TARGET, notify_lead_fills=True)
        [(job, _)] = self.store.pending("ai")
        self.assertEqual((job.identity, job.portfolio), (f"{TARGET}:shared-id", TARGET))
        self.assertFalse(self.store.has_source_close(job))
        quotes = Quote(D(100), D("100.01"), NOW + 1)
        context = self.runner.engine.ai_context(job, quotes, NOW + 1)
        self.assertEqual(context["lead_portfolio_id"], TARGET)
        self.assertNotIn("熬鹰资本", context["trader_profile"])
        self.runner.engine.process("ai", job, {SYMBOL: quotes}, (D(".15"), 3, "AI"), NOW + 1)
        self.assertEqual(len(self.store.positions("ai")), 1)
        with patch.object(self.bot, "send") as send:
            self.bot.push_trader_notification(self.store)
            self.bot.push_trader_notification(self.store)
            send.assert_called_once()
        with closing(Store(self.config.database, self.old, D(1000), ()).db) as reopened:
            self.assertEqual(reopened.execute("SELECT value FROM settings WHERE key='trader.active'").fetchone()[0],
                             TARGET)

    def test_existing_database_assigns_its_signals_to_original_trader(self):
        path = self.config.database.with_name("legacy.sqlite")
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            db.execute("INSERT INTO settings VALUES ('portfolio', ?)", (self.old,))
            db.execute(
                "CREATE TABLE signals (identity TEXT PRIMARY KEY, time INTEGER, symbol TEXT, leg TEXT, "
                "action TEXT, quantity TEXT, price TEXT, opened INTEGER, first_seen INTEGER)"
            )
            db.execute("INSERT INTO signals VALUES ('old', ?, ?, 'LONG', 'close', '0', '0', ?, ?)",
                       (NOW, SYMBOL, NOW - 2000, NOW))
        with closing(Store(path, self.old, D(1000), ()).db) as db:
            self.assertEqual(db.execute("SELECT portfolio FROM signals WHERE identity='old'").fetchone()[0], self.old)
