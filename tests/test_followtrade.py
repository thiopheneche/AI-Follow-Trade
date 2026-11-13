from contextlib import closing
from concurrent.futures import Future
from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from followtrade.__main__ import OrderTask, Runner
from followtrade.clients import BinanceFeed, Model, Quote, ServiceError, Signal
from followtrade.config import AIConfig, load_config
from followtrade.paper import PaperEngine
from followtrade.store import Store
from followtrade.telegram import Telegram
from probe import ProbeError


D = Decimal
NOW = 1_790_000_000_000
SYMBOL = "ZECUSDT"


def observed(identity, *, action="increase", leg="SHORT", at=NOW - 1000, opened=0, qty="20"):
    return Signal(identity, at, SYMBOL, leg, action, D(qty), D("100"), opened)


class PaperTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = Path(self.tmp.name) / "state.sqlite"
        example = Path(__file__).resolve().parents[1] / "config.example.json"
        self.cfg = replace(load_config(example), database=path)
        self.store = Store(path, self.cfg.portfolio, self.cfg.starting, ("fixed", "ai"))
        self.addCleanup(self.store.db.close)
        self.engine = PaperEngine(self.store, self.cfg)
        self.quotes = {SYMBOL: Quote(D("100"), D("100.01"), NOW)}

    def entry(self, signal):
        self.store.ingest([signal], ("fixed", "ai"), NOW)
        self.engine.process("fixed", signal, self.quotes, (D(".15"), 3, "fixed"), NOW)

    def test_bootstrap_trade_duplicate_and_restart_persistence(self):
        original = observed("bootstrap")
        self.store.ingest([original], ("fixed", "ai"), NOW, baseline=True, progress={"orders.last_end": NOW})
        self.assertEqual(self.store.pending("fixed"), [])
        new = observed("new", leg="LONG")
        self.entry(new)
        self.entry(new)
        self.assertEqual(len(self.store.positions("fixed")), 1)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 1)
        with closing(Store(self.cfg.database, self.cfg.portfolio, D(500), ("fixed",)).db) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 1)
            cash = D(db.execute("SELECT cash FROM accounts WHERE name='fixed'").fetchone()[0])
            self.assertTrue(D(999) < cash < D(1000))

    def test_partial_reduce_then_confirmed_close(self):
        self.entry(observed("open"))
        initial = D(self.store.positions("fixed")[0]["qty"])
        reduce = observed("reduce", action="decrease", at=NOW, qty="10")
        close = observed("close", action="close", at=NOW + 1, opened=NOW - 2000)
        self.store.ingest([reduce, close], ("fixed",), NOW)
        self.engine.process("fixed", reduce, {}, None, NOW)
        self.assertEqual(len(self.store.pending("fixed")), 2)
        self.engine.process("fixed", reduce, self.quotes, None, NOW)
        self.assertEqual(D(self.store.positions("fixed")[0]["qty"]), initial / 2)
        self.engine.process("fixed", close, self.quotes, None, NOW + 1)
        self.assertEqual(self.store.positions("fixed"), [])
        self.assertEqual(self.store.pending("fixed"), [])

    def test_ai_caps_leverage_and_notional(self):
        signal = observed("ai")
        self.store.ingest([signal], ("ai",), NOW)
        self.engine.process("ai", signal, self.quotes, (D(10), 100, "ai"), NOW)
        _, equity, gross, margin, positions = self.store.snapshot("ai")
        self.assertEqual(positions[0][0]["leverage"], self.cfg.max_leverage)
        self.assertLessEqual(gross, equity * self.cfg.symbol_limit)
        self.assertLessEqual(margin, equity * self.cfg.margin_limit)
        self.assertIn(SYMBOL, Telegram.reply(self.store, ("ai",), "/positions"))
        self.assertIn("10x", Telegram.reply(self.store, ("ai",), "/trades"))

    def test_stale_and_already_closed_signals_do_not_enter(self):
        self.entry(observed("stale", at=NOW - 301_000))
        self.store.ingest([observed("closed", action="close", at=NOW, opened=NOW - 3000)], ("fixed",), NOW)
        self.entry(observed("delayed"))
        self.assertEqual(self.store.positions("fixed"), [])
        results = [r[0] for r in self.store.db.execute("SELECT result FROM jobs")]
        self.assertIn("signal expired", results)
        self.assertIn("leader cycle already closed", results)

    def test_quote_based_stop_operates_without_model(self):
        self.entry(observed("open"))
        losing = {SYMBOL: Quote(D("110"), D("110.01"), NOW + 1000)}
        self.engine.protect(("fixed",), losing, NOW + 1000)
        self.assertEqual(self.store.positions("fixed"), [])
        self.assertLess(self.store.snapshot("fixed")[0], D(1000))

    def test_runner_runs_both_strategies_and_closes_without_ai(self):
        class ImmediatePool:
            def submit(self, function, *args):
                future = Future()
                future.set_result(function(*args))
                return future

        cfg = replace(self.cfg, ai=AIConfig("https://example.test/v1", "test", "fake", 5, "profile"))
        runner = Runner(cfg, self.store, ImmediatePool())
        runner.quote_data = self.quotes
        initial = observed("baseline", at=NOW - 2000)
        signal = observed("open")
        with patch.object(runner.model, "decide", return_value=(D(".20"), 5, "AI")) as model:
            for rows in ([initial], [initial, signal]):
                task = Future()
                task.set_result((rows, NOW, NOW - 5000, []))
                runner.order_task = OrderTask(task, NOW)
                runner._read_orders(0)
                runner._jobs(NOW)
                with patch("followtrade.__main__.now_ms", return_value=NOW):
                    runner._ready_ai()
            self.assertEqual(model.call_count, 1)
        self.assertEqual(self.store.positions("fixed")[0]["leverage"], 3)
        self.assertEqual(self.store.positions("ai")[0]["leverage"], 5)
        closing_signal = observed("closed", action="close", at=NOW + 1, opened=NOW - 2000)
        self.store.ingest([closing_signal], runner.accounts, NOW + 1)
        runner._jobs(NOW + 1)
        self.assertEqual(self.store.positions("fixed"), [])
        self.assertEqual(self.store.positions("ai"), [])
        self.assertIn("平仓", Telegram.reply(self.store, runner.accounts, "/trades"))

    def test_ai_deadline_marks_job_and_does_not_allow_late_fill(self):
        cfg = replace(self.cfg, ai=AIConfig("https://example.test/v1", "test", "fake", 5, "profile"))
        runner = Runner(cfg, self.store, None)
        signal = observed("open")
        self.store.ingest([signal], ("ai",), NOW)
        runner.quote_data = self.quotes
        task = Future()
        runner.ai_task = signal, task, NOW + 5000
        runner._expire_ai(NOW + 5000)
        task.set_result((D(".20"), 5, "late reply"))
        with patch("followtrade.__main__.now_ms", return_value=NOW + 5001):
            runner._ready_ai()
        self.assertEqual(self.store.positions("ai"), [])
        self.assertEqual(self.store.db.execute("SELECT result FROM jobs").fetchone()[0], "model deadline exceeded")


class ClientsTest(unittest.TestCase):
    @staticmethod
    def order(at):
        return {"symbol": SYMBOL, "positionSide": "SHORT", "side": "SELL", "type": "MARKET",
                "orderTime": at, "orderUpdateTime": at, "executedQty": "10", "avgPrice": "100"}

    def test_dense_window_bisects_and_keeps_all_records(self):
        feed = BinanceFeed("42")
        source = [self.order(i) for i in range(60)]

        def fetch(_route, body):
            rows = [r for r in source if body["startTime"] <= r["orderUpdateTime"] <= body["endTime"]]
            return {"list": rows[-50:], "total": len(rows)}

        with patch.object(feed, "_fetch", side_effect=fetch), patch("followtrade.clients.clock.sleep"):
            rows = feed.orders(0, 59)
        self.assertEqual({r.time for r in rows}, set(range(60)))

    def test_truncated_timestamp_fails_instead_of_advancing(self):
        feed = BinanceFeed("42")
        with patch.object(feed, "_fetch", return_value={"list": [self.order(9)], "total": 51}):
            with self.assertRaises(ProbeError):
                feed.orders(9, 9)

    def test_model_call_parses_structured_output_and_hides_key_from_prompt(self):
        model = Model(AIConfig("https://example.test/v1", "test", "placeholder-secret", 5, "先看流动性"))
        result = {"choices": [{"message": {"content": json.dumps(
            {"action": "follow", "target_notional_fraction": ".15", "leverage": 3, "reason": "限额内"})}}]}
        with patch("followtrade.clients.request_json", return_value=result) as api:
            self.assertEqual(model.decide({"direction": "SHORT"})[:2], (D(".15"), 3))
        self.assertNotIn("placeholder-secret", json.dumps(api.call_args.kwargs["body"]))
        with patch("followtrade.clients.request_json", return_value={"choices": []}):
            with self.assertRaises(ServiceError):
                model.decide({})


if __name__ == "__main__":
    unittest.main()
