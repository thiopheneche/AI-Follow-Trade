from concurrent.futures import Future
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from followtrade.binance import BASE_URLS, ExecutionAccount, Futures
from followtrade.clients import BinanceAPIError, PartialPosition, Quote, ServiceError, Signal
from followtrade.config import AIConfig, TelegramConfig, load_config
from followtrade.execution import ExchangeAccount, WAITING_FOR_MODEL, choose_mode
from followtrade.store import Store, now_ms, utc_day
from followtrade.telegram import Telegram


D = Decimal
SYMBOL = "ETHUSDT"
LOT = {"stepSize": "0.001", "minQty": "0.001", "maxQty": "100"}
RULES = {"PRICE_FILTER": {"tickSize": "0.01"}, "LOT_SIZE": LOT, "MARKET_LOT_SIZE": LOT,
         "MIN_NOTIONAL": {"notional": "5"}}


class ExecutionTest(TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        demo = ExecutionAccount("demo", "demo-key", "demo-secret", D(1000), "fixed")
        mainnet = ExecutionAccount("mainnet", "mainnet-key", "mainnet-secret", None, "fixed")
        cfg = load_config(Path(__file__).resolve().parents[1] / "config.example.json")
        self.cfg = replace(cfg, database=Path(tmp.name) / "test.sqlite", execution=(demo, mainnet), exit_policy="protected")
        self.store = Store(self.cfg.database, cfg.portfolio, cfg.starting, ("fixed",))
        self.addCleanup(self.store.db.close)
        self.modes = ("demo", "mainnet")
        self.trader = ExchangeAccount(self.cfg, demo)
        self.client = self.trader.client = Mock()
        self.client.quote.return_value = (D("100"), D("100.01"))
        self.client.filters.return_value = RULES
        self.asset = {"walletBalance": "1000", "availableBalance": "1000", "unrealizedProfit": "0"}
        self.client.account.return_value = self.asset, []
        self.orders = []
        self.client.signed.side_effect = self.exchange_request

    def exchange_request(self, route, method="GET", **params):
        if route == "/fapi/v1/leverage":
            return {"leverage": params["leverage"], "maxNotionalValue": "50000"}
        self.assertEqual((route, method), ("/fapi/v1/order", "POST"))
        self.orders.append(params)
        return self.result(params, "FILLED", params["quantity"])

    @staticmethod
    def result(params, status, executed):
        return {"status": status, "executedQty": executed, "avgPrice": "100",
                "clientOrderId": params["newClientOrderId"], "symbol": params["symbol"], "side": params["side"]}

    def observe(self, identity, action="increase", opened=0, symbol=SYMBOL, quantity=D(10)):
        stamp = now_ms()
        signal = Signal(identity, stamp, symbol, "LONG", action, quantity, D(100), opened)
        self.store.ingest([signal], ("fixed",), stamp, progress={"orders.last_end": stamp, "feed_status": "正常"},
                          execution_modes=self.modes)
        return signal

    def set_position(self, quantity):
        self.client.account.return_value = self.asset, [{
            "symbol": SYMBOL, "positionAmt": str(quantity), "positionSide": "BOTH", "entryPrice": "100",
            "markPrice": "100", "notional": str(D(quantity) * 100), "initialMargin": "50",
        }]

    def test_routing_keeps_existing_exits_on_original_account(self):
        self.observe("paper-only")
        self.assertFalse(self.store.pending("demo"))
        self.assertIn("币安 Demo", Telegram.reply(self.store, ("fixed",), "/demo", self.cfg.execution))
        signal = self.observe("demo-entry")
        self.trader.tick(self.store)
        self.assertFalse(self.store.pending("mainnet"))
        entry = self.orders[-1]
        self.assertEqual((entry["side"], entry["type"], entry["timeInForce"]), ("BUY", "LIMIT", "IOC"))
        self.assertEqual(entry["price"], "100.50")
        self.set_position(entry["quantity"])
        choose_mode(self.store, self.cfg.execution, "mainnet")
        self.observe("exit", action="close", opened=signal.time)
        self.trader.tick(self.store)
        close = self.orders[-1]
        self.assertEqual((close["side"], close["type"], close["reduceOnly"]), ("SELL", "MARKET", "true"))
        self.assertEqual(close["quantity"], entry["quantity"])
        self.assertFalse(self.trader.owned(self.store))
        self.assertEqual(self.store.get("execution.mode"), "mainnet")

    def test_demo_add_uses_incremental_ai_size_and_keeps_existing_leverage(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.observe("initial")
        self.trader.tick(self.store)
        initial = self.orders[-1]["quantity"]
        self.set_position(initial)
        cfg = replace(self.cfg, ai=AIConfig("https://example.test/v1", "test", "test", 60, "profile"))
        worker = ExchangeAccount(cfg, replace(cfg.execution[0], strategy="ai"), Mock())
        worker.client = self.client
        self.client.signed.side_effect = lambda route, method="GET", **params: (
            [{"symbol": SYMBOL, "leverage": 3}] if route.endswith("/symbolConfig") else
            self.exchange_request(route, method, **params)
        )
        signal = self.observe("add")
        worker.ai_result = signal.identity, (D(".10"), 8, "提高目标敞口"), None
        worker.tick(self.store)
        self.assertEqual(len(self.orders), 2)
        self.assertEqual(D(self.orders[-1]["quantity"]), D(".995"))
        self.assertEqual(D(worker.owned(self.store)[SYMBOL]["quantity"]), D(initial) + D(".995"))
        order = self.store.db.execute("SELECT leverage,status FROM exchange_orders WHERE signal=?", (signal.identity,)).fetchone()
        self.assertEqual((order["leverage"], order["status"]), (3, "FILLED"))

    def test_demo_price_guard_reports_skip_and_never_submits_order(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.client.quote.return_value = (D("101"), D("101.01"))
        self.observe("outside-price-limit")
        self.trader.tick(self.store)
        self.assertEqual(self.orders, [])
        reply = Telegram.reply(self.store, ("fixed",), "/orders", self.cfg.execution)
        self.assertIn("未下单", reply)
        self.assertIn("价格超出入场范围 · 带单成交价 100 · 可成交报价 101.01 · 限价 100.50", reply)

    def test_capped_orders_allow_repeated_additions_and_three_times_gross_exposure(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.trader.config = replace(self.cfg, fixed_leverage=10, deviation=D(0))
        self.client.quote.return_value = D(100), D(100)
        self.client.signed.side_effect = lambda route, method="GET", **params: (
            [{"symbol": params["symbol"], "leverage": 10}] if route.endswith("/symbolConfig") else
            self.exchange_request(route, method, **params)
        )

        def refresh_positions():
            self.client.account.return_value = self.asset, [
                {"symbol": row["symbol"], "positionAmt": row["quantity"], "positionSide": "BOTH",
                 "entryPrice": "100", "markPrice": "100", "notional": str(D(row["quantity"]) * 100),
                 "initialMargin": str(D(row["quantity"]) * 10)}
                for row in self.trader.owned(self.store).values()
            ]

        for symbol in (SYMBOL, "BTCUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"):
            for addition in range(5):
                with self.subTest(symbol=symbol, addition=addition):
                    refresh_positions()
                    self.observe(f"{symbol}-{addition}", symbol=symbol)
                    self.trader.tick(self.store)
            self.assertEqual(D(self.trader.owned(self.store)[symbol]["quantity"]) * 100, D(600))
        self.assertEqual(len(self.orders), 20)
        self.assertTrue(all(D(order["quantity"]) * 100 == D(150) for order in self.orders))
        self.assertEqual(sum((D(row["quantity"]) * 100 for row in self.trader.owned(self.store).values()), D(0)), D(3000))
        refresh_positions()
        self.observe("gross-limit", symbol="ADAUSDT")
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 20)

    def test_execution_caps_ai_order_even_if_model_exceeds_the_limit(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.trader.account = replace(self.trader.account, strategy="ai")
        signal = self.observe("oversized-ai")
        self.trader.ai_result = signal.identity, (D(".60"), 10, "跟随"), None
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 1)
        self.assertLessEqual(D(self.orders[0]["quantity"]) * D(self.orders[0]["price"]), D(150))
        self.set_position(self.orders[-1]["quantity"])
        self.client.signed.side_effect = lambda route, method="GET", **params: (
            [{"symbol": SYMBOL, "leverage": 10}] if route.endswith("/symbolConfig") else
            self.exchange_request(route, method, **params)
        )
        signal = self.observe("oversized-ai-add")
        self.trader.ai_result = signal.identity, (D(".60"), 3, "继续加仓"), None
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 2)
        self.assertLessEqual(D(self.orders[1]["quantity"]) * D(self.orders[1]["price"]), D(150))

    def managed_ai(self):
        self.trader.config = replace(self.cfg, ai=AIConfig("https://example.test/v1", "test", "key", 60, "profile", False))
        self.trader.account = replace(self.trader.account, strategy="ai", budget=None)
        self.client.leverage_brackets.return_value = {"symbol": SYMBOL, "notionalCoef": None,
            "brackets": [{"initialLeverage": 150, "notionalFloor": D(0), "notionalCap": D(50000)}]}
        choose_mode(self.store, self.cfg.execution, "demo")

    def test_ai_managed_mu_sizing_is_not_clipped_by_old_percentages(self):
        self.managed_ai()
        self.client.signed.side_effect = lambda route, method="GET", **params: (
            [{"symbol": SYMBOL, "leverage": 20, "maxNotionalValue": "50000"}] if route.endswith("/symbolConfig") else
            self.exchange_request(route, method, **params))
        for i, fraction in enumerate((".4", ".1", ".5", ".5", ".038")):
            if self.orders:
                owned = D(self.trader.owned(self.store)[SYMBOL]["quantity"])
                self.set_position(owned)
                self.asset["availableBalance"] = str(D(1000) - owned * 100 / 20)
            signal = self.observe(f"managed-{i}")
            self.trader.ai_result = signal.identity, (D(fraction), 20, "AI"), None
            self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 5)
        self.assertGreater(D(self.trader.owned(self.store)[SYMBOL]["quantity"]) * 100, D(1500))
        self.assertGreater(D(self.orders[0]["quantity"]) * 100, D(390))
        self.assertGreater(D(self.orders[2]["quantity"]) * 100, D(490))

    def test_ai_managed_can_exceed_gross_margin_and_leverage_strategy_caps(self):
        self.managed_ai()
        signal = self.observe("large-managed-order")
        self.trader.ai_result = signal.identity, (D(9), 20, "AI"), None
        self.trader.tick(self.store)
        notional = D(self.orders[0]["quantity"]) * D(self.orders[0]["price"])
        self.assertGreater(notional, D(8900))
        self.assertGreater(notional / 20, D(400))
        self.assertEqual(self.store.db.execute("SELECT leverage FROM exchange_orders").fetchone()[0], 20)

    def test_ai_managed_available_funds_and_exchange_notional_still_bind(self):
        self.managed_ai()
        self.asset["availableBalance"] = "100"
        signal = self.observe("funds-bound")
        self.trader.ai_result = signal.identity, (D(1000), 50, "AI"), None
        self.trader.tick(self.store)
        notional = D(self.orders[-1]["quantity"]) * D(self.orders[-1]["price"])
        self.assertLessEqual(notional / 50 + notional * self.cfg.fee, D(100))
        self.assertGreater(notional, D(4000))

    def test_ai_managed_demo_uses_full_equity_without_old_budget_offset(self):
        self.managed_ai()
        self.asset.update(walletBalance="2000", availableBalance="2000")
        with self.store.transaction():
            self.store.put("execution.wallet_basis.demo", "500")
        signal = self.observe("full-demo-wallet")
        self.trader.ai_result = signal.identity, (D(".5"), 20, "AI"), None
        self.trader.tick(self.store)
        notional = D(self.orders[-1]["quantity"]) * D(self.orders[-1]["price"])
        self.assertGreater(notional, D(990))
        self.assertLessEqual(notional, D(1000))

    def test_ai_managed_exchange_cap_reduces_order_and_preserves_price_guard(self):
        self.managed_ai()
        def signed(route, method="GET", **params):
            if route.endswith("/leverage"):
                return {"leverage": params["leverage"], "maxNotionalValue": "2000"}
            return self.exchange_request(route, method, **params)
        self.client.signed.side_effect = signed
        signal = self.observe("exchange-cap")
        self.trader.ai_result = signal.identity, (D(9), 20, "AI"), None
        self.trader.tick(self.store)
        self.assertLessEqual(D(self.orders[-1]["quantity"]) * D(self.orders[-1]["price"]), D(2000))
        self.assertGreater(D(self.orders[-1]["quantity"]) * D(self.orders[-1]["price"]), D(1900))
        self.set_position(self.orders[-1]["quantity"])
        self.client.quote.return_value = D(101), D("101.01")
        signal = self.observe("managed-outside-price")
        self.trader.ai_result = signal.identity, (D(1), 20, "AI"), None
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 1)

    def test_ai_managed_context_uses_exchange_limits_and_available_funds(self):
        self.managed_ai()
        signal = self.observe("managed-context")
        self.trader._model_decision = Mock(return_value=WAITING_FOR_MODEL)
        self.trader.tick(self.store)
        context = self.trader._model_decision.call_args.args[1]
        self.assertFalse(context["position_limits_enabled"])
        for key in ("order_notional_fraction_limit", "symbol_notional_fraction_limit", "gross_notional_fraction_limit", "margin_fraction_limit"):
            self.assertIsNone(context[key])
        self.assertEqual(context["max_leverage"], 150)
        self.assertEqual(context["available_balance_usdt"], D(1000))
        self.assertEqual(self.orders, [])

    def test_ai_managed_existing_exchange_headroom_and_daily_stop_still_bind(self):
        self.managed_ai()
        initial = self.observe("owned-managed")
        self.trader.ai_result = initial.identity, (D(1), 20, "AI"), None
        self.trader.tick(self.store)
        qty = self.orders[-1]["quantity"]
        self.set_position(qty)
        actual_notional = D(qty) * 100
        self.client.signed.side_effect = lambda route, method="GET", **params: (
            [{"symbol": SYMBOL, "leverage": 20, "maxNotionalValue": str(actual_notional)}]
            if route.endswith("/symbolConfig") else self.exchange_request(route, method, **params))
        addition = self.observe("no-exchange-headroom")
        self.trader.ai_result = addition.identity, (D(1), 20, "AI"), None
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 1)
        signal = self.observe("daily-guard")
        result = self.trader._entry(self.store, signal, self.asset, {}, {}, D(1000), D(1000), True)
        self.assertEqual(result, "daily loss guard")
        self.assertEqual(len(self.orders), 1)

    def test_leverage_brackets_are_validated_and_cached_per_account(self):
        client = Futures("demo", "key", "secret")
        payload = {"symbol": SYMBOL, "notionalCoef": "1.5", "brackets": [
            {"initialLeverage": 150, "notionalFloor": "0", "notionalCap": "50000"}]}
        with patch.object(client, "signed", return_value=[payload]) as api:
            first = client.leverage_brackets(SYMBOL)
            self.assertEqual(first, client.leverage_brackets(SYMBOL))
            api.assert_called_once_with("/fapi/v1/leverageBracket", symbol=SYMBOL)
        self.assertEqual(first["brackets"][0]["initialLeverage"], 150)
        self.assertEqual(first["notionalCoef"], "1.5")
        for bad in ({**payload, "symbol": "OTHERUSDT"}, {**payload, "brackets": []},
                    {**payload, "brackets": [{"initialLeverage": 0, "notionalFloor": 0, "notionalCap": 10}]}):
            with self.subTest(payload=bad), patch.object(client, "signed", return_value=bad):
                client.brackets.clear()
                with self.assertRaises(ServiceError):
                    client.leverage_brackets(SYMBOL)

    def test_demo_deviation_override_accepts_prices_within_its_own_limit(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.trader.account = replace(self.trader.account, entry_deviation=D(".0075"))
        self.client.quote.return_value = (D("100.59"), D("100.60"))
        self.observe("inside-demo-limit")
        self.trader.tick(self.store)
        self.assertEqual(self.orders[-1]["price"], "100.75")
        self.set_position(self.orders[-1]["quantity"])
        self.client.quote.return_value = (D("100.75"), D("100.76"))
        self.observe("outside-demo-limit")
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 1)

    def test_status_reads_demo_and_mainnet_and_shows_position_matches(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.observe("demo-entry")
        self.trader.tick(self.store)
        qty = self.orders[-1]["quantity"]
        rows = {
            "demo": [{"symbol": SYMBOL, "positionAmt": qty, "positionSide": "BOTH",
                      "entryPrice": "100", "markPrice": "101", "unRealizedProfit": "4.25", "marginAsset": "USDT"}],
            "mainnet": [{"symbol": "BTCUSDT", "positionAmt": "-0.01", "positionSide": "BOTH"},
                        {"symbol": "SOLUSDT", "positionAmt": "0", "positionSide": "BOTH"}],
        }

        def signed(client, route, method="GET"):
            self.assertEqual((route, method), ("/fapi/v3/positionRisk", "GET"))
            self.assertEqual(client.base_url, BASE_URLS[client.key.split("-")[0]])
            return rows[client.key.split("-")[0]]

        with patch.object(Futures, "signed", autospec=True, side_effect=signed) as query, \
                patch("followtrade.telegram.BinanceFeed.trader_name", return_value="熬鹰资本"):
            reply = Telegram.reply(self.store, ("fixed",), "/status", self.cfg.execution)
            self.assertEqual(query.call_count, 2)
        self.assertIn("【币安 Demo仓位】 币安返回 1 个持仓，Bot 记录 1 个", reply)
        self.assertIn("标记价 101 · 浮盈亏 +4.2500 USDT · 与 Bot 记录一致", reply)
        self.assertIn("【币安正式账户仓位】 币安返回 1 个持仓，Bot 记录 0 个", reply)
        self.assertIn("BTCUSDT 空 数量 0.01 · 无 Bot 跟单记录", reply)
        self.assertNotIn("SOLUSDT", reply)

    def test_status_reports_read_error_empty_account_and_unbound_account(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.observe("demo-entry")
        self.trader.tick(self.store)
        with patch("followtrade.telegram.BinanceFeed.trader_name", return_value="熬鹰资本"), \
                patch.object(Futures, "position_risk", side_effect=[BinanceAPIError(401, -2015), [], []]):
            both = Telegram.reply(self.store, ("fixed",), "/status", self.cfg.execution)
            demo_only = Telegram.reply(self.store, ("fixed",), "/status", self.cfg.execution[:1])
        self.assertIn("【币安 Demo仓位】 查询失败", both)
        self.assertIn("Bot 记录 1 个仓位", both)
        self.assertIn("【币安正式账户仓位】 币安返回 0 个持仓，Bot 记录 0 个\n空仓", both)
        self.assertIn(f"{SYMBOL} · Bot 有跟单记录，币安未返回持仓", demo_only)
        self.assertIn("【币安正式账户仓位】 未绑定", demo_only)

    def test_pnl_reads_both_accounts_and_excludes_transfers(self):
        zone = timezone(timedelta(hours=8))
        start = int(datetime(2026, 11, 18, tzinfo=zone).timestamp() * 1000)
        now = start + 60_000
        with self.store.transaction():
            self.store.db.execute("INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                  ("fixed", SYMBOL, "SHORT", "2", "100", 3, start, start, "2"))
            self.store.save_marks({SYMBOL: Quote(D("98"), D("98"), now - 1000)})
            self.store.db.executemany(
                "INSERT INTO fills(account,time,symbol,leg,action,qty,price,leverage,fee,realized,reason) "
                "VALUES ('fixed', ?, ?, 'SHORT', 'exit', '1', '100', 3, ?, ?, 'test')",
                [(start - 1, SYMBOL, ".5", "99"), (start, SYMBOL, ".25", "10")],
            )

        def signed(client, route, method="GET", **params):
            self.assertEqual(method, "GET")
            demo = client.base_url == BASE_URLS["demo"]
            if route == "/fapi/v3/account":
                assets = [{"asset": "USDT", "marginBalance": "1004", "unrealizedProfit": "4"}]
                if demo:
                    assets.append({"asset": "BTC", "marginBalance": ".002", "unrealizedProfit": "0"})
                return {"assets": assets}
            self.assertEqual(route, "/fapi/v1/income")
            self.assertEqual(params, {"startTime": start, "endTime": now, "limit": 1000})
            if not demo:
                raise BinanceAPIError(401, -2015)
            return [
                {"time": start, "asset": asset, "incomeType": kind, "income": value}
                for asset, kind, value in (
                    ("USDT", "TRANSFER", "1000"), ("USDT", "REALIZED_PNL", "12"),
                    ("USDT", "COMMISSION", "-2"), ("USDT", "FUNDING_FEE", "-3"),
                    ("USDC", "FUNDING_FEE", ".125"),
                )
            ]

        with patch.object(Futures, "signed", autospec=True, side_effect=signed), \
                patch("followtrade.telegram.now_ms", return_value=now):
            reply = Telegram.reply(self.store, ("fixed",), "/pnl", self.cfg.execution)
        self.assertIn("11-18 00:01:00（北京时间）", reply)
        self.assertIn("【本地模拟 · 固定策略】\nUSDT ｜ 权益 1004.0000 · 浮动盈亏 +4.0000", reply)
        self.assertIn("已实现 +10.0000 · 手续费 -0.2500 · 今日净额 +9.7500", reply)
        self.assertIn("BTC ｜ 权益 0.00200000", reply)
        self.assertIn("已实现 +12.0000 · 手续费 -2.0000 · 资金费 -3.0000 · 今日净额 +7.0000", reply)
        self.assertIn("USDC ｜ 已实现 +0.0000 · 手续费 +0.0000 · 资金费 +0.1250 · 今日净额 +0.1250", reply)
        self.assertIn("【币安正式账户】\nUSDT ｜ 权益 1004.0000 · 浮动盈亏 +4.0000\n今日盈亏查询失败", reply)
        self.assertIn("/pnl", Telegram.reply(self.store, ("fixed",), "/help"))

    def test_income_history_reads_complete_disjoint_windows_when_full(self):
        client = Futures("demo", "test-key", "test-secret")
        with patch.object(client, "signed", side_effect=lambda route, **params: [
            {"time": ms} for ms in range(params["startTime"], params["endTime"] + 1)
        ][:params["limit"]]) as signed:
            self.assertEqual([row["time"] for row in client.income_history(1, 1002)], list(range(1, 1003)))
            self.assertEqual(signed.call_count, 3)

    def test_history_groups_demo_fills_and_caches_only_complete_orders(self):
        signal = self.observe("historical-close", action="close")
        with self.store.transaction():
            self.store.db.execute(
                "INSERT INTO exchange_orders(client_id,mode,signal,side,quantity,leverage,time,action,status,executed,price) "
                "VALUES ('ft-historical','demo',?,'SELL','3',0,?,'close','EXPIRED','2','100')",
                (signal.identity, signal.time),
            )
        order = {"clientOrderId": "ft-historical", "symbol": SYMBOL, "side": "SELL", "orderId": 71, "executedQty": "2"}
        fills = [{"orderId": 71, "symbol": SYMBOL, "side": "SELL", "id": n, "time": signal.time + n,
                  "qty": "1", "marginAsset": "USDT", "realizedPnl": pnl, "commissionAsset": asset,
                  "commission": fee}
                 for n, pnl, asset, fee in [(1, "7", "USDT", ".1"), (2, "-2", "BNB", ".005")]]

        def signed(client, route, method="GET", **params):
            self.assertEqual(client.base_url, BASE_URLS["demo"])
            self.assertEqual(method, "GET")
            if route == "/fapi/v1/order":
                self.assertEqual(params, {"symbol": SYMBOL, "origClientOrderId": "ft-historical"})
                return order
            self.assertEqual(route, "/fapi/v1/userTrades")
            self.assertEqual(params["orderId"], 71)
            return [*fills, {"orderId": 72, "realizedPnl": "900"}]

        with patch.object(Futures, "signed", side_effect=[order, fills[:1]]):
            partial = Telegram.reply(self.store, ("fixed",), "/history", self.cfg.execution, argument="demo")
        self.assertIn("盈亏暂不可用 · 币安成交明细暂不完整", partial)
        self.assertIsNone(self.store.db.execute("SELECT amounts FROM exchange_order_pnl").fetchone())

        with patch.object(Futures, "signed", autospec=True, side_effect=signed):
            bot = Telegram(TelegramConfig("123:fake", 1, True))
            report = bot.handle_message(self.store, ("fixed",), self.cfg.execution, "/history@Bot demo", 0)
        self.assertIn("平仓 · 部分成交（已过期）", report)
        self.assertIn("USDT ｜ 已实现 +5.0000 · 手续费 0.1000 · 净额 +4.9000", report)
        self.assertIn("BNB ｜ 已实现 +0.00000000 · 手续费 0.00500000 · 净额 -0.00500000", report)
        with patch.object(Futures, "signed") as api:
            cached = Telegram.reply(self.store, ("fixed",), "/history", self.cfg.execution, argument="demo")
            api.assert_not_called()
        self.assertEqual(cached, report)

    def test_history_paginates_paper_and_includes_entry_fees(self):
        with self.store.transaction():
            self.store.db.executemany(
                "INSERT INTO fills(account,time,symbol,leg,action,qty,price,leverage,fee,realized,reason) "
                "VALUES ('fixed', ?, ?, 'LONG', ?, '1', '100', 3, '0.25', ?, 'test')",
                [(1000 + i, f"TEST{i}USDT", "开仓" if i == 0 else "平仓", "0" if i == 0 else "2")
                 for i in range(9)],
            )
        first = Telegram.reply(self.store, ("fixed",), "/history", argument="paper")
        second = Telegram.reply(self.store, ("fixed",), "/history", argument="paper 2")
        self.assertIn("下一页 /history paper 2", first)
        self.assertNotIn("TEST0USDT", first)
        self.assertIn("TEST0USDT 多 · 开仓", second)
        self.assertIn("已实现 +0.0000 · 手续费 0.2500 · 净额 -0.2500", second)
        self.assertIn("上一页 /history paper 1", second)
        self.assertIn("暂无跟单记录", Telegram.reply(self.store, ("fixed",), "/history", argument="live"))

    def test_trader_switch_waits_for_exchange_position_without_opening_again(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        entry = self.observe("demo-entry")
        self.trader.tick(self.store)
        self.set_position(self.orders[-1]["quantity"])

        self.observe("pending-add")
        self.store.request_trader("5231135285416047873", "目标交易员")
        self.observe("later-add")
        self.assertIsNone(self.store.activate_requested_trader())
        self.observe("leader-close", action="close", opened=entry.time)
        self.trader.tick(self.store)

        self.assertEqual(len(self.orders), 2)
        self.assertEqual(self.orders[-1]["reduceOnly"], "true")
        self.assertEqual(self.store.activate_requested_trader(), "5231135285416047873")
        self.assertEqual(self.store.get("execution.mode"), "demo")

    def test_timeout_queries_same_id_and_uses_actual_partial_fill(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.observe("partial-entry")

        def submit_timeout(route, method="GET", **params):
            if route.endswith("/leverage"):
                return self.exchange_request(route, method, **params)
            self.orders.append(params)
            raise ServiceError("connection failed")

        self.client.signed.side_effect = submit_timeout
        with self.assertRaises(ServiceError):
            self.trader.tick(self.store)
        self.assertFalse(self.trader.owned(self.store))
        self.assertEqual(len(self.orders), 1)
        self.set_position("0.125")
        self.client.signed.side_effect = None
        self.client.signed.reset_mock()
        self.client.signed.return_value = self.result(self.orders[0], "EXPIRED", "0.125")
        self.trader.tick(self.store)
        self.client.signed.assert_called_once_with("/fapi/v1/order", symbol=SYMBOL,
                                                   origClientOrderId=self.orders[0]["newClientOrderId"])
        self.assertEqual(self.trader.owned(self.store)[SYMBOL]["quantity"], "0.125")
        self.assertFalse(self.store.pending("demo"))

    def test_unsigned_tradfi_agreement_rejects_order_without_blocking_next_entry(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.observe("agreement-required")
        self.observe("next-entry")

        def reject_first_order(route, method="GET", **params):
            if route == "/fapi/v1/order":
                self.assertEqual(method, "POST")
                raise BinanceAPIError(400, -4411)
            return self.exchange_request(route, method, **params)

        self.client.signed.side_effect = reject_first_order
        self.trader.tick(self.store)
        self.assertFalse(self.trader.owned(self.store))
        self.assertEqual([signal.identity for signal, _ in self.store.pending("demo")], ["next-entry"])
        self.assertIn("TradFi", Telegram.reply(self.store, ("fixed",), "/orders", self.cfg.execution))

        self.client.signed.side_effect = self.exchange_request
        self.trader.tick(self.store)
        statuses = [row[0] for row in self.store.db.execute("SELECT status FROM exchange_orders ORDER BY rowid")]
        self.assertEqual(statuses, ["REJECTED", "FILLED"])
        self.assertFalse(self.store.pending("demo"))

    def test_waiting_for_model_does_not_block_exchange_close(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        entry_signal = self.observe("entry")
        self.trader.tick(self.store)
        self.set_position(self.orders[0]["quantity"])
        ai = AIConfig("https://example.test/v1", "test", "secret", 60, "profile")
        cfg = replace(self.cfg, ai=ai, telegram=TelegramConfig("test", 1, True))
        account = replace(self.cfg.execution[0], strategy="ai")
        pending_decision = Future()
        pool = Mock()
        pool.submit.return_value = pending_decision
        worker = ExchangeAccount(cfg, account, pool)
        worker.client = self.client
        self.client.signed.side_effect = lambda route, method="GET", **params: (
            [{"symbol": SYMBOL, "leverage": 3}] if route.endswith("/symbolConfig")
            else self.exchange_request(route, method, **params)
        )
        self.observe("wait-for-ai")
        worker.tick(self.store)
        self.assertEqual(len(self.orders), 1)
        self.observe("leader-close", action="close", opened=entry_signal.time)
        worker.tick(self.store)
        self.assertFalse(pending_decision.done())
        self.assertEqual(self.orders[-1]["reduceOnly"], "true")
        self.client.account.return_value = self.asset, []

        pending_decision.set_result((D("0.25"), 3, "historical decision"))
        self.assertTrue(worker.wake.is_set())
        worker.tick(self.store)
        self.assertEqual(len(self.orders), 2)
        decision = self.store.db.execute("SELECT account,message FROM telegram_ai_decisions").fetchone()
        self.assertEqual(decision["account"], "demo")
        self.assertIn("25.00%", decision["message"])
        self.assertFalse(self.store.pending("demo"))

    def test_confirmed_full_exit_retries_only_remaining_quantity(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        signal = self.observe("entry")
        self.trader.tick(self.store)
        self.set_position(self.orders[0]["quantity"])
        self.observe("exit", action="close", opened=signal.time)

        def partial_exit(route, method="GET", **params):
            self.assertEqual((route, method, params["reduceOnly"]), ("/fapi/v1/order", "POST", "true"))
            self.orders.append(params)
            return self.result(params, "EXPIRED", "0.5")

        self.client.signed.side_effect = partial_exit
        self.trader.tick(self.store)
        self.assertEqual(len(self.store.pending("demo")), 1)
        remainder = D(self.orders[0]["quantity"]) - D("0.5")
        self.set_position(remainder)
        self.client.signed.side_effect = self.exchange_request
        self.trader.tick(self.store)
        self.assertEqual(D(self.orders[-1]["quantity"]), remainder)
        self.assertNotEqual(self.orders[-1]["newClientOrderId"], self.orders[-2]["newClientOrderId"])
        self.assertFalse(self.trader.owned(self.store))
        self.assertFalse(self.store.pending("demo"))

    def test_leader_only_exchange_ignores_local_stops_and_follows_reduce_then_close(self):
        self.trader.config = replace(self.cfg, exit_policy="leader_only")
        choose_mode(self.store, self.cfg.execution, "demo")
        opening = self.observe("leader-only-open")
        self.trader.tick(self.store)
        initial = D(self.orders[0]["quantity"])
        self.set_position(initial)
        self.client.account.return_value[1][0]["markPrice"] = "90"
        self.asset["unrealizedProfit"] = "-100"
        with self.store.transaction():
            self.store.put(f"execution.daily.demo.{utc_day(now_ms())}.tripped", "1")
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 1)
        self.observe("old-auto-stop", action="daily_stop")
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 1)
        reduction = self.observe("source-reduce", action="decrease", quantity=D(5))
        self.store.ingest([], (), reduction.time, partial_positions=(
            PartialPosition("leader-cycle", SYMBOL, reduction.leg, opening.time, reduction.time, D(10), D(5)),
        ))
        self.trader.tick(self.store)
        self.assertEqual(D(self.orders[-1]["quantity"]), initial / 2)
        self.set_position(D(self.trader.owned(self.store)[SYMBOL]["quantity"]))
        self.observe("source-close", action="close", opened=opening.time)
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 3)
        self.assertEqual(self.orders[-1]["reduceOnly"], "true")
        self.assertFalse(self.trader.owned(self.store))
        self.assertEqual([row[0] for row in self.store.db.execute("SELECT action FROM exchange_orders ORDER BY rowid")],
                         ["increase", "decrease", "close"])

    def test_reduction_counts_unfollowed_zec_entries_and_waits_for_reconciled_history(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        stamp = now_ms() - 5000
        opening = Signal("zec-early-unfollowed", stamp, SYMBOL, "LONG", "increase", D("1133.266"), D(100))
        copied = Signal("zec-demo-copied", stamp + 1, SYMBOL, "LONG", "increase", D("322.483"), D(100))
        reduce = Signal("zec-partial", stamp + 2, SYMBOL, "LONG", "decrease", D("727.874"), D(100))
        pending = Signal("zec-another-add", stamp + 3, SYMBOL, "LONG", "increase", D(50), D(100))
        history = PartialPosition("zec-cycle", SYMBOL, "LONG", opening.time, reduce.time, D("1455.749"), D("727.874"))
        self.store.ingest([copied], (), stamp + 1, baseline=True)
        with self.store.transaction():
            self.store.db.execute("INSERT INTO exchange_positions VALUES (?,?,?,?,?,?,?,?)",
                                  ("demo", SYMBOL, "LONG", "4.952", copied.time, copied.time, copied.identity, "322.483"))
        self.set_position(D("4.952"))
        self.store.ingest([reduce, pending], (), now_ms(), execution_modes=self.modes, partial_positions=(history,),
                          progress={"orders.last_end": now_ms(), "feed_status": "正常"})
        self.trader.tick(self.store)
        self.assertEqual(self.orders, [])  # Crucially, don't guess using only the copied 322.483.
        self.store.ingest([opening], (), now_ms(), baseline=True)
        self.trader.tick(self.store)
        self.assertEqual(D(self.orders[0]["quantity"]), D("2.475"))  # 2.475998... rounded down to the lot step.
        self.assertEqual(self.orders[0]["reduceOnly"], "true")
        self.assertEqual(D(self.trader.owned(self.store)[SYMBOL]["quantity"]), D("2.477"))

    def test_confirmed_full_close_proceeds_when_partial_reduction_is_unverified(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        opening = self.observe("before-full-close")
        self.trader.tick(self.store)
        full_qty = self.orders[0]["quantity"]
        self.set_position(full_qty)
        reduce = Signal("unverified-reduction", opening.time + 1, SYMBOL, "LONG", "decrease", D(8), D(100))
        full = Signal("confirmed-full-close", opening.time + 2, SYMBOL, "LONG", "close", D(0), D(0), opening.time)
        self.store.ingest([reduce, full], (), now_ms(), execution_modes=self.modes)
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 2)
        self.assertEqual(self.orders[-1]["quantity"], full_qty)
        self.assertFalse(self.trader.owned(self.store))
        self.assertFalse(self.store.pending("demo"))

    def test_leader_only_fresh_source_entry_is_not_blocked_by_old_auto_stop(self):
        choose_mode(self.store, self.cfg.execution, "demo")
        self.observe("protected-open")
        self.trader.tick(self.store)
        self.set_position(self.orders[-1]["quantity"])
        self.client.account.return_value[1][0]["markPrice"] = "90"
        self.trader.tick(self.store)
        self.assertEqual(self.store.db.execute("SELECT action FROM exchange_orders ORDER BY rowid DESC LIMIT 1").fetchone()[0], "position_stop")
        stop_time = self.store.db.execute("SELECT MAX(time) FROM signals WHERE action='position_stop'").fetchone()[0]
        self.assertFalse(self.trader.owned(self.store))
        self.trader.config = replace(self.cfg, exit_policy="leader_only")
        self.client.account.return_value = self.asset, []
        with self.store.transaction():
            self.store.put(f"execution.daily.demo.{utc_day(now_ms())}.tripped", "1")
        signal = Signal("late-visible-add", stop_time, SYMBOL, "LONG", "increase", D(5), D(100))
        self.store.ingest([signal], ("fixed",), now_ms(), execution_modes=self.modes)
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 3)
        self.assertIn(SYMBOL, self.trader.owned(self.store))

    def test_leader_only_close_does_not_wait_for_ai_or_trigger_own_stop(self):
        self.managed_ai()
        self.trader.config = replace(self.trader.config, exit_policy="leader_only")
        opening = self.observe("managed-leader-only-open")
        self.trader.ai_result = opening.identity, (D(".4"), 20, "AI"), None
        self.trader.tick(self.store)
        self.set_position(self.orders[-1]["quantity"])
        self.client.account.return_value[1][0]["markPrice"] = "90"
        self.asset["unrealizedProfit"] = "-100"
        future = Future()
        self.trader.model_pool = Mock()
        self.trader.model_pool.submit.return_value = future
        self.observe("awaiting-managed-ai")
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 1)
        self.observe("managed-source-close", action="close", opened=opening.time)
        self.trader.tick(self.store)
        self.assertFalse(future.done())
        self.assertEqual(len(self.orders), 2)
        self.assertEqual(self.orders[-1]["reduceOnly"], "true")
        self.client.account.return_value = self.asset, []
        future.set_result((D(".5"), 20, "late AI"))
        self.trader.tick(self.store)
        self.assertEqual(len(self.orders), 2)
        self.assertFalse(self.trader.owned(self.store))

    def test_model_deadline_is_logged_once_and_late_answer_does_not_submit_order(self):
        self.managed_ai()
        future = Future()
        self.trader.model_pool = Mock()
        self.trader.model_pool.submit.return_value = future
        signal = self.observe("slow-model")
        with patch("followtrade.execution.time.monotonic", return_value=0):
            self.trader.tick(self.store)
        with patch("followtrade.execution.time.monotonic", return_value=61), \
                self.assertLogs(level="WARNING") as logs:
            self.trader.tick(self.store)
        self.assertIn("AI deadline exceeded mode=demo", "\n".join(logs.output))
        self.assertIn("task_done=False", "\n".join(logs.output))
        self.assertEqual(self.store.db.execute(
            "SELECT result FROM jobs WHERE account='demo' AND signal=?", (signal.identity,)).fetchone()[0],
            "model deadline exceeded")
        future.set_result((D(".4"), 3, "late answer"))
        with patch("followtrade.execution.time.monotonic", return_value=62):
            self.trader.tick(self.store)
        self.assertEqual(self.orders, [])
        self.assertFalse(self.store.pending("demo"))

    def test_signed_requests_use_each_accounts_host_and_key(self):
        with patch("followtrade.binance.request_json", return_value={}) as request:
            Futures("demo", "demo-key", "secret").signed("/fapi/v1/order/test", "POST")
            Futures("mainnet", "live-key", "secret").signed("/fapi/v3/account")
        demo, live = request.call_args_list
        self.assertTrue(demo.args[1].startswith("https://demo-fapi.binance.com/"))
        self.assertTrue(live.args[1].startswith("https://fapi.binance.com/"))
        self.assertEqual([item.kwargs["headers"]["X-MBX-APIKEY"] for item in (demo, live)], ["demo-key", "live-key"])

    def test_mainnet_uses_wallet_equity_with_no_configured_budget(self):
        settings = self.cfg.database.with_name("config.json")
        settings.write_text(json.dumps({"execution": {"mainnet": {"budget_usdt": "100", "strategy": "fixed"}}}),
                            encoding="utf-8")
        with patch.dict("os.environ", {"FOLLOWTRADE_BINANCE_MAINNET_KEY": "live-key",
                                      "FOLLOWTRADE_BINANCE_MAINNET_SECRET": "live-secret"}):
            mainnet = load_config(settings).execution[0]
        self.assertIsNone(mainnet.budget)
        trader = ExchangeAccount(self.cfg, mainnet)
        trader.client = self.client
        choose_mode(self.store, self.cfg.execution, "mainnet")
        with self.store.transaction():
            self.store.put("execution.wallet_basis.mainnet", "900")
        self.observe("live-entry")
        trader.tick(self.store)
        order = self.orders[-1]
        notional = D(order["quantity"]) * D(order["price"])
        self.assertGreater(notional, D("149.80"))
        self.assertLessEqual(notional, D(150))
        self.assertIn("按子账户 USDT 权益", Telegram.reply(self.store, ("fixed",), "/mode", self.cfg.execution))

    def test_account_mode_error_identifies_which_setting_is_wrong(self):
        client = Futures("mainnet", "live-key", "secret")
        account_config = {"dualSidePosition": False, "multiAssetsMargin": False, "canTrade": True}
        with patch.object(client, "signed", return_value=account_config):
            client.check_mode()
            account_config["multiAssetsMargin"] = True
            with self.assertRaisesRegex(ServiceError, "保证金资产模式为多资产") as error:
                client.check_mode()
            self.assertNotIn("双向持仓", str(error.exception))
            account_config.update(dualSidePosition=True, multiAssetsMargin=False)
            with self.assertRaisesRegex(ServiceError, "持仓模式为双向持仓") as error:
                client.check_mode()
            self.assertNotIn("多资产", str(error.exception))
