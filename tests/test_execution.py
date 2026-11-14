from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from followtrade.binance import ExecutionAccount, Futures
from followtrade.clients import BinanceAPIError, ServiceError, Signal
from followtrade.config import load_config
from followtrade.execution import ExchangeAccount, choose_mode
from followtrade.store import Store, now_ms
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
        self.cfg = replace(cfg, database=Path(tmp.name) / "test.sqlite", execution=(demo, mainnet))
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

    def observe(self, identity, action="increase", opened=0):
        stamp = now_ms()
        signal = Signal(identity, stamp, SYMBOL, "LONG", action, D(10), D(100), opened)
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
