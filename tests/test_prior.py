from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from followtrade.clients import Model, Quote, Signal
from followtrade.config import AIConfig, load_config
from followtrade.execution import ExchangeAccount, WAITING_FOR_MODEL
from followtrade.paper import PaperEngine
from followtrade.prior import (DAY_MS, HISTORY, KEY_PREFIX, PRIOR_MAX_AGE_MS, analyze_evidence,
                               collect_evidence, prior_context, save_prior)
from followtrade.store import Store


D = Decimal
PID = "5075281354358777856"
NOW = 1_790_000_000_000


class PriorTest(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        cfg = load_config(Path(__file__).resolve().parents[1] / "config.example.json")
        self.cfg = replace(cfg, database=Path(temporary.name) / "test.sqlite")
        self.store = Store(self.cfg.database, PID, D(1000), ("ai",))
        self.addCleanup(self.store.db.close)
        self.opened, self.closed = NOW - 100_000, NOW - 1000
        self.history = [{"positionId": "1", "symbol": "BTCUSDT", "side": "Long", "status": "All Closed",
                         "opened": self.opened, "closed": self.closed, "type": "UM", "avgCost": "100",
                         "maxOpenInterest": "10", "leverage": "5", "isolated": "Cross"}]
        self.orders = [self.order("a", "BUY", "4", self.opened),
                       self.order("b", "BUY", "6", self.opened + 5000),
                       self.order("c", "SELL", "10", self.closed)]

    @staticmethod
    def order(identity, side, quantity, at):
        return {"orderId": identity, "symbol": "BTCUSDT", "positionSide": "LONG", "side": side,
                "executedQty": quantity, "avgPrice": "100", "orderTime": at, "orderUpdateTime": at}

    def evidence(self):
        stamp = datetime.fromtimestamp(NOW / 1000, timezone.utc).isoformat()

        def response(endpoint, data):
            return {"http_status": 200, "endpoint": endpoint, "observed_at": stamp,
                    "request": {"portfolioId": PID},
                    "response": {"code": "000000", "success": True, "data": data}}

        return {"snapshot": {"portfolioId": PID, "marginBalance": "500", "marginBalanceAsset": "USDT",
                             "positionVisibility": "hidden", "positions": None, "effectiveLeverage": None,
                             "metadata": {"marginBalance": {"observedAt": stamp}}},
                "orderCollection": {"startTime": NOW - DAY_MS, "endTime": NOW, "complete": True},
                "rawResponses": [response("lead-portfolio/detail", {"leadPortfolioId": PID, "nickname": "测试交易员"}),
                                 response(HISTORY, {"total": 1, "list": self.history}),
                                 response("lead-portfolio/order-history", {"total": 3, "list": self.orders})]}

    def test_ratios_use_cycle_peak_and_never_current_balance_as_historical_equity(self):
        prior = analyze_evidence(self.evidence())
        self.assertEqual(prior["history"]["rows"], 1)
        self.assertTrue(prior["history"]["complete"])
        self.assertIsNone(prior["historicalAccountExposureFractions"])
        self.assertIsNone(prior["snapshotAtCollection"]["accountGrossExposureFraction"])
        self.assertEqual(prior["executionStyle"]["reconciledClosedCycles"], 1)
        self.assertEqual(D(prior["executionStyle"]["initialQuantityFractionOfCyclePeak"]["median"]), D(".4"))
        self.assertEqual(D(prior["executionStyle"]["additionQuantityFractionOfCyclePeak"]["median"]), D(".6"))
        self.assertEqual(D(prior["history"]["all"]["cyclePeakCostNotionalProxyUsdt"]["median"]), D(1000))

    def test_incomplete_or_unreconciled_orders_do_not_produce_sizing_ratios(self):
        for case in ("incomplete", "missing_close", "wrong_peak", "started_before_window", "ambiguous"):
            with self.subTest(case=case):
                evidence = deepcopy(self.evidence())
                rows = evidence["rawResponses"][1]["response"]["data"]["list"]
                if case == "incomplete":
                    evidence["orderCollection"]["complete"] = False
                elif case == "missing_close":
                    evidence["rawResponses"][2]["response"]["data"]["list"].pop()
                elif case == "wrong_peak":
                    rows[0]["maxOpenInterest"] = "12"
                elif case == "started_before_window":
                    evidence["orderCollection"]["startTime"] = self.opened + 1
                else:
                    rows.append({**rows[0], "positionId": "2"})
                prior = analyze_evidence(evidence)
                self.assertEqual(prior["executionStyle"]["reconciledClosedCycles"], 0)
                self.assertIsNone(prior["executionStyle"]["initialQuantityFractionOfCyclePeak"]["median"])

    def test_duplicate_pages_and_outside_window_orders_do_not_inflate_samples(self):
        evidence = self.evidence()
        evidence["rawResponses"].append(deepcopy(evidence["rawResponses"][-1]))
        evidence["rawResponses"][-1]["response"]["data"]["list"].append(self.order("old", "BUY", "100", NOW - 2 * DAY_MS))
        prior = analyze_evidence(evidence)
        self.assertEqual(prior["executionStyle"]["uniqueOrders"], 3)
        self.assertEqual(prior["executionStyle"]["reconciledClosedCycles"], 1)
        self.assertEqual(prior["executionStyle"]["cyclesWithAdditions"], 1)

    def test_bad_leverage_and_non_usdt_notional_stay_unknown(self):
        self.history[0].update(leverage="NaN", symbol="BTCUSDC")
        prior = analyze_evidence(self.evidence())
        self.assertEqual(prior["history"]["all"]["reportedLeverage"]["count"], 0)
        self.assertIsNone(prior["history"]["all"]["cyclePeakCostNotionalProxyUsdt"]["median"])

    def test_portfolio_scope_expiration_and_invalid_cache(self):
        prior = analyze_evidence(self.evidence())
        save_prior(self.cfg.database, prior)
        context = prior_context(self.store, PID, NOW, symbol="BTCUSDT")["lead_trader_sizing_prior"]
        self.assertTrue(context["available"])
        self.assertEqual(context["signalSymbolRecent30Days"]["closedCycles"], 1)
        self.assertNotIn("bySymbolRecent30Days", context)
        self.assertFalse(prior_context(self.store, "123", NOW)["lead_trader_sizing_prior"]["available"])
        for at in (NOW + PRIOR_MAX_AGE_MS + 1, NOW - 5001):
            self.assertEqual(prior_context(self.store, PID, at)["lead_trader_sizing_prior"]["reason"], "expired")
        for raw in ("bad json", "[]", json.dumps({**prior, "portfolioId": "123"}),
                    json.dumps({**prior, "collectedAtMs": "bad"}),
                    json.dumps({**prior, "bySymbolRecent30Days": None})):
            with self.subTest(raw=raw[:40]):
                with self.store.transaction():
                    self.store.put(KEY_PREFIX + PID, raw)
                self.assertEqual(prior_context(self.store, PID, NOW)["lead_trader_sizing_prior"]["reason"], "invalid_cache")

    def test_both_paper_and_exchange_ai_receive_prior_without_extra_api_calls(self):
        save_prior(self.cfg.database, analyze_evidence(self.evidence()))
        signal = Signal("test", NOW - 1, "BTCUSDT", "LONG", "increase", D(10), D(100), portfolio=PID)
        context = PaperEngine(self.store, self.cfg).ai_context(signal, Quote(D(100), D(100), NOW), NOW)
        self.assertTrue(context["lead_trader_sizing_prior"]["available"])
        account = Mock(mode="demo", key="key", secret="secret", entry_deviation=None, strategy="ai")
        worker = ExchangeAccount(self.cfg, account)
        worker.client = Mock()
        worker.client.quote.return_value = D(100), D(100)
        worker.client.filters.return_value = {"PRICE_FILTER": {"tickSize": ".01"}, "MIN_NOTIONAL": {"notional": "5"}}
        worker._model_decision = Mock(return_value=WAITING_FOR_MODEL)
        with patch("followtrade.execution.now_ms", return_value=NOW):
            worker._entry(self.store, signal, {"availableBalance": "1000"}, {}, {}, D(1000), D(1000), False)
        sent = worker._model_decision.call_args.args[1]
        self.assertTrue(sent["lead_trader_sizing_prior"]["available"])
        self.assertEqual(sent["order_notional_fraction_limit"], D(".15"))
        worker.client.signed.assert_not_called()
        result = {"choices": [{"message": {"content": json.dumps({"action": "follow",
                  "order_notional_fraction": ".1", "leverage": 3, "reason": "参考历史分批加仓"})}}]}
        model = Model(AIConfig("https://example.test/v1", "test", "test-key", 5, "profile"))
        with patch("followtrade.clients.request_json", return_value=result) as api:
            model.decide(sent)
        payload = json.loads(api.call_args.kwargs["body"]["messages"][1]["content"])
        self.assertEqual(payload["lead_trader_sizing_prior"]["portfolioId"], PID)
        self.assertEqual(payload["lead_trader_sizing_prior"]["executionStyle"]["reconciledClosedCycles"], 1)

    def test_access_stop_prevents_subsequent_public_order_collection(self):
        snapshot = {"accessStopped": True, "positionHistory": None}
        with patch("followtrade.prior.get_trader_position_leverage", return_value=snapshot), \
                patch("followtrade.prior.BinanceFeed.orders") as orders:
            evidence = collect_evidence(PID)
        orders.assert_not_called()
        self.assertFalse(evidence["orderCollection"]["complete"])


if __name__ == "__main__":
    unittest.main()
