from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import unittest
from unittest.mock import patch

from followtrade import trader
from probe import AccessStopped


PORTFOLIO = "5075281354358777856"


def position(symbol="BTCUSDT", **fields):
    return {
        "symbol": symbol, "positionSide": "BOTH", "positionAmount": "2", "entryPrice": "90",
        "markPrice": "100", "leverage": 10, "isolated": False, "collateral": "USDT", **fields,
    }


class TraderTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.detail = {
            "leadPortfolioId": PORTFOLIO, "marginBalance": "1000", "initInvestAsset": "USDT",
            "status": "ACTIVE", "positionShow": True, "futuresType": "UM",
            "aumAmount": "50000", "copyAum": "49000",
        }
        self.positions = [position()]
        self.history = {"total": 2, "list": [
            {"symbol": "BTCUSDT", "side": "Long", "leverage": "3", "isolated": "Cross", "positionId": "7",
             "status": "All Closed", "opened": 100, "closed": 200, "updateTime": 210},
            {"symbol": "BTCUSDT", "side": "Short", "leverage": "8", "isolated": "Isolated",
             "positionId": "8", "status": "Partially Closed"},
        ]}
        self.age = {}
        self.delay = {}
        self.access_stop = None
        mocked = patch.object(trader, "fetch", side_effect=self.response)
        self.fetch = mocked.start()
        self.addCleanup(mocked.stop)

    def response(self, portfolio, endpoint, body, *, observer):
        blocked = endpoint == self.access_stop
        data = {
            trader.DETAIL: self.detail, trader.POSITIONS: self.positions, trader.HISTORY: self.history,
        }[endpoint]
        at = (self.now - timedelta(seconds=self.delay.get(endpoint, 0))).isoformat()
        raw = {"code": "000000", "data": data, "success": True}
        if blocked:
            raw = {"code": "blocked", "data": None, "success": False}
        observer(
            "response", endpoint=endpoint, portfolio_id=portfolio, observed_at=at,
            http_status=429 if blocked else 200, cache_age=self.age.get(endpoint), raw_json=json.dumps(raw),
        )
        if blocked:
            raise AccessStopped(f"{endpoint}: stopped on HTTP 429")
        return data, at

    def test_balance_uses_only_margin_balance_and_captures_sanitized_response(self):
        self.detail.update(marginBalance="1234.56789012", userId="private", contact={"api_key": "private"})
        capture = []
        result = trader.get_trader_margin_balance(PORTFOLIO, capture=capture)
        self.assertEqual(result["marginBalance"], Decimal("1234.56789012"))
        self.assertEqual(result["marginBalanceAsset"], "USDT")
        source = result["metadata"]["marginBalance"]
        self.assertEqual(source["path"], "data.marginBalance")
        self.assertEqual(source["observedAt"], self.now.isoformat())
        self.assertFalse(source["calculated"])
        self.assertEqual(capture[0]["response"]["data"]["marginBalance"], "1234.56789012")
        self.assertNotIn('"private"', json.dumps(capture))
        del self.detail["marginBalance"]
        self.assertIsNone(trader.get_trader_margin_balance(PORTFOLIO)["marginBalance"])

    def test_public_long_and_short_report_raw_leverage_and_estimated_metrics(self):
        self.positions = [
            position("XRPUSDT", positionAmount="0"),
            position(),
            position("ETHUSDT", positionAmount="-5", markPrice="40", leverage="5", isolated=True),
        ]
        result = trader.get_trader_position_leverage(PORTFOLIO, history_page=2)
        long, short = result["positions"]
        self.assertEqual((long["side"], short["side"]), ("LONG", "SHORT"))
        self.assertEqual((long["marginMode"], short["marginMode"]), ("CROSS", "ISOLATED"))
        self.assertEqual((long["configuredLeverage"], short["configuredLeverage"]), (10, 5))
        self.assertEqual(long["metadata"]["configuredLeverage"]["path"], "data[1].leverage")
        self.assertFalse(long["metadata"]["configuredLeverage"]["calculated"])
        self.assertEqual(short["estimatedInitialMargin"], Decimal("40"))
        self.assertEqual(short["positionExposureRatio"], Decimal("0.2"))
        self.assertEqual(result["effectiveLeverage"], Decimal("0.4"))
        self.assertEqual(result["totalAbsolutePositionNotional"], Decimal("400"))
        self.assertTrue(result["metadata"]["effectiveLeverage"]["calculated"])
        history = result["positionHistory"]["positions"]
        self.assertEqual((history[0]["configuredLeverage"], history[1]["marginMode"]), (3, "ISOLATED"))
        self.assertEqual(history[0]["metadata"]["configuredLeverage"]["path"], "data.list[0].leverage")
        self.assertEqual(history[0]["recordUpdatedAtMs"], 210)
        self.assertEqual(self.fetch.call_args.args[2], {"pageNumber": 2, "pageSize": 50, "sort": "CLOSING"})

    def test_hidden_positions_do_not_take_leverage_from_history(self):
        self.detail["positionShow"] = False
        result = trader.get_trader_position_leverage(PORTFOLIO, history_page=1)
        self.assertEqual(result["positionVisibility"], "hidden")
        self.assertIsNone(result["positions"])
        self.assertIsNone(result["effectiveLeverage"])
        self.assertEqual(result["positionHistory"]["positions"][0]["configuredLeverage"], 3)
        self.assertEqual([call.args[1] for call in self.fetch.call_args_list], [trader.DETAIL, trader.HISTORY])

    def test_empty_payload_does_not_confirm_flat_account(self):
        self.positions = []
        result = trader.get_trader_position_leverage(PORTFOLIO)
        self.assertEqual(result["positionVisibility"], "empty_unconfirmed")
        self.assertIsNone(result["totalAbsolutePositionNotional"])
        self.positions = [position(positionAmount="0", markPrice="0")]
        result = trader.get_trader_position_leverage(PORTFOLIO)
        self.assertEqual(result["positions"], [])
        self.assertEqual(result["effectiveLeverage"], Decimal(0))

    def test_bad_optional_fields_do_not_discard_positions_or_invent_values(self):
        self.positions = [
            position(leverage="3.5", isolated="false"),
            position("ETHUSDT", markPrice="NaN", leverage=-1),
        ]
        result = trader.get_trader_position_leverage(PORTFOLIO)
        first, second = result["positions"]
        self.assertIsNone(first["configuredLeverage"])
        self.assertIsNone(first["marginMode"])
        self.assertIsNone(first["estimatedInitialMargin"])
        self.assertEqual(first["positionExposureRatio"], Decimal("0.2"))
        self.assertIsNone(second["markPrice"])
        self.assertIsNone(second["configuredLeverage"])
        self.assertIsNone(result["totalAbsolutePositionNotional"])
        self.assertIsNone(result["effectiveLeverage"])

    def test_invalid_balance_or_timing_prevents_cross_response_metrics(self):
        cases = [
            ("0", {}, {}, True),
            ("-5", {}, {}, True),
            ("garbage", {}, {}, True),
            ("1000", {trader.DETAIL: "120"}, {}, True),
            ("1000", {}, {trader.DETAIL: 20}, True),
            ("1000", {trader.POSITIONS: "120"}, {}, False),
        ]
        for balance, self.age, self.delay, notional_available in cases:
            with self.subTest(balance=balance, age=self.age, delay=self.delay):
                self.detail["marginBalance"] = balance
                result = trader.get_trader_position_leverage(PORTFOLIO)
                first = result["positions"][0]
                self.assertEqual(first["configuredLeverage"], 10)
                self.assertEqual(first["positionNotional"] is not None, notional_available)
                self.assertIsNone(first["positionExposureRatio"])
                self.assertIsNone(result["effectiveLeverage"])

    def test_non_usdt_collateral_keeps_raw_fields_without_account_metrics(self):
        self.positions = [position(collateral="USDC")]
        result = trader.get_trader_position_leverage(PORTFOLIO)
        self.assertEqual(result["positions"][0]["configuredLeverage"], 10)
        self.assertIsNone(result["positions"][0]["positionNotional"])
        self.assertIsNone(result["effectiveLeverage"])

    def test_access_stop_records_status_and_skips_further_requests(self):
        self.access_stop = trader.POSITIONS
        result = trader.get_trader_position_leverage(PORTFOLIO, history_page=1)
        self.assertEqual(result["marginBalance"], Decimal("1000"))
        self.assertTrue(result["accessStopped"])
        self.assertEqual(result["errors"][0]["httpStatus"], 429)
        self.assertIsNone(result["positionHistory"])
        self.assertEqual(self.fetch.call_count, 2)

    def test_different_portfolio_does_not_expose_details_or_fetch_positions(self):
        self.detail["leadPortfolioId"] = "1"
        result = trader.get_trader_position_leverage(PORTFOLIO)
        self.assertIsNone(result["marginBalance"])
        self.assertIsNone(result["positions"])
        self.assertTrue(result["errors"])
        self.assertEqual(self.fetch.call_count, 1)
