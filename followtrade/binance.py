from dataclasses import dataclass
from decimal import Decimal, DecimalException, ROUND_DOWN, ROUND_UP
import hashlib
import hmac
import time
from typing import Optional
from urllib.parse import urlencode

from .clients import ServiceError, request_json


BASE_URLS = {"demo": "https://demo-fapi.binance.com", "mainnet": "https://fapi.binance.com"}
MODE_LABELS = {"paper": "本地模拟", "demo": "币安 Demo", "mainnet": "币安正式账户"}


def credentials_prefix(mode):
    return f"FOLLOWTRADE_BINANCE_{mode.upper()}_"


def as_decimal(value):
    try:
        number = Decimal(str(value))
    except DecimalException:
        raise ServiceError("Binance returned an invalid numeric field") from None
    if not number.is_finite():
        raise ServiceError("Binance returned a non-finite value")
    return number


def rounded(value, step, direction=ROUND_DOWN):
    return (value / step).to_integral_value(rounding=direction) * step


@dataclass(frozen=True)
class ExecutionAccount:
    mode: str
    key: str
    secret: str
    budget: Optional[Decimal]
    strategy: str
    entry_deviation: Optional[Decimal] = None


class Futures:
    def __init__(self, mode, key, secret):
        self.base_url, self.key, self.secret = BASE_URLS[mode], key, secret
        self.rules = {}
        self.rules_until = 0
        self.brackets = {}

    def signed(self, route, method="GET", **params):
        query = urlencode({**params, "timestamp": time.time_ns() // 1_000_000, "recvWindow": 5000})
        signature = hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        result = request_json("Binance", f"{self.base_url}{route}?{query}&signature={signature}",
                              headers={"X-MBX-APIKEY": self.key}, method=method, binance_errors=True, timeout=5)
        if isinstance(result, dict) and type(result.get("code")) is int and result["code"] < 0:
            raise ServiceError(f"Binance code {result['code']}")
        return result

    def public(self, route, **params):
        url = self.base_url + route
        if params:
            url += "?" + urlencode(params)
        return request_json("Binance", url, timeout=5)

    def account(self):
        asset = next((row for row in self.account_assets() if row["asset"] == "USDT"), None)
        if not asset:
            raise ServiceError("Binance account has no USDT asset record")
        return asset, self.position_risk()

    def account_assets(self):
        account = self.signed("/fapi/v3/account")
        if not isinstance(account, dict) or not isinstance(account.get("assets"), list):
            raise ServiceError("Binance returned an invalid account response")
        return account["assets"]

    def income_history(self, start, end):
        limit = 1000
        windows = [(start, end)]
        for _ in range(15):
            left, right = windows.pop()
            rows = self.signed("/fapi/v1/income", startTime=left, endTime=right, limit=limit)
            if not isinstance(rows, list) or any(
                not isinstance(row, dict) or type(row.get("time")) is not int or not left <= row["time"] <= right
                for row in rows
            ):
                raise ServiceError("币安资金流水格式异常")
            if len(rows) < limit:
                yield from rows
            elif left < right:
                middle = (left + right) // 2
                windows.extend(((middle + 1, right), (left, middle)))
            else:
                raise ServiceError("同一毫秒的币安资金流水超出查询上限")
            if not windows:
                return
        raise ServiceError("当日资金流水超出本次查询上限")

    def position_risk(self):
        positions = self.signed("/fapi/v3/positionRisk")
        if not isinstance(positions, list):
            raise ServiceError("Binance returned an invalid position response")
        return positions

    def check_mode(self):
        mode = self.signed("/fapi/v1/accountConfig")
        if not isinstance(mode, dict):
            raise ServiceError("币安未返回有效的 U 本位合约账户配置。")
        problems = []
        for field, enabled, disabled in (
            ("dualSidePosition", "持仓模式为双向持仓，请在子账户的 U 本位合约设置里切换为单向持仓（One-way Mode）。",
             "币安未返回有效的持仓模式（dualSidePosition）。"),
            ("multiAssetsMargin", "保证金资产模式为多资产，请在 U 本位合约页面点击 M，切换为单资产模式（Single-Asset Mode）。",
             "币安未返回有效的保证金资产模式（multiAssetsMargin）。"),
        ):
            value = mode.get(field)
            if value is True or value == "true":
                problems.append(enabled)
            elif value is not False and value != "false":
                problems.append(disabled)
        if problems:
            raise ServiceError("\n".join(problems))
        if mode.get("canTrade") is not True and mode.get("canTrade") != "true":
            raise ServiceError("该 U 本位合约账户当前不可交易，请检查账户状态。")

    def filters(self, symbol):
        if self.rules_until <= time.monotonic():
            info = self.public("/fapi/v1/exchangeInfo")
            self.rules = {row["symbol"]: {item["filterType"]: item for item in row["filters"]}
                          for row in info["symbols"] if row["status"] == "TRADING" and row["quoteAsset"] == "USDT"}
            self.rules_until = time.monotonic() + 900
        if symbol not in self.rules:
            raise ServiceError(f"Binance contract {symbol} unavailable")
        return self.rules[symbol]

    def quote(self, symbol):
        quote = self.public("/fapi/v1/ticker/bookTicker", symbol=symbol)
        bid, ask = as_decimal(quote["bidPrice"]), as_decimal(quote["askPrice"])
        if not 0 < bid <= ask or abs(time.time_ns() // 1_000_000 - quote["time"]) > 30_000:
            raise ServiceError(f"Binance quote stale or invalid for {symbol}")
        return bid, ask

    def leverage_brackets(self, symbol):
        cached = self.brackets.get(symbol)
        if cached is not None and cached[0] > time.monotonic():
            return cached[1]
        result = self.signed("/fapi/v1/leverageBracket", symbol=symbol)
        rows = result if isinstance(result, list) else [result]
        matching = [row for row in rows if isinstance(row, dict) and row.get("symbol") == symbol]
        if len(matching) != 1 or not isinstance(matching[0].get("brackets"), list) or not matching[0]["brackets"]:
            raise ServiceError("Binance returned invalid leverage brackets")
        brackets = []
        for row in matching[0]["brackets"]:
            if not isinstance(row, dict) or type(row.get("initialLeverage")) is not int or row["initialLeverage"] < 1:
                raise ServiceError("Binance returned invalid leverage brackets")
            floor, cap = as_decimal(row.get("notionalFloor")), as_decimal(row.get("notionalCap"))
            if not 0 <= floor < cap:
                raise ServiceError("Binance returned invalid notional brackets")
            brackets.append({"initialLeverage": row["initialLeverage"], "notionalFloor": floor, "notionalCap": cap})
        value = {"symbol": symbol, "notionalCoef": matching[0].get("notionalCoef"), "brackets": brackets}
        self.brackets[symbol] = time.monotonic() + 300, value
        return value

    def test_order(self):
        symbol = "BTCUSDT"
        rules = self.filters(symbol)
        lot, price_rule = rules["LOT_SIZE"], rules["PRICE_FILTER"]
        bid, _ = self.quote(symbol)
        price = rounded(bid, as_decimal(price_rule["tickSize"]))
        notional = as_decimal(rules["MIN_NOTIONAL"]["notional"]) * Decimal("1.01")
        quantity = max(as_decimal(lot["minQty"]), rounded(notional / price, as_decimal(lot["stepSize"]), ROUND_UP))
        self.signed("/fapi/v1/order/test", "POST", symbol=symbol, side="BUY", type="LIMIT",
                    timeInForce="IOC", price=format(price, "f"), quantity=format(quantity, "f"))
