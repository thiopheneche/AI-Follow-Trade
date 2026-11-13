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


class Futures:
    def __init__(self, mode, key, secret):
        self.base_url, self.key, self.secret = BASE_URLS[mode], key, secret
        self.rules = {}
        self.rules_until = 0

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
        account = self.signed("/fapi/v3/account")
        if not isinstance(account, dict) or not isinstance(account.get("assets"), list):
            raise ServiceError("Binance returned an invalid account response")
        asset = next((row for row in account["assets"] if row["asset"] == "USDT"), None)
        if not asset:
            raise ServiceError("Binance account has no USDT asset record")
        positions = self.signed("/fapi/v3/positionRisk")
        if not isinstance(positions, list):
            raise ServiceError("Binance returned an invalid position response")
        return asset, positions

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
