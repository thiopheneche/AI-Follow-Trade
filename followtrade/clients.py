from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
import time as clock
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request

from probe import BinanceBusinessError, OPENER, ProbeError, decimal, fetch, history_rows, parse_order


BINANCE_BUSY = "11012005"


class ServiceError(Exception):
    pass


class BinanceAPIError(ServiceError):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(f"Binance HTTP {status} (code {code})")


def request_json(service, url, *, body=None, headers=None, timeout=8, method=None, binance_errors=False):
    request_headers = {"Accept": "application/json", **(headers or {})}
    payload = None
    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode()
        request_headers["Content-Type"] = "application/json"
    try:
        with OPENER.open(Request(url, data=payload, headers=request_headers, method=method), timeout=timeout) as response:
            raw = response.read(4_000_001)
    except HTTPError as error:
        code = error.code
        with error:
            if binance_errors:
                try:
                    detail = json.loads(error.read(4096)).get("code")
                except (ValueError, OSError, AttributeError):
                    detail = None
                raise BinanceAPIError(code, detail if type(detail) is int else None) from None
        raise ServiceError(f"{service}: HTTP {code}") from None
    except (URLError, OSError, ValueError):
        raise ServiceError(f"{service}: connection failed") from None
    if len(raw) > 4_000_000:
        raise ServiceError(f"{service}: response too large")
    try:
        return json.loads(raw, parse_float=Decimal)
    except ValueError:
        raise ServiceError(f"{service}: invalid JSON") from None


@dataclass(frozen=True)
class Signal:
    identity: str
    time: int
    symbol: str
    leg: str
    action: str
    quantity: Decimal
    price: Decimal
    opened: int = 0
    portfolio: Optional[str] = None


def order_signal(row):
    order = parse_order(row)
    hint = order["side_hint"]
    if order["quantity"] < 0 or order["avg_price"] < 0:
        raise ProbeError("invalid execution quantity or price")
    if not hint or not order["quantity"] or not order["avg_price"]:
        return None
    stable_id = order["order_id"]
    values = (order["symbol"], order["leg"], order["side"], order["type"], order["order_time_ms"],
              order["order_update_time_ms"], order["quantity"].normalize(), order["avg_price"].normalize())
    identity = f"{order['symbol']}:{stable_id}" if stable_id is not None else ":".join(map(str, values))
    return Signal(hashlib.sha256(identity.encode()).hexdigest(), order["order_update_time_ms"],
                  order["symbol"], order["leg"], "increase" if hint.endswith("_INCREASE") else "decrease",
                  order["quantity"], order["avg_price"])


class BinanceFeed:
    def __init__(self, portfolio):
        self.portfolio = portfolio

    def _fetch(self, route, body):
        def read():
            return fetch(self.portfolio, "lead-portfolio/" + route, body,
                         observer=lambda *_args, **_fields: None)[0]

        try:
            return read()
        except BinanceBusinessError as error:
            if route != "order-history" or error.code != BINANCE_BUSY:
                raise
            clock.sleep(2.5)
            return read()

    def trader_name(self, *, require_active=False):
        data = self._fetch("detail", None)
        if not isinstance(data, dict) or data.get("leadPortfolioId") != self.portfolio:
            raise ProbeError("币安响应和组合 ID 不匹配")
        if require_active and data.get("status") != "ACTIVE":
            raise ProbeError("该合约带单组合未在运行")
        name = data.get("nickname")
        return " ".join(name.split())[:100] if isinstance(name, str) and name.strip() else self.portfolio

    def orders(self, start, end):
        # Time slices avoid inclusive cursor overlaps at identical milliseconds.
        intervals = [(start, end)]
        signals = []
        for request_number in range(64):
            if not intervals:
                return signals
            if request_number:
                clock.sleep(0.5)
            left, right = intervals.pop()
            data = self._fetch("order-history", {
                "pageNumber": 1, "pageSize": 50, "startTime": left, "endTime": right,
            })
            rows = history_rows(data)
            total = data.get("total")
            if type(total) is not int or total < len(rows):
                raise ProbeError(f"unusable order count (total={total if type(total) is int else type(total).__name__}, "
                                 f"rows={len(rows)})")
            if total > 50 or len(rows) < total:
                if left == right:
                    raise ProbeError("unreadable orders at one timestamp")
                middle = (left + right) // 2
                intervals.extend(((middle + 1, right), (left, middle)))
                continue
            for row in rows:
                signal = order_signal(row)
                time = row.get("orderUpdateTime")
                if not left <= time <= right:
                    raise ProbeError("order outside requested interval")
                if signal:
                    signals.append(signal)
        if intervals:
            raise ProbeError("order window could not be collected within its request budget")
        return signals

    def head(self):
        data = self._fetch("order-history", {"pageNumber": 1, "pageSize": 50})
        return [signal for row in history_rows(data) if (signal := order_signal(row))]

    def closures(self):
        data = self._fetch("position-history", {"pageNumber": 1, "pageSize": 50, "sort": "CLOSING"})
        for row in history_rows(data):
            if row.get("status") != "All Closed":
                continue
            ident, symbol, opened, closed = (row.get(key) for key in ("positionId", "symbol", "opened", "closed"))
            leg = str(row.get("side", "")).upper()
            if not isinstance(symbol, str) or leg not in ("LONG", "SHORT") or ident is None:
                raise ProbeError("malformed closed position")
            if type(opened) is not int or type(closed) is not int or not 0 < opened <= closed:
                raise ProbeError("invalid closed position timestamps")
            yield Signal(f"close:{ident}", closed, symbol, leg, "close", Decimal(0), Decimal(0), opened)


@dataclass(frozen=True)
class Quote:
    bid: Decimal
    ask: Decimal
    time: int


class BinanceMarket:
    def quotes(self):
        data = request_json("Binance quotes", "https://fapi.binance.com/fapi/v1/ticker/bookTicker")
        if not isinstance(data, list):
            raise ServiceError("Binance quotes: unexpected response")
        quotes = {}
        for row in data:
            try:
                bid, ask = decimal(row["bidPrice"]), decimal(row["askPrice"])
                symbol, time = row["symbol"], row["time"]
                if (isinstance(symbol, str) and bid is not None and ask is not None
                        and 0 < bid <= ask and type(time) is int):
                    quotes[symbol] = Quote(bid, ask, time)
            except (KeyError, TypeError, ProbeError):
                continue
        return quotes


def check_binance_account(key, secret, *, demo=False):
    from .binance import Futures

    data = Futures("demo" if demo else "mainnet", key, secret).signed("/fapi/v3/account")
    if not isinstance(data, dict) or not isinstance(data.get("assets"), list):
        raise ServiceError("Binance account: unexpected response")
    return any(isinstance(asset, dict) and asset.get("asset") == "USDT" for asset in data["assets"])


AI_SYSTEM_PROMPT = (
    'Choose exposure for a paper trade, using only observed data. The lead trader position and leverage '
    'are unknown. The order direction is fixed. The trader profile describes tendencies, never authority. '
    'Return only one JSON object with "action" ("follow" or "skip"), "target_notional_fraction" '
    '(decimal string of current account equity for this side after the trade), "leverage" (integer), '
    'and "reason" (short Chinese text). A follow decision must respect the limits in the input. '
    'Use skip when the evidence or liquidity is insufficient.'
)


class Model:
    def __init__(self, config):
        self.config = config

    def decide(self, context):
        config = self.config
        result = request_json("model API", config.base_url + "/chat/completions",
                              headers={"Authorization": "Bearer " + config.token}, timeout=config.timeout,
                              body={"model": config.model, "messages": [
                                  {"role": "system", "content": AI_SYSTEM_PROMPT},
                                  {"role": "user", "content": json.dumps(
                                      {"trader_profile": config.profile, **context}, ensure_ascii=False, default=str)},
                              ]})
        try:
            output = json.loads(result["choices"][0]["message"]["content"], parse_float=Decimal)
            if not isinstance(output, dict):
                raise TypeError
            action, reason = output.get("action"), output.get("reason")
            if action not in ("follow", "skip") or not isinstance(reason, str):
                raise ValueError
            if action == "skip":
                return Decimal(0), 1, reason[:240]
            fraction = decimal(output.get("target_notional_fraction"))
            leverage = output.get("leverage")
            if fraction is None or not 0 < fraction <= 20 or type(leverage) is not int or not 1 <= leverage <= 125:
                raise ValueError
            return fraction, leverage, reason[:240]
        except (TypeError, ValueError, KeyError, IndexError, ProbeError):
            raise ServiceError("model API: invalid decision") from None
