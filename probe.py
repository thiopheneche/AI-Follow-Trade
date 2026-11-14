"""Read-only Binance Futures Copy Trading probe. Python 3.10+, stdlib only."""

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


BASE = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/"


class ProbeError(Exception):
    pass


class AccessStopped(ProbeError):
    pass


class BinanceBusinessError(ProbeError):
    def __init__(self, path, code):
        self.code = code
        super().__init__(f"{path}: unexpected response (code {code})")


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


OPENER = build_opener(NoRedirects)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def emit(kind, **fields):
    print(json.dumps({"kind": kind, **fields}, ensure_ascii=True, default=str), flush=True)


def portfolio_id(value):
    if re.fullmatch(r"[0-9]{1,30}", value):
        return value
    try:
        url = urlsplit(value)
    except ValueError:
        raise argparse.ArgumentTypeError("invalid Binance share URL") from None
    match = re.fullmatch(r"/(?:[^/]+/)?copy-trading/lead-details/([0-9]{1,30})/?", url.path)
    if url.scheme == "https" and url.netloc.lower() == "www.binance.com" and match:
        return match.group(1)
    raise argparse.ArgumentTypeError("use a numeric portfolioId or Binance lead-details URL")


def fetch(pid, path, body=None, *, observer=emit):
    headers = {"Accept": "application/json", "User-Agent": "TradeFollowing research probe/0.1"}
    url = BASE + path
    data = None
    if body is None:
        url += "?" + urlencode({"portfolioId": pid})
    else:
        data = json.dumps({"portfolioId": pid, **body}).encode()
        headers["Content-Type"] = "application/json"
    sent_at = utc_now()
    started = time.monotonic()
    try:
        with OPENER.open(Request(url, data=data, headers=headers), timeout=10) as response:
            raw = response.read().decode("utf-8", errors="replace")
            status, response_headers = response.status, response.headers
    except HTTPError as error:
        with error:
            raw = error.read().decode("utf-8", errors="replace")
            status, response_headers = error.code, error.headers
    except (URLError, OSError) as error:
        raise ProbeError(f"{path}: {error}") from error
    observed_at = utc_now()
    observer(
        "response", portfolio_id=pid, endpoint=path, sent_at=sent_at, observed_at=observed_at,
        http_status=status, round_trip_ms=round((time.monotonic() - started) * 1000),
        server_date=response_headers.get("Date"), cache_age=response_headers.get("Age"),
        retry_after=response_headers.get("Retry-After"), raw_json=raw,
    )
    if status in {202, 401, 403, 418, 429, 451} or 300 <= status < 400:
        raise AccessStopped(f"{path}: stopped on HTTP {status}")
    if status != 200:
        raise ProbeError(f"{path}: HTTP {status}")
    if response_headers.get_content_type() == "text/html":
        raise AccessStopped(f"{path}: unexpected HTML")
    try:
        payload = json.loads(raw, parse_float=Decimal)
    except ValueError as error:
        raise ProbeError(f"{path}: invalid JSON") from error
    if not isinstance(payload, dict) or payload.get("code") != "000000" or payload.get("success") is not True:
        code = payload.get("code") if isinstance(payload, dict) else None
        raise BinanceBusinessError(path, code)
    return payload.get("data"), observed_at


def decimal(value):
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation as error:
        raise ProbeError(f"invalid decimal {value!r}") from error
    if not number.is_finite():
        raise ProbeError(f"invalid decimal {value!r}")
    return number


def parse_positions(rows, *, optional_number=decimal):
    if not isinstance(rows, list) or not rows:
        raise ProbeError("positions missing or empty, visibility and flat state cannot be inferred")
    positions = {}
    active_modes = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ProbeError("invalid position row")
        symbol, leg = row.get("symbol"), row.get("positionSide")
        quantity = decimal(row.get("positionAmount"))
        if not isinstance(symbol, str) or not symbol or leg not in ("BOTH", "LONG", "SHORT") or quantity is None:
            raise ProbeError("invalid symbol, leg or positionAmount")
        key = (symbol, leg)
        if key in positions:
            raise ProbeError(f"duplicate position key {key}")
        if quantity:
            mode = "ONE_WAY" if leg == "BOTH" else "HEDGE"
            if active_modes.setdefault(symbol, mode) != mode:
                raise ProbeError(f"conflicting position modes for {symbol}")
        isolated = row.get("isolated")
        positions[key] = {
            "symbol": symbol,
            "leg": leg,
            "direction": ("LONG" if quantity > 0 else "SHORT") if leg == "BOTH" and quantity else leg,
            "quantity": quantity if leg == "BOTH" else abs(quantity),
            "notional_value": optional_number(row.get("notionalValue")),
            "entry_price": optional_number(row.get("entryPrice")),
            "mark_price": optional_number(row.get("markPrice")),
            "configured_leverage": optional_number(row.get("leverage")),
            "isolated_wallet": optional_number(row.get("isolatedWallet")),
            "initial_margin": optional_number(row.get("positionInitialMargin")),
            "margin_mode": {True: "ISOLATED", False: "CROSS"}.get(isolated) if type(isolated) is bool else None,
        }
    return positions


def position_changes(previous, current):
    for key in sorted(previous.keys() | current.keys()):
        old, new = previous.get(key), current.get(key)
        before = old["quantity"] if old else None
        after = new["quantity"] if new else None
        if old is None:
            if after:
                yield "POSITION_FIRST_SEEN", old, new
        elif new is None:
            if before:
                yield "POSITION_ROW_MISSING", old, new
        elif before != after:
            if not before:
                kind = "OPEN"
            elif not after:
                kind = "CLOSE"
            elif (before > 0) != (after > 0):
                kind = "REVERSE"
            else:
                kind = "ADD" if abs(after) > abs(before) else "REDUCE"
            yield kind, old, new
        elif after and old["entry_price"] != new["entry_price"]:
            if old["entry_price"] is not None and new["entry_price"] is not None:
                yield "ENTRY_PRICE_CHANGED", old, new


def history_rows(data):
    rows = data.get("list") if isinstance(data, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ProbeError("history data.list is missing or malformed")
    return rows


def parse_order(row):
    symbol, side, leg = row.get("symbol"), row.get("side"), row.get("positionSide")
    order_time, update_time = row.get("orderTime"), row.get("orderUpdateTime")
    order_id, order_type = row.get("orderId"), row.get("type")
    quantity, price = decimal(row.get("executedQty")), decimal(row.get("avgPrice"))
    if (
        not isinstance(symbol, str) or not symbol or side not in ("BUY", "SELL")
        or leg not in ("LONG", "SHORT", "BOTH") or type(order_time) is not int
        or type(update_time) is not int or quantity is None or price is None
        or (order_id is not None and type(order_id) not in (str, int))
        or (order_type is not None and not isinstance(order_type, str))
    ):
        raise ProbeError("malformed order record")
    order = {
        "symbol": symbol, "side": side, "leg": leg, "quantity": quantity, "avg_price": price,
        "order_time_ms": order_time, "order_update_time_ms": update_time, "type": order_type,
        "order_id": order_id, "total_pnl": decimal(row.get("totalPnl")),
    }
    direction = {("BUY", "LONG"): "LONG_INCREASE", ("SELL", "LONG"): "LONG_DECREASE",
                 ("SELL", "SHORT"): "SHORT_INCREASE", ("BUY", "SHORT"): "SHORT_DECREASE"}
    order["side_hint"] = direction.get((side, leg))
    return order


class Monitor:
    def __init__(self, pid, page_size):
        self.pid = pid
        self.page = {"pageNumber": 1, "pageSize": page_size}
        self.previous_positions = None
        self.seen_orders = None
        self.previous_order_page = set()

    def positions(self):
        pid = self.pid
        detail, observed = fetch(pid, "lead-portfolio/detail")
        if not isinstance(detail, dict) or detail.get("leadPortfolioId") != pid:
            raise ProbeError("detail does not match portfolioId")
        visibility = detail.get("positionShow")
        if detail.get("status") != "ACTIVE" or visibility is not True:
            self.previous_positions = None
            emit("position_visibility", portfolio_id=pid, observed_at=observed, position_show=visibility,
                 portfolio_status=detail.get("status"), state="unavailable")
            return
        raw, observed = fetch(pid, "lead-data/positions")
        positions = parse_positions(raw)
        active = [row for row in positions.values() if row["quantity"]]
        emit("positions", portfolio_id=pid, observed_at=observed, row_count=len(raw), active=active)
        if self.previous_positions is not None:
            for kind, before, after in position_changes(self.previous_positions, positions):
                emit("position_change", portfolio_id=pid, observed_at=observed, detected_at=utc_now(), event=kind,
                     before=before, after=after)
        self.previous_positions = positions

    def orders(self):
        pid = self.pid
        data, observed = fetch(pid, "lead-portfolio/order-history", self.page)
        orders = [parse_order(row) for row in history_rows(data)]
        emit("orders", portfolio_id=pid, observed_at=observed, total=data.get("total"), recent=orders)
        occurrences = Counter()
        for order in reversed(orders):
            key = tuple(order[name] for name in (
                "order_id", "symbol", "side", "leg", "order_time_ms", "order_update_time_ms",
                "quantity", "avg_price", "type", "total_pnl",
            ))
            occurrences[key] += 1
            if self.seen_orders is not None and occurrences[key] > self.seen_orders[key]:
                emit("order_first_seen", portfolio_id=pid, observed_at=observed, detected_at=utc_now(), order=order)
        if self.previous_order_page and len(orders) >= self.page["pageSize"]:
            if self.previous_order_page.isdisjoint(occurrences):
                emit("order_page_gap_possible", portfolio_id=pid, observed_at=observed)
        self.seen_orders = occurrences if self.seen_orders is None else self.seen_orders | occurrences
        if occurrences:
            self.previous_order_page = set(occurrences)

    def closed_history(self):
        data, observed = fetch(self.pid, "lead-portfolio/position-history", self.page)
        emit("closed_history", portfolio_id=self.pid, observed_at=observed,
             total=data.get("total") if isinstance(data, dict) else None, rows=len(history_rows(data)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("portfolio", nargs="+", type=portfolio_id)
    parser.add_argument("--interval", type=int, help="repeat after this many seconds; default is one round")
    parser.add_argument("--page-size", type=int, default=20, choices=range(1, 51), metavar="1..50")
    parser.add_argument("--closed-history", action="store_true", help="also read closed history on the first round")
    args = parser.parse_args()
    if args.interval is not None and args.interval <= 0:
        parser.error("--interval must be positive")
    monitors = [Monitor(pid, args.page_size) for pid in dict.fromkeys(args.portfolio)]
    first = True
    retry_delay = 0
    try:
        while True:
            failed = False
            for monitor in monitors:
                actions = [monitor.positions, monitor.orders]
                if first and args.closed_history:
                    actions.append(monitor.closed_history)
                for action in actions:
                    try:
                        action()
                    except AccessStopped as error:
                        emit("access_stopped", portfolio_id=monitor.pid, message=str(error))
                        return 2
                    except ProbeError as error:
                        failed = True
                        if action == monitor.positions:
                            monitor.previous_positions = None
                        emit("probe_error", portfolio_id=monitor.pid, message=str(error))
            if args.interval is None:
                return int(failed)
            first = False
            retry_delay = min(600, max(30, retry_delay * 2)) if failed else 0
            time.sleep(max(args.interval, retry_delay))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
