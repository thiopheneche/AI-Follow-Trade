"""Read Binance Futures lead-trader balances and position configuration."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Optional

from probe import AccessStopped, ProbeError, decimal, fetch, history_rows, parse_positions, portfolio_id


DETAIL = "lead-portfolio/detail"
POSITIONS = "lead-data/positions"
HISTORY = "lead-portfolio/position-history"
MAX_AGE = timedelta(seconds=60)
MAX_SKEW = timedelta(seconds=15)
PRIVATE_FIELDS = {
    "cookie", "setcookie", "authorization", "apikey", "apisecret", "secret",
    "privatekey", "token", "accesstoken", "refreshtoken", "userid", "email", "mobile",
    "privateleadportfolioid", "avatarurl", "description", "desctranslate", "pgcusername",
}


def _number(value):
    try:
        return decimal(value)
    except ProbeError:
        return None


def _text(value):
    return value if isinstance(value, str) and value.strip() else None


def _positive(value):
    return value if value is not None and value > 0 else None


def _leverage(value):
    return int(value) if _positive(value) is not None and value == value.to_integral_value() else None


def _redact(value):
    if isinstance(value, dict):
        return {
            key: "[redacted]" if key.lower().replace("-", "").replace("_", "") in PRIVATE_FIELDS else _redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def _invalid_constant(value):
    raise ValueError(f"invalid JSON constant {value}")


def _response_json(raw):
    if raw is None:
        return None
    try:
        return json.loads(raw, parse_constant=_invalid_constant)
    except ValueError:
        return None


@dataclass
class _Sample:
    endpoint: str
    data: object = None
    observed_at: Optional[str] = None
    cache_age: Optional[str] = None
    failed: bool = False

    def source(self, path):
        return {
            "endpoint": self.endpoint, "path": path, "observedAt": self.observed_at,
            "httpAge": self.cache_age, "calculated": False,
        }

    def snapshot_time(self):
        if self.observed_at is None:
            return None
        try:
            observed = datetime.fromisoformat(self.observed_at)
            age = int(self.cache_age or 0)
            if observed.tzinfo is not None and age >= 0:
                return observed - timedelta(seconds=age)
        except (OverflowError, ValueError, TypeError):
            return None


class _Query:
    def __init__(self, pid, capture):
        self.pid = portfolio_id(pid)
        self.capture = capture
        self.errors = []
        self.stopped = False

    def read(self, endpoint, body=None):
        sample = _Sample(endpoint)
        response = {}
        raw_response = None
        if self.stopped:
            sample.failed = True
            return sample

        def observe(_kind, *, raw_json, **metadata):
            nonlocal raw_response
            raw_response = raw_json
            response.update(metadata)
            sample.observed_at = metadata["observed_at"]
            sample.cache_age = metadata["cache_age"]
            if self.capture is not None:
                payload = _response_json(raw_json)
                self.capture.append({
                    **metadata, "method": "GET" if body is None else "POST",
                    "request": {"portfolioId": self.pid, **(body or {})},
                    "response": _redact(payload), "json": payload is not None,
                })

        try:
            sample.data, sample.observed_at = fetch(self.pid, endpoint, body, observer=observe)
        except ProbeError as error:
            sample.failed = True
            self.stopped = isinstance(error, AccessStopped) or bool(response.get("retry_after"))
            status = response.get("http_status")
            if status is not None:
                message = str(error)
            else:
                cause = error.__cause__
                reason = type(getattr(cause, "reason", cause)).__name__ if cause else "ProbeError"
                message = f"Binance request failed before receiving an HTTP response ({reason})"
            payload = _response_json(raw_response)
            error_fields = {
                "responseType": type(payload).__name__ if payload is not None else None,
                "responseKeys": sorted(payload) if isinstance(payload, dict) else None,
            }
            self.error(sample, message, status, **error_fields)
        return sample

    def error(self, sample, message, status=200, **details):
        self.errors.append({
            "endpoint": sample.endpoint, "observedAt": sample.observed_at,
            "httpStatus": status, "message": message, **details,
        })

    def result(self, fields):
        return {**fields, "errors": self.errors, "accessStopped": self.stopped}


def _fields(sample, prefix, entries):
    result = {"metadata": {}}
    for name, (raw_name, value) in entries.items():
        result[name] = value
        result["metadata"][name] = sample.source(f"{prefix}.{raw_name}")
    return result


def _add_field(result, name, value, source):
    result[name] = value
    result["metadata"][name] = source


def _calculated(result, name, value, formula, sources):
    _add_field(result, name, value, {
        "calculated": True, "formula": formula, "sources": sources,
    })


def _balance(query, detail):
    data = detail.data
    if data is None:
        # An HTTP or business error from fetch is already recorded.
        if not detail.failed:
            query.error(detail, "detail has no data object")
        data = {}
    elif not isinstance(data, dict) or data.get("leadPortfolioId") != query.pid:
        query.error(detail, "detail does not match portfolioId")
        data = {}

    show = data.get("positionShow")
    fields = _fields(detail, "data", {
        "portfolioId": ("leadPortfolioId", query.pid),
        "marginBalance": ("marginBalance", _number(data.get("marginBalance"))),
        "marginBalanceAsset": ("initInvestAsset", _text(data.get("initInvestAsset"))),
        "positionShow": ("positionShow", show if type(show) is bool else None),
        "portfolioStatus": ("status", _text(data.get("status"))),
        "futuresType": ("futuresType", _text(data.get("futuresType"))),
    })
    if not data:
        fields["metadata"]["portfolioId"] = {
            "endpoint": None, "path": "request.portfolioId", "observedAt": None, "calculated": False,
        }
    return fields


def get_trader_margin_balance(portfolio_id, *, capture=None):
    """Read Leading Margin Balance and position visibility from the public detail response."""
    query = _Query(portfolio_id, capture)
    return query.result(_balance(query, query.read(DETAIL)))


def _current_positions(query, sample):
    if sample.data == []:
        return [], "empty_unconfirmed"
    if sample.failed:
        return None, "unavailable"
    try:
        parsed = parse_positions(sample.data, optional_number=_number)
    except ProbeError:
        query.error(sample, "positions response has malformed rows or position keys")
        return None, "unavailable"

    positions = []
    for index, (row, position) in enumerate(zip(sample.data, parsed.values())):
        if not position["quantity"]:
            continue
        prefix = f"data[{index}]"
        fields = _fields(sample, prefix, {
            "symbol": ("symbol", position["symbol"]),
            "side": ("positionSide", position["direction"]),
            "quantity": ("positionAmount", _number(row.get("positionAmount"))),
            "entryPrice": ("entryPrice", _positive(position["entry_price"])),
            "markPrice": ("markPrice", _positive(position["mark_price"])),
            "configuredLeverage": ("leverage", _leverage(position["configured_leverage"])),
            "marginMode": ("isolated", position["margin_mode"]),
            "collateral": ("collateral", _text(row.get("collateral"))),
        })
        if position["leg"] == "BOTH":
            _calculated(fields, "side", position["direction"], "side from the sign of quantity", [
                fields["metadata"]["side"], fields["metadata"]["quantity"],
            ])
        positions.append(fields)
    return positions, "visible"


def _history(query, page, page_size):
    sample = query.read(HISTORY, {"pageNumber": page, "pageSize": page_size, "sort": "CLOSING"})
    if sample.failed:
        return None
    try:
        rows = history_rows(sample.data)
    except ProbeError as error:
        query.error(sample, str(error))
        return None

    positions = []
    for index, row in enumerate(rows):
        mode = _text(row.get("isolated"))
        mode = mode.upper() if mode is not None else None
        positions.append(_fields(sample, f"data.list[{index}]", {
            "positionId": ("positionId", str(row["positionId"]) if type(row.get("positionId")) is int
                           else _text(row.get("positionId"))),
            "symbol": ("symbol", _text(row.get("symbol"))),
            "side": ("side", _text(row.get("side"))),
            "status": ("status", _text(row.get("status"))),
            "averageCost": ("avgCost", _number(row.get("avgCost"))),
            "maxOpenInterest": ("maxOpenInterest", _number(row.get("maxOpenInterest"))),
            "configuredLeverage": ("leverage", _leverage(_number(row.get("leverage")))),
            "marginMode": ("isolated", mode if mode in ("CROSS", "ISOLATED") else None),
            "openedAtMs": ("opened", row.get("opened")),
            "closedAtMs": ("closed", row.get("closed")),
            "recordUpdatedAtMs": ("updateTime", row.get("updateTime")),
        }))
    history = _fields(sample, "data", {"total": ("total", sample.data.get("total"))})
    _add_field(history, "positions", positions, sample.source("data.list"))
    return {**history, "page": page}


def _is_fresh(snapshot_time, now):
    return snapshot_time is not None and timedelta(0) <= now - snapshot_time <= MAX_AGE


def _metrics(snapshot, positions, detail, current, state):
    balance_time, position_time = detail.snapshot_time(), current.snapshot_time()
    now = datetime.now(timezone.utc)
    positions_fresh = _is_fresh(position_time, now)
    balance_fresh = _is_fresh(balance_time, now)
    matched = (
        positions_fresh and balance_fresh and abs(position_time - balance_time) <= MAX_SKEW
    )
    usdt_linear = snapshot["futuresType"] == "UM" and snapshot["marginBalanceAsset"] == "USDT"
    balance = _positive(snapshot["marginBalance"]) if matched and usdt_linear else None
    margin_source = snapshot["metadata"]["marginBalance"]
    currency_sources = [snapshot["metadata"][name] for name in ("marginBalanceAsset", "futuresType")]
    notionals, sources = [], []
    for position in positions or []:
        metadata = position["metadata"]
        mark = position["markPrice"]
        leverage = position["configuredLeverage"]
        notional = None
        if positions_fresh and usdt_linear and position["collateral"] == "USDT" and mark is not None:
            notional = abs(position["quantity"] * mark)
        _calculated(position, "positionNotional", notional, "abs(quantity * markPrice)", [
            metadata["quantity"], metadata["markPrice"], metadata["collateral"], *currency_sources,
        ])
        _calculated(position, "estimatedInitialMargin",
                    notional / leverage if notional is not None and leverage is not None else None,
                    "positionNotional / configuredLeverage", [
                        metadata["positionNotional"], metadata["configuredLeverage"],
                    ])
        _calculated(position, "positionExposureRatio",
                    notional / balance if notional is not None and balance is not None else None,
                    "positionNotional / marginBalance", [metadata["positionNotional"], margin_source])
        notionals.append(notional)
        sources.append(metadata["positionNotional"])

    total = None
    if state == "visible" and positions_fresh and usdt_linear and None not in notionals:
        total = sum(notionals, Decimal(0))
    _calculated(snapshot, "totalAbsolutePositionNotional", total, "sum(positionNotional)", [
        current.source("data"), *currency_sources, *sources,
    ])
    _calculated(snapshot, "effectiveLeverage",
                total / balance if total is not None and balance is not None else None,
                "totalAbsolutePositionNotional / marginBalance",
                [snapshot["metadata"]["totalAbsolutePositionNotional"], margin_source])
    snapshot["freshness"] = {
        "basis": "response observation time minus HTTP Age when supplied; exchange snapshot times unavailable",
        "evaluatedAt": now.isoformat(timespec="milliseconds"),
        "maxAgeSeconds": int(MAX_AGE.total_seconds()), "maxSkewSeconds": int(MAX_SKEW.total_seconds()),
        "balanceFresh": balance_fresh, "positionsFresh": positions_fresh, "withinSkew": matched,
    }


def get_trader_position_leverage(portfolio_id, *, history_page=None, history_page_size=50, capture=None):
    """Read the public balance and current leverage, optionally including one history page."""
    if history_page is not None and (history_page < 1 or not 1 <= history_page_size <= 50):
        raise ValueError("history page must be positive and page size must be in 1..50")
    query = _Query(portfolio_id, capture)
    detail = query.read(DETAIL)
    snapshot = _balance(query, detail)
    current = _Sample(POSITIONS)
    positions, state = None, "unavailable"
    if snapshot["positionShow"] is False:
        state = "hidden"
    elif snapshot["positionShow"] is True and snapshot["portfolioStatus"] == "ACTIVE":
        current = query.read(POSITIONS)
        positions, state = _current_positions(query, current)
    _add_field(snapshot, "positions", positions, current.source("data"))
    _calculated(snapshot, "positionVisibility", state, "derive visibility from the portfolio and position response", [
        snapshot["metadata"]["positionShow"], snapshot["metadata"]["portfolioStatus"], current.source("data"),
    ])
    _metrics(snapshot, positions, detail, current, state)
    if history_page is not None:
        snapshot["positionHistory"] = _history(query, history_page, history_page_size)
    return query.result(snapshot)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("portfolio_id", type=portfolio_id)
    parser.add_argument("--history-page", type=int, help="also read this page of historical positions")
    parser.add_argument("--history-page-size", type=int, default=50, choices=range(1, 51), metavar="1..50")
    parser.add_argument("--raw", type=Path, help="save sanitized Binance responses and HTTP metadata as JSON")
    args = parser.parse_args()
    if args.history_page is not None and args.history_page < 1:
        parser.error("--history-page must be positive")
    capture = [] if args.raw is not None else None
    result = get_trader_position_leverage(
        args.portfolio_id, history_page=args.history_page, history_page_size=args.history_page_size,
        capture=capture,
    )
    if args.raw is not None:
        args.raw.write_text(json.dumps(capture, indent=2, ensure_ascii=True, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=True, default=str))
    return 2 if result["accessStopped"] else int(bool(result["errors"]))


if __name__ == "__main__":
    raise SystemExit(main())
