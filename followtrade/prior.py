"""Build a descriptive sizing prior from public Binance evidence; no order submission."""

import argparse
from collections import Counter
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import sqlite3

from probe import ProbeError, parse_order, portfolio_id
from .clients import BinanceFeed, order_signal
from .store import now_ms
from .trader import DETAIL, HISTORY, _number, _Query, _redact, _response_json, get_trader_position_leverage


DAY_MS = 86_400_000
PRIOR_MAX_AGE_MS = 7 * DAY_MS
KEY_PREFIX = "trader.sizing_prior."


def distribution(values):
    """Linear-interpolated quantiles rounded to 8 decimals; ratios are fractions."""
    values = sorted(values)
    if not values:
        return {"count": 0, "p10": None, "median": None, "p90": None, "max": None}

    def quantile(fraction):
        index = Decimal(len(values) - 1) * Decimal(fraction)
        left = int(index)
        right = min(left + 1, len(values) - 1)
        value = values[left] + (values[right] - values[left]) * (index - left)
        return format(value, ".8f").rstrip("0").rstrip(".") or "0"

    return {"count": len(values), "p10": quantile(".1"), "median": quantile(".5"),
            "p90": quantile(".9"), "max": quantile("1")}


def _history_summary(rows):
    leverage = [_number(row.get("leverage")) for row in rows]
    leverage = [value for value in leverage if value is not None and 1 <= value <= 125
                and value == value.to_integral_value()]
    cost_notionals = []
    for row in rows:
        price, quantity = _number(row.get("avgCost")), _number(row.get("maxOpenInterest"))
        if (row.get("type") == "UM" and str(row.get("symbol", "")).endswith("USDT")
                and price is not None and price > 0 and quantity is not None and quantity > 0):
            cost_notionals.append(price * quantity)
    return {"closedCycles": len(rows), "reportedLeverage": distribution(leverage),
            "leverageCounts": dict(sorted(Counter(str(value) for value in leverage).items(),
                                          key=lambda item: Decimal(item[0]))),
            "marginModeCounts": dict(Counter(str(row.get("isolated", "unknown")).upper() for row in rows)),
            "cyclePeakCostNotionalProxyUsdt": distribution(cost_notionals)}


def _capital_summary(rows, capital):
    exposure, margin = [], []
    for row in rows:
        price, quantity = _number(row.get("avgCost")), _number(row.get("maxOpenInterest"))
        if (row.get("type") != "UM" or not str(row.get("symbol", "")).endswith("USDT")
                or price is None or price <= 0 or quantity is None or quantity <= 0):
            continue
        fraction = price * quantity / capital
        exposure.append(fraction)
        leverage = _number(row.get("leverage"))
        if leverage is not None and 1 <= leverage <= 125 and leverage == leverage.to_integral_value():
            margin.append(fraction / leverage)
    return {"cyclePeakCostExposureFraction": distribution(exposure),
            "estimatedInitialMarginFraction": distribution(margin)}


def _replay_cycles(rows, orders, window):
    """Use only complete, unambiguous cycles whose quantities reconcile with history."""
    matched = {str(row["positionId"]): [] for row in rows}
    for order in orders:
        candidates = [row for row in rows if row["symbol"] == order["symbol"]
                      and str(row["side"]).upper() == order["leg"]
                      and row["opened"] - 1000 <= order["order_time_ms"]
                      and order["order_update_time_ms"] <= row["closed"] + 1000]
        if len(candidates) == 1:
            matched[str(candidates[0]["positionId"])].append(order)
    results = []
    for row in rows:
        if not window["startTime"] <= row["opened"] <= row["closed"] <= window["endTime"]:
            continue
        cycle = sorted(matched[str(row["positionId"])], key=lambda order: (
            order["order_update_time_ms"], order["order_time_ms"]))
        expected = _number(row.get("maxOpenInterest"))
        if not cycle or expected is None or expected <= 0:
            continue
        tolerance = max(Decimal(".00000001"), expected * Decimal(".000001"))
        balance, peak = Decimal(0), Decimal(0)
        increases = []
        increase_notionals = []
        valid = cycle[0]["side_hint"] and cycle[0]["side_hint"].endswith("_INCREASE")
        valid = valid and abs(cycle[0]["order_time_ms"] - row["opened"]) <= 1000
        for order in cycle:
            if not order["side_hint"]:
                valid = False
                break
            if order["side_hint"].endswith("_INCREASE"):
                increases.append(order["quantity"])
                increase_notionals.append(order["quantity"] * order["avg_price"])
                balance += order["quantity"]
                peak = max(peak, balance)
            else:
                balance -= order["quantity"]
                if balance < -tolerance:
                    valid = False
                    break
        if not valid or not increases or abs(balance) > tolerance or abs(peak - expected) > tolerance:
            continue
        results.append({"positionId": str(row["positionId"]), "symbol": row["symbol"],
                        "initialQuantityFractionOfCyclePeak": increases[0] / peak,
                        "additionQuantityFractionsOfCyclePeak": [qty / peak for qty in increases[1:]],
                        "initialOrderNotionalUsdt": increase_notionals[0],
                        "additionOrderNotionalsUsdt": increase_notionals[1:],
                        "increaseOrders": len(increases)})
    return results


def analyze_evidence(evidence, *, assumed_capital_usdt=None):
    capital = _number(assumed_capital_usdt)
    if assumed_capital_usdt is not None and (capital is None or capital <= 0):
        raise ValueError("assumed capital must be a finite positive USDT amount")
    snapshot = evidence["snapshot"]
    pid = portfolio_id(snapshot["portfolioId"])
    responses = [item for item in evidence["rawResponses"]
                 if item.get("http_status") == 200 and isinstance(item.get("response"), dict)
                 and item["response"].get("success") is True and item["response"].get("code") == "000000"
                 and item.get("request", {}).get("portfolioId") == pid]
    history, orders = {}, {}
    total = None
    observed = []
    detail = {}
    for item in responses:
        data = item["response"].get("data")
        observed.append(item["observed_at"])
        if item["endpoint"] == DETAIL and isinstance(data, dict) and data.get("leadPortfolioId") == pid:
            detail = data
        if not isinstance(data, dict) or not isinstance(data.get("list"), list):
            continue
        if item["endpoint"] == HISTORY:
            total = data.get("total")
            for row in data["list"]:
                if row.get("positionId") is not None:
                    history[str(row["positionId"])] = row
        elif item["endpoint"] == "lead-portfolio/order-history":
            for row in data["list"]:
                try:
                    signal = order_signal(row)
                    order = parse_order(row)
                except (ProbeError, TypeError, KeyError):
                    continue
                if signal is not None and (signal.identity not in orders or
                        order["order_update_time_ms"] > orders[signal.identity]["order_update_time_ms"]):
                    orders[signal.identity] = order
    collected = max(observed) if observed else snapshot["metadata"]["marginBalance"]["observedAt"]
    collected_ms = int(datetime.fromisoformat(collected).timestamp() * 1000)
    closed = [row for row in history.values() if row.get("status") == "All Closed"
              and isinstance(row.get("symbol"), str) and str(row.get("side")).upper() in ("LONG", "SHORT")
              and type(row.get("opened")) is int and type(row.get("closed")) is int
              and 0 < row["opened"] <= row["closed"] <= collected_ms]
    recent = [row for row in closed if row["closed"] >= collected_ms - 30 * DAY_MS]
    window = evidence.get("orderCollection", {})
    complete_orders = window.get("complete") is True
    if type(window.get("startTime")) is int and type(window.get("endTime")) is int:
        orders = {key: order for key, order in orders.items()
                  if window["startTime"] <= order["order_update_time_ms"] <= window["endTime"]}
    replayed = _replay_cycles(closed, list(orders.values()), window) if complete_orders else []
    initial = [cycle["initialQuantityFractionOfCyclePeak"] for cycle in replayed]
    additions = [fraction for cycle in replayed for fraction in cycle["additionQuantityFractionsOfCyclePeak"]]
    prior = {
        "schemaVersion": 1, "portfolioId": pid, "traderName": detail.get("nickname"),
        "collectedAtMs": collected_ms, "collectedAt": collected,
        "snapshotAtCollection": {"marginBalanceUsdt": str(snapshot["marginBalance"]) if
            snapshot.get("marginBalanceAsset") == "USDT" and snapshot.get("marginBalance") is not None else None,
            "positionVisibility": snapshot.get("positionVisibility"),
            "positionExposureRatios": [position.get("positionExposureRatio") for position in snapshot["positions"]]
                if snapshot.get("positions") is not None else None,
            "accountGrossExposureFraction": snapshot.get("effectiveLeverage"),
            "observedAt": snapshot["metadata"]["marginBalance"]["observedAt"], "isLive": False},
        "historicalAccountExposureFractions": None,
        "history": {"rows": len(history), "totalReported": total,
                    "complete": type(total) is int and len(history) == total,
                    "firstOpenedAtMs": min((row["opened"] for row in closed), default=None),
                    "lastClosedAtMs": max((row["closed"] for row in closed), default=None),
                    "all": _history_summary(closed), "recent30Days": _history_summary(recent)},
        "bySymbolRecent30Days": {symbol: _history_summary([row for row in recent if row["symbol"] == symbol])
                                 for symbol in sorted({row["symbol"] for row in recent})},
        "executionStyle": {"orderWindow": window, "uniqueOrders": len(orders),
            "reconciledClosedCycles": len(replayed),
            "cyclesWithAdditions": sum(cycle["increaseOrders"] > 1 for cycle in replayed),
            "initialQuantityFractionOfCyclePeak": distribution(initial),
            "additionQuantityFractionOfCyclePeak": distribution(additions),
            "increaseOrdersPerCycle": distribution([Decimal(cycle["increaseOrders"]) for cycle in replayed])},
        "sources": {"baseUrl": "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/",
            "balance": "GET lead-portfolio/detail: data.marginBalance (USDT)",
            "historicalLeverage": "POST lead-portfolio/position-history: data.list[].leverage",
            "peakCostNotionalProxy": "data.list[].avgCost * data.list[].maxOpenInterest; cost-based proxy, not peak mark notional",
            "orderSizing": "POST lead-portfolio/order-history: executedQty, side, positionSide, orderTime, orderUpdateTime"},
        "limitations": [
            "历史账户仓位比例未知：缺少与历史仓位同期的账户权益，不能用本次余额或 AUM 代替。",
            "首仓/加仓比例以该轮峰值持仓数量为分母，不是账户权益比例，也不是模型建议仓位。",
            "历史杠杆是查询时的仓位历史字段，不能视为开仓瞬间或当前未平仓合约的设定杠杆。",
            "峰值数量乘平均成本只是成本名义金额估算，不能把多轮峰值相加当作同时持仓。",
            "加仓指同轮内后续增加数量的订单；多个订单可能属于同一交易意图。",
            "样本仅覆盖公开可见历史；缺失、不完整或无法对齐的交易轮次不参与首仓/加仓比例统计。",
        ],
    }
    if capital is not None:
        # These are paired, per-order calculations, not products of independent medians.
        usdt_ids = {str(row["positionId"]) for row in closed if row.get("type") == "UM"
                    and row["symbol"].endswith("USDT")}
        usdt_cycles = [cycle for cycle in replayed if cycle["positionId"] in usdt_ids]
        prior["assumedCapitalScenario"] = {
            "capitalUsdt": str(capital), "source": "user_estimate", "verifiedByBinance": False,
            "isHypothetical": True,
            "denominator": "Assume this amount is equity backing the lead portfolio; external assets may not back it.",
            "allClosedCycles": _capital_summary(closed, capital),
            "recent30Days": _capital_summary(recent, capital),
            "bySymbolRecent30Days": {symbol: _capital_summary([row for row in recent if row["symbol"] == symbol], capital)
                                     for symbol in sorted({row["symbol"] for row in recent})},
            "reconciledOrderSizing": {
                "cycles": len(usdt_cycles),
                "initialOrderNotionalFraction": distribution([cycle["initialOrderNotionalUsdt"] / capital
                                                               for cycle in usdt_cycles]),
                "additionOrderNotionalFraction": distribution([notional / capital for cycle in usdt_cycles
                                                                for notional in cycle["additionOrderNotionalsUsdt"]]),
            },
            "accountGrossExposureFraction": None,
            "formulas": {
                "cyclePeakCostExposureFraction": "avgCost * maxOpenInterest / assumedCapitalUsdt",
                "estimatedInitialMarginFraction": "avgCost * maxOpenInterest / reportedHistoricalLeverage / assumedCapitalUsdt",
                "orderNotionalFraction": "executedQty * avgPrice / assumedCapitalUsdt",
            },
            "limitations": [
                "资金基数是用户估计，不是币安返回的同期带单账户权益；不能用作当前余额或历史实测仓位比例。",
                "逐轮按固定资金基数估算，不含资金进出、历史权益变化、费用和资金费。",
                "估算初始保证金不等于全仓账户实际分配保证金；历史杠杆也不保证是开仓当时的设置。",
                "每轮峰值名义金额按平均成本近似；多个标的的峰值不能相加当作同时账户敞口。",
            ],
        }
    return prior


def collect_evidence(pid, *, history_pages=10, order_days=7):
    capture = []
    snapshot = get_trader_position_leverage(pid, history_page=1, capture=capture)
    query = _Query(pid, capture)
    query.stopped = snapshot["accessStopped"]
    total = (snapshot.get("positionHistory") or {}).get("total")
    if type(total) is int:
        for page in range(2, min(history_pages, (total + 49) // 50) + 1):
            if query.read(HISTORY, {"pageNumber": page, "pageSize": 50, "sort": "CLOSING"}).failed:
                break
    end = now_ms()
    window = {"startTime": end - order_days * DAY_MS, "endTime": end, "complete": False}

    class RecordedFeed(BinanceFeed):
        def _fetch(self, route, body):
            def observe(_kind, *, raw_json, **metadata):
                payload = _response_json(raw_json)
                capture.append({**metadata, "method": "POST", "request": {"portfolioId": pid, **(body or {})},
                                "response": _redact(payload), "json": payload is not None})
            self.observer = observe
            return super()._fetch(route, body)

    if not query.stopped:
        try:
            signals = RecordedFeed(pid).orders(window["startTime"], end)
            window.update(complete=True, signals=len(signals))
        except ProbeError as error:
            window["error"] = str(error)
    return {"snapshot": snapshot, "rawResponses": capture, "orderCollection": window,
            "additionalErrors": query.errors}


def save_prior(database, prior):
    """Write only the portfolio-scoped prior into an existing project database."""
    if not database.is_file():
        raise ValueError("existing project database required")
    with sqlite3.connect(database) as db:
        db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)",
                   (KEY_PREFIX + portfolio_id(prior["portfolioId"]), json.dumps(prior, ensure_ascii=False, default=str)))


def prior_context(store, pid, at_ms, *, symbol=None):
    raw = store.get(KEY_PREFIX + pid)
    unavailable = {"available": False, "portfolioId": pid}
    try:
        if raw is None or len(raw) > 100_000:
            return {"lead_trader_sizing_prior": {**unavailable, "reason": "not_collected"}}
        prior = json.loads(raw)
        if not isinstance(prior, dict) or prior.get("schemaVersion") != 1 or prior.get("portfolioId") != pid:
            raise ValueError
        if type(prior.get("collectedAtMs")) is not int:
            raise ValueError
        age = at_ms - prior["collectedAtMs"]
        if not -5000 <= age <= PRIOR_MAX_AGE_MS:
            return {"lead_trader_sizing_prior": {**unavailable, "reason": "expired"}}
        prior = dict(prior)
        by_symbol = prior.pop("bySymbolRecent30Days", {})
        if not isinstance(by_symbol, dict):
            raise ValueError
        prior["signalSymbolRecent30Days"] = by_symbol.get(symbol)
        scenario = prior.get("assumedCapitalScenario")
        if scenario is not None:
            if not isinstance(scenario, dict):
                raise ValueError
            scenario = dict(scenario)
            by_symbol = scenario.pop("bySymbolRecent30Days", {})
            if not isinstance(by_symbol, dict):
                raise ValueError
            scenario["signalSymbolRecent30Days"] = by_symbol.get(symbol)
            prior["assumedCapitalScenario"] = scenario
        return {"lead_trader_sizing_prior": {**prior, "available": True, "ageSeconds": max(0, age // 1000)}}
    except (ValueError, KeyError, TypeError):
        return {"lead_trader_sizing_prior": {**unavailable, "reason": "invalid_cache"}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("portfolio_id", type=portfolio_id)
    parser.add_argument("--evidence", type=Path, help="analyze saved public responses instead of requesting Binance")
    parser.add_argument("--raw", type=Path, help="save sanitized public responses")
    parser.add_argument("--output", type=Path, help="save the computed prior")
    parser.add_argument("--database", type=Path, help="store the prior in the existing bot database")
    parser.add_argument("--assumed-capital-usdt", help="user-estimated capital for a separate hypothetical sizing scenario")
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_text(encoding="utf-8-sig")) if args.evidence else collect_evidence(args.portfolio_id)
    if evidence["snapshot"]["portfolioId"] != args.portfolio_id:
        parser.error("evidence portfolio does not match request")
    try:
        prior = analyze_evidence(evidence, assumed_capital_usdt=args.assumed_capital_usdt)
    except ValueError as error:
        parser.error(str(error))
    if args.raw:
        args.raw.write_text(json.dumps(evidence, ensure_ascii=True, indent=2, default=str) + "\n", encoding="utf-8")
    if not prior["history"]["all"]["closedCycles"]:
        parser.exit(2, "No usable public closed-position history; prior was not installed.\n")
    if args.output:
        args.output.write_text(json.dumps(prior, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    if args.database:
        save_prior(args.database, prior)
    print(json.dumps(prior, ensure_ascii=True, indent=2, default=str))


if __name__ == "__main__":
    main()
