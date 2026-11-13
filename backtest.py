"""Replay public Binance lead orders using historical USD-M minute prices."""

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import groupby
import json
from pathlib import Path

from backtest_market import MINUTE, fetch_market, utc_day, utc_ms
from probe import parse_order


DEFAULT_LEVERAGE = 3
EPSILON = 1e-9
FEE = 0.0005
MIN_ORDER = 10
MAINTENANCE_PROXY = 0.01


@dataclass
class Position:
    direction: int
    quantity: float
    entry: float


def report_time(minute):
    return datetime.fromtimestamp(minute * 60, timezone.utc).isoformat()


def get_signals(raw, start, stop):
    orders = sorted((parse_order(row) for row in raw), key=lambda row: row["order_update_time_ms"])
    signals = []
    for row in orders:
        if not start <= row["order_update_time_ms"] < stop:
            continue
        if row["leg"] in ("LONG", "SHORT") and row["quantity"] > 0:
            signals.append(row)
    return signals


def market_minute(order, delay):
    return (order["order_update_time_ms"] + MINUTE - 1) // MINUTE + delay


def simulate(signals, market, start, stop, *, initial=10_000, base_margin=0.05,
             symbol_margin=0.15, total_margin=0.40, delay=5, slip=0.0005,
             missing_funding_debit=0, leverage=DEFAULT_LEVERAGE):
    trades, marks, funding, funding_months, _ = market
    first = max(start // MINUTE, market_minute(signals[0], 0))
    last = stop // MINUTE - 1
    positions = {}
    leader = defaultdict(float)
    scale = {}
    pending_reduction = defaultdict(float)
    last_marks = {}
    counts = Counter()
    costs = Counter()
    net_by_symbol = Counter()
    missing_funding = Counter()
    equity_curve = []
    month_end = {}
    fills = []
    cash, peak, max_drawdown = initial, initial, 0.0
    max_gross, min_buffer = 0.0, initial
    max_gross_at, drawdown_at = first, first
    earliest_peak, worst_peak = first, first

    def account():
        upnl = sum(pos.direction * pos.quantity * (last_marks[key[0]] - pos.entry)
                   for key, pos in positions.items())
        gross = sum(pos.quantity * last_marks[key[0]] for key, pos in positions.items())
        return cash + upnl, gross

    def execute(minute, key, delta, price, reason, source=None):
        nonlocal cash
        symbol, leg = key
        direction = 1 if leg == "LONG" else -1
        side = direction if delta > 0 else -direction
        fill = price * (1 + side * slip)
        fee = abs(delta) * fill * FEE
        counts[reason] += 1
        costs["fees"] += fee
        costs["slippage"] += abs(delta) * abs(fill - price)
        costs["turnover"] += abs(delta) * fill
        net_by_symbol[symbol] -= fee
        cash -= fee
        current = positions.get(key)
        realized = 0.0
        if delta > 0:
            old_quantity = current.quantity if current else 0
            old_value = current.entry * old_quantity if current else 0
            positions[key] = Position(direction, old_quantity + delta, (old_value + delta * fill) / (old_quantity + delta))
        else:
            realized = direction * (-delta) * (fill - current.entry)
            cash += realized
            net_by_symbol[symbol] += realized
            costs["realized"] += realized
            current.quantity += delta
            if current.quantity < EPSILON:
                del positions[key]
                counts["closed_legs"] += 1
        fills.append({
            "ts": report_time(minute), "symbol": symbol, "leg": leg, "side": "BUY" if side > 0 else "SELL",
            "quantity": round(abs(delta), 10), "price": round(fill, 10), "fee": round(fee, 8),
            "reason": reason, "realized": round(realized, 8), "signal_ms": source,
        })

    by_minute = {
        minute: list(rows)
        for minute, rows in groupby(signals, key=lambda order: market_minute(order, delay))
    }
    counts["signals_beyond_end"] = sum(len(rows) for t, rows in by_minute.items() if t > last)
    for minute in range(first, last + 1):
        batch = by_minute.get(minute, ())
        symbols = {key[0] for key in positions} | {row["symbol"] for row in batch}
        month = utc_day(minute * MINUTE).strftime("%Y-%m")
        for symbol in symbols:
            mark = marks[symbol].get(minute)
            if mark:
                last_marks[symbol] = mark[0]
            elif any(key[0] == symbol for key in positions):
                raise ValueError(f"Missing mark for held {symbol} at {report_time(minute)}")
        for key in positions:
            if (key[0], month) not in funding_months:
                missing_funding[key[0]] += 1
                if missing_funding_debit and minute % (8 * 60) == 0:
                    debit = positions[key].quantity * last_marks[key[0]] * missing_funding_debit
                    cash -= debit
                    costs["funding_stress"] -= debit
                    net_by_symbol[key[0]] -= debit
        for symbol, rate in funding.get(minute, ()):
            for key, pos in positions.items():
                if key[0] == symbol:
                    payment = -pos.direction * pos.quantity * last_marks[symbol] * rate
                    cash += payment
                    costs["funding"] += payment
                    net_by_symbol[symbol] += payment
                    counts["funding_events"] += 1
        for order in batch:
            symbol, leg = order["symbol"], order["leg"]
            key = (symbol, leg)
            quantity = float(order["quantity"])
            increase = order["side"] == ("BUY" if leg == "LONG" else "SELL")
            old = leader[key]
            candle = trades[symbol].get(minute)
            price = candle[0] if candle else None
            pos = positions.get(key)
            if increase:
                leader[key] += quantity
                if price is None or minute not in marks[symbol]:
                    counts["missing_signal_price"] += 1
                    continue
                equity, gross = account()
                leg_mark = last_marks[symbol]
                symbol_gross = sum(p.quantity * leg_mark for k, p in positions.items() if k[0] == symbol)
                if key not in scale:
                    scale[key] = max(equity, 0) * base_margin * leverage / (price * quantity)
                room = min(equity * symbol_margin * leverage - symbol_gross,
                           equity * total_margin * leverage - gross)
                proposed = quantity * scale[key]
                cancelled = min(proposed, pending_reduction[key])
                pending_reduction[key] -= cancelled
                proposed -= cancelled
                risk_price = max(leg_mark, price * (1 + slip))
                room = max(0, room - max(equity, 0) * 0.001)
                size = min(proposed, room / risk_price)
                if size * price < MIN_ORDER:
                    counts["opens_below_minimum_or_capped"] += 1
                    continue
                if size < proposed - EPSILON:
                    counts["clipped_opens"] += 1
                execute(minute, key, size, price, "add" if pos else "open", order["order_update_time_ms"])
            else:
                if old + EPSILON < quantity:
                    counts["reductions_exceed_known_leader_size"] += 1
                leader[key] = max(0.0, old - quantity)
                if leader[key] < EPSILON:
                    leader[key] = 0.0
                    scale.pop(key, None)
                if pos is None:
                    counts["no_owned_position"] += 1
                    continue
                if price is None:
                    raise ValueError(f"Missing trade price for exit {symbol} at {report_time(minute)}")
                fraction = min(1.0, quantity / old) if leader[key] else 1.0
                remaining_target = max(0, pos.quantity - pending_reduction[key])
                size = pos.quantity - remaining_target * (1 - fraction)
                if fraction < 1.0 and size * price < MIN_ORDER:
                    pending_reduction[key] = size
                    counts["reductions_below_minimum"] += 1
                    continue
                pending_reduction.pop(key, None)
                execute(minute, key, -size, price, "close" if fraction == 1.0 else "reduce",
                        order["order_update_time_ms"])

        equity, gross = account()
        if equity > 0 and gross / equity > max_gross:
            max_gross, max_gross_at = gross / equity, minute
        adverse_pnl, adverse_gross = 0.0, 0.0
        for key, pos in positions.items():
            mark = marks[key[0]][minute]
            adverse_price = mark[2] if pos.direction > 0 else mark[1]
            adverse_pnl += pos.direction * pos.quantity * (adverse_price - pos.entry)
            adverse_gross += pos.quantity * max(mark[1], adverse_price)
            last_marks[key[0]] = mark[3]
        buffer = cash + adverse_pnl - adverse_gross * MAINTENANCE_PROXY
        min_buffer = min(min_buffer, buffer)
        if buffer <= 0:
            raise ValueError(f"Maintenance margin proxy breached at {report_time(minute)}")

        if minute == last:
            counts["open_legs_before_end"] = len(positions)
            for key, pos in list(positions.items()):
                execute(minute, key, -pos.quantity, trades[key[0]][minute][1], "end_close")
        equity, gross = account()
        if equity > 0 and gross / equity > max_gross:
            max_gross, max_gross_at = gross / equity, minute
        if equity > peak:
            peak, earliest_peak = equity, minute
        drawdown = (peak - equity) / peak
        if drawdown > max_drawdown:
            max_drawdown, drawdown_at, worst_peak = drawdown, minute, earliest_peak
        if minute % 60 == 59 or minute == last:
            equity_curve.append([report_time(minute), round(equity, 6)])
        month_end[month] = equity

    assert not positions
    assert abs(sum(net_by_symbol.values()) - (cash - initial)) < 1e-5
    monthly = {}
    previous = initial
    for month, balance in month_end.items():
        monthly[month] = balance / previous - 1
        previous = balance
    return {
        "initial": initial, "final": cash, "return": cash / initial - 1, "max_drawdown": max_drawdown,
        "leverage": leverage,
        "drawdown_at": report_time(drawdown_at), "drawdown_peak_at": report_time(worst_peak),
        "max_gross_to_equity": max_gross, "max_gross_at": report_time(max_gross_at),
        "min_maintenance_buffer_proxy": min_buffer, "delay_minutes": delay, "base_margin": base_margin,
        "symbol_margin": symbol_margin, "total_margin": total_margin, "slippage_rate": slip,
        "missing_funding_debit": missing_funding_debit,
        "first_minute": report_time(first), "last_minute": report_time(last),
        "counts": dict(counts), "costs": dict(costs), "monthly": monthly, "net_by_symbol": dict(net_by_symbol),
        "funding_missing_leg_minutes": dict(missing_funding), "curve": equity_curve, "fills": fills,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=Path("backtest_orders.json"))
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("backtest_results.json"))
    parser.add_argument("--leverage", type=int, choices=(3, 10), default=DEFAULT_LEVERAGE)
    args = parser.parse_args()
    data = json.loads(args.data.read_text(encoding="utf-8"))
    start, stop = map(utc_ms, (data["window_start_utc"], data["window_end_exclusive_utc"]))
    orders = get_signals(data["orders"], start, stop)
    if not orders:
        parser.error("no eligible order signals in the input window")
    first_by_symbol = {}
    for row in orders:
        symbol = row["symbol"]
        first_by_symbol.setdefault(symbol, utc_day(row["order_update_time_ms"]))
    market = fetch_market(first_by_symbol, utc_day(stop), args.cache, progress=True)
    scenarios = {
        "base": {}, "delay_1m": {"delay": 1}, "delay_30m": {"delay": 30}, "delay_60m": {"delay": 60},
        "small": {"base_margin": 0.025, "symbol_margin": 0.075, "total_margin": 0.20},
        "large": {"base_margin": 0.10, "symbol_margin": 0.30, "total_margin": 0.60},
        "slip_20bps": {"slip": 0.002},
        "delay_4h": {"delay": 240},
        "combined_stress": {"delay": 60, "slip": 0.002, "missing_funding_debit": 0.0005},
        "through_last_full_month": {},
    }
    if args.leverage != DEFAULT_LEVERAGE:
        ratio = DEFAULT_LEVERAGE / args.leverage
        scenarios["same_notional_as_3x"] = {
            "base_margin": 0.05 * ratio, "symbol_margin": 0.15 * ratio, "total_margin": 0.40 * ratio,
        }
    results = {}
    for name, config in scenarios.items():
        cutoff = stop
        if name == "through_last_full_month":
            cutoff = utc_ms(utc_day(stop).replace(day=1))
            if not start < cutoff < stop:
                continue
        scenario_orders = orders if cutoff == stop else [
            row for row in orders if row["order_update_time_ms"] < cutoff
        ]
        if not scenario_orders:
            continue
        results[name] = simulate(scenario_orders, market, start, cutoff, leverage=args.leverage, **config)
        print(name, round(results[name]["return"] * 100, 3), round(results[name]["max_drawdown"] * 100, 3), flush=True)
    args.output.write_text(
        json.dumps({"source_orders": len(orders), "market": market[-1], "scenarios": results},
                   ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
