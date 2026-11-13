from contextlib import closing
from decimal import Decimal, DecimalException, ROUND_UP
import hashlib
import logging
import sqlite3

from .binance import Futures, MODE_LABELS, as_decimal, rounded
from .clients import BinanceAPIError, Model, ServiceError, Signal
from .store import Store, now_ms, utc_day


ZERO = Decimal(0)
ONE = Decimal(1)
FINAL = {"FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"}


def mode_status(store, accounts):
    selected = store.get("execution.mode", "paper")
    lines = [f"新开仓去向：{MODE_LABELS.get(selected, selected)}"]
    for account in accounts:
        count = store.db.execute("SELECT COUNT(*) FROM exchange_positions WHERE mode=?", (account.mode,)).fetchone()[0]
        state = store.get(f"execution.health.{account.mode}", "等待检查")
        funds = "按子账户 USDT 权益" if account.budget is None else f"预算 {account.budget:f} USDT"
        lines.append(f"{MODE_LABELS[account.mode]} · {account.strategy} · {funds} · {count} 个仓位 · {state}")
    return "\n".join(lines)


def exchange_positions(store):
    rows = store.db.execute("SELECT * FROM exchange_positions ORDER BY mode,symbol").fetchall()
    return [f"[{MODE_LABELS[row['mode']]}] {row['symbol']} {row['leg']} · 数量 {row['quantity']}" for row in rows]


def format_exchange_order(row):
    return (f"[{MODE_LABELS[row['mode']]}] {row['symbol']} {row['side']} · {row['status']}\n"
            f"委托 {row['quantity']} · 成交 {row['executed']} @ {row['price']}\n"
            f"{row['failure'] or row['client_id']}")


def choose_mode(store, accounts, mode):
    if mode != "paper" and not any(account.mode == mode for account in accounts):
        return f"{MODE_LABELS[mode]}需要先在 VPS 录入密钥并验证交易权限。"
    with store.transaction():
        if store.get("execution.mode", "paper") != mode:
            store.put("execution.mode", mode)
            store.put("execution.selected_at", now_ms())
            store.db.execute("UPDATE jobs SET result='execution route changed' WHERE account IN ('demo','mainnet') "
                             "AND result IS NULL AND signal IN (SELECT identity FROM signals WHERE action='increase') "
                             "AND NOT EXISTS (SELECT 1 FROM exchange_orders o WHERE o.mode=jobs.account AND o.signal=jobs.signal)")
    return f"新开仓切换至{MODE_LABELS[mode]}。已提交委托和持仓留在原账户处理。"


class ExchangeAccount:
    def __init__(self, config, account):
        self.config, self.account = config, account
        self.mode = account.mode
        self.client = Futures(self.mode, account.key, account.secret)

    def entry_enabled(self, store, signal):
        cfg = self.config
        now = now_ms()
        last_poll = int(store.get("orders.last_end", 0))
        return (store.get("execution.mode") == self.mode and -5000 <= now - signal.time < cfg.max_age * 1000
                and signal.time >= int(store.get("execution.selected_at", 0)) and not store.get("orders.gap")
                and now - last_poll <= cfg.poll_seconds * 2_000 + 15_000
                and store.get("feed_status", "").startswith("正常"))

    def owned(self, store):
        return {row["symbol"]: row for row in store.db.execute(
            "SELECT * FROM exchange_positions WHERE mode=?", (self.mode,))}

    def _complete(self, store, order, response):
        state = response["status"]
        if state not in FINAL:
            return False
        if (response.get("clientOrderId") != order["client_id"] or response.get("symbol") != order["symbol"]
                or response.get("side") != order["side"]):
            raise ServiceError("Binance order response does not match the submitted order")
        qty, price = as_decimal(response["executedQty"]), as_decimal(response["avgPrice"])
        if (not 0 <= qty <= Decimal(order["quantity"]) or (qty > 0 and price <= 0)
                or (state == "FILLED" and qty != Decimal(order["quantity"]))):
            raise ServiceError("Binance order returned an invalid executed quantity or price")
        with store.transaction():
            result = store.db.execute("UPDATE exchange_orders SET status=?,executed=?,price=?,failure=NULL "
                                      "WHERE client_id=? AND status='CHECKING'", (state, str(qty), str(price), order["client_id"]))
            if not result.rowcount:
                return True
            owned = self.owned(store).get(order["symbol"])
            if qty and order["action"] == "increase":
                old_qty = Decimal(owned["quantity"]) if owned else ZERO
                lead_qty = Decimal(owned["lead_quantity"]) if owned else ZERO
                store.db.execute("INSERT OR REPLACE INTO exchange_positions VALUES (?,?,?,?,?,?,?,?)", (
                    self.mode, order["symbol"], order["leg"], str(old_qty + qty),
                    owned["opened"] if owned else order["signal_time"], order["signal_time"],
                    order["signal"], str(lead_qty + Decimal(order["lead_qty"])),
                ))
            elif qty and owned:
                remaining = Decimal(owned["quantity"]) - qty
                if remaining > 0:
                    lead_qty = Decimal(owned["lead_quantity"])
                    if order["action"] == "decrease":
                        lead_qty = max(ZERO, lead_qty - Decimal(order["lead_qty"]) * qty / Decimal(order["quantity"]))
                    store.db.execute("UPDATE exchange_positions SET quantity=?,lead_quantity=? WHERE mode=? AND symbol=?",
                                     (str(remaining), str(lead_qty), self.mode, order["symbol"]))
                else:
                    store.db.execute("DELETE FROM exchange_positions WHERE mode=? AND symbol=?", (self.mode, order["symbol"]))
            if order["action"] != "close" or not owned or qty == Decimal(owned["quantity"]):
                store.db.execute("UPDATE jobs SET result=? WHERE account=? AND signal=?",
                                 (f"Binance {state} · {qty:f}", self.mode, order["signal"]))
        return True

    def reconcile(self, store):
        orders = store.db.execute(
            "SELECT o.*, s.symbol,s.leg,s.time AS signal_time,s.quantity AS lead_qty "
            "FROM exchange_orders o JOIN signals s ON s.identity=o.signal "
            "WHERE o.mode=? AND o.status='CHECKING' ORDER BY o.time", (self.mode,),
        ).fetchall()
        for order in orders:
            try:
                response = self.client.signed("/fapi/v1/order", symbol=order["symbol"], origClientOrderId=order["client_id"])
            except BinanceAPIError as error:
                if error.code == -2013:
                    raise ServiceError(f"等待核对委托 {order['client_id']}，此账户暂停提交新委托") from None
                raise
            if not self._complete(store, order, response):
                raise ServiceError(f"等待币安确认委托 {order['client_id']}")

    def _submit(self, store, signal, quantity, leverage, price=None):
        action = signal.action
        entry = action == "increase"
        side = "BUY" if (signal.leg == "LONG") == entry else "SELL"
        with store.transaction():
            if entry:
                job = store.db.execute("SELECT result FROM jobs WHERE account=? AND signal=?",
                                       (self.mode, signal.identity)).fetchone()
                if not self.entry_enabled(store, signal) or not job or job[0] is not None:
                    store.done(self.mode, signal, "entry expired or route changed")
                    return
            attempt = store.db.execute("SELECT COUNT(*) FROM exchange_orders WHERE mode=? AND signal=?",
                                       (self.mode, signal.identity)).fetchone()[0]
            identity = hashlib.sha256(f"{self.mode}:{signal.identity}:{action}:{attempt}".encode()).hexdigest()
            client_id = "ft-" + identity[:30]
            store.db.execute("INSERT INTO exchange_orders(client_id,mode,signal,side,quantity,leverage,time,action) "
                             "VALUES (?,?,?,?,?,?,?,?)",
                             (client_id, self.mode, signal.identity, side, str(quantity), leverage, now_ms(), action))
        params = {"symbol": signal.symbol, "side": side, "type": "LIMIT" if entry else "MARKET",
                  "quantity": format(quantity, "f"), "newClientOrderId": client_id, "newOrderRespType": "RESULT"}
        if entry:
            params.update(price=format(price, "f"), timeInForce="IOC")
        else:
            params["reduceOnly"] = "true"
        try:
            response = self.client.signed("/fapi/v1/order", "POST", **params)
        except BinanceAPIError as error:
            # A generic timeout, duplicate ID, or upstream failure needs an order lookup.
            if error.code not in {-1013, -1021, -1022, -1100, -1102, -1111, -1116, -1121, -2014, -2015,
                                  -2019, -2022, -2027, -4003, -4004, -4005, -4014, -4023, -4061, -4164}:
                raise
            with store.transaction():
                store.db.execute("UPDATE exchange_orders SET status='REJECTED',failure=? WHERE client_id=?",
                                 (str(error), client_id))
                if action != "close":
                    store.done(self.mode, signal, str(error))
            if not entry:
                raise ServiceError(f"退出委托被拒绝，稍后重试：{error}") from None
            return
        self._complete(store, {"client_id": client_id, "signal": signal.identity, "symbol": signal.symbol,
                               "leg": signal.leg, "lead_qty": signal.quantity, "signal_time": signal.time,
                               "side": side, "quantity": quantity, "action": action}, response)

    def _stop_signal(self, store, position, action, when):
        identity = f"{action}:{self.mode}:{position['entry_signal']}:{when}"
        signal = Signal(identity, when, position["symbol"], position["leg"], action, ZERO, ZERO)
        store.ingest([signal], (), when)
        return signal

    def _exit(self, store, signal, position, rules, fraction, leverage):
        lot = rules["MARKET_LOT_SIZE"]
        step = as_decimal(lot["stepSize"]) or as_decimal(rules["LOT_SIZE"]["stepSize"])
        quantity = rounded(min(Decimal(position["quantity"]) * fraction, as_decimal(lot["maxQty"])),
                           step)
        if quantity <= 0 or quantity < as_decimal(lot["minQty"]):
            return "exit quantity below Binance minimum"
        self._submit(store, signal, quantity, leverage)
        return None

    def _entry(self, store, signal, asset, positions, owned, wallet, equity, daily):
        cfg = self.config
        bid, ask = self.client.quote(signal.symbol)
        is_long = signal.leg == "LONG"
        rules = self.client.filters(signal.symbol)
        tick = as_decimal(rules["PRICE_FILTER"]["tickSize"])
        limit = signal.price * (ONE + cfg.deviation if is_long else ONE - cfg.deviation)
        limit = rounded(limit, tick) if is_long else rounded(limit, tick, ROUND_UP)
        if not limit or (is_long and ask > limit) or (not is_long and bid < limit):
            return "price moved past entry limit"
        current = positions.get(signal.symbol)
        if current and (signal.symbol not in owned or owned[signal.symbol]["leg"] != signal.leg):
            return "existing opposite or unowned position"
        gross = sum((abs(as_decimal(row["notional"])) for row in positions.values()), ZERO)
        margin = sum((as_decimal(row["initialMargin"]) for row in positions.values()), ZERO)
        notional = abs(as_decimal(current["notional"])) if current else ZERO
        if daily or equity <= 0:
            return "daily loss guard"
        if self.account.strategy == "ai":
            context = {"symbol": signal.symbol, "direction": signal.leg, "lead_reported_qty": signal.quantity,
                       "lead_execution_price": signal.price, "signal_age_seconds": (now_ms() - signal.time) // 1000,
                       "bid": bid, "ask": ask, "account_equity_usdt": equity, "current_side_notional_usdt": notional,
                       "gross_notional_usdt": gross, "estimated_margin_usdt": margin, "max_leverage": cfg.max_leverage,
                       "symbol_notional_fraction_limit": cfg.symbol_limit,
                       "gross_notional_fraction_limit": cfg.gross_limit, "margin_fraction_limit": cfg.margin_limit}
            fraction, leverage, reason = Model(cfg.ai).decide(context)
            if not fraction:
                return f"AI skip: {reason}"
        else:
            fraction, leverage = cfg.fixed_margin * cfg.fixed_leverage, cfg.fixed_leverage
        if current:
            info = self.client.signed("/fapi/v1/symbolConfig", symbol=signal.symbol)
            leverage = int(next(row for row in info if row["symbol"] == signal.symbol)["leverage"])
        leverage = min(leverage, cfg.max_leverage) if not current else leverage
        if not 1 <= leverage <= cfg.max_leverage:
            return "existing contract leverage exceeds cap"
        lot = rules["LOT_SIZE"]
        risk_price = max(limit, ask)
        desired = min(equity * fraction - notional, equity * cfg.symbol_limit - notional,
                      equity * cfg.gross_limit - gross, (equity * cfg.margin_limit - margin) * leverage,
                      min(wallet, as_decimal(asset["availableBalance"])) * leverage)
        quantity = rounded(min(desired / risk_price, as_decimal(lot["maxQty"])), as_decimal(lot["stepSize"]))
        floor = max(cfg.minimum, as_decimal(rules["MIN_NOTIONAL"]["notional"]))
        if quantity < as_decimal(lot["minQty"]) or quantity * min(limit, bid) < floor:
            return "target reached or below Binance minimum"
        if not current:
            result = self.client.signed("/fapi/v1/leverage", "POST", symbol=signal.symbol, leverage=leverage)
            if int(result["leverage"]) != leverage or quantity * risk_price > as_decimal(result["maxNotionalValue"]):
                return "Binance leverage or notional limit"
        self._submit(store, signal, quantity, leverage, limit)
        return None

    def tick(self, store):
        self.reconcile(store)
        pending, owned = store.pending(self.mode), self.owned(store)
        if not pending and not owned:
            return
        if not owned:
            with store.transaction():
                for signal, _seen in pending:
                    if signal.action != "increase":
                        store.done(self.mode, signal, "no owned position")
            pending = [(signal, seen) for signal, seen in pending if signal.action == "increase"]
            if not pending:
                return
        self.client.check_mode()
        asset, raw_positions = self.client.account()
        positions = {row["symbol"]: row for row in raw_positions if as_decimal(row["positionAmt"])}
        for symbol, pos in positions.items():
            position = owned.get(symbol)
            signed_qty = as_decimal(pos["positionAmt"])
            if (not position or pos.get("positionSide") != "BOTH" or abs(signed_qty) != Decimal(position["quantity"])
                    or (signed_qty > 0) != (position["leg"] == "LONG")):
                raise ServiceError(f"{symbol} 币安仓位与 Bot 记录不一致，请检查账户")
        missing = owned.keys() - positions.keys()
        if missing:
            raise ServiceError(f"{', '.join(sorted(missing))} 币安未返回 Bot 记录中的仓位，请检查账户")
        wallet = as_decimal(asset["walletBalance"])
        equity = wallet + as_decimal(asset["unrealizedProfit"])
        budget = self.account.budget
        with store.transaction():
            if budget is not None:
                basis_key = f"execution.wallet_basis.{self.mode}"
                if store.get(basis_key) is None:
                    store.put(basis_key, max(ZERO, wallet - budget))
                equity = min(budget, equity - Decimal(store.get(basis_key)))
            # Keep the full-wallet loss baseline separate from any older budget-based baseline.
            basis = self.mode if budget is not None else f"{self.mode}.wallet"
            daily_key = f"execution.daily.{basis}.{utc_day(now_ms())}"
            day_start = Decimal(store.get(daily_key, equity))
            if store.get(daily_key) is None:
                store.put(daily_key, equity)
            guard_key = daily_key + ".tripped"
            daily = store.get(guard_key) is not None or equity <= day_start * (ONE - self.config.daily_loss)
            if daily:
                store.put(guard_key, 1)
        for symbol, position in owned.items():
            pos = positions[symbol]
            entry = as_decimal(pos["entryPrice"])
            mark = as_decimal(pos["markPrice"])
            if mark <= 0 or entry <= 0:
                raise ServiceError(f"Binance position mark or entry price invalid for {symbol}")
            adverse = (entry - mark if position["leg"] == "LONG" else mark - entry) / entry
            if daily or adverse >= self.config.position_stop:
                signal = self._stop_signal(store, position, "daily_stop" if daily else "position_stop", now_ms())
                self._exit(store, signal, position, self.client.filters(symbol), ONE, 0)
                return
        for signal, _seen in pending:
            position = owned.get(signal.symbol)
            if signal.action == "increase":
                closed = store.db.execute("SELECT 1 FROM signals WHERE symbol=? AND leg=? AND action='close' "
                                          "AND opened<=? AND time>=? LIMIT 1", (signal.symbol, signal.leg, signal.time, signal.time)).fetchone()
                newest = store.db.execute("SELECT MAX(s.time) FROM exchange_orders o JOIN signals s ON s.identity=o.signal "
                                          "WHERE o.mode=? AND s.symbol=? AND o.action<>'increase' AND o.executed<>'0'",
                                          (self.mode, signal.symbol)).fetchone()[0]
                if not self.entry_enabled(store, signal):
                    result = "entry expired, feed unavailable or route changed"
                elif closed or (newest is not None and signal.time <= newest):
                    result = "lead position closed or stale entry after exit"
                else:
                    try:
                        result = self._entry(store, signal, asset, positions, owned, wallet, equity, daily)
                    except ServiceError as error:
                        # If submission was reserved, its exchange state must be checked before any other order.
                        if store.db.execute("SELECT 1 FROM exchange_orders WHERE mode=? AND signal=? AND status='CHECKING'",
                                            (self.mode, signal.identity)).fetchone():
                            raise
                        result = str(error)
            elif (not position or position["leg"] != signal.leg or signal.time < position["last_entry"]
                  or (signal.action == "close" and signal.opened > position["opened"])):
                result = "no matching Binance position"
            else:
                lead_qty = Decimal(position["lead_quantity"])
                fraction = ONE if signal.action == "close" else min(Decimal(".95"), signal.quantity / lead_qty) if lead_qty else ZERO
                result = self._exit(store, signal, position, self.client.filters(signal.symbol), fraction, 0)
            if result is None:
                return
            with store.transaction():
                store.done(self.mode, signal, result)


    def serve(self, stop):
        cfg = self.config
        store = Store(cfg.database, cfg.portfolio, cfg.starting, ())
        with closing(store.db):
            while not stop.is_set():
                state, delay = "就绪", 5
                try:
                    self.tick(store)
                except (ServiceError, ValueError, KeyError, TypeError, DecimalException, StopIteration, sqlite3.Error) as error:
                    state = str(error) if isinstance(error, ServiceError) else "暂时无法核对账户或委托数据"
                    delay = 15
                try:
                    key = f"execution.health.{self.mode}"
                    if store.get(key) != state:
                        with store.transaction():
                            store.put(key, state)
                        if delay == 15:
                            logging.warning("Binance %s execution: %s", self.mode, state)
                except sqlite3.OperationalError:
                    logging.warning("Could not update Binance %s execution status", self.mode)
                stop.wait(delay)
