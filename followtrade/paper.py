from decimal import Decimal

from .store import utc_day


ZERO = Decimal(0)
ONE = Decimal(1)
QUOTE_MAX_AGE_MS = 30_000


def current_quote(quotes, symbol, now):
    quote = quotes.get(symbol)
    return quote if quote and -5000 <= now - quote.time <= QUOTE_MAX_AGE_MS else None


class PaperEngine:
    def __init__(self, store, config):
        self.store = store
        self.config = config

    def _entry(self, account, signal, quote, fraction, leverage, reason, now):
        db, cfg = self.store.db, self.config
        cash, equity, gross, margin, details = self.store.snapshot(account)
        if any(not -5000 <= now - row[4] <= QUOTE_MAX_AGE_MS for row in details):
            return "account quotes stale"
        if equity <= 0 or self._daily(account, equity, now):
            return "daily loss guard"
        same_symbol = [row for row in details if row[0]["symbol"] == signal.symbol]
        current = next((row for row in same_symbol if row[0]["leg"] == signal.leg), None)
        direction = 1 if signal.leg == "LONG" else -1
        price = (quote.ask if direction > 0 else quote.bid) * (ONE + direction * cfg.slip)
        if direction * (price - signal.price) / signal.price > cfg.deviation:
            return "price moved past entry limit"
        leverage = same_symbol[0][0]["leverage"] if same_symbol else min(leverage, cfg.max_leverage)
        if leverage > cfg.max_leverage:
            return "existing contract leverage exceeds cap"
        current_qty = current[1] if current else ZERO
        mid = (quote.bid + quote.ask) / 2
        risk_price = max(price, mid)
        contract_notional = sum((row[1] * row[3] for row in same_symbol), ZERO)
        # Reserve entry fee and any immediate mark-to-market loss when computing the caps.
        drag = max(ZERO, direction * (price - mid)) + price * cfg.fee
        quantity = min(
            (equity * fraction - current_qty * mid) / (risk_price + fraction * drag),
            (equity * cfg.symbol_limit - contract_notional) / (risk_price + cfg.symbol_limit * drag),
            (equity * cfg.gross_limit - gross) / (risk_price + cfg.gross_limit * drag),
            (equity * cfg.margin_limit - margin) / (risk_price / leverage + cfg.margin_limit * drag),
        )
        if quantity <= 0 or quantity * price < cfg.minimum:
            return "target reached or below minimum"
        entry = ((current[2] * current_qty if current else ZERO) + price * quantity) / (current_qty + quantity)
        fee = quantity * price * cfg.fee
        old = current[0] if current else None
        db.execute("INSERT OR REPLACE INTO positions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            account, signal.symbol, signal.leg, str(current_qty + quantity), str(entry), leverage,
            old["opened"] if old else signal.time, signal.time,
            old["observed_lead_qty"] if old else str(signal.quantity),
        ))
        db.execute("UPDATE accounts SET cash=? WHERE name=?", (str(cash - fee), account))
        self._fill(account, signal.symbol, signal.leg, "加仓" if old else "开仓",
                   quantity, price, leverage, fee, ZERO, signal.identity, reason, now)
        return "filled"

    def _fill(self, account, symbol, leg, action, quantity, price, leverage, fee, realized, signal, reason, now):
        self.store.db.execute(
            "INSERT INTO fills(account,time,symbol,leg,action,qty,price,leverage,fee,realized,signal,reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (account, now, symbol, leg, action, str(quantity), str(price), leverage,
             str(fee), str(realized), signal, reason),
        )

    def _exit(self, position, quote, fraction, reason, now, signal=None):
        db = self.store.db
        key = (position["account"], position["symbol"], position["leg"])
        account, symbol, leg = key
        qty = Decimal(position["qty"])
        direction = 1 if leg == "LONG" else -1
        price = (quote.bid if direction > 0 else quote.ask) * (ONE - direction * self.config.slip)
        size = qty * fraction
        fee = size * price * self.config.fee
        realized = direction * size * (price - Decimal(position["entry"]))
        cash = Decimal(db.execute("SELECT cash FROM accounts WHERE name=?", (account,)).fetchone()[0])
        db.execute("UPDATE accounts SET cash=? WHERE name=?", (str(cash + realized - fee), account))
        if fraction == ONE:
            db.execute("DELETE FROM positions WHERE account=? AND symbol=? AND leg=?", key)
        else:
            db.execute("UPDATE positions SET qty=? WHERE account=? AND symbol=? AND leg=?", (str(qty - size), *key))
        self._fill(account, symbol, leg, "平仓" if fraction == ONE else "减仓",
                   size, price, position["leverage"], fee, realized, signal, reason, now)

    def _daily(self, account, equity, now):
        day = utc_day(now)
        db = self.store.db
        last = db.execute("SELECT * FROM daily WHERE account=?", (account,)).fetchone()
        if not last or last["day"] != day:
            db.execute("INSERT OR REPLACE INTO daily VALUES (?, ?, ?, 0)", (account, day, str(equity)))
            return False
        if last["tripped"] or equity <= Decimal(last["start_equity"]) * (ONE - self.config.daily_loss):
            db.execute("UPDATE daily SET tripped=1 WHERE account=?", (account,))
            return True
        return False

    def protect(self, accounts, quotes, now):
        with self.store.transaction():
            self.store.save_marks(quotes)
            for account in accounts:
                _, equity, _, _, details = self.store.snapshot(account)
                complete = all(current_quote(quotes, row[0]["symbol"], now) for row in details)
                daily = complete and self._daily(account, equity, now)
                for position, _, entry, _, _, _ in details:
                    quote = current_quote(quotes, position["symbol"], now)
                    if quote is None:
                        continue
                    adverse = (entry - quote.bid) if position["leg"] == "LONG" else (quote.ask - entry)
                    if daily or adverse / entry >= self.config.position_stop:
                        self._exit(position, quote, ONE, "每日亏损保护" if daily else "单仓止损", now)
                        self.store.put(f"cooldown:{account}:{position['symbol']}:{position['leg']}", now)

    def process(self, account, signal, quotes, decision, now, *, dead_reason=None, ai_message=None):
        db, cfg = self.store.db, self.config
        key = (account, signal.symbol, signal.leg)
        with self.store.transaction():
            job = db.execute("SELECT result FROM jobs WHERE account=? AND signal=?",
                             (account, signal.identity)).fetchone()
            if not job or job[0] is not None:
                return
            if signal.time > now + 5000:
                return
            if ai_message is not None:
                self.store.queue_ai_decision(account, signal, ai_message)
            position = db.execute("SELECT * FROM positions WHERE account=? AND symbol=? AND leg=?", key).fetchone()
            quote = current_quote(quotes, signal.symbol, now)
            if quote:
                self.store.save_marks({signal.symbol: quote})
            cursor = f"event:{account}:{signal.symbol}:{signal.leg}"
            previous_event = int(self.store.get(cursor, 0))
            if signal.time < previous_event:
                self.store.done(account, signal, "old or out-of-order event")
                return
            if signal.action == "increase":
                closed = self.store.has_source_close(signal)
                if position:
                    observed = Decimal(position["observed_lead_qty"]) + signal.quantity
                    db.execute("UPDATE positions SET observed_lead_qty=? WHERE account=? AND symbol=? AND leg=?",
                               (str(observed), *key))
                cooldown = int(self.store.get(f"cooldown:{account}:{signal.symbol}:{signal.leg}", 0))
                if now - signal.time >= cfg.max_age * 1000:
                    result = "signal expired"
                elif closed:
                    result = "leader cycle already closed"
                elif signal.time <= cooldown:
                    result = "signal before local stop"
                elif dead_reason:
                    result = dead_reason
                elif not quote:
                    result = "fresh quote unavailable"
                else:
                    fraction, leverage, reason = decision
                    result = self._entry(account, signal, quote, fraction, leverage, reason, now) if fraction else reason
            elif not position:
                result = "no owned position"
            elif signal.time < position["last_entry"] or (signal.action == "close" and signal.opened > position["opened"]):
                result = "closure does not match owned position"
            elif not quote:
                return
            elif signal.action == "close":
                self._exit(position, quote, ONE, "带单员全平记录", now, signal.identity)
                result = "filled"
            else:
                observed = Decimal(position["observed_lead_qty"])
                if observed <= 0:
                    result = "leader quantity unknown; wait for closed record"
                else:
                    fraction = signal.quantity / observed
                    # On an estimated zero balance, wait for the separate closed-position record.
                    fraction = min(fraction, Decimal("0.95"))
                    self._exit(position, quote, fraction, "按可见成交比例减仓", now, signal.identity)
                    db.execute("UPDATE positions SET observed_lead_qty=? WHERE account=? AND symbol=? AND leg=?",
                               (str(max(ZERO, observed - signal.quantity)), *key))
                    result = "filled"
            self.store.put(cursor, signal.time)
            self.store.done(account, signal, result)

    def ai_context(self, signal, quote, now):
        _, equity, gross, margin, details = self.store.snapshot("ai")
        pos = [row for row in details if row[0]["symbol"] == signal.symbol and row[0]["leg"] == signal.leg]
        return {
            **self.config.ai_trader_context(signal.portfolio),
            "symbol": signal.symbol, "direction": signal.leg, "lead_reported_qty": signal.quantity,
            "lead_execution_price": signal.price, "signal_age_seconds": (now - signal.time) // 1000,
            "bid": quote.bid, "ask": quote.ask, "account_equity_usdt": equity,
            "current_side_notional_usdt": pos[0][1] * pos[0][3] if pos else ZERO,
            "gross_notional_usdt": gross, "estimated_margin_usdt": margin, "max_leverage": self.config.max_leverage,
            "symbol_notional_fraction_limit": self.config.symbol_limit,
            "gross_notional_fraction_limit": self.config.gross_limit, "margin_fraction_limit": self.config.margin_limit,
        }
