from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import sqlite3

from .clients import Signal


SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS accounts (name TEXT PRIMARY KEY, cash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS positions (
    account TEXT NOT NULL, symbol TEXT NOT NULL, leg TEXT NOT NULL,
    qty TEXT NOT NULL, entry TEXT NOT NULL, leverage INTEGER NOT NULL,
    opened INTEGER NOT NULL, last_entry INTEGER NOT NULL, observed_lead_qty TEXT NOT NULL,
    PRIMARY KEY (account, symbol, leg)
);
CREATE TABLE IF NOT EXISTS signals (
    identity TEXT PRIMARY KEY, time INTEGER NOT NULL, symbol TEXT NOT NULL, leg TEXT NOT NULL,
    action TEXT NOT NULL, quantity TEXT NOT NULL, price TEXT NOT NULL, opened INTEGER NOT NULL,
    first_seen INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS source_closes ON signals (symbol, leg, time) WHERE action = 'close';
CREATE TABLE IF NOT EXISTS telegram_lead_fills (
    signal TEXT PRIMARY KEY REFERENCES signals(identity)
);
CREATE TABLE IF NOT EXISTS telegram_ai_decisions (
    account TEXT NOT NULL, signal TEXT NOT NULL REFERENCES signals(identity), message TEXT NOT NULL,
    PRIMARY KEY (account, signal)
);
CREATE TABLE IF NOT EXISTS jobs (
    account TEXT NOT NULL, signal TEXT NOT NULL, result TEXT,
    PRIMARY KEY (account, signal), FOREIGN KEY (signal) REFERENCES signals(identity)
);
CREATE INDEX IF NOT EXISTS jobs_pending ON jobs (account) WHERE result IS NULL;
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY, account TEXT NOT NULL, time INTEGER NOT NULL, symbol TEXT NOT NULL,
    leg TEXT NOT NULL, action TEXT NOT NULL, qty TEXT NOT NULL, price TEXT NOT NULL,
    leverage INTEGER NOT NULL, fee TEXT NOT NULL, realized TEXT NOT NULL,
    signal TEXT, reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS daily (
    account TEXT PRIMARY KEY, day TEXT NOT NULL, start_equity TEXT NOT NULL, tripped INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS marks (
    symbol TEXT PRIMARY KEY, price TEXT NOT NULL, time INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS exchange_orders (
    client_id TEXT PRIMARY KEY, mode TEXT NOT NULL, signal TEXT NOT NULL REFERENCES signals(identity),
    side TEXT NOT NULL, quantity TEXT NOT NULL, leverage INTEGER NOT NULL, time INTEGER NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'CHECKING', executed TEXT NOT NULL DEFAULT '0', price TEXT NOT NULL DEFAULT '0',
    failure TEXT, notified INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS exchange_orders_mode ON exchange_orders(mode, status);
CREATE TABLE IF NOT EXISTS exchange_positions (
    mode TEXT NOT NULL, symbol TEXT NOT NULL, leg TEXT NOT NULL, quantity TEXT NOT NULL,
    opened INTEGER NOT NULL, last_entry INTEGER NOT NULL, entry_signal TEXT NOT NULL REFERENCES signals(identity),
    lead_quantity TEXT NOT NULL, PRIMARY KEY (mode, symbol)
);
"""


def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def utc_day(at):
    return datetime.fromtimestamp(at / 1000, timezone.utc).date().isoformat()


class Store:
    def __init__(self, path, portfolio, starting, accounts):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=8)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript(SCHEMA)
        with self.transaction():
            stored = self.get("portfolio")
            if stored not in (None, portfolio):
                raise ValueError("database belongs to another portfolio")
            self.put("portfolio", portfolio)
            for account in accounts:
                self.db.execute("INSERT OR IGNORE INTO accounts VALUES (?, ?)", (account, str(starting)))

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default

    def put(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO settings VALUES (?, ?)", (key, str(value)))

    def ingest(self, signals, accounts, first_seen, *, baseline=False, progress=None, notify_lead_fills=False,
               execution_modes=()):
        with self.transaction():
            first_start = int(self.get("service.first_start", first_seen)) if notify_lead_fills else 0
            selected = self.get("execution.mode", "paper")
            selected_at = int(self.get("execution.selected_at", 0))
            for signal in signals:
                result = self.db.execute("INSERT OR IGNORE INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                    signal.identity, signal.time, signal.symbol, signal.leg, signal.action,
                    str(signal.quantity), str(signal.price), signal.opened, first_seen,
                ))
                if not result.rowcount:
                    continue
                if not baseline:
                    self.db.executemany("INSERT INTO jobs VALUES (?, ?, NULL)",
                                        ((account, signal.identity) for account in accounts))
                    for mode in execution_modes:
                        if signal.action != "increase" or (mode == selected and signal.time >= selected_at):
                            self.db.execute("INSERT INTO jobs VALUES (?, ?, NULL)", (mode, signal.identity))
                if notify_lead_fills and signal.action in ("increase", "decrease") and signal.time >= first_start:
                    self.db.execute("INSERT INTO telegram_lead_fills VALUES (?)", (signal.identity,))
            for key, value in (progress or {}).items():
                self.put(key, value)

    def pending(self, account):
        rows = self.db.execute(
            "SELECT s.* FROM jobs j JOIN signals s ON s.identity = j.signal "
            "WHERE j.account = ? AND j.result IS NULL ORDER BY s.time, s.action = 'close', s.rowid", (account,),
        )
        return [(Signal(r["identity"], r["time"], r["symbol"], r["leg"], r["action"],
                        Decimal(r["quantity"]), Decimal(r["price"]), r["opened"]), r["first_seen"]) for r in rows]

    def done(self, account, signal, result):
        self.db.execute("UPDATE jobs SET result = ? WHERE account = ? AND signal = ?",
                        (result, account, signal.identity))

    def queue_ai_decision(self, account, signal, message):
        self.db.execute("INSERT OR IGNORE INTO telegram_ai_decisions VALUES (?, ?, ?)",
                        (account, signal.identity, message))

    def positions(self, account):
        return self.db.execute("SELECT * FROM positions WHERE account = ?", (account,)).fetchall()

    def save_marks(self, quotes):
        if quotes:
            self.db.executemany("INSERT OR REPLACE INTO marks VALUES (?, ?, ?)", (
                (symbol, str((quote.bid + quote.ask) / 2), quote.time) for symbol, quote in quotes.items()
            ))

    def snapshot(self, account):
        cash = Decimal(self.db.execute("SELECT cash FROM accounts WHERE name = ?", (account,)).fetchone()[0])
        positions = self.positions(account)
        rows = self.db.execute(
            "SELECT DISTINCT m.* FROM marks m JOIN positions p ON p.symbol=m.symbol WHERE p.account=?",
            (account,),
        )
        marks = {r["symbol"]: (Decimal(r["price"]), r["time"]) for r in rows}
        equity, gross, margin = cash, Decimal(0), Decimal(0)
        details = []
        for pos in positions:
            qty, entry = Decimal(pos["qty"]), Decimal(pos["entry"])
            mark, mark_time = marks.get(pos["symbol"], (entry, 0))
            direction = 1 if pos["leg"] == "LONG" else -1
            pnl = direction * qty * (mark - entry)
            equity += pnl
            gross += mark * qty
            margin += mark * qty / pos["leverage"]
            details.append((pos, qty, entry, mark, mark_time, pnl))
        return cash, equity, gross, margin, details
