import argparse
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import logging
import os
from pathlib import Path
import signal as process_signal
import threading
import time

from probe import AccessStopped, ProbeError

from .clients import BinanceFeed, BinanceMarket, Model, ServiceError, check_binance_account
from .config import load_config
from .execution import ExchangeAccount
from .paper import PaperEngine, current_quote
from .store import Store, now_ms
from .telegram import Telegram


OVERLAP_MS = 300_000
MAX_LOOKBACK_MS = 1_800_000


def collect(feed, last_end, end, *, include_head=False):
    start = max(end - MAX_LOOKBACK_MS, min(end - OVERLAP_MS, last_end - OVERLAP_MS))
    orders = feed.orders(start, end)
    orders_seen = now_ms()
    late = []
    if include_head:
        try:
            late = [signal for signal in feed.head() if signal.time < start]
        except AccessStopped:
            raise
        except ProbeError as error:
            logging.warning("Recent order head poll failed: %s", error)
    return orders, orders_seen, start, late


def close_batch(feed):
    signals = list(feed.closures())
    return signals, now_ms()


class Runner:
    def __init__(self, config, store, pool):
        self.config, self.store, self.pool = config, store, pool
        self.accounts = ("fixed", "ai") if config.ai else ("fixed",)
        self.execution_modes = tuple(account.mode for account in config.execution)
        self.engine = PaperEngine(store, config)
        self.feed = BinanceFeed(config.portfolio)
        self.market = BinanceMarket()
        self.model = Model(config.ai) if config.ai else None
        self.quote_data = {}
        self.order_task = self.close_task = self.quote_task = self.ai_task = None
        self.next_poll = self.next_quote = self.next_close = self.next_head = 0
        self.poll_failures = 0
        self.access_stopped = False

    def update(self, key, text):
        with self.store.transaction():
            self.store.put(key, text)

    def _read_orders(self, monotonic):
        if not self.order_task or not self.order_task.done():
            return
        task, end = self.order_task.future, self.order_task.end
        self.order_task = None
        try:
            signals, seen, start, late = task.result()
        except AccessStopped as error:
            self.access_stopped = True
            self.update("feed_status", "访问被拒绝，等待人工检查后重启")
            logging.warning("Binance public feed stopped: %s", error)
        except ProbeError as error:
            self.poll_failures += 1
            self.next_poll = monotonic + min(600, self.config.poll_seconds * 2 ** min(self.poll_failures, 5))
            self.update("feed_status", "订单源失败，正在退避")
            logging.warning("Binance order poll failed: %s", error)
        else:
            self.poll_failures = 0
            previous = self.store.get("orders.last_end")
            progress = {"orders.last_end": end}
            if previous is not None and start > int(previous):
                progress["orders.gap"] = f"{previous}–{start}"
                logging.warning("Unobserved order interval; new entries paused")
            gap = progress.get("orders.gap") or self.store.get("orders.gap")
            progress["feed_status"] = ("检测到轮询缺口，暂停新增敞口" if gap
                                       else f"正常，最近一轮 {len(signals)} 条成交（含重复）")
            notify = self.config.telegram is not None and self.config.telegram.push_fills
            self.store.ingest(signals, self.accounts, seen, baseline=previous is None, progress=progress,
                              notify_lead_fills=notify, execution_modes=self.execution_modes)
            self.store.ingest(late, self.accounts, now_ms(), baseline=previous is None, notify_lead_fills=notify,
                              execution_modes=self.execution_modes)
            self.next_poll = monotonic + self.config.poll_seconds

    def _read_closes(self, monotonic):
        if not self.close_task or not self.close_task.done():
            return
        task, self.close_task = self.close_task, None
        try:
            signals, seen = task.result()
        except AccessStopped as error:
            self.access_stopped = True
            self.update("feed_status", "访问被拒绝，等待人工检查后重启")
            logging.warning("Binance public feed stopped: %s", error)
        except ProbeError as error:
            self.update("close_status", "全平记录暂不可读")
            self.next_close = monotonic + max(self.config.poll_seconds, 90)
            logging.warning("Binance close poll failed: %s", error)
        else:
            # A paper position can't predate bootstrap for the selected account.
            baseline = int(self.store.get("service.first_start", seen))
            self.store.ingest((s for s in signals if s.time >= baseline), self.accounts, seen,
                              execution_modes=self.execution_modes)
            self.update("close_status", "全平记录正常")
            self.next_close = monotonic + self.config.poll_seconds

    def _read_quotes(self, monotonic):
        if not self.quote_task or not self.quote_task.done():
            return
        task, self.quote_task = self.quote_task, None
        try:
            quotes = task.result()
            now = now_ms()
            self.quote_data = {s: q for s, q in quotes.items() if current_quote(quotes, s, now)}
            if not self.quote_data:
                raise ServiceError("no timely futures quotes")
        except ServiceError as error:
            self.quote_data = {}
            self.update("quote_status", str(error))
            logging.warning("Quotes unavailable: %s", error)
            self.next_quote = monotonic + 30
        else:
            required = {p["symbol"] for account in self.accounts for p in self.store.positions(account)}
            self.engine.protect(self.accounts, {s: q for s, q in self.quote_data.items() if s in required}, now)
            self.update("quote_status", f"可用 · {time.strftime('%H:%M:%S UTC', time.gmtime())}")
            self.next_quote = monotonic + 12

    def _ready_ai(self):
        if not self.ai_task or not self.ai_task[1].done():
            return
        signal, task, _ = self.ai_task
        self.ai_task = None
        try:
            decision = task.result()
        except ServiceError as error:
            self.engine.process("ai", signal, self.quote_data, None, now_ms(), dead_reason=str(error))
        else:
            reason = "feed unavailable" if self.store.get("orders.gap") or self.access_stopped or self.poll_failures else None
            self.engine.process("ai", signal, self.quote_data, decision, now_ms(), dead_reason=reason)

    def _jobs(self, now):
        opening_disabled = self.store.get("orders.gap") or self.access_stopped or self.poll_failures
        decision = (self.config.fixed_margin * self.config.fixed_leverage,
                    self.config.fixed_leverage, "固定仓位目标")
        for account in self.accounts:
            for signal, _seen in self.store.pending(account):
                if signal.action != "increase":
                    self.engine.process(account, signal, self.quote_data, None, now)
                    continue
                if now - signal.time >= self.config.max_age * 1000 or opening_disabled:
                    self.engine.process(account, signal, self.quote_data, None, now, dead_reason="feed unavailable")
                    continue
                if signal.time > now + 5000 or not current_quote(self.quote_data, signal.symbol, now):
                    continue
                if account == "fixed":
                    self.engine.process(account, signal, self.quote_data, decision, now)
                elif self.ai_task is None:
                    context = self.engine.ai_context(signal, self.quote_data[signal.symbol], now)
                    self.ai_task = signal, self.pool.submit(self.model.decide, context), now + self.config.ai.timeout * 1000

    def _expire_ai(self, now):
        if self.ai_task and now >= self.ai_task[2]:
            signal, _task, _deadline = self.ai_task
            # Keep a single in-flight request until its transport finishes.
            self.engine.process("ai", signal, self.quote_data, None, now, dead_reason="model deadline exceeded")

    def tick(self, stop):
        monotonic = time.monotonic()
        self._read_orders(monotonic)
        self._read_closes(monotonic)
        self._read_quotes(monotonic)
        now = now_ms()
        self._expire_ai(now)
        self._ready_ai()
        self._jobs(now)
        if self.quote_task is None and monotonic >= self.next_quote:
            self.quote_task = self.pool.submit(self.market.quotes)
        if not self.access_stopped and self.order_task is None and self.close_task is None:
            if monotonic >= self.next_poll:
                previous = int(self.store.get("orders.last_end", now))
                include_head = monotonic >= self.next_head
                if include_head:
                    self.next_head = monotonic + 180
                self.order_task = OrderTask(
                    self.pool.submit(collect, self.feed, previous, now, include_head=include_head), now)
            elif monotonic >= self.next_close:
                self.close_task = self.pool.submit(close_batch, self.feed)
        stop.wait(1)


class OrderTask:
    def __init__(self, future, end):
        self.future, self.end = future, end

    def done(self):
        return self.future.done()


def main():
    parser = argparse.ArgumentParser(description="Binance USD-M paper copy-trading monitor")
    parser.add_argument("--config", type=Path, default=Path("config.json"))
    parser.add_argument("--once", action="store_true", help="check the source and quotes without starting the bot")
    parser.add_argument("--check-account", choices=("demo", "mainnet"), help="check signed, read-only Futures account access")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.check_account:
        key, secret = os.getenv("FOLLOWTRADE_BINANCE_KEY"), os.getenv("FOLLOWTRADE_BINANCE_SECRET")
        if not key or not secret:
            parser.error("--check-account requires FOLLOWTRADE_BINANCE_KEY and FOLLOWTRADE_BINANCE_SECRET")
        try:
            has_usdt = check_binance_account(key, secret, demo=args.check_account == "demo")
        except ServiceError as error:
            print(error)
            return 1
        print(f"Signed {args.check_account} Futures account read: OK (USDT row present: {has_usdt})")
        return 0
    try:
        cfg = load_config(args.config)
    except (OSError, ValueError, TypeError) as error:
        parser.error(str(error))
    if args.once:
        failures = 0
        try:
            end = now_ms()
            orders, _, _, _ = collect(BinanceFeed(cfg.portfolio), end, end)
            print(f"order feed: OK ({len(orders)} recent signals)")
        except ProbeError as error:
            print(f"order feed: {error}")
            failures += 1
        try:
            print(f"futures quotes: OK ({len(BinanceMarket().quotes())} symbols)")
        except ServiceError as error:
            print(f"futures quotes: {error}")
            failures += 1
        return int(failures > 0)
    stop = threading.Event()
    process_signal.signal(process_signal.SIGTERM, lambda *_: stop.set())
    accounts = ("fixed", "ai") if cfg.ai else ("fixed",)
    store = Store(cfg.database, cfg.portfolio, cfg.starting, accounts)
    with store.transaction():
        if store.get("service.first_start") is None:
            store.put("service.first_start", now_ms())
    telegram = None
    if cfg.telegram:
        telegram = threading.Thread(target=Telegram(cfg.telegram).serve, args=(cfg, accounts, stop), daemon=True)
        telegram.start()
    execution = [threading.Thread(target=ExchangeAccount(cfg, account).serve, args=(stop,), daemon=True)
                 for account in cfg.execution]
    for worker in execution:
        worker.start()
    logging.info("Paper accounts %s for portfolio %s", ", ".join(accounts), cfg.portfolio)
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            runner = Runner(cfg, store, pool)
            try:
                while not stop.is_set():
                    runner.tick(stop)
            except KeyboardInterrupt:
                stop.set()
    finally:
        stop.set()
        if telegram:
            telegram.join(timeout=14)
        for worker in execution:
            worker.join(timeout=6)
        store.db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
