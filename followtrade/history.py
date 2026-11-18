from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal
import json

from .binance import Futures, MODE_LABELS, as_decimal
from .clients import ServiceError
from .pnl import PAPER_ACCOUNT_LABELS, precision


PAGE_SIZE = 8
SCOPES = {"all": "all", "paper": "paper", "demo": "demo", "live": "mainnet"}
LABELS = {"all": "全部账户", **MODE_LABELS}
ACTIONS = {"increase": "开仓/加仓", "decrease": "减仓", "close": "平仓", "position_stop": "止损平仓",
           "daily_stop": "风控平仓"}
STATUSES = {"FILLED": "已成交", "CHECKING": "待核对", "EXPIRED": "已过期", "EXPIRED_IN_MATCH": "撮合过期",
            "CANCELED": "已取消", "REJECTED": "已拒绝"}
USAGE = "用法 /history [all|paper|demo|live] [页码]，例如 /history demo 2"


def order_pnl(account, row):
    client = Futures(account.mode, account.key, account.secret)
    symbol = row["symbol"]
    order = client.signed("/fapi/v1/order", symbol=symbol, origClientOrderId=row["client_id"])
    expected_qty = as_decimal(row["qty"])
    if (not isinstance(order, dict) or order.get("clientOrderId") != row["client_id"]
            or order.get("symbol") != symbol or order.get("side") != row["side"]
            or type(order.get("orderId")) is not int or as_decimal(order.get("executedQty")) != expected_qty):
        raise ServiceError("币安委托与账本记录不一致")
    order_id = order["orderId"]
    trades = client.signed("/fapi/v1/userTrades", symbol=symbol, orderId=order_id, limit=1000)
    if not isinstance(trades, list):
        raise ServiceError("币安成交明细格式异常")

    totals = defaultdict(lambda: [Decimal(0), Decimal(0)])
    filled_at, quantity, trade_ids = 0, Decimal(0), set()
    for trade in trades:
        if not isinstance(trade, dict):
            raise ServiceError("币安成交明细格式异常")
        if trade.get("orderId") != order_id:
            continue
        if (trade.get("symbol") != symbol or trade.get("side") != row["side"]
                or type(trade.get("id")) is not int or trade["id"] in trade_ids
                or type(trade.get("time")) is not int or trade["time"] <= 0):
            raise ServiceError("币安成交明细与委托不一致")
        margin_asset, commission_asset = trade.get("marginAsset"), trade.get("commissionAsset")
        if not all(isinstance(asset, str) and asset for asset in (margin_asset, commission_asset)):
            raise ServiceError("币安未返回盈亏或手续费币种")
        size = as_decimal(trade.get("qty"))
        if size <= 0:
            raise ServiceError("币安成交数量异常")
        quantity += size
        filled_at = max(filled_at, trade["time"])
        trade_ids.add(trade["id"])
        totals[margin_asset][0] += as_decimal(trade.get("realizedPnl"))
        totals[commission_asset][1] += as_decimal(trade.get("commission"))
    if quantity != expected_qty or not trade_ids:
        raise ServiceError("币安成交明细暂不完整")
    return filled_at, [(asset, str(pnl), str(fee)) for asset, (pnl, fee) in sorted(totals.items())]


def format_amounts(amounts):
    lines = []
    for asset, pnl, fee in amounts:
        realized, commission = Decimal(pnl), Decimal(fee)
        places = precision(asset)
        lines.append(f"{asset} ｜ 已实现 {realized:+.{places}f} · 手续费 {commission:.{places}f} · "
                     f"净额 {realized - commission:+.{places}f}")
    return "\n".join(lines)


def format_record(row, zone, result=None, error=None):
    scope, action = row["scope"], row["action"]
    direction = {"LONG": "多", "SHORT": "空"}.get(row["leg"], row["leg"])
    if scope == "paper":
        label = f"{LABELS[scope]} · {PAPER_ACCOUNT_LABELS.get(row['account'], row['account'])}"
        amounts = [("USDT", row["realized"], row["fee"])]
        stamp = row["time"]
        status = ""
    else:
        label = LABELS[scope]
        stamp, amounts = result if result else (row["time"], None)
        action = ACTIONS.get(action, action)
        state = STATUSES.get(row["status"], row["status"])
        if Decimal(row["qty"]) and row["status"] != "FILLED":
            state = f"部分成交（{state}）"
        status = f" · {state}"
    when = datetime.fromtimestamp(stamp / 1000, zone).strftime("%m-%d %H:%M:%S")
    lines = [f"{when} · {label} · {row['symbol']} {direction} · {action}{status}"]
    if Decimal(row["qty"]):
        lines.append(f"成交 {Decimal(row['qty']):.8g} @ {Decimal(row['price']):.8g}")
        lines.append(format_amounts(amounts) if amounts else f"盈亏暂不可用 · {error}")
    elif row["status"] == "CHECKING":
        lines.append("等待币安确认成交")
    else:
        lines.append(row["failure"] or "无成交")
    return "\n".join(lines)


def trade_history(store, execution, argument, zone):
    parts = argument.lower().split()
    selected = parts.pop(0) if parts and parts[0] in SCOPES else "all"
    if parts and (len(parts) != 1 or not parts[0].isascii() or not parts[0].isdigit()
                  or len(parts[0]) > 9 or int(parts[0]) < 1):
        return USAGE
    page = int(parts[0]) if parts else 1
    scope = SCOPES[selected]
    rows = store.db.execute(
        "WITH history AS ("
        "SELECT 'paper' AS scope, id AS seq, account, time, symbol, leg, action, qty, price, fee, realized, "
        "NULL AS client_id, NULL AS side, NULL AS status, NULL AS failure, NULL AS filled_at, NULL AS amounts "
        "FROM fills WHERE ? IN ('all','paper') "
        "UNION ALL "
        "SELECT o.mode, o.rowid, o.mode, o.time, s.symbol, s.leg, o.action, o.executed, o.price, NULL, NULL, "
        "o.client_id, o.side, o.status, o.failure, p.filled_at, p.amounts "
        "FROM exchange_orders o JOIN signals s ON s.identity=o.signal "
        "LEFT JOIN exchange_order_pnl p ON p.client_id=o.client_id WHERE ? IN ('all',o.mode)) "
        "SELECT * FROM history ORDER BY time DESC, scope, seq DESC LIMIT ? OFFSET ?",
        (scope, scope, PAGE_SIZE + 1, (page - 1) * PAGE_SIZE),
    ).fetchall()
    if not rows:
        return f"{LABELS[scope]} · 第 {page} 页暂无跟单记录"

    accounts = {account.mode: account for account in execution}
    results, errors, cache = {}, {}, []
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {}
        for row in rows[:PAGE_SIZE]:
            client_id = row["client_id"]
            if client_id is None or not Decimal(row["qty"]):
                continue
            if row["amounts"] is not None:
                results[client_id] = row["filled_at"], json.loads(row["amounts"])
            elif row["scope"] in accounts:
                jobs[pool.submit(order_pnl, accounts[row["scope"]], row)] = client_id
            else:
                errors[client_id] = "账户未绑定"
        for future in as_completed(jobs):
            client_id = jobs[future]
            try:
                filled_at, amounts = future.result()
            except ServiceError as error:
                errors[client_id] = str(error)
            else:
                results[client_id] = filled_at, amounts
                cache.append((client_id, filled_at, json.dumps(amounts)))
    if cache:
        with store.transaction():
            store.db.executemany("INSERT OR IGNORE INTO exchange_order_pnl VALUES (?, ?, ?)", cache)

    title = (f"【跟单历史 · {LABELS[scope]} · 第 {page} 页】（北京时间）\n"
             "净额按本单成交计入手续费。资金费见 /pnl。")
    records = [format_record(row, zone, results.get(row["client_id"]), errors.get(row["client_id"]))
               for row in rows[:PAGE_SIZE]]
    pages = []
    if page > 1:
        pages.append(f"上一页 /history {selected} {page - 1}")
    if len(rows) > PAGE_SIZE:
        pages.append(f"下一页 /history {selected} {page + 1}")
    return "\n\n".join([title, *records, *pages])
