from datetime import datetime
from decimal import Decimal

from .binance import BASE_URLS, MODE_LABELS, Futures, as_decimal
from .clients import ServiceError
from .paper import QUOTE_MAX_AGE_MS


ZERO = Decimal(0)
INCOME_TYPES = ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE")
PAPER_ACCOUNT_LABELS = {"fixed": "固定策略", "ai": "AI 策略"}


def precision(asset):
    return 4 if asset in ("USDT", "USDC") else 8


def format_pnl(asset, realized, commission, funding=None):
    decimals = precision(asset)
    net = realized + commission + (funding if funding is not None else ZERO)
    amounts = [f"已实现 {realized:+.{decimals}f}", f"手续费 {commission:+.{decimals}f}"]
    if funding is not None:
        amounts.append(f"资金费 {funding:+.{decimals}f}")
    amounts.append(f"今日净额 {net:+.{decimals}f}")
    return f"{asset} ｜ " + " · ".join(amounts)


def paper_pnl(store, accounts, start, now):
    sections = []
    with store.db:
        store.db.execute("BEGIN")
        for account in accounts:
            cash, equity, _, _, positions = store.snapshot(account)
            rows = store.db.execute(
                "SELECT realized,fee FROM fills WHERE account=? AND time BETWEEN ? AND ?", (account, start, now),
            )
            realized, fees = ZERO, ZERO
            for realized_pnl, fee in rows:
                realized += Decimal(realized_pnl)
                fees += Decimal(fee)
            if any(not -5000 <= now - row[4] <= QUOTE_MAX_AGE_MS for row in positions):
                balance = "当前权益和浮动盈亏暂不可用（行情缺失或已过期）"
            else:
                balance = f"USDT ｜ 权益 {equity:.4f} · 浮动盈亏 {equity - cash:+.4f}"
            label = PAPER_ACCOUNT_LABELS.get(account, account)
            sections.append(f"【本地模拟 · {label}】\n{balance}\n{format_pnl('USDT', realized, -fees)}")
    return sections


def binance_pnl(account, start, now):
    futures = Futures(account.mode, account.key, account.secret)
    lines = [f"【{MODE_LABELS[account.mode]}】"]
    try:
        balances = {}
        for row in futures.account_assets():
            asset = row["asset"]
            equity = as_decimal(row["marginBalance"])
            unrealized = as_decimal(row["unrealizedProfit"])
            if asset == "USDT" or equity or unrealized:
                balances[asset] = equity, unrealized
        if not balances:
            raise ServiceError("币安返回的账户资产为空")
        for asset, (equity, unrealized) in sorted(balances.items(), key=lambda item: (item[0] != "USDT", item[0])):
            decimals = precision(asset)
            lines.append(f"{asset} ｜ 权益 {equity:.{decimals}f} · 浮动盈亏 {unrealized:+.{decimals}f}")
    except (ServiceError, TypeError, KeyError) as error:
        detail = str(error) if isinstance(error, ServiceError) else "币安账户数据格式异常"
        lines.append(f"当前权益及浮动盈亏查询失败 · {detail}")

    try:
        totals = {"USDT": dict.fromkeys(INCOME_TYPES, ZERO)}
        for row in futures.income_history(start, now):
            kind = row["incomeType"]
            if kind in INCOME_TYPES:
                asset = row["asset"]
                income = totals.setdefault(asset, dict.fromkeys(INCOME_TYPES, ZERO))
                income[kind] += as_decimal(row["income"])
        for asset, income in sorted(totals.items(), key=lambda item: (item[0] != "USDT", item[0])):
            lines.append(format_pnl(asset, *(income[kind] for kind in INCOME_TYPES)))
    except (ServiceError, TypeError, KeyError) as error:
        detail = str(error) if isinstance(error, ServiceError) else "币安资金流水格式异常"
        lines.append(f"今日盈亏查询失败 · {detail}")
    return "\n".join(lines)


def pnl_report(store, accounts, execution, now, zone):
    stamp = datetime.fromtimestamp(now / 1000, zone)
    start = int(stamp.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    sections = [
        f"账户盈亏 · {stamp:%m-%d %H:%M:%S}（北京时间）\n"
        "今日自 00:00 起统计。净额为已实现盈亏计入费用后的金额，浮动盈亏单列。",
        *paper_pnl(store, accounts, start, now),
    ]
    configured = {account.mode: account for account in execution}
    for mode in BASE_URLS:
        if mode in configured:
            sections.append(binance_pnl(configured[mode], start, now))
        else:
            sections.append(f"【{MODE_LABELS[mode]}】未绑定")
    return "\n\n".join(sections)
