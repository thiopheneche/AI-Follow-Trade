from decimal import Decimal

from .binance import MODE_LABELS


def format_ai_decision(account, signal, equity, decision, error=None, elapsed=None):
    labels = {**MODE_LABELS, "ai": "本地 AI 模拟", "test": "历史信号测试，仅评估"}
    direction = {"LONG": "做多", "SHORT": "做空"}[signal.leg]
    lines = [f"[AI 判决 · {labels[account]}] {signal.symbol} {direction}"]
    if signal.portfolio:
        lines.append(f"组合 ID {signal.portfolio}")
    if error is not None:
        lines.append(f"未获得有效判决 · {error}")
    else:
        fraction, leverage, reason = decision
        if fraction:
            lines.append(f"建议跟单 · 目标名义仓位 {fraction:.2%}（{fraction * Decimal(equity):.2f} USDT）")
            lines.append(f"建议杠杆 {leverage} 倍 · 按建议估算保证金 {fraction * Decimal(equity) / leverage:.2f} USDT")
        else:
            lines.append("建议跳过")
        lines.append(f"理由 {reason}")
    if elapsed is not None:
        lines.append(f"用时 {elapsed:.2f} 秒")
    if account != "test":
        lines.append("成交状态以随后发出的委托或成交回报为准。")
    return "\n".join(lines)
