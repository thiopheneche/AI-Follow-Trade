# AI Follow Trade

自托管的币安 U 本位合约跟单机器人。监控带单员的公开成交，按固定规则或 AI 策略决定跟单仓位和杠杆，支持本地模拟、币安 Demo 和实盘。

## 快速开始

需要 Python 3.9+。程序使用 Python 标准库和 SQLite，可在本机或 Linux VPS 上运行。

1. 将 `config.example.json` 复制为 `config.json`。
2. 在 `config.json` 中填写要跟踪的 `portfolio_id`，并设置预算、仓位和杠杆上限。
3. 检查成交源和行情，再启动本地模拟。

   ```sh
   python -m followtrade --config config.json --once
   python -m followtrade --config config.json
   ```

成交默认每 30 秒查询一次。首次采集建立基线，后续新成交才进入跟单流程。跟单受敞口、保证金和杠杆上限约束。信号、策略结果和成交记录保存在 SQLite。

## Telegram

用 BotFather 创建机器人并向它发送一条私聊消息。将 `getUpdates` 返回的 `message.chat.id` 填入 `telegram.chat_id`，通过环境变量 `FOLLOWTRADE_TELEGRAM_TOKEN` 提供 Bot Token。绑定后可接收成交、AI 决策和委托回报。

| 指令 | 用途 |
| --- | --- |
| `/status` | 查看跟踪目标、运行状态和各账户持仓 |
| `/trader <ID>` | 切换带单员，也支持币安带单详情链接 |
| `/paper`、`/demo`、`/live` | 选择新开仓使用本地模拟、币安 Demo 或实盘 |
| `/help` | 查看全部指令 |

切换带单员会等待旧仓位和委托处理完毕。切换执行账户只影响新开仓。

## 可选配置

- **AI 策略** · 设置 `ai.enabled`、`ai.base_url`、`ai.model`、`ai.trader_profile`，并通过 `FOLLOWTRADE_AI_KEY` 提供模型密钥。只在币安账户使用 AI 时，将 `ai.local_paper_enabled` 设为 `false`。
- **币安账户** · 在已配置 `ai-follow-trade.service` 的 VPS 上运行 `python3 deploy/configure-binance.py demo` 或 `python3 deploy/configure-binance.py mainnet`，录入并验证对应的 HMAC 凭据。实盘使用真实资金。
- **只读查询** · 运行 `python -m followtrade.trader PORTFOLIO_ID --history-page 1` 查看带单账户保证金、公开持仓和历史仓位。

## 测试

```sh
python -m unittest discover -s tests
```
