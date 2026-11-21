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
| `/pnl` | 查看账户权益、浮动盈亏及今日已实现净额，按北京时间统计 |
| `/history [all\|paper\|demo\|live] [页码]` | 按单查看成交量、均价、已实现盈亏和手续费 |
| `/orders` | 查看币安委托和跟单跳过原因 |
| `/trader <ID>` | 切换带单员，也支持币安带单详情链接 |
| `/paper`、`/demo`、`/live` | 选择新开仓使用本地模拟、币安 Demo 或实盘 |
| `/help` | 查看全部指令 |

切换带单员会等待旧仓位和委托处理完毕。切换执行账户只影响新开仓。

## 可选配置

- **AI 策略** · 设置 `ai.enabled`、`ai.base_url`、`ai.model`、`ai.trader_profile`，并通过 `FOLLOWTRADE_AI_KEY` 提供模型密钥。只在币安账户使用 AI 时，将 `ai.local_paper_enabled` 设为 `false`。
- **币安账户** · 在已配置 `ai-follow-trade.service` 的 VPS 上运行 `python3 deploy/configure-binance.py demo` 或 `python3 deploy/configure-binance.py mainnet`，录入并验证对应的 HMAC 凭据。实盘使用真实资金。
- **只读查询** · 运行 `python -m followtrade.trader PORTFOLIO_ID --history-page 1` 查看带单账户保证金、公开持仓和历史仓位。

`max_entry_deviation_fraction` 设置相对带单成交价的不利入场价差上限。可在 `execution.demo` 或 `execution.mainnet` 下单独设置。用 `/mode` 查看各币安账户的生效值。

DeepSeek 可设置 `ai.reasoning_effort` 为 `low`，减少思考耗时；`null` 使用服务商默认值，官方 DeepSeek 默认为 `high`。该参数直接传给 API，无需给模型名添加 `-fast`。程序记录请求开始、响应耗时和失败原因。

AI 的 `ai.position_limits_enabled` 设为 `false` 时，开仓金额和新仓杠杆由模型判断，取消额外的单笔、单标的、总敞口、保证金占用及杠杆上限。程序读取币安合约档位，按实际可用保证金和交易规则执行；已有仓位加仓沿用当前合约杠杆，价格保护继续生效。

`exit_policy` 设为 `leader_only` 时，币安跟单和本地模拟只跟随带单员减仓、全平，不按自身浮亏或日亏损主动退出。设为 `protected` 则额外启用 `position_stop_fraction` 和 `daily_loss_fraction`。用 `/mode` 查看当前退出策略；币安强平不受该配置控制。

固定策略仍使用 `order_notional_fraction` 等限额：单笔 15%、单标的 60%、全账户 300%、保证金 40%。将 `ai.position_limits_enabled` 设为 `true` 可让 AI 同样使用这些限额。Demo 的 `execution.demo.budget_usdt` 设为 `null` 时按账户实际权益计算。

AI 可读取按交易员 ID 缓存的历史仓位先验，包括历史杠杆、首仓和加仓占该轮峰值数量的比例。先验缺失或超过 7 天时不使用，历史数据不会替代现有仓位限制。采集并写入正在使用的数据库：

```sh
python -m followtrade.prior <portfolioId> --database /path/to/state.sqlite3 --raw public-evidence.json --output sizing-prior.json
```

只有数量能够完整对齐的已平仓轮次参与首仓和加仓统计。缺少同期权益时，账户仓位比例保持未知。
可通过 `--assumed-capital-usdt 3000000` 额外按用户估计的资金基数计算仓位和单笔成交比例；AI 会将其作为假设情景读取，与币安实测字段分开标注。

## 测试

```sh
python -m unittest discover -s tests
```

`compare_reasoning.py --cases CASES.json --output RESULTS.json` 可对固定输入比较 `low`、`high`，每档重复两次。只使用 `FOLLOWTRADE_AI_KEY` 调用模型，不提交交易。
