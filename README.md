# AI Follow Trade

监控币安合约带单员的公开成交，用实时报价同时维护固定策略和 AI 策略的模拟账户。默认组合为熬鹰资本 `5075281354358777856`。程序使用 Python 3.9+ 标准库、SQLite 和按需调用的远程模型，适合单进程部署在 2 核 1 GB 的 Linux VPS。执行路径为模拟账本。

## 本机运行

```powershell
Copy-Item .\config.example.json .\config.json
python -m followtrade --config .\config.json --once
python -m followtrade --config .\config.json
```

`--once` 检查公开订单和正式行情，一项失败就返回非零。正常运行后按 Ctrl+C 停止。启动时默认只开固定策略模拟账户，初始预算为 1,000 USDT。已有数据库会继续复用原模拟余额，修改 `starting_usdt` 只影响新账户。对同一数据库只运行一个机器人实例。用另一份配置和数据库可以开始一轮独立模拟。

设置 `portfolio_id`、分配预算和各项敞口上限后，按允许的请求频率设置 `poll_seconds`。订单源初次成功采集只建立基线。之后按重叠时间窗采集，窗口结果超过 50 条会分段补齐。长时间中断超出回补窗口会记录缺口，暂停新增敞口并继续处理可归属的退出。检查缺口后，可用新数据库从空仓重建模拟基线，保留原库用于审计。访问拦截会停止信号源请求，需要先检查 VPS 出口和服务可用性。

订单首次观测时间、跳过原因和两套模拟成交保存在 SQLite。入场使用买卖一档加设定滑点，按订单更新时间判断 5 分钟新鲜度，超时则跳过。实际发布延迟和一档可成交量需要另行验证。全平以带单员的仓位历史核对，减仓比例按可见成交估计。止损与日亏保护按本机可读报价轮询执行。模拟包含显式手续费及滑点，资金费、历史合约过滤器、盘口深度、逐仓强平和真实成交另有差异。

## 启用 AI

在本机配置中设置 `ai.enabled=true`、`ai.base_url`、`ai.model` 和 `ai.trader_profile`。`base_url` 为支持 Chat Completions 的 HTTPS API 根路径，通常以 `/v1` 结尾。通过进程环境变量 `FOLLOWTRADE_AI_KEY` 提供模型密钥。

对于本项目的 VPS 部署，从本机终端运行 `ssh -t USER@HOST 'python3 -B ~/ai-follow-trade/deploy/configure-ai.py'`。按提示填写服务商的 Base URL、模型 ID 和 API Key。密钥输入不回显。脚本先用虚构信号验证模型响应，再将密钥保存到仅服务用户可读的 `~/.config/ai-follow-trade/secrets.env`，启用 AI 账户并重启 `ai-follow-trade.service`。

模型收到带单员画像、已观测成交、报价、执行账户权益、敞口和限制，输出跟单或跳过、目标名义仓位占比、建议杠杆和简短理由。`ai.timeout_seconds` 默认 60 秒，可设为 1–180 秒，入场仍受信号有效期 `max_signal_age_seconds` 限制。程序限制单合约与全账户的名义敞口、占用保证金和最大杠杆，已有同合约仓位使用原杠杆。模型错误或超时会跳过该次 AI 入场，固定策略和减平仓继续运行。绑定 Telegram 后，Bot 会发送 AI 建议、耗时或超时提示，成交状态以委托回报为准。

## Telegram

通过 BotFather 创建 Bot，在 Telegram 私聊里先向 Bot 发一条消息。用 Bot API 的 `getUpdates` 查看这条消息的 `message.chat.id`，将正整数填入 `telegram.chat_id`。通过进程环境变量 `FOLLOWTRADE_TELEGRAM_TOKEN` 提供 Bot Token。同一个 Bot 的更新由一个实例读取。开启 `telegram.push_fills` 后会推送监控启动后首次检测到的带单员成交，以及各模拟账户的新成交。带单员成交按公开订单记录去重，即使策略跳过也会通知。设置为 `false` 可仅查询。

| 指令 | 内容 |
|---|---|
| `/func`、`/help`、`/start` | 查看完整的 Bot 指令与用途 |
| `/status` | 信号源、行情和模拟账户权益 |
| `/positions` | 模拟仓位及按已核对成交记录的币安仓位 |
| `/trades` | 最近模拟成交 |
| `/orders` | 最近币安委托及币安回报 |
| `/mode` | 当前新开仓去向和已配置的币安账户 |
| `/demo` | 新开仓交由币安 Demo 账户 |
| `/live` | 新开仓交由币安正式账户，使用真实资金 |
| `/paper` | 新开仓只在本地模拟，币安已有仓位继续按规则退出 |

Telegram 仅接受配置好的私聊，连接失败会独立退避。推送在网络异常重试时可能重复。

## 币安账户连接

默认执行模式是本地模拟。在 VPS 上分别运行下面的录入命令，输入专用的 HMAC Key、Secret 和执行策略。Demo 录入时还需填写模拟预算。正式账户以专用子账户 U 本位合约钱包的 USDT 权益计算仓位。请使用空仓、无未成交委托的 U 本位合约账户，并设置单向持仓和单资产保证金模式。脚本先读取账户并调用币安测试下单接口，然后保存密钥到仓库外的服务环境文件。绑定后，在 Bot 私聊中选择执行账户。

```powershell
ssh -t USER@HOST 'python3 -B ~/ai-follow-trade/deploy/configure-binance.py demo'
ssh -t USER@HOST 'python3 -B ~/ai-follow-trade/deploy/configure-binance.py mainnet'
```

币安账户分别记录仓位，并各自执行录入时选择的 `fixed` 或 `ai` 策略。正式账户的开仓金额仍受可用保证金、仓位比例和杠杆上限约束。本地两套模拟继续记账。Telegram 切换适用于切换后产生的新入场信号。已发出的委托继续核对，已有仓位留在原账户处理减仓、全平和止损。币安入场使用 IOC 限价单，退出使用只减仓市价单。请求结果不确定时用客户订单编号核对，暂停该账户的新委托。止损由 Bot 根据币安账户快照触发，需要进程与币安接口可用。

也可以使用临时环境变量 `FOLLOWTRADE_BINANCE_KEY` 与 `FOLLOWTRADE_BINANCE_SECRET` 做只读检查。

```powershell
python -m followtrade --check-account demo
python -m followtrade --check-account mainnet
```

账户检查只查询 `GET /fapi/v3/account`，模拟预算使用配置中的 USDT。订单信号、AI 决策和 Telegram 查询均无需币安账户密钥。

VPS 部署可以用 `systemd` 管理一个 Python 进程，将配置、数据库和密钥放在仓库之外，目录赋予服务用户写权限。Windows 可直接运行相同模块。Git 忽略本机配置、密钥文件及账本数据。

```powershell
python -m unittest discover -s tests -v
```
