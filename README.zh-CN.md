# tg-conductor

[English](README.md) | **简体中文**

> 自托管的 Telegram 自动化。一份 YAML **workflow** 把*触发器*映射到*动作计划*；
> 服务在你的账号上执行、自动重试限流、并通过 HTTP + SSE 实时输出每次运行。

```
触发器 ──► 动作计划 ──► 你的 Telegram 账号
(cron / 时间窗 /  (发送 / 转发 /          │
 消息匹配)         ai_reply / …)          └─► 运行事件 ─► HTTP + SSE
```

## 特性

- **触发器：** `cron`、`time_window`（在每日窗口内随机分布 N 次、带最小间隔）、`message_match`（正则 / 发送者 / 话题）、`startup`。
- **动作计划：** 多步骤 + 步间延迟；文案池随机抽样；命名 variants 轮转/随机，让重复运行不机械。
- **AI 动作：** 通过任意 OpenAI 兼容端点做回复 / 图片理解，每次调用计量。
- **限流：** 按账号串行 + 最小间隔 + FloodWait 自动重试。
- **可观测：** 结构化运行事件、可查历史、可用 `?since=` 续传的实时 SSE 流。
- **密钥加密：** session 字符串仅以 AES-GCM 密文存储，密钥由你掌控。
- **热重载：** workflow 即 YAML，一次 HTTP 调用或 SIGHUP 即重载，无需重启。

## 快速开始

需要 Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)。

```sh
git clone https://github.com/zS1m/tg-conductor.git && cd tg-conductor
uv sync

cp .env.example .env
uv run python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
#  └─ 粘贴到 .env 的  APP_MASTER_KEY=<value>   （唯一必填项）

uv run tg-conductor migrate
uv run tg-conductor account login --owner 1 --label main   # 交互输入：api_id、api_hash、phone、code
uv run tg-conductor serve
```

`api_id` / `api_hash` 在 [my.telegram.org](https://my.telegram.org) →
*API development tools* 创建应用获取（标识*应用*，非你的账号）。

放入一个 workflow 并重载：

```yaml
# workflows/daily-checkin.yaml
name: daily-checkin
account_id: 1                 # 来自 `tg-conductor account list`
trigger:
  type: cron
  expression: "30 9 * * *"    # 5 段 cron，按 SCHEDULER_TZ 解释
action_plan:
  steps:
    - action: send_text
      chat_id: -1000000000000 # ← 你的群 id（负数）或用户 id
      text: "早安"
```

```sh
curl -X POST http://127.0.0.1:8765/reload          # 重载 workflow
curl -s     http://127.0.0.1:8765/healthz          # 健康检查
curl -N     http://127.0.0.1:8765/runs/<id>/stream # 实时运行事件
```

### 用 Docker 运行

GHCR 上有预构建的多架构(amd64/arm64)镜像:

```sh
docker run -d --name tg-conductor \
  -e APP_MASTER_KEY="$(python3 -c 'import secrets,base64;print(base64.b64encode(secrets.token_bytes(32)).decode())')" \
  -e SCHEDULER_TZ=Asia/Shanghai \
  -v "$PWD/data:/app/data" -v "$PWD/workflows:/app/workflows:ro" \
  -p 127.0.0.1:8765:8765 \
  ghcr.io/zs1m/tg-conductor:latest

docker exec -it tg-conductor tg-conductor account login --owner 1 --label main
docker restart tg-conductor   # 让服务连接账号(账号在启动时连接)
```

或使用 [`docker-compose.example.yml`](docker-compose.example.yml)。

## 配置

通过环境变量 / `.env` 设置，仅 `APP_MASTER_KEY` 必填。

| 变量 | 默认 | 用途 |
| --- | --- | --- |
| `APP_MASTER_KEY` | —（必填） | base64 的 32 字节；加密 session 字符串 |
| `OPENAI_API_KEY` | 未设 | 仅 AI 动作需要 |
| `BIND_HOST` / `BIND_PORT` | `127.0.0.1` / `8765` | HTTP 绑定（Docker 用 `0.0.0.0`） |
| `SCHEDULER_TZ` | `UTC` | cron / time_window 时区 —— **设成你的时区(如 `Asia/Shanghai`),否则按 UTC 触发** |
| `WORKFLOW_DIR` | `workflows` | workflow YAML 目录 |

全部选项见 [`.env.example`](.env.example)。

## 须知

- **密钥：** session 字符串仅为 AES-GCM 密文，由 `APP_MASTER_KEY` 加密——丢失密钥则无法恢复。切勿提交 `.env`、`*.session`、`data/`、`workflows/`（`.gitignore` 已覆盖）。
- **合规使用：** 本工具通过 Telegram 客户端 API 自动化*真人账号*，可能违反 Telegram ToS 并使账号面临风险。请仅用于自己的正当自动化、遵守频率限制、勿用于群发。后果自负。
- **状态：** 早期但可用，引擎有测试覆盖；1.0 前 API/schema 可能变动。

## 许可证

[Apache-2.0](LICENSE)。
