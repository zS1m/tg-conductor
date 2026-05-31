# tg-conductor

**English** | [简体中文](README.zh-CN.md)

> Self-hosted Telegram automation. A YAML **workflow** maps a *trigger* to an
> *action plan*; the service runs them on your account, retries through rate
> limits, and streams every run over HTTP + SSE.

```
trigger ──► action plan ──► your Telegram account
(cron / window /   (send / forward /        │
 message match)     ai_reply / …)           └─► run events ─► HTTP + SSE
```

## Features

- **Triggers:** `cron`, `time_window` (N runs spread randomly across a daily window with a min gap), `message_match` (regex / sender / topic), `startup`.
- **Action plans:** multi-step with per-step delays; text pools with random sampling; named variants picked round-robin/random so runs aren't mechanical.
- **AI actions:** reply / image understanding via any OpenAI-compatible endpoint, with per-call usage metering.
- **Rate limiting:** per-account throttle + minimum interval + automatic FloodWait retry.
- **Observable:** structured run events, queryable history, live SSE stream resumable via `?since=`.
- **Encrypted secrets:** session strings stored only as AES-GCM ciphertext under a key you control.
- **Hot reload:** workflows are YAML; reload via one HTTP call or SIGHUP, no restart.

## Quickstart

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```sh
git clone https://github.com/zS1m/tg-conductor.git && cd tg-conductor
uv sync

cp .env.example .env
uv run python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
#  └─ paste into .env as  APP_MASTER_KEY=<value>   (the only required setting)

uv run tg-conductor migrate
uv run tg-conductor account login --owner 1 --label main   # prompts: api_id, api_hash, phone, code
uv run tg-conductor serve
```

Get `api_id` / `api_hash` at [my.telegram.org](https://my.telegram.org) → *API
development tools* (they identify the app, not your account).

Add a workflow and reload:

```yaml
# workflows/daily-checkin.yaml
name: daily-checkin
account_id: 1                 # id from `tg-conductor account list`
trigger:
  type: cron
  expression: "30 9 * * *"    # 5-field cron, in SCHEDULER_TZ
action_plan:
  steps:
    - action: send_text
      chat_id: -1000000000000 # ← your group id (negative) or user id
      text: "good morning"
```

```sh
curl -X POST http://127.0.0.1:8765/reload          # reload workflows
curl -s     http://127.0.0.1:8765/healthz          # health
curl -N     http://127.0.0.1:8765/runs/<id>/stream # live run events
```

### Run with Docker

A prebuilt multi-arch (amd64/arm64) image is published to GHCR:

```sh
docker run -d --name tg-conductor \
  -e APP_MASTER_KEY="$(python3 -c 'import secrets,base64;print(base64.b64encode(secrets.token_bytes(32)).decode())')" \
  -e SCHEDULER_TZ=Asia/Shanghai \
  -v "$PWD/data:/app/data" -v "$PWD/workflows:/app/workflows:ro" \
  -p 127.0.0.1:8765:8765 \
  ghcr.io/zs1m/tg-conductor:latest

docker exec -it tg-conductor tg-conductor account login --owner 1 --label main
docker restart tg-conductor   # connect the account (it connects at startup)
```

Or use [`docker-compose.example.yml`](docker-compose.example.yml).

## Configuration

Set via environment / `.env`. Only `APP_MASTER_KEY` is required.

| Variable | Default | Purpose |
| --- | --- | --- |
| `APP_MASTER_KEY` | — (required) | base64 32 bytes; encrypts session strings |
| `OPENAI_API_KEY` | unset | only for AI actions |
| `BIND_HOST` / `BIND_PORT` | `127.0.0.1` / `8765` | HTTP bind (`0.0.0.0` in Docker) |
| `SCHEDULER_TZ` | `UTC` | cron / time_window timezone — **set to your zone (e.g. `Asia/Shanghai`) or schedules run in UTC** |
| `WORKFLOW_DIR` | `workflows` | workflow YAML location |

All options are documented in [`.env.example`](.env.example).

## Notes

- **Secrets:** session strings are only ever AES-GCM ciphertext keyed by
  `APP_MASTER_KEY` — lose the key and sessions are unrecoverable. Never commit
  `.env`, `*.session`, `data/`, or `workflows/` (covered by `.gitignore`).
- **Responsible use:** this automates a *real user account* via Telegram's client
  API, which can violate Telegram's ToS and risk the account. Use it for your own
  legitimate automation, respect the limits, no spam. You are responsible.
- **Status:** early but functional; engine is tested. APIs/schema may change
  before 1.0.

## License

[Apache-2.0](LICENSE).
