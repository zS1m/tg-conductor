"""Application settings loaded from environment variables.

Validation contract:
- ``APP_MASTER_KEY``: required, base64-encoded raw 32 bytes (AES-256). Both
  standard and url-safe alphabets are accepted. Missing / invalid → fail-fast
  at :func:`load_settings`.
- ``OPENAI_API_KEY``: optional at load. Missing only emits a WARNING; AI
  actions fail-fast at execution time (see ai-usage spec).
"""

from __future__ import annotations

import base64
import binascii
import logging
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Annotated, Any
from zoneinfo import ZoneInfo

from pydantic import Field, SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

log = logging.getLogger(__name__)


def _decode_master_key(raw: str) -> bytes:
    """Decode a base64 string accepting both standard and url-safe alphabets."""
    normalized = raw.strip().replace("-", "+").replace("_", "/")
    return base64.b64decode(normalized, validate=True)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_master_key: SecretStr = Field(
        description="Base64-encoded 32-byte AES-256 master key",
    )

    database_url: str = "sqlite+aiosqlite:///data/tg-conductor.sqlite3"

    workflow_dir: Path = Path("workflows")

    openai_api_key: SecretStr | None = None
    openai_base_url: str | None = None
    openai_model_chat: str = "gpt-4o-mini"
    openai_model_vision: str = "gpt-4o"
    # Proxy for OpenAI HTTP calls specifically. Unlike TG_PROXY (Telegram
    # only), this routes the OpenAI SDK's httpx client. Empty / unset →
    # httpx falls back to system env (ALL_PROXY / HTTPS_PROXY). Set e.g.
    # socks5://127.0.0.1:7890 to force a proxy regardless of env.
    openai_proxy: str | None = None

    bind_host: str = "127.0.0.1"
    bind_port: int = 8765
    # ``NoDecode`` opts this field out of pydantic-settings's automatic
    # JSON parsing on env values — we want the raw string so the
    # ``_split_cors_origins`` validator below can accept "" / "a,b" /
    # '["a","b"]' alike.
    cors_origins: Annotated[list[str], NoDecode] = Field(default_factory=list)

    default_owner_id: int = 1

    tg_proxy: str | None = None
    tg_min_interval_seconds: float = 1.0
    tg_floodwait_max_retries: int = 3
    tg_floodwait_padding_seconds: float = 1.5
    tg_reconnect_initial_seconds: float = 1.0
    tg_reconnect_max_seconds: float = 300.0

    job_default_timeout_seconds: float = 300.0

    # IANA timezone name used to interpret cron expressions and time_window
    # triggers (e.g. "Asia/Shanghai"). Default "UTC" preserves the historical
    # behavior; set SCHEDULER_TZ=Asia/Shanghai to make "0 9 * * *" mean 09:00
    # 北京时间. fire_at is always stored in UTC regardless. Validated against
    # the system tz database at load time.
    scheduler_tz: str = "UTC"

    # spec runs §"事件保留与清理": run_events older than ``run_events_ttl_days``
    # are purged by the background cleaner. ``0`` or ``-1`` disables purging
    # (events kept forever). The cleaner wakes every
    # ``run_events_cleanup_interval_seconds``; the runs row is never deleted
    # — only its events.
    run_events_ttl_days: int = 30
    run_events_cleanup_interval_seconds: int = 3600

    # Empty-string-to-None: pydantic-settings keeps env vars verbatim, so
    # writing ``TG_PROXY=`` in ``.env`` yields ``""`` not ``None``. That
    # tripped real users at first deploy — every "optional, leave empty"
    # field below collapses ``""`` to ``None`` here so the rest of the
    # codebase only has to handle the proper ``None``.
    @field_validator(
        "openai_api_key",
        "openai_base_url",
        "openai_proxy",
        "tg_proxy",
        mode="before",
    )
    @classmethod
    def _empty_str_to_none(cls, v: Any) -> Any:
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    # ``CORS_ORIGINS`` in ``.env`` is most natural as a comma-separated
    # list (``CORS_ORIGINS=https://a.example,https://b.example``).
    # pydantic-settings's default for ``list[str]`` tries JSON-decode
    # first, which blows up on both ``""`` and ``a,b``. Normalize early:
    # accept empty string → []; comma list → split; JSON-shaped string
    # → let the default machinery handle it.
    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_cors_origins(cls, v: Any) -> Any:
        if v is None:
            return []
        if isinstance(v, str):
            stripped = v.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                import json

                return json.loads(stripped)  # JSON-shaped list
            return [piece.strip() for piece in stripped.split(",") if piece.strip()]
        return v

    @field_validator("scheduler_tz")
    @classmethod
    def _validate_scheduler_tz(cls, v: str) -> str:
        from zoneinfo import ZoneInfoNotFoundError

        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, ValueError) as e:
            raise ValueError(
                f"unknown timezone {v!r} (use an IANA name like 'Asia/Shanghai')"
            ) from e
        return v

    @field_validator("app_master_key")
    @classmethod
    def _validate_master_key(cls, v: SecretStr) -> SecretStr:
        raw = v.get_secret_value()
        if not raw:
            raise ValueError("must not be empty")
        try:
            decoded = _decode_master_key(raw)
        except (binascii.Error, ValueError) as e:
            raise ValueError(f"must be valid base64 ({e})") from e
        if len(decoded) != 32:
            raise ValueError(
                f"must decode to exactly 32 bytes, got {len(decoded)}",
            )
        return v

    @cached_property
    def master_key_bytes(self) -> bytes:
        return _decode_master_key(self.app_master_key.get_secret_value())

    @cached_property
    def scheduler_tzinfo(self) -> ZoneInfo:
        """The validated ``scheduler_tz`` as a ``ZoneInfo`` (cron / time_window)."""
        return ZoneInfo(self.scheduler_tz)


_GENERATE_HINT = (
    "Generate one with: "
    'uv run python -c "import secrets, base64; '
    'print(base64.b64encode(secrets.token_bytes(32)).decode())"'
)


def load_settings() -> Settings:
    """Load settings, exiting the process with a clear message on failure."""
    try:
        s = Settings()
    except ValidationError as e:
        for err in e.errors():
            if err.get("loc") == ("app_master_key",):
                raise SystemExit(
                    f"FATAL: APP_MASTER_KEY {err['msg']}\n{_GENERATE_HINT}",
                ) from e
        raise SystemExit(f"FATAL: invalid settings\n{e}") from e

    if s.openai_api_key is None:
        log.warning(
            "OPENAI_API_KEY is not set; workflows using AI actions will fail "
            "fast at execution time",
        )
    return s


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide singleton ``Settings`` instance."""
    return load_settings()
