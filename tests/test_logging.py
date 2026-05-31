"""§15 — structlog JSON output + request_id binding + ERROR traceback + startup.

Spec scenarios covered:

* §15.1 ``configure_logging`` produces JSON-shaped records on stdout with
  ``ts`` / ``level`` / ``event`` populated and stdlib loggers bridged
  through the same renderer.
* §15.2 the request_id middleware binds onto ``structlog.contextvars`` so
  any module emitting a log during the request automatically carries
  ``request_id=...``, and clears the var on the way out.
* §15.3 ERROR-level logs include the traceback when ``exc_info=True``.
* §15.4 ``log_startup_summary`` masks secrets — ``APP_MASTER_KEY``,
  ``OPENAI_API_KEY`` plaintexts and DB-URL credentials never appear.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from io import StringIO
from typing import Any

import pytest
import structlog
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.api import create_app
from tg_conductor.config.settings import get_settings
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.logging import (
    bind_request_id,
    clear_request_id,
    configure_logging,
    get_logger,
    log_startup_summary,
)


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
def _isolate_structlog() -> None:
    """structlog config + contextvars survive across tests by design.

    Reset both before each test so order-of-execution doesn't matter.
    """
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def _capture_json_lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


# ----------------------------------------------------------- §15.1 JSON shape


def test_configure_logging_emits_iso_ts_level_event(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level=logging.INFO)
    log = get_logger("test")
    log.info("hello.world", item="x")
    lines = _capture_json_lines(capsys)
    assert len(lines) == 1
    rec = lines[0]
    assert rec["event"] == "hello.world"
    assert rec["level"] == "info"
    assert rec["item"] == "x"
    assert rec["ts"].endswith("Z") or "+00:00" in rec["ts"]


def test_stdlib_logger_is_bridged_into_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level=logging.INFO)
    logging.getLogger("tg_conductor.test").warning("stdlib.bridge value=%s", 42)
    lines = _capture_json_lines(capsys)
    assert any(rec.get("event", "").startswith("stdlib.bridge") for rec in lines)
    rec = next(r for r in lines if r.get("event", "").startswith("stdlib.bridge"))
    assert rec["level"] == "warning"
    assert rec["logger"] == "tg_conductor.test"


# ----------------------------------------------------------- §15.2 request_id


def test_bind_request_id_appears_in_subsequent_logs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level=logging.INFO)
    bind_request_id("req-abc")
    try:
        get_logger("test").info("inside.request")
    finally:
        clear_request_id()
    get_logger("test").info("outside.request")
    lines = _capture_json_lines(capsys)
    inside = next(r for r in lines if r["event"] == "inside.request")
    outside = next(r for r in lines if r["event"] == "outside.request")
    assert inside["request_id"] == "req-abc"
    assert "request_id" not in outside


@pytest.mark.asyncio
async def test_middleware_binds_request_id_during_handler(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An in-handler log emitted while serving a request carries request_id."""
    configure_logging(level=logging.INFO)
    app = create_app(session_factory=session_factory, default_owner_id=1)

    @app.get("/_log_check")
    async def _log_check() -> dict[str, str]:
        get_logger("test.handler").info("handler.fired")
        return {"ok": "true"}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/_log_check", headers={"X-Request-ID": "trace-from-test"}
        )

    assert resp.status_code == 200
    lines = _capture_json_lines(capsys)
    fired = next(r for r in lines if r.get("event") == "handler.fired")
    assert fired["request_id"] == "trace-from-test"


# ----------------------------------------------------------- §15.3 traceback


def test_error_with_exc_info_includes_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level=logging.INFO)
    log = get_logger("test")
    try:
        raise RuntimeError("explode here")
    except RuntimeError:
        log.error("boom.happened", exc_info=True)
    lines = _capture_json_lines(capsys)
    rec = next(r for r in lines if r["event"] == "boom.happened")
    assert "exception" in rec
    assert "RuntimeError: explode here" in rec["exception"]
    assert "Traceback" in rec["exception"]


def test_stdlib_logger_exception_includes_traceback(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level=logging.INFO)
    stdlib_log = logging.getLogger("tg_conductor.bridge_exc")
    try:
        raise ValueError("std-bridge")
    except ValueError:
        stdlib_log.exception("bridged.error")
    lines = _capture_json_lines(capsys)
    rec = next(r for r in lines if r.get("event") == "bridged.error")
    assert "exception" in rec
    assert "ValueError: std-bridge" in rec["exception"]


# ----------------------------------------------------------- §15.4 startup


def test_log_startup_summary_emits_event_with_counts_and_masks_secrets(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    master_key: str,  # noqa: ARG001 — sets APP_MASTER_KEY env so Settings loads
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-very-secret-DO-NOT-LEAK")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://alice:SECRETPW@db.internal:5432/tg",
    )
    monkeypatch.setenv("TG_PROXY", "socks5://user:hunter2@proxy:1080")
    settings = get_settings()
    configure_logging(level=logging.INFO)

    log_startup_summary(
        settings=settings,
        version="0.1.0",
        accounts_total=3,
        workflows_total=5,
        workflow_dir=str(settings.workflow_dir),
    )

    lines = _capture_json_lines(capsys)
    rec = next(r for r in lines if r["event"] == "startup.summary")
    assert rec["version"] == "0.1.0"
    assert rec["accounts_total"] == 3
    assert rec["workflows_total"] == 5
    assert rec["has_openai_api_key"] is True
    assert rec["has_app_master_key"] is True

    # Secrets MUST NOT leak.
    serialized = json.dumps(rec)
    assert "sk-very-secret" not in serialized
    assert "SECRETPW" not in serialized
    assert "hunter2" not in serialized
    # But the credential-masked DB URL / proxy DOES show up.
    assert "db.internal:5432" in rec["database_url"]
    assert rec["tg_proxy"] is not None and "proxy:1080" in rec["tg_proxy"]


# ----------------------------------------------------------- structlog reset side-channel


def test_filter_level_drops_debug_when_configured_at_info(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level=logging.INFO)
    log = get_logger("test")
    log.debug("should.not.appear")
    log.info("should.appear")
    lines = _capture_json_lines(capsys)
    events = [r["event"] for r in lines]
    assert "should.appear" in events
    assert "should.not.appear" not in events


# ----------------------------------------------------------- helper


def _swallow_root_handlers() -> None:
    """Drop root handlers between tests so capsys captures cleanly."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)


@pytest.fixture(autouse=True)
def _reset_stdlib_handlers() -> None:
    # Tests configure stdlib root handlers; clean up afterwards so
    # unrelated test runs don't double-emit.
    yield
    _swallow_root_handlers()


@pytest.fixture
def _silence_stdout(capsys: pytest.CaptureFixture[str]) -> StringIO:
    # Helper for tests that want to assert on raw bytes; unused but kept
    # for future use.
    return capsys  # type: ignore[return-value]
