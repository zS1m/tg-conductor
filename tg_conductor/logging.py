"""structlog configuration — JSON to stdout + stdlib bridge + contextvars.

spec §15 wires:

* §15.1 ``configure_logging`` — one JSON line per record on stdout, with
  ``ts`` / ``level`` / ``event`` always present. Existing stdlib loggers
  (``tg_conductor.*``, ``sqlalchemy``, ``uvicorn``, etc.) route through
  the same formatter so we have a single output format.
* §15.2 — :func:`bind_request_id` / :func:`clear_request_id` use
  ``structlog.contextvars`` so the request_id chosen by the HTTP
  middleware automatically appears on every log call made during the
  request, regardless of which module emits it.
* §15.3 — ``format_exc_info`` is the last user-facing processor, so any
  ``log.error("...", exc_info=True)`` or ``log.exception(...)`` produces
  a ``"exception": "<traceback>"`` field.
* :func:`log_startup_summary` (§15.4) emits one ``startup.summary``
  event with version, masked config, account / workflow counts.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    merge_contextvars,
    unbind_contextvars,
)

from tg_conductor.config.settings import Settings

__all__ = [
    "configure_logging",
    "bind_request_id",
    "clear_request_id",
    "bind_run_context",
    "log_startup_summary",
    "get_logger",
]


def configure_logging(*, level: int = logging.INFO) -> None:
    """Idempotent: configure structlog + the stdlib root logger to JSON stdout.

    Safe to call multiple times. Tests can re-invoke between cases without
    leaking handlers because we clear the root before reattaching.
    """
    # NB: ``structlog.stdlib.add_logger_name`` is only in the stdlib-bridge
    # processor chain — it pulls ``logger.name`` which exists on stdlib
    # loggers but not on ``structlog.PrintLogger``.
    processors: list[Any] = [
        merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Bridge stdlib logging through the same JSON renderer so SQLAlchemy /
    # uvicorn / alembic / our own module-level ``logging.getLogger(__name__)``
    # calls all emit the same JSON shape.
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
            foreign_pre_chain=[
                merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
                structlog.stdlib.add_logger_name,
                structlog.processors.format_exc_info,
            ],
        )
    )
    root = logging.getLogger()
    for old in list(root.handlers):
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str | None = None) -> Any:
    """Return a structlog BoundLogger; convenience wrapper for callers."""
    return structlog.get_logger(name) if name else structlog.get_logger()


# ---------------------------------------------------------- request scope


def bind_request_id(request_id: str) -> None:
    """Bind ``request_id`` onto the current contextvars scope.

    All subsequent ``structlog.get_logger().info(...)`` calls in this
    coroutine / task get a ``request_id="..."`` field for free.
    """
    bind_contextvars(request_id=request_id)


def clear_request_id() -> None:
    unbind_contextvars("request_id")


def bind_run_context(
    *,
    owner_id: int | None = None,
    run_id: int | None = None,
    workflow_id: int | None = None,
    account_id: int | None = None,
) -> None:
    """Bind per-Run identifiers for the active task (AccountWorker uses this)."""
    kwargs: dict[str, Any] = {}
    if owner_id is not None:
        kwargs["owner_id"] = owner_id
    if run_id is not None:
        kwargs["run_id"] = run_id
    if workflow_id is not None:
        kwargs["workflow_id"] = workflow_id
    if account_id is not None:
        kwargs["account_id"] = account_id
    if kwargs:
        bind_contextvars(**kwargs)


def clear_run_context() -> None:
    clear_contextvars()


# ---------------------------------------------------------- startup summary


def log_startup_summary(
    *,
    settings: Settings,
    version: str,
    accounts_total: int,
    workflows_total: int,
    workflow_dir: str,
) -> None:
    """Emit the §15.4 startup summary log line.

    The keys here are the contract — tooling / dashboards parse on
    ``event="startup.summary"``. Secrets (``APP_MASTER_KEY``,
    ``OPENAI_API_KEY``, ``session_string`` etc.) are intentionally never
    serialized; only their *presence* is hinted via boolean flags.
    """
    logger = get_logger("tg_conductor.startup")
    logger.info(
        "startup.summary",
        version=version,
        bind_host=settings.bind_host,
        bind_port=settings.bind_port,
        database_url=_mask_database_url(settings.database_url),
        workflow_dir=workflow_dir,
        default_owner_id=settings.default_owner_id,
        has_app_master_key=settings.app_master_key is not None,
        has_openai_api_key=settings.openai_api_key is not None,
        openai_model_chat=settings.openai_model_chat,
        openai_model_vision=settings.openai_model_vision,
        openai_base_url=settings.openai_base_url,
        tg_proxy=_mask_proxy(settings.tg_proxy),
        tg_min_interval_seconds=settings.tg_min_interval_seconds,
        job_default_timeout_seconds=settings.job_default_timeout_seconds,
        run_events_ttl_days=settings.run_events_ttl_days,
        scheduler_tz=settings.scheduler_tz,
        accounts_total=accounts_total,
        workflows_total=workflows_total,
        cors_origins=list(settings.cors_origins),
    )


def _mask_database_url(url: str) -> str:
    """Strip ``user:password@`` from a SQLAlchemy URL for safe logging."""
    if "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" not in rest:
        return url
    creds, host = rest.split("@", 1)
    if ":" in creds:
        return f"{scheme}://***:***@{host}"
    return f"{scheme}://***@{host}"


def _mask_proxy(proxy: str | None) -> str | None:
    """Same idea as the URL mask, but preserves the scheme + host."""
    if not proxy:
        return proxy
    return _mask_database_url(proxy)


# Unused-symbol guard for ruff (we re-export contextvars helpers).
_ = unbind_contextvars
