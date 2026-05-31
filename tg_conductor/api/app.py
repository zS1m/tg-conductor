"""FastAPI application factory.

``create_app`` returns a :class:`fastapi.FastAPI` instance wired to a
caller-supplied :class:`AsyncSession` factory. State lives on
``app.state`` rather than module globals so tests can spin up disposable
apps against a per-test SQLite DB.

§12 lands ``GET /runs/{id}/events`` only. §14 will add the rest
(``/accounts``, ``/workflows``, ``/runs``, ``/reload``, ``/healthz``…),
plus middlewares (request id, CORS, error envelope) and lifespan-managed
state (event bus, dispatcher, reloader).
"""

from __future__ import annotations

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.api.middleware import install_middlewares
from tg_conductor.api.routes.accounts import router as accounts_router
from tg_conductor.api.routes.admin import router as admin_router
from tg_conductor.api.routes.healthz import router as healthz_router
from tg_conductor.api.routes.jobs import router as jobs_router
from tg_conductor.api.routes.runs import router as runs_router
from tg_conductor.api.routes.usage import router as usage_router
from tg_conductor.api.routes.workflows import router as workflows_router
from tg_conductor.config.settings import get_settings
from tg_conductor.config_loader.reload import Reloader
from tg_conductor.runs.event_bus import EventBus, InMemoryEventBus


def create_app(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    event_bus: EventBus | None = None,
    default_owner_id: int | None = None,
    sse_keepalive_seconds: float = 15.0,
    cors_origins: list[str] | None = None,
    reloader: Reloader | None = None,
) -> FastAPI:
    """Build a FastAPI app bound to ``session_factory``.

    ``event_bus`` defaults to a fresh :class:`InMemoryEventBus`; the
    production lifespan (§16) will share one bus across the worker pool
    and the API.

    ``default_owner_id`` overrides ``settings.default_owner_id`` (handy
    in tests that want a non-default tenant); ``None`` uses the setting.

    ``sse_keepalive_seconds`` controls how often the SSE stream emits a
    keepalive comment when idle. Defaults to spec's 15 s; tests crank it
    down to keep wall-clock waits small.

    ``cors_origins`` overrides ``settings.cors_origins`` (empty list
    means CORS is effectively disabled — browser-side cross-origin
    reads are refused).
    """
    settings = get_settings()
    app = FastAPI(title="tg-conductor", version="0.1.0")
    app.state.session_factory = session_factory
    app.state.event_bus = event_bus or InMemoryEventBus()
    app.state.default_owner_id = (
        default_owner_id if default_owner_id is not None else settings.default_owner_id
    )
    app.state.sse_keepalive_seconds = sse_keepalive_seconds
    app.state.reloader = reloader

    install_middlewares(
        app,
        cors_origins=(
            cors_origins if cors_origins is not None else settings.cors_origins
        ),
    )

    app.include_router(accounts_router)
    app.include_router(workflows_router)
    app.include_router(jobs_router)
    app.include_router(runs_router)
    app.include_router(usage_router)
    app.include_router(healthz_router)
    app.include_router(admin_router)
    return app
