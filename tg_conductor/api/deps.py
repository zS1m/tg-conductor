"""FastAPI dependencies shared across routes.

Two pieces of state are injected: the per-request owner id and the async
session factory. Both come from ``app.state`` so that :func:`create_app`
can wire them at construction time without globals.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import Request

from tg_conductor.runs.event_bus import EventBus


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    """Return the app-scoped async session factory.

    Stored on ``app.state.session_factory`` by :func:`create_app`.
    """
    return request.app.state.session_factory  # type: ignore[no-any-return]


def get_owner_id(request: Request) -> int:
    """Resolve the active owner id for this request.

    v1 returns the configured ``settings.default_owner_id`` (single-tenant
    self-hosted deployment). v2 will parse a bearer token / session cookie.
    """
    return request.app.state.default_owner_id  # type: ignore[no-any-return]


def get_event_bus(request: Request) -> EventBus:
    """Return the app-scoped event bus (publish + subscribe)."""
    return request.app.state.event_bus  # type: ignore[no-any-return]
