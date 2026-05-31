"""Async SQLAlchemy engine factory.

For SQLite the engine enables WAL journaling, NORMAL synchronous mode, and
foreign-key enforcement via a ``connect`` event listener — these pragmas
must be re-applied on every new connection.
"""

from __future__ import annotations

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tg_conductor.config.settings import get_settings


def create_engine(database_url: str | None = None) -> AsyncEngine:
    url = database_url or get_settings().database_url
    engine = create_async_engine(url, future=True)
    install_sqlite_pragmas(engine)
    return engine


def install_sqlite_pragmas(engine: AsyncEngine) -> None:
    """Attach a ``connect`` listener that sets WAL/NORMAL/FK pragmas.

    No-op for non-SQLite dialects. Safe to call on engines constructed
    elsewhere (e.g. Alembic's own engine in ``alembic_migrations/env.py``).
    """
    if engine.dialect.name != "sqlite":
        return

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_conn, _connection_record):  # type: ignore[no-untyped-def]
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
        finally:
            cursor.close()
