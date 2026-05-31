"""Alembic environment configured to read the DB URL from tg-conductor settings.

Importing ``tg_conductor.db.models`` (and future model modules) registers
their tables on ``SQLModel.metadata``, which Alembic uses as the autogenerate
target. New capability tables must be imported here before their first
migration is generated.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlmodel import SQLModel

# Side-effect import: ``tg_conductor.db`` re-exports every SQLModel
# table module so the metadata graph is fully populated before
# Alembic's autogenerate runs. See ``tg_conductor/db/__init__.py``.
import tg_conductor.db  # noqa: F401

config = context.config

if config.config_file_name is not None:
    # disable_existing_loggers=False keeps already-created application
    # loggers (tg_conductor.*) functional. The default (True) wipes
    # them, which silently breaks any code that called
    # ``logging.getLogger(...)`` at import time — and in tests, makes
    # every later log assertion fail.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = SQLModel.metadata


def _resolve_url() -> str:
    """Prefer the URL injected programmatically; fall back to settings."""
    url = config.get_main_option("sqlalchemy.url")
    if url:  # set by programmatic injection or alembic.ini override
        return url
    from tg_conductor.config.settings import get_settings

    return get_settings().database_url


def run_migrations_offline() -> None:
    context.configure(
        url=_resolve_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _resolve_url()
    connectable = async_engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    from tg_conductor.db.engine import install_sqlite_pragmas

    install_sqlite_pragmas(connectable)
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
