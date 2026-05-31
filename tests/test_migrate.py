"""§3.7 — verify ``alembic upgrade head`` is idempotent and seeds the default owner."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head


@pytest.mark.asyncio
async def test_upgrade_head_seeds_default_owner_and_is_idempotent(
    tmp_sqlite_url: str,
) -> None:
    # upgrade_head is sync and spins its own asyncio.run inside Alembic env.py,
    # so we offload it to a worker thread to avoid nested loops.
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)

    engine = create_engine(tmp_sqlite_url)
    try:
        async with engine.connect() as conn:
            rows = (await conn.execute(text("SELECT id, name FROM owners"))).all()
        assert rows == [(1, "self")]

        await asyncio.to_thread(upgrade_head, tmp_sqlite_url)

        async with engine.connect() as conn:
            rows = (await conn.execute(text("SELECT id, name FROM owners"))).all()
        assert rows == [(1, "self")], "second upgrade must not duplicate the seed row"
    finally:
        await engine.dispose()
