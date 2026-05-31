"""§13.6 / §13.10 — ``GET /usage`` aggregated query endpoint.

Spec scenarios covered:

* ``group_by=day``: returns one bucket per UTC date, summed integer
  ``units`` and ``cost_micros``.
* ``group_by=kind``: returns one bucket per ``kind`` (e.g.
  ``"openai.chat"`` vs ``"openai.chat.failed"``).
* ``from`` / ``to`` time-bound filtering.
* Cross-owner isolation: another tenant's rows never bleed in.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.ai import repo as usage_repo
from tg_conductor.api import create_app
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head


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


async def _seed(
    factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: int,
    items: list[tuple[datetime, str, int, int]],
) -> None:
    """``items = [(ts, kind, units, cost_micros), ...]``."""
    async with factory() as s, s.begin():
        for ts, kind, units, cost in items:
            await usage_repo.record_usage(
                s,
                owner_id=owner_id,
                kind=kind,
                units=units,
                cost_micros=cost,
                ts=ts,
            )


def _app(factory, *, owner_id: int = 1):
    return create_app(session_factory=factory, default_owner_id=owner_id)


@pytest.mark.asyncio
async def test_group_by_day_sums_per_utc_date(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec ai-usage §"按天聚合" — 3 days × multiple calls → 3 buckets."""
    await _seed(
        session_factory,
        owner_id=1,
        items=[
            (datetime(2026, 5, 1, 10, 0, tzinfo=UTC), "openai.chat", 100, 105),
            (datetime(2026, 5, 1, 22, 0, tzinfo=UTC), "openai.chat", 200, 210),
            (datetime(2026, 5, 2, 8, 0, tzinfo=UTC), "openai.chat", 50, 53),
            (datetime(2026, 5, 3, 0, 0, tzinfo=UTC), "openai.chat", 1000, 1050),
        ],
    )
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/usage", params={"group_by": "day"})
    assert resp.status_code == 200
    body = resp.json()
    assert [b["date"] for b in body] == ["2026-05-01", "2026-05-02", "2026-05-03"]
    assert [b["calls"] for b in body] == [2, 1, 1]
    assert [b["units"] for b in body] == [300, 50, 1000]
    assert [b["cost_micros"] for b in body] == [315, 53, 1050]


@pytest.mark.asyncio
async def test_group_by_kind_separates_success_and_failed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed(
        session_factory,
        owner_id=1,
        items=[
            (datetime(2026, 5, 1, tzinfo=UTC), "openai.chat", 100, 100),
            (datetime(2026, 5, 1, tzinfo=UTC), "openai.chat", 200, 200),
            (datetime(2026, 5, 1, tzinfo=UTC), "openai.chat.failed", 0, 0),
            (datetime(2026, 5, 1, tzinfo=UTC), "openai.vision", 50, 50),
        ],
    )
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/usage", params={"group_by": "kind"})
    body = resp.json()
    by_kind = {b["kind"]: b for b in body}
    assert by_kind["openai.chat"]["calls"] == 2
    assert by_kind["openai.chat"]["units"] == 300
    assert by_kind["openai.chat"]["cost_micros"] == 300
    assert by_kind["openai.chat.failed"]["calls"] == 1
    assert by_kind["openai.chat.failed"]["units"] == 0
    assert by_kind["openai.vision"]["calls"] == 1


@pytest.mark.asyncio
async def test_from_and_to_filter_bounds(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed(
        session_factory,
        owner_id=1,
        items=[
            (datetime(2026, 4, 30, 23, 59, tzinfo=UTC), "openai.chat", 1, 1),
            (datetime(2026, 5, 1, 12, 0, tzinfo=UTC), "openai.chat", 10, 10),
            (datetime(2026, 5, 2, 12, 0, tzinfo=UTC), "openai.chat", 20, 20),
            (datetime(2026, 5, 3, 0, 0, tzinfo=UTC), "openai.chat", 100, 100),
        ],
    )
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/usage",
            params={
                "from": "2026-05-01T00:00:00+00:00",
                "to": "2026-05-03T00:00:00+00:00",  # exclusive
                "group_by": "day",
            },
        )
    body = resp.json()
    # Only May 1 + May 2 within [from, to).
    assert [b["date"] for b in body] == ["2026-05-01", "2026-05-02"]
    assert [b["units"] for b in body] == [10, 20]


@pytest.mark.asyncio
async def test_cross_owner_isolated(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Tenant 1's rows must not surface for tenant 2."""
    await _seed(
        session_factory,
        owner_id=1,
        items=[(datetime(2026, 5, 1, tzinfo=UTC), "openai.chat", 1000, 1000)],
    )
    transport = ASGITransport(app=_app(session_factory, owner_id=2))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/usage", params={"group_by": "day"})
    assert resp.json() == []


@pytest.mark.asyncio
async def test_invalid_group_by_returns_422(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/usage", params={"group_by": "hour"})
    assert resp.status_code == 422
