"""§12.1 — ``GET /runs/{id}/events`` REST endpoint.

Covers the spec scenarios:

* Pagination via ``since`` (incremental pull, 200-row pages).
* ``limit`` clamp at 1000, default 200.
* Cross-owner / unknown run → ``[]`` (multi-tenant invariant; no 404).
* Response field shape matches the SSE message contract (same keys).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.api import create_app
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.models import RunEventRow
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            acc = await account_repo.upsert_session(
                session,
                owner_id=1,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await workflow_repo.upsert_by_source(
                session,
                owner_id=1,
                source=WorkflowSource.yaml,
                workflow=Workflow.model_validate(
                    {
                        "name": "w1",
                        "account_id": acc.id,
                        "trigger": {"type": "startup"},
                        "action_plan": {
                            "steps": [
                                {"action": "send_text", "chat_id": 1, "text": "x"},
                            ]
                        },
                    }
                ),
            )
        yield factory
    finally:
        await engine.dispose()


async def _seed_run_with_events(
    factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: int,
    n_events: int,
) -> int:
    async with factory() as s, s.begin():
        accs = await account_repo.list_for_owner(s, owner_id=owner_id)
        wfs = await workflow_repo.list_for_owner(s, owner_id=owner_id)
        if not accs or not wfs:
            raise RuntimeError("seed fixture missing data for this owner")
        job = await job_repo.create_pending(
            s,
            owner_id=owner_id,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            fire_at=datetime.now(UTC),
        )
        run = await run_repo.create_run(
            s,
            owner_id=owner_id,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            job_id=job.id,  # type: ignore[arg-type]
        )
    async with factory() as s, s.begin():
        for i in range(n_events):
            s.add(
                RunEventRow(
                    owner_id=owner_id,
                    run_id=run.id,  # type: ignore[arg-type]
                    seq=i,
                    type=f"t{i}",
                    message=f"event {i}",
                    attrs={"i": i},
                )
            )
    return run.id  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_get_events_returns_full_history(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run_with_events(session_factory, owner_id=1, n_events=3)
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/runs/{run_id}/events")
    assert resp.status_code == 200
    body = resp.json()
    assert [e["seq"] for e in body] == [0, 1, 2]
    # Field shape mirrors the SSE bus message.
    first = body[0]
    assert set(first.keys()) >= {
        "id",
        "owner_id",
        "run_id",
        "seq",
        "ts",
        "level",
        "type",
        "message",
        "attrs",
    }
    assert first["owner_id"] == 1
    assert first["type"] == "t0"
    assert first["level"] == "INFO"
    assert first["attrs"] == {"i": 0}


@pytest.mark.asyncio
async def test_get_events_paginates_with_since(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec runs §历史事件 REST 查询 Scenario 增量拉取 — strict ``seq > since``."""
    run_id = await _seed_run_with_events(session_factory, owner_id=1, n_events=350)
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # First page — default limit (200).
        page1 = (await client.get(f"/runs/{run_id}/events")).json()
        assert [e["seq"] for e in page1] == list(range(0, 200))
        # Increment.
        last_seq = page1[-1]["seq"]
        page2 = (
            await client.get(f"/runs/{run_id}/events", params={"since": last_seq})
        ).json()
    assert [e["seq"] for e in page2] == list(range(200, 350))


@pytest.mark.asyncio
async def test_limit_clamp_rejects_over_1000(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run_with_events(session_factory, owner_id=1, n_events=1)
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/runs/{run_id}/events", params={"limit": 1001})
    assert resp.status_code == 422  # FastAPI Query(ge=1, le=1000)


@pytest.mark.asyncio
async def test_unknown_run_returns_empty_not_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/runs/9999/events")
    # Tenant invariant from CLAUDE.md: cross-owner / unknown ⇒ [] not 4xx.
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_cross_owner_returns_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Events for owner=1's run must not surface for owner=2."""
    run_id = await _seed_run_with_events(session_factory, owner_id=1, n_events=3)
    # App configured for owner=2 must see nothing for owner=1's run.
    app = create_app(session_factory=session_factory, default_owner_id=2)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/runs/{run_id}/events")
    assert resp.status_code == 200
    assert resp.json() == []
