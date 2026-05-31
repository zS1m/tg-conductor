"""§12.5 / §12.8 — ``run_events`` TTL pruning.

Spec scenarios:

* Old events (``ts < now - ttl_days``) are deleted; recent ones survive.
* The parent ``runs`` row is **never** deleted by this task (runs retention
  is separate; spec runs §"事件保留与清理" defaults to 365 d there).
* ``ttl_days <= 0`` (``0`` or ``-1``) disables the purge — events live
  forever.
* The :class:`EventsTtlCleaner` daemon ticks at the configured cadence and
  shuts down cooperatively.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.cleanup import EventsTtlCleaner, purge_expired_events
from tg_conductor.runs.models import RunEventRow, RunRow, RunStatus
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
        async with factory() as s, s.begin():
            acc = await account_repo.upsert_session(
                s,
                owner_id=1,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await workflow_repo.upsert_by_source(
                s,
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


async def _seed_run(factory: async_sessionmaker[AsyncSession]) -> int:
    async with factory() as s, s.begin():
        accs = await account_repo.list_for_owner(s, owner_id=1)
        wfs = await workflow_repo.list_for_owner(s, owner_id=1)
        job = await job_repo.create_pending(
            s,
            owner_id=1,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            fire_at=datetime.now(UTC),
        )
        run = await run_repo.create_run(
            s,
            owner_id=1,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            job_id=job.id,  # type: ignore[arg-type]
        )
        await run_repo.mark_finished(
            s, run_id=run.id, owner_id=1, status=RunStatus.succeeded
        )
        return run.id  # type: ignore[return-value]


async def _seed_events_with_ts(
    factory: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    items: list[tuple[int, str, datetime]],
) -> None:
    async with factory() as s, s.begin():
        for seq, typ, ts in items:
            s.add(
                RunEventRow(
                    owner_id=1,
                    run_id=run_id,
                    seq=seq,
                    type=typ,
                    message="",
                    attrs=None,
                    ts=ts,
                )
            )


@pytest.mark.asyncio
async def test_purge_deletes_old_keeps_recent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    now = datetime.now(UTC)
    await _seed_events_with_ts(
        session_factory,
        run_id=run_id,
        items=[
            (0, "old.0", now - timedelta(days=40)),
            (1, "old.1", now - timedelta(days=31)),
            (2, "fresh.0", now - timedelta(days=29)),
            (3, "fresh.1", now - timedelta(hours=1)),
        ],
    )

    async with session_factory() as s, s.begin():
        n = await purge_expired_events(s, ttl_days=30, now=now)
    assert n == 2

    async with session_factory() as s:
        rows = (
            (await s.execute(select(RunEventRow).order_by(RunEventRow.seq)))
            .scalars()
            .all()
        )
    assert [r.type for r in rows] == ["fresh.0", "fresh.1"]


@pytest.mark.asyncio
async def test_purge_never_touches_runs_row(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §"事件保留与清理": the run row outlives its events."""
    run_id = await _seed_run(session_factory)
    now = datetime.now(UTC)
    await _seed_events_with_ts(
        session_factory,
        run_id=run_id,
        items=[(0, "ancient", now - timedelta(days=400))],
    )

    async with session_factory() as s, s.begin():
        await purge_expired_events(s, ttl_days=30, now=now)

    async with session_factory() as s:
        run = await run_repo.get_by_id(s, run_id=run_id, owner_id=1)
        # All events gone…
        evts = (
            (await s.execute(select(RunEventRow).where(RunEventRow.run_id == run_id)))
            .scalars()
            .all()
        )
    assert run is not None  # …but the run row is intact.
    assert run.status == RunStatus.succeeded
    assert evts == []


@pytest.mark.parametrize("ttl_days", [0, -1])
@pytest.mark.asyncio
async def test_purge_disabled_keeps_everything(
    session_factory: async_sessionmaker[AsyncSession],
    ttl_days: int,
) -> None:
    """``ttl_days=0`` or ``-1`` keeps events forever."""
    run_id = await _seed_run(session_factory)
    now = datetime.now(UTC)
    await _seed_events_with_ts(
        session_factory,
        run_id=run_id,
        items=[
            (0, "very.old", now - timedelta(days=10000)),
            (1, "old", now - timedelta(days=100)),
        ],
    )

    async with session_factory() as s, s.begin():
        n = await purge_expired_events(s, ttl_days=ttl_days, now=now)
    assert n == 0

    async with session_factory() as s:
        evts = (await s.execute(select(RunEventRow))).scalars().all()
    assert len(evts) == 2


# ------------------------------------------------------------ daemon lifecycle


@pytest.mark.asyncio
async def test_cleaner_disabled_when_ttl_nonpositive(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    cleaner = EventsTtlCleaner(
        session_factory=session_factory,
        ttl_days=0,
        interval_seconds=0.01,
    )
    assert cleaner.disabled is True
    await cleaner.start()
    # No background task should have been created.
    assert cleaner._task is None  # noqa: SLF001 — sanity check
    await cleaner.stop()


@pytest.mark.asyncio
async def test_cleaner_ticks_then_stops_cleanly(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Daemon runs at least one purge, then stops within grace."""
    run_id = await _seed_run(session_factory)
    now = datetime.now(UTC)
    # Insert one ancient row that any reasonable TTL would prune.
    await _seed_events_with_ts(
        session_factory,
        run_id=run_id,
        items=[(0, "ancient", now - timedelta(days=10000))],
    )

    cleaner = EventsTtlCleaner(
        session_factory=session_factory,
        ttl_days=30,
        interval_seconds=0.05,  # fast enough to make the test snappy
        shutdown_grace_seconds=2.0,
    )
    await cleaner.start()
    # Give the loop one full tick.
    await asyncio.sleep(0.08)
    await cleaner.stop()
    assert cleaner._task is None  # noqa: SLF001

    async with session_factory() as s:
        rows = (await s.execute(select(RunEventRow))).scalars().all()
        # Run row still there.
        runs = (await s.execute(select(RunRow))).scalars().all()
    assert rows == []
    assert len(runs) == 1
