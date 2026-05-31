"""§10.5 / §10.15 — cron tick + no-duplicate-per-minute idempotency."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.cron_tick import tick
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> async_sessionmaker[AsyncSession]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            acc = await account_repo.upsert_session(
                session,
                owner_id=1,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await session.commit()
            await _seed(factory, "every-min", acc.id, expression="* * * * *")  # type: ignore[arg-type]
            await _seed(factory, "daily-9", acc.id, expression="0 9 * * *")  # type: ignore[arg-type]
            # A non-cron workflow that should be ignored.
            await _seed(factory, "startup-wf", acc.id, expression=None)  # type: ignore[arg-type]
        yield factory
    finally:
        await engine.dispose()


async def _seed(
    factory: async_sessionmaker[AsyncSession],
    name: str,
    account_id: int,
    *,
    expression: str | None,
) -> None:
    trigger = (
        {"type": "cron", "expression": expression}
        if expression is not None
        else {"type": "startup"}
    )
    async with factory() as session, session.begin():
        await workflow_repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=Workflow.model_validate(
                {
                    "name": name,
                    "account_id": account_id,
                    "trigger": trigger,
                    "action_plan": {
                        "steps": [{"action": "send_text", "chat_id": 1, "text": "hi"}]
                    },
                }
            ),
        )


# ------------------------------------------------------------ basic happy path


@pytest.mark.asyncio
async def test_tick_creates_job_for_due_cron(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """At 12:00:30, the every-minute cron has missed slots since 11:58:00."""
    now = datetime(2026, 5, 29, 12, 0, 30, tzinfo=UTC)
    async with session_factory() as session, session.begin():
        report = await tick(session, owner_id=1, now=now, lookback_minutes=2.0)
    # Anchor = 12:00:30 − 2min = 11:58:30. cron("* * * * *") strictly after
    # 11:58:30 gives 11:59:00 then 12:00:00; 12:01:00 > now stops the loop.
    # daily-9 cron's next fire after lookback is tomorrow 09:00, after now.
    fire_minutes = {j.fire_at.minute for j in (await _fetch_jobs(session_factory))}
    assert fire_minutes == {59, 0}
    assert len(report.created_job_ids) == 2


@pytest.mark.asyncio
async def test_tick_ignores_non_cron_workflows(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        await tick(session, owner_id=1, now=datetime(2026, 5, 29, 9, 0, 30, tzinfo=UTC))
    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    workflow_ids = {j.workflow_id for j in jobs}
    # startup-wf must not contribute any jobs.
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    startup_wf_id = next(w.id for w in wfs if w.name == "startup-wf")
    assert startup_wf_id not in workflow_ids


# ------------------------------------------------------------ §10.15 idempotency


@pytest.mark.asyncio
async def test_tick_does_not_duplicate_within_same_minute(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Two ticks within the same minute must NOT produce duplicate jobs."""
    base = datetime(2026, 5, 29, 12, 0, 5, tzinfo=UTC)
    async with session_factory() as session, session.begin():
        first = await tick(session, owner_id=1, now=base, lookback_minutes=2.0)
    # Second tick 20 seconds later — still inside minute 12:00.
    async with session_factory() as session, session.begin():
        second = await tick(
            session,
            owner_id=1,
            now=base + timedelta(seconds=20),
            lookback_minutes=2.0,
        )
    assert len(first.created_job_ids) >= 1
    assert second.created_job_ids == []


@pytest.mark.asyncio
async def test_tick_creates_one_per_minute_when_advancing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Tick at 12:00:30, then again at 12:01:30 → exactly one new job."""
    first_now = datetime(2026, 5, 29, 12, 0, 30, tzinfo=UTC)
    second_now = datetime(2026, 5, 29, 12, 1, 30, tzinfo=UTC)
    async with session_factory() as session, session.begin():
        first = await tick(session, owner_id=1, now=first_now)
    async with session_factory() as session, session.begin():
        second = await tick(session, owner_id=1, now=second_now)
    # Second tick adds exactly the 12:01:00 slot for every-minute cron.
    assert len(second.created_job_ids) == 1
    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1, limit=1000)
    minutes = sorted({j.fire_at.minute for j in jobs})
    # First batch: 11:59 + 12:00. Second tick adds 12:01.
    assert minutes == [0, 1, 59]
    assert len(jobs) == len(first.created_job_ids) + 1


@pytest.mark.asyncio
async def test_tick_disabled_workflows_skipped(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
        for wf in wfs:
            await workflow_repo.set_enabled(
                session,
                workflow_id=wf.id,
                owner_id=1,
                enabled=False,  # type: ignore[arg-type]
            )
    async with session_factory() as session, session.begin():
        report = await tick(
            session,
            owner_id=1,
            now=datetime(2026, 5, 29, 12, 0, 30, tzinfo=UTC),
        )
    assert report.created_job_ids == []


@pytest.mark.asyncio
async def test_tick_catchup_cap_truncates_unbounded_burst(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A 30-day silent window with * * * * * cron would yield 43k jobs; cap it."""
    # `lookback_minutes` huge → anchor far in the past → many missed slots.
    async with session_factory() as session, session.begin():
        report = await tick(
            session,
            owner_id=1,
            now=datetime(2026, 5, 29, 12, 0, 30, tzinfo=UTC),
            lookback_minutes=60 * 24,  # 1 day → would yield ~1440 slots
            max_catchup_per_workflow=10,
        )
    # Only the every-minute cron hits the cap; daily-9 produces 0 or 1 fire.
    every_min = sum(1 for jid in report.created_job_ids if jid is not None)
    assert every_min <= 11  # 10 from every-min cap + ≤ 1 from daily-9


async def _fetch_jobs(factory: async_sessionmaker[AsyncSession]) -> list:
    async with factory() as session:
        return await job_repo.list_for_owner(session, owner_id=1, limit=1000)
