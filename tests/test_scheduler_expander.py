"""§10.4 / §10.8 / §10.16 — daily time_window expander + idempotency."""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, date, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.expander import expand_daily
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
            await _seed_workflow(
                factory,
                name="signin",
                account_id=acc.id,  # type: ignore[arg-type]
                trigger={
                    "type": "time_window",
                    "window": "10:00-23:00",
                    "count": 4,
                    "min_gap": "1h",
                },
            )
            await _seed_workflow(
                factory,
                name="other-cron",
                account_id=acc.id,  # type: ignore[arg-type]
                trigger={"type": "cron", "expression": "0 9 * * *"},
            )
        yield factory
    finally:
        await engine.dispose()


async def _seed_workflow(
    factory: async_sessionmaker[AsyncSession],
    *,
    name: str,
    account_id: int,
    trigger: dict,
) -> None:
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


@pytest.mark.asyncio
async def test_expand_creates_jobs_for_time_window(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    target = date(2026, 5, 29)
    now = datetime(2026, 5, 29, 0, 0, tzinfo=UTC)
    async with session_factory() as session, session.begin():
        report = await expand_daily(
            session, owner_id=1, target_date=target, now=now, rng=random.Random(0)
        )
    assert len(report.created_job_ids) == 4
    assert len(report.expanded_workflows) == 1  # only the time_window one
    assert report.skipped_workflows == []

    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    assert all(j.expansion_date == target for j in jobs)


@pytest.mark.asyncio
async def test_expand_ignores_non_time_window_triggers(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The cron workflow seeded by the fixture must NOT be touched by expand_daily."""
    async with session_factory() as session, session.begin():
        report = await expand_daily(
            session,
            owner_id=1,
            target_date=date(2026, 5, 29),
            now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
            rng=random.Random(0),
        )
    assert len(report.expanded_workflows) == 1

    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    # All produced jobs link to the time_window workflow.
    workflow_ids = {j.workflow_id for j in jobs}
    assert len(workflow_ids) == 1


@pytest.mark.asyncio
async def test_expand_is_idempotent_within_same_day(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §10.16 — calling expand_daily twice for the same date is a no-op."""
    target = date(2026, 5, 29)
    now = datetime(2026, 5, 29, 0, 0, tzinfo=UTC)
    async with session_factory() as session, session.begin():
        first = await expand_daily(
            session, owner_id=1, target_date=target, now=now, rng=random.Random(0)
        )
    async with session_factory() as session, session.begin():
        second = await expand_daily(
            session, owner_id=1, target_date=target, now=now, rng=random.Random(0)
        )
    assert len(first.created_job_ids) == 4
    assert second.created_job_ids == []
    assert len(second.skipped_workflows) == 1

    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    assert len(jobs) == 4


@pytest.mark.asyncio
async def test_expand_disabled_workflows_skipped(
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
        report = await expand_daily(
            session,
            owner_id=1,
            target_date=date(2026, 5, 29),
            now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
            rng=random.Random(0),
        )
    assert report.created_job_ids == []
    assert report.expanded_workflows == []


@pytest.mark.asyncio
async def test_expand_past_window_returns_no_fires(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §9.7 / §10.8 — now > window_end → fire_ats=[] but workflow noted."""
    async with session_factory() as session, session.begin():
        report = await expand_daily(
            session,
            owner_id=1,
            target_date=date(2026, 5, 29),
            now=datetime(2026, 5, 30, 0, 0, tzinfo=UTC),  # day after
            rng=random.Random(0),
        )
    assert report.created_job_ids == []
    # Workflow is still in expanded_workflows because we considered it; it
    # just had no fires today.
    assert len(report.expanded_workflows) == 1


@pytest.mark.asyncio
async def test_expand_cross_owner_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        report = await expand_daily(
            session,
            owner_id=2,
            target_date=date(2026, 5, 29),
            now=datetime(2026, 5, 29, 0, 0, tzinfo=UTC),
        )
    assert report.created_job_ids == []
    assert report.expanded_workflows == []
