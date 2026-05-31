"""§10.10 / §10.12 — startup spawner + trigger_now."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.control import spawn_startup_jobs, trigger_now
from tg_conductor.scheduler.models import JobStatus
from tg_conductor.triggers.startup import StartupTracker
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
            for name, trigger in [
                ("startup-a", {"type": "startup"}),
                ("startup-b", {"type": "startup"}),
                ("daily-cron", {"type": "cron", "expression": "0 9 * * *"}),
            ]:
                async with factory() as s, s.begin():
                    await workflow_repo.upsert_by_source(
                        s,
                        owner_id=1,
                        source=WorkflowSource.yaml,
                        workflow=Workflow.model_validate(
                            {
                                "name": name,
                                "account_id": acc.id,
                                "trigger": trigger,
                                "action_plan": {
                                    "steps": [
                                        {
                                            "action": "send_text",
                                            "chat_id": 1,
                                            "text": "hi",
                                        }
                                    ]
                                },
                            }
                        ),
                    )
        yield factory
    finally:
        await engine.dispose()


# ------------------------------------------------------------ §10.10 startup spawn


@pytest.mark.asyncio
async def test_startup_spawn_creates_one_per_startup_workflow(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    tracker = StartupTracker()
    async with session_factory() as session, session.begin():
        report = await spawn_startup_jobs(session, owner_id=1, tracker=tracker)
    assert len(report.created_job_ids) == 2
    assert report.already_fired is False
    assert tracker.fired is True

    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    # Each startup job is pending and ready to fire immediately.
    assert all(j.status == JobStatus.pending for j in jobs)


@pytest.mark.asyncio
async def test_startup_spawn_is_one_shot(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §9.10 — a reload-driven second call must not re-fire startup jobs."""
    tracker = StartupTracker()
    async with session_factory() as session, session.begin():
        await spawn_startup_jobs(session, owner_id=1, tracker=tracker)
    async with session_factory() as session, session.begin():
        report = await spawn_startup_jobs(session, owner_id=1, tracker=tracker)
    assert report.already_fired is True
    assert report.created_job_ids == []


@pytest.mark.asyncio
async def test_startup_spawn_ignores_non_startup_workflows(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    tracker = StartupTracker()
    async with session_factory() as session, session.begin():
        await spawn_startup_jobs(session, owner_id=1, tracker=tracker)
    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    workflow_ids = {j.workflow_id for j in jobs}
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    cron_wf_id = next(w.id for w in wfs if w.name == "daily-cron")
    assert cron_wf_id not in workflow_ids


@pytest.mark.asyncio
async def test_startup_spawn_skips_disabled_workflows(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
        startup_a = next(w for w in wfs if w.name == "startup-a")
        await workflow_repo.set_enabled(
            session,
            workflow_id=startup_a.id,
            owner_id=1,
            enabled=False,  # type: ignore[arg-type]
        )

    tracker = StartupTracker()
    async with session_factory() as session, session.begin():
        report = await spawn_startup_jobs(session, owner_id=1, tracker=tracker)
    # Only startup-b spawned.
    assert len(report.created_job_ids) == 1


# ------------------------------------------------------------ §10.12 trigger_now


@pytest.mark.asyncio
async def test_trigger_now_creates_immediate_pending_job(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    target = next(w for w in wfs if w.name == "daily-cron")

    before = datetime.now(UTC)
    async with session_factory() as session, session.begin():
        job_id = await trigger_now(
            session,
            workflow_id=target.id,
            owner_id=1,  # type: ignore[arg-type]
        )
    after = datetime.now(UTC)
    assert job_id is not None

    async with session_factory() as session:
        row = await job_repo.get_by_id(session, job_id, owner_id=1)
    assert row is not None
    assert row.status == JobStatus.pending
    assert row.workflow_id == target.id
    assert before <= row.fire_at <= after


@pytest.mark.asyncio
async def test_trigger_now_disabled_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    target = next(w for w in wfs if w.name == "daily-cron")
    async with session_factory() as session, session.begin():
        await workflow_repo.set_enabled(
            session,
            workflow_id=target.id,
            owner_id=1,
            enabled=False,  # type: ignore[arg-type]
        )

    async with session_factory() as session, session.begin():
        result = await trigger_now(
            session,
            workflow_id=target.id,
            owner_id=1,  # type: ignore[arg-type]
        )
    assert result is None


@pytest.mark.asyncio
async def test_trigger_now_unknown_workflow_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        result = await trigger_now(session, workflow_id=9999, owner_id=1)
    assert result is None


@pytest.mark.asyncio
async def test_trigger_now_cross_owner_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    target = next(w for w in wfs if w.name == "daily-cron")
    async with session_factory() as session, session.begin():
        result = await trigger_now(
            session,
            workflow_id=target.id,  # type: ignore[arg-type]
            owner_id=2,  # different owner
        )
    assert result is None
