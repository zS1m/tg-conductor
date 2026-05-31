"""§11.1 — Run + RunEventRow persistence + tenant isolation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.models import RunEventRow, RunStatus
from tg_conductor.scheduler import repo as job_repo
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
            async with factory() as s, s.begin():
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


async def _seed_run(factory: async_sessionmaker[AsyncSession]) -> int:
    async with factory() as session, session.begin():
        accs = await account_repo.list_for_owner(session, owner_id=1)
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
        job = await job_repo.create_pending(
            session,
            owner_id=1,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            fire_at=datetime.now(UTC),
        )
        run = await run_repo.create_run(
            session,
            owner_id=1,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            job_id=job.id,  # type: ignore[arg-type]
        )
        return run.id  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_create_run_starts_in_running_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    async with session_factory() as session:
        row = await run_repo.get_by_id(session, run_id=run_id, owner_id=1)
    assert row is not None
    assert row.status == RunStatus.running
    assert row.finished_at is None
    assert row.started_at is not None


@pytest.mark.asyncio
async def test_cross_owner_returns_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    async with session_factory() as session:
        assert await run_repo.get_by_id(session, run_id=run_id, owner_id=2) is None
        assert await run_repo.list_for_owner(session, owner_id=2) == []


@pytest.mark.asyncio
async def test_mark_finished_flips_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    async with session_factory() as session, session.begin():
        ok = await run_repo.mark_finished(
            session,
            run_id=run_id,
            owner_id=1,
            status=RunStatus.succeeded,
        )
    assert ok is True
    async with session_factory() as session:
        row = await run_repo.get_by_id(session, run_id=run_id, owner_id=1)
    assert row is not None
    assert row.status == RunStatus.succeeded
    assert row.finished_at is not None
    assert row.error is None


@pytest.mark.asyncio
async def test_mark_finished_records_error_on_failed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    async with session_factory() as session, session.begin():
        await run_repo.mark_finished(
            session,
            run_id=run_id,
            owner_id=1,
            status=RunStatus.failed,
            error="step_timeout",
        )
    async with session_factory() as session:
        row = await run_repo.get_by_id(session, run_id=run_id, owner_id=1)
    assert row is not None
    assert row.status == RunStatus.failed
    assert row.error == "step_timeout"


@pytest.mark.asyncio
async def test_mark_finished_noop_when_already_terminal(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    async with session_factory() as session, session.begin():
        await run_repo.mark_finished(
            session, run_id=run_id, owner_id=1, status=RunStatus.succeeded
        )
    async with session_factory() as session, session.begin():
        again = await run_repo.mark_finished(
            session, run_id=run_id, owner_id=1, status=RunStatus.failed
        )
    assert again is False


@pytest.mark.asyncio
async def test_mark_finished_rejects_running_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    async with session_factory() as session, session.begin():
        with pytest.raises(ValueError, match="terminal"):
            await run_repo.mark_finished(
                session, run_id=run_id, owner_id=1, status=RunStatus.running
            )


@pytest.mark.asyncio
async def test_run_events_unique_constraint_on_run_seq(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    from sqlalchemy.exc import IntegrityError

    run_id = await _seed_run(session_factory)
    async with session_factory() as session, session.begin():
        session.add(RunEventRow(owner_id=1, run_id=run_id, seq=0, type="x"))
    async with session_factory() as session:
        session.add(RunEventRow(owner_id=1, run_id=run_id, seq=0, type="dup"))
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.asyncio
async def test_list_events_filters_by_since(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id = await _seed_run(session_factory)
    async with session_factory() as session, session.begin():
        for i in range(5):
            session.add(RunEventRow(owner_id=1, run_id=run_id, seq=i, type=f"t{i}"))
    async with session_factory() as session:
        all_evts = await run_repo.list_events(session, run_id=run_id, owner_id=1)
        after_2 = await run_repo.list_events(
            session, run_id=run_id, owner_id=1, since=2
        )
    assert [e.seq for e in all_evts] == [0, 1, 2, 3, 4]
    assert [e.seq for e in after_2] == [3, 4]


@pytest.mark.asyncio
async def test_list_events_respects_owner(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Events for a run belonging to owner=1 must not surface for owner=2."""
    run_id = await _seed_run(session_factory)
    async with session_factory() as session, session.begin():
        session.add(RunEventRow(owner_id=1, run_id=run_id, seq=0, type="x"))
    async with session_factory() as session:
        evts = await run_repo.list_events(session, run_id=run_id, owner_id=2)
    assert evts == []
