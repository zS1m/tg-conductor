"""§10.6 — Dispatcher tick_once + dispatch_immediate + lifecycle."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.dispatcher import Dispatcher
from tg_conductor.scheduler.models import JobStatus
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


async def _wid(factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    async with factory() as session:
        accs = await account_repo.list_for_owner(session, owner_id=1)
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    return accs[0].id, wfs[0].id  # type: ignore[return-value]


# ------------------------------------------------------------ tick_once


@pytest.mark.asyncio
async def test_tick_once_with_no_pending_returns_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    report = await dispatcher.tick_once()
    assert report.cron_created_ids == []
    assert report.claimed_ids == []
    assert report.skipped_ids == []


@pytest.mark.asyncio
async def test_tick_once_claims_pending_and_enqueues(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid, wid = await _wid(session_factory)
    now = datetime.now(UTC)
    async with session_factory() as s, s.begin():
        for off in (-1, -2, -3):
            await job_repo.create_pending(
                s,
                owner_id=1,
                workflow_id=wid,
                account_id=aid,
                fire_at=now + timedelta(seconds=off),
            )

    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    report = await dispatcher.tick_once()
    assert len(report.claimed_ids) == 3
    queue = dispatcher.queue_for(aid)
    assert queue.qsize() == 3


@pytest.mark.asyncio
async def test_tick_once_passes_compensation_window_to_claim(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid, wid = await _wid(session_factory)
    now = datetime.now(UTC)
    async with session_factory() as s, s.begin():
        ancient = await job_repo.create_pending(
            s,
            owner_id=1,
            workflow_id=wid,
            account_id=aid,
            fire_at=now - timedelta(seconds=600),
        )
        fresh = await job_repo.create_pending(
            s,
            owner_id=1,
            workflow_id=wid,
            account_id=aid,
            fire_at=now - timedelta(seconds=10),
        )

    dispatcher = Dispatcher(
        session_factory=session_factory,
        owner_id=1,
        compensation_window_seconds=60.0,
    )
    report = await dispatcher.tick_once()
    assert report.claimed_ids == [fresh.id]
    assert report.skipped_ids == [ancient.id]


# ------------------------------------------------------------ dispatch_immediate


@pytest.mark.asyncio
async def test_dispatch_immediate_creates_running_job_and_enqueues(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid, wid = await _wid(session_factory)
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    job_id = await dispatcher.dispatch_immediate(workflow_id=wid, account_id=aid)
    assert job_id is not None

    async with session_factory() as session:
        row = await job_repo.get_by_id(session, job_id, owner_id=1)
    assert row is not None
    assert row.status == JobStatus.running
    assert row.started_at is not None
    assert dispatcher.queue_for(aid).qsize() == 1


@pytest.mark.asyncio
async def test_dispatch_immediate_persists_trigger_message(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """message_match's firing message round-trips through resolved_payload.

    Regression: ai_reply on a message_match trigger timed out in a wait_for
    because the triggering message was dropped at dispatch and never seeded
    into the run context.
    """
    from datetime import datetime

    from tg_conductor.tg_core.protocol import Message, message_from_payload

    aid, wid = await _wid(session_factory)
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    msg = Message(
        id=42,
        chat_id=-100123,
        date=datetime(2026, 5, 30, 12, 0, tzinfo=UTC),
        text="问 ai：今天摸鱼吗",
        from_user_id=7,
        topic_id=None,
    )
    job_id = await dispatcher.dispatch_immediate(
        workflow_id=wid, account_id=aid, trigger_message=msg
    )
    assert job_id is not None

    async with session_factory() as session:
        row = await job_repo.get_by_id(session, job_id, owner_id=1)
    assert row is not None
    assert row.resolved_payload is not None
    restored = message_from_payload(row.resolved_payload["trigger_message"])
    assert restored.id == 42
    assert restored.text == "问 ai：今天摸鱼吗"
    assert restored.chat_id == -100123
    assert restored.from_user_id == 7


@pytest.mark.asyncio
async def test_dispatch_immediate_without_trigger_message_leaves_payload_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid, wid = await _wid(session_factory)
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    job_id = await dispatcher.dispatch_immediate(workflow_id=wid, account_id=aid)
    assert job_id is not None
    async with session_factory() as session:
        row = await job_repo.get_by_id(session, job_id, owner_id=1)
    assert row is not None
    assert row.resolved_payload is None


@pytest.mark.asyncio
async def test_dispatch_immediate_disabled_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid, wid = await _wid(session_factory)
    async with session_factory() as session, session.begin():
        await workflow_repo.set_enabled(
            session, workflow_id=wid, owner_id=1, enabled=False
        )
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    result = await dispatcher.dispatch_immediate(workflow_id=wid, account_id=aid)
    assert result is None
    assert dispatcher.queue_for(aid).qsize() == 0


@pytest.mark.asyncio
async def test_dispatch_immediate_cross_owner_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid, wid = await _wid(session_factory)
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=2)
    result = await dispatcher.dispatch_immediate(workflow_id=wid, account_id=aid)
    assert result is None


@pytest.mark.asyncio
async def test_dispatch_immediate_unknown_workflow_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    assert await dispatcher.dispatch_immediate(workflow_id=9999, account_id=1) is None


# ------------------------------------------------------------ lifecycle


@pytest.mark.asyncio
async def test_start_then_stop_cleanly_exits(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    dispatcher = Dispatcher(
        session_factory=session_factory, owner_id=1, tick_seconds=0.05
    )
    await dispatcher.start()
    # Let the loop tick at least once.
    await asyncio.sleep(0.1)
    await dispatcher.stop()
    # No leftover task.
    assert dispatcher._task is None  # noqa: SLF001 - test-only


@pytest.mark.asyncio
async def test_start_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    dispatcher = Dispatcher(
        session_factory=session_factory, owner_id=1, tick_seconds=0.05
    )
    await dispatcher.start()
    await dispatcher.start()  # second start is a no-op
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_loop_continues_through_tick_errors(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exception inside tick_once must not crash the loop."""
    dispatcher = Dispatcher(
        session_factory=session_factory, owner_id=1, tick_seconds=0.02
    )
    call_count = 0

    async def flaky_tick():  # type: ignore[no-untyped-def]
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("boom")
        # subsequent calls succeed

    monkeypatch.setattr(dispatcher, "tick_once", flaky_tick)
    await dispatcher.start()
    await asyncio.sleep(0.1)
    await dispatcher.stop()
    assert call_count >= 2, "loop should have survived the first failure"
