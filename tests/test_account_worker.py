"""§11.12 / §11.14 / §11.20 — AccountWorker lifecycle, job_timeout, Run state machine."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests._helpers import FakeAIClient
from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import InMemoryEventBus, run_topic
from tg_conductor.runs.models import RunStatus
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.account_worker import AccountWorker
from tg_conductor.scheduler.models import JobRow, JobStatus
from tg_conductor.tg_core.fake import FakeTGClient
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
                            "name": "happy",
                            "account_id": acc.id,
                            "trigger": {"type": "startup"},
                            "action_plan": {
                                "steps": [
                                    {
                                        "action": "send_text",
                                        "chat_id": -100,
                                        "text": "hello",
                                    }
                                ]
                            },
                        }
                    ),
                )
        yield factory
    finally:
        await engine.dispose()


async def _seed_running_job(
    factory: async_sessionmaker[AsyncSession],
) -> JobRow:
    """Create a Job row in 'running' state (dispatcher would have done this)."""
    async with factory() as session:
        accs = await account_repo.list_for_owner(session, owner_id=1)
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    aid, wid = accs[0].id, wfs[0].id
    async with factory() as session, session.begin():
        job = await job_repo.create_pending(
            session,
            owner_id=1,
            workflow_id=wid,
            account_id=aid,
            fire_at=datetime.now(UTC),
        )
    return job


def _new_worker(
    factory: async_sessionmaker[AsyncSession],
    *,
    queue: asyncio.Queue[JobRow] | None = None,
    tg_client: FakeTGClient | None = None,
) -> tuple[AccountWorker, FakeTGClient, asyncio.Queue[JobRow], InMemoryEventBus]:
    queue = queue or asyncio.Queue()
    tg = tg_client or FakeTGClient(label="main")
    bus = InMemoryEventBus()
    worker = AccountWorker(
        owner_id=1,
        account_id=1,
        queue=queue,
        session_factory=factory,
        tg_client=tg,
        ai_client=FakeAIClient(),
        event_bus=bus,
        shutdown_grace_seconds=2.0,
    )
    return worker, tg, queue, bus


# ----------------------------------------------------------------- happy path


@pytest.mark.asyncio
async def test_handle_job_runs_action_and_finishes_run(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    job = await _seed_running_job(session_factory)
    worker, tg, _q, _bus = _new_worker(session_factory)
    await tg.connect()

    result = await worker.handle_job(job)
    assert result.success

    sends = tg.calls_to("send_text")
    assert sends[0].args == (-100, "hello")

    # Run row terminal.
    async with session_factory() as session:
        runs = await run_repo.list_for_owner(session, owner_id=1)
    assert len(runs) == 1
    assert runs[0].status == RunStatus.succeeded
    assert runs[0].finished_at is not None
    assert runs[0].job_id == job.id

    # Events: run.started, action.send.text, run.finished.
    async with session_factory() as session:
        events = await run_repo.list_events(session, run_id=runs[0].id, owner_id=1)
    topics = [e.type for e in events]
    assert topics == ["run.started", "action.send.text", "run.finished"]
    finished_payload = events[-1].attrs
    assert finished_payload["status"] == "succeeded"

    # Job row updated.
    async with session_factory() as session:
        job_row = await job_repo.get_by_id(session, job.id, owner_id=1)
    assert job_row is not None
    assert job_row.status == JobStatus.succeeded
    assert job_row.run_id == runs[0].id
    assert job_row.finished_at is not None


@pytest.mark.asyncio
async def test_handle_job_emits_to_subscriber(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    job = await _seed_running_job(session_factory)
    worker, tg, _q, bus = _new_worker(session_factory)
    await tg.connect()

    received: list[dict] = []

    async def consume() -> None:
        # We don't know run_id ahead of time; subscribe to a wildcard? bus
        # is per-topic. Just open a future-tense subscriber on a guessed
        # topic — the first Run gets id=1 deterministically.
        async for msg in bus.subscribe(run_topic(1)):
            received.append(msg)
            if msg["type"] == "run.finished":
                return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    await worker.handle_job(job)
    await asyncio.wait_for(task, timeout=2.0)
    topics = [m["type"] for m in received]
    assert "run.started" in topics
    assert "run.finished" in topics


@pytest.mark.asyncio
async def test_trigger_message_seeds_last_matched_for_ai_reply(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """A message_match-style trigger message reaches ai_reply with no wait_for.

    Regression: ai_reply on a message_match trigger used to time out because
    the firing message was never seeded into the run context. Dispatcher now
    persists it into resolved_payload; the worker rebuilds it onto
    ctx.last_matched_message before the plan runs.
    """
    async with session_factory() as session:
        accs = await account_repo.list_for_owner(session, owner_id=1)
    aid = accs[0].id
    async with session_factory() as s, s.begin():
        ai_wf = await workflow_repo.upsert_by_source(
            s,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=Workflow.model_validate(
                {
                    "name": "ai",
                    "account_id": aid,
                    "trigger": {
                        "type": "message_match",
                        "chat_id": -100123,
                    },
                    "action_plan": {
                        "steps": [
                            {
                                "action": "ai_reply",
                                "to_chat_id": -100123,
                                "prompt_template": "回复：{message_text}",
                                "max_tokens": 50,
                            }
                        ]
                    },
                }
            ),
        )

    # Dispatcher would have stored the firing message here.
    async with session_factory() as s, s.begin():
        job = await job_repo.create_pending(
            s,
            owner_id=1,
            workflow_id=ai_wf.id,
            account_id=aid,
            fire_at=datetime.now(UTC),
        )
        job.resolved_payload = {
            "trigger_message": {
                "id": 99,
                "chat_id": -100123,
                "date": datetime(2026, 5, 30, 12, 0, tzinfo=UTC).isoformat(),
                "text": "问 ai：今天摸鱼吗",
                "from_user_id": 7,
                "topic_id": None,
            }
        }
        s.add(job)

    ai = FakeAIClient()
    worker = AccountWorker(
        owner_id=1,
        account_id=1,
        queue=asyncio.Queue(),
        session_factory=session_factory,
        tg_client=FakeTGClient(label="main"),
        ai_client=ai,
        event_bus=InMemoryEventBus(),
        shutdown_grace_seconds=2.0,
    )
    await worker._tg_client.connect()  # type: ignore[attr-defined]

    result = await worker.handle_job(job)
    assert result.success, result.error
    # The AI client saw the triggering message text, no wait_for involved.
    assert len(ai.chat_calls) == 1
    assert "今天摸鱼吗" in ai.chat_calls[0]["messages"][0].content


# ----------------------------------------------------------------- failure paths


@pytest.mark.asyncio
async def test_handle_job_records_run_failed_on_action_error(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """A failing action ends the Run in 'failed' with the error message preserved."""
    job = await _seed_running_job(session_factory)
    worker, tg, _q, _bus = _new_worker(session_factory)
    await tg.connect()
    # Force the next send_text to raise.
    tg.next_send_text_raises = RuntimeError("network down")

    result = await worker.handle_job(job)
    assert not result.success
    assert "network down" in (result.error or "")

    async with session_factory() as session:
        runs = await run_repo.list_for_owner(session, owner_id=1)
    assert runs[0].status == RunStatus.failed
    assert "network down" in (runs[0].error or "")

    async with session_factory() as session:
        job_row = await job_repo.get_by_id(session, job.id, owner_id=1)
    assert job_row.status == JobStatus.failed  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_handle_job_workflow_missing_marks_job_failed(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    job = await _seed_running_job(session_factory)
    # Delete the workflow row to simulate the spec edge case.
    async with session_factory() as session, session.begin():
        await workflow_repo.delete_by_id(
            session, workflow_id=job.workflow_id, owner_id=1
        )

    worker, tg, _q, _bus = _new_worker(session_factory)
    await tg.connect()
    result = await worker.handle_job(job)
    assert not result.success
    assert result.error == "workflow_not_found"

    async with session_factory() as session:
        job_row = await job_repo.get_by_id(session, job.id, owner_id=1)
    assert job_row.status == JobStatus.failed  # type: ignore[union-attr]


# ----------------------------------------------------------------- consume loop


@pytest.mark.asyncio
async def test_start_consume_processes_queued_job(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    job = await _seed_running_job(session_factory)
    worker, tg, queue, _bus = _new_worker(session_factory)
    await tg.connect()

    await worker.start()
    await queue.put(job)
    # Wait until the Run reaches a terminal state.
    for _ in range(50):
        await asyncio.sleep(0.05)
        async with session_factory() as session:
            runs = await run_repo.list_for_owner(session, owner_id=1)
        if runs and runs[0].status != RunStatus.running:
            break
    await worker.stop()

    async with session_factory() as session:
        runs = await run_repo.list_for_owner(session, owner_id=1)
    assert runs[0].status == RunStatus.succeeded


@pytest.mark.asyncio
async def test_stop_is_idempotent_with_no_pending_jobs(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    worker, _tg, _q, _bus = _new_worker(session_factory)
    await worker.start()
    await asyncio.sleep(0.1)
    await worker.stop()
    await worker.stop()  # second stop is a no-op


# ----------------------------------------------------------------- §11.14 job_timeout


@pytest.mark.asyncio
async def test_job_timeout_triggers_run_failed(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """plan.job_timeout caps the whole Run; on timeout, Run = failed / error=job_timeout.

    Schema rule ``job_timeout >= max(step.timeout)`` means we can't make
    a single step exceed job_timeout. We compose with ``inter_step_delay``
    that, accumulated across steps, blows past job_timeout.
    """
    async with session_factory() as session, session.begin():
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
        target = wfs[0]
        new_plan = Workflow.model_validate(
            {
                "name": target.name,
                "account_id": target.account_id,
                "trigger": {"type": "startup"},
                "action_plan": {
                    "steps": [
                        {
                            "action": "send_text",
                            "chat_id": -1,
                            "text": s,
                            "timeout": 0.1,
                        }
                        for s in ("a", "b", "c")
                    ],
                    "inter_step_delay": {"min": 0.08, "max": 0.08},
                    "job_timeout": 0.1,
                },
            }
        )
        await workflow_repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=new_plan,
        )

    job = await _seed_running_job(session_factory)
    worker, tg, _q, _bus = _new_worker(session_factory)
    await tg.connect()
    result = await worker.handle_job(job)
    assert not result.success
    assert result.error == "job_timeout"

    async with session_factory() as session:
        runs = await run_repo.list_for_owner(session, owner_id=1)
    assert runs[0].status == RunStatus.failed
    assert runs[0].error == "job_timeout"
