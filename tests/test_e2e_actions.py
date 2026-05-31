"""§11.21 — end-to-end: Job → Run → events full chain via FakeTGClient + Dispatcher.

A trimmed version of the spec §17.1 e2e: 1 time_window workflow with a
text_pool send_text step, expanded to 3 Jobs, all consumed by a single
AccountWorker. Asserts the full event chain (run.started + action.send.text
+ run.finished) for every Run, and the Job state machine ends in 'succeeded'
for every Job.
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests._helpers import FakeAIClient
from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import InMemoryEventBus
from tg_conductor.runs.models import RunStatus
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.account_worker import AccountWorker
from tg_conductor.scheduler.dispatcher import Dispatcher
from tg_conductor.scheduler.expander import expand_daily
from tg_conductor.scheduler.models import JobStatus
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
                            "name": "daily-pool",
                            "account_id": acc.id,
                            "trigger": {
                                "type": "time_window",
                                "window": "10:00-23:00",
                                "count": 3,
                                "min_gap": "1h",
                            },
                            "action_plan": {
                                "steps": [
                                    {
                                        "action": "send_text",
                                        "chat_id": -100,
                                        "text_pool": ["A", "B", "C", "D", "E"],
                                        "pick_n": 2,
                                        "shuffle": False,
                                    }
                                ]
                            },
                        }
                    ),
                )
        yield factory
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_full_pipeline_time_window_to_succeeded(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """time_window 3 jobs → Dispatcher claims → AccountWorker drains → all succeed."""
    # 1) Expand for "today". Use a deterministic now so fire_at is past.
    morning = datetime(2026, 5, 29, 9, 0, tzinfo=UTC)
    async with session_factory() as session, session.begin():
        report = await expand_daily(
            session,
            owner_id=1,
            target_date=date(2026, 5, 29),
            now=morning,
            rng=random.Random(42),
        )
    assert len(report.created_job_ids) == 3

    # 2) Force fire_at into the past so claim picks them up immediately.
    past = datetime.now(UTC) - timedelta(seconds=10)
    async with session_factory() as session, session.begin():
        from sqlalchemy import update

        from tg_conductor.scheduler.models import JobRow

        await session.execute(update(JobRow).values(fire_at=past))

    # 3) Wire dispatcher + worker.
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    tg = FakeTGClient(label="main")
    await tg.connect()
    bus = InMemoryEventBus()

    async with session_factory() as session:
        accs = await account_repo.list_for_owner(session, owner_id=1)
    account_id = accs[0].id

    worker = AccountWorker(
        owner_id=1,
        account_id=account_id,
        queue=dispatcher.queue_for(account_id),
        session_factory=session_factory,
        tg_client=tg,
        ai_client=FakeAIClient(),
        event_bus=bus,
    )
    await worker.start()

    # 4) Tick the dispatcher to claim and enqueue.
    tick_report = await dispatcher.tick_once()
    assert len(tick_report.claimed_ids) == 3

    # 5) Wait for all 3 Runs to reach a terminal state.
    for _ in range(80):
        await asyncio.sleep(0.05)
        async with session_factory() as session:
            runs = await run_repo.list_for_owner(session, owner_id=1)
        if len(runs) == 3 and all(r.status != RunStatus.running for r in runs):
            break
    await worker.stop()

    # 6) Assertions.
    async with session_factory() as session:
        runs = await run_repo.list_for_owner(session, owner_id=1)
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    assert len(runs) == 3
    assert all(r.status == RunStatus.succeeded for r in runs)
    assert all(r.finished_at is not None for r in runs)
    assert all(j.status == JobStatus.succeeded for j in jobs)
    assert all(j.run_id is not None for j in jobs)

    # Each Run has exactly: run.started + 1 action.send.text + run.finished
    async with session_factory() as session:
        for run in runs:
            events = await run_repo.list_events(session, run_id=run.id, owner_id=1)
            topics = [e.type for e in events]
            assert topics == [
                "run.started",
                "action.send.text",
                "run.finished",
            ]
            send_evt = events[1]
            # pick_n=2 → 2 texts sent per Run from the pool of 5.
            assert len(send_evt.attrs["texts"]) == 2
            assert all(t in "ABCDE" for t in send_evt.attrs["texts"])

    # 7) Each Run sent 2 texts → 6 total send_text calls across the worker.
    sends = tg.calls_to("send_text")
    assert len(sends) == 6
