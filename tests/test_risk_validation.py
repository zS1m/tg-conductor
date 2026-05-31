"""§19 — design.md risk validation.

§19.2 ``bump_rr_counter`` under concurrent triggers must be strictly
monotonic — no lost updates, no duplicates — even when many coroutines
hammer the same workflow row. This validates that SQLite ≥ 3.35's
``UPDATE ... RETURNING`` cooperates with aiosqlite's serialized
writer semantics.

§19.3 the schema must reject ``job_timeout < max(step.timeout)`` at
write time, and the executor must surface ``error="job_timeout"`` on
the ``run.finished`` event when the wall-clock cap fires.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import InMemoryEventBus
from tg_conductor.runs.event_writer import EventWriter
from tg_conductor.runs.models import RunStatus
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.account_worker import AccountWorker
from tg_conductor.tg_core.fake import FakeTGClient
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import ActionPlan, Workflow

# ----------------------------------------------------------- fixture


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
                        "name": "w-rr",
                        "account_id": acc.id,
                        "trigger": {"type": "startup"},
                        "action_plan": {
                            "variants": [
                                {
                                    "id": "A",
                                    "steps": [
                                        {
                                            "action": "send_text",
                                            "chat_id": 1,
                                            "text": "A",
                                        }
                                    ],
                                },
                                {
                                    "id": "B",
                                    "steps": [
                                        {
                                            "action": "send_text",
                                            "chat_id": 1,
                                            "text": "B",
                                        }
                                    ],
                                },
                            ],
                            "pick_variant": "round_robin",
                        },
                    }
                ),
            )
        yield factory
    finally:
        await engine.dispose()


# ----------------------------------------------------------- §19.2


@pytest.mark.asyncio
async def test_19_2_bump_rr_counter_under_concurrency_is_strictly_monotonic(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Hammer ``bump_rr_counter`` from N coroutines and check no value is
    lost or duplicated.

    Each call must open its own session — sharing a session across
    concurrent ``asyncio.gather`` users is not safe in SQLAlchemy. With
    SQLite + aiosqlite, the engine serializes writes; combined with
    ``UPDATE ... RETURNING`` the returned value is the post-update
    counter for that exact transaction, never reused.
    """
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    workflow_id = wfs[0].id
    assert workflow_id is not None

    n = 200

    async def one_bump() -> int | None:
        async with session_factory() as s, s.begin():
            return await workflow_repo.bump_rr_counter(
                s, workflow_id=workflow_id, owner_id=1
            )

    results = await asyncio.gather(*(one_bump() for _ in range(n)))

    assert all(r is not None for r in results), "every call must succeed"
    values = sorted(r for r in results if r is not None)
    # Strict +1 monotone: values are exactly {1, 2, ..., n}.
    assert values == list(range(1, n + 1)), "duplicates or lost updates detected"

    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    assert wfs[0].rr_counter == n


@pytest.mark.asyncio
async def test_19_2_bump_rr_counter_cross_owner_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Wrong owner_id → ``None``; the legitimate counter is untouched."""
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    workflow_id = wfs[0].id
    assert workflow_id is not None

    async with session_factory() as s, s.begin():
        result = await workflow_repo.bump_rr_counter(
            s, workflow_id=workflow_id, owner_id=999
        )
    assert result is None

    async with session_factory() as s:
        wfs = await workflow_repo.list_for_owner(s, owner_id=1)
    assert wfs[0].rr_counter == 0


# ----------------------------------------------------------- §19.3 schema


def test_19_3_schema_rejects_job_timeout_below_max_step_timeout() -> None:
    """``ActionPlan.model_validate`` raises when ``job_timeout < max(step.timeout)``."""
    with pytest.raises(ValidationError) as excinfo:
        ActionPlan.model_validate(
            {
                "job_timeout": 5,
                "steps": [
                    {
                        "action": "wait_for",
                        "chat_id": 1,
                        "text_pattern": "x",
                        "timeout": 60,  # > job_timeout
                    },
                ],
            }
        )
    assert "job_timeout" in str(excinfo.value)
    assert "must be >= max step budget" in str(excinfo.value)


def test_19_3_schema_accepts_job_timeout_equal_to_max_step_timeout() -> None:
    """Boundary: equal is OK."""
    plan = ActionPlan.model_validate(
        {
            "job_timeout": 60,
            "steps": [
                {
                    "action": "wait_for",
                    "chat_id": 1,
                    "text_pattern": "x",
                    "timeout": 60,
                },
            ],
        }
    )
    assert plan.job_timeout == 60


def test_19_3_schema_uses_type_default_when_step_timeout_omitted() -> None:
    """``send_text`` defaults to 30s — job_timeout=20 must fail."""
    with pytest.raises(ValidationError):
        ActionPlan.model_validate(
            {
                "job_timeout": 20,
                "steps": [
                    {"action": "send_text", "chat_id": 1, "text": "x"},
                ],
            }
        )


# ----------------------------------------------------------- §19.3 runtime


@pytest.mark.asyncio
async def test_19_3_job_timeout_marks_run_failed_with_error_field(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A Job whose plan exceeds ``job_timeout`` → Run.error = 'job_timeout'."""
    # Wipe the rr_counter fixture workflow and replace with a long-sleeping
    # plan that the worker can't possibly finish in 0.1s.
    async with session_factory() as s, s.begin():
        async with s.begin_nested():
            from sqlalchemy import delete

            from tg_conductor.workflows.models import WorkflowRow

            await s.execute(delete(WorkflowRow).where(WorkflowRow.owner_id == 1))
        accs = await account_repo.list_for_owner(s, owner_id=1)
        wf = await workflow_repo.upsert_by_source(
            s,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=Workflow.model_validate(
                {
                    "name": "slow",
                    "account_id": accs[0].id,
                    "trigger": {"type": "startup"},
                    "action_plan": {
                        # 3 steps × 0.08s inter-step delay = enough cumulative
                        # waiting to blow past job_timeout=0.1.
                        "job_timeout": 0.1,
                        "inter_step_delay": {"min": 0.08, "max": 0.08},
                        "steps": [
                            {
                                "action": "send_text",
                                "chat_id": 1,
                                "text": "a",
                                "timeout": 0.1,
                            },
                            {
                                "action": "send_text",
                                "chat_id": 1,
                                "text": "b",
                                "timeout": 0.1,
                            },
                            {
                                "action": "send_text",
                                "chat_id": 1,
                                "text": "c",
                                "timeout": 0.1,
                            },
                        ],
                    },
                }
            ),
        )
        from datetime import UTC, datetime

        job = await job_repo.create_pending(
            s,
            owner_id=1,
            workflow_id=wf.id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            fire_at=datetime.now(UTC),
        )
        job_id = job.id

    bus = InMemoryEventBus()
    tg = FakeTGClient(label="main")
    await tg.connect()

    from tests._helpers import FakeAIClient

    worker = AccountWorker(
        owner_id=1,
        account_id=accs[0].id,  # type: ignore[arg-type]
        queue=asyncio.Queue(),
        session_factory=session_factory,
        tg_client=tg,
        ai_client=FakeAIClient(),
        event_bus=bus,
    )

    # Pull Job out of DB and feed to handle_job.
    async with session_factory() as s:
        loaded = await job_repo.get_by_id(s, job_id, 1)
    assert loaded is not None

    await worker.handle_job(loaded)

    # Run terminated as failed with error='job_timeout'.
    async with session_factory() as s:
        runs = await run_repo.list_for_owner(s, 1)
    assert len(runs) == 1
    run = runs[0]
    assert run.status == RunStatus.failed
    assert run.error == "job_timeout"

    # The run.finished event carries the same error in its attrs.
    async with session_factory() as s:
        events = await run_repo.list_events(s, run_id=run.id, owner_id=1)
    finished = [e for e in events if e.type == "run.finished"]
    assert len(finished) == 1
    assert finished[0].attrs is not None
    assert finished[0].attrs["error"] == "job_timeout"
    # Tell EventWriter we used something (silences ruff for the import).
    _ = EventWriter
