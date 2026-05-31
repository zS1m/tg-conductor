"""§11.13 / §11.16 / §11.17 / §11.20 — ActionPlan executor unit tests.

Covers: variants picking (random + round_robin persistence), inter-step
delay distribution, step timeout, continue_on_error, and the named-ref
stash for ``wait_for_step_<idx>``.
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests._helpers import FakeAIClient
from tg_conductor.accounts import repo as account_repo
from tg_conductor.actions.context import ActionContext
from tg_conductor.actions.executor import ActionPlanExecutor
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import InMemoryEventBus
from tg_conductor.runs.event_writer import EventWriter
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.tg_core.fake import FakeTGClient, make_fake_message
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import ActionPlan, Workflow


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
                                        "text": "x",
                                    }
                                ]
                            },
                        }
                    ),
                )
        yield factory
    finally:
        await engine.dispose()


async def _make_ctx(
    factory: async_sessionmaker[AsyncSession],
    *,
    tg_client: FakeTGClient | None = None,
) -> tuple[ActionContext, int]:
    if tg_client is None:
        tg_client = FakeTGClient(label="main")
        await tg_client.connect()
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
        run = await run_repo.create_run(
            session,
            owner_id=1,
            workflow_id=wid,
            account_id=aid,
            job_id=job.id,
        )
        run_id = run.id

    bus = InMemoryEventBus()
    writer = EventWriter(session_factory=factory, event_bus=bus)
    ctx = ActionContext(
        owner_id=1,
        account_id=aid,
        workflow_id=wid,
        run_id=run_id,
        job_id=job.id,
        tg_client=tg_client,
        ai_client=FakeAIClient(),
        event_writer=writer,
        session_factory=factory,
    )
    return ctx, run_id


def _plan(payload: dict) -> ActionPlan:
    return ActionPlan.model_validate(payload)


# ----------------------------------------------------------------- basic flow


@pytest.mark.asyncio
async def test_executor_runs_steps_in_order(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, run_id = await _make_ctx(session_factory)
    plan = _plan(
        {
            "steps": [
                {"action": "send_text", "chat_id": 1, "text": "A"},
                {"action": "send_text", "chat_id": 1, "text": "B"},
            ]
        }
    )
    result = await ActionPlanExecutor(plan).execute(ctx)
    assert result.success
    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    assert [c.args[1] for c in sends] == ["A", "B"]


# ----------------------------------------------------------------- §11.16 round_robin


@pytest.mark.asyncio
async def test_round_robin_persists_counter_across_runs(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """spec §11.16 — counter is durable in DB; 6 runs of 3 variants cycle A,B,C,A,B,C."""
    plan = _plan(
        {
            "variants": [
                {
                    "id": "A",
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "A"}],
                },
                {
                    "id": "B",
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "B"}],
                },
                {
                    "id": "C",
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "C"}],
                },
            ],
            "pick_variant": "round_robin",
        }
    )

    picked: list[str] = []
    for _ in range(6):
        ctx, _ = await _make_ctx(session_factory)
        await ActionPlanExecutor(plan).execute(ctx)
        sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
        picked.append(sends[-1].args[1])

    assert picked == ["A", "B", "C", "A", "B", "C"]

    # Counter is persisted on the workflow row.
    async with session_factory() as session:
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    assert wfs[0].rr_counter == 6


@pytest.mark.asyncio
async def test_random_variant_uses_rng(session_factory) -> None:  # type: ignore[no-untyped-def]
    plan = _plan(
        {
            "variants": [
                {
                    "id": f"V{i}",
                    "steps": [{"action": "send_text", "chat_id": 1, "text": f"V{i}"}],
                }
                for i in range(5)
            ],
            "pick_variant": "random",
        }
    )

    picked: list[str] = []
    for _ in range(10):
        ctx, _ = await _make_ctx(session_factory)
        await ActionPlanExecutor(plan, rng=random.Random(1)).execute(ctx)
        sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
        picked.append(sends[-1].args[1])
    # Seeded → all picks are identical.
    assert len(set(picked)) == 1


# ----------------------------------------------------------------- §11.17 inter_step_delay


@pytest.mark.asyncio
async def test_inter_step_delay_in_range_and_not_after_last(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, _ = await _make_ctx(session_factory)
    plan = _plan(
        {
            "steps": [
                {"action": "send_text", "chat_id": 1, "text": "a"},
                {"action": "send_text", "chat_id": 1, "text": "b"},
                {"action": "send_text", "chat_id": 1, "text": "c"},
            ],
            "inter_step_delay": {"min": 2.5, "max": 6.0},
        }
    )

    slept: list[float] = []

    async def _record(value: float) -> None:
        slept.append(value)

    await ActionPlanExecutor(plan, sleep=_record).execute(ctx)
    # 3 steps → 2 inter-step waits, none after the last.
    assert len(slept) == 2
    assert all(2.5 <= t <= 6.0 for t in slept)


# ------------------------------------------------------- delay_before (add-step-delay-before)


@pytest.mark.asyncio
async def test_delay_before_fixed_sleeps_and_emits(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, run_id = await _make_ctx(session_factory)
    plan = _plan(
        {
            "steps": [
                {"action": "send_text", "chat_id": 1, "text": "a", "delay_before": 5}
            ]
        }
    )
    slept: list[float] = []

    async def _record(value: float) -> None:
        slept.append(value)

    await ActionPlanExecutor(plan, sleep=_record).execute(ctx)
    # Single step → no inter_step_delay; the only sleep is the pre-step delay.
    assert slept == [5.0]
    async with session_factory() as session:
        events = await run_repo.list_events(session, run_id=run_id, owner_id=1)
    delays = [e for e in events if e.type == "action.delay"]
    assert len(delays) == 1
    assert delays[0].attrs["step_index"] == 0
    assert delays[0].attrs["action"] == "send_text"
    assert delays[0].attrs["seconds"] == 5.0


@pytest.mark.asyncio
async def test_delay_before_range_in_bounds(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, _ = await _make_ctx(session_factory)
    plan = _plan(
        {
            "steps": [
                {
                    "action": "send_text",
                    "chat_id": 1,
                    "text": "a",
                    "delay_before": {"min": 10, "max": 30},
                }
            ]
        }
    )
    slept: list[float] = []

    async def _record(value: float) -> None:
        slept.append(value)

    await ActionPlanExecutor(plan, rng=random.Random(1), sleep=_record).execute(ctx)
    assert len(slept) == 1
    assert 10.0 <= slept[0] <= 30.0


@pytest.mark.asyncio
async def test_delay_before_not_killed_by_step_timeout(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """Pre-step delay runs outside the step's asyncio.wait_for window, so a
    delay longer than the step timeout does NOT trigger step_timeout."""
    ctx, _ = await _make_ctx(session_factory)
    plan = _plan(
        {
            "steps": [
                {
                    "action": "send_text",
                    "chat_id": 1,
                    "text": "a",
                    "delay_before": 0.1,
                    "timeout": 0.05,
                }
            ]
        }
    )
    # Real asyncio.sleep (default): 0.1s delay > 0.05s step timeout. If the
    # delay were inside the timeout, the step would fail with step_timeout.
    result = await ActionPlanExecutor(plan).execute(ctx)
    assert result.success is True
    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    assert sends and sends[-1].args[1] == "a"


@pytest.mark.asyncio
async def test_no_delay_before_no_sleep_no_event(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, run_id = await _make_ctx(session_factory)
    plan = _plan({"steps": [{"action": "send_text", "chat_id": 1, "text": "a"}]})
    slept: list[float] = []

    async def _record(value: float) -> None:
        slept.append(value)

    await ActionPlanExecutor(plan, sleep=_record).execute(ctx)
    assert slept == []
    async with session_factory() as session:
        events = await run_repo.list_events(session, run_id=run_id, owner_id=1)
    assert [e for e in events if e.type == "action.delay"] == []


# ----------------------------------------------------------------- §11.14a step_timeout


@pytest.mark.asyncio
async def test_step_timeout_emits_action_failed_and_breaks(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, run_id = await _make_ctx(session_factory)
    plan = _plan(
        {
            "steps": [
                {
                    "action": "wait_for",
                    "chat_id": 1,
                    "text_pattern": "never",
                    "timeout": 0.05,
                },
                {"action": "send_text", "chat_id": 1, "text": "should-not-run"},
            ]
        }
    )

    result = await ActionPlanExecutor(plan).execute(ctx)
    assert result.success is False
    assert "step 0" in (result.error or "")

    async with session_factory() as session:
        events = await run_repo.list_events(session, run_id=run_id, owner_id=1)
    failed_evts = [e for e in events if e.type == "action.failed"]
    assert any(e.attrs.get("error") == "step_timeout" for e in failed_evts)
    # Subsequent step did NOT run.
    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    assert sends == []


@pytest.mark.asyncio
async def test_continue_on_error_runs_subsequent_steps(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, _ = await _make_ctx(session_factory)
    plan = _plan(
        {
            "steps": [
                {
                    "action": "wait_for",
                    "chat_id": 1,
                    "text_pattern": "never",
                    "timeout": 0.05,
                    "continue_on_error": True,
                },
                {"action": "send_text", "chat_id": 1, "text": "I ran"},
            ]
        }
    )
    result = await ActionPlanExecutor(plan).execute(ctx)
    # Plan success is True (no break), but the failed step is in step_errors.
    assert result.success is True
    assert result.step_errors == [(0, "step_timeout")]
    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    assert sends and sends[-1].args[1] == "I ran"


# ----------------------------------------------------------------- §11.20 generic action failure


@pytest.mark.asyncio
async def test_generic_exception_emits_action_failed(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """Non-timeout exceptions also produce action.failed with the message."""
    ctx, run_id = await _make_ctx(session_factory)
    # forward without a source message → ValueError.
    plan = _plan(
        {
            "steps": [
                {"action": "forward", "to_chat_id": -200},
            ]
        }
    )
    result = await ActionPlanExecutor(plan).execute(ctx)
    assert result.success is False
    async with session_factory() as session:
        events = await run_repo.list_events(session, run_id=run_id, owner_id=1)
    failed = [e for e in events if e.type == "action.failed"]
    assert len(failed) == 1
    assert "ValueError" in failed[0].attrs["error"]


# ----------------------------------------------------------------- named refs


@pytest.mark.asyncio
async def test_wait_for_step_named_ref_available_to_later_steps(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """wait_for at step 0 stashes its message under ``wait_for_step_0`` for step 1 forward."""
    ctx, _ = await _make_ctx(session_factory)
    fake: FakeTGClient = ctx.tg_client  # type: ignore[assignment]

    async def feeder() -> None:
        await asyncio.sleep(0)
        fake.inject_message(make_fake_message(chat_id=-100, text="match", id=77))

    asyncio.create_task(feeder())

    plan = _plan(
        {
            "steps": [
                {
                    "action": "wait_for",
                    "chat_id": -100,
                    "text_pattern": "match",
                    "timeout": 1.0,
                },
                {
                    "action": "forward",
                    "to_chat_id": -200,
                    "source": "wait_for_step_0",
                },
            ]
        }
    )
    result = await ActionPlanExecutor(plan).execute(ctx)
    assert result.success
    fwds = ctx.tg_client.calls_to("forward")  # type: ignore[union-attr]
    assert fwds[-1].args[0:2] == (-100, 77)
