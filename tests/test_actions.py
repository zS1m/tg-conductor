"""§11.6-§11.11 + §11.15 / §11.18 / §11.19 — action implementations.

Each action gets a focused test (+ pool sample, wait_for timeout,
click_button text match as called out in tasks.md). They share a tiny
``_make_ctx`` factory that wires real DB + EventBus + EventWriter against
``FakeTGClient`` / ``FakeAIClient``.
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests._helpers import FakeAIClient
from tg_conductor.accounts import repo as account_repo
from tg_conductor.actions import (
    ai_reply,
    click_button,
    forward,
    send_dice,
    send_text,
    wait_for,
)
from tg_conductor.actions.context import ActionContext
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import InMemoryEventBus
from tg_conductor.runs.event_writer import EventWriter
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.tg_core.fake import FakeTGClient, make_fake_message
from tg_conductor.tg_core.protocol import ButtonSpec
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import (
    AIReplyStep,
    ClickButtonMatch,
    ClickButtonStep,
    ForwardStep,
    SendDiceStep,
    SendTextStep,
    WaitForStep,
    Workflow,
)


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
    ai_client: FakeAIClient | None = None,
) -> tuple[ActionContext, InMemoryEventBus, int]:
    if tg_client is None:
        tg_client = FakeTGClient(label="main")
        await tg_client.connect()
    if ai_client is None:
        ai_client = FakeAIClient()

    async with factory() as session:
        accs = await account_repo.list_for_owner(session, owner_id=1)
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    aid = accs[0].id
    wid = wfs[0].id

    async with factory() as session, session.begin():
        job = await job_repo.create_pending(
            session,
            owner_id=1,
            workflow_id=wid,  # type: ignore[arg-type]
            account_id=aid,  # type: ignore[arg-type]
            fire_at=datetime.now(UTC),
        )
        run = await run_repo.create_run(
            session,
            owner_id=1,
            workflow_id=wid,  # type: ignore[arg-type]
            account_id=aid,  # type: ignore[arg-type]
            job_id=job.id,  # type: ignore[arg-type]
        )
        run_id = run.id

    bus = InMemoryEventBus()
    writer = EventWriter(session_factory=factory, event_bus=bus)
    ctx = ActionContext(
        owner_id=1,
        account_id=aid,  # type: ignore[arg-type]
        workflow_id=wid,  # type: ignore[arg-type]
        run_id=run_id,  # type: ignore[arg-type]
        job_id=job.id,  # type: ignore[arg-type]
        tg_client=tg_client,
        ai_client=ai_client,  # type: ignore[arg-type]
        event_writer=writer,
        session_factory=factory,
    )
    return ctx, bus, run_id  # type: ignore[return-value]


async def _events(factory: async_sessionmaker[AsyncSession], run_id: int) -> list:
    async with factory() as session:
        return await run_repo.list_events(session, run_id=run_id, owner_id=1)


# =================================================================
# send_text
# =================================================================


@pytest.mark.asyncio
async def test_send_text_single_emits_event(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, _bus, run_id = await _make_ctx(session_factory)
    step = SendTextStep(action="send_text", chat_id=-100, text="hello")

    await send_text.execute(step, ctx)

    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    assert len(sends) == 1
    assert sends[0].args == (-100, "hello")

    [evt] = await _events(session_factory, run_id)
    assert evt.type == "action.send.text"
    assert evt.attrs["texts"] == ["hello"]


@pytest.mark.asyncio
async def test_send_text_pool_no_replacement_and_shuffle(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    """§11.15 — pool sampling: N distinct picks, shuffle vs sorted order."""
    ctx, _bus, run_id = await _make_ctx(session_factory)
    pool = ["a", "b", "c", "d", "e"]
    step = SendTextStep(
        action="send_text",
        chat_id=-100,
        text_pool=pool,
        pick_n=3,
        shuffle=False,
    )

    # Seed rng so test is deterministic.
    await send_text.execute(step, ctx, rng=random.Random(42))

    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    sent_texts = [c.args[1] for c in sends]
    assert len(sent_texts) == 3
    assert len(set(sent_texts)) == 3, "must be no-replacement (all distinct)"
    assert set(sent_texts) <= set(pool)
    # shuffle=False preserves pool order.
    sent_indices = [pool.index(t) for t in sent_texts]
    assert sent_indices == sorted(sent_indices)

    [evt] = await _events(session_factory, run_id)
    assert evt.attrs["pick_n"] == 3
    assert evt.attrs["shuffled"] is False
    assert evt.attrs["text_pool_size"] == 5


@pytest.mark.asyncio
async def test_send_text_pool_shuffle_true_may_break_pool_order(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, _bus, _ = await _make_ctx(session_factory)
    pool = list("abcdefghij")
    step = SendTextStep(
        action="send_text",
        chat_id=-100,
        text_pool=pool,
        pick_n=10,
        shuffle=True,
    )

    # Use a seed known to produce a non-sorted shuffle.
    await send_text.execute(step, ctx, rng=random.Random(7))
    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    sent_texts = [c.args[1] for c in sends]
    assert set(sent_texts) == set(pool)
    # With shuffle=True we should typically see a non-pool order.
    assert sent_texts != pool


# =================================================================
# send_dice
# =================================================================


@pytest.mark.asyncio
async def test_send_dice_default_emoji(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, _bus, run_id = await _make_ctx(session_factory)
    step = SendDiceStep(action="send_dice", chat_id=-100)

    await send_dice.execute(step, ctx)

    [call] = ctx.tg_client.calls_to("send_dice")  # type: ignore[union-attr]
    assert call.args == (-100, "🎲")
    [evt] = await _events(session_factory, run_id)
    assert evt.type == "action.send.dice"
    assert evt.attrs["emoji"] == "🎲"


# =================================================================
# forward
# =================================================================


@pytest.mark.asyncio
async def test_forward_uses_last_matched_message(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, _bus, run_id = await _make_ctx(session_factory)
    ctx.last_matched_message = make_fake_message(chat_id=-100, text="source", id=42)
    step = ForwardStep(action="forward", to_chat_id=-200)

    await forward.execute(step, ctx)

    [call] = ctx.tg_client.calls_to("forward")  # type: ignore[union-attr]
    assert call.args == (-100, 42, -200)
    [evt] = await _events(session_factory, run_id)
    assert evt.type == "action.forward"
    assert evt.attrs["from_message_id"] == 42


@pytest.mark.asyncio
async def test_forward_missing_source_raises(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, _bus, _ = await _make_ctx(session_factory)
    step = ForwardStep(action="forward", to_chat_id=-200)

    with pytest.raises(ValueError, match="not available"):
        await forward.execute(step, ctx)


# =================================================================
# click_button (§11.19)
# =================================================================


def _msg_with_buttons(*labels: str):  # type: ignore[no-untyped-def]
    return make_fake_message(
        chat_id=-100,
        text="pick one",
        id=99,
        buttons=[[ButtonSpec(text=t) for t in labels]],
    )


@pytest.mark.asyncio
async def test_click_button_text_match(session_factory) -> None:  # type: ignore[no-untyped-def]
    """§11.19 — exact-text mode finds and clicks the right button."""
    ctx, _bus, run_id = await _make_ctx(session_factory)
    ctx.last_matched_message = _msg_with_buttons("签到", "取消")
    step = ClickButtonStep(action="click_button", match=ClickButtonMatch(text="签到"))

    await click_button.execute(step, ctx)

    [call] = ctx.tg_client.calls_to("click_button")  # type: ignore[union-attr]
    assert call.args == (-100, 99)
    assert call.kwargs["text"] == "签到"
    [evt] = await _events(session_factory, run_id)
    assert evt.attrs["matched_text"] == "签到"


@pytest.mark.asyncio
async def test_click_button_text_regex_match(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, _bus, _ = await _make_ctx(session_factory)
    ctx.last_matched_message = _msg_with_buttons("Day 1 签到", "取消")
    step = ClickButtonStep(
        action="click_button", match=ClickButtonMatch(text_regex=r"签到$")
    )

    await click_button.execute(step, ctx)

    [call] = ctx.tg_client.calls_to("click_button")  # type: ignore[union-attr]
    assert call.kwargs["text"] == "Day 1 签到"


@pytest.mark.asyncio
async def test_click_button_no_match_raises(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, _bus, _ = await _make_ctx(session_factory)
    ctx.last_matched_message = _msg_with_buttons("yes", "no")
    step = ClickButtonStep(action="click_button", match=ClickButtonMatch(text="maybe"))

    with pytest.raises(ValueError, match="no button matched"):
        await click_button.execute(step, ctx)


@pytest.mark.asyncio
async def test_click_button_ai_image_prompt_not_implemented(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, _bus, _ = await _make_ctx(session_factory)
    ctx.last_matched_message = _msg_with_buttons("a", "b")
    step = ClickButtonStep(
        action="click_button",
        match=ClickButtonMatch(ai_image_prompt="pick the cat"),
    )

    with pytest.raises(NotImplementedError, match="§13"):
        await click_button.execute(step, ctx)


@pytest.mark.asyncio
async def test_click_button_message_without_buttons_raises(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, _bus, _ = await _make_ctx(session_factory)
    ctx.last_matched_message = make_fake_message(chat_id=-100, text="x")
    step = ClickButtonStep(action="click_button", match=ClickButtonMatch(text="x"))
    with pytest.raises(ValueError, match="no inline keyboard"):
        await click_button.execute(step, ctx)


# =================================================================
# wait_for (§11.18)
# =================================================================


@pytest.mark.asyncio
async def test_wait_for_success_updates_last_matched(
    session_factory,  # type: ignore[no-untyped-def]
) -> None:
    ctx, _bus, run_id = await _make_ctx(session_factory)
    fake: FakeTGClient = ctx.tg_client  # type: ignore[assignment]

    async def feeder() -> None:
        await asyncio.sleep(0)
        fake.inject_message(make_fake_message(chat_id=-100, text="pong-123"))

    asyncio.create_task(feeder())

    step = WaitForStep(
        action="wait_for",
        chat_id=-100,
        text_pattern="pong",
        timeout=1.0,
    )
    await wait_for.execute(step, ctx)

    assert ctx.last_matched_message is not None
    assert "pong" in (ctx.last_matched_message.text or "")
    [evt] = await _events(session_factory, run_id)
    assert evt.type == "action.wait_for"


@pytest.mark.asyncio
async def test_wait_for_timeout_raises(session_factory) -> None:  # type: ignore[no-untyped-def]
    """§11.18 — no message arrives → raises asyncio.TimeoutError after timeout."""
    ctx, _bus, _ = await _make_ctx(session_factory)
    step = WaitForStep(
        action="wait_for",
        chat_id=-100,
        text_pattern="never",
        timeout=0.05,
    )
    with pytest.raises(asyncio.TimeoutError):
        await wait_for.execute(step, ctx)
    # Action did NOT emit (executor will emit action.failed in C3).
    assert ctx.last_matched_message is None


# =================================================================
# ai_reply
# =================================================================


@pytest.mark.asyncio
async def test_ai_reply_formats_prompt_and_sends(
    session_factory,
    master_key,  # type: ignore[no-untyped-def]  # noqa: ARG001
) -> None:
    ctx, _bus, run_id = await _make_ctx(session_factory)
    ctx.last_matched_message = make_fake_message(chat_id=-100, text="今天天气如何")
    step = AIReplyStep(
        action="ai_reply",
        to_chat_id=-200,
        prompt_template="用一句话回复: {message_text}",
        model="gpt-test",
        max_tokens=50,
    )

    await ai_reply.execute(step, ctx)

    ai: FakeAIClient = ctx.ai_client  # type: ignore[assignment]
    [call] = ai.chat_calls
    assert [m.content for m in call["messages"]] == ["用一句话回复: 今天天气如何"]
    assert [m.role for m in call["messages"]] == ["user"]
    assert call["model"] == "gpt-test"
    assert call["max_tokens"] == 50
    # owner / run / workflow / account tagged onto the call so usage
    # accounting can attribute it to the right tenant + Run.
    assert call["owner_id"] == ctx.owner_id
    assert call["run_id"] == ctx.run_id
    assert call["workflow_id"] == ctx.workflow_id
    assert call["account_id"] == ctx.account_id

    sends = ctx.tg_client.calls_to("send_text")  # type: ignore[union-attr]
    assert sends[-1].args == (-200, "ai-reply")

    [evt] = await _events(session_factory, run_id)
    assert evt.type == "action.ai_reply"
    assert evt.attrs["prompt_tokens"] == 10
    assert evt.attrs["completion_tokens"] == 5
    assert evt.attrs["total_tokens"] == 15
    assert evt.attrs["latency_ms"] == 20


@pytest.mark.asyncio
async def test_ai_reply_uses_settings_default_model_when_step_model_none(
    session_factory,
    master_key,  # type: ignore[no-untyped-def]  # noqa: ARG001
) -> None:
    from tg_conductor.config.settings import get_settings

    ctx, _bus, _ = await _make_ctx(session_factory)
    ctx.last_matched_message = make_fake_message(chat_id=-100, text="hello")
    step = AIReplyStep(
        action="ai_reply",
        to_chat_id=-200,
        prompt_template="echo: {message_text}",
    )
    await ai_reply.execute(step, ctx)

    ai: FakeAIClient = ctx.ai_client  # type: ignore[assignment]
    assert ai.chat_calls[0]["model"] == get_settings().openai_model_chat


@pytest.mark.asyncio
async def test_ai_reply_no_source_message_raises(session_factory) -> None:  # type: ignore[no-untyped-def]
    ctx, _bus, _ = await _make_ctx(session_factory)
    step = AIReplyStep(
        action="ai_reply",
        to_chat_id=-200,
        prompt_template="x: {message_text}",
    )
    with pytest.raises(ValueError, match="no source message"):
        await ai_reply.execute(step, ctx)
