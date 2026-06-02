"""``account_needs_updates`` — derive receiving vs send-only from workflows.

Covers the design D2 matrix: cron-only / time_window-only / startup-only →
send-only; message_match / wait_for (incl. inside variants) / ai_reply-with-
prior-wait_for → receiving; no workflow → send-only; disabled rows ignored;
per-account isolation.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow
from tg_conductor.workflows.update_mode import account_needs_updates


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
            # Two accounts so per-account aggregation is exercised.
            await account_repo.upsert_session(
                session, owner_id=1, label="a1", api_id=1, api_hash="h",
                session_string="s",
            )
            await account_repo.upsert_session(
                session, owner_id=1, label="a2", api_id=1, api_hash="h",
                session_string="s",
            )
            await session.commit()
        yield factory
    finally:
        await engine.dispose()


_CRON = {"type": "cron", "expression": "* * * * *"}
_TIME_WINDOW = {
    "type": "time_window",
    "window": "09:00-18:00",
    "count": 1,
    "min_gap": "30m",
}
_STARTUP = {"type": "startup"}
_MESSAGE_MATCH = {"type": "message_match", "chat_id": -100123}
_SEND_TEXT = {"action": "send_text", "chat_id": 1, "text": "hi"}
_WAIT_FOR = {"action": "wait_for", "chat_id": -100123}
_AI_REPLY = {"action": "ai_reply", "to_chat_id": 1, "prompt_template": "p"}


async def _add(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    name: str,
    trigger: dict,
    steps: list[dict] | None = None,
    variants: list[dict] | None = None,
    account_id: int = 1,
    enabled: bool = True,
) -> None:
    plan: dict = {}
    if variants is not None:
        plan = {"variants": variants, "pick_variant": "random"}
    else:
        plan = {"steps": steps if steps is not None else [_SEND_TEXT]}
    wf = Workflow.model_validate(
        {
            "name": name,
            "account_id": account_id,
            "enabled": enabled,
            "trigger": trigger,
            "action_plan": plan,
        }
    )
    async with session_factory() as session, session.begin():
        await workflow_repo.upsert_by_source(
            session, owner_id=1, source=WorkflowSource.yaml, workflow=wf
        )


async def _needs(session_factory, account_id: int = 1) -> bool:
    async with session_factory() as session:
        return await account_needs_updates(
            session, owner_id=1, account_id=account_id
        )


@pytest.mark.asyncio
async def test_no_workflow_is_send_only(session_factory) -> None:
    assert await _needs(session_factory) is False


@pytest.mark.asyncio
async def test_cron_only_is_send_only(session_factory) -> None:
    await _add(session_factory, name="c", trigger=_CRON)
    assert await _needs(session_factory) is False


@pytest.mark.asyncio
async def test_time_window_only_is_send_only(session_factory) -> None:
    await _add(session_factory, name="tw", trigger=_TIME_WINDOW)
    assert await _needs(session_factory) is False


@pytest.mark.asyncio
async def test_startup_only_is_send_only(session_factory) -> None:
    await _add(session_factory, name="su", trigger=_STARTUP)
    assert await _needs(session_factory) is False


@pytest.mark.asyncio
async def test_message_match_trigger_is_receiving(session_factory) -> None:
    await _add(session_factory, name="mm", trigger=_MESSAGE_MATCH)
    assert await _needs(session_factory) is True


@pytest.mark.asyncio
async def test_cron_with_wait_for_step_is_receiving(session_factory) -> None:
    await _add(
        session_factory, name="cw", trigger=_CRON, steps=[_WAIT_FOR, _SEND_TEXT]
    )
    assert await _needs(session_factory) is True


@pytest.mark.asyncio
async def test_wait_for_inside_variant_is_receiving(session_factory) -> None:
    await _add(
        session_factory,
        name="vw",
        trigger=_CRON,
        variants=[
            {"id": "v1", "steps": [_SEND_TEXT]},
            {"id": "v2", "steps": [_WAIT_FOR]},
        ],
    )
    assert await _needs(session_factory) is True


@pytest.mark.asyncio
async def test_message_match_with_ai_reply_is_receiving(session_factory) -> None:
    # ai_reply always rides on a message_match / wait_for workflow.
    await _add(
        session_factory, name="air", trigger=_MESSAGE_MATCH, steps=[_AI_REPLY]
    )
    assert await _needs(session_factory) is True


@pytest.mark.asyncio
async def test_disabled_message_match_does_not_flip_to_receiving(
    session_factory,
) -> None:
    await _add(session_factory, name="mm", trigger=_MESSAGE_MATCH, enabled=False)
    assert await _needs(session_factory) is False


@pytest.mark.asyncio
async def test_per_account_isolation(session_factory) -> None:
    # account 1 cron-only; account 2 message_match.
    await _add(session_factory, name="c1", trigger=_CRON, account_id=1)
    await _add(session_factory, name="m2", trigger=_MESSAGE_MATCH, account_id=2)
    assert await _needs(session_factory, account_id=1) is False
    assert await _needs(session_factory, account_id=2) is True
