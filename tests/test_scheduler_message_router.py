"""§10.9 — MessageRouter routes matching messages through Dispatcher."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler.dispatcher import Dispatcher
from tg_conductor.scheduler.message_router import MessageRouter
from tg_conductor.tg_core.fake import make_fake_message
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow

CHAT_ID = -1001234567890


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
            other_acc = await account_repo.upsert_session(
                session,
                owner_id=1,
                label="other",
                api_id=2,
                api_hash="h2",
                session_string="s2",
            )
            await session.commit()
            await _seed(
                factory,
                name="mm-main",
                account_id=acc.id,  # type: ignore[arg-type]
                trigger={
                    "type": "message_match",
                    "chat_id": CHAT_ID,
                    "text_pattern": "签到成功",
                },
            )
            await _seed(
                factory,
                name="mm-other-account",
                account_id=other_acc.id,  # type: ignore[arg-type]
                trigger={
                    "type": "message_match",
                    "chat_id": CHAT_ID,
                    "text_pattern": "签到成功",
                },
            )
            await _seed(
                factory,
                name="cron-noise",
                account_id=acc.id,  # type: ignore[arg-type]
                trigger={"type": "cron", "expression": "0 9 * * *"},
            )
        yield factory
    finally:
        await engine.dispose()


async def _seed(
    factory: async_sessionmaker[AsyncSession],
    *,
    name: str,
    account_id: int,
    trigger: dict,
) -> None:
    async with factory() as s, s.begin():
        await workflow_repo.upsert_by_source(
            s,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=Workflow.model_validate(
                {
                    "name": name,
                    "account_id": account_id,
                    "trigger": trigger,
                    "action_plan": {
                        "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}]
                    },
                }
            ),
        )


async def _main_account_id(factory: async_sessionmaker[AsyncSession]) -> int:
    async with factory() as session:
        accs = await account_repo.list_for_owner(session, owner_id=1)
    return next(a.id for a in accs if a.label == "main")  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_matching_message_dispatches_immediate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid = await _main_account_id(session_factory)
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    router = MessageRouter(
        session_factory=session_factory,
        dispatcher=dispatcher,
        owner_id=1,
        account_id=aid,
    )
    msg = make_fake_message(chat_id=CHAT_ID, text="账号 签到成功")
    created = await router.on_message(msg)
    assert len(created) == 1
    # Job is enqueued for the main account.
    assert dispatcher.queue_for(aid).qsize() == 1


@pytest.mark.asyncio
async def test_non_matching_message_does_not_dispatch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid = await _main_account_id(session_factory)
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    router = MessageRouter(
        session_factory=session_factory,
        dispatcher=dispatcher,
        owner_id=1,
        account_id=aid,
    )
    # Wrong text — pattern won't match.
    msg = make_fake_message(chat_id=CHAT_ID, text="happy birthday")
    created = await router.on_message(msg)
    assert created == []


@pytest.mark.asyncio
async def test_other_accounts_workflow_is_ignored(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Router for account=main must NOT dispatch mm-other-account's workflow."""
    aid_main = await _main_account_id(session_factory)
    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    router = MessageRouter(
        session_factory=session_factory,
        dispatcher=dispatcher,
        owner_id=1,
        account_id=aid_main,
    )
    msg = make_fake_message(chat_id=CHAT_ID, text="签到成功")
    created = await router.on_message(msg)
    # Only 1 dispatched even though 2 mm workflows are configured for owner=1.
    assert len(created) == 1


@pytest.mark.asyncio
async def test_disabled_workflow_skipped(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid = await _main_account_id(session_factory)
    async with session_factory() as session, session.begin():
        [target] = [
            wf
            for wf in await workflow_repo.list_for_owner(session, owner_id=1)
            if wf.name == "mm-main"
        ]
        await workflow_repo.set_enabled(
            session, workflow_id=target.id, owner_id=1, enabled=False
        )

    dispatcher = Dispatcher(session_factory=session_factory, owner_id=1)
    router = MessageRouter(
        session_factory=session_factory,
        dispatcher=dispatcher,
        owner_id=1,
        account_id=aid,
    )
    created = await router.on_message(
        make_fake_message(chat_id=CHAT_ID, text="签到成功")
    )
    assert created == []
