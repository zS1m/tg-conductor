"""§7.6 — DB-aware Workflow validation (account_id reference checks)."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.workflows.schema import Workflow
from tg_conductor.workflows.validate import (
    WorkflowValidationError,
    validate_account_ref,
    validate_workflow,
)


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> async_sessionmaker[AsyncSession]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO owners (id, name, created_at) "
                "VALUES (2, 'other', '2026-01-01')"
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await account_repo.upsert_session(
                session,
                owner_id=1,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await account_repo.upsert_session(
                session,
                owner_id=2,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await session.commit()
        yield factory
    finally:
        await engine.dispose()


def _make_workflow(account_id: int):
    return Workflow.model_validate(
        {
            "name": "x",
            "account_id": account_id,
            "trigger": {"type": "startup"},
            "action_plan": {
                "steps": [{"action": "send_text", "chat_id": 1, "text": "hi"}]
            },
        }
    )


@pytest.mark.asyncio
async def test_existing_active_account_passes(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        accts = await account_repo.list_for_owner(session, owner_id=1)
        await validate_account_ref(
            session,
            owner_id=1,
            account_id=accts[0].id,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_missing_account_rejected(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        with pytest.raises(WorkflowValidationError, match="does not exist"):
            await validate_account_ref(session, owner_id=1, account_id=9999)


@pytest.mark.asyncio
async def test_account_from_other_owner_rejected(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """owner=1 referencing owner=2's account_id reads as "not exists" — spec invariant."""
    async with session_factory() as session:
        accts_2 = await account_repo.list_for_owner(session, owner_id=2)
        with pytest.raises(WorkflowValidationError, match="does not exist"):
            await validate_account_ref(
                session,
                owner_id=1,
                account_id=accts_2[0].id,  # type: ignore[arg-type]
            )


@pytest.mark.asyncio
async def test_disabled_account_rejected(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        [acc] = await account_repo.list_for_owner(session, owner_id=1)
        await account_repo.mark_disabled(
            session,
            acc.id,
            owner_id=1,  # type: ignore[arg-type]
        )
        await session.commit()

        with pytest.raises(WorkflowValidationError, match="disabled"):
            await validate_account_ref(
                session,
                owner_id=1,
                account_id=acc.id,  # type: ignore[arg-type]
            )


@pytest.mark.asyncio
async def test_validate_workflow_happy_path(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        [acc] = await account_repo.list_for_owner(session, owner_id=1)
        await validate_workflow(
            session,
            owner_id=1,
            workflow=_make_workflow(acc.id),  # type: ignore[arg-type]
        )
