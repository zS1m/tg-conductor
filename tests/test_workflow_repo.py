"""§7.5 / §7.7 — Workflow repo: tenant isolation, source-aware sync, rr_counter."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.workflows import repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> async_sessionmaker[AsyncSession]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    # Seed a second owner so cross-owner tests work.
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
            # One account per owner so we have FK targets.
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


def _make_workflow(name: str, account_id: int = 1) -> Workflow:
    return Workflow.model_validate(
        {
            "name": name,
            "account_id": account_id,
            "trigger": {"type": "cron", "expression": "* * * * *"},
            "action_plan": {
                "steps": [{"action": "send_text", "chat_id": 1, "text": "hi"}]
            },
        }
    )


async def _account_id_for_owner(
    factory: async_sessionmaker[AsyncSession], owner_id: int
) -> int:
    async with factory() as session:
        rows = await account_repo.list_for_owner(session, owner_id=owner_id)
    assert rows[0].id is not None
    return rows[0].id


# ----------------------------------------------------------------- §7.7 dedup


@pytest.mark.asyncio
async def test_same_owner_duplicate_name_rejected(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    a1 = await _account_id_for_owner(session_factory, 1)
    async with session_factory() as session:
        await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=_make_workflow("daily", account_id=a1),
        )
        await session.commit()

    # Insert a *raw* row bypassing upsert_by_source (which would update in
    # place). The unique constraint must reject it.
    async with session_factory() as session:
        from tg_conductor.workflows.models import WorkflowRow

        session.add(
            WorkflowRow(
                owner_id=1,
                account_id=a1,
                name="daily",
                enabled=True,
                source=WorkflowSource.yaml,
                trigger={"type": "startup"},  # type: ignore[arg-type]
                action_plan={
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "x"}]
                },  # type: ignore[arg-type]
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.asyncio
async def test_cross_owner_same_name_allowed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    a1 = await _account_id_for_owner(session_factory, 1)
    a2 = await _account_id_for_owner(session_factory, 2)
    async with session_factory() as session:
        r1 = await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=_make_workflow("daily", account_id=a1),
        )
        r2 = await repo.upsert_by_source(
            session,
            owner_id=2,
            source=WorkflowSource.yaml,
            workflow=_make_workflow("daily", account_id=a2),
        )
        await session.commit()
    assert r1.id != r2.id
    assert r1.owner_id == 1 and r2.owner_id == 2


# ----------------------------------------------------------------- §7.5 upsert / source


@pytest.mark.asyncio
async def test_upsert_updates_row_preserves_id_and_source(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    a1 = await _account_id_for_owner(session_factory, 1)
    async with session_factory() as session:
        first = await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=_make_workflow("daily", account_id=a1),
        )
        await session.commit()
        original_id = first.id
        original_created = first.created_at

        # Same name, different trigger → updates in place.
        replacement = Workflow.model_validate(
            {
                "name": "daily",
                "account_id": a1,
                "trigger": {"type": "startup"},
                "action_plan": {
                    "steps": [{"action": "send_text", "chat_id": 2, "text": "v2"}]
                },
            }
        )
        second = await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=replacement,
        )
        await session.commit()
        assert second.id == original_id
        assert second.created_at == original_created
        # Trigger replaced (discriminator preserved as a real instance).
        assert second.trigger.type == "startup"


@pytest.mark.asyncio
async def test_upsert_refuses_cross_source_overwrite(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    a1 = await _account_id_for_owner(session_factory, 1)
    async with session_factory() as session:
        await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.api,
            workflow=_make_workflow("api-managed", account_id=a1),
        )
        await session.commit()

        with pytest.raises(ValueError, match="refusing to overwrite"):
            await repo.upsert_by_source(
                session,
                owner_id=1,
                source=WorkflowSource.yaml,
                workflow=_make_workflow("api-managed", account_id=a1),
            )


# ----------------------------------------------------------------- set_enabled


@pytest.mark.asyncio
async def test_set_enabled_flips_flag(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    a1 = await _account_id_for_owner(session_factory, 1)
    async with session_factory() as session:
        wf = await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=_make_workflow("x", account_id=a1),
        )
        await session.commit()
        ok = await repo.set_enabled(
            session,
            workflow_id=wf.id,
            owner_id=1,
            enabled=False,  # type: ignore[arg-type]
        )
        await session.commit()
    assert ok is True

    async with session_factory() as session:
        active = await repo.list_for_owner(session, owner_id=1, enabled_only=True)
        all_rows = await repo.list_for_owner(session, owner_id=1)
    assert active == []
    assert len(all_rows) == 1


# ----------------------------------------------------------------- rr_counter


@pytest.mark.asyncio
async def test_bump_rr_counter_returns_monotonically_increasing(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    a1 = await _account_id_for_owner(session_factory, 1)
    async with session_factory() as session:
        wf = await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=_make_workflow("rr", account_id=a1),
        )
        await session.commit()
        wf_id = wf.id

    values: list[int] = []
    async with session_factory() as session:
        for _ in range(5):
            v = await repo.bump_rr_counter(
                session,
                workflow_id=wf_id,
                owner_id=1,  # type: ignore[arg-type]
            )
            await session.commit()
            assert v is not None
            values.append(v)

    assert values == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_bump_rr_counter_unknown_returns_none(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        v = await repo.bump_rr_counter(session, workflow_id=999, owner_id=1)
        assert v is None


@pytest.mark.asyncio
async def test_bump_rr_counter_isolated_across_owners(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    a1 = await _account_id_for_owner(session_factory, 1)
    async with session_factory() as session:
        wf = await repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.yaml,
            workflow=_make_workflow("rr", account_id=a1),
        )
        await session.commit()
        # Bumping from the wrong owner must be a no-op (returns None).
        miss = await repo.bump_rr_counter(
            session,
            workflow_id=wf.id,
            owner_id=2,  # type: ignore[arg-type]
        )
        await session.commit()
    assert miss is None
