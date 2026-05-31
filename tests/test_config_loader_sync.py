"""§8.3 / §8.4 / §8.10 / §8.11 — sync_to_db diff + on_deleted hook + rollback."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.config_loader.sync import SyncReport, sync_to_db
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
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
            await account_repo.upsert_session(
                session,
                owner_id=1,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await session.commit()
        yield factory
    finally:
        await engine.dispose()


def _wf(name: str, account_id: int = 1, *, chat_text: str = "hi") -> Workflow:
    return Workflow.model_validate(
        {
            "name": name,
            "account_id": account_id,
            "trigger": {"type": "startup"},
            "action_plan": {
                "steps": [{"action": "send_text", "chat_id": 1, "text": chat_text}]
            },
        }
    )


async def _account_id(factory: async_sessionmaker[AsyncSession]) -> int:
    async with factory() as session:
        rows = await account_repo.list_for_owner(session, owner_id=1)
    return rows[0].id  # type: ignore[return-value]


async def _run_sync(
    factory: async_sessionmaker[AsyncSession],
    desired: list[Workflow],
    on_deleted: Any = None,
) -> SyncReport:
    async with factory() as session, session.begin():
        return await sync_to_db(
            session,
            owner_id=1,
            desired=desired,
            on_deleted=on_deleted,
        )


# ----------------------------------------------------------------- inserts


@pytest.mark.asyncio
async def test_first_sync_creates_all(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid = await _account_id(session_factory)
    report = await _run_sync(session_factory, [_wf("a", aid), _wf("b", aid)])
    assert sorted(report.created) == ["a", "b"]
    assert report.updated == []
    assert report.deleted == []
    assert report.validation_errors == []

    async with session_factory() as session:
        rows = await workflow_repo.list_for_owner(session, owner_id=1)
    assert sorted(r.name for r in rows) == ["a", "b"]
    assert all(r.source == WorkflowSource.yaml for r in rows)


@pytest.mark.asyncio
async def test_second_sync_with_same_set_updates_all(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid = await _account_id(session_factory)
    await _run_sync(session_factory, [_wf("a", aid)])
    report = await _run_sync(session_factory, [_wf("a", aid, chat_text="v2")])
    assert report.updated == ["a"]
    assert report.created == []
    assert report.deleted == []


# ----------------------------------------------------------------- §8.4 / §8.10


@pytest.mark.asyncio
async def test_removed_workflow_is_deleted_and_calls_hook(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid = await _account_id(session_factory)
    await _run_sync(session_factory, [_wf("a", aid), _wf("b", aid)])

    received: list[list[int]] = []

    async def hook(_session: AsyncSession, ids: list[int]) -> None:
        received.append(ids)

    report = await _run_sync(
        session_factory,
        [_wf("b", aid)],
        on_deleted=hook,
    )
    assert report.deleted == ["a"]
    assert len(report.deleted_ids) == 1
    assert received == [report.deleted_ids]


@pytest.mark.asyncio
async def test_api_source_rows_untouched_by_yaml_sync(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §8.10 scenario: YAML drops A; an api-managed B stays put."""
    aid = await _account_id(session_factory)
    # Seed an api-source row directly via the repo.
    async with session_factory() as session, session.begin():
        await workflow_repo.upsert_by_source(
            session,
            owner_id=1,
            source=WorkflowSource.api,
            workflow=_wf("api-managed", aid),
        )

    # Seed a yaml-source row, then drop it via empty sync.
    await _run_sync(session_factory, [_wf("yaml-only", aid)])
    report = await _run_sync(session_factory, [])
    assert report.deleted == ["yaml-only"]

    async with session_factory() as session:
        all_rows = await workflow_repo.list_for_owner(session, owner_id=1)
    names = sorted(r.name for r in all_rows)
    sources = {r.name: r.source for r in all_rows}
    assert names == ["api-managed"]
    assert sources["api-managed"] == WorkflowSource.api


# ----------------------------------------------------------------- validation


@pytest.mark.asyncio
async def test_invalid_account_ref_reported_not_written(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    report = await _run_sync(session_factory, [_wf("bad", account_id=9999)])
    assert report.validation_errors
    [(name, msg)] = report.validation_errors
    assert name == "bad"
    assert "9999" in msg

    async with session_factory() as session:
        rows = await workflow_repo.list_for_owner(session, owner_id=1)
    assert rows == []


@pytest.mark.asyncio
async def test_invalid_workflow_keeps_existing_same_name_row(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A row that fails validation MUST NOT cause its existing twin to be deleted."""
    aid = await _account_id(session_factory)
    await _run_sync(session_factory, [_wf("survivor", aid)])

    # Now propose a bad version of "survivor" — should be skipped, row kept.
    bad = _wf("survivor", account_id=9999)
    report = await _run_sync(session_factory, [bad])
    assert report.validation_errors and report.validation_errors[0][0] == "survivor"
    assert report.deleted == []

    async with session_factory() as session:
        rows = await workflow_repo.list_for_owner(session, owner_id=1)
    assert [r.name for r in rows] == ["survivor"]


# ----------------------------------------------------------------- §8.11 rollback


@pytest.mark.asyncio
async def test_transaction_rollback_on_mid_sync_failure(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """If a write fails partway through sync, NO change must persist."""
    aid = await _account_id(session_factory)
    # Pre-seed two valid rows.
    await _run_sync(session_factory, [_wf("keep", aid), _wf("alsokeep", aid)])

    # Now run a sync that intentionally fails after the first delete: we
    # simulate by injecting an on_deleted hook that raises.
    async def boom(_session: AsyncSession, _ids: list[int]) -> None:
        raise RuntimeError("simulated downstream failure")

    with pytest.raises(RuntimeError, match="simulated downstream failure"):
        await _run_sync(session_factory, [], on_deleted=boom)

    # State must be unchanged.
    async with session_factory() as session:
        rows = await workflow_repo.list_for_owner(session, owner_id=1)
    assert sorted(r.name for r in rows) == ["alsokeep", "keep"]


@pytest.mark.asyncio
async def test_no_change_count_when_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid = await _account_id(session_factory)
    await _run_sync(session_factory, [_wf("x", aid)])
    second = await _run_sync(session_factory, [_wf("x", aid)])
    # The current sync semantics: same input is reported as "updated"
    # (timestamp bump). created/deleted are stable.
    assert second.created == []
    assert second.deleted == []
    assert second.updated == ["x"]


# Silence "unused import" lint for `text` (kept for future raw-SQL setups).
_ = text
