"""§8.5 / §8.12 — Reloader single-flight + concurrent-call coalescing."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.config_loader.parser import LoadResult
from tg_conductor.config_loader.reload import Reloader
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
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


def _wf(name: str) -> Workflow:
    return Workflow.model_validate(
        {
            "name": name,
            "account_id": 1,
            "trigger": {"type": "startup"},
            "action_plan": {
                "steps": [{"action": "send_text", "chat_id": 1, "text": "hi"}]
            },
        }
    )


@pytest.mark.asyncio
async def test_reload_applies_loaded_workflows(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    async with session_factory() as session:
        accounts = await account_repo.list_for_owner(session=session, owner_id=1)
    aid = accounts[0].id
    assert aid is not None
    loaded = LoadResult(workflows=[(Path("fake"), _wf("alpha"))])

    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=tmp_path,
        owner_id=1,
        load_workflows=lambda _dir: loaded,
    )
    result = await reloader.reload()
    assert result.sync.created == ["alpha"]
    assert result.parse_errors == []


@pytest.mark.asyncio
async def test_reload_propagates_parse_errors(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    from tg_conductor.config_loader.parser import FileError

    loaded = LoadResult(
        workflows=[],
        errors=[FileError(path=Path("broken.yaml"), field_path="", message="oops")],
    )
    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=tmp_path,
        owner_id=1,
        load_workflows=lambda _dir: loaded,
    )
    result = await reloader.reload()
    assert result.parse_errors
    assert result.parse_errors[0].message == "oops"
    assert result.has_errors


# ----------------------------------------------------------------- §8.12


@pytest.mark.asyncio
async def test_concurrent_reloads_coalesce_to_two_executions(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    """5 simultaneous reload() calls → at most 2 actual underlying runs."""
    call_count = 0
    gate = asyncio.Event()

    def slow_loader(_dir: Path) -> LoadResult:
        nonlocal call_count
        call_count += 1
        return LoadResult(workflows=[])

    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=tmp_path,
        owner_id=1,
        load_workflows=slow_loader,
    )

    # Patch _execute to add an awaitable so we control overlap precisely.
    original_execute = reloader._execute

    async def slow_execute():  # type: ignore[no-untyped-def]
        await gate.wait()
        return await original_execute()

    reloader._execute = slow_execute  # type: ignore[assignment]

    # Fire 5 concurrent reloads.
    tasks = [asyncio.create_task(reloader.reload()) for _ in range(5)]
    # Yield so they all enter and queue up.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    # Now release them.
    gate.set()
    results = await asyncio.gather(*tasks)

    assert len(results) == 5
    # All 5 callers must have received a result.
    assert all(r.sync.change_count == 0 for r in results)
    # The coalesce rule: at most 2 underlying runs (1 running + 1 pending).
    assert call_count <= 2, f"expected ≤ 2 underlying runs, got {call_count}"


@pytest.mark.asyncio
async def test_duplicate_workflow_name_across_files_is_caught(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    """Two files with the same ``name``: first wins, second emits a parse error."""
    from tg_conductor.config_loader.parser import FileError

    first = Path("aa-good.yaml")
    second = Path("bb-clash.yaml")
    loaded = LoadResult(workflows=[(first, _wf("shared")), (second, _wf("shared"))])
    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=tmp_path,
        owner_id=1,
        load_workflows=lambda _dir: loaded,
    )
    result = await reloader.reload()

    # First file wins → exactly one row created.
    assert result.sync.created == ["shared"]
    # Second file gets a parse error pointing at its path.
    dupes = [e for e in result.parse_errors if isinstance(e, FileError)]
    assert len(dupes) == 1
    assert dupes[0].path == second
    assert "duplicate" in dupes[0].message.lower()
    assert str(first) in dupes[0].message


@pytest.mark.asyncio
async def test_sequential_reloads_each_get_own_execution(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    """When calls are spaced out (no overlap), each gets its own run."""
    call_count = 0

    def loader(_dir: Path) -> LoadResult:
        nonlocal call_count
        call_count += 1
        return LoadResult(workflows=[])

    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=tmp_path,
        owner_id=1,
        load_workflows=loader,
    )
    await reloader.reload()
    await reloader.reload()
    await reloader.reload()
    assert call_count == 3
