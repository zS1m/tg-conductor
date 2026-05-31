"""§8.4 兑现 — `on_deleted` hook 把 workflow 删除与 pending Job 取消放进同一事务."""

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
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.hooks import make_cancel_pending_jobs_hook
from tg_conductor.scheduler.models import JobStatus
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
                            "name": "doomed",
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


@pytest.mark.asyncio
async def test_reload_removal_cancels_pending_jobs(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    """Workflow disappears from YAML → its pending Jobs flip to canceled atomically."""
    # Seed 2 pending jobs for the doomed workflow.
    async with session_factory() as s:
        wfs = await workflow_repo.list_for_owner(s, owner_id=1)
        accs = await account_repo.list_for_owner(s, owner_id=1)
    doomed_id = wfs[0].id
    aid = accs[0].id
    async with session_factory() as s, s.begin():
        from datetime import UTC, datetime

        for _off in (5, 10):
            await job_repo.create_pending(
                s,
                owner_id=1,
                workflow_id=doomed_id,  # type: ignore[arg-type]
                account_id=aid,  # type: ignore[arg-type]
                fire_at=datetime.now(UTC).replace(microsecond=0),
            )

    # Now reload with EMPTY desired set → doomed workflow gets deleted.
    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=tmp_path,
        owner_id=1,
        on_deleted=make_cancel_pending_jobs_hook(owner_id=1),
        load_workflows=lambda _dir: LoadResult(workflows=[]),
    )
    result = await reloader.reload()
    assert result.sync.deleted == ["doomed"]

    # Pending jobs must now be canceled.
    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    assert len(jobs) == 2
    assert all(j.status == JobStatus.canceled for j in jobs)
    assert all(j.finished_at is not None for j in jobs)


@pytest.mark.asyncio
async def test_hook_does_not_touch_jobs_of_other_owners(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    """A hook bound to owner=1 must never cancel owner=2's jobs."""
    # Manually call the hook with owner=2 — pre-seeded jobs are for owner=1
    # and must remain pending.
    async with session_factory() as s:
        wfs = await workflow_repo.list_for_owner(s, owner_id=1)
        accs = await account_repo.list_for_owner(s, owner_id=1)
    wid = wfs[0].id
    aid = accs[0].id
    async with session_factory() as s, s.begin():
        from datetime import UTC, datetime

        await job_repo.create_pending(
            s,
            owner_id=1,
            workflow_id=wid,  # type: ignore[arg-type]
            account_id=aid,  # type: ignore[arg-type]
            fire_at=datetime.now(UTC),
        )

    hook = make_cancel_pending_jobs_hook(owner_id=2)
    async with session_factory() as session, session.begin():
        await hook(session, [wid])  # type: ignore[list-item]

    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1)
    assert all(j.status == JobStatus.pending for j in jobs)
