"""External control hooks: ``startup`` trigger spawn + ``trigger_now`` + ``cancel``.

These are user / system entry points (CLI, HTTP API, lifespan hooks) that
need to interact with the queue. The dispatcher loop itself stays in
:mod:`scheduler.dispatcher` (Commit 3).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.scheduler import repo as job_repo
from tg_conductor.triggers.startup import StartupTracker
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.schema import StartupTrigger

log = logging.getLogger(__name__)


@dataclass
class StartupSpawnReport:
    created_job_ids: list[int] = field(default_factory=list)
    already_fired: bool = False


async def spawn_startup_jobs(
    session: AsyncSession,
    *,
    owner_id: int,
    tracker: StartupTracker,
    now: datetime | None = None,
) -> StartupSpawnReport:
    """Create one ``pending`` Job per enabled startup workflow — exactly once per process.

    The ``tracker`` is consumed on the first successful call; subsequent
    calls (e.g. from a misconfigured caller that wires this into the reload
    path) return ``already_fired=True`` without creating jobs.
    """
    if not tracker.consume():
        return StartupSpawnReport(already_fired=True)

    if now is None:
        now = datetime.now(UTC)

    workflows = await workflow_repo.list_for_owner(
        session, owner_id=owner_id, enabled_only=True
    )
    report = StartupSpawnReport()
    for wf in workflows:
        if not isinstance(wf.trigger, StartupTrigger):
            continue
        assert wf.id is not None
        row = await job_repo.create_pending(
            session,
            owner_id=owner_id,
            workflow_id=wf.id,
            account_id=wf.account_id,
            fire_at=now,
        )
        assert row.id is not None
        report.created_job_ids.append(row.id)
    await session.flush()
    if report.created_job_ids:
        log.info(
            "control.startup_spawn owner_id=%d count=%d",
            owner_id,
            len(report.created_job_ids),
        )
    return report


async def trigger_now(
    session: AsyncSession,
    *,
    workflow_id: int,
    owner_id: int,
) -> int | None:
    """Fire a workflow immediately. Returns the new Job's id or ``None``.

    ``None`` means: workflow does not exist, belongs to a different owner,
    or is disabled. Caller should map this to 404 (HTTP) / non-zero
    (CLI) accordingly.
    """
    wf = await workflow_repo.get_by_id(
        session, workflow_id=workflow_id, owner_id=owner_id
    )
    if wf is None or not wf.enabled:
        return None
    assert wf.id is not None
    row = await job_repo.create_pending(
        session,
        owner_id=owner_id,
        workflow_id=wf.id,
        account_id=wf.account_id,
        fire_at=datetime.now(UTC),
    )
    return row.id
