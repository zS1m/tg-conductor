"""Closures that satisfy :class:`tg_conductor.config_loader.sync.JobCancelHook`.

The §8 Reloader takes an ``on_deleted`` callable that runs inside the YAML
sync transaction. Wiring it to :func:`repo.cancel_pending_for_workflows`
makes "workflow removed from YAML" atomic with "its pending jobs cancel".
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.config_loader.sync import JobCancelHook
from tg_conductor.scheduler import repo as job_repo


def make_cancel_pending_jobs_hook(owner_id: int) -> JobCancelHook:
    """Build a hook that cancels all ``pending`` Jobs for the given workflow ids."""

    async def hook(session: AsyncSession, workflow_ids: list[int]) -> None:
        if not workflow_ids:
            return
        await job_repo.cancel_pending_for_workflows(
            session, workflow_ids=workflow_ids, owner_id=owner_id
        )

    return hook
