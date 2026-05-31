"""Periodic evaluator for ``cron``-triggered workflows.

Called from the dispatcher loop every N seconds. For each enabled cron
workflow, looks up the most recent ``fire_at`` already in the jobs table
and advances through every cron occurrence in ``(last_fire, now]`` —
creating a ``pending`` Job for each. This catches up correctly after a
service downtime: if a daily cron should have fired three times while we
were down, three jobs queue up (some may then be skipped by the claim
phase's compensation window).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, tzinfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.models import JobRow
from tg_conductor.triggers.cron import next_fire_at
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.schema import CronTrigger

log = logging.getLogger(__name__)


@dataclass
class CronTickReport:
    created_job_ids: list[int] = field(default_factory=list)


async def tick(
    session: AsyncSession,
    *,
    owner_id: int,
    now: datetime | None = None,
    tz: tzinfo = UTC,
    lookback_minutes: float = 2.0,
    max_catchup_per_workflow: int = 60,
) -> CronTickReport:
    """Create Jobs for every cron occurrence in ``(last_fire, now]``.

    Cron expressions are interpreted in ``tz`` (wall-clock), then each
    ``fire_at`` is stored in UTC. The ``(last_fire, now]`` comparison stays
    entirely in UTC, so ``tz`` only affects *which* wall-clock instants the
    expression maps to — not the catch-up window arithmetic.
    """
    if now is None:
        now = datetime.now(UTC)

    workflows = await workflow_repo.list_for_owner(
        session, owner_id=owner_id, enabled_only=True
    )

    report = CronTickReport()
    for wf in workflows:
        if not isinstance(wf.trigger, CronTrigger):
            continue
        assert wf.id is not None

        last_fire = await _last_fire_at(session, workflow_id=wf.id)
        anchor = (
            last_fire
            if last_fire is not None
            else now - timedelta(minutes=lookback_minutes)
        )

        # Advance through any missed slots. Bounded by max_catchup_per_workflow
        # to keep a stuck cron expression (e.g. * * * * *) from producing an
        # unbounded burst on first tick after a long outage.
        produced = 0
        cursor = anchor
        while produced < max_catchup_per_workflow:
            next_fire = next_fire_at(wf.trigger.expression, cursor, tz=tz)
            if next_fire > now:
                break
            row = await job_repo.create_pending(
                session,
                owner_id=owner_id,
                workflow_id=wf.id,
                account_id=wf.account_id,
                fire_at=next_fire,
            )
            assert row.id is not None
            report.created_job_ids.append(row.id)
            cursor = next_fire
            produced += 1

        if produced == max_catchup_per_workflow:
            log.warning(
                "cron_tick.catchup_truncated workflow_id=%d (more than %d missed slots)",
                wf.id,
                max_catchup_per_workflow,
            )

    await session.flush()
    return report


async def _last_fire_at(session: AsyncSession, *, workflow_id: int) -> datetime | None:
    """Most recent ``fire_at`` across any-status Jobs for the given workflow."""
    stmt = select(func.max(JobRow.fire_at)).where(JobRow.workflow_id == workflow_id)
    result = (await session.execute(stmt)).scalar_one_or_none()
    return result
