"""Daily expansion of ``time_window`` workflows into ``Job`` rows.

Runs at a configured time each day (default 00:05) and also on service
startup as a catch-up pass for any day that was missed while the process
was down. Idempotent on ``(workflow_id, expansion_date)``: if any job
already carries that pair, the workflow is skipped for that day.

The :func:`~tg_conductor.triggers.time_window.expand` algorithm handles the
"mid-day enablement" case — if ``now`` is past the configured window start,
fire_at values are clamped to ``[now, window_end]``.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, tzinfo

from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.scheduler import repo as job_repo
from tg_conductor.triggers.time_window import expand as expand_time_window
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.schema import TimeWindowTrigger

log = logging.getLogger(__name__)


@dataclass
class ExpansionReport:
    created_job_ids: list[int] = field(default_factory=list)
    skipped_workflows: list[int] = field(default_factory=list)
    expanded_workflows: list[int] = field(default_factory=list)


async def expand_daily(
    session: AsyncSession,
    *,
    owner_id: int,
    target_date: date,
    now: datetime | None = None,
    rng: random.Random | None = None,
    tz: tzinfo = UTC,
) -> ExpansionReport:
    """Iterate enabled time_window workflows for ``owner_id``; expand each into Jobs.

    A workflow whose ``(id, target_date)`` already has any Job row in the DB
    is skipped — the expander is safe to invoke repeatedly on the same day.
    """
    if now is None:
        now = datetime.now(tz)
    if rng is None:
        rng = random.Random()

    workflows = await workflow_repo.list_for_owner(
        session, owner_id=owner_id, enabled_only=True
    )

    report = ExpansionReport()
    for wf in workflows:
        if not isinstance(wf.trigger, TimeWindowTrigger):
            continue
        assert wf.id is not None

        existing_count = await job_repo.count_for_expansion(
            session, workflow_id=wf.id, expansion_date=target_date
        )
        if existing_count > 0:
            report.skipped_workflows.append(wf.id)
            continue

        fire_ats = expand_time_window(wf.trigger, target_date, now=now, rng=rng, tz=tz)
        if not fire_ats:
            log.info(
                "expander.no_fires workflow_id=%d date=%s (now past window or "
                "infeasible)",
                wf.id,
                target_date,
            )
            # Mark as expanded so we don't retry today — record a sentinel by
            # NOT creating jobs is enough because next call sees count=0 and
            # tries again. For v1, we accept that empty-result workflows are
            # re-evaluated each invocation; the cost is one schema validation
            # + window computation, no inserts.
            report.expanded_workflows.append(wf.id)
            continue

        for fire_at in fire_ats:
            row = await job_repo.create_pending(
                session,
                owner_id=owner_id,
                workflow_id=wf.id,
                account_id=wf.account_id,
                fire_at=fire_at,
                expansion_date=target_date,
            )
            assert row.id is not None
            report.created_job_ids.append(row.id)
        report.expanded_workflows.append(wf.id)

    await session.flush()
    if report.created_job_ids:
        log.info(
            "expander.batch owner_id=%d date=%s created=%d skipped_workflows=%d",
            owner_id,
            target_date,
            len(report.created_job_ids),
            len(report.skipped_workflows),
        )
    return report
