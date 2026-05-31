"""Tenant-aware Job persistence + atomic dispatch primitives.

The key piece here is :func:`claim_due_jobs`: a two-phase batched
``UPDATE ... RETURNING`` that

1. retires ``pending`` jobs whose ``fire_at`` is older than the
   compensation window (status → ``skipped`` with
   ``skip_reason="missed_window"``), then
2. flips up to ``batch_size`` of the still-fresh ``pending`` jobs to
   ``running``, returning the claimed rows.

The double-check (``WHERE status='pending'`` in the UPDATE) makes the claim
race-safe: two concurrent dispatchers that select overlapping candidate
sets can both run their UPDATEs without either of them claiming the same
row twice. SQLite serializes writes anyway, but the SQL stays correct
across stronger backends too.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.scheduler.models import JobRow, JobStatus, _utcnow


@dataclass
class ClaimResult:
    claimed: list[JobRow] = field(default_factory=list)
    skipped: list[JobRow] = field(default_factory=list)


# --------------------------------------------------------------- basic CRUD


async def create_pending(
    session: AsyncSession,
    *,
    owner_id: int,
    workflow_id: int,
    account_id: int,
    fire_at: datetime,
    variant_id: str | None = None,
    resolved_payload: dict | None = None,
    expansion_date: date | None = None,
) -> JobRow:
    row = JobRow(
        owner_id=owner_id,
        workflow_id=workflow_id,
        account_id=account_id,
        fire_at=fire_at,
        status=JobStatus.pending,
        variant_id=variant_id,
        resolved_payload=resolved_payload,
        expansion_date=expansion_date,
    )
    session.add(row)
    await session.flush()
    return row


async def get_by_id(session: AsyncSession, job_id: int, owner_id: int) -> JobRow | None:
    stmt = select(JobRow).where(JobRow.id == job_id, JobRow.owner_id == owner_id)
    return (await session.execute(stmt)).scalar_one_or_none()


async def list_for_owner(
    session: AsyncSession,
    owner_id: int,
    *,
    workflow_id: int | None = None,
    status: JobStatus | None = None,
    limit: int = 100,
) -> list[JobRow]:
    stmt = select(JobRow).where(JobRow.owner_id == owner_id)
    if workflow_id is not None:
        stmt = stmt.where(JobRow.workflow_id == workflow_id)
    if status is not None:
        stmt = stmt.where(JobRow.status == status)
    stmt = stmt.order_by(JobRow.fire_at).limit(limit)
    return list((await session.execute(stmt)).scalars().all())


async def count_for_expansion(
    session: AsyncSession,
    *,
    workflow_id: int,
    expansion_date: date,
) -> int:
    """Return the number of jobs already produced by this workflow on this date.

    Used by the daily expander for idempotency: if > 0, the expander skips
    creating more jobs for ``(workflow_id, expansion_date)``.
    """
    stmt = select(func.count(JobRow.id)).where(
        JobRow.workflow_id == workflow_id,
        JobRow.expansion_date == expansion_date,
    )
    return int((await session.execute(stmt)).scalar_one())


# --------------------------------------------------------------- state transitions


async def cancel_pending(
    session: AsyncSession,
    *,
    job_id: int,
    owner_id: int,
) -> bool:
    """Flip ``pending → canceled``. No-op (returns ``False``) on other states."""
    stmt = (
        update(JobRow)
        .where(
            JobRow.id == job_id,
            JobRow.owner_id == owner_id,
            JobRow.status == JobStatus.pending,
        )
        .values(status=JobStatus.canceled, finished_at=_utcnow())
    )
    return (await session.execute(stmt)).rowcount > 0


async def cancel_pending_for_workflows(
    session: AsyncSession,
    *,
    workflow_ids: list[int],
    owner_id: int,
) -> int:
    """Bulk-cancel every ``pending`` Job belonging to any of ``workflow_ids``.

    Used by the §8 ``on_deleted`` hook when YAML reload removes a workflow:
    every pending job derived from that workflow flips to ``canceled`` in
    the same transaction as the workflow row's delete.
    """
    if not workflow_ids:
        return 0
    stmt = (
        update(JobRow)
        .where(
            JobRow.owner_id == owner_id,
            JobRow.workflow_id.in_(workflow_ids),
            JobRow.status == JobStatus.pending,
        )
        .values(status=JobStatus.canceled, finished_at=_utcnow())
    )
    return (await session.execute(stmt)).rowcount


async def mark_failed(
    session: AsyncSession,
    *,
    job_id: int,
    owner_id: int,
    reason: str,
    run_id: int | None = None,
) -> bool:
    values: dict = {
        "status": JobStatus.failed,
        "skip_reason": reason,
        "finished_at": _utcnow(),
    }
    if run_id is not None:
        values["run_id"] = run_id
    stmt = (
        update(JobRow)
        .where(JobRow.id == job_id, JobRow.owner_id == owner_id)
        .values(**values)
    )
    return (await session.execute(stmt)).rowcount > 0


async def mark_succeeded(
    session: AsyncSession,
    *,
    job_id: int,
    owner_id: int,
    run_id: int | None = None,
) -> bool:
    values: dict = {
        "status": JobStatus.succeeded,
        "finished_at": _utcnow(),
    }
    if run_id is not None:
        values["run_id"] = run_id
    stmt = (
        update(JobRow)
        .where(JobRow.id == job_id, JobRow.owner_id == owner_id)
        .values(**values)
    )
    return (await session.execute(stmt)).rowcount > 0


# --------------------------------------------------------------- atomic claim


async def claim_due_jobs(
    session: AsyncSession,
    *,
    owner_id: int,
    now: datetime | None = None,
    batch_size: int = 50,
    compensation_window_seconds: float = 300.0,
) -> ClaimResult:
    """Two-phase batched claim: skip too-old, then flip ready → running."""
    if now is None:
        now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=compensation_window_seconds)

    # Phase A: retire pendings older than the compensation window.
    skip_stmt = (
        update(JobRow)
        .where(
            JobRow.owner_id == owner_id,
            JobRow.status == JobStatus.pending,
            JobRow.fire_at < cutoff,
        )
        .values(
            status=JobStatus.skipped,
            skip_reason="missed_window",
            finished_at=now,
        )
        .returning(JobRow)
    )
    skipped = list((await session.execute(skip_stmt)).scalars().all())

    # Phase B: candidate selection — fresh pendings whose fire_at has arrived.
    candidate_stmt = (
        select(JobRow.id)
        .where(
            JobRow.owner_id == owner_id,
            JobRow.status == JobStatus.pending,
            JobRow.fire_at <= now,
            JobRow.fire_at >= cutoff,
        )
        .order_by(JobRow.fire_at)
        .limit(batch_size)
    )
    candidate_ids = list((await session.execute(candidate_stmt)).scalars().all())
    if not candidate_ids:
        return ClaimResult(claimed=[], skipped=skipped)

    # Phase B continued: claim. The status double-check makes the UPDATE
    # safe against concurrent claims that selected overlapping candidates.
    claim_stmt = (
        update(JobRow)
        .where(
            JobRow.id.in_(candidate_ids),
            JobRow.status == JobStatus.pending,
        )
        .values(status=JobStatus.running, started_at=now)
        .returning(JobRow)
    )
    claimed = list((await session.execute(claim_stmt)).scalars().all())
    return ClaimResult(claimed=claimed, skipped=skipped)
