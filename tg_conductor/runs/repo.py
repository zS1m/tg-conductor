"""Tenant-aware persistence for :class:`RunRow` and :class:`RunEventRow`.

All public functions take ``owner_id`` and embed it in ``WHERE``.
Cross-tenant reads return ``None`` / empty list.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.runs.models import RunEventRow, RunRow, RunStatus, _utcnow


async def create_run(
    session: AsyncSession,
    *,
    owner_id: int,
    workflow_id: int,
    account_id: int,
    job_id: int,
    started_at: datetime | None = None,
) -> RunRow:
    row = RunRow(
        owner_id=owner_id,
        workflow_id=workflow_id,
        account_id=account_id,
        job_id=job_id,
        status=RunStatus.running,
        started_at=started_at or _utcnow(),
    )
    session.add(row)
    await session.flush()
    return row


async def get_by_id(session: AsyncSession, run_id: int, owner_id: int) -> RunRow | None:
    stmt = select(RunRow).where(RunRow.id == run_id, RunRow.owner_id == owner_id)
    return (await session.execute(stmt)).scalar_one_or_none()


async def list_for_owner(
    session: AsyncSession,
    owner_id: int,
    *,
    workflow_id: int | None = None,
    status: RunStatus | None = None,
    limit: int = 50,
) -> list[RunRow]:
    stmt = select(RunRow).where(RunRow.owner_id == owner_id)
    if workflow_id is not None:
        stmt = stmt.where(RunRow.workflow_id == workflow_id)
    if status is not None:
        stmt = stmt.where(RunRow.status == status)
    stmt = stmt.order_by(RunRow.started_at.desc()).limit(limit)
    return list((await session.execute(stmt)).scalars().all())


async def mark_finished(
    session: AsyncSession,
    *,
    run_id: int,
    owner_id: int,
    status: RunStatus,
    error: str | None = None,
    finished_at: datetime | None = None,
) -> bool:
    if status == RunStatus.running:
        raise ValueError("mark_finished requires a terminal status")
    stmt = (
        update(RunRow)
        .where(
            RunRow.id == run_id,
            RunRow.owner_id == owner_id,
            RunRow.status == RunStatus.running,
        )
        .values(
            status=status,
            error=error,
            finished_at=finished_at or datetime.now(UTC),
        )
    )
    return (await session.execute(stmt)).rowcount > 0


async def list_events(
    session: AsyncSession,
    *,
    run_id: int,
    owner_id: int,
    since: int = -1,
    limit: int = 1000,
) -> list[RunEventRow]:
    """Return events for ``run_id`` with ``seq > since``, ordered by seq.

    Default ``since=-1`` means "give me everything from the start" — seq=0
    is included. SSE clients reconnect with ``since=<last_seq>`` and get
    strictly-newer events (standard ``Last-Event-ID`` semantic).
    """
    # ``run_events.owner_id`` is denormalized from ``runs.owner_id`` so we
    # can filter in one statement without a join. Cross-tenant access on a
    # run not owned by ``owner_id`` returns an empty list (spec invariant).
    stmt = (
        select(RunEventRow)
        .where(
            RunEventRow.run_id == run_id,
            RunEventRow.owner_id == owner_id,
            RunEventRow.seq > since,
        )
        .order_by(RunEventRow.seq)
        .limit(limit)
    )
    return list((await session.execute(stmt)).scalars().all())
