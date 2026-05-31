"""Tenant-aware persistence for ``usage_events``.

All queries take ``owner_id`` and inject it in ``WHERE`` per the
multi-tenant invariant. Cross-owner reads return empty.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.ai.usage import UsageRow


async def record_usage(
    session: AsyncSession,
    *,
    owner_id: int,
    kind: str,
    units: int,
    cost_micros: int,
    run_id: int | None = None,
    workflow_id: int | None = None,
    account_id: int | None = None,
    meta: dict[str, Any] | None = None,
    ts: datetime | None = None,
) -> UsageRow:
    row = UsageRow(
        owner_id=owner_id,
        kind=kind,
        units=units,
        cost_micros=cost_micros,
        run_id=run_id,
        workflow_id=workflow_id,
        account_id=account_id,
        meta=meta,
    )
    if ts is not None:
        row.ts = ts
    session.add(row)
    await session.flush()
    return row


async def list_for_owner(
    session: AsyncSession,
    *,
    owner_id: int,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 200,
) -> list[UsageRow]:
    stmt = select(UsageRow).where(UsageRow.owner_id == owner_id)
    if since is not None:
        stmt = stmt.where(UsageRow.ts >= since)
    if until is not None:
        stmt = stmt.where(UsageRow.ts < until)
    stmt = stmt.order_by(UsageRow.ts.desc()).limit(limit)
    return list((await session.execute(stmt)).scalars().all())


async def aggregate_by_day(
    session: AsyncSession,
    *,
    owner_id: int,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[dict[str, Any]]:
    """Group usage by UTC date. Returns ``[{date, calls, units, cost_micros}, ...]``."""
    day = func.date(UsageRow.ts).label("day")
    stmt = select(
        day,
        func.count(UsageRow.id).label("calls"),
        func.coalesce(func.sum(UsageRow.units), 0).label("units"),
        func.coalesce(func.sum(UsageRow.cost_micros), 0).label("cost_micros"),
    ).where(UsageRow.owner_id == owner_id)
    if since is not None:
        stmt = stmt.where(UsageRow.ts >= since)
    if until is not None:
        stmt = stmt.where(UsageRow.ts < until)
    stmt = stmt.group_by(day).order_by(day)
    rows = (await session.execute(stmt)).all()
    return [
        {
            "date": r.day,
            "calls": int(r.calls),
            "units": int(r.units),
            "cost_micros": int(r.cost_micros),
        }
        for r in rows
    ]


async def aggregate_by_kind(
    session: AsyncSession,
    *,
    owner_id: int,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[dict[str, Any]]:
    stmt = select(
        UsageRow.kind,
        func.count(UsageRow.id).label("calls"),
        func.coalesce(func.sum(UsageRow.units), 0).label("units"),
        func.coalesce(func.sum(UsageRow.cost_micros), 0).label("cost_micros"),
    ).where(UsageRow.owner_id == owner_id)
    if since is not None:
        stmt = stmt.where(UsageRow.ts >= since)
    if until is not None:
        stmt = stmt.where(UsageRow.ts < until)
    stmt = stmt.group_by(UsageRow.kind).order_by(UsageRow.kind)
    rows = (await session.execute(stmt)).all()
    return [
        {
            "kind": r.kind,
            "calls": int(r.calls),
            "units": int(r.units),
            "cost_micros": int(r.cost_micros),
        }
        for r in rows
    ]
