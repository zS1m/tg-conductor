"""``/usage`` route — aggregated AI-call usage for the active owner.

§13.6 ``GET /usage?from=&to=&group_by=day|kind``

* ``from`` / ``to`` are ISO-8601 timestamps (URL-encoded). Both
  optional; omitted means "no lower / upper bound".
* ``group_by`` is ``day`` (default) or ``kind``.
* Response: list of buckets, each carrying ``calls`` / ``units`` /
  ``cost_micros`` (all integers). Bucket key is either ``date`` (string,
  ISO date) or ``kind`` (string).

Cross-owner reads are silently empty per ``CLAUDE.md`` multi-tenant
invariant — no 4xx leak.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.ai import repo as usage_repo
from tg_conductor.api.deps import get_owner_id, get_session_factory

router = APIRouter(prefix="/usage", tags=["usage"])


class UsageDayBucket(BaseModel):
    date: str
    calls: int
    units: int
    cost_micros: int


class UsageKindBucket(BaseModel):
    kind: str
    calls: int
    units: int
    cost_micros: int


@router.get("")
async def get_usage(
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None),
    group_by: Literal["day", "kind"] = Query(default="day"),
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> list[UsageDayBucket] | list[UsageKindBucket]:
    async with session_factory() as session:
        if group_by == "day":
            rows = await usage_repo.aggregate_by_day(
                session, owner_id=owner_id, since=from_, until=to
            )
            return [
                UsageDayBucket(
                    date=str(r["date"]),
                    calls=r["calls"],
                    units=r["units"],
                    cost_micros=r["cost_micros"],
                )
                for r in rows
            ]
        rows = await usage_repo.aggregate_by_kind(
            session, owner_id=owner_id, since=from_, until=to
        )
        return [
            UsageKindBucket(
                kind=r["kind"],
                calls=r["calls"],
                units=r["units"],
                cost_micros=r["cost_micros"],
            )
            for r in rows
        ]
