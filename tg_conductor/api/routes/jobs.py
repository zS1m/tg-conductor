"""``/jobs`` routes — scheduler queue + history reads."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.api.deps import get_owner_id, get_session_factory
from tg_conductor.api.schemas import JobResponse
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.models import JobStatus

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("", response_model=list[JobResponse])
async def list_jobs(
    workflow_id: int | None = Query(default=None),
    status: JobStatus | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> list[JobResponse]:
    async with session_factory() as session:
        rows = await job_repo.list_for_owner(
            session,
            owner_id,
            workflow_id=workflow_id,
            status=status,
            limit=limit,
        )
    return [JobResponse.from_row(r) for r in rows]


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: int,
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> JobResponse:
    async with session_factory() as session:
        row = await job_repo.get_by_id(session, job_id, owner_id)
    if row is None:
        raise HTTPException(status_code=404, detail="job not found")
    return JobResponse.from_row(row)
