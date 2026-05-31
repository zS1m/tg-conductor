"""``/workflows`` routes — read-only list + fetch.

POST/PUT/PATCH/DELETE are intentionally absent in v1 (CLAUDE.md "配置入口"
invariant: only YAML + ``POST /reload`` may mutate Workflow rows).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.api.deps import get_owner_id, get_session_factory
from tg_conductor.api.schemas import WorkflowResponse
from tg_conductor.workflows import repo as workflow_repo

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("", response_model=list[WorkflowResponse])
async def list_workflows(
    enabled_only: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> list[WorkflowResponse]:
    async with session_factory() as session:
        rows = await workflow_repo.list_for_owner(
            session, owner_id=owner_id, enabled_only=enabled_only
        )
    return [WorkflowResponse.from_row(r) for r in rows[:limit]]


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(
    workflow_id: int,
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> WorkflowResponse:
    async with session_factory() as session:
        row = await workflow_repo.get_by_id(
            session, workflow_id=workflow_id, owner_id=owner_id
        )
    if row is None:
        raise HTTPException(status_code=404, detail="workflow not found")
    return WorkflowResponse.from_row(row)
