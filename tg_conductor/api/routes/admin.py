"""Mutating control endpoints — ``POST /reload`` and ``POST /workflows/{id}/run``.

These are the *only* HTTP entry points that change server state in v1.
The Workflow-config invariant from ``CLAUDE.md`` (yaml + reload only)
is preserved: ``/reload`` re-reads the on-disk yaml; ``/workflows/{id}/run``
just enqueues a Job — it doesn't mutate the workflow row.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.api.deps import get_owner_id, get_session_factory
from tg_conductor.scheduler.control import trigger_now
from tg_conductor.workflows import repo as workflow_repo

router = APIRouter(tags=["admin"])


class ReloadResponse(BaseModel):
    created: int
    updated: int
    deleted: int
    parse_errors: list[dict[str, Any]] = []
    validation_errors: list[dict[str, Any]] = []


class TriggerNowResponse(BaseModel):
    job_id: int


@router.post("/reload", response_model=ReloadResponse)
async def post_reload(request: Request) -> ReloadResponse:
    """spec config-loader §"POST /reload" — re-read the workflow_dir.

    Requires a :class:`Reloader` attached at ``app.state.reloader`` —
    the §16 lifespan wires it. If absent (e.g. a bare ``create_app`` in
    tests), this returns 503.
    """
    reloader = getattr(request.app.state, "reloader", None)
    if reloader is None:
        raise HTTPException(
            status_code=503,
            detail="reload is not available — Reloader not configured",
        )
    result = await reloader.reload()
    return ReloadResponse(
        created=len(result.sync.created),
        updated=len(result.sync.updated),
        deleted=len(result.sync.deleted),
        parse_errors=[
            {
                "path": str(e.path),
                "field_path": e.field_path,
                "message": e.message,
            }
            for e in result.parse_errors
        ],
        validation_errors=[
            {"name": name, "message": message}
            for name, message in result.sync.validation_errors
        ],
    )


@router.post(
    "/workflows/{workflow_id}/run",
    response_model=TriggerNowResponse,
    status_code=202,
)
async def post_workflow_run(
    workflow_id: int,
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> TriggerNowResponse:
    """Enqueue an immediate Job for ``workflow_id``.

    Distinguishes 404 (unknown / cross-owner) from 409 (disabled) so
    operators can tell why a trigger didn't fire (spec §14.15).
    """
    async with session_factory() as session, session.begin():
        wf = await workflow_repo.get_by_id(
            session, workflow_id=workflow_id, owner_id=owner_id
        )
        if wf is None:
            raise HTTPException(status_code=404, detail="workflow not found")
        if not wf.enabled:
            raise HTTPException(
                status_code=409,
                detail="workflow is disabled — enable it before triggering",
            )
        job_id = await trigger_now(session, workflow_id=workflow_id, owner_id=owner_id)
    if job_id is None:
        # trigger_now's only ``None`` return paths are unknown / cross-owner /
        # disabled — all caught above. If we land here something raced.
        raise HTTPException(
            status_code=409,
            detail="workflow could not be triggered (concurrent state change)",
        )
    return TriggerNowResponse(job_id=job_id)
