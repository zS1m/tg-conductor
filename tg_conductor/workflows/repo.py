"""Tenant-aware Workflow persistence.

All public functions take ``owner_id`` and inject it into the SQL ``WHERE``
clause (CLAUDE.md multi-tenant invariant). Cross-owner reads return
``None`` / empty list, never raise.

``source`` is set at insert time and treated as immutable: ``upsert_by_source``
preserves it on update; there is no ``set_source`` API by design.
"""

from __future__ import annotations

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.workflows.models import WorkflowRow, WorkflowSource, _utcnow
from tg_conductor.workflows.schema import Workflow


async def list_for_owner(
    session: AsyncSession,
    owner_id: int,
    *,
    enabled_only: bool = False,
    source: WorkflowSource | None = None,
) -> list[WorkflowRow]:
    stmt = select(WorkflowRow).where(WorkflowRow.owner_id == owner_id)
    if enabled_only:
        stmt = stmt.where(WorkflowRow.enabled.is_(True))
    if source is not None:
        stmt = stmt.where(WorkflowRow.source == source)
    stmt = stmt.order_by(WorkflowRow.id)
    return list((await session.execute(stmt)).scalars().all())


async def get_by_id(
    session: AsyncSession,
    workflow_id: int,
    owner_id: int,
) -> WorkflowRow | None:
    stmt = select(WorkflowRow).where(
        WorkflowRow.id == workflow_id,
        WorkflowRow.owner_id == owner_id,
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_by_name(
    session: AsyncSession,
    owner_id: int,
    name: str,
) -> WorkflowRow | None:
    stmt = select(WorkflowRow).where(
        WorkflowRow.owner_id == owner_id,
        WorkflowRow.name == name,
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def upsert_by_source(
    session: AsyncSession,
    *,
    owner_id: int,
    source: WorkflowSource,
    workflow: Workflow,
) -> WorkflowRow:
    """Insert or update by ``(owner_id, name)``; ``source`` immutable on update.

    If a row with this name exists but has a different ``source``, raises
    ``ValueError`` — the caller chose the wrong sync path.
    """
    existing = await get_by_name(session, owner_id, workflow.name)
    if existing is None:
        row = WorkflowRow(
            owner_id=owner_id,
            account_id=workflow.account_id,
            name=workflow.name,
            enabled=workflow.enabled,
            source=source,
            trigger=workflow.trigger,
            action_plan=workflow.action_plan,
        )
        session.add(row)
        await session.flush()
        return row

    if existing.source != source:
        raise ValueError(
            f"workflow {workflow.name!r} exists with source={existing.source!r}; "
            f"refusing to overwrite from source={source!r}"
        )
    existing.account_id = workflow.account_id
    existing.enabled = workflow.enabled
    existing.trigger = workflow.trigger
    existing.action_plan = workflow.action_plan
    existing.updated_at = _utcnow()
    await session.flush()
    return existing


async def set_enabled(
    session: AsyncSession,
    *,
    workflow_id: int,
    owner_id: int,
    enabled: bool,
) -> bool:
    stmt = (
        update(WorkflowRow)
        .where(WorkflowRow.id == workflow_id, WorkflowRow.owner_id == owner_id)
        .values(enabled=enabled, updated_at=_utcnow())
    )
    result = await session.execute(stmt)
    return result.rowcount > 0


async def bump_rr_counter(
    session: AsyncSession,
    *,
    workflow_id: int,
    owner_id: int,
) -> int | None:
    """Atomic ``UPDATE workflows SET rr_counter = rr_counter + 1 ... RETURNING``.

    SQLite ≥ 3.35 supports RETURNING; we install on Python 3.13 which links
    a recent enough SQLite. Returns the new counter value, or ``None`` if the
    row does not exist (or belongs to a different owner).
    """
    stmt = (
        update(WorkflowRow)
        .where(WorkflowRow.id == workflow_id, WorkflowRow.owner_id == owner_id)
        .values(rr_counter=WorkflowRow.rr_counter + 1, updated_at=_utcnow())
        .returning(WorkflowRow.rr_counter)
    )
    result = await session.execute(stmt)
    row = result.first()
    return None if row is None else int(row[0])


async def delete_by_id(
    session: AsyncSession,
    workflow_id: int,
    owner_id: int,
) -> bool:
    stmt = delete(WorkflowRow).where(
        WorkflowRow.id == workflow_id,
        WorkflowRow.owner_id == owner_id,
    )
    result = await session.execute(stmt)
    return result.rowcount > 0
