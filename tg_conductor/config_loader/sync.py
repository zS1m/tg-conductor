"""Diff between desired (parsed YAML) and current (``source=yaml`` rows in DB).

The caller controls the transaction. ``sync_to_db`` calls ``session.flush``
but never commits; wrap the call in ``async with session.begin()`` so
**all** inserts / updates / deletes happen atomically — a single failing
write rolls back the whole reload (spec ``config-loader`` "事务一致性").

DB-aware validation (``workflows.validate``) runs **before** any mutation so
an invalid workflow never partially lands.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.workflows import repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow
from tg_conductor.workflows.validate import (
    WorkflowValidationError,
    validate_workflow,
)

JobCancelHook = Callable[[AsyncSession, list[int]], Awaitable[None]]
"""``(session, workflow_ids) -> None``. Called *inside* the sync transaction
so the hook's writes commit (or roll back) atomically with the workflow row
deletes."""


@dataclass
class SyncReport:
    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    deleted_ids: list[int] = field(default_factory=list)
    validation_errors: list[tuple[str, str]] = field(default_factory=list)

    @property
    def change_count(self) -> int:
        return len(self.created) + len(self.updated) + len(self.deleted)


async def sync_to_db(
    session: AsyncSession,
    *,
    owner_id: int,
    desired: list[Workflow],
    on_deleted: JobCancelHook | None = None,
) -> SyncReport:
    """Reconcile ``source=yaml`` rows for ``owner_id`` with ``desired``.

    Parameters
    ----------
    on_deleted:
        Async callback invoked with the ids of rows that were deleted in this
        sync. The §10 scheduler installs a callback that flips matching
        ``pending`` Jobs to ``canceled`` (spec ``config-loader`` 8.4).
        Defaults to no-op so this module is independently testable.
    """
    report = SyncReport()

    # DB-aware validation up front: account_id ref, status==disabled, etc.
    # Invalid workflows are reported but do not block sync of the valid ones —
    # they are simply NOT written, which means an existing same-name row stays
    # untouched (less destructive than failing the whole sync).
    valid_by_name: dict[str, Workflow] = {}
    for wf in desired:
        try:
            await validate_workflow(session, owner_id=owner_id, workflow=wf)
        except WorkflowValidationError as exc:
            report.validation_errors.append((wf.name, str(exc)))
            continue
        valid_by_name[wf.name] = wf

    existing = await repo.list_for_owner(
        session, owner_id=owner_id, source=WorkflowSource.yaml
    )
    existing_by_name = {row.name: row for row in existing}

    # Upserts. The repo handles insert vs update internally; we only need to
    # bucket the result for the report.
    for name, wf in valid_by_name.items():
        is_new = name not in existing_by_name
        await repo.upsert_by_source(
            session,
            owner_id=owner_id,
            source=WorkflowSource.yaml,
            workflow=wf,
        )
        (report.created if is_new else report.updated).append(name)

    # Deletes — only consider workflows that were validated; if a row is
    # missing from `valid_by_name` because it failed validation, keep it.
    keep_names = set(valid_by_name.keys()) | {
        name for name, _ in report.validation_errors
    }
    for name, row in existing_by_name.items():
        if name not in keep_names:
            assert row.id is not None
            report.deleted.append(row.name)
            report.deleted_ids.append(row.id)
            await session.delete(row)

    # Run the hook BEFORE flush so any FK-dependent updates the hook makes
    # (e.g. flipping ``pending`` Jobs to ``canceled``) get topologically
    # ordered ahead of the workflow row delete that would otherwise trip
    # ``ON DELETE NO ACTION``.
    if on_deleted is not None and report.deleted_ids:
        await on_deleted(session, report.deleted_ids)

    await session.flush()

    return report
