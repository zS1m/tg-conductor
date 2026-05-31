"""SQLModel definition for ``jobs`` — the scheduler's queue + history table.

Lifecycle: ``pending → running → {succeeded, failed, skipped, canceled}``.

* ``running``: claimed by dispatcher; ``started_at`` set.
* ``skipped``: fire_at fell outside the compensation window before claim;
  ``skip_reason="missed_window"`` and ``finished_at`` set.
* ``canceled``: explicit cancel (CLI / API / yaml-removal hook); ``finished_at`` set.
* ``succeeded`` / ``failed``: §11 writes these on Run completion.

``expansion_date`` is only set for ``time_window``-derived jobs; it's the
basis of the daily idempotency check that prevents the expander from
producing duplicate batches when invoked twice on the same day. The
``run_id`` column is a logical reference; no FK constraint because the
``runs`` table lands in §11.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, Column
from sqlmodel import Field, Index, SQLModel

from tg_conductor.db.types import UtcDateTime


class JobStatus(StrEnum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    skipped = "skipped"
    canceled = "canceled"


_TERMINAL_STATUSES = frozenset(
    {
        JobStatus.succeeded,
        JobStatus.failed,
        JobStatus.skipped,
        JobStatus.canceled,
    }
)


def is_terminal(status: JobStatus) -> bool:
    return status in _TERMINAL_STATUSES


def _utcnow() -> datetime:
    return datetime.now(UTC)


class JobRow(SQLModel, table=True):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_status_fire_at", "status", "fire_at"),
        Index("ix_jobs_owner_workflow_fire_at", "owner_id", "workflow_id", "fire_at"),
        Index("ix_jobs_workflow_expansion_date", "workflow_id", "expansion_date"),
    )

    id: int | None = Field(default=None, primary_key=True)
    owner_id: int = Field(foreign_key="owners.id", nullable=False)
    # Logical reference, no FK constraint — see migration 0004 for rationale.
    workflow_id: int = Field(nullable=False)
    account_id: int = Field(foreign_key="accounts.id", nullable=False)

    fire_at: datetime = Field(sa_column=Column(UtcDateTime(), nullable=False))
    status: JobStatus = Field(default=JobStatus.pending, nullable=False)

    # Set by §11 ActionPlan evaluator when round_robin / random picks a variant.
    variant_id: str | None = Field(default=None)
    # Pre-resolved per-job data (e.g. text_pool sample, picked variant index).
    # Generic JSON — no Pydantic schema because shape is action-specific.
    resolved_payload: dict[str, Any] | None = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )
    # Logical reference to runs.id; FK added when §11 creates the runs table.
    run_id: int | None = Field(default=None)

    skip_reason: str | None = Field(default=None)

    started_at: datetime | None = Field(
        default=None, sa_column=Column(UtcDateTime(), nullable=True)
    )
    finished_at: datetime | None = Field(
        default=None, sa_column=Column(UtcDateTime(), nullable=True)
    )
    created_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(UtcDateTime(), nullable=False),
    )

    # Only set for time_window expansions; used by the daily-idempotency check.
    expansion_date: date | None = Field(default=None)
