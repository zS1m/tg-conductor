"""SQLModel definitions for ``runs`` and ``run_events``.

A Run is the per-Job execution record (one Job ⇒ one Run). It exposes the
ActionPlan's progress to the outside world via ``run_events`` rows — one per
emitted event — strictly ordered within a Run by ``(run_id, seq)``.

Sequence numbers are assigned by the writer (an ``ActionContext`` held by
the AccountWorker that owns the Run). With at most one writer per Run the
sequence is monotonically increasing without needing DB-level coordination.

``workflow_id`` and ``job_id`` are logical references, not FK constraints.
``workflow_id`` matches the policy on ``jobs`` (workflow is physically
deletable, see §10 Commit 3 commit message). ``job_id``: we keep Job rows
indefinitely (cancel/failed/skipped are status flips, never deletes), so
the FK would be safe — but omitting it keeps the migration symmetric with
``jobs``'s own ``run_id`` (which already deferred FK to here).
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, Column
from sqlmodel import Field, Index, SQLModel, UniqueConstraint

from tg_conductor.db.types import UtcDateTime


class RunStatus(StrEnum):
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    canceled = "canceled"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class RunRow(SQLModel, table=True):
    __tablename__ = "runs"
    __table_args__ = (
        Index(
            "ix_runs_owner_workflow_started", "owner_id", "workflow_id", "started_at"
        ),
        Index("ix_runs_workflow_started", "workflow_id", "started_at"),
    )

    id: int | None = Field(default=None, primary_key=True)
    owner_id: int = Field(foreign_key="owners.id", nullable=False)
    workflow_id: int = Field(nullable=False)
    account_id: int = Field(foreign_key="accounts.id", nullable=False)
    job_id: int = Field(nullable=False)
    status: RunStatus = Field(default=RunStatus.running, nullable=False)
    error: str | None = Field(default=None)
    started_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(UtcDateTime(), nullable=False),
    )
    finished_at: datetime | None = Field(
        default=None,
        sa_column=Column(UtcDateTime(), nullable=True),
    )


class EventLevel(StrEnum):
    """Log-style levels mirrored from specs/runs/spec.md."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class RunEventRow(SQLModel, table=True):
    __tablename__ = "run_events"
    __table_args__ = (
        UniqueConstraint("run_id", "seq", name="uq_run_events_run_seq"),
        Index("ix_run_events_run_seq", "run_id", "seq"),
        Index("ix_run_events_owner_ts", "owner_id", "ts"),
    )

    id: int | None = Field(default=None, primary_key=True)
    owner_id: int = Field(foreign_key="owners.id", nullable=False)
    run_id: int = Field(foreign_key="runs.id", nullable=False)
    seq: int = Field(nullable=False)
    ts: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(UtcDateTime(), nullable=False),
    )
    level: str = Field(default=EventLevel.INFO.value, nullable=False)
    type: str = Field(nullable=False)
    message: str = Field(default="", nullable=False)
    attrs: dict[str, Any] | None = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )
