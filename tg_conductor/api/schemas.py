"""Pydantic response models shared across API routes.

Two principles:

1. ``RunEventResponse`` mirrors the SSE bus message published by
   :class:`tg_conductor.runs.event_writer.EventWriter` so REST and SSE
   clients see the same JSON keys.
2. Account / Workflow responses **strip secrets** at this layer.
   ``api_hash`` and ``session_string_enc`` are never serialized. The
   ``CLAUDE.md`` security invariant lives in code here, not "by
   convention".
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from tg_conductor.accounts.models import Account
from tg_conductor.runs.models import RunEventRow, RunRow
from tg_conductor.scheduler.models import JobRow
from tg_conductor.workflows.models import WorkflowRow


class RunEventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    owner_id: int
    run_id: int
    seq: int
    ts: datetime
    level: str
    type: str
    message: str
    attrs: dict[str, Any] | None

    @classmethod
    def from_row(cls, row: RunEventRow) -> RunEventResponse:
        assert row.id is not None
        return cls(
            id=row.id,
            owner_id=row.owner_id,
            run_id=row.run_id,
            seq=row.seq,
            ts=row.ts,
            level=row.level,
            type=row.type,
            message=row.message,
            attrs=row.attrs,
        )


class AccountResponse(BaseModel):
    """Account row with ``api_hash`` and ``session_string_enc`` stripped.

    Also surfaces a boolean ``has_session`` so the UI can tell whether
    the account has ever been logged in without leaking the ciphertext.
    """

    id: int
    owner_id: int
    label: str
    api_id: int
    proxy: str | None
    status: str
    floodwait_until: datetime | None
    last_error: str | None
    has_session: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: Account) -> AccountResponse:
        assert row.id is not None
        return cls(
            id=row.id,
            owner_id=row.owner_id,
            label=row.label,
            api_id=row.api_id,
            proxy=row.proxy,
            status=row.status,
            floodwait_until=row.floodwait_until,
            last_error=row.last_error,
            has_session=row.session_string_enc is not None,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


class WorkflowResponse(BaseModel):
    """Workflow row with the validated trigger / action_plan inline."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: int
    owner_id: int
    account_id: int
    name: str
    enabled: bool
    source: str
    trigger: dict[str, Any]
    action_plan: dict[str, Any]
    rr_counter: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: WorkflowRow) -> WorkflowResponse:
        assert row.id is not None
        return cls(
            id=row.id,
            owner_id=row.owner_id,
            account_id=row.account_id,
            name=row.name,
            enabled=row.enabled,
            source=row.source,
            trigger=row.trigger.model_dump(mode="json"),
            action_plan=row.action_plan.model_dump(mode="json"),
            rr_counter=row.rr_counter,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


class JobResponse(BaseModel):
    id: int
    owner_id: int
    workflow_id: int
    account_id: int
    status: str
    fire_at: datetime
    variant_id: str | None
    run_id: int | None
    skip_reason: str | None
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime

    @classmethod
    def from_row(cls, row: JobRow) -> JobResponse:
        assert row.id is not None
        return cls(
            id=row.id,
            owner_id=row.owner_id,
            workflow_id=row.workflow_id,
            account_id=row.account_id,
            status=row.status,
            fire_at=row.fire_at,
            variant_id=row.variant_id,
            run_id=row.run_id,
            skip_reason=row.skip_reason,
            started_at=row.started_at,
            finished_at=row.finished_at,
            created_at=row.created_at,
        )


class RunResponse(BaseModel):
    id: int
    owner_id: int
    workflow_id: int
    account_id: int
    job_id: int
    status: str
    error: str | None
    started_at: datetime
    finished_at: datetime | None

    @classmethod
    def from_row(cls, row: RunRow) -> RunResponse:
        assert row.id is not None
        return cls(
            id=row.id,
            owner_id=row.owner_id,
            workflow_id=row.workflow_id,
            account_id=row.account_id,
            job_id=row.job_id,
            status=row.status,
            error=row.error,
            started_at=row.started_at,
            finished_at=row.finished_at,
        )


class HealthzResponse(BaseModel):
    status: str  # "ok" | "degraded"
    db: str  # "ok" | error string
    accounts_online: int
    accounts_total: int
