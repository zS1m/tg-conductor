"""SQLModel persistence row for Workflows.

The wire shape (:class:`tg_conductor.workflows.schema.Workflow`) is reused
for ``trigger`` and ``action_plan`` via :class:`PydanticJSONType` so the DB
round-trip preserves the discriminated subclass identities (see design D13).
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import Column, Index, UniqueConstraint
from sqlmodel import Field, SQLModel

from tg_conductor.db.types import PydanticJSONType
from tg_conductor.workflows.schema import (
    ActionPlan,
    ActionPlanAdapter,
    Trigger,
    TriggerAdapter,
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class WorkflowSource(StrEnum):
    yaml = "yaml"
    api = "api"


class WorkflowRow(SQLModel, table=True):
    __tablename__ = "workflows"
    __table_args__ = (
        UniqueConstraint("owner_id", "name", name="uq_workflows_owner_name"),
        Index("ix_workflows_owner_enabled", "owner_id", "enabled"),
        Index("ix_workflows_account_id", "account_id"),
    )

    id: int | None = Field(default=None, primary_key=True)
    owner_id: int = Field(foreign_key="owners.id", nullable=False, index=True)
    account_id: int = Field(foreign_key="accounts.id", nullable=False)
    name: str = Field(nullable=False)
    enabled: bool = Field(default=True, nullable=False)
    source: WorkflowSource = Field(nullable=False)
    trigger: Trigger = Field(
        sa_column=Column(PydanticJSONType(TriggerAdapter), nullable=False),
    )
    action_plan: ActionPlan = Field(
        sa_column=Column(PydanticJSONType(ActionPlanAdapter), nullable=False),
    )
    rr_counter: int = Field(default=0, nullable=False)
    created_at: datetime = Field(default_factory=_utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=_utcnow, nullable=False)
