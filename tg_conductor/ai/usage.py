"""``usage_events`` SQLModel — one row per AI call (success or failure).

Field shape from ``specs/ai-usage/spec.md`` §"每次调用写 usage_events":

* ``id``, ``owner_id``, ``ts`` — identity + when.
* ``kind`` — call category, e.g. ``"openai.chat"`` / ``"openai.vision"``
  / ``"openai.chat.failed"`` / ``"openai.vision.failed"``.
* ``units`` — token count (success: prompt+completion; failure: 0 or
  whatever was emitted before the error).
* ``cost_micros`` — integer micro-yuan (1 元 = 1e6 µ¥). Computed by
  :class:`tg_conductor.ai.pricing.PricingTable.cost_micros`; unknown
  models record 0.
* ``run_id`` / ``workflow_id`` / ``account_id`` — provenance, all
  nullable so cross-tenant tools that don't run inside a Run still get
  recorded.
* ``meta`` — JSON blob: model, latency_ms, prompt_tokens,
  completion_tokens, error summary if any.

Indexes: ``(owner_id, ts)`` for time-bounded aggregation,
``(owner_id, kind, ts)`` for kind-grouped aggregation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, Column
from sqlmodel import Field, Index, SQLModel

from tg_conductor.db.types import UtcDateTime


class UsageKind(StrEnum):
    """Canonical ``kind`` strings used for SQL filters / aggregation."""

    openai_chat = "openai.chat"
    openai_chat_failed = "openai.chat.failed"
    openai_vision = "openai.vision"
    openai_vision_failed = "openai.vision.failed"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class UsageRow(SQLModel, table=True):
    __tablename__ = "usage_events"
    __table_args__ = (
        Index("ix_usage_events_owner_ts", "owner_id", "ts"),
        Index("ix_usage_events_owner_kind_ts", "owner_id", "kind", "ts"),
    )

    id: int | None = Field(default=None, primary_key=True)
    owner_id: int = Field(foreign_key="owners.id", nullable=False)
    ts: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(UtcDateTime(), nullable=False),
    )
    kind: str = Field(nullable=False)
    units: int = Field(default=0, nullable=False)
    cost_micros: int = Field(default=0, nullable=False)
    run_id: int | None = Field(default=None)
    workflow_id: int | None = Field(default=None)
    account_id: int | None = Field(default=None)
    meta: dict[str, Any] | None = Field(
        default=None, sa_column=Column(JSON, nullable=True)
    )
