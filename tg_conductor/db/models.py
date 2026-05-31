"""Foundational SQLModel tables shared across capabilities.

Owner is the multi-tenancy anchor: every business table holds an
``owner_id`` FK pointing here. v1 self-use seeds a single row (id=1,
name='self') in the first Alembic migration.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlmodel import Field, SQLModel


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Owner(SQLModel, table=True):
    __tablename__ = "owners"

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(nullable=False)
    created_at: datetime = Field(default_factory=_utcnow, nullable=False)
