"""SQLModel definition for the ``accounts`` table.

Status lifecycle (spec ``accounts/spec.md``):

    offline → connecting → online
                ↓             ↓
                error      floodwait → online
                              ↓
                          (back to connecting on retry)

    disabled — terminal state set by ``account logout``. Excluded from
               start-up loading, reconnect, and workflow validation; row
               is kept so historical ``runs.account_id`` FKs stay intact.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import Index, UniqueConstraint
from sqlmodel import Field, SQLModel


class AccountStatus(StrEnum):
    offline = "offline"
    connecting = "connecting"
    online = "online"
    error = "error"
    floodwait = "floodwait"
    disabled = "disabled"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Account(SQLModel, table=True):
    __tablename__ = "accounts"
    __table_args__ = (
        UniqueConstraint("owner_id", "label", name="uq_accounts_owner_label"),
        Index("ix_accounts_owner_status", "owner_id", "status"),
    )

    id: int | None = Field(default=None, primary_key=True)
    owner_id: int = Field(foreign_key="owners.id", nullable=False, index=True)
    label: str = Field(nullable=False)
    api_id: int = Field(nullable=False)
    api_hash: str = Field(nullable=False)
    # base64(nonce || ciphertext || tag); NULL once an account is logged out.
    session_string_enc: str | None = Field(default=None)
    proxy: str | None = Field(default=None)
    status: AccountStatus = Field(default=AccountStatus.offline, nullable=False)
    floodwait_until: datetime | None = Field(default=None)
    last_error: str | None = Field(default=None)
    created_at: datetime = Field(default_factory=_utcnow, nullable=False)
    updated_at: datetime = Field(default_factory=_utcnow, nullable=False)
