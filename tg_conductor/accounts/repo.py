"""Tenant-aware persistence for :class:`Account`.

Every public method takes ``owner_id`` and embeds it in the ``WHERE`` clause.
Cross-tenant reads return ``None`` / empty list — never raise — so callers
can't probe id existence by error type (CLAUDE.md multi-tenant note).

The repo is the boundary that hides the AES-GCM wire format from callers:
``upsert_session`` takes plaintext ``session_string`` and encrypts before
writing; :func:`decrypt_session` reverses that for the caller. Plaintext
never crosses the DB boundary.
"""

from __future__ import annotations

import base64
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tg_conductor.accounts.models import Account, AccountStatus, _utcnow
from tg_conductor.db import crypto


async def list_for_owner(
    session: AsyncSession,
    owner_id: int,
    *,
    include_disabled: bool = False,
) -> list[Account]:
    stmt = select(Account).where(Account.owner_id == owner_id)
    if not include_disabled:
        stmt = stmt.where(Account.status != AccountStatus.disabled)
    stmt = stmt.order_by(Account.id)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_by_id(
    session: AsyncSession,
    account_id: int,
    owner_id: int,
) -> Account | None:
    stmt = select(Account).where(
        Account.id == account_id,
        Account.owner_id == owner_id,
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_by_label(
    session: AsyncSession,
    owner_id: int,
    label: str,
) -> Account | None:
    stmt = select(Account).where(
        Account.owner_id == owner_id,
        Account.label == label,
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def upsert_session(
    session: AsyncSession,
    *,
    owner_id: int,
    label: str,
    api_id: int,
    api_hash: str,
    session_string: str,
    proxy: str | None = None,
) -> Account:
    """Insert a new account or refresh credentials on an existing ``label``.

    The plaintext ``session_string`` is encrypted with the app master key
    before being stored.
    """
    enc_b64 = _encrypt_session(session_string)
    existing = await get_by_label(session, owner_id, label)
    if existing is None:
        row = Account(
            owner_id=owner_id,
            label=label,
            api_id=api_id,
            api_hash=api_hash,
            session_string_enc=enc_b64,
            proxy=proxy,
            status=AccountStatus.offline,
        )
        session.add(row)
        await session.flush()
        return row

    existing.api_id = api_id
    existing.api_hash = api_hash
    existing.session_string_enc = enc_b64
    existing.proxy = proxy
    # Re-enabling a previously disabled account on re-login is intentional.
    if existing.status == AccountStatus.disabled:
        existing.status = AccountStatus.offline
        existing.last_error = None
    existing.updated_at = _utcnow()
    await session.flush()
    return existing


async def update_status(
    session: AsyncSession,
    *,
    account_id: int,
    owner_id: int,
    status: AccountStatus,
    last_error: str | None = None,
    floodwait_until: datetime | None = None,
) -> bool:
    """Patch the operational status of an account; returns False if not found."""
    row = await get_by_id(session, account_id, owner_id)
    if row is None:
        return False
    row.status = status
    row.last_error = last_error
    row.floodwait_until = floodwait_until
    row.updated_at = _utcnow()
    await session.flush()
    return True


async def mark_disabled(
    session: AsyncSession,
    account_id: int,
    owner_id: int,
) -> bool:
    """Soft delete: status=disabled, session_string_enc=NULL; returns False if not found."""
    row = await get_by_id(session, account_id, owner_id)
    if row is None:
        return False
    row.status = AccountStatus.disabled
    row.session_string_enc = None
    row.updated_at = _utcnow()
    await session.flush()
    return True


def decrypt_session(account: Account) -> str | None:
    """Return the plaintext ``session_string``, or ``None`` if not present."""
    if account.session_string_enc is None:
        return None
    raw = base64.b64decode(account.session_string_enc.encode("ascii"), validate=True)
    return crypto.decrypt(raw).decode("utf-8")


def _encrypt_session(plaintext: str) -> str:
    ct = crypto.encrypt(plaintext.encode("utf-8"))
    return base64.b64encode(ct).decode("ascii")
