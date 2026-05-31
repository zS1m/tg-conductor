"""``/accounts`` routes — read-only listing. Secrets are stripped at the
:class:`tg_conductor.api.schemas.AccountResponse` layer (``api_hash`` and
``session_string_enc`` never leave the DB).

§14.3 lists active + optionally disabled rows; cross-owner / unknown id
returns 404 — but only for *individual* fetches; lists collapse to
empty arrays to avoid leaking owner id existence (CLAUDE.md invariant).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.api.deps import get_owner_id, get_session_factory
from tg_conductor.api.schemas import AccountResponse

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get("", response_model=list[AccountResponse])
async def list_accounts(
    include_disabled: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> list[AccountResponse]:
    async with session_factory() as session:
        rows = await account_repo.list_for_owner(
            session, owner_id=owner_id, include_disabled=include_disabled
        )
    return [AccountResponse.from_row(r) for r in rows[:limit]]


@router.get("/{account_id}", response_model=AccountResponse)
async def get_account(
    account_id: int,
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> AccountResponse:
    async with session_factory() as session:
        row = await account_repo.get_by_id(session, account_id, owner_id=owner_id)
    if row is None:
        raise HTTPException(status_code=404, detail="account not found")
    return AccountResponse.from_row(row)
