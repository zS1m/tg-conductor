"""``/healthz`` — liveness + lightweight readiness probe.

§14.11 returns the service status plus DB ping and account counts.
Always responds 200 with a body that includes a ``status`` string
(``"ok"`` / ``"degraded"``); ops parses the body, not the HTTP code,
so transient DB blips don't flap k8s. Inspired by the Kubernetes
convention.

Account counts come from :class:`tg_conductor.accounts.manager.AccountManager`
if one is attached to ``app.state.account_manager``; in tests / non-
lifespan setups it's ``None`` and we fall back to the DB row count
(spec doesn't mandate one source over the other).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import Request

from tg_conductor.accounts.models import Account, AccountStatus
from tg_conductor.api.deps import get_owner_id, get_session_factory
from tg_conductor.api.schemas import HealthzResponse

router = APIRouter(tags=["health"])


@router.get("/healthz", response_model=HealthzResponse)
async def healthz(
    request: Request,
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> HealthzResponse:
    db_status = "ok"
    accounts_online = 0
    accounts_total = 0
    try:
        async with session_factory() as session:
            await session.execute(select(1))
            accounts_total = (
                await session.execute(
                    select(func.count(Account.id)).where(
                        Account.owner_id == owner_id,
                        Account.status != AccountStatus.disabled,
                    )
                )
            ).scalar_one()
            accounts_online = await _online_count(request, session, owner_id)
    except Exception as exc:  # noqa: BLE001 — surface to caller, don't crash
        db_status = f"{type(exc).__name__}: {exc}"

    overall = "ok" if db_status == "ok" else "degraded"
    return HealthzResponse(
        status=overall,
        db=db_status,
        accounts_online=accounts_online,
        accounts_total=int(accounts_total),
    )


async def _online_count(request: Request, session: AsyncSession, owner_id: int) -> int:
    """Prefer the in-process AccountManager's live view; fall back to DB."""
    manager = getattr(request.app.state, "account_manager", None)
    if manager is not None and hasattr(manager, "active_account_ids"):
        # In §16 lifespan, AccountManager owns the runtime status. Tests
        # without lifespan see ``None`` here and fall through.
        return len(manager.active_account_ids())
    result = await session.execute(
        select(func.count(Account.id)).where(
            Account.owner_id == owner_id,
            Account.status == AccountStatus.online,
        )
    )
    return int(result.scalar_one())
