"""Long-lived ``TGClient`` instances managed per Account.

Responsibilities:

* On :meth:`start_all`, load every non-disabled Account for the owner, build a
  ``TGClient`` via the injected factory, attempt ``connect``, and reflect the
  outcome into the row's ``status`` (``online`` / ``error``).
* Spawn a watcher coroutine per account that, on disconnection, retries
  ``connect`` with exponential backoff (capped by
  ``Settings.tg_reconnect_max_seconds``).
* On :meth:`stop_all`, cancel every watcher and ``close`` every client.

The DB session factory and the TGClient factory are both injected so tests
can swap in :class:`FakeTGClient` without touching Telegram.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo
from tg_conductor.accounts.models import Account, AccountStatus
from tg_conductor.tg_core.protocol import TGClient

log = logging.getLogger(__name__)

ClientFactory = Callable[[Account], TGClient]


class AccountManager:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        client_factory: ClientFactory,
        owner_id: int,
        initial_reconnect_seconds: float = 1.0,
        max_reconnect_seconds: float = 300.0,
    ) -> None:
        self._session_factory = session_factory
        self._client_factory = client_factory
        self._owner_id = owner_id
        self._initial_reconnect = initial_reconnect_seconds
        self._max_reconnect = max_reconnect_seconds
        self._clients: dict[int, TGClient] = {}
        self._watchers: dict[int, asyncio.Task[None]] = {}
        self._shutting_down = asyncio.Event()

    # -- public API -----------------------------------------------------

    async def start_all(self) -> None:
        """Load every non-disabled Account and bring up its TGClient."""
        async with self._session_factory() as session:
            accounts = await repo.list_for_owner(session, self._owner_id)
        for acc in accounts:
            await self._start_one(acc)

    async def stop_all(self) -> None:
        """Cancel watchers, close clients, leave the manager empty."""
        self._shutting_down.set()
        for task in list(self._watchers.values()):
            task.cancel()
        if self._watchers:
            await asyncio.gather(*self._watchers.values(), return_exceptions=True)
        self._watchers.clear()
        for account_id, client in list(self._clients.items()):
            try:
                await client.close()
            except Exception:
                log.exception("error closing tg client account_id=%s", account_id)
        self._clients.clear()

    def get(self, account_id: int) -> TGClient | None:
        return self._clients.get(account_id)

    def active_account_ids(self) -> list[int]:
        return list(self._clients.keys())

    # -- internals ------------------------------------------------------

    async def _start_one(self, account: Account) -> None:
        assert account.id is not None
        client = self._client_factory(account)
        self._clients[account.id] = client
        await self._set_status(account.id, AccountStatus.connecting)
        try:
            await client.connect()
        except Exception as exc:  # noqa: BLE001 - record cause, keep going
            await self._set_status(
                account.id, AccountStatus.error, last_error=repr(exc)
            )
            log.warning(
                "account connect failed account_id=%s label=%s err=%r",
                account.id,
                account.label,
                exc,
            )
        else:
            await self._set_status(account.id, AccountStatus.online)

        # Watcher monitors connection state and retries on disconnect.
        self._watchers[account.id] = asyncio.create_task(
            self._watch_connection(account.id, account.label),
            name=f"acc-watcher-{account.id}",
        )

    async def _watch_connection(self, account_id: int, label: str) -> None:
        backoff = self._initial_reconnect
        try:
            while not self._shutting_down.is_set():
                try:
                    await asyncio.sleep(backoff)
                except asyncio.CancelledError:
                    return
                client = self._clients.get(account_id)
                if client is None or self._shutting_down.is_set():
                    return
                if client.is_connected():
                    backoff = self._initial_reconnect
                    continue
                await self._set_status(account_id, AccountStatus.connecting)
                try:
                    await client.connect()
                except Exception as exc:  # noqa: BLE001
                    await self._set_status(
                        account_id, AccountStatus.error, last_error=repr(exc)
                    )
                    log.warning(
                        "reconnect failed account_id=%s label=%s backoff=%.1fs err=%r",
                        account_id,
                        label,
                        backoff,
                        exc,
                    )
                    backoff = min(backoff * 2, self._max_reconnect)
                else:
                    await self._set_status(account_id, AccountStatus.online)
                    backoff = self._initial_reconnect
        except asyncio.CancelledError:
            return

    async def _set_status(
        self,
        account_id: int,
        status: AccountStatus,
        *,
        last_error: str | None = None,
    ) -> None:
        async with self._session_factory() as session:
            await repo.update_status(
                session,
                account_id=account_id,
                owner_id=self._owner_id,
                status=status,
                last_error=last_error,
            )
            await session.commit()
