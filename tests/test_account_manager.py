"""§6.11 — AccountManager integration test with FakeTGClient.

Covers the start-up scenario from the accounts spec:

* 2 enabled Accounts → 2 TGClient instances, both connected, both reflected
  as ``online`` in DB.
* 1 enabled Account whose connect fails → that account becomes ``error``,
  the other one stays ``online``; the manager is still functional.
* ``stop_all`` cleanly closes every client.
* Disabled accounts are never loaded.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo
from tg_conductor.accounts.manager import AccountManager
from tg_conductor.accounts.models import Account, AccountStatus
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.tg_core.fake import FakeTGClient


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> async_sessionmaker[AsyncSession]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


async def _seed_accounts(
    factory: async_sessionmaker[AsyncSession],
    *,
    labels: list[str],
) -> list[int]:
    ids: list[int] = []
    async with factory() as session:
        for label in labels:
            acc = await repo.upsert_session(
                session,
                owner_id=1,
                label=label,
                api_id=1,
                api_hash="h",
                session_string=f"s-{label}",
            )
            ids.append(acc.id)  # type: ignore[arg-type]
        await session.commit()
    return ids


def _fast_manager(
    session_factory: async_sessionmaker[AsyncSession],
    client_map: dict[str, FakeTGClient] | None = None,
    *,
    fail_on_connect_labels: set[str] | None = None,
) -> tuple[AccountManager, dict[int, FakeTGClient]]:
    """Build a manager whose watcher cycles fast and remembers each fake client."""
    created: dict[int, FakeTGClient] = {}
    fail = fail_on_connect_labels or set()

    def factory(account: Account) -> FakeTGClient:
        client = FakeTGClient(label=account.label)
        if account.label in fail:
            client.next_connect_raises = RuntimeError(f"boom-{account.label}")
        created[account.id] = client  # type: ignore[index]
        if client_map is not None:
            client_map[account.label] = client
        return client

    manager = AccountManager(
        session_factory=session_factory,
        client_factory=factory,
        owner_id=1,
        initial_reconnect_seconds=0.02,
        max_reconnect_seconds=0.05,
    )
    return manager, created


@pytest.mark.asyncio
async def test_start_all_brings_up_two_accounts(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _seed_accounts(session_factory, labels=["a", "b"])
    manager, created = _fast_manager(session_factory)

    await manager.start_all()
    try:
        assert sorted(manager.active_account_ids()) == sorted(ids)
        for client in created.values():
            assert client.is_connected()

        async with session_factory() as session:
            rows = await repo.list_for_owner(session, owner_id=1)
        statuses = {r.label: r.status for r in rows}
        assert statuses == {"a": AccountStatus.online, "b": AccountStatus.online}
    finally:
        await manager.stop_all()

    for client in created.values():
        assert not client.is_connected()


@pytest.mark.asyncio
async def test_single_account_failure_isolated(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed_accounts(session_factory, labels=["good", "bad"])
    manager, created = _fast_manager(session_factory, fail_on_connect_labels={"bad"})

    await manager.start_all()
    try:
        async with session_factory() as session:
            rows = await repo.list_for_owner(session, owner_id=1)
        by_label = {r.label: r for r in rows}
        # The watcher will eventually retry, so we may see online↔error churn.
        # Right after start_all (before watcher's first tick) the spec-required
        # state is: good=online, bad=error.
        assert by_label["good"].status == AccountStatus.online
        assert by_label["bad"].status == AccountStatus.error
        assert by_label["bad"].last_error is not None
        assert "boom-bad" in by_label["bad"].last_error
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_disabled_accounts_are_not_loaded(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _seed_accounts(session_factory, labels=["live", "gone"])
    # Disable the second one.
    async with session_factory() as session:
        await repo.mark_disabled(session, ids[1], owner_id=1)
        await session.commit()

    manager, created = _fast_manager(session_factory)
    await manager.start_all()
    try:
        assert manager.active_account_ids() == [ids[0]]
        assert ids[1] not in created
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_watcher_reconnects_after_simulated_disconnect(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """If a client transitions to is_connected=False, the watcher must reconnect."""
    [account_id] = await _seed_accounts(session_factory, labels=["solo"])
    manager, created = _fast_manager(session_factory)

    await manager.start_all()
    try:
        client = created[account_id]
        assert client.is_connected()
        # Simulate a disconnect on the wire.
        client._connected = False  # noqa: SLF001 - test bypasses public API

        # Wait long enough for the watcher to tick (initial=0.02s) + reconnect.
        for _ in range(50):
            await asyncio.sleep(0.02)
            if client.is_connected():
                break
        assert client.is_connected(), "watcher did not reconnect within budget"

        async with session_factory() as session:
            row = await repo.get_by_id(session, account_id, owner_id=1)
        assert row is not None
        assert row.status == AccountStatus.online
    finally:
        await manager.stop_all()


@pytest.mark.asyncio
async def test_get_returns_none_for_unknown_account(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    manager, _ = _fast_manager(session_factory)
    assert manager.get(999) is None
    await manager.stop_all()  # no-op, should not raise


@pytest.mark.asyncio
async def test_start_all_idempotent_with_no_accounts(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    manager, _ = _fast_manager(session_factory)
    await manager.start_all()
    try:
        assert manager.active_account_ids() == []
    finally:
        await manager.stop_all()
