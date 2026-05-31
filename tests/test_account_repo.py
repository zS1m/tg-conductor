"""§6.9 / §6.10 — Account repo: multi-tenant isolation + session encryption."""

from __future__ import annotations

import asyncio
import base64

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo
from tg_conductor.accounts.models import AccountStatus
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001 - sets APP_MASTER_KEY in env
    tmp_sqlite_url: str,
) -> async_sessionmaker[AsyncSession]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    # Seed a second owner so the cross-owner test has a real foreign target.
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO owners (id, name, created_at) VALUES (2, 'other', '2026-01-01')"
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_upsert_inserts_then_updates_in_place(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        a1 = await repo.upsert_session(
            session,
            owner_id=1,
            label="main",
            api_id=12345,
            api_hash="hash-v1",
            session_string="session-v1",
        )
        await session.commit()

        a2 = await repo.upsert_session(
            session,
            owner_id=1,
            label="main",
            api_id=12345,
            api_hash="hash-v2",
            session_string="session-v2",
        )
        await session.commit()

        assert a1.id == a2.id, "upsert on same (owner, label) must reuse the row"
        assert a2.api_hash == "hash-v2"


@pytest.mark.asyncio
async def test_session_string_roundtrip_via_repo(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    plaintext = "1ZbVNAAAAAAAAA-fake-session-string-blob-here"
    async with session_factory() as session:
        created = await repo.upsert_session(
            session,
            owner_id=1,
            label="main",
            api_id=1,
            api_hash="h",
            session_string=plaintext,
        )
        await session.commit()

        # Plaintext does NOT leak into the encrypted column.
        assert created.session_string_enc is not None
        decoded = base64.b64decode(created.session_string_enc.encode("ascii"))
        assert plaintext.encode() not in decoded

        fetched = await repo.get_by_id(session, created.id, owner_id=1)
        assert fetched is not None
        assert repo.decrypt_session(fetched) == plaintext


@pytest.mark.asyncio
async def test_cross_owner_returns_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        created = await repo.upsert_session(
            session,
            owner_id=1,
            label="main",
            api_id=1,
            api_hash="h",
            session_string="sek",
        )
        await session.commit()
        assert created.id is not None

        # Same id, wrong owner -> None (not raise, not the row).
        assert await repo.get_by_id(session, created.id, owner_id=2) is None
        assert await repo.list_for_owner(session, owner_id=2) == []

        # Same label across owners is allowed and isolated.
        other = await repo.upsert_session(
            session,
            owner_id=2,
            label="main",
            api_id=2,
            api_hash="h2",
            session_string="sek2",
        )
        await session.commit()
        assert other.id != created.id
        assert other.owner_id == 2


@pytest.mark.asyncio
async def test_list_for_owner_excludes_disabled_by_default(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        a = await repo.upsert_session(
            session,
            owner_id=1,
            label="a",
            api_id=1,
            api_hash="h",
            session_string="s1",
        )
        await repo.upsert_session(
            session,
            owner_id=1,
            label="b",
            api_id=1,
            api_hash="h",
            session_string="s2",
        )
        await session.commit()

        await repo.mark_disabled(session, a.id, owner_id=1)
        await session.commit()

        active = await repo.list_for_owner(session, owner_id=1)
        assert [acc.label for acc in active] == ["b"]

        all_rows = await repo.list_for_owner(session, owner_id=1, include_disabled=True)
        assert sorted(acc.label for acc in all_rows) == ["a", "b"]


@pytest.mark.asyncio
async def test_mark_disabled_clears_session_string_enc(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        a = await repo.upsert_session(
            session,
            owner_id=1,
            label="a",
            api_id=1,
            api_hash="h",
            session_string="sek",
        )
        await session.commit()
        assert a.session_string_enc is not None

        ok = await repo.mark_disabled(session, a.id, owner_id=1)
        await session.commit()
        assert ok is True

        refreshed = await repo.get_by_id(
            session,
            a.id,
            owner_id=1,
        )
        assert refreshed is not None
        assert refreshed.status == AccountStatus.disabled
        assert refreshed.session_string_enc is None


@pytest.mark.asyncio
async def test_update_status_records_floodwait(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    from datetime import UTC, datetime, timedelta

    async with session_factory() as session:
        a = await repo.upsert_session(
            session,
            owner_id=1,
            label="a",
            api_id=1,
            api_hash="h",
            session_string="s",
        )
        await session.commit()

        until = datetime.now(UTC) + timedelta(seconds=30)
        ok = await repo.update_status(
            session,
            account_id=a.id,
            owner_id=1,
            status=AccountStatus.floodwait,
            last_error="hit FloodWait",
            floodwait_until=until,
        )
        await session.commit()
        assert ok is True

        refreshed = await repo.get_by_id(session, a.id, owner_id=1)
        assert refreshed is not None
        assert refreshed.status == AccountStatus.floodwait
        assert refreshed.last_error == "hit FloodWait"
        assert refreshed.floodwait_until is not None


@pytest.mark.asyncio
async def test_wrong_master_key_decryption_fails(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec D7 / accounts spec: rotating APP_MASTER_KEY without re-encrypting must raise."""
    from cryptography.exceptions import InvalidTag

    from tg_conductor.config.settings import get_settings

    async with session_factory() as session:
        a = await repo.upsert_session(
            session,
            owner_id=1,
            label="main",
            api_id=1,
            api_hash="h",
            session_string="secret-text",
        )
        await session.commit()
        assert a.session_string_enc is not None

    # Swap the master key and clear the cached singleton, then attempt decrypt.
    monkeypatch.setenv(
        "APP_MASTER_KEY",
        base64.urlsafe_b64encode(b"\xff" * 32).decode(),
    )
    get_settings.cache_clear()

    async with session_factory() as session:
        refreshed = await repo.get_by_id(session, a.id, owner_id=1)
    assert refreshed is not None
    with pytest.raises(InvalidTag):
        repo.decrypt_session(refreshed)
