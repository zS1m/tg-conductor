"""§6.6 / §6.7 / §6.8 — CLI ``account`` subcommands end-to-end.

``login`` interacts with Telegram via pyrogram; full coverage requires a
real account (see :func:`tests.test_kurigram_live`). Here we exercise:

* ``logout`` against a seeded DB
* ``list`` against a seeded DB (default vs --all)
* ``login`` option parsing + early-fail when env / DB are unusable
"""

from __future__ import annotations

import asyncio
import base64
from pathlib import Path

import pytest
from click.testing import CliRunner

from tg_conductor.__main__ import cli
from tg_conductor.accounts import repo
from tg_conductor.accounts.models import AccountStatus
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.db.session import make_session_factory


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[CliRunner, str]:
    """Spin up a fresh SQLite + env that the CLI commands will pick up."""
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'cli.sqlite3'}"
    monkeypatch.setenv(
        "APP_MASTER_KEY", base64.urlsafe_b64encode(b"\x00" * 32).decode()
    )
    monkeypatch.setenv("DATABASE_URL", db_url)
    asyncio.run(asyncio.to_thread(upgrade_head, db_url))
    return CliRunner(), db_url


async def _seed(db_url: str, *, label: str = "main") -> int:
    engine = create_engine(db_url)
    try:
        factory = make_session_factory(engine)
        async with factory() as session:
            acc = await repo.upsert_session(
                session,
                owner_id=1,
                label=label,
                api_id=12345,
                api_hash="dummy-hash",
                session_string="dummy-session-string",
            )
            await session.commit()
            return acc.id  # type: ignore[return-value]
    finally:
        await engine.dispose()


async def _read_account(db_url: str, account_id: int):  # type: ignore[no-untyped-def]
    engine = create_engine(db_url)
    try:
        factory = make_session_factory(engine)
        async with factory() as session:
            return await repo.get_by_id(session, account_id, owner_id=1)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------- logout


def test_logout_marks_account_disabled(cli_env: tuple[CliRunner, str]) -> None:
    runner, db_url = cli_env
    account_id = asyncio.run(_seed(db_url))

    result = runner.invoke(
        cli, ["account", "logout", "--owner", "1", "--label", "main"]
    )
    assert result.exit_code == 0, result.output
    assert "Disabled account" in result.stdout

    row = asyncio.run(_read_account(db_url, account_id))
    assert row is not None
    assert row.status == AccountStatus.disabled
    assert row.session_string_enc is None


def test_logout_unknown_label_warns_and_exits_nonzero(
    cli_env: tuple[CliRunner, str],
) -> None:
    runner, _ = cli_env
    result = runner.invoke(
        cli, ["account", "logout", "--owner", "1", "--label", "missing"]
    )
    assert result.exit_code == 1
    assert "No account" in result.stderr


def test_logout_already_disabled_is_noop(cli_env: tuple[CliRunner, str]) -> None:
    runner, db_url = cli_env
    asyncio.run(_seed(db_url))
    runner.invoke(cli, ["account", "logout", "--owner", "1", "--label", "main"])
    second = runner.invoke(
        cli, ["account", "logout", "--owner", "1", "--label", "main"]
    )
    assert second.exit_code == 0
    assert "already disabled" in second.stdout


# ---------------------------------------------------------------- list


def test_list_excludes_disabled_by_default(cli_env: tuple[CliRunner, str]) -> None:
    runner, db_url = cli_env
    asyncio.run(_seed(db_url, label="alpha"))
    asyncio.run(_seed(db_url, label="beta"))
    runner.invoke(cli, ["account", "logout", "--owner", "1", "--label", "alpha"])

    result = runner.invoke(cli, ["account", "list", "--owner", "1"])
    assert result.exit_code == 0
    assert "beta" in result.stdout
    assert "alpha" not in result.stdout


def test_list_all_includes_disabled(cli_env: tuple[CliRunner, str]) -> None:
    runner, db_url = cli_env
    asyncio.run(_seed(db_url, label="alpha"))
    asyncio.run(_seed(db_url, label="beta"))
    runner.invoke(cli, ["account", "logout", "--owner", "1", "--label", "alpha"])

    result = runner.invoke(cli, ["account", "list", "--owner", "1", "--all"])
    assert result.exit_code == 0
    assert "alpha" in result.stdout
    assert "beta" in result.stdout
    assert "disabled" in result.stdout


def test_list_empty_db_prints_message(cli_env: tuple[CliRunner, str]) -> None:
    runner, _ = cli_env
    result = runner.invoke(cli, ["account", "list", "--owner", "1"])
    assert result.exit_code == 0
    assert "No accounts" in result.stdout


def test_list_does_not_leak_sensitive_fields(cli_env: tuple[CliRunner, str]) -> None:
    """api_hash and the encrypted session blob must never appear in CLI output."""
    runner, db_url = cli_env
    asyncio.run(_seed(db_url))
    result = runner.invoke(cli, ["account", "list", "--owner", "1", "--all"])
    assert result.exit_code == 0
    assert "dummy-hash" not in result.stdout
    assert "dummy-session-string" not in result.stdout


# ---------------------------------------------------------------- login (option parsing)


def test_login_requires_owner_and_label(cli_env: tuple[CliRunner, str]) -> None:
    runner, _ = cli_env
    result = runner.invoke(cli, ["account", "login"])
    assert result.exit_code != 0
    assert "--owner" in result.stderr or "Missing option" in result.stderr


def test_login_aborts_when_label_exists_and_overwrite_declined(
    cli_env: tuple[CliRunner, str],
) -> None:
    runner, db_url = cli_env
    asyncio.run(_seed(db_url))
    # Answer "no" to the overwrite confirmation.
    result = runner.invoke(
        cli,
        ["account", "login", "--owner", "1", "--label", "main"],
        input="n\n",
    )
    assert result.exit_code == 1
    assert "Aborted" in result.stdout
