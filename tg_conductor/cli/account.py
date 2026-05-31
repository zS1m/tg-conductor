"""Implementation of ``tg-conductor account {login,logout,list}``.

The click wrappers in ``tg_conductor/__main__.py`` are thin: they parse
options and ``asyncio.run`` one of the coroutines below. Logic lives here so
it can be unit-tested with ``click.testing.CliRunner`` and a temp SQLite DB
without having to invoke ``asyncio.run`` from inside another loop.

CLI does NOT import pyrogram. The interactive Telegram flow is delegated to
``tg_core.login.interactive_login``, which accepts injectable prompters so
this layer only needs ``click.prompt`` lambdas.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import click

from tg_conductor.accounts import repo
from tg_conductor.accounts.models import AccountStatus
from tg_conductor.config.settings import get_settings, load_settings
from tg_conductor.db.engine import create_engine
from tg_conductor.db.session import make_session_factory
from tg_conductor.tg_core.login import LoginError, interactive_login

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

log = logging.getLogger(__name__)


@asynccontextmanager
async def _open_session_factory() -> "AsyncIterator[async_sessionmaker[AsyncSession]]":
    settings = load_settings()
    engine = create_engine(settings.database_url)
    try:
        yield make_session_factory(engine)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------- login


async def run_login(
    *,
    owner: int,
    label: str,
    proxy: str | None,
) -> int:
    """Interactive Telegram login → encrypted session into DB.

    Returns the click exit code: 0 on success, 1 on user-cancelled overwrite,
    2 on Telegram-side failure.
    """
    async with _open_session_factory() as factory:
        async with factory() as session:
            existing = await repo.get_by_label(session, owner_id=owner, label=label)

        if existing is not None and existing.status != AccountStatus.disabled:
            click.echo(
                f"Account (owner={owner}, label={label!r}) already exists "
                f"with status={existing.status}."
            )
            if not click.confirm("Overwrite the stored session?", default=False):
                click.echo("Aborted.")
                return 1

        api_id = click.prompt("api_id", type=int)
        api_hash = click.prompt("api_hash", hide_input=True)
        phone = click.prompt("phone number (e.g. +12345678901)")

        try:
            session_string = await interactive_login(
                api_id=api_id,
                api_hash=api_hash,
                phone=phone,
                proxy=proxy,
                env_proxy=get_settings().tg_proxy,
                get_code=lambda: click.prompt("verification code from Telegram"),
                get_password=lambda: click.prompt("2FA password", hide_input=True),
            )
        except LoginError as exc:
            click.secho(f"Login failed: {exc}", fg="red", err=True)
            log.exception("account login failed")
            return 2

        async with factory() as session:
            acc = await repo.upsert_session(
                session,
                owner_id=owner,
                label=label,
                api_id=api_id,
                api_hash=api_hash,
                session_string=session_string,
                proxy=proxy,
            )
            await session.commit()

        click.echo("")
        click.echo(f"Saved account id={acc.id} owner={owner} label={label}")
        click.secho(
            "WARNING: session_string equals account credentials. "
            "If APP_MASTER_KEY is lost the session cannot be recovered.",
            fg="yellow",
        )
    return 0


# ---------------------------------------------------------------------- logout


async def run_logout(*, owner: int, label: str) -> int:
    async with _open_session_factory() as factory, factory() as session:
        existing = await repo.get_by_label(session, owner_id=owner, label=label)
        if existing is None:
            click.secho(
                f"No account (owner={owner}, label={label!r}); nothing to do.",
                fg="yellow",
                err=True,
            )
            return 1
        if existing.status == AccountStatus.disabled:
            click.echo(f"Account id={existing.id} already disabled.")
            return 0
        await repo.mark_disabled(session, existing.id, owner_id=owner)  # type: ignore[arg-type]
        await session.commit()
        click.echo(
            f"Disabled account id={existing.id} owner={owner} label={label}. "
            "Connection (if running) will be closed by the service."
        )
    return 0


# ---------------------------------------------------------------------- list


async def run_list(*, owner: int, include_disabled: bool) -> int:
    async with _open_session_factory() as factory, factory() as session:
        rows = await repo.list_for_owner(
            session, owner_id=owner, include_disabled=include_disabled
        )

    if not rows:
        click.echo(f"No accounts for owner={owner}.")
        return 0

    header = f"{'ID':<5} {'LABEL':<20} {'STATUS':<11} {'API_ID':<10} {'PROXY':<28} LAST_ERROR"
    click.echo(header)
    click.echo("-" * len(header))
    for r in rows:
        click.echo(
            f"{r.id:<5} "
            f"{r.label:<20.20} "
            f"{str(r.status):<11} "
            f"{r.api_id:<10} "
            f"{(r.proxy or '-'):<28.28} "
            f"{(r.last_error or '')[:60]}"
        )
    return 0
