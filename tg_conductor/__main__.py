"""tg-conductor command-line entry point.

Subcommand bodies are stubs at this stage; they only print a NotImplemented
message and exit with status 2. Real implementations land in §2.x / §6 / §3
of `openspec/changes/bootstrap-mvp/tasks.md`.
"""

from __future__ import annotations

import click

from tg_conductor import __version__


def _not_implemented(name: str) -> None:
    click.echo(f"{name}: not yet implemented", err=True)
    raise SystemExit(2)


@click.group()
@click.version_option(__version__, package_name="tg-conductor")
def cli() -> None:
    """tg-conductor CLI."""


@cli.command()
def version() -> None:
    """Print the installed package version."""
    click.echo(__version__)


@cli.command()
@click.option(
    "--host",
    default=None,
    help="Bind host (overrides BIND_HOST / settings.bind_host)",
)
@click.option(
    "--port",
    type=int,
    default=None,
    help="Bind port (overrides BIND_PORT / settings.bind_port)",
)
def serve(host: str | None, port: int | None) -> None:
    """Start the HTTP + scheduler service.

    Wires every long-lived component via :func:`tg_conductor.api.lifespan.build_app`
    (DB → AccountManager → Reloader → Dispatcher → AccountWorkers → TTL
    cleaner) and hands the FastAPI app to uvicorn. SIGTERM / SIGINT
    triggers a graceful shutdown that drains in-flight Runs (spec §16).
    """
    import uvicorn

    from tg_conductor.api.lifespan import build_app
    from tg_conductor.config.settings import get_settings
    from tg_conductor.logging import configure_logging

    configure_logging()
    settings = get_settings()
    app = build_app(settings)
    uvicorn.run(
        app,
        host=host or settings.bind_host,
        port=port or settings.bind_port,
        log_config=None,  # we own logging via configure_logging()
    )


@cli.command()
def migrate() -> None:
    """Run database migrations (alembic upgrade head)."""
    from tg_conductor.db.migrate import upgrade_head

    upgrade_head()
    click.echo("migrate: alembic upgrade head completed")


@cli.group()
def account() -> None:
    """Manage Telegram accounts."""


@account.command("login")
@click.option("--owner", type=int, required=True, help="Owner id")
@click.option("--label", required=True, help="Account label, unique per owner")
@click.option(
    "--proxy",
    default=None,
    help="Optional proxy URL (socks5://...) for this account",
)
def account_login(owner: int, label: str, proxy: str | None) -> None:
    """Interactive Telegram login; encrypts session_string into DB."""
    import asyncio

    from tg_conductor.cli.account import run_login

    rc = asyncio.run(run_login(owner=owner, label=label, proxy=proxy))
    raise SystemExit(rc)


@account.command("logout")
@click.option("--owner", type=int, required=True, help="Owner id")
@click.option("--label", required=True, help="Account label")
def account_logout(owner: int, label: str) -> None:
    """Soft-delete an account: status=disabled, session_string_enc=NULL."""
    import asyncio

    from tg_conductor.cli.account import run_logout

    rc = asyncio.run(run_logout(owner=owner, label=label))
    raise SystemExit(rc)


@account.command("list")
@click.option("--owner", type=int, required=True, help="Owner id")
@click.option(
    "--all",
    "include_disabled",
    is_flag=True,
    default=False,
    help="Also show disabled accounts",
)
def account_list(owner: int, include_disabled: bool) -> None:
    """List accounts for the given owner (sensitive fields masked)."""
    import asyncio

    from tg_conductor.cli.account import run_list

    rc = asyncio.run(run_list(owner=owner, include_disabled=include_disabled))
    raise SystemExit(rc)


if __name__ == "__main__":
    cli()
