"""CLI surface tests via click.testing.CliRunner.

Covers what the manual smoke test verifies: subcommand registration,
``--version`` output, and that not-yet-implemented stubs exit non-zero so
they can't be confused with success in scripts / CI.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from tg_conductor import __version__
from tg_conductor.__main__ import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_top_level_help_lists_all_subcommands(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    for cmd in ("serve", "migrate", "account", "version"):
        assert cmd in result.output


def test_account_group_lists_subcommands(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["account", "--help"])
    assert result.exit_code == 0
    for cmd in ("login", "logout", "list"):
        assert cmd in result.output


def test_version_subcommand_prints_package_version(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_version_flag_prints_package_version(runner: CliRunner) -> None:
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_serve_help_describes_lifespan(runner: CliRunner) -> None:
    """``serve`` is wired in §16 — the stub behavior is gone.

    We don't actually run uvicorn here (it'd block); we just assert the
    command is registered and exposes its options.
    """
    result = runner.invoke(cli, ["serve", "--help"])
    assert result.exit_code == 0, result.output
    assert "--host" in result.output
    assert "--port" in result.output
