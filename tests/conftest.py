"""Shared pytest fixtures for tg-conductor tests."""

from __future__ import annotations

import base64
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Iterator[None]:
    """Clear the ``get_settings`` lru_cache around every test.

    Tests that mutate ``APP_MASTER_KEY`` (or any other env-backed field) via
    ``monkeypatch.setenv`` would otherwise see a stale singleton from a
    previous test. Clearing before and after keeps each test independent.
    """
    from tg_conductor.config.settings import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def master_key(monkeypatch: pytest.MonkeyPatch) -> str:
    """Fixed 32-byte AES-256 key for tests, exported via APP_MASTER_KEY."""
    key = base64.urlsafe_b64encode(b"\x00" * 32).decode()
    monkeypatch.setenv("APP_MASTER_KEY", key)
    return key


@pytest.fixture
def tmp_sqlite_url(tmp_path: Path) -> str:
    """A throwaway aiosqlite URL backed by a per-test temp file."""
    return f"sqlite+aiosqlite:///{tmp_path / 'test.sqlite3'}"
