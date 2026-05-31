"""Settings parsing — particularly the ``.env`` empty-string and CORS
comma-list shapes that bit real users on first deploy.

We don't test the ``APP_MASTER_KEY`` validator path here — that's
covered indirectly by every other test via the ``master_key`` fixture.
"""

from __future__ import annotations

import pytest

from tg_conductor.config.settings import Settings, get_settings


def test_cors_origins_empty_string_becomes_empty_list(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``CORS_ORIGINS=`` in .env yielded a JSONDecodeError pre-fix."""
    monkeypatch.setenv("CORS_ORIGINS", "")
    s = Settings()
    assert s.cors_origins == []


def test_cors_origins_comma_string_splits(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "CORS_ORIGINS", "https://a.example , https://b.example ,, https://c.example"
    )
    s = Settings()
    assert s.cors_origins == [
        "https://a.example",
        "https://b.example",
        "https://c.example",
    ]


def test_cors_origins_json_string_still_works(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CORS_ORIGINS", '["https://a.example","https://b.example"]')
    s = Settings()
    assert s.cors_origins == ["https://a.example", "https://b.example"]


def test_cors_origins_default_is_empty(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CORS_ORIGINS", raising=False)
    s = Settings()
    assert s.cors_origins == []


def test_tg_proxy_empty_string_collapses_to_none(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``TG_PROXY=`` in .env used to leak ``""`` into ``resolve_proxy``."""
    monkeypatch.setenv("TG_PROXY", "")
    s = Settings()
    assert s.tg_proxy is None


def test_tg_proxy_whitespace_only_collapses_to_none(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TG_PROXY", "   ")
    s = Settings()
    assert s.tg_proxy is None


def test_tg_proxy_real_value_kept(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TG_PROXY", "socks5://127.0.0.1:7890")
    s = Settings()
    assert s.tg_proxy == "socks5://127.0.0.1:7890"


def test_openai_api_key_empty_string_collapses_to_none(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "")
    s = Settings()
    assert s.openai_api_key is None


def test_openai_base_url_empty_string_collapses_to_none(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_BASE_URL", "")
    s = Settings()
    assert s.openai_base_url is None


def test_load_settings_succeeds_with_blank_optional_envs(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: a .env-style config with every optional left blank loads."""
    for k in ("CORS_ORIGINS", "TG_PROXY", "OPENAI_API_KEY", "OPENAI_BASE_URL"):
        monkeypatch.setenv(k, "")
    s = get_settings()
    assert s.cors_origins == []
    assert s.tg_proxy is None
    assert s.openai_api_key is None
    assert s.openai_base_url is None
