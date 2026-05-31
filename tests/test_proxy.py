"""§5.9 — proxy resolution fallback chain + URL parsing."""

from __future__ import annotations

import pytest

from tg_conductor.tg_core.proxy import parse_proxy_url, resolve_proxy


def test_account_proxy_wins_over_env() -> None:
    proxy = resolve_proxy(
        account_proxy="socks5://1.2.3.4:1080",
        env_proxy="socks5://5.6.7.8:1080",
    )
    assert proxy is not None
    assert proxy["hostname"] == "1.2.3.4"
    assert proxy["port"] == 1080


def test_env_used_when_account_absent() -> None:
    proxy = resolve_proxy(account_proxy=None, env_proxy="socks5://5.6.7.8:1080")
    assert proxy is not None
    assert proxy["hostname"] == "5.6.7.8"


def test_both_absent_returns_none() -> None:
    assert resolve_proxy(account_proxy=None, env_proxy=None) is None
    assert resolve_proxy(account_proxy="", env_proxy="") is None


def test_parse_socks5_with_credentials() -> None:
    proxy = parse_proxy_url("socks5://user:pass@host.example:1080")
    assert proxy == {
        "scheme": "socks5",
        "hostname": "host.example",
        "port": 1080,
        "username": "user",
        "password": "pass",
    }


def test_parse_percent_encoded_credentials() -> None:
    """Credentials with reserved chars (@, :, /) survive via percent-encoding."""
    proxy = parse_proxy_url("socks5://us%40er:p%40ss%2F1@host.example:1080")
    assert proxy["username"] == "us@er"
    assert proxy["password"] == "p@ss/1"


def test_parse_http_proxy() -> None:
    assert parse_proxy_url("http://proxy.example:3128") == {
        "scheme": "http",
        "hostname": "proxy.example",
        "port": 3128,
    }


def test_unsupported_scheme_raises() -> None:
    with pytest.raises(ValueError, match="scheme"):
        parse_proxy_url("ftp://host:21")


def test_missing_port_raises() -> None:
    with pytest.raises(ValueError, match="host and port"):
        parse_proxy_url("socks5://host.example")
