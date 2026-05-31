"""Telegram proxy resolution.

Effective proxy = ``account.proxy`` ⟶ ``TG_PROXY`` env ⟶ ``None``.

URL form: ``<scheme>://[user:pass@]host:port`` where scheme is
``socks5`` / ``socks4`` / ``http``. The parsed dict matches the keys
Kurigram expects in ``Client(proxy=...)``.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

_SUPPORTED_SCHEMES = {"socks5", "socks4", "http"}


def resolve_proxy(
    account_proxy: str | None,
    env_proxy: str | None,
) -> dict[str, Any] | None:
    """Pick the effective proxy URL and parse it; ``None`` means direct."""
    url = account_proxy or env_proxy
    if not url:
        return None
    return parse_proxy_url(url)


def parse_proxy_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in _SUPPORTED_SCHEMES:
        raise ValueError(
            f"Unsupported proxy scheme: {scheme!r} (want one of {_SUPPORTED_SCHEMES})"
        )
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"Proxy URL must include host and port: {url!r}")
    proxy: dict[str, Any] = {
        "scheme": scheme,
        "hostname": parsed.hostname,
        "port": parsed.port,
    }
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    return proxy
