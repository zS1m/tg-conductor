"""Interactive Telegram auth: phone → code → (2FA) → session_string.

This is the only ``tg_core`` module that touches pyrogram outside of
``KurigramAdapter`` / ``_kurigram_patches``. It exists because the ``login``
flow precedes a usable ``TGClient`` instance: the whole point is to obtain
the ``session_string`` that ``TGClient`` is later constructed from.

Keeping the flow here means CLI / HTTP front-ends can import a backend-
agnostic function and inject their own ``get_code`` / ``get_password``
prompt mechanisms (click, websockets, web form, …) without pulling pyrogram
into the caller's import graph.
"""

from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from typing import TypeAlias, TypeVar

from tg_conductor.tg_core.exceptions import TGError
from tg_conductor.tg_core.proxy import resolve_proxy

T = TypeVar("T")
PromptResult: TypeAlias = str | Awaitable[str]
Prompter: TypeAlias = Callable[[], PromptResult]


class LoginError(TGError):
    """Telegram rejected the login (bad code, expired code, banned, ...)."""


async def _resolve(value: PromptResult) -> str:
    return await value if isawaitable(value) else value  # type: ignore[return-value]


async def interactive_login(
    *,
    api_id: int,
    api_hash: str,
    phone: str,
    get_code: Prompter,
    get_password: Prompter,
    proxy: str | None = None,
    env_proxy: str | None = None,
    name: str | None = None,
) -> str:
    """Drive Telegram's interactive auth flow; return the exported session_string.

    Parameters
    ----------
    get_code, get_password:
        Prompters returning either a ``str`` or an awaitable yielding ``str``.
        ``get_password`` is only invoked if Telegram reports 2FA enabled.
    name:
        Optional pyrogram client name (the in-memory session identifier).
        If omitted a random short token is used so PII (phone) doesn't show
        up in pyrogram log lines.

    Raises
    ------
    LoginError
        Telegram-side failure: ``BadRequest`` family (invalid code / expired
        code / phone banned, etc.) or any other unexpected exception during
        the flow. The cause is chained.
    """
    import pyrogram
    from pyrogram import errors as pyrogram_errors

    proxy_dict = resolve_proxy(proxy, env_proxy)
    client_name = name or f"login-{secrets.token_hex(4)}"
    client = pyrogram.Client(
        name=client_name,
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
        proxy=proxy_dict,
    )

    await client.connect()
    try:
        sent = await client.send_code(phone)
        code = await _resolve(get_code())
        try:
            await client.sign_in(phone, sent.phone_code_hash, code)
        except pyrogram_errors.SessionPasswordNeeded:
            password = await _resolve(get_password())
            await client.check_password(password)
        return await client.export_session_string()
    except pyrogram_errors.BadRequest as exc:
        raise LoginError(str(exc)) from exc
    except Exception as exc:
        if isinstance(exc, LoginError):
            raise
        raise LoginError(repr(exc)) from exc
    finally:
        try:
            await client.disconnect()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass
