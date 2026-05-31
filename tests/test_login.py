"""Unit tests for ``tg_core.login.interactive_login``.

Stubs ``pyrogram.Client`` so we exercise the orchestration (connect →
send_code → sign_in → 2FA branch → export_session_string → disconnect)
without touching the network. End-to-end real-Telegram coverage stays in
``test_kurigram_live``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pyrogram
import pytest
from pyrogram import errors as pyrogram_errors

from tg_conductor.tg_core.login import LoginError, interactive_login


class _FakeClient:
    """Records orchestration calls; pluggable per-test behaviour."""

    instances: list[_FakeClient] = []

    def __init__(self, **init_kwargs: Any) -> None:
        self.init_kwargs = init_kwargs
        self.events: list[str] = []
        self.sign_in_raises: BaseException | None = None
        self.check_password_raises: BaseException | None = None
        self.send_code_raises: BaseException | None = None
        self.exported = "session-string-exported"
        _FakeClient.instances.append(self)

    async def connect(self) -> None:
        self.events.append("connect")

    async def send_code(self, phone: str) -> SimpleNamespace:
        self.events.append(f"send_code:{phone}")
        if self.send_code_raises is not None:
            raise self.send_code_raises
        return SimpleNamespace(phone_code_hash=f"hash-{phone}")

    async def sign_in(self, phone: str, phone_code_hash: str, code: str) -> None:
        self.events.append(f"sign_in:{phone}:{phone_code_hash}:{code}")
        if self.sign_in_raises is not None:
            raise self.sign_in_raises

    async def check_password(self, password: str) -> None:
        self.events.append(f"check_password:{password}")
        if self.check_password_raises is not None:
            raise self.check_password_raises

    async def export_session_string(self) -> str:
        self.events.append("export")
        return self.exported

    async def disconnect(self) -> None:
        self.events.append("disconnect")


@pytest.fixture(autouse=True)
def _patch_pyrogram_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.instances.clear()
    monkeypatch.setattr(pyrogram, "Client", _FakeClient)


def _make_session_password_needed() -> pyrogram_errors.SessionPasswordNeeded:
    exc = pyrogram_errors.SessionPasswordNeeded.__new__(
        pyrogram_errors.SessionPasswordNeeded
    )
    exc.value = None
    return exc


def _make_bad_request(msg: str = "PHONE_CODE_INVALID") -> pyrogram_errors.BadRequest:
    exc = pyrogram_errors.BadRequest.__new__(pyrogram_errors.BadRequest)
    exc.MESSAGE = msg
    exc.value = None
    return exc


@pytest.mark.asyncio
async def test_happy_path_no_2fa() -> None:
    session = await interactive_login(
        api_id=1,
        api_hash="hash",
        phone="+1555",
        get_code=lambda: "12345",
        get_password=lambda: "should-not-be-called",
    )
    assert session == "session-string-exported"
    [client] = _FakeClient.instances
    assert client.events == [
        "connect",
        "send_code:+1555",
        "sign_in:+1555:hash-+1555:12345",
        "export",
        "disconnect",
    ]
    # Defensive: phone must NOT be smuggled into the pyrogram session name.
    assert "+1555" not in client.init_kwargs["name"]
    assert client.init_kwargs["in_memory"] is True


@pytest.mark.asyncio
async def test_2fa_branch_invokes_password_prompt() -> None:
    def factory_get_code() -> str:
        return "12345"

    async def factory_get_password() -> str:
        return "secret-2fa"

    # Pre-seed sign_in_raises by patching the next FakeClient instance after construction.
    original_init = _FakeClient.__init__

    def init_with_2fa(self: _FakeClient, **kw: Any) -> None:
        original_init(self, **kw)
        self.sign_in_raises = _make_session_password_needed()

    _FakeClient.__init__ = init_with_2fa  # type: ignore[method-assign]
    try:
        session = await interactive_login(
            api_id=1,
            api_hash="hash",
            phone="+1555",
            get_code=factory_get_code,
            get_password=factory_get_password,
        )
    finally:
        _FakeClient.__init__ = original_init  # type: ignore[method-assign]

    assert session == "session-string-exported"
    [client] = _FakeClient.instances
    assert "check_password:secret-2fa" in client.events
    # Order matters: sign_in (raises) → check_password → export → disconnect
    assert client.events[-3:] == ["check_password:secret-2fa", "export", "disconnect"]


@pytest.mark.asyncio
async def test_bad_request_during_sign_in_raises_LoginError() -> None:
    original_init = _FakeClient.__init__

    def init_raising(self: _FakeClient, **kw: Any) -> None:
        original_init(self, **kw)
        self.sign_in_raises = _make_bad_request("PHONE_CODE_INVALID")

    _FakeClient.__init__ = init_raising  # type: ignore[method-assign]
    try:
        with pytest.raises(LoginError):
            await interactive_login(
                api_id=1,
                api_hash="h",
                phone="+1555",
                get_code=lambda: "00000",
                get_password=lambda: "n/a",
            )
    finally:
        _FakeClient.__init__ = original_init  # type: ignore[method-assign]

    [client] = _FakeClient.instances
    # Even on failure, disconnect MUST run (resource cleanup).
    assert client.events[-1] == "disconnect"


@pytest.mark.asyncio
async def test_disconnect_runs_even_when_send_code_explodes() -> None:
    original_init = _FakeClient.__init__

    def init_send_code_boom(self: _FakeClient, **kw: Any) -> None:
        original_init(self, **kw)
        self.send_code_raises = RuntimeError("network down")

    _FakeClient.__init__ = init_send_code_boom  # type: ignore[method-assign]
    try:
        with pytest.raises(LoginError):
            await interactive_login(
                api_id=1,
                api_hash="h",
                phone="+1555",
                get_code=lambda: "00000",
                get_password=lambda: "n/a",
            )
    finally:
        _FakeClient.__init__ = original_init  # type: ignore[method-assign]

    [client] = _FakeClient.instances
    assert client.events == ["connect", "send_code:+1555", "disconnect"]


@pytest.mark.asyncio
async def test_proxy_is_resolved_and_passed_to_client() -> None:
    await interactive_login(
        api_id=1,
        api_hash="h",
        phone="+1555",
        get_code=lambda: "12345",
        get_password=lambda: "n/a",
        proxy="socks5://1.2.3.4:1080",
    )
    [client] = _FakeClient.instances
    proxy = client.init_kwargs["proxy"]
    assert proxy is not None
    assert proxy["hostname"] == "1.2.3.4"
    assert proxy["port"] == 1080


@pytest.mark.asyncio
async def test_async_prompts_are_awaited() -> None:
    async def code_async() -> str:
        return "12345"

    async def pw_async() -> str:
        return "n/a"

    session = await interactive_login(
        api_id=1,
        api_hash="h",
        phone="+1555",
        get_code=code_async,
        get_password=pw_async,
    )
    assert session == "session-string-exported"
