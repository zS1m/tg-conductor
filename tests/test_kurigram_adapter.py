"""Unit tests for :class:`KurigramAdapter` that don't touch the network.

The :func:`tests.test_kurigram_live` module exercises the full stack against
real Telegram (skipped by default). These tests cover the parts that can be
verified with mocks: the pyrogram → tg_core ``Message`` projection, the
FloodWait translation in ``_call``, and the lifecycle guards.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pyrogram import errors as pyrogram_errors

from tg_conductor.tg_core.exceptions import TGFloodWait
from tg_conductor.tg_core.kurigram_adapter import KurigramAdapter, _to_message
from tg_conductor.tg_core.throttle import AccountThrottle


def _fast_throttle() -> AccountThrottle:
    """Throttle config that makes negative-path tests fast (no retries, no sleep)."""
    return AccountThrottle(
        min_interval_seconds=0.0,
        max_floodwait_retries=0,
        floodwait_padding_seconds=0.0,
    )


def _fake_pyrogram_message(**overrides) -> SimpleNamespace:  # type: ignore[no-untyped-def]
    """Return a minimal duck-typed stand-in for ``pyrogram.types.Message``."""
    defaults = {
        "id": 42,
        "chat": SimpleNamespace(id=-100123, title="t"),
        "date": datetime(2026, 5, 28, tzinfo=UTC),
        "text": "hello",
        "caption": None,
        "from_user": SimpleNamespace(id=7),
        "message_thread_id": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_to_message_projects_pyrogram_fields() -> None:
    msg = _to_message(_fake_pyrogram_message())
    assert msg.id == 42
    assert msg.chat_id == -100123
    assert msg.text == "hello"
    assert msg.from_user_id == 7
    assert msg.topic_id is None
    assert msg.raw is not None  # raw passes through


def test_to_message_falls_back_to_caption_when_text_is_none() -> None:
    msg = _to_message(_fake_pyrogram_message(text=None, caption="caption-text"))
    assert msg.text == "caption-text"


def test_to_message_tolerates_no_from_user() -> None:
    msg = _to_message(_fake_pyrogram_message(from_user=None))
    assert msg.from_user_id is None


def test_to_message_picks_up_topic_id_when_present() -> None:
    msg = _to_message(_fake_pyrogram_message(message_thread_id=99))
    assert msg.topic_id == 99


def _new_adapter_without_session() -> KurigramAdapter:
    return KurigramAdapter(
        api_id=1,
        api_hash="x",
        session_string=None,
        account_label="tester",
        throttle=_fast_throttle(),
    )


def test_no_session_means_no_client() -> None:
    adapter = _new_adapter_without_session()
    assert adapter._client is None
    assert adapter.is_connected() is False


@pytest.mark.asyncio
async def test_connect_without_session_raises() -> None:
    adapter = _new_adapter_without_session()
    with pytest.raises(RuntimeError, match="session_string"):
        await adapter.connect()


@pytest.mark.asyncio
async def test_send_text_without_session_raises() -> None:
    adapter = _new_adapter_without_session()
    with pytest.raises(RuntimeError, match="no client"):
        await adapter.send_text(1, "hi")


@pytest.mark.asyncio
async def test_call_translates_pyrogram_floodwait_to_tg_floodwait() -> None:
    adapter = _new_adapter_without_session()

    async def boom() -> None:
        # The real pyrogram FloodWait wants an `Object`-shaped arg; construct
        # the bare instance and stuff the value attribute manually so we don't
        # have to import its parents.
        exc = pyrogram_errors.FloodWait.__new__(pyrogram_errors.FloodWait)
        exc.value = 7
        raise exc

    with pytest.raises(TGFloodWait) as excinfo:
        await adapter._call("send_text", boom)
    assert excinfo.value.seconds == 7.0
    assert excinfo.value.operation == "send_text"


def test_default_adapter_builds_client_with_updates_enabled() -> None:
    """Default (``receive_updates=True``) keeps pyrogram's update behaviour."""
    adapter = KurigramAdapter(
        api_id=1,
        api_hash="x",
        session_string="some-base64-session",
        account_label="recv",
        throttle=_fast_throttle(),
    )
    assert adapter.receive_updates is True
    # ``not True`` → False, behaviourally identical to pyrogram's default.
    assert adapter._client is not None
    assert bool(adapter._client.no_updates) is False


def test_send_only_adapter_builds_client_with_no_updates() -> None:
    """``receive_updates=False`` builds the underlying client with no_updates=True."""
    adapter = KurigramAdapter(
        api_id=1,
        api_hash="x",
        session_string="some-base64-session",
        account_label="sendonly",
        throttle=_fast_throttle(),
        receive_updates=False,
    )
    assert adapter.receive_updates is False
    assert adapter._client is not None
    assert adapter._client.no_updates is True


@pytest.mark.asyncio
async def test_send_only_adapter_can_still_send_text() -> None:
    """``no_updates`` must not break outbound calls — send_text still works."""
    import asyncio

    adapter = KurigramAdapter(
        api_id=1,
        api_hash="x",
        session_string=None,
        account_label="sendonly",
        throttle=_fast_throttle(),
        receive_updates=False,
    )
    sent: list[tuple] = []

    class _FakeClient:
        async def send_message(self, **kwargs):  # type: ignore[no-untyped-def]
            sent.append((kwargs["chat_id"], kwargs["text"]))
            return _fake_pyrogram_message(text=kwargs["text"])

    adapter._client = _FakeClient()  # type: ignore[assignment]
    msg = await adapter.send_text(-100, "ping")
    assert msg.text == "ping"
    assert sent == [(-100, "ping")]
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_login_with_session_string_rebuilds_client() -> None:
    adapter = _new_adapter_without_session()
    await adapter.login_with_session_string("some-base64-session")
    assert adapter._client is not None
    # Connection state is unchanged — login just swaps the client object.
    assert adapter.is_connected() is False


@pytest.mark.asyncio
async def test_click_button_calls_get_messages_then_click() -> None:
    """Adapter resolves the message then forwards the click to pyrogram."""
    import asyncio

    adapter = _new_adapter_without_session()
    click_log: list[str] = []

    class _FakePyrogramMessage:
        async def click(self, text: str) -> None:
            click_log.append(text)

    class _FakeClient:
        async def get_messages(self, *, chat_id, message_ids):  # type: ignore[no-untyped-def]
            assert chat_id == -100
            assert message_ids == 42
            return _FakePyrogramMessage()

    adapter._client = _FakeClient()  # type: ignore[assignment]
    await adapter.click_button(-100, 42, text="签到")
    assert click_log == ["签到"]

    # Wait for any pending throttle bookkeeping.
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_click_button_requires_text_or_pattern() -> None:
    adapter = _new_adapter_without_session()
    adapter._client = object()  # type: ignore[assignment]
    with pytest.raises(ValueError, match="text"):
        await adapter.click_button(1, 1)


@pytest.mark.asyncio
async def test_wait_for_registers_handler_and_resolves_on_match() -> None:
    """A predicate-matching message completes the future; non-matches are skipped."""
    adapter = _new_adapter_without_session()
    registered: list[object] = []
    removed: list[object] = []
    captured_callbacks: list = []  # type: ignore[var-annotated]

    class _FakeClient:
        def add_handler(self, handler) -> None:  # type: ignore[no-untyped-def]
            registered.append(handler)
            captured_callbacks.append(handler.callback)

        def remove_handler(self, handler) -> None:  # type: ignore[no-untyped-def]
            removed.append(handler)

    adapter._client = _FakeClient()  # type: ignore[assignment]

    async def trigger() -> None:
        # The handler is registered synchronously inside wait_for; we feed a
        # pyrogram-shaped message after a yield so the future awaits first.
        import asyncio

        await asyncio.sleep(0)
        cb = captured_callbacks[-1]
        # Wrong chat first — should be ignored.
        await cb(None, _fake_pyrogram_message(chat=SimpleNamespace(id=-999), text="x"))
        # Matching chat + matching predicate.
        await cb(None, _fake_pyrogram_message(text="pong"))

    import asyncio

    asyncio.create_task(trigger())
    msg = await adapter.wait_for(
        -100123,
        predicate=lambda m: "pong" in (m.text or ""),
        timeout=0.5,
    )
    assert msg.text == "pong"
    assert len(registered) == 1
    assert len(removed) == 1  # handler cleaned up in finally


@pytest.mark.asyncio
async def test_wait_for_timeout_still_cleans_up_handler() -> None:
    import asyncio

    adapter = _new_adapter_without_session()
    removed: list[object] = []

    class _FakeClient:
        def add_handler(self, handler) -> None:  # type: ignore[no-untyped-def]
            pass

        def remove_handler(self, handler) -> None:  # type: ignore[no-untyped-def]
            removed.append(handler)

    adapter._client = _FakeClient()  # type: ignore[assignment]
    with pytest.raises(asyncio.TimeoutError):
        await adapter.wait_for(1, predicate=lambda _m: True, timeout=0.05)
    assert len(removed) == 1
