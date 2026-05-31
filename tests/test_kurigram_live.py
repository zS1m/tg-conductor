"""§5.10 — live integration test for KurigramAdapter.

Default skipped. To run:

    export TG_LIVE_API_ID=...
    export TG_LIVE_API_HASH=...
    export TG_LIVE_SESSION_STRING=...     # from a real Telegram account
    uv run pytest -m live -vv

The test connects with the given session_string, posts a marker message to
Saved Messages, and closes. No assertions about message content beyond
"the call returned a Message with the expected chat id" — the goal is to
confirm the whole adapter stack (connect → throttle → wire → close) works
against a real server.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

_REQUIRED_ENV = ("TG_LIVE_API_ID", "TG_LIVE_API_HASH", "TG_LIVE_SESSION_STRING")


def _env_present() -> bool:
    return all(os.getenv(v) for v in _REQUIRED_ENV)


@pytest.mark.live
@pytest.mark.skipif(not _env_present(), reason=f"set {', '.join(_REQUIRED_ENV)} to run")
async def test_connect_send_to_saved_messages_then_close() -> None:
    from tg_conductor.tg_core.kurigram_adapter import KurigramAdapter

    adapter = KurigramAdapter(
        api_id=int(os.environ["TG_LIVE_API_ID"]),
        api_hash=os.environ["TG_LIVE_API_HASH"],
        session_string=os.environ["TG_LIVE_SESSION_STRING"],
        account_label="tg-conductor-live-test",
    )
    assert not adapter.is_connected()

    await adapter.connect()
    try:
        assert adapter.is_connected()
        marker = f"tg-conductor live test @ {datetime.now(UTC).isoformat()}"
        # "me" → Saved Messages
        msg = await adapter.send_text("me", marker)
        assert msg.text == marker
        assert msg.id > 0
    finally:
        await adapter.close()
        assert not adapter.is_connected()
