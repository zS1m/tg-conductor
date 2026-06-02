"""Kurigram-backed implementation of :class:`TGClient`.

The adapter is the only file in ``tg_core`` that imports ``pyrogram`` /
``kurigram``. Every public method:

* runs through an :class:`AccountThrottle` (per-instance serial + min-interval
  + FloodWait retry),
* translates ``pyrogram.errors.FloodWait`` into our backend-agnostic
  :class:`TGFloodWait` so the throttle and any caller stay vendor-neutral,
* converts ``pyrogram.types.Message`` into our :class:`Message` dataclass
  so business code never holds a vendor object.

Lifecycle: ``KurigramAdapter(session_string=..., ...)`` builds the underlying
client lazily; the network connection only opens on :meth:`connect`. Use
:meth:`login_with_session_string` to swap the session at runtime (rare —
session is normally set once at account-login time).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pyrogram
from pyrogram import errors as pyrogram_errors
from pyrogram import types as pyrogram_types
from pyrogram.handlers import MessageHandler as PyrogramMessageHandler

from tg_conductor.tg_core._kurigram_patches import SafeGetForumTopics
from tg_conductor.tg_core.exceptions import TGFloodWait
from tg_conductor.tg_core.protocol import (
    Dialog,
    ForumTopic,
    Message,
    MessageHandler,
    MessagePredicate,
)
from tg_conductor.tg_core.proxy import resolve_proxy
from tg_conductor.tg_core.throttle import AccountThrottle


class _PatchedClient(SafeGetForumTopics, pyrogram.Client):
    """Pyrogram ``Client`` with our local bug patches mixed in via MRO."""


def _to_message(pyr_msg: pyrogram_types.Message) -> Message:
    """Project a pyrogram Message onto our minimal :class:`Message` surface."""
    from_user_id = pyr_msg.from_user.id if pyr_msg.from_user is not None else None
    topic_id = getattr(pyr_msg, "message_thread_id", None)
    return Message(
        id=pyr_msg.id,
        chat_id=pyr_msg.chat.id,
        date=pyr_msg.date,
        text=pyr_msg.text or pyr_msg.caption,
        from_user_id=from_user_id,
        topic_id=topic_id,
        raw=pyr_msg,
    )


class KurigramAdapter:
    """Concrete :class:`TGClient` over Kurigram. See module docstring."""

    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        session_string: str | None = None,
        proxy: str | None = None,
        account_label: str,
        env_proxy: str | None = None,
        throttle: AccountThrottle | None = None,
        receive_updates: bool = True,
    ) -> None:
        self.label = account_label
        self._api_id = api_id
        self._api_hash = api_hash
        self._proxy_url = proxy
        self._env_proxy_url = env_proxy
        # Backend-agnostic "does this connection subscribe to Telegram updates?"
        # flag (see accounts spec "Account 常驻连接生命周期" / tg-core spec). When
        # False the underlying Kurigram client is built with ``no_updates=True``
        # so it never runs ``updates.GetDifference`` / ``GetChannelDifference``;
        # outbound sends are unaffected.
        self._receive_updates = receive_updates
        self._client: pyrogram.Client | None = None
        self._throttle = throttle or AccountThrottle()
        if session_string:
            self._build_client(session_string)

    @property
    def receive_updates(self) -> bool:
        """Whether this connection subscribes to inbound Telegram updates."""
        return self._receive_updates

    # -- lifecycle ------------------------------------------------------

    def _build_client(self, session_string: str) -> None:
        proxy_dict = resolve_proxy(self._proxy_url, self._env_proxy_url)
        self._client = _PatchedClient(
            name=self.label,
            api_id=self._api_id,
            api_hash=self._api_hash,
            session_string=session_string,
            in_memory=True,
            proxy=proxy_dict,
            # send-only accounts: ``no_updates=True`` wraps every request in
            # InvokeWithoutUpdates and starts no dispatcher workers, so the
            # server stops pushing updates and the runtime GetChannelDifference
            # path never fires. ``False`` is behaviourally identical to
            # pyrogram's default (updates enabled) — keeps default unchanged.
            no_updates=not self._receive_updates,
        )

    async def connect(self) -> None:
        if self._client is None:
            raise RuntimeError(
                "KurigramAdapter has no session_string yet; "
                "call login_with_session_string() first",
            )
        await self._client.start()

    async def close(self) -> None:
        if self._client is not None and self._client.is_connected:
            await self._client.stop()

    def is_connected(self) -> bool:
        return self._client is not None and bool(self._client.is_connected)

    async def login_with_session_string(self, session_string: str) -> None:
        """Swap the session and rebuild the underlying client (does not connect)."""
        if self._client is not None and self._client.is_connected:
            await self._client.stop()
        self._build_client(session_string)

    # -- wire calls -----------------------------------------------------

    async def _call(self, operation: str, fn: Any) -> Any:
        """Run ``fn`` through the throttle, mapping FloodWait to ours."""

        async def _wrapped() -> Any:
            try:
                return await fn()
            except pyrogram_errors.FloodWait as exc:
                seconds = float(getattr(exc, "value", 0) or 0)
                raise TGFloodWait(seconds=seconds, operation=operation) from exc

        return await self._throttle.call(operation, _wrapped)

    def _require_client(self) -> pyrogram.Client:
        if self._client is None:
            raise RuntimeError(
                "KurigramAdapter has no client yet; "
                "construct with session_string or call login_with_session_string()",
            )
        return self._client

    async def send_text(
        self,
        chat_id: int | str,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        disable_web_page_preview: bool = False,
    ) -> Message:
        client = self._require_client()

        async def _fn() -> pyrogram_types.Message:
            return await client.send_message(
                chat_id=chat_id,
                text=text,
                reply_to_message_id=reply_to_message_id,
                disable_web_page_preview=disable_web_page_preview,
            )

        return _to_message(await self._call("send_text", _fn))

    async def send_dice(self, chat_id: int | str, emoji: str = "🎲") -> Message:
        client = self._require_client()

        async def _fn() -> pyrogram_types.Message:
            return await client.send_dice(chat_id=chat_id, emoji=emoji)

        return _to_message(await self._call("send_dice", _fn))

    async def forward(
        self,
        from_chat_id: int | str,
        message_id: int,
        to_chat_id: int | str,
    ) -> Message:
        client = self._require_client()

        async def _fn() -> pyrogram_types.Message:
            result = await client.forward_messages(
                chat_id=to_chat_id,
                from_chat_id=from_chat_id,
                message_ids=message_id,
            )
            # forward_messages returns Message | list[Message] depending on input.
            if isinstance(result, list):
                return result[0]
            return result

        return _to_message(await self._call("forward", _fn))

    async def click_button(
        self,
        chat_id: int | str,
        message_id: int,
        *,
        text: str | None = None,
        pattern: str | None = None,
    ) -> None:
        """Fetch the target message and click its inline button by ``text``.

        The action layer (``tg_conductor.actions.click_button``) does its own
        matching against ``Message.buttons`` and passes us the literal button
        text. We just resolve the live ``pyrogram.types.Message`` and call
        ``msg.click(text)``; pyrogram emits the callback for the matching
        button. ``pattern`` is part of the protocol but unused in our flow
        — the action passes ``text`` for both modes after resolution.
        """
        client = self._require_client()
        target_text = text if text is not None else pattern
        if target_text is None:
            raise ValueError(
                "click_button requires either `text` or `pattern` (got neither)"
            )

        async def _fn() -> None:
            msg = await client.get_messages(chat_id=chat_id, message_ids=message_id)
            await msg.click(target_text)

        await self._call("click_button", _fn)

    async def wait_for(
        self,
        chat_id: int | str,
        *,
        predicate: MessagePredicate,
        timeout: float,
    ) -> Message:
        """Block until a message arrives in ``chat_id`` that satisfies ``predicate``.

        Implemented by registering a transient pyrogram MessageHandler and
        completing an ``asyncio.Future`` on the first match. The handler is
        always removed in ``finally`` — including on timeout — so we never
        leak handler registrations.
        """
        client = self._require_client()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Message] = loop.create_future()
        want_chat_id = chat_id if isinstance(chat_id, int) else None

        async def _on_message(_c: pyrogram.Client, msg: pyrogram_types.Message) -> None:
            if fut.done():
                return
            try:
                wrapped = _to_message(msg)
            except Exception:  # noqa: BLE001 - skip messages we cannot project
                return
            if want_chat_id is not None and wrapped.chat_id != want_chat_id:
                return
            try:
                if not predicate(wrapped):
                    return
            except Exception:  # noqa: BLE001 - predicate authored elsewhere
                return
            if not fut.done():
                fut.set_result(wrapped)

        handler_obj = PyrogramMessageHandler(_on_message)
        client.add_handler(handler_obj)
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            try:
                client.remove_handler(handler_obj)
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass

    async def get_dialogs(self, limit: int = 0) -> AsyncIterator[Dialog]:
        client = self._require_client()
        # pyrogram's get_dialogs is itself an async iterator; we do not run
        # it under the throttle because it is a streaming endpoint that pages
        # internally. Individual page fetches go through the rate-limited
        # invoke path inside Kurigram.
        async for d in client.get_dialogs(limit=limit):
            yield Dialog(
                chat_id=d.chat.id,
                title=d.chat.title or d.chat.first_name or "",
                is_forum=bool(getattr(d.chat, "is_forum", False)),
            )

    async def list_topics(self, chat_id: int | str) -> AsyncIterator[ForumTopic]:
        client = self._require_client()
        async for t in client.get_forum_topics(chat_id):  # patched via mixin
            yield ForumTopic(topic_id=t.id, title=t.title)

    def on_message(self, handler: MessageHandler) -> None:
        client = self._require_client()

        async def _adapt(_c: pyrogram.Client, msg: pyrogram_types.Message) -> None:
            await handler(_to_message(msg))

        client.add_handler(PyrogramMessageHandler(_adapt))
