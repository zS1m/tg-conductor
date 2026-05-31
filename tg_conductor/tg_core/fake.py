"""In-memory ``TGClient`` for unit tests.

Records every call (method name + args/kwargs) and lets the test feed
inbound messages to subscribed handlers and to outstanding ``wait_for``
awaiters. Sufficient for §11 action / §10 scheduler tests; not a full
Telegram simulator.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from tg_conductor.tg_core.protocol import (
    Dialog,
    ForumTopic,
    Message,
    MessageHandler,
    MessagePredicate,
)


@dataclass(slots=True)
class FakeCall:
    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


class FakeTGClient:
    """Minimal ``TGClient`` implementation for tests.

    Inject prepared messages with :meth:`inject_message`; they fan out to
    every handler registered via :meth:`on_message` and unblock any pending
    :meth:`wait_for` whose predicate accepts the message.
    """

    def __init__(self, *, label: str = "fake") -> None:
        self.label = label
        self.calls: list[FakeCall] = []
        self._connected = False
        self._handlers: list[MessageHandler] = []
        self._waiters: list[tuple[asyncio.Future[Message], MessagePredicate]] = []
        self._next_message_id = 1
        self._chat_id_registry: dict[str, int] = {}
        self.dialogs: list[Dialog] = []
        self.topics: dict[int | str, list[ForumTopic]] = {}
        # If non-None, the next call to ``send_text`` / ``connect`` raises
        # this exception and clears the slot (one-shot fault injection).
        self.next_send_text_raises: BaseException | None = None
        self.next_connect_raises: BaseException | None = None

    # -- bookkeeping ----------------------------------------------------

    def _record(self, method: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append(FakeCall(method, args, kwargs))

    def resolve_chat_id(self, chat_id: int | str) -> int:
        """Map ``int`` through unchanged; assign a stable negative int per ``str``.

        Telegram usernames / channel handles (``@foo``) only resolve to numeric
        ids at the wire level; tests just need a stable mapping so message
        equality holds across repeated calls to the same handle.
        """
        if isinstance(chat_id, int):
            return chat_id
        if chat_id not in self._chat_id_registry:
            self._chat_id_registry[chat_id] = -(len(self._chat_id_registry) + 1)
        return self._chat_id_registry[chat_id]

    def _make_message(
        self,
        chat_id: int | str,
        text: str | None = None,
        **extras: Any,
    ) -> Message:
        msg = Message(
            id=self._next_message_id,
            chat_id=self.resolve_chat_id(chat_id),
            date=datetime.now(UTC),
            text=text,
            **extras,
        )
        self._next_message_id += 1
        return msg

    # -- TGClient surface ----------------------------------------------

    async def connect(self) -> None:
        self._record("connect")
        if self.next_connect_raises is not None:
            exc, self.next_connect_raises = self.next_connect_raises, None
            raise exc
        self._connected = True

    async def close(self) -> None:
        self._record("close")
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    async def login_with_session_string(self, session_string: str) -> None:
        self._record("login_with_session_string", session_string)

    async def send_text(
        self,
        chat_id: int | str,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        disable_web_page_preview: bool = False,
    ) -> Message:
        self._record(
            "send_text",
            chat_id,
            text,
            reply_to_message_id=reply_to_message_id,
            disable_web_page_preview=disable_web_page_preview,
        )
        if self.next_send_text_raises is not None:
            exc, self.next_send_text_raises = self.next_send_text_raises, None
            raise exc
        return self._make_message(chat_id, text=text)

    async def send_dice(self, chat_id: int | str, emoji: str = "🎲") -> Message:
        self._record("send_dice", chat_id, emoji)
        return self._make_message(chat_id, text=emoji)

    async def forward(
        self,
        from_chat_id: int | str,
        message_id: int,
        to_chat_id: int | str,
    ) -> Message:
        self._record("forward", from_chat_id, message_id, to_chat_id)
        return self._make_message(to_chat_id)

    async def click_button(
        self,
        chat_id: int | str,
        message_id: int,
        *,
        text: str | None = None,
        pattern: str | None = None,
    ) -> None:
        self._record("click_button", chat_id, message_id, text=text, pattern=pattern)

    async def wait_for(
        self,
        chat_id: int | str,
        *,
        predicate: MessagePredicate,
        timeout: float,
    ) -> Message:
        self._record("wait_for", chat_id, timeout=timeout)
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Message] = loop.create_future()
        target = self.resolve_chat_id(chat_id)

        def _pred(msg: Message) -> bool:
            return msg.chat_id == target and predicate(msg)

        self._waiters.append((fut, _pred))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._waiters = [(f, p) for (f, p) in self._waiters if f is not fut]

    async def get_dialogs(self, limit: int = 0) -> AsyncIterator[Dialog]:  # type: ignore[override]
        self._record("get_dialogs", limit=limit)
        for d in self.dialogs[: limit or None]:
            yield d

    async def list_topics(  # type: ignore[override]
        self, chat_id: int | str
    ) -> AsyncIterator[ForumTopic]:
        self._record("list_topics", chat_id)
        for t in self.topics.get(chat_id, []):
            yield t

    def on_message(self, handler: MessageHandler) -> None:
        self._record("on_message", handler)
        self._handlers.append(handler)

    # -- test helpers ---------------------------------------------------

    def inject_message(self, msg: Message) -> list[asyncio.Task[None]]:
        """Fan-out ``msg`` to handlers and unblock matching waiters."""
        for fut, pred in list(self._waiters):
            if not fut.done() and pred(msg):
                fut.set_result(msg)

        tasks: list[asyncio.Task[None]] = []
        for handler in self._handlers:
            tasks.append(asyncio.create_task(handler(msg)))
        return tasks

    def calls_to(self, method: str) -> list[FakeCall]:
        return [c for c in self.calls if c.method == method]


def make_fake_message(
    chat_id: int,
    text: str | None = None,
    **extras: Any,
) -> Message:
    """Standalone ``Message`` builder for tests (does not increment any counter)."""
    return Message(
        id=extras.pop("id", 1),
        chat_id=chat_id,
        date=extras.pop("date", datetime.now(UTC)),
        text=text,
        **extras,
    )


__all__ = ["FakeCall", "FakeTGClient", "make_fake_message"]
