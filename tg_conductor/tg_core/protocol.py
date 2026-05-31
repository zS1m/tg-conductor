"""Backend-agnostic Telegram client protocol.

Business modules (accounts / workflows / actions / scheduler / runs) MUST
depend only on this protocol — never directly on ``kurigram`` / ``pyrogram``.
This keeps the upstream library swappable (e.g. Telethon in the future) and
makes business code unit-testable against ``FakeTGClient``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

# JSON-safe representation of a Message's typed fields (no ``raw`` / no
# ``buttons`` — those don't survive a JSON round-trip and aren't needed by
# the consumers that read a persisted trigger message). See
# :func:`message_to_payload` / :func:`message_from_payload`.
MessagePayload = dict[str, Any]


@dataclass(slots=True)
class ButtonSpec:
    """A single inline-keyboard button as seen by ``click_button``."""

    text: str
    callback_data: str | None = None
    url: str | None = None


@dataclass(slots=True)
class Message:
    """Minimal Telegram message surface used by tg-conductor.

    The adapter populates this from its native representation. ``raw`` holds
    the backend object for cases where we genuinely need richer fields; new
    business code SHOULD prefer the typed attrs and extend this dataclass
    rather than reaching into ``raw``.
    """

    id: int
    chat_id: int
    date: datetime
    text: str | None = None
    from_user_id: int | None = None
    topic_id: int | None = None
    buttons: list[list[ButtonSpec]] | None = None
    raw: Any = None


def message_to_payload(msg: Message) -> MessagePayload:
    """Serialize a ``Message``'s typed fields to a JSON-safe dict.

    Used to stash a ``message_match`` trigger message into ``jobs``'
    ``resolved_payload`` so the worker can rebuild it later. ``raw`` and
    ``buttons`` are intentionally dropped — they don't JSON-serialize and
    no consumer of a persisted trigger message reads them.
    """
    return {
        "id": msg.id,
        "chat_id": msg.chat_id,
        "date": msg.date.isoformat(),
        "text": msg.text,
        "from_user_id": msg.from_user_id,
        "topic_id": msg.topic_id,
    }


def message_from_payload(payload: MessagePayload) -> Message:
    """Inverse of :func:`message_to_payload` (``raw``/``buttons`` stay None)."""
    return Message(
        id=payload["id"],
        chat_id=payload["chat_id"],
        date=datetime.fromisoformat(payload["date"]),
        text=payload.get("text"),
        from_user_id=payload.get("from_user_id"),
        topic_id=payload.get("topic_id"),
    )


@dataclass(slots=True)
class Dialog:
    chat_id: int
    title: str
    is_forum: bool = False


@dataclass(slots=True)
class ForumTopic:
    topic_id: int
    title: str


MessageHandler = Callable[[Message], Awaitable[None]]
MessagePredicate = Callable[[Message], bool]


class TGClient(Protocol):
    """Required surface for any Telegram adapter (Kurigram, Telethon, Fake, ...)."""

    label: str

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    def is_connected(self) -> bool: ...

    async def login_with_session_string(self, session_string: str) -> None: ...

    async def send_text(
        self,
        chat_id: int | str,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        disable_web_page_preview: bool = False,
    ) -> Message: ...

    async def send_dice(self, chat_id: int | str, emoji: str = "🎲") -> Message: ...

    async def forward(
        self,
        from_chat_id: int | str,
        message_id: int,
        to_chat_id: int | str,
    ) -> Message: ...

    async def click_button(
        self,
        chat_id: int | str,
        message_id: int,
        *,
        text: str | None = None,
        pattern: str | None = None,
    ) -> None: ...

    async def wait_for(
        self,
        chat_id: int | str,
        *,
        predicate: MessagePredicate,
        timeout: float,
    ) -> Message: ...

    def get_dialogs(self, limit: int = 0) -> AsyncIterator[Dialog]: ...

    def list_topics(self, chat_id: int | str) -> AsyncIterator[ForumTopic]: ...

    def on_message(self, handler: MessageHandler) -> None: ...
