"""Per-Run execution context passed through every action step.

The AccountWorker constructs one ``ActionContext`` per Run and threads it
through the executor → step functions. Mutable state on the context
(``last_matched_message``, ``step_index``, ``next_seq``) is updated in
place during plan execution.

``AIClient`` is a Protocol here so §11 actions can import a backend-agnostic
type; §13 supplies the real OpenAI-compatible implementation. Tests can
inject a hand-rolled stub.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from tg_conductor.runs.event_writer import EventWriter
    from tg_conductor.tg_core.protocol import Message, TGClient


# ---------------------------------------------------------------- AI client


@dataclass(slots=True)
class ChatMessage:
    """One entry in the OpenAI-style ``messages`` list."""

    role: str  # "system" | "user" | "assistant" | "tool"
    content: str


@dataclass(slots=True)
class ChatResult:
    """Return shape mandated by ``specs/ai-usage/spec.md``.

    ``total_tokens`` is normally ``prompt + completion`` but providers
    sometimes return a slightly different number (e.g. cached prompt
    tokens); store whatever the SDK reports.
    """

    text: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    latency_ms: int


@dataclass(slots=True)
class VisionResult:
    text: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    latency_ms: int


class AIClient(Protocol):
    """OpenAI-compatible chat + vision surface used by ``ai_reply`` / ``click_button``.

    ``owner_id`` / ``run_id`` / ``workflow_id`` / ``account_id`` are
    threaded through the call so the implementation can write a
    ``usage_events`` row tagged with the right tenant + provenance
    without each caller doing it (spec ai-usage §"每次调用写 usage_events").
    """

    async def chat(
        self,
        messages: list[ChatMessage],
        *,
        model: str | None = None,
        max_tokens: int | None = None,
        owner_id: int,
        run_id: int | None = None,
        workflow_id: int | None = None,
        account_id: int | None = None,
    ) -> ChatResult: ...

    async def vision(
        self,
        images: list[bytes],
        prompt: str,
        *,
        model: str | None = None,
        owner_id: int,
        run_id: int | None = None,
        workflow_id: int | None = None,
        account_id: int | None = None,
    ) -> VisionResult: ...


# ---------------------------------------------------------------- context


@dataclass
class ActionContext:
    """Mutable carrier threaded through every step of one Run.

    A single instance lives for the duration of one Run; the AccountWorker
    creates it after writing the ``runs`` row and discards it on Run end.
    """

    owner_id: int
    account_id: int
    workflow_id: int
    run_id: int
    job_id: int
    tg_client: TGClient
    ai_client: AIClient
    event_writer: EventWriter
    session_factory: async_sessionmaker[AsyncSession]

    # Mutable, updated by the executor / actions during plan execution.
    last_matched_message: Message | None = None
    step_index: int = 0
    next_seq: int = 0
    extras: dict[str, Any] = field(default_factory=dict)

    async def emit(
        self,
        event_type: str,
        attrs: dict[str, Any] | None = None,
        *,
        message: str = "",
        level: str = "INFO",
    ) -> None:
        """Bump seq and persist + publish one event.

        ``event_type`` / ``attrs`` / ``message`` / ``level`` match the
        ``run_events`` column shape from ``specs/runs/spec.md``. ``level``
        defaults to INFO; failure paths should pass ``"ERROR"``.
        """
        seq = self.next_seq
        self.next_seq += 1
        await self.event_writer.write_event(
            owner_id=self.owner_id,
            run_id=self.run_id,
            seq=seq,
            event_type=event_type,
            message=message,
            level=level,
            attrs=attrs,
        )
