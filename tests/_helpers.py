"""Shared test fakes that don't belong in production ``tg_conductor.*`` packages.

Currently:

* :class:`FakeAIClient` — deterministic ``AIClient`` Protocol implementation.
  Records every call; :attr:`reply_text` controls what ``chat`` returns.
"""

from __future__ import annotations

from typing import Any

from tg_conductor.actions.context import ChatMessage, ChatResult, VisionResult


class FakeAIClient:
    def __init__(self, *, reply_text: str = "ai-reply") -> None:
        self.reply_text = reply_text
        self.chat_calls: list[dict[str, Any]] = []
        self.vision_calls: list[dict[str, Any]] = []

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
    ) -> ChatResult:
        self.chat_calls.append(
            {
                "model": model,
                "messages": messages,
                "max_tokens": max_tokens,
                "owner_id": owner_id,
                "run_id": run_id,
                "workflow_id": workflow_id,
                "account_id": account_id,
            }
        )
        return ChatResult(
            text=self.reply_text,
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=15,
            model=model or "fake-chat",
            latency_ms=20,
        )

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
    ) -> VisionResult:
        self.vision_calls.append(
            {
                "model": model,
                "prompt": prompt,
                "image_count": len(images),
                "owner_id": owner_id,
                "run_id": run_id,
                "workflow_id": workflow_id,
                "account_id": account_id,
            }
        )
        return VisionResult(
            text="vision-reply",
            prompt_tokens=20,
            completion_tokens=10,
            total_tokens=30,
            model=model or "fake-vision",
            latency_ms=50,
        )
