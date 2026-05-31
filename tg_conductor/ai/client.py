"""OpenAI-backed :class:`AIClient` implementation.

Wraps a low-level *invoker* (a callable that talks to the OpenAI SDK)
with the cross-cutting concerns mandated by ``specs/ai-usage/spec.md``:

* ``OPENAI_API_KEY`` fail-fast at call time (not at boot, so the
  process can host non-AI workflows without leaking config errors).
* Per-call latency measurement.
* ``usage_events`` row written on **every** terminal state — success
  and failure — with kind ``openai.{chat,vision}`` /
  ``openai.{chat,vision}.failed`` and integer ``cost_micros`` from
  :class:`PricingTable`.
* Exceptions are re-raised after the failed row is committed.

For testability the actual OpenAI HTTP call lives in an injectable
``ChatInvoker`` / ``VisionInvoker`` Protocol. Tests pass a stub
returning a fixed ``_RawChatResponse`` / ``_RawVisionResponse``; the
production path's default invokers lazy-import the ``openai`` SDK so
unit tests don't need a real API key.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.actions.context import (
    ChatMessage,
    ChatResult,
    VisionResult,
)
from tg_conductor.ai import repo as usage_repo
from tg_conductor.ai.pricing import PricingTable
from tg_conductor.ai.usage import UsageKind
from tg_conductor.config.settings import Settings

log = logging.getLogger(__name__)


# ---------------------------------------------------------------- raw types


@dataclass(slots=True)
class RawChatResponse:
    """Provider-neutral envelope an invoker returns."""

    text: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str


@dataclass(slots=True)
class RawVisionResponse:
    text: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str


class ChatInvoker(Protocol):
    async def __call__(
        self,
        *,
        messages: list[ChatMessage],
        model: str,
        max_tokens: int | None,
    ) -> RawChatResponse: ...


class VisionInvoker(Protocol):
    async def __call__(
        self,
        *,
        images: list[bytes],
        prompt: str,
        model: str,
    ) -> RawVisionResponse: ...


# ---------------------------------------------------------------- OpenAIClient


class OpenAIClient:
    """Concrete :class:`AIClient` writing ``usage_events`` per call."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        pricing: PricingTable,
        settings: Settings,
        chat_invoker: ChatInvoker | None = None,
        vision_invoker: VisionInvoker | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._pricing = pricing
        self._settings = settings
        self._chat_invoker = chat_invoker or _build_real_chat_invoker(settings)
        self._vision_invoker = vision_invoker or _build_real_vision_invoker(settings)

    # ------------------------------------------------------------ chat

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
        self._require_api_key()
        resolved_model = model or self._settings.openai_model_chat
        start = time.perf_counter()
        try:
            raw = await self._chat_invoker(
                messages=messages, model=resolved_model, max_tokens=max_tokens
            )
        except Exception as exc:
            latency_ms = _elapsed_ms(start)
            await self._record(
                kind=UsageKind.openai_chat_failed.value,
                units=0,
                cost_micros=0,
                owner_id=owner_id,
                run_id=run_id,
                workflow_id=workflow_id,
                account_id=account_id,
                meta={
                    "model": resolved_model,
                    "latency_ms": latency_ms,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
            raise

        latency_ms = _elapsed_ms(start)
        cost = self._pricing.cost_micros(
            model=raw.model,
            prompt_tokens=raw.prompt_tokens,
            completion_tokens=raw.completion_tokens,
        )
        await self._record(
            kind=UsageKind.openai_chat.value,
            units=raw.prompt_tokens + raw.completion_tokens,
            cost_micros=cost,
            owner_id=owner_id,
            run_id=run_id,
            workflow_id=workflow_id,
            account_id=account_id,
            meta={
                "model": raw.model,
                "latency_ms": latency_ms,
                "prompt_tokens": raw.prompt_tokens,
                "completion_tokens": raw.completion_tokens,
                "total_tokens": raw.total_tokens,
            },
        )
        return ChatResult(
            text=raw.text,
            prompt_tokens=raw.prompt_tokens,
            completion_tokens=raw.completion_tokens,
            total_tokens=raw.total_tokens,
            model=raw.model,
            latency_ms=latency_ms,
        )

    # ------------------------------------------------------------ vision

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
        self._require_api_key()
        resolved_model = model or self._settings.openai_model_vision
        start = time.perf_counter()
        try:
            raw = await self._vision_invoker(
                images=images, prompt=prompt, model=resolved_model
            )
        except Exception as exc:
            latency_ms = _elapsed_ms(start)
            await self._record(
                kind=UsageKind.openai_vision_failed.value,
                units=0,
                cost_micros=0,
                owner_id=owner_id,
                run_id=run_id,
                workflow_id=workflow_id,
                account_id=account_id,
                meta={
                    "model": resolved_model,
                    "latency_ms": latency_ms,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
            raise

        latency_ms = _elapsed_ms(start)
        cost = self._pricing.cost_micros(
            model=raw.model,
            prompt_tokens=raw.prompt_tokens,
            completion_tokens=raw.completion_tokens,
        )
        await self._record(
            kind=UsageKind.openai_vision.value,
            units=raw.prompt_tokens + raw.completion_tokens,
            cost_micros=cost,
            owner_id=owner_id,
            run_id=run_id,
            workflow_id=workflow_id,
            account_id=account_id,
            meta={
                "model": raw.model,
                "latency_ms": latency_ms,
                "prompt_tokens": raw.prompt_tokens,
                "completion_tokens": raw.completion_tokens,
                "total_tokens": raw.total_tokens,
                "image_count": len(images),
            },
        )
        return VisionResult(
            text=raw.text,
            prompt_tokens=raw.prompt_tokens,
            completion_tokens=raw.completion_tokens,
            total_tokens=raw.total_tokens,
            model=raw.model,
            latency_ms=latency_ms,
        )

    # ------------------------------------------------------------ internals

    def _require_api_key(self) -> None:
        # spec ai-usage §"缺凭据 fail-fast" — surfaced at call time, not boot.
        if self._settings.openai_api_key is None:
            raise RuntimeError(
                "OPENAI_API_KEY missing — set it in the environment or .env "
                "before invoking AI actions"
            )

    async def _record(
        self,
        *,
        kind: str,
        units: int,
        cost_micros: int,
        owner_id: int,
        run_id: int | None,
        workflow_id: int | None,
        account_id: int | None,
        meta: dict[str, Any],
    ) -> None:
        async with self._session_factory() as session, session.begin():
            await usage_repo.record_usage(
                session,
                owner_id=owner_id,
                kind=kind,
                units=units,
                cost_micros=cost_micros,
                run_id=run_id,
                workflow_id=workflow_id,
                account_id=account_id,
                meta=meta,
            )


def _elapsed_ms(start: float) -> int:
    return int(round((time.perf_counter() - start) * 1000))


# ---------------------------------------------------------------- real invokers
#
# Lazy-import the ``openai`` SDK so tests that inject stubs don't pay the
# import cost — and so the unit-test suite still passes if ``openai`` is
# uninstalled (rare but worth keeping cheap).


def _build_real_chat_invoker(settings: Settings) -> ChatInvoker:
    async def _invoke(
        *, messages: list[ChatMessage], model: str, max_tokens: int | None
    ) -> RawChatResponse:
        client = _build_async_openai(settings)
        sdk_messages = [{"role": m.role, "content": m.content} for m in messages]
        resp = await client.chat.completions.create(
            model=model,
            messages=sdk_messages,
            max_tokens=max_tokens,
        )
        choice = resp.choices[0]
        usage = resp.usage
        return RawChatResponse(
            text=choice.message.content or "",
            prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
            completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
            total_tokens=getattr(usage, "total_tokens", 0) or 0,
            model=resp.model,
        )

    return _invoke


def _build_real_vision_invoker(settings: Settings) -> VisionInvoker:
    async def _invoke(
        *, images: list[bytes], prompt: str, model: str
    ) -> RawVisionResponse:
        import base64

        client = _build_async_openai(settings)
        content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
        for img in images:
            b64 = base64.b64encode(img).decode("ascii")
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{b64}"},
                }
            )
        resp = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": content}],
        )
        choice = resp.choices[0]
        usage = resp.usage
        return RawVisionResponse(
            text=choice.message.content or "",
            prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
            completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
            total_tokens=getattr(usage, "total_tokens", 0) or 0,
            model=resp.model,
        )

    return _invoke


def _build_async_openai(settings: Settings) -> Any:
    from openai import AsyncOpenAI

    assert settings.openai_api_key is not None  # _require_api_key already checked

    # OPENAI_PROXY (if set) forces a dedicated httpx client through that
    # proxy — independent of TG_PROXY (Telegram-only) and of the process'
    # ALL_PROXY / HTTPS_PROXY env. When unset we pass no http_client, so the
    # SDK's default client keeps honoring system env proxies as before.
    http_client: Any = None
    if settings.openai_proxy is not None:
        import httpx

        http_client = httpx.AsyncClient(proxy=settings.openai_proxy)

    return AsyncOpenAI(
        api_key=settings.openai_api_key.get_secret_value(),
        base_url=settings.openai_base_url,
        http_client=http_client,
    )
