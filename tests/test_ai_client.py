"""§13.3 / §13.5 / §13.9 — OpenAIClient writes ``usage_events`` on every call.

Covers:

* Successful chat → ``ChatResult`` returned; row written with
  ``kind="openai.chat"``, integer ``units`` and ``cost_micros``.
* Failed chat → row written with ``kind="openai.chat.failed"`` and
  ``meta.error``; exception re-raised (spec ai-usage §"失败调用也写").
* Same for vision.
* ``OPENAI_API_KEY`` missing → fail-fast at call time.
* Unknown model still records a usage row (cost_micros=0).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.actions.context import ChatMessage
from tg_conductor.ai.client import (
    OpenAIClient,
    RawChatResponse,
    RawVisionResponse,
    _build_async_openai,
)
from tg_conductor.ai.pricing import PricingTable
from tg_conductor.ai.usage import UsageRow
from tg_conductor.config.settings import get_settings
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


def _pricing() -> PricingTable:
    return PricingTable.load_default(Path("pricing.yaml.example"))


def _client_with_key(
    factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    *,
    chat_invoker=None,
    vision_invoker=None,
) -> OpenAIClient:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    return OpenAIClient(
        session_factory=factory,
        pricing=_pricing(),
        settings=get_settings(),
        chat_invoker=chat_invoker,
        vision_invoker=vision_invoker,
    )


# ---------------------------------------------------------------- chat success


@pytest.mark.asyncio
async def test_chat_success_records_usage_row(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_chat(*, messages, model, max_tokens):  # type: ignore[no-untyped-def]
        return RawChatResponse(
            text="hello back",
            prompt_tokens=1000,
            completion_tokens=500,
            total_tokens=1500,
            model="gpt-4o-mini",
        )

    client = _client_with_key(session_factory, monkeypatch, chat_invoker=fake_chat)
    result = await client.chat(
        [ChatMessage(role="user", content="hi")],
        model="gpt-4o-mini",
        owner_id=1,
        run_id=42,
        workflow_id=7,
        account_id=3,
    )

    assert result.text == "hello back"
    assert result.total_tokens == 1500
    assert result.latency_ms >= 0

    async with session_factory() as s:
        [row] = (await s.execute(select(UsageRow))).scalars().all()
    assert row.kind == "openai.chat"
    assert row.units == 1500
    # ceil(1000*1.05) + ceil(500*4.20) per pricing.yaml.example.
    assert row.cost_micros == 3150
    assert row.run_id == 42
    assert row.workflow_id == 7
    assert row.account_id == 3
    assert row.meta is not None
    assert row.meta["model"] == "gpt-4o-mini"
    assert row.meta["prompt_tokens"] == 1000
    assert row.meta["completion_tokens"] == 500
    assert row.meta["latency_ms"] >= 0


# ---------------------------------------------------------------- chat failure


@pytest.mark.asyncio
async def test_chat_failure_records_failed_row_and_reraises(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ProviderDown(RuntimeError):
        pass

    async def fake_chat(*, messages, model, max_tokens):  # type: ignore[no-untyped-def]
        raise _ProviderDown("network down")

    client = _client_with_key(session_factory, monkeypatch, chat_invoker=fake_chat)

    with pytest.raises(_ProviderDown, match="network down"):
        await client.chat(
            [ChatMessage(role="user", content="x")],
            model="gpt-4o-mini",
            owner_id=1,
            run_id=99,
        )

    async with session_factory() as s:
        [row] = (await s.execute(select(UsageRow))).scalars().all()
    assert row.kind == "openai.chat.failed"
    assert row.units == 0
    assert row.cost_micros == 0
    assert row.run_id == 99
    assert row.meta is not None
    assert "network down" in row.meta["error"]
    assert "_ProviderDown" in row.meta["error"]


# ---------------------------------------------------------------- vision


@pytest.mark.asyncio
async def test_vision_success_records_usage_row(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_vision(*, images, prompt, model):  # type: ignore[no-untyped-def]
        return RawVisionResponse(
            text="cat",
            prompt_tokens=200,
            completion_tokens=10,
            total_tokens=210,
            model="gpt-4o",
        )

    client = _client_with_key(session_factory, monkeypatch, vision_invoker=fake_vision)
    result = await client.vision(
        [b"\x89PNG"],
        prompt="what is this?",
        model="gpt-4o",
        owner_id=1,
    )

    assert result.text == "cat"

    async with session_factory() as s:
        [row] = (await s.execute(select(UsageRow))).scalars().all()
    assert row.kind == "openai.vision"
    assert row.units == 210
    # gpt-4o: 35 input / 105 output per 1M; 200*35 + 10*105 = 7000 + 1050.
    assert row.cost_micros == 8050
    assert row.meta is not None
    assert row.meta["image_count"] == 1


@pytest.mark.asyncio
async def test_vision_failure_records_failed_row(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_vision(*, images, prompt, model):  # type: ignore[no-untyped-def]
        raise ValueError("bad image")

    client = _client_with_key(session_factory, monkeypatch, vision_invoker=fake_vision)
    with pytest.raises(ValueError):
        await client.vision([b""], prompt="?", owner_id=1)

    async with session_factory() as s:
        [row] = (await s.execute(select(UsageRow))).scalars().all()
    assert row.kind == "openai.vision.failed"
    assert "bad image" in (row.meta or {}).get("error", "")


# ---------------------------------------------------------------- api key


@pytest.mark.asyncio
async def test_missing_api_key_fails_fast_no_call_no_row(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """spec ai-usage §"缺凭据 fail-fast"."""
    # ``setenv("", "")`` collapses to None via the empty-string validator,
    # which beats any ``OPENAI_API_KEY`` that might be left over in a
    # local ``.env`` (env vars override .env in pydantic-settings).
    monkeypatch.setenv("OPENAI_API_KEY", "")

    calls: list[str] = []

    async def should_not_run(*, messages, model, max_tokens):  # type: ignore[no-untyped-def]
        calls.append("chat")
        raise AssertionError("invoker must not be called when key is missing")

    client = OpenAIClient(
        session_factory=session_factory,
        pricing=_pricing(),
        settings=get_settings(),
        chat_invoker=should_not_run,
    )
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY missing"):
        await client.chat(
            [ChatMessage(role="user", content="x")],
            owner_id=1,
        )

    assert calls == []
    async with session_factory() as s:
        rows = (await s.execute(select(UsageRow))).scalars().all()
    assert rows == []  # nothing recorded — pre-call failure


# ---------------------------------------------------------------- OPENAI_PROXY


def test_openai_proxy_builds_proxied_http_client(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OPENAI_PROXY set → AsyncOpenAI gets an httpx client bound to that proxy."""
    import httpx
    import openai

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    monkeypatch.setenv("OPENAI_PROXY", "socks5://127.0.0.1:7890")

    seen_proxy: list[object] = []
    seen_http_client: list[object] = []

    class _FakeAsyncClient:
        def __init__(self, *, proxy=None, **_kw):  # type: ignore[no-untyped-def]
            seen_proxy.append(proxy)

    def _fake_openai(*, api_key, base_url, http_client):  # type: ignore[no-untyped-def]
        seen_http_client.append(http_client)
        return object()

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)
    monkeypatch.setattr(openai, "AsyncOpenAI", _fake_openai)

    _build_async_openai(get_settings())
    assert seen_proxy == ["socks5://127.0.0.1:7890"]
    assert isinstance(seen_http_client[0], _FakeAsyncClient)


def test_no_openai_proxy_leaves_http_client_none(
    master_key: str,  # noqa: ARG001
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OPENAI_PROXY unset → http_client=None so the SDK keeps env-proxy behavior."""
    import openai

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    monkeypatch.setenv("OPENAI_PROXY", "")  # empty → None via validator

    seen_http_client: list[object] = []

    def _fake_openai(*, api_key, base_url, http_client):  # type: ignore[no-untyped-def]
        seen_http_client.append(http_client)
        return object()

    monkeypatch.setattr(openai, "AsyncOpenAI", _fake_openai)
    _build_async_openai(get_settings())
    assert seen_http_client == [None]


# ---------------------------------------------------------------- unknown model


@pytest.mark.asyncio
async def test_unknown_model_still_records_row_with_zero_cost(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_chat(*, messages, model, max_tokens):  # type: ignore[no-untyped-def]
        return RawChatResponse(
            text="ok",
            prompt_tokens=10,
            completion_tokens=20,
            total_tokens=30,
            model="brand-new-model",
        )

    client = _client_with_key(session_factory, monkeypatch, chat_invoker=fake_chat)
    await client.chat(
        [ChatMessage(role="user", content="x")],
        model="brand-new-model",
        owner_id=1,
    )

    async with session_factory() as s:
        [row] = (await s.execute(select(UsageRow))).scalars().all()
    assert row.kind == "openai.chat"
    assert row.units == 30  # still recorded
    assert row.cost_micros == 0  # unknown model → 0
