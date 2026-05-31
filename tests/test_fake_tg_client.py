"""Smoke tests for ``FakeTGClient`` so it doesn't rot before §6 / §11 use it."""

from __future__ import annotations

import asyncio

import pytest

from tg_conductor.tg_core.exceptions import TGFloodWait
from tg_conductor.tg_core.fake import FakeTGClient, make_fake_message
from tg_conductor.tg_core.protocol import Dialog, ForumTopic


@pytest.mark.asyncio
async def test_connect_send_close_records_calls() -> None:
    client = FakeTGClient(label="acc1")
    assert not client.is_connected()
    await client.connect()
    assert client.is_connected()
    msg = await client.send_text(123, "hi")
    await client.close()

    methods = [c.method for c in client.calls]
    assert methods == ["connect", "send_text", "close"]
    assert msg.chat_id == 123 and msg.text == "hi"


@pytest.mark.asyncio
async def test_send_text_can_be_made_to_raise() -> None:
    client = FakeTGClient()
    client.next_send_text_raises = TGFloodWait(seconds=2.0, operation="send_text")
    with pytest.raises(TGFloodWait):
        await client.send_text(1, "boom")
    # one-shot: next call succeeds again
    msg = await client.send_text(1, "ok")
    assert msg.text == "ok"


@pytest.mark.asyncio
async def test_wait_for_is_unblocked_by_inject_message() -> None:
    client = FakeTGClient()

    async def feeder() -> None:
        await asyncio.sleep(0)  # let wait_for register its waiter first
        client.inject_message(make_fake_message(42, text="pong"))

    asyncio.create_task(feeder())
    msg = await client.wait_for(
        42, predicate=lambda m: (m.text or "").startswith("pong"), timeout=1.0
    )
    assert msg.text == "pong"


@pytest.mark.asyncio
async def test_on_message_handlers_receive_injection() -> None:
    client = FakeTGClient()
    received: list[str] = []

    async def handler(msg) -> None:  # type: ignore[no-untyped-def]
        if msg.text is not None:
            received.append(msg.text)

    client.on_message(handler)
    tasks = client.inject_message(make_fake_message(1, text="hello"))
    await asyncio.gather(*tasks)
    assert received == ["hello"]


@pytest.mark.asyncio
async def test_get_dialogs_and_list_topics_are_async_iterators() -> None:
    client = FakeTGClient()
    client.dialogs = [Dialog(chat_id=1, title="a"), Dialog(chat_id=2, title="b")]
    client.topics = {1: [ForumTopic(topic_id=10, title="t10")]}
    seen = [d.title async for d in client.get_dialogs()]
    topic_titles = [t.title async for t in client.list_topics(1)]
    assert seen == ["a", "b"]
    assert topic_titles == ["t10"]
