"""§11.4 — InMemoryEventBus fan-out + subscribe AsyncIterator semantics."""

from __future__ import annotations

import asyncio

import pytest

from tg_conductor.runs.event_bus import InMemoryEventBus, run_topic


@pytest.mark.asyncio
async def test_publish_then_subscribe_does_not_replay_past() -> None:
    """Bus has no replay; the SSE history catch-up is done via DB."""
    bus = InMemoryEventBus()
    bus.publish("a", {"v": 1})

    received: list[dict] = []
    done = asyncio.Event()

    async def consumer() -> None:
        async for msg in bus.subscribe("a"):
            received.append(msg)
            if msg.get("v") == 2:
                done.set()
                return

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)  # let consumer subscribe
    bus.publish("a", {"v": 2})
    await asyncio.wait_for(done.wait(), timeout=1.0)
    task.cancel()

    # v=1 was published before subscribe → not delivered.
    assert received == [{"v": 2}]


@pytest.mark.asyncio
async def test_multiple_subscribers_each_get_all_published() -> None:
    bus = InMemoryEventBus()
    a_msgs: list[dict] = []
    b_msgs: list[dict] = []

    async def consumer(out: list[dict]) -> None:
        async for msg in bus.subscribe("type"):
            out.append(msg)
            if len(out) >= 3:
                return

    ta = asyncio.create_task(consumer(a_msgs))
    tb = asyncio.create_task(consumer(b_msgs))
    await asyncio.sleep(0)

    for i in range(3):
        bus.publish("type", {"i": i})

    await asyncio.gather(ta, tb)
    assert a_msgs == [{"i": 0}, {"i": 1}, {"i": 2}]
    assert b_msgs == [{"i": 0}, {"i": 1}, {"i": 2}]


@pytest.mark.asyncio
async def test_unsubscribe_via_cancellation_cleans_up() -> None:
    bus = InMemoryEventBus()

    async def consumer() -> None:
        async for _ in bus.subscribe("type"):
            return  # never actually reached because we cancel first

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)
    assert bus.subscriber_count("type") == 1
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # Subscriber list pruned on generator finally.
    assert bus.subscriber_count("type") == 0


@pytest.mark.asyncio
async def test_publish_to_topic_without_subscribers_is_noop() -> None:
    bus = InMemoryEventBus()
    bus.publish("lonely", {"ok": True})
    assert bus.subscriber_count("lonely") == 0


@pytest.mark.asyncio
async def test_run_topic_returns_canonical_key() -> None:
    assert run_topic(42) == "run:42"


@pytest.mark.asyncio
async def test_full_queue_drops_oldest_keeps_newest() -> None:
    bus = InMemoryEventBus(per_subscriber_queue_size=2)
    # Subscribe but don't consume yet — fill the queue.
    sub_started = asyncio.Event()
    received: list[dict] = []

    async def slow_consumer() -> None:
        async for msg in bus.subscribe("t"):
            sub_started.set()
            await asyncio.sleep(0.01)
            received.append(msg)
            if len(received) == 2:
                return

    task = asyncio.create_task(slow_consumer())
    await asyncio.sleep(0)
    # Publish 4 — first two fill the queue, then we drop-oldest for the
    # remaining two. Consumer eventually drains the surviving 2.
    for i in range(4):
        bus.publish("t", {"i": i})
    await asyncio.wait_for(task, timeout=1.0)
    # Surviving messages are the newest two.
    assert {m["i"] for m in received} <= {2, 3}
