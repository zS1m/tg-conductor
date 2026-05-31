"""Backend-agnostic event bus + in-memory default for development & tests.

The bus is **fan-out by topic**: one publish lands in every subscriber's
queue for that topic. Topics for Run events are ``"run:<id>"`` so SSE
consumers subscribe by Run id.

The in-memory backend's ``subscribe`` is an async generator yielding
messages until cancelled; cleanup (queue de-registration) happens in its
``finally`` block. Publishing is synchronous and non-blocking — a slow
subscriber's queue is bounded only by ``maxsize``; when full, we drop
oldest-first and log (configurable: §12 may move to bounded-blocking).
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any, Protocol

log = logging.getLogger(__name__)


class EventBus(Protocol):
    def publish(self, topic: str, message: dict[str, Any]) -> None: ...

    def subscribe(self, topic: str) -> AsyncIterator[dict[str, Any]]: ...


class InMemoryEventBus:
    def __init__(self, *, per_subscriber_queue_size: int = 1024) -> None:
        self._subscribers: dict[str, list[asyncio.Queue[dict[str, Any]]]] = defaultdict(
            list
        )
        self._queue_size = per_subscriber_queue_size

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        # Snapshot the list — subscribers may unsubscribe concurrently.
        for queue in list(self._subscribers.get(topic, [])):
            if queue.full():
                # Drop oldest to make room; logging at INFO to keep noise low.
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                log.info("event_bus.drop_oldest topic=%s", topic)
            queue.put_nowait(message)

    async def subscribe(self, topic: str) -> AsyncIterator[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(self._queue_size)
        self._subscribers[topic].append(queue)
        try:
            while True:
                msg = await queue.get()
                yield msg
        finally:
            try:
                self._subscribers[topic].remove(queue)
            except ValueError:
                pass

    def subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, []))


def run_topic(run_id: int) -> str:
    return f"run:{run_id}"
