"""Per-account async throttle: serial calls + min-interval + FloodWait retry.

One ``AccountThrottle`` instance guards one account. Multiple instances are
independent (their own locks and timing state), so different accounts run in
parallel automatically.

Lock and timing state live on the instance (rather than module-level
globals) so the throttle is straightforward to unit test. It only knows
about ``TGFloodWait`` — the adapter is responsible for translating
Kurigram's exception into ours.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from tg_conductor.tg_core.exceptions import TGFloodWait

log = logging.getLogger(__name__)
T = TypeVar("T")


class AccountThrottle:
    def __init__(
        self,
        *,
        min_interval_seconds: float = 1.0,
        max_floodwait_retries: int = 3,
        floodwait_padding_seconds: float = 1.5,
    ) -> None:
        self.min_interval_seconds = min_interval_seconds
        self.max_floodwait_retries = max_floodwait_retries
        self.floodwait_padding_seconds = floodwait_padding_seconds
        self._lock = asyncio.Lock()
        self._last_called_at: float | None = None

    async def call(
        self,
        operation: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        """Run ``fn`` under the account's serial lock with rate + FloodWait policy."""
        retries_left = self.max_floodwait_retries
        while True:
            async with self._lock:
                loop = asyncio.get_running_loop()
                if self._last_called_at is not None:
                    elapsed = loop.time() - self._last_called_at
                    wait = self.min_interval_seconds - elapsed
                    if wait > 0:
                        await asyncio.sleep(wait)
                try:
                    result = await fn()
                    self._last_called_at = loop.time()
                    return result
                except TGFloodWait as exc:
                    self._last_called_at = loop.time()
                    if retries_left <= 0:
                        raise
                    retries_left -= 1
                    wait_seconds = (
                        max(exc.seconds, 0.0) + self.floodwait_padding_seconds
                    )
                    log.warning(
                        "tg.floodwait operation=%s wait_s=%.2f retries_left=%d",
                        operation,
                        wait_seconds,
                        retries_left,
                    )
                    await asyncio.sleep(wait_seconds)
