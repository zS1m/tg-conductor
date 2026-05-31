"""``run_events`` TTL pruning — pure DB op + a background tick loop.

spec runs §"事件保留与清理": old ``run_events`` are deleted once their
``ts`` is older than ``ttl_days`` days; the parent ``runs`` row is kept
intact (separate retention policy lives there, see §12.8). When
``ttl_days <= 0`` the purge is disabled — events live forever.

§16 lifespan wires :class:`EventsTtlCleaner` and calls :meth:`start`;
``stop`` is cooperative and bounded.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.runs.models import RunEventRow

log = logging.getLogger(__name__)


def _disabled(ttl_days: int) -> bool:
    """spec §12.5: ``0`` or ``-1`` (or anything <= 0) means "keep forever"."""
    return ttl_days <= 0


async def purge_expired_events(
    session: AsyncSession,
    *,
    ttl_days: int,
    now: datetime | None = None,
) -> int:
    """Delete ``run_events`` whose ``ts`` is older than ``now - ttl_days``.

    Returns the number of rows deleted. Caller owns the session and the
    surrounding transaction. Disabled (``ttl_days <= 0``) returns 0
    without touching the DB.
    """
    if _disabled(ttl_days):
        return 0
    cutoff = (now or datetime.now(UTC)) - timedelta(days=ttl_days)
    stmt = delete(RunEventRow).where(RunEventRow.ts < cutoff)
    result = await session.execute(stmt)
    return result.rowcount or 0


class EventsTtlCleaner:
    """Periodic background task that calls :func:`purge_expired_events`.

    Use:
        cleaner = EventsTtlCleaner(
            session_factory=factory,
            ttl_days=30,
            interval_seconds=3600,
        )
        await cleaner.start()
        ...
        await cleaner.stop()
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        ttl_days: int,
        interval_seconds: float,
        shutdown_grace_seconds: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._ttl_days = ttl_days
        self._interval = interval_seconds
        self._grace = shutdown_grace_seconds
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def disabled(self) -> bool:
        return _disabled(self._ttl_days)

    async def start(self) -> None:
        if self._task is not None:
            return
        if self.disabled:
            log.info(
                "events_ttl_cleaner.disabled ttl_days=%s",
                self._ttl_days,
            )
            return
        self._shutdown.clear()
        self._task = asyncio.create_task(self._loop(), name="run-events-ttl-cleaner")

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._task, timeout=self._grace)
        except asyncio.TimeoutError:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None

    async def tick_once(self) -> int:
        """Run one purge cycle and return the deleted row count.

        Public so §16 / ops can force a purge without waiting on the loop.
        """
        if self.disabled:
            return 0
        async with self._session_factory() as session, session.begin():
            n = await purge_expired_events(session, ttl_days=self._ttl_days)
        if n:
            log.info("run_events.purged count=%s ttl_days=%s", n, self._ttl_days)
        return n

    async def _loop(self) -> None:
        # Tick once on startup so freshly-booted services with stale data
        # get cleaned right away, then settle into the interval cadence.
        while not self._shutdown.is_set():
            try:
                await self.tick_once()
            except Exception:  # noqa: BLE001 - keep the loop alive
                log.exception("run_events.cleanup_tick_failed")
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                continue
