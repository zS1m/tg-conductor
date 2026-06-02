"""Daily plan-expansion background task.

spec scheduler §"每日 plan 展开": the service SHALL, at a fixed wall-clock
time each day (default 00:05, configurable), expand every enabled
``time_window`` Workflow's window for that day into ``pending`` Jobs.

Startup already runs a one-off catch-up expansion for the *current* day
(see ``api.lifespan`` step 6). This task covers every *subsequent* day.

Why polling instead of one long sleep
-------------------------------------
The first version slept on a single multi-hour ``asyncio.wait_for`` timer
(``next_run - now``, often >6000s) and expanded once it fired. In
production that timer silently never woke: the dispatcher's *short*
repeated 10s tick kept running fine on the same event loop, but the
expander's one long sleep left every day after startup silent until a
manual restart — the exact bug this task exists to prevent.

So we mirror the dispatcher's proven shape: wake every ``poll_seconds``
and, whenever the wall clock has reached ``expand_at`` for a day we have
not expanded yet, expand it. ``expand_daily`` is idempotent on
``(workflow_id, date)``, so a missed / late / duplicate tick self-heals
instead of going silent, and overlap with the startup catch-up creates no
duplicate Jobs.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import date, datetime, time, tzinfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.scheduler.expander import ExpansionReport, expand_daily

log = logging.getLogger(__name__)


class DailyExpander:
    """Background task that expands time_window Workflows once per day.

    Use::

        expander = DailyExpander(
            session_factory=factory,
            owner_id=1,
            expand_at=time(0, 5),
            tz=ZoneInfo("Asia/Shanghai"),
        )
        await expander.start()
        ...
        await expander.stop()
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        owner_id: int,
        expand_at: time,
        tz: tzinfo,
        poll_seconds: float = 60.0,
        shutdown_grace_seconds: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._owner_id = owner_id
        self._expand_at = expand_at
        self._tz = tz
        self._poll_seconds = poll_seconds
        self._grace = shutdown_grace_seconds
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        # Last calendar day (in ``tz``) we successfully expanded. Guards
        # against re-expanding the same day on every poll tick.
        self._last_expanded: date | None = None

    def _now(self) -> datetime:
        """Current wall-clock time in ``tz``. Seam for tests to drive the loop."""
        return datetime.now(self._tz)

    async def start(self) -> None:
        if self._task is not None:
            return
        self._shutdown.clear()
        self._task = asyncio.create_task(self._loop(), name="daily-expander")

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

    async def expand_for(self, target_date: date) -> ExpansionReport:
        """Expand all time_window Workflows for ``target_date`` (one txn).

        Public so ops / tests can force an expansion without the loop.
        Idempotent: a date already expanded yields an empty-created report.
        """
        async with self._session_factory() as session, session.begin():
            report = await expand_daily(
                session,
                owner_id=self._owner_id,
                target_date=target_date,
                tz=self._tz,
                rng=random.Random(),
            )
        if report.created_job_ids:
            log.info(
                "daily_expander.expanded date=%s created=%d",
                target_date,
                len(report.created_job_ids),
            )
        return report

    async def _loop(self) -> None:
        while not self._shutdown.is_set():
            now = self._now()
            today = now.date()
            # Once the wall clock passes ``expand_at`` for a day we have not
            # expanded yet, expand it. Idempotent, so this also re-confirms
            # the startup catch-up day harmlessly on the first tick.
            if now.time() >= self._expand_at and self._last_expanded != today:
                try:
                    await self.expand_for(today)
                except Exception:  # noqa: BLE001 - keep the loop alive
                    log.exception("daily_expander.tick_failed date=%s", today)
                else:
                    # Only mark done on success — a crashed expand retries next
                    # tick instead of skipping the whole day.
                    self._last_expanded = today
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self._poll_seconds)
            except asyncio.TimeoutError:
                continue  # next poll
            else:
                return  # shutdown signaled
