"""Daily plan-expansion background task.

spec scheduler §"每日 plan 展开": the service SHALL, at a fixed wall-clock
time each day (default 00:05, configurable), expand every enabled
``time_window`` Workflow's window for that day into ``pending`` Jobs.

Startup already runs a one-off catch-up expansion for the *current* day
(see ``api.lifespan`` step 6). This task covers every *subsequent* day:
it sleeps until the next configured wall-clock instant (in
``scheduler_tz``), then calls :func:`expand_daily` for that date and loops.
Because :func:`expand_daily` is idempotent on ``(workflow_id, date)``, an
early/late wake or an overlap with the startup catch-up creates no
duplicate Jobs.

Without this loop, time_window Workflows only ever get expanded for the
day the process started — every later day stays silent until restart.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import date, datetime, time, timedelta, tzinfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.scheduler.expander import ExpansionReport, expand_daily

log = logging.getLogger(__name__)


def _next_run_at(now: datetime, expand_at: time, tz: tzinfo) -> datetime:
    """First wall-clock ``expand_at`` instant strictly after ``now``."""
    candidate = datetime.combine(now.date(), expand_at, tzinfo=tz)
    if candidate <= now:
        candidate = datetime.combine(
            now.date() + timedelta(days=1), expand_at, tzinfo=tz
        )
    return candidate


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
        shutdown_grace_seconds: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._owner_id = owner_id
        self._expand_at = expand_at
        self._tz = tz
        self._grace = shutdown_grace_seconds
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

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
            now = datetime.now(self._tz)
            next_run = _next_run_at(now, self._expand_at, self._tz)
            sleep_seconds = (next_run - now).total_seconds()
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=sleep_seconds)
            except asyncio.TimeoutError:
                # Woke at (or just after) next_run — expand that day.
                try:
                    await self.expand_for(next_run.date())
                except Exception:  # noqa: BLE001 - keep the loop alive
                    log.exception("daily_expander.tick_failed date=%s", next_run.date())
            else:
                return  # shutdown signaled
