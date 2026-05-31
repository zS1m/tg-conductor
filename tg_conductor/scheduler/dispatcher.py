"""Per-owner dispatch loop: tick → cron_tick → claim → enqueue.

The Dispatcher owns one ``asyncio.Queue[JobRow]`` per account; §11's
``AccountWorker`` (or, for tests, any other consumer) drains them. Two
entry points push into the queues:

* :meth:`tick_once` — periodic batch path. Runs ``cron_tick`` then
  ``claim_due_jobs`` in a single transaction; every claimed Job goes into
  its account's queue after commit.
* :meth:`dispatch_immediate` — sub-tick latency path used by
  :class:`MessageRouter` and the v2 HTTP "trigger now" endpoint. Inserts a
  row directly in ``running`` state (skipping the pending/claim ping-pong)
  and enqueues it.

Background loop semantics: :meth:`start` schedules :meth:`_loop`; cancel
via :meth:`stop`. Loop exceptions are logged and swallowed so a single
broken tick can't kill the dispatcher.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, tzinfo

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.scheduler import cron_tick
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.models import JobRow, JobStatus, _utcnow
from tg_conductor.tg_core.protocol import Message, message_to_payload
from tg_conductor.workflows import repo as workflow_repo

log = logging.getLogger(__name__)


@dataclass
class DispatchReport:
    cron_created_ids: list[int] = field(default_factory=list)
    claimed_ids: list[int] = field(default_factory=list)
    skipped_ids: list[int] = field(default_factory=list)


class Dispatcher:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        owner_id: int,
        tick_seconds: float = 10.0,
        batch_size: int = 50,
        compensation_window_seconds: float = 300.0,
        cron_tz: tzinfo = UTC,
    ) -> None:
        self._session_factory = session_factory
        self._owner_id = owner_id
        self._tick_seconds = tick_seconds
        self._batch_size = batch_size
        self._comp_window = compensation_window_seconds
        self._cron_tz = cron_tz
        self._queues: dict[int, asyncio.Queue[JobRow]] = {}
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------ queue access

    def queue_for(self, account_id: int) -> asyncio.Queue[JobRow]:
        """Get or create the queue for an account."""
        queue = self._queues.get(account_id)
        if queue is None:
            queue = asyncio.Queue()
            self._queues[account_id] = queue
        return queue

    def active_account_ids(self) -> list[int]:
        return list(self._queues.keys())

    # ------------------------------------------------------------ batched path

    async def tick_once(self) -> DispatchReport:
        """Single dispatch cycle. Safe to call concurrently with the loop."""
        async with self._session_factory() as session, session.begin():
            cron_report = await cron_tick.tick(
                session,
                owner_id=self._owner_id,
                now=datetime.now(UTC),
                tz=self._cron_tz,
            )
            claim_result = await job_repo.claim_due_jobs(
                session,
                owner_id=self._owner_id,
                batch_size=self._batch_size,
                compensation_window_seconds=self._comp_window,
            )
        # Enqueue after commit. Row attributes stay accessible because
        # session_factory uses ``expire_on_commit=False``.
        for job in claim_result.claimed:
            self.queue_for(job.account_id).put_nowait(job)
        return DispatchReport(
            cron_created_ids=list(cron_report.created_job_ids),
            claimed_ids=[j.id for j in claim_result.claimed],  # type: ignore[misc]
            skipped_ids=[j.id for j in claim_result.skipped],  # type: ignore[misc]
        )

    # ------------------------------------------------------------ immediate path

    async def dispatch_immediate(
        self,
        *,
        workflow_id: int,
        account_id: int,
        trigger_message: Message | None = None,
    ) -> int | None:
        """Create + claim + enqueue in one shot. Returns the new Job id.

        ``None`` means the workflow is missing / belongs to another owner /
        is disabled. The caller should map this to 404 (HTTP) or a no-op
        warning (message router).

        ``trigger_message`` is the inbound message that fired a
        ``message_match`` workflow. We persist its typed fields into
        ``resolved_payload`` so the AccountWorker can seed
        ``ActionContext.last_matched_message`` before the plan runs — that
        is what lets ``ai_reply`` / ``forward`` consume the triggering
        message directly, without a redundant ``wait_for`` step.
        """
        resolved_payload = (
            {"trigger_message": message_to_payload(trigger_message)}
            if trigger_message is not None
            else None
        )
        async with self._session_factory() as session, session.begin():
            wf = await workflow_repo.get_by_id(
                session, workflow_id=workflow_id, owner_id=self._owner_id
            )
            if wf is None or not wf.enabled:
                return None
            now = _utcnow()
            row = JobRow(
                owner_id=self._owner_id,
                workflow_id=workflow_id,
                account_id=account_id,
                fire_at=now,
                status=JobStatus.running,
                started_at=now,
                resolved_payload=resolved_payload,
            )
            session.add(row)
            await session.flush()
        # row.id stays accessible after commit (expire_on_commit=False).
        self.queue_for(account_id).put_nowait(row)
        return row.id

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        if self._task is not None:
            return
        self._shutdown.clear()
        self._task = asyncio.create_task(
            self._loop(), name=f"dispatcher-{self._owner_id}"
        )

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task is None:
            return
        try:
            await self._task
        except Exception:  # noqa: BLE001 - already logged inside the loop
            pass
        self._task = None

    async def _loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self._tick_seconds
                )
                # Shutdown event woke us up early — exit loop.
                return
            except TimeoutError:
                pass
            try:
                await self.tick_once()
            except Exception:  # noqa: BLE001 - keep loop alive across failures
                log.exception("dispatcher.tick_failed owner_id=%d", self._owner_id)
