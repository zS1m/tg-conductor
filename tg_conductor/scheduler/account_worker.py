"""Per-account consumer that drives a Run for each Job pulled off the queue.

The dispatcher (§10) puts ``running``-status JobRow into the per-account
``asyncio.Queue``. The worker:

1. Opens a Run row (status=running).
2. Builds an :class:`ActionContext` carrying the bus, writer, TG client,
   AI client.
3. Emits ``run.started``.
4. Wraps :meth:`ActionPlanExecutor.execute` in
   ``asyncio.wait_for(timeout=plan.job_timeout or settings.job_default_timeout_seconds)``.
5. Translates the result to the terminal Run / Job status + ``run.finished``
   event.

Exception safety: any exception within the per-job handler is captured into
the Run / Job state — the loop keeps consuming the next Job. Worker shutdown
is cooperative: :meth:`stop` sets an asyncio.Event and cancels the consume
task; in-flight Jobs are allowed to finish (best-effort, bounded by
``shutdown_grace_seconds``).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.actions.context import ActionContext, AIClient
from tg_conductor.actions.executor import ActionPlanExecutor, ExecutionResult
from tg_conductor.config.settings import get_settings
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import EventBus
from tg_conductor.runs.event_writer import EventWriter
from tg_conductor.runs.models import RunStatus
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.models import JobRow
from tg_conductor.tg_core.protocol import TGClient, message_from_payload
from tg_conductor.workflows import repo as workflow_repo

log = logging.getLogger(__name__)


class AccountWorker:
    def __init__(
        self,
        *,
        owner_id: int,
        account_id: int,
        queue: asyncio.Queue[JobRow],
        session_factory: async_sessionmaker[AsyncSession],
        tg_client: TGClient,
        ai_client: AIClient,
        event_bus: EventBus,
        shutdown_grace_seconds: float = 30.0,
    ) -> None:
        self._owner_id = owner_id
        self._account_id = account_id
        self._queue = queue
        self._session_factory = session_factory
        self._tg_client = tg_client
        self._ai_client = ai_client
        self._event_writer = EventWriter(
            session_factory=session_factory, event_bus=event_bus
        )
        self._shutdown = asyncio.Event()
        self._grace = shutdown_grace_seconds
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._shutdown.clear()
        self._task = asyncio.create_task(
            self._consume(),
            name=f"account-worker-{self._account_id}",
        )

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task is None:
            return
        # Wait for the consume loop to notice. We don't cancel the task —
        # we let it finish whatever Job is in flight (executor wraps with
        # job_timeout, so this can't hang forever).
        try:
            await asyncio.wait_for(self._task, timeout=self._grace)
        except asyncio.TimeoutError:
            log.warning(
                "account_worker.shutdown_grace_exceeded account_id=%s",
                self._account_id,
            )
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None

    async def _consume(self) -> None:
        while not self._shutdown.is_set():
            try:
                # Wake periodically to honor shutdown even if the queue is idle.
                job = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                await self.handle_job(job)
            except Exception:  # noqa: BLE001 - record + keep consuming
                log.exception(
                    "account_worker.unhandled job_id=%s account_id=%s",
                    job.id,
                    self._account_id,
                )

    async def handle_job(self, job: JobRow) -> ExecutionResult:
        """Run one Job end-to-end. Public for unit tests; loop calls it too."""
        assert job.id is not None

        # Load the workflow + plan.
        async with self._session_factory() as session:
            workflow = await workflow_repo.get_by_id(
                session, workflow_id=job.workflow_id, owner_id=self._owner_id
            )
        if workflow is None:
            await self._mark_terminal(
                job_id=job.id,
                run_id=None,
                run_status=None,
                error="workflow_not_found",
            )
            return ExecutionResult(success=False, error="workflow_not_found")

        # Open the Run row.
        started_at = datetime.now(UTC)
        async with self._session_factory() as session, session.begin():
            run = await run_repo.create_run(
                session,
                owner_id=self._owner_id,
                workflow_id=job.workflow_id,
                account_id=self._account_id,
                job_id=job.id,
                started_at=started_at,
            )
            run_id = run.id
        assert run_id is not None

        ctx = ActionContext(
            owner_id=self._owner_id,
            account_id=self._account_id,
            workflow_id=job.workflow_id,
            run_id=run_id,
            job_id=job.id,
            tg_client=self._tg_client,
            ai_client=self._ai_client,
            event_writer=self._event_writer,
            session_factory=self._session_factory,
        )

        # Seed the message_match trigger message (persisted by
        # Dispatcher.dispatch_immediate) so ai_reply / forward can consume it
        # directly — no redundant wait_for needed for the firing message.
        if job.resolved_payload and "trigger_message" in job.resolved_payload:
            ctx.last_matched_message = message_from_payload(
                job.resolved_payload["trigger_message"]
            )

        await ctx.emit(
            "run.started",
            {"workflow_name": workflow.name, "account_id": self._account_id},
            message=f"run started for workflow {workflow.name!r}",
        )

        executor = ActionPlanExecutor(workflow.action_plan)
        job_timeout = (
            workflow.action_plan.job_timeout
            or get_settings().job_default_timeout_seconds
        )

        try:
            result = await asyncio.wait_for(executor.execute(ctx), timeout=job_timeout)
        except asyncio.TimeoutError:
            result = ExecutionResult(success=False, error="job_timeout")
        except Exception as exc:  # noqa: BLE001 - we record and move on
            result = ExecutionResult(
                success=False, error=f"{type(exc).__name__}: {exc}"
            )

        finished_status = "succeeded" if result.success else "failed"
        await ctx.emit(
            "run.finished",
            {"status": finished_status, "error": result.error},
            message=f"run {finished_status}"
            + (f": {result.error}" if result.error else ""),
            level="INFO" if result.success else "ERROR",
        )

        await self._mark_terminal(
            job_id=job.id,
            run_id=run_id,
            run_status=(RunStatus.succeeded if result.success else RunStatus.failed),
            error=result.error,
        )
        return result

    async def _mark_terminal(
        self,
        *,
        job_id: int,
        run_id: int | None,
        run_status: RunStatus | None,
        error: str | None,
    ) -> None:
        async with self._session_factory() as session, session.begin():
            if run_id is not None and run_status is not None:
                await run_repo.mark_finished(
                    session,
                    run_id=run_id,
                    owner_id=self._owner_id,
                    status=run_status,
                    error=error,
                )
            if run_status == RunStatus.failed or run_status is None:
                await job_repo.mark_failed(
                    session,
                    job_id=job_id,
                    owner_id=self._owner_id,
                    reason=error or "unknown",
                    run_id=run_id,
                )
            else:
                await job_repo.mark_succeeded(
                    session,
                    job_id=job_id,
                    owner_id=self._owner_id,
                    run_id=run_id,
                )
