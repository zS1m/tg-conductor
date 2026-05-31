"""Single-flight ``reload()`` orchestrator: parse YAML → sync_to_db.

Coalescing rule (spec ``config-loader`` 8.5):

* If no reload is in progress, the caller's reload runs immediately.
* If a reload is already in progress, the caller is queued behind a single
  *pending* run that starts as soon as the current one finishes.
* Subsequent callers that arrive while a pending run is queued all share
  that same pending run's result — they do NOT trigger additional reloads.

Effect: under bursty SIGHUP / POST /reload pressure, at most one extra run
is scheduled regardless of caller count. Callers always see "the freshest
result that started after I called".
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.config_loader.parser import (
    FileError,
    LoadResult,
    load_workflows_from_dir,
)
from tg_conductor.config_loader.sync import (
    JobCancelHook,
    SyncReport,
    sync_to_db,
)
from tg_conductor.workflows.schema import Workflow

log = logging.getLogger(__name__)


@dataclass
class ReloadResult:
    sync: SyncReport
    parse_errors: list[FileError] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.parse_errors) or bool(self.sync.validation_errors)


class Reloader:
    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        workflow_dir: Path,
        owner_id: int,
        on_deleted: JobCancelHook | None = None,
        load_workflows: Callable[[Path], LoadResult] = load_workflows_from_dir,
    ) -> None:
        self._session_factory = session_factory
        self._workflow_dir = workflow_dir
        self._owner_id = owner_id
        self._on_deleted = on_deleted
        self._load_workflows = load_workflows
        self._lock = asyncio.Lock()
        self._running: asyncio.Task[ReloadResult] | None = None
        self._pending: asyncio.Task[ReloadResult] | None = None

    async def reload(self) -> ReloadResult:
        async with self._lock:
            if self._running is None:
                self._running = asyncio.create_task(self._wrap_run())
                task = self._running
            elif self._pending is None:
                self._pending = asyncio.create_task(self._wrap_after_current())
                task = self._pending
            else:
                # Both slots full → piggyback on the pending run.
                task = self._pending
        return await task

    async def _wrap_run(self) -> ReloadResult:
        try:
            return await self._execute()
        finally:
            async with self._lock:
                # Promote pending → running so any caller queued behind this
                # one already has its task observable; new arrivals start a
                # fresh pending.
                self._running = self._pending
                self._pending = None

    async def _wrap_after_current(self) -> ReloadResult:
        async with self._lock:
            previous = self._running
        if previous is not None:
            try:
                await previous
            except Exception:  # noqa: BLE001 - upstream exception is its own concern
                pass
        try:
            return await self._execute()
        finally:
            async with self._lock:
                self._running = self._pending
                self._pending = None

    async def _execute(self) -> ReloadResult:
        load = self._load_workflows(self._workflow_dir)

        # Cross-file duplicate-name detection. parser sees one file at a time
        # so this has to happen here. First occurrence (alphabetically first
        # via parser's sorted scan) wins; later occurrences become parse
        # errors pointing at the conflicting path.
        seen_names: dict[str, Path] = {}
        deduped: list[Workflow] = []
        duplicate_errors: list[FileError] = []
        for path, wf in load.workflows:
            if wf.name in seen_names:
                duplicate_errors.append(
                    FileError(
                        path=path,
                        field_path="name",
                        message=(
                            f"duplicate workflow name {wf.name!r}; "
                            f"first defined in {seen_names[wf.name]}"
                        ),
                    )
                )
                continue
            seen_names[wf.name] = path
            deduped.append(wf)

        all_parse_errors = load.errors + duplicate_errors
        for err in all_parse_errors:
            log.warning("config_loader.parse_error %s", err.format())

        async with self._session_factory() as session, session.begin():
            sync_report = await sync_to_db(
                session,
                owner_id=self._owner_id,
                desired=deduped,
                on_deleted=self._on_deleted,
            )

        for name, msg in sync_report.validation_errors:
            log.warning("config_loader.validation_error workflow=%s %s", name, msg)
        if sync_report.change_count:
            log.info(
                "config_loader.reload created=%d updated=%d deleted=%d",
                len(sync_report.created),
                len(sync_report.updated),
                len(sync_report.deleted),
            )
        return ReloadResult(sync=sync_report, parse_errors=all_parse_errors)


def install_sighup_handler(reloader: Reloader) -> None:
    """Wire SIGHUP → ``reloader.reload()`` (fire-and-forget) on the running loop.

    No-op on platforms without SIGHUP (Windows): we log and move on so the
    serve command stays cross-platform.
    """
    if not hasattr(signal, "SIGHUP"):
        log.info("SIGHUP not available on this platform; skipping handler")
        return
    loop = asyncio.get_running_loop()

    def _trigger() -> None:
        log.info("SIGHUP received → scheduling config reload")
        asyncio.create_task(reloader.reload(), name="config-reload-sighup")

    loop.add_signal_handler(signal.SIGHUP, _trigger)
