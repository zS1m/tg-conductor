"""``/runs`` routes — Run-scoped reads. §12 lands history + SSE.

§12.1 ``GET /runs/{run_id}/events?since=&limit=``
    Returns events with ``seq > since`` for ``run_id``, ascending by
    ``seq``, capped by ``limit`` (default 200, max 1000 per
    ``specs/runs/spec.md``).

§12.2 ``GET /runs/{run_id}/stream?since=``
    Server-Sent Events long-polling. Algorithm:

    1. Subscribe to the bus topic ``run:<id>`` **before** reading
       history — this closes the race where a new event lands between
       the DB fetch and the live drain.
    2. Page through DB events with ``seq > since`` in 1000-row chunks
       (covers Runs longer than the REST endpoint's 200 page cap),
       yielding each as an SSE frame, recording seen ``seq`` values.
    3. After history, check the Run row's status. If terminal, close
       the stream — history already contained whatever the Run will
       ever emit.
    4. Otherwise, drain the bus. Use ``asyncio.wait_for`` with the
       configured keepalive timeout so an idle Run still emits
       ``: keepalive`` comments every 15 s (spec §12.3).
    5. Close after the first live ``run.finished`` event (spec §12.4).

Cross-owner / nonexistent run returns an empty list (REST) or an
immediately-closed stream (SSE). The multi-tenant invariant from
``CLAUDE.md`` keeps owner id existence private — no 404.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.api.deps import get_event_bus, get_owner_id, get_session_factory
from tg_conductor.api.schemas import RunEventResponse, RunResponse
from tg_conductor.api.sse import KEEPALIVE_FRAME, format_event, row_to_message
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import EventBus, run_topic
from tg_conductor.runs.models import RunStatus

router = APIRouter(prefix="/runs", tags=["runs"])
log = logging.getLogger(__name__)

_SSE_HISTORY_PAGE = 1000


@router.get("", response_model=list[RunResponse])
async def list_runs(
    workflow_id: int | None = Query(default=None),
    status: RunStatus | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> list[RunResponse]:
    async with session_factory() as session:
        rows = await run_repo.list_for_owner(
            session, owner_id, workflow_id=workflow_id, status=status, limit=limit
        )
    return [RunResponse.from_row(r) for r in rows]


@router.get("/{run_id}", response_model=RunResponse)
async def get_run(
    run_id: int,
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> RunResponse:
    async with session_factory() as session:
        row = await run_repo.get_by_id(session, run_id=run_id, owner_id=owner_id)
    if row is None:
        raise HTTPException(status_code=404, detail="run not found")
    return RunResponse.from_row(row)


@router.get("/{run_id}/events", response_model=list[RunEventResponse])
async def list_events(
    run_id: int,
    since: int = Query(default=-1, description="Return events with seq > since"),
    limit: int = Query(default=200, ge=1, le=1000),
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> list[RunEventResponse]:
    async with session_factory() as session:
        rows = await run_repo.list_events(
            session,
            run_id=run_id,
            owner_id=owner_id,
            since=since,
            limit=limit,
        )
    return [RunEventResponse.from_row(r) for r in rows]


@router.get("/{run_id}/stream")
async def stream_events(
    request: Request,
    run_id: int,
    since: int = Query(default=-1, description="Resume after this seq"),
    owner_id: int = Depends(get_owner_id),
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
    bus: EventBus = Depends(get_event_bus),
) -> StreamingResponse:
    keepalive_seconds: float = request.app.state.sse_keepalive_seconds

    async def gen() -> AsyncIterator[bytes]:
        # Subscribe FIRST so we don't miss anything published while we
        # page through history. ``InMemoryEventBus.subscribe`` is an
        # async generator — calling it (no await) returns the iterator.
        sub = bus.subscribe(run_topic(run_id))
        seen_seqs: set[int] = set()
        try:
            # ---- 1. DB catch-up, in 1000-row pages. ----
            saw_finished = False
            cursor = since
            while True:
                async with session_factory() as session:
                    batch = await run_repo.list_events(
                        session,
                        run_id=run_id,
                        owner_id=owner_id,
                        since=cursor,
                        limit=_SSE_HISTORY_PAGE,
                    )
                if not batch:
                    break
                for row in batch:
                    seen_seqs.add(row.seq)
                    yield format_event(row_to_message(row))
                    if row.type == "run.finished":
                        saw_finished = True
                cursor = batch[-1].seq
                if saw_finished:
                    return
                if len(batch) < _SSE_HISTORY_PAGE:
                    break

            # ---- 2. If the Run is already terminal, nothing more will
            # ever be published — close cleanly. (Handles the
            # ``since=<last_seq>`` reconnect against a finished Run.)
            async with session_factory() as session:
                run = await run_repo.get_by_id(
                    session, run_id=run_id, owner_id=owner_id
                )
            if run is None or run.status != RunStatus.running:
                return

            # ---- 3. Live drain with idle keepalive. ----
            # We use ``asyncio.wait`` (not ``asyncio.wait_for``) because
            # ``wait_for`` cancels the underlying coroutine on timeout —
            # and cancelling ``sub.__anext__()`` propagates into the
            # subscribe() async generator's ``await queue.get()``, which
            # then runs its ``finally`` and unregisters the queue from
            # the bus. The result is that after the first keepalive
            # tick, all subsequent publishes silently bypass us. By
            # keeping the same ``next_task`` alive across timeouts we
            # preserve the subscription.
            next_task: asyncio.Task[dict[str, object]] | None = None
            try:
                while True:
                    if next_task is None:
                        next_task = asyncio.ensure_future(sub.__anext__())
                    done, _pending = await asyncio.wait(
                        {next_task}, timeout=keepalive_seconds
                    )
                    if next_task not in done:
                        yield KEEPALIVE_FRAME
                        continue
                    try:
                        msg = next_task.result()
                    except StopAsyncIteration:
                        return
                    next_task = None
                    if msg["seq"] in seen_seqs:
                        continue
                    yield format_event(msg)
                    if msg["type"] == "run.finished":
                        return
            finally:
                if next_task is not None and not next_task.done():
                    next_task.cancel()
                    try:
                        await next_task
                    except (asyncio.CancelledError, StopAsyncIteration, Exception):  # noqa: BLE001
                        pass
        finally:
            await sub.aclose()

    return StreamingResponse(gen(), media_type="text/event-stream")
