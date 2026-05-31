"""§12.2 / §12.3 / §12.4 / §12.6 / §12.7 — SSE stream end-to-end.

Spec scenarios covered:

* History → live no gap, no dup (§12.6).
* Idle Run emits ``: keepalive`` comments every ``sse_keepalive_seconds``
  (§12.3 / §12.7).
* SSE closes after ``run.finished`` (§12.4).
* ``since`` resumes strictly after a seq (§spec runs §历史 + 实时无缝衔接).
* Cross-owner / unknown run closes the stream immediately with no body.

Implementation uses httpx ``ASGITransport`` + ``client.stream(...)``;
both the route and the test parse a tiny SSE dialect (``data:`` lines +
``: keepalive`` comments).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.api import create_app
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.runs import repo as run_repo
from tg_conductor.runs.event_bus import InMemoryEventBus, run_topic
from tg_conductor.runs.event_writer import EventWriter
from tg_conductor.runs.models import RunEventRow, RunStatus
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow

# ----------------------------------------------------------------- fixtures


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            acc = await account_repo.upsert_session(
                session,
                owner_id=1,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await workflow_repo.upsert_by_source(
                session,
                owner_id=1,
                source=WorkflowSource.yaml,
                workflow=Workflow.model_validate(
                    {
                        "name": "w1",
                        "account_id": acc.id,
                        "trigger": {"type": "startup"},
                        "action_plan": {
                            "steps": [
                                {"action": "send_text", "chat_id": 1, "text": "x"},
                            ]
                        },
                    }
                ),
            )
        yield factory
    finally:
        await engine.dispose()


async def _seed_run(
    factory: async_sessionmaker[AsyncSession],
    *,
    owner_id: int = 1,
    status: RunStatus = RunStatus.running,
) -> int:
    async with factory() as s, s.begin():
        accs = await account_repo.list_for_owner(s, owner_id=owner_id)
        wfs = await workflow_repo.list_for_owner(s, owner_id=owner_id)
        job = await job_repo.create_pending(
            s,
            owner_id=owner_id,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            fire_at=datetime.now(UTC),
        )
        run = await run_repo.create_run(
            s,
            owner_id=owner_id,
            workflow_id=wfs[0].id,  # type: ignore[arg-type]
            account_id=accs[0].id,  # type: ignore[arg-type]
            job_id=job.id,  # type: ignore[arg-type]
        )
        if status != RunStatus.running:
            await run_repo.mark_finished(
                s, run_id=run.id, owner_id=owner_id, status=status
            )
        return run.id  # type: ignore[return-value]


async def _seed_events(
    factory: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    owner_id: int,
    items: list[tuple[int, str]],
) -> None:
    """Insert events with explicit (seq, type) pairs, all attrs empty."""
    async with factory() as s, s.begin():
        for seq, typ in items:
            s.add(
                RunEventRow(
                    owner_id=owner_id,
                    run_id=run_id,
                    seq=seq,
                    type=typ,
                    message="",
                    attrs=None,
                )
            )


def _make_app(
    factory: async_sessionmaker[AsyncSession],
    *,
    bus: InMemoryEventBus | None = None,
    owner_id: int = 1,
    keepalive: float = 15.0,
):
    bus = bus or InMemoryEventBus()
    app = create_app(
        session_factory=factory,
        event_bus=bus,
        default_owner_id=owner_id,
        sse_keepalive_seconds=keepalive,
    )
    return app, bus


def _parse_sse(raw: bytes) -> tuple[list[dict], int]:
    """Return (data events parsed as JSON, keepalive-comment count)."""
    events: list[dict] = []
    keepalives = 0
    text = raw.decode("utf-8")
    for frame in text.split("\n\n"):
        if not frame.strip():
            continue
        if frame.startswith(":"):
            keepalives += 1
            continue
        for line in frame.splitlines():
            if line.startswith("data: "):
                events.append(json.loads(line[len("data: ") :]))
                break
    return events, keepalives


# --------------------------------------------------------------- §12.2 history-only


@pytest.mark.asyncio
async def test_history_only_for_terminal_run_then_close(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Run already finished — SSE delivers DB history then closes."""
    run_id = await _seed_run(session_factory, status=RunStatus.succeeded)
    await _seed_events(
        session_factory,
        run_id=run_id,
        owner_id=1,
        items=[
            (0, "run.started"),
            (1, "action.send.text"),
            (2, "run.finished"),
        ],
    )

    app, _ = _make_app(session_factory)
    transport = ASGITransport(app=app)
    raw = b""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream("GET", f"/runs/{run_id}/stream") as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")
            async for chunk in resp.aiter_bytes():
                raw += chunk

    events, keepalives = _parse_sse(raw)
    assert [e["seq"] for e in events] == [0, 1, 2]
    assert [e["type"] for e in events] == [
        "run.started",
        "action.send.text",
        "run.finished",
    ]
    assert keepalives == 0


# --------------------------------------------------------------- §12.6 history + live, no dup / no gap


@pytest.mark.asyncio
async def test_history_plus_live_no_gap_no_dup(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Run is running with 2 pre-existing events; 2 more arrive live; close on finished.

    The dedupe path is also exercised: we replay seq=1 via a direct
    ``bus.publish`` to simulate the race where a single committed event
    is observed by *both* history fetch and live drain. The client must
    see seq=1 exactly once.
    """
    run_id = await _seed_run(session_factory, status=RunStatus.running)
    await _seed_events(
        session_factory,
        run_id=run_id,
        owner_id=1,
        items=[(0, "run.started"), (1, "action.send.text")],
    )

    app, bus = _make_app(session_factory)
    writer = EventWriter(session_factory=session_factory, event_bus=bus)

    async def producer() -> None:
        # Let the SSE handler subscribe + read history.
        await asyncio.sleep(0.08)
        # Replay seq=1 directly on the bus (no DB write) — must be deduped.
        bus.publish(
            run_topic(run_id),
            {
                "id": -1,
                "owner_id": 1,
                "run_id": run_id,
                "seq": 1,
                "type": "action.send.text",
                "message": "",
                "level": "INFO",
                "attrs": None,
                "ts": datetime.now(UTC).isoformat(),
            },
        )
        # Two genuine live events.
        await writer.write_event(
            owner_id=1, run_id=run_id, seq=2, event_type="action.forward"
        )
        await writer.write_event(
            owner_id=1, run_id=run_id, seq=3, event_type="run.finished"
        )

    transport = ASGITransport(app=app)
    raw = b""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        task = asyncio.create_task(producer())
        async with client.stream("GET", f"/runs/{run_id}/stream") as resp:
            async for chunk in resp.aiter_bytes():
                raw += chunk
        await task

    events, _ = _parse_sse(raw)
    seqs = [e["seq"] for e in events]
    types = [e["type"] for e in events]
    assert seqs == [0, 1, 2, 3]  # no dup of 1, no gap
    assert types == [
        "run.started",
        "action.send.text",
        "action.forward",
        "run.finished",
    ]


# --------------------------------------------------------------- §12.3 / §12.7 keepalive


@pytest.mark.asyncio
async def test_keepalive_emitted_when_idle(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Idle Run produces ``: keepalive`` comments at the configured interval."""
    run_id = await _seed_run(session_factory, status=RunStatus.running)

    app, bus = _make_app(session_factory, keepalive=0.05)
    writer = EventWriter(session_factory=session_factory, event_bus=bus)

    async def finisher() -> None:
        # Wait for at least 3 keepalive intervals before closing the run.
        await asyncio.sleep(0.25)
        await writer.write_event(
            owner_id=1, run_id=run_id, seq=0, event_type="run.finished"
        )

    transport = ASGITransport(app=app)
    raw = b""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        task = asyncio.create_task(finisher())
        async with client.stream("GET", f"/runs/{run_id}/stream") as resp:
            async for chunk in resp.aiter_bytes():
                raw += chunk
        await task

    events, keepalives = _parse_sse(raw)
    assert keepalives >= 3, raw  # at least three idle ticks
    assert [e["type"] for e in events] == ["run.finished"]


# --------------------------------------------------------------- since= resume


@pytest.mark.asyncio
async def test_since_resumes_strictly_after_seq(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec runs §历史 + 实时 — Scenario "不丢不重"."""
    run_id = await _seed_run(session_factory, status=RunStatus.succeeded)
    await _seed_events(
        session_factory,
        run_id=run_id,
        owner_id=1,
        items=[
            (0, "run.started"),
            (1, "action.send.text"),
            (2, "action.forward"),
            (3, "run.finished"),
        ],
    )

    app, _ = _make_app(session_factory)
    transport = ASGITransport(app=app)
    raw = b""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "GET", f"/runs/{run_id}/stream", params={"since": 1}
        ) as resp:
            async for chunk in resp.aiter_bytes():
                raw += chunk

    events, _ = _parse_sse(raw)
    assert [e["seq"] for e in events] == [2, 3]


# --------------------------------------------------------------- cross-owner


@pytest.mark.asyncio
async def test_cross_owner_closes_immediately(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A different owner sees no body and stream closes (no 404 leak)."""
    run_id = await _seed_run(session_factory, owner_id=1, status=RunStatus.succeeded)
    await _seed_events(
        session_factory,
        run_id=run_id,
        owner_id=1,
        items=[(0, "run.started"), (1, "run.finished")],
    )

    # App configured for owner=2 must not see anything.
    app, _ = _make_app(session_factory, owner_id=2)
    transport = ASGITransport(app=app)
    raw = b""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream("GET", f"/runs/{run_id}/stream") as resp:
            assert resp.status_code == 200
            async for chunk in resp.aiter_bytes():
                raw += chunk

    events, keepalives = _parse_sse(raw)
    assert events == []
    assert keepalives == 0


# --------------------------------------------------------------- since past last seq


@pytest.mark.asyncio
async def test_since_past_last_seq_for_terminal_run_closes_with_no_body(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """``since=<last_seq>`` against a terminal Run → empty stream that closes.

    Without this guard the stream would hang on the keepalive loop because
    nothing more will ever be published.
    """
    run_id = await _seed_run(session_factory, status=RunStatus.succeeded)
    await _seed_events(
        session_factory,
        run_id=run_id,
        owner_id=1,
        items=[(0, "run.started"), (1, "run.finished")],
    )

    app, _ = _make_app(session_factory)
    transport = ASGITransport(app=app)
    raw = b""
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "GET", f"/runs/{run_id}/stream", params={"since": 1}
        ) as resp:
            async for chunk in resp.aiter_bytes():
                raw += chunk

    assert raw == b""
