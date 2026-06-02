"""spec scheduler §"每日 plan 展开" — the daily re-expansion background task.

Regression for the "works day 1, silent day 2" bug. The first fix added a
daily loop but slept on one multi-hour timer, which in production never
woke — every day after startup stayed silent. The loop now *polls* on a
short interval (like the dispatcher, which never had the problem). These
tests pin:

* :meth:`DailyExpander.expand_for` produces Jobs for *each* day it's asked
  about (the missing multi-day capability).
* The polling loop expands a day once it passes ``expand_at`` and — the
  decisive case the old single-timer design failed — expands the *next*
  day too, with the same long-lived task, after the wall clock rolls over.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, time

import pytest
from freezegun import freeze_time
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler import daily_expander as de
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.daily_expander import DailyExpander
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> async_sessionmaker[AsyncSession]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            acc = await account_repo.upsert_session(
                session,
                owner_id=1,
                label="main",
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await session.commit()
            async with factory() as s2, s2.begin():
                await workflow_repo.upsert_by_source(
                    s2,
                    owner_id=1,
                    source=WorkflowSource.yaml,
                    workflow=Workflow.model_validate(
                        {
                            "name": "daily-chat",
                            "account_id": acc.id,
                            "trigger": {
                                "type": "time_window",
                                "window": "10:00-23:00",
                                "count": 4,
                                "min_gap": "1h",
                            },
                            "action_plan": {
                                "steps": [
                                    {"action": "send_text", "chat_id": 1, "text": "hi"}
                                ]
                            },
                        }
                    ),
                )
        yield factory
    finally:
        await engine.dispose()


# --------------------------------------------------------------- expand_for


@pytest.mark.asyncio
async def test_expand_for_creates_jobs_each_day(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The core regression: a second day yields its own Jobs, not zero."""
    expander = DailyExpander(
        session_factory=session_factory,
        owner_id=1,
        expand_at=time(0, 5),
        tz=UTC,
    )

    with freeze_time("2026-06-01 00:05:00"):
        r1 = await expander.expand_for(date(2026, 6, 1))
    with freeze_time("2026-06-02 00:05:00"):
        r2 = await expander.expand_for(date(2026, 6, 2))

    assert len(r1.created_job_ids) == 4
    assert len(r2.created_job_ids) == 4  # day 2 is NOT silent

    async with session_factory() as session:
        jobs = await job_repo.list_for_owner(session, owner_id=1, limit=100)
    dates = sorted({j.expansion_date for j in jobs})
    assert dates == [date(2026, 6, 1), date(2026, 6, 2)]


@pytest.mark.asyncio
async def test_expand_for_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    expander = DailyExpander(
        session_factory=session_factory,
        owner_id=1,
        expand_at=time(0, 5),
        tz=UTC,
    )
    with freeze_time("2026-06-01 00:05:00"):
        first = await expander.expand_for(date(2026, 6, 1))
        second = await expander.expand_for(date(2026, 6, 1))
    assert len(first.created_job_ids) == 4
    assert second.created_job_ids == []  # same day → no duplicates


# --------------------------------------------------------------- polling loop


async def _wait_until(predicate, timeout: float = 2.0) -> None:
    """Poll ``predicate`` in real time until true (loop uses real asyncio sleep)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met within timeout")


@pytest.mark.asyncio
async def test_loop_waits_until_expand_at_then_expands(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before ``expand_at`` the loop must NOT expand; once past it, it must.

    ``_now`` is the injected clock seam — we hold it before 00:05, confirm
    nothing fires, then move it past and confirm the expansion runs.
    """
    expander = DailyExpander(
        session_factory=session_factory,
        owner_id=1,
        expand_at=time(0, 5),
        tz=UTC,
        poll_seconds=0.01,
    )
    clock = {"now": datetime(2026, 6, 2, 0, 4, tzinfo=UTC)}  # before 00:05
    seen: list[date] = []

    async def recorder(d: date) -> object:
        seen.append(d)
        return de.ExpansionReport()

    monkeypatch.setattr(expander, "_now", lambda: clock["now"])
    monkeypatch.setattr(expander, "expand_for", recorder)

    await expander.start()
    try:
        await asyncio.sleep(0.1)  # several poll ticks while still before 00:05
        assert seen == []  # not yet
        clock["now"] = datetime(2026, 6, 2, 0, 6, tzinfo=UTC)  # past 00:05
        await _wait_until(lambda: seen == [date(2026, 6, 2)])
    finally:
        await expander.stop()


@pytest.mark.asyncio
async def test_loop_expands_each_day_across_rollover(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The decisive regression: ONE long-lived task expands day N+1 too.

    The old single-long-timer design woke once (or never) and went silent.
    The polling loop must keep firing across midnight: day 2, then day 3,
    each exactly once, from the same task.
    """
    expander = DailyExpander(
        session_factory=session_factory,
        owner_id=1,
        expand_at=time(0, 5),
        tz=UTC,
        poll_seconds=0.01,
    )
    clock = {"now": datetime(2026, 6, 2, 0, 6, tzinfo=UTC)}
    seen: list[date] = []

    async def recorder(d: date) -> object:
        seen.append(d)
        return de.ExpansionReport()

    monkeypatch.setattr(expander, "_now", lambda: clock["now"])
    monkeypatch.setattr(expander, "expand_for", recorder)

    await expander.start()
    try:
        await _wait_until(lambda: seen == [date(2026, 6, 2)])
        clock["now"] = datetime(2026, 6, 3, 0, 6, tzinfo=UTC)  # roll over
        await _wait_until(lambda: seen == [date(2026, 6, 2), date(2026, 6, 3)])
        await asyncio.sleep(0.05)  # let more ticks pass — no duplicates
        assert seen == [date(2026, 6, 2), date(2026, 6, 3)]
    finally:
        await expander.stop()
