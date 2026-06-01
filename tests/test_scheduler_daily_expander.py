"""spec scheduler §"每日 plan 展开" — the daily re-expansion background task.

Regression for the "works day 1, silent day 2" bug: time_window Workflows
were only ever expanded for the day the process started, because the daily
expansion loop the spec mandates was never wired up. These tests pin:

* :func:`_next_run_at` picks the correct wall-clock day boundary.
* :meth:`DailyExpander.expand_for` produces Jobs for *each* day it's asked
  about (the missing multi-day capability).
* The running loop actually invokes an expansion at the scheduled instant.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
from freezegun import freeze_time
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler import daily_expander as de
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.daily_expander import DailyExpander, _next_run_at
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


# --------------------------------------------------------------- _next_run_at


def test_next_run_at_before_expand_time_picks_today() -> None:
    tz = UTC
    now = datetime(2026, 6, 1, 0, 0, tzinfo=tz)  # before 00:05
    assert _next_run_at(now, time(0, 5), tz) == datetime(2026, 6, 1, 0, 5, tzinfo=tz)


def test_next_run_at_at_or_after_expand_time_picks_tomorrow() -> None:
    tz = UTC
    now = datetime(2026, 6, 1, 0, 5, tzinfo=tz)  # exactly at → strictly after
    assert _next_run_at(now, time(0, 5), tz) == datetime(2026, 6, 2, 0, 5, tzinfo=tz)

    later = datetime(2026, 6, 1, 18, 0, tzinfo=tz)
    assert _next_run_at(later, time(0, 5), tz) == datetime(2026, 6, 2, 0, 5, tzinfo=tz)


def test_next_run_at_uses_wall_clock_in_tz() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    # 16:00 UTC == 00:00 CST next-ish; pick a time so "now" (CST) is before 00:05.
    now = datetime(2026, 6, 1, 0, 0, tzinfo=tz)
    nxt = _next_run_at(now, time(0, 5), tz)
    assert nxt == datetime(2026, 6, 1, 0, 5, tzinfo=tz)
    # The instant is 00:05 Shanghai, i.e. 16:05 UTC the previous day.
    assert nxt.astimezone(UTC) == datetime(2026, 5, 31, 16, 5, tzinfo=UTC)


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


# --------------------------------------------------------------- running loop


@pytest.mark.asyncio
async def test_loop_invokes_expansion_at_scheduled_instant(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop must actually wake and expand — not just compute a schedule.

    We collapse the wait to ~50ms via a patched ``_next_run_at`` and record
    the date the loop asks to expand.
    """
    expander = DailyExpander(
        session_factory=session_factory,
        owner_id=1,
        expand_at=time(0, 5),
        tz=UTC,
    )

    called = asyncio.Event()
    seen: list[date] = []
    fire = datetime.now(UTC) + timedelta(seconds=0.05)

    def fake_next(now: datetime, expand_at: time, tz: object) -> datetime:
        # Collapse the wait to ~50ms; .date() is what the loop expands for.
        return fire

    async def recorder(d: date) -> object:
        seen.append(d)
        called.set()
        # Sleep a touch so the loop doesn't busy-respin before we stop it.
        await asyncio.sleep(0.5)
        return de.ExpansionReport()

    monkeypatch.setattr(de, "_next_run_at", fake_next)
    monkeypatch.setattr(expander, "expand_for", recorder)

    await expander.start()
    try:
        await asyncio.wait_for(called.wait(), timeout=2.0)
    finally:
        await expander.stop()

    assert seen == [fire.date()]
