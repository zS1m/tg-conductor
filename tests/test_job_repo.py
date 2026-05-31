"""§10.1 / §10.3 / §10.11 / §10.13 / §10.14 — JobRow + atomic claim semantics."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.scheduler.models import JobStatus
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
                                {"action": "send_text", "chat_id": 1, "text": "hi"}
                            ]
                        },
                    }
                ),
            )
            await session.commit()
        yield factory
    finally:
        await engine.dispose()


async def _ids(factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    async with factory() as session:
        accs = await account_repo.list_for_owner(session, owner_id=1)
        wfs = await workflow_repo.list_for_owner(session, owner_id=1)
    return accs[0].id, wfs[0].id  # type: ignore[return-value]


async def _seed_jobs(
    factory: async_sessionmaker[AsyncSession],
    *,
    fire_offsets_seconds: list[float],
    expansion_date: date | None = None,
) -> list[int]:
    aid, wid = await _ids(factory)
    now = datetime.now(UTC)
    ids: list[int] = []
    async with factory() as session, session.begin():
        for off in fire_offsets_seconds:
            row = await job_repo.create_pending(
                session,
                owner_id=1,
                workflow_id=wid,
                account_id=aid,
                fire_at=now + timedelta(seconds=off),
                expansion_date=expansion_date,
            )
            ids.append(row.id)  # type: ignore[arg-type]
    return ids


# ------------------------------------------------------------ basic CRUD


@pytest.mark.asyncio
async def test_create_pending_round_trip(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    aid, wid = await _ids(session_factory)
    fire = datetime.now(UTC) + timedelta(seconds=10)
    async with session_factory() as session, session.begin():
        row = await job_repo.create_pending(
            session,
            owner_id=1,
            workflow_id=wid,
            account_id=aid,
            fire_at=fire,
            variant_id="v0",
            resolved_payload={"picked": ["a", "b"]},
            expansion_date=date(2026, 5, 29),
        )
    assert row.id is not None
    async with session_factory() as session:
        fetched = await job_repo.get_by_id(session, row.id, owner_id=1)
    assert fetched is not None
    assert fetched.status == JobStatus.pending
    assert fetched.variant_id == "v0"
    assert fetched.resolved_payload == {"picked": ["a", "b"]}
    assert fetched.expansion_date == date(2026, 5, 29)


@pytest.mark.asyncio
async def test_cross_owner_returns_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _seed_jobs(session_factory, fire_offsets_seconds=[5, 10])
    async with session_factory() as session:
        assert await job_repo.list_for_owner(session, owner_id=2) == []
        assert await job_repo.get_by_id(session, ids[0], owner_id=2) is None


@pytest.mark.asyncio
async def test_list_for_owner_filters_by_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    [a, b] = await _seed_jobs(session_factory, fire_offsets_seconds=[-1, -2])
    async with session_factory() as session, session.begin():
        await job_repo.cancel_pending(session, job_id=a, owner_id=1)
    async with session_factory() as session:
        pendings = await job_repo.list_for_owner(
            session, owner_id=1, status=JobStatus.pending
        )
        cancels = await job_repo.list_for_owner(
            session, owner_id=1, status=JobStatus.canceled
        )
    assert [j.id for j in pendings] == [b]
    assert [j.id for j in cancels] == [a]


# ------------------------------------------------------------ §10.11 cancel


@pytest.mark.asyncio
async def test_cancel_pending_flips_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    [job_id] = await _seed_jobs(session_factory, fire_offsets_seconds=[5])
    async with session_factory() as session, session.begin():
        ok = await job_repo.cancel_pending(session, job_id=job_id, owner_id=1)
    assert ok is True
    async with session_factory() as session:
        row = await job_repo.get_by_id(session, job_id, owner_id=1)
    assert row is not None
    assert row.status == JobStatus.canceled
    assert row.finished_at is not None


@pytest.mark.asyncio
async def test_cancel_pending_is_noop_when_already_running(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A claimed-and-running job must NOT be cancellable via cancel_pending."""
    [job_id] = await _seed_jobs(session_factory, fire_offsets_seconds=[-1])
    # Claim it first (flips to running).
    async with session_factory() as session, session.begin():
        await job_repo.claim_due_jobs(session, owner_id=1)
    async with session_factory() as session, session.begin():
        ok = await job_repo.cancel_pending(session, job_id=job_id, owner_id=1)
    assert ok is False
    async with session_factory() as session:
        row = await job_repo.get_by_id(session, job_id, owner_id=1)
    assert row is not None
    assert row.status == JobStatus.running


@pytest.mark.asyncio
async def test_cancel_pending_for_workflows_bulk(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Used by §8 on_deleted hook: removed workflow → its pending jobs cancel."""
    ids = await _seed_jobs(session_factory, fire_offsets_seconds=[5, 10, 15])
    _aid, wid = await _ids(session_factory)
    async with session_factory() as session, session.begin():
        n = await job_repo.cancel_pending_for_workflows(
            session, workflow_ids=[wid], owner_id=1
        )
    assert n == 3
    async with session_factory() as session:
        statuses = [
            (await job_repo.get_by_id(session, jid, owner_id=1)).status  # type: ignore[union-attr]
            for jid in ids
        ]
    assert all(s == JobStatus.canceled for s in statuses)


# ------------------------------------------------------------ §10.3 / §10.14 claim


@pytest.mark.asyncio
async def test_claim_due_jobs_flips_ready_to_running(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Three pending jobs all due now → all claimed in one batch."""
    ids = await _seed_jobs(session_factory, fire_offsets_seconds=[-1, -2, -3])
    async with session_factory() as session, session.begin():
        result = await job_repo.claim_due_jobs(session, owner_id=1)
    assert sorted(j.id for j in result.claimed) == sorted(ids)  # type: ignore[type-var]
    assert result.skipped == []
    async with session_factory() as session:
        rows = await job_repo.list_for_owner(session, owner_id=1)
    assert all(r.status == JobStatus.running for r in rows)
    assert all(r.started_at is not None for r in rows)


@pytest.mark.asyncio
async def test_claim_skips_jobs_past_compensation_window(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §10.7 / §10.14 — fire_at < now - compensation_window → skipped."""
    # Window=60s. Two jobs: one 30s ago (ok), one 120s ago (too old).
    ids = await _seed_jobs(session_factory, fire_offsets_seconds=[-30, -120])
    fresh, ancient = ids
    async with session_factory() as session, session.begin():
        result = await job_repo.claim_due_jobs(
            session, owner_id=1, compensation_window_seconds=60.0
        )
    assert [j.id for j in result.claimed] == [fresh]
    assert [j.id for j in result.skipped] == [ancient]
    async with session_factory() as session:
        skipped_row = await job_repo.get_by_id(session, ancient, owner_id=1)
    assert skipped_row is not None
    assert skipped_row.status == JobStatus.skipped
    assert skipped_row.skip_reason == "missed_window"
    assert skipped_row.finished_at is not None


@pytest.mark.asyncio
async def test_claim_respects_batch_size(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _seed_jobs(session_factory, fire_offsets_seconds=[-1, -2, -3, -4, -5])
    async with session_factory() as session, session.begin():
        result = await job_repo.claim_due_jobs(session, owner_id=1, batch_size=2)
    assert len(result.claimed) == 2


@pytest.mark.asyncio
async def test_claim_does_not_touch_future_jobs(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _seed_jobs(session_factory, fire_offsets_seconds=[-1, 60])
    async with session_factory() as session, session.begin():
        result = await job_repo.claim_due_jobs(session, owner_id=1)
    assert [j.id for j in result.claimed] == [ids[0]]


# ------------------------------------------------------------ §10.13 atomicity


@pytest.mark.asyncio
async def test_concurrent_claims_do_not_double_dispatch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §10.13 — 2 concurrent claim batches → each row claimed at most once."""
    ids = await _seed_jobs(
        session_factory, fire_offsets_seconds=[-1, -2, -3, -4, -5, -6]
    )

    async def claim_one() -> list[int]:
        async with session_factory() as session, session.begin():
            result = await job_repo.claim_due_jobs(session, owner_id=1)
        return [j.id for j in result.claimed]  # type: ignore[misc]

    a, b = await asyncio.gather(claim_one(), claim_one())
    combined = a + b
    assert len(combined) == len(set(combined)), f"duplicate claim detected: {combined}"
    assert set(combined) == set(ids)


@pytest.mark.asyncio
async def test_claim_with_no_pending_returns_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        result = await job_repo.claim_due_jobs(session, owner_id=1)
    assert result.claimed == []
    assert result.skipped == []


# ------------------------------------------------------------ count_for_expansion


@pytest.mark.asyncio
async def test_count_for_expansion_only_matches_same_date(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _aid, wid = await _ids(session_factory)
    today = date(2026, 5, 29)
    yesterday = date(2026, 5, 28)
    await _seed_jobs(
        session_factory, fire_offsets_seconds=[5, 10, 15], expansion_date=today
    )
    await _seed_jobs(
        session_factory, fire_offsets_seconds=[20], expansion_date=yesterday
    )
    async with session_factory() as session:
        n_today = await job_repo.count_for_expansion(
            session, workflow_id=wid, expansion_date=today
        )
        n_yesterday = await job_repo.count_for_expansion(
            session, workflow_id=wid, expansion_date=yesterday
        )
        n_other_day = await job_repo.count_for_expansion(
            session, workflow_id=wid, expansion_date=date(2030, 1, 1)
        )
    assert n_today == 3
    assert n_yesterday == 1
    assert n_other_day == 0
