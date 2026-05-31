"""§14.3 / 14.4 / 14.5 / 14.11 / 14.12 / 14.13 — read-only HTTP endpoints.

Covers:

* ``GET /accounts`` / ``/accounts/{id}`` — list + single, secrets stripped.
* ``GET /workflows`` / ``/workflows/{id}``.
* ``GET /jobs`` / ``/jobs/{id}``.
* ``GET /runs`` / ``/runs/{id}``.
* ``GET /healthz``.
* §14.12 ``session_string_enc`` / ``api_hash`` MUST NOT appear in any
  response body.
* §14.13 ``limit`` Query bound is enforced (422 above cap).
* Cross-owner reads return 404 on single fetch, ``[]`` on lists.
"""

from __future__ import annotations

import asyncio
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
from tg_conductor.scheduler import repo as job_repo
from tg_conductor.workflows import repo as workflow_repo
from tg_conductor.workflows.models import WorkflowSource
from tg_conductor.workflows.schema import Workflow


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as s, s.begin():
            acc = await account_repo.upsert_session(
                s,
                owner_id=1,
                label="main",
                api_id=1234,
                api_hash="HASH-DO-NOT-LEAK",
                session_string="SESSION-DO-NOT-LEAK",
                proxy="socks5://example:9050",
            )
            wf = await workflow_repo.upsert_by_source(
                s,
                owner_id=1,
                source=WorkflowSource.yaml,
                workflow=Workflow.model_validate(
                    {
                        "name": "w1",
                        "account_id": acc.id,
                        "trigger": {"type": "startup"},
                        "action_plan": {
                            "steps": [
                                {"action": "send_text", "chat_id": 1, "text": "x"}
                            ]
                        },
                    }
                ),
            )
            job = await job_repo.create_pending(
                s,
                owner_id=1,
                workflow_id=wf.id,  # type: ignore[arg-type]
                account_id=acc.id,  # type: ignore[arg-type]
                fire_at=datetime.now(UTC),
            )
            run = await run_repo.create_run(
                s,
                owner_id=1,
                workflow_id=wf.id,  # type: ignore[arg-type]
                account_id=acc.id,  # type: ignore[arg-type]
                job_id=job.id,  # type: ignore[arg-type]
            )
            # stash ids on the fixture for downstream tests via session.info
            s.info["seed_ids"] = {
                "account_id": acc.id,
                "workflow_id": wf.id,
                "job_id": job.id,
                "run_id": run.id,
            }
        yield factory
    finally:
        await engine.dispose()


async def _ids(
    factory: async_sessionmaker[AsyncSession],
) -> dict[str, int]:
    async with factory() as s:
        accs = await account_repo.list_for_owner(s, owner_id=1)
        wfs = await workflow_repo.list_for_owner(s, owner_id=1)
        runs = await run_repo.list_for_owner(s, 1)
    return {
        "account_id": accs[0].id,  # type: ignore[dict-item]
        "workflow_id": wfs[0].id,  # type: ignore[dict-item]
        "run_id": runs[0].id,  # type: ignore[dict-item]
    }


def _app(factory, owner_id: int = 1):
    return create_app(session_factory=factory, default_owner_id=owner_id)


# ----------------------------------------------------------- accounts


@pytest.mark.asyncio
async def test_list_accounts_strips_secrets(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/accounts")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    acc = body[0]
    assert acc["api_id"] == 1234
    assert acc["label"] == "main"
    assert acc["proxy"] == "socks5://example:9050"
    assert acc["has_session"] is True
    # Secrets MUST NOT appear in the body.
    raw = resp.text
    assert "HASH-DO-NOT-LEAK" not in raw
    assert "SESSION-DO-NOT-LEAK" not in raw
    assert "api_hash" not in acc
    assert "session_string_enc" not in acc


@pytest.mark.asyncio
async def test_get_account_404_on_unknown_id(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/accounts/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_account_cross_owner_returns_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _ids(session_factory)
    transport = ASGITransport(app=_app(session_factory, owner_id=2))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/accounts/{ids['account_id']}")
    assert resp.status_code == 404


# ----------------------------------------------------------- workflows


@pytest.mark.asyncio
async def test_list_workflows_returns_validated_payload(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/workflows")
    body = resp.json()
    assert len(body) == 1
    wf = body[0]
    assert wf["name"] == "w1"
    assert wf["trigger"]["type"] == "startup"
    assert wf["action_plan"]["steps"][0]["action"] == "send_text"


@pytest.mark.asyncio
async def test_get_workflow_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/workflows/9999")
    assert resp.status_code == 404


# ----------------------------------------------------------- jobs


@pytest.mark.asyncio
async def test_list_jobs_filters_by_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/jobs", params={"status": "pending"})
    body = resp.json()
    assert len(body) == 1
    assert body[0]["status"] == "pending"


@pytest.mark.asyncio
async def test_jobs_limit_clamped_at_500(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/jobs", params={"limit": 501})
    assert resp.status_code == 422


# ----------------------------------------------------------- runs


@pytest.mark.asyncio
async def test_list_runs_and_get_by_id(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _ids(session_factory)
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        list_resp = await client.get("/runs")
        get_resp = await client.get(f"/runs/{ids['run_id']}")
    assert list_resp.status_code == 200
    assert get_resp.status_code == 200
    assert list_resp.json()[0]["id"] == ids["run_id"]
    assert get_resp.json()["job_id"] == list_resp.json()[0]["job_id"]


@pytest.mark.asyncio
async def test_get_run_cross_owner_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _ids(session_factory)
    transport = ASGITransport(app=_app(session_factory, owner_id=2))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(f"/runs/{ids['run_id']}")
    assert resp.status_code == 404


# ----------------------------------------------------------- healthz


@pytest.mark.asyncio
async def test_healthz_reports_db_ok_and_account_counts(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["db"] == "ok"
    assert body["accounts_total"] == 1
    # No AccountManager attached → falls back to DB online count.
    assert body["accounts_online"] == 0


@pytest.mark.asyncio
async def test_healthz_no_secrets_in_response(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz")
    raw = resp.text
    assert "HASH-DO-NOT-LEAK" not in raw
    assert "SESSION-DO-NOT-LEAK" not in raw


# ----------------------------------------------------------- list cross-owner


@pytest.mark.asyncio
async def test_lists_cross_owner_return_empty_not_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    transport = ASGITransport(app=_app(session_factory, owner_id=2))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        for path in ("/accounts", "/workflows", "/jobs", "/runs"):
            r = await client.get(path)
            assert r.status_code == 200, path
            assert r.json() == [], path
