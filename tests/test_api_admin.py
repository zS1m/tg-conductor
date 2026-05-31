"""§14.6 / 14.7 / 14.15 — ``POST /reload`` and ``POST /workflows/{id}/run``.

Scenarios:

* ``/reload`` 503 when no Reloader is wired (bare ``create_app``).
* ``/reload`` returns the SyncReport counts when a real Reloader is
  attached.
* ``/workflows/{id}/run`` enqueues a Job (202 + ``job_id``).
* ``/workflows/{id}/run`` returns 404 for unknown / cross-owner ids.
* ``/workflows/{id}/run`` returns 409 for **disabled** workflows
  (spec §14.15 — "禁用 workflow 立即触发返回 4xx").
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import yaml
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.accounts import repo as account_repo
from tg_conductor.api import create_app
from tg_conductor.config_loader.reload import Reloader
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head
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
                api_id=1,
                api_hash="h",
                session_string="s",
            )
            await workflow_repo.upsert_by_source(
                s,
                owner_id=1,
                source=WorkflowSource.yaml,
                workflow=Workflow.model_validate(
                    {
                        "name": "enabled-wf",
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
            await workflow_repo.upsert_by_source(
                s,
                owner_id=1,
                source=WorkflowSource.yaml,
                workflow=Workflow.model_validate(
                    {
                        "name": "disabled-wf",
                        "enabled": False,
                        "account_id": acc.id,
                        "trigger": {"type": "startup"},
                        "action_plan": {
                            "steps": [
                                {"action": "send_text", "chat_id": 1, "text": "y"}
                            ]
                        },
                    }
                ),
            )
        yield factory
    finally:
        await engine.dispose()


async def _ids(factory: async_sessionmaker[AsyncSession]) -> dict[str, int]:
    async with factory() as s:
        wfs = await workflow_repo.list_for_owner(s, owner_id=1)
    by_name = {w.name: w for w in wfs}
    return {
        "enabled_wf_id": by_name["enabled-wf"].id,  # type: ignore[dict-item]
        "disabled_wf_id": by_name["disabled-wf"].id,  # type: ignore[dict-item]
    }


# ----------------------------------------------------------- POST /reload


@pytest.mark.asyncio
async def test_post_reload_503_when_reloader_absent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/reload")
    assert resp.status_code == 503
    assert resp.json()["error"]["type"] == "internal_error"


@pytest.mark.asyncio
async def test_post_reload_returns_sync_report(
    session_factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    """A real Reloader pointed at a yaml dir picks up changes."""
    # Seed the on-disk yaml that the reloader will read.
    wf_dir = tmp_path / "workflows"
    wf_dir.mkdir()
    (wf_dir / "newone.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "fresh-from-disk",
                "account_id": 1,
                "trigger": {"type": "startup"},
                "action_plan": {
                    "steps": [{"action": "send_text", "chat_id": 1, "text": "z"}]
                },
            }
        ),
        encoding="utf-8",
    )
    reloader = Reloader(
        session_factory=session_factory,
        workflow_dir=wf_dir,
        owner_id=1,
    )
    app = create_app(
        session_factory=session_factory,
        default_owner_id=1,
        reloader=reloader,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/reload")
    assert resp.status_code == 200
    body = resp.json()
    # The 2 seeded workflows from the fixture are not in the yaml dir, so
    # they get deleted; the on-disk one is created.
    assert body["created"] == 1
    assert body["deleted"] == 2


# ----------------------------------------------------------- POST /workflows/{id}/run


@pytest.mark.asyncio
async def test_trigger_now_enqueues_job_returns_202(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _ids(session_factory)
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(f"/workflows/{ids['enabled_wf_id']}/run")
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]
    async with session_factory() as s:
        row = await job_repo.get_by_id(s, job_id, 1)
    assert row is not None
    assert row.workflow_id == ids["enabled_wf_id"]
    assert row.status == "pending"


@pytest.mark.asyncio
async def test_trigger_unknown_workflow_returns_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/workflows/9999/run")
    assert resp.status_code == 404
    assert resp.json()["error"]["type"] == "not_found"


@pytest.mark.asyncio
async def test_trigger_cross_owner_returns_404(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await _ids(session_factory)
    app = create_app(session_factory=session_factory, default_owner_id=2)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(f"/workflows/{ids['enabled_wf_id']}/run")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_disabled_workflow_returns_409(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """spec §14.15 — disabled workflow → 4xx (we use 409 Conflict)."""
    ids = await _ids(session_factory)
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(f"/workflows/{ids['disabled_wf_id']}/run")
    assert resp.status_code == 409
    assert "disabled" in resp.json()["error"]["message"]
    # No job created.
    async with session_factory() as s:
        rows = await job_repo.list_for_owner(s, 1)
    assert all(r.workflow_id != ids["disabled_wf_id"] for r in rows)
