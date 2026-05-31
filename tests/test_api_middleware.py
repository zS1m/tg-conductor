"""§14.8 / 14.9 / 14.10 / 14.14 / 14.16 — middleware behavior.

Covers:

* Unified error envelope on 404, 422, 500.
* ``X-Request-ID`` echoed on success and error; honors caller-supplied
  header.
* CORS default disabled — no ``Access-Control-Allow-Origin``.
* CORS explicit origins → preflight + actual response pass through.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from fastapi import APIRouter
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tg_conductor.api import create_app
from tg_conductor.db.engine import create_engine
from tg_conductor.db.migrate import upgrade_head


@pytest.fixture
async def session_factory(
    master_key: str,  # noqa: ARG001
    tmp_sqlite_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    await asyncio.to_thread(upgrade_head, tmp_sqlite_url)
    engine = create_engine(tmp_sqlite_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


def _add_boom_route(app) -> None:
    router = APIRouter()

    @router.get("/_test/boom")
    async def boom() -> dict:
        raise RuntimeError("kaboom")

    app.include_router(router)


# ----------------------------------------------------------- envelope


@pytest.mark.asyncio
async def test_404_uses_error_envelope_with_type_and_request_id(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/accounts/99999")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"]["type"] == "not_found"
    assert "account not found" in body["error"]["message"]
    assert body["error"]["request_id"]
    assert resp.headers["X-Request-ID"] == body["error"]["request_id"]


@pytest.mark.asyncio
async def test_422_validation_error_envelope_includes_errors_list(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/jobs", params={"limit": 9999})
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["type"] == "validation_error"
    assert "errors" in body["error"]
    assert resp.headers["X-Request-ID"]


@pytest.mark.asyncio
async def test_500_unhandled_exception_envelope(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    _add_boom_route(app)
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/_test/boom")
    assert resp.status_code == 500
    body = resp.json()
    assert body["error"]["type"] == "internal_error"
    # Generic message — the raw exception text MUST NOT leak.
    assert body["error"]["message"] == "internal server error"
    assert "kaboom" not in resp.text


# ----------------------------------------------------------- request id


@pytest.mark.asyncio
async def test_request_id_generated_per_request(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.get("/healthz")
        r2 = await client.get("/healthz")
    rid1 = r1.headers["X-Request-ID"]
    rid2 = r2.headers["X-Request-ID"]
    assert rid1 and rid2 and rid1 != rid2


@pytest.mark.asyncio
async def test_request_id_honors_caller_header(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(session_factory=session_factory, default_owner_id=1)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/healthz", headers={"X-Request-ID": "trace-from-caller"}
        )
    assert resp.headers["X-Request-ID"] == "trace-from-caller"


# ----------------------------------------------------------- CORS


@pytest.mark.asyncio
async def test_cors_default_disabled_no_allow_origin_header(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(
        session_factory=session_factory, default_owner_id=1, cors_origins=[]
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz", headers={"Origin": "http://evil.example"})
    assert resp.status_code == 200
    assert "access-control-allow-origin" not in {k.lower() for k in resp.headers}


@pytest.mark.asyncio
async def test_cors_explicit_origin_allows(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(
        session_factory=session_factory,
        default_owner_id=1,
        cors_origins=["http://app.example"],
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz", headers={"Origin": "http://app.example"})
    assert resp.status_code == 200
    assert resp.headers.get("access-control-allow-origin") == "http://app.example"


@pytest.mark.asyncio
async def test_cors_origin_not_in_allowlist_blocked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = create_app(
        session_factory=session_factory,
        default_owner_id=1,
        cors_origins=["http://app.example"],
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/healthz", headers={"Origin": "http://other.example"})
    # Server still serves the body (CORS is enforced by the browser) but
    # does not echo Allow-Origin → browser refuses.
    assert resp.status_code == 200
    assert "access-control-allow-origin" not in {k.lower() for k in resp.headers}
