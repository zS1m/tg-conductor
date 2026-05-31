"""Cross-cutting HTTP plumbing — request id, unified error envelope, CORS.

§14.8 — *every* error response (4xx + 5xx) is shaped as:

    {"error": {"type": "<short_code>", "message": "<human>",
               "request_id": "<uuid>"}}

§14.9 — every request gets a UUID4 id (or the caller's
``X-Request-ID`` header if present). It's exposed on
``request.state.request_id`` for downstream code (so logs can pin a
trace to the response), and echoed in the response header
``X-Request-ID``.

§14.10 — CORS uses Starlette's :class:`CORSMiddleware` wired against
``settings.cors_origins``. Empty list (default) means no
``Access-Control-Allow-Origin`` is sent — the browser refuses
cross-origin reads. Production deployments add explicit origins via
``CORS_ORIGINS`` env.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from tg_conductor.logging import bind_request_id, clear_request_id

log = logging.getLogger(__name__)

_REQUEST_ID_HEADER = "X-Request-ID"


def install_middlewares(app: FastAPI, *, cors_origins: list[str]) -> None:
    """Attach the cross-cutting middlewares + error handlers to ``app``."""
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=False,
            allow_methods=["GET", "POST"],
            allow_headers=["*"],
        )

    @app.middleware("http")
    async def request_id_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[JSONResponse]],
    ) -> JSONResponse:
        request_id = request.headers.get(_REQUEST_ID_HEADER) or uuid.uuid4().hex
        request.state.request_id = request_id
        # Bind onto structlog contextvars so every log line emitted
        # while this request is in flight carries ``request_id=...``.
        bind_request_id(request_id)
        try:
            response = await call_next(request)
        finally:
            clear_request_id()
        response.headers[_REQUEST_ID_HEADER] = request_id
        return response

    @app.exception_handler(HTTPException)
    async def _http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        return _envelope(
            request,
            status=exc.status_code,
            type_=_http_code_to_type(exc.status_code),
            message=str(exc.detail),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return _envelope(
            request,
            status=422,
            type_="validation_error",
            message="request validation failed",
            extra={"errors": exc.errors()},
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        log.exception(
            "api.unhandled_exception request_id=%s path=%s",
            request_id,
            request.url.path,
        )
        return _envelope(
            request,
            status=500,
            type_="internal_error",
            message="internal server error",
        )


def _envelope(
    request: Request,
    *,
    status: int,
    type_: str,
    message: str,
    extra: dict[str, object] | None = None,
) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    body: dict[str, object] = {
        "error": {"type": type_, "message": message, "request_id": request_id}
    }
    if extra:
        body["error"].update(extra)  # type: ignore[union-attr]
    headers = {_REQUEST_ID_HEADER: request_id} if request_id else {}
    return JSONResponse(content=body, status_code=status, headers=headers)


def _http_code_to_type(code: int) -> str:
    if code == 404:
        return "not_found"
    if code == 401:
        return "unauthorized"
    if code == 403:
        return "forbidden"
    if code == 409:
        return "conflict"
    if 400 <= code < 500:
        return "bad_request"
    return "internal_error"
