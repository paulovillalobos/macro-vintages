"""One error shape for every failure: RFC 9457 ``application/problem+json``."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from macro_vintages.logs import request_id_var

log = logging.getLogger(__name__)

PROBLEM_TYPE_BASE = "https://github.com/imperiobaal69-web/macro-vintages/blob/main/docs/API.md#"


class ApiError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        detail: str,
        *,
        headers: dict[str, str] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.code = code
        self.detail = detail
        self.headers = headers or {}
        self.extra = extra or {}


_TITLES = {
    400: "Bad request",
    401: "Unauthorized",
    404: "Not found",
    405: "Method not allowed",
    422: "Invalid request",
    429: "Too many requests",
    500: "Internal error",
    503: "Service unavailable",
}


def problem(
    request: Request,
    status: int,
    code: str,
    detail: str,
    *,
    headers: dict[str, str] | None = None,
    extra: dict[str, Any] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {
        "type": PROBLEM_TYPE_BASE + code,
        "title": _TITLES.get(status, "Error"),
        "status": status,
        "detail": detail,
        "instance": request.url.path,
        "request_id": request_id_var.get(),
    }
    body.update(extra or {})
    return JSONResponse(
        body, status_code=status, headers=headers, media_type="application/problem+json"
    )


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError) -> JSONResponse:
        return problem(
            request, exc.status, exc.code, exc.detail, headers=exc.headers, extra=exc.extra
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")}
            for e in exc.errors()
        ]
        return problem(
            request,
            422,
            "invalid-request",
            "request parameters failed validation",
            extra={"errors": errors},
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {404: "not-found", 405: "method-not-allowed"}.get(exc.status_code, "http-error")
        return problem(
            request, exc.status_code, code, str(exc.detail), headers=dict(exc.headers or {})
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error")
        return problem(request, 500, "internal-error", "unexpected server error")
