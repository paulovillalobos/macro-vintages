"""Request id, access log and metrics as one pure ASGI middleware.

Starlette's ``BaseHTTPMiddleware`` wraps every response body in an extra task
and stream; a plain ASGI callable avoids that cost on the hot path.
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from macro_vintages import metrics
from macro_vintages.logs import request_id_var

log = logging.getLogger("macro_vintages.access")

_VALID_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = None
        for name, value in scope["headers"]:
            if name == b"x-request-id":
                incoming = value.decode("latin-1")
                break
        request_id = incoming if incoming and _VALID_ID.match(incoming) else uuid.uuid4().hex
        token = request_id_var.set(request_id)
        started = time.perf_counter()
        status_holder: dict[str, int] = {"status": 500}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode()))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - started
            route_obj: Any = scope.get("route")
            route = getattr(route_obj, "path", None) or "unmatched"
            status = status_holder["status"]
            metrics.HTTP_REQUESTS.labels(scope["method"], route, str(status)).inc()
            metrics.HTTP_LATENCY.labels(scope["method"], route).observe(elapsed)
            state = scope.get("state") or {}
            log.info(
                "request",
                extra={
                    "method": scope["method"],
                    "path": scope["path"],
                    "route": route,
                    "status": status,
                    "duration_ms": round(elapsed * 1000, 2),
                    "api_key": state.get("api_key_name"),
                },
            )
            request_id_var.reset(token)
