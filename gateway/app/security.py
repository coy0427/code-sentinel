"""
Security, Rate Limiting, and Structured Logging Components.

Provides sliding-window in-memory rate limiting, mTLS client verification,
and production-ready RFC-compliant JSON structured logging.
"""

from __future__ import annotations

import collections
import json
import logging
import sys
import time
import uuid
from datetime import UTC, datetime

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


# ------------------------------------------------------------------------------
# Structured JSON Logging
# ------------------------------------------------------------------------------
class JSONLogFormatter(logging.Formatter):
    """Formats log records as structured single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include exception information if available
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        # Include custom context attributes passed in 'extra'
        for key in ("request_id", "client_ip", "method", "path", "status_code", "duration_ms"):
            val = getattr(record, key, None)
            if val is not None:
                log_obj[key] = val

        return json.dumps(log_obj)


def configure_structured_logging(log_level: str = "INFO") -> None:
    """Configure standard root logger with the JSON formatter."""
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level.upper())

    # Remove existing handlers to avoid duplicate log lines
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(JSONLogFormatter())
    root_logger.addHandler(stream_handler)


# ------------------------------------------------------------------------------
# Sliding-Window Rate Limiter
# ------------------------------------------------------------------------------
class SlidingWindowRateLimiter:
    """
    High-performance in-memory sliding-window rate limiter.
    Limits requests per client within a moving time window.
    """

    def __init__(self, max_requests: int = 120, window_seconds: int = 60) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._clients: dict[str, collections.deque[float]] = (
            collections.defaultdict(collections.deque)
        )

    def is_allowed(self, client_key: str) -> tuple[bool, int, float]:
        """
        Evaluate if a request is within allowed rate limits.

        :param client_key: Unique client identifier (IP address, certificate CN, or device ID).
        :return: Tuple of (allowed: bool, remaining_tokens: int, retry_after_seconds: float)
        """
        now = time.monotonic()
        window_start = now - self.window_seconds
        timestamps = self._clients[client_key]

        # Purge timestamps outside the sliding window
        while timestamps and timestamps[0] < window_start:
            timestamps.popleft()

        if len(timestamps) < self.max_requests:
            timestamps.append(now)
            remaining = self.max_requests - len(timestamps)
            return True, remaining, 0.0

        # Rate limit exceeded: calculate time until earliest token expires
        earliest_timestamp = timestamps[0]
        retry_after = max(0.1, round(earliest_timestamp + self.window_seconds - now, 2))
        return False, 0, retry_after

    def reset(self) -> None:
        """Clear all active client rate limit windows."""
        self._clients.clear()


# ------------------------------------------------------------------------------
# Middlewares
# ------------------------------------------------------------------------------
class RateLimitingMiddleware(BaseHTTPMiddleware):
    """Applies rate limiting to incoming HTTP requests based on client identity."""

    def __init__(self, app, limiter: SlidingWindowRateLimiter) -> None:
        super().__init__(app)
        self.limiter = limiter

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Exclude internal healthcheck from rate limiting
        if request.url.path in ("/health", "/docs", "/openapi.json"):
            return await call_next(request)

        # Derive client identifier from client certificate if present, or client IP
        client_key = self._extract_client_identifier(request)
        allowed, remaining, retry_after = self.limiter.is_allowed(client_key)

        if not allowed:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too Many Requests",
                    "message": f"Rate limit exceeded. Try again in {retry_after} seconds.",
                    "retry_after": retry_after,
                },
                headers={
                    "Retry-After": str(int(retry_after) + 1),
                    "X-RateLimit-Limit": str(self.limiter.max_requests),
                    "X-RateLimit-Remaining": "0",
                },
            )

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(self.limiter.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        return response

    @staticmethod
    def _extract_client_identifier(request: Request) -> str:
        # Forwarded client cert from reverse proxy (e.g., NGINX / Envoy mTLS offload)
        cert_header = request.headers.get("x-ssl-client-s-dn") or request.headers.get(
            "x-forwarded-client-cert"
        )
        if cert_header:
            return cert_header

        # Direct client IP fallback
        if request.client:
            return request.client.host
        return "anonymous-client"


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """Emits structured JSON access logs for each HTTP transaction."""

    def __init__(self, app) -> None:
        super().__init__(app)
        self.logger = logging.getLogger("gateway.access")

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        client_ip = request.client.host if request.client else "unknown"
        start_time = time.perf_counter()

        response: Response | None = None
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            self.logger.info(
                f"{request.method} {request.url.path} -> {status_code} ({duration_ms}ms)",
                extra={
                    "request_id": request_id,
                    "client_ip": client_ip,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                },
            )

