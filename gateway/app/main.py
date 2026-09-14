"""
FastAPI Ingestion Gateway Application.

Production-grade entry point exposing:
- POST /api/v1/telemetry: Batch telemetry ingestion
- GET  /health: Service uptime and database connectivity check
- GET  /api/v1/telemetry/stats: Aggregated metrics (min/max/avg) per device
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import func, insert, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from gateway.app.config import get_settings
from gateway.app.database import close_database, get_db_session, get_engine, init_database
from gateway.app.models import (
    DeviceStatsResponse,
    HealthResponse,
    MetricStats,
    TelemetryBatchRequest,
    TelemetryIngestResponse,
    TelemetryRecord,
)
from gateway.app.security import (
    RateLimitingMiddleware,
    SlidingWindowRateLimiter,
    StructuredLoggingMiddleware,
    configure_structured_logging,
)

settings = get_settings()

# Configure JSON structured logging
configure_structured_logging(settings.LOG_LEVEL)

# Module-level tracking
_app_start_time: float = time.monotonic()
rate_limiter = SlidingWindowRateLimiter(
    max_requests=settings.RATE_LIMIT_REQUESTS,
    window_seconds=settings.RATE_LIMIT_WINDOW_SECONDS,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan context manager handling startup and shutdown hooks."""
    global _app_start_time
    _app_start_time = time.monotonic()

    # Startup: initialize database schema & hypertables
    engine = get_engine(settings)
    await init_database(engine)

    yield

    # Shutdown: gracefully close database connections
    await close_database()


app = FastAPI(
    title=settings.APP_NAME,
    description="Industrial IoT Telemetry Ingestion Engine with strict mTLS & TimescaleDB",
    version="1.0.0",
    lifespan=lifespan,
)

# Register custom middlewares (order: logging first, then rate limiter)
app.add_middleware(StructuredLoggingMiddleware)
app.add_middleware(RateLimitingMiddleware, limiter=rate_limiter)


# ------------------------------------------------------------------------------
# Exception Handlers
# ------------------------------------------------------------------------------
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Provide clean, structured error responses on payload validation failure."""
    return JSONResponse(
        status_code=getattr(
            status, "HTTP_422_UNPROCESSABLE_CONTENT", status.HTTP_422_UNPROCESSABLE_ENTITY
        ),
        content={
            "error": "Validation Error",
            "message": "The telemetry payload violates schema constraints.",
            "details": exc.errors(),
        },
    )


# ------------------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------------------
@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health & Readiness Check",
    tags=["System"],
)
async def health_check(
    db: AsyncSession = Depends(get_db_session),
) -> HealthResponse:
    """Verify application uptime and database connectivity."""
    db_status = "connected"
    try:
        # Perform active ping to verify database responsiveness
        await db.execute(text("SELECT 1;"))
    except Exception:
        db_status = "disconnected"

    uptime = round(time.monotonic() - _app_start_time, 2)
    overall_status = "healthy" if db_status == "connected" else "degraded"

    return HealthResponse(
        status=overall_status,
        database=db_status,
        uptime_seconds=uptime,
        server_time=datetime.now(UTC),
    )


@app.post(
    "/api/v1/telemetry",
    response_model=TelemetryIngestResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest Telemetry Batch",
    tags=["Telemetry"],
)
async def ingest_telemetry(
    payload: TelemetryBatchRequest,
    db: AsyncSession = Depends(get_db_session),
) -> TelemetryIngestResponse:
    """Ingest, validate, and bulk-persist a batch of telemetry readings."""
    if not payload.readings:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Batch payload cannot be empty.",
        )

    now_utc = datetime.now(UTC)
    records = [
        {
            "device_id": reading.device_id,
            "timestamp": reading.timestamp,
            "temperature": reading.temperature,
            "pressure": reading.pressure,
            "vibration": reading.vibration,
            "voltage": reading.voltage,
            "ingested_at": now_utc,
        }
        for reading in payload.readings
    ]

    try:
        await db.execute(insert(TelemetryRecord), records)
        await db.commit()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist telemetry batch: {str(exc)}",
        ) from exc

    return TelemetryIngestResponse(
        status="success",
        accepted_count=len(records),
        ingested_at=now_utc,
    )


@app.get(
    "/api/v1/telemetry/stats",
    response_model=list[DeviceStatsResponse],
    summary="Retrieve Aggregated Telemetry Statistics",
    tags=["Telemetry"],
)
async def get_telemetry_stats(
    device_id: str | None = Query(
        default=None,
        description="Optional filter for a specific device identifier",
    ),
    db: AsyncSession = Depends(get_db_session),
) -> list[DeviceStatsResponse]:
    """Calculate aggregated min/max/average statistics grouped by device."""
    query = select(
        TelemetryRecord.device_id,
        func.count().label("reading_count"),
        func.min(TelemetryRecord.temperature).label("min_temp"),
        func.max(TelemetryRecord.temperature).label("max_temp"),
        func.avg(TelemetryRecord.temperature).label("avg_temp"),
        func.min(TelemetryRecord.pressure).label("min_pressure"),
        func.max(TelemetryRecord.pressure).label("max_pressure"),
        func.avg(TelemetryRecord.pressure).label("avg_pressure"),
        func.min(TelemetryRecord.vibration).label("min_vib"),
        func.max(TelemetryRecord.vibration).label("max_vib"),
        func.avg(TelemetryRecord.vibration).label("avg_vib"),
        func.min(TelemetryRecord.voltage).label("min_volt"),
        func.max(TelemetryRecord.voltage).label("max_volt"),
        func.avg(TelemetryRecord.voltage).label("avg_volt"),
        func.min(TelemetryRecord.timestamp).label("first_ts"),
        func.max(TelemetryRecord.timestamp).label("last_ts"),
    ).group_by(TelemetryRecord.device_id)

    if device_id:
        query = query.where(TelemetryRecord.device_id == device_id)

    result = await db.execute(query)
    rows = result.fetchall()

    stats_list: list[DeviceStatsResponse] = []
    for r in rows:
        stats_list.append(
            DeviceStatsResponse(
                device_id=r.device_id,
                reading_count=int(r.reading_count),
                temperature=MetricStats(
                    min=round(float(r.min_temp), 2),
                    max=round(float(r.max_temp), 2),
                    avg=round(float(r.avg_temp), 2),
                ),
                pressure=MetricStats(
                    min=round(float(r.min_pressure), 3),
                    max=round(float(r.max_pressure), 3),
                    avg=round(float(r.avg_pressure), 3),
                ),
                vibration=MetricStats(
                    min=round(float(r.min_vib), 3),
                    max=round(float(r.max_vib), 3),
                    avg=round(float(r.avg_vib), 3),
                ),
                voltage=MetricStats(
                    min=round(float(r.min_volt), 2),
                    max=round(float(r.max_volt), 2),
                    avg=round(float(r.avg_volt), 2),
                ),
                first_timestamp=r.first_ts,
                last_timestamp=r.last_ts,
            )
        )

    return stats_list

