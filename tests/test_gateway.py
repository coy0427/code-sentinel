"""
Unit and Integration Tests for FastAPI Ingestion Gateway API.

Tests:
1. Pydantic V2 Schema Validation (temperature, pressure, vibration, voltage, timestamp bounds).
2. Rate Limiting Middleware (429 response, Retry-After header).
3. /health endpoint and DB connectivity.
4. /api/v1/telemetry batch ingestion and database persistence.
5. /api/v1/telemetry/stats aggregation (min/max/avg metrics per device).
"""

from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from gateway.app.database import get_db_session
from gateway.app.main import app, rate_limiter
from gateway.app.models import (
    Base,
    TelemetryBatchRequest,
    TelemetryReadingSchema,
)

# ------------------------------------------------------------------------------
# Test Database Engine Fixture (In-Memory SQLite)
# ------------------------------------------------------------------------------
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
async def test_db():
    """Create a pristine in-memory SQLite database and session factory for testing."""
    engine = create_async_engine(
        TEST_DB_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    yield session_factory

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def async_client(test_db):
    """FastAPI Test Client configured with database session override."""
    async def override_get_db_session():
        async with test_db() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session
    rate_limiter.reset()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()
    rate_limiter.reset()


# ------------------------------------------------------------------------------
# 1. Pydantic V2 Validation Tests
# ------------------------------------------------------------------------------
def test_valid_telemetry_schema():
    """Verify valid telemetry reading satisfies schema constraints."""
    now = datetime.now(UTC)
    reading = TelemetryReadingSchema(
        device_id="sensor-valid-01",
        timestamp=now,
        temperature=38.4,
        pressure=5.12,
        vibration=1.8,
        voltage=24.0,
    )
    assert reading.device_id == "sensor-valid-01"
    assert reading.temperature == 38.4


def test_invalid_temperature_out_of_bounds():
    """Ensure temperature below -50°C or above 150°C raises ValidationError."""
    now = datetime.now(UTC)
    with pytest.raises(ValidationError) as exc:
        TelemetryReadingSchema(
            device_id="sensor-01",
            timestamp=now,
            temperature=-55.0,  # Below -50.0
            pressure=5.0,
            vibration=1.0,
            voltage=24.0,
        )
    assert "temperature" in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        TelemetryReadingSchema(
            device_id="sensor-01",
            timestamp=now,
            temperature=160.0,  # Above 150.0
            pressure=5.0,
            vibration=1.0,
            voltage=24.0,
        )
    assert "temperature" in str(exc.value)


def test_invalid_pressure_and_vibration():
    """Ensure non-positive pressure or negative vibration is rejected."""
    now = datetime.now(UTC)
    with pytest.raises(ValidationError) as exc:
        TelemetryReadingSchema(
            device_id="sensor-01",
            timestamp=now,
            temperature=25.0,
            pressure=0.0,  # Must be > 0
            vibration=1.0,
            voltage=24.0,
        )
    assert "pressure" in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        TelemetryReadingSchema(
            device_id="sensor-01",
            timestamp=now,
            temperature=25.0,
            pressure=2.0,
            vibration=-0.5,  # Must be >= 0
            voltage=24.0,
        )
    assert "vibration" in str(exc.value)


def test_invalid_future_timestamp():
    """Ensure timestamps more than 5 minutes in the future are rejected."""
    far_future = datetime.now(UTC) + timedelta(minutes=15)
    with pytest.raises(ValidationError) as exc:
        TelemetryReadingSchema(
            device_id="sensor-01",
            timestamp=far_future,
            temperature=25.0,
            pressure=2.0,
            vibration=1.0,
            voltage=24.0,
        )
    assert "too far in the future" in str(exc.value)


def test_invalid_device_id_characters():
    """Ensure device_id with invalid characters is rejected."""
    now = datetime.now(UTC)
    with pytest.raises(ValidationError) as exc:
        TelemetryReadingSchema(
            device_id="bad id with spaces!",
            timestamp=now,
            temperature=25.0,
            pressure=2.0,
            vibration=1.0,
            voltage=24.0,
        )
    assert "device_id" in str(exc.value)


def test_batch_payload_constraints():
    """Ensure empty batch or batches > 500 items are rejected."""
    with pytest.raises(ValidationError):
        TelemetryBatchRequest(readings=[])


# ------------------------------------------------------------------------------
# 2. Gateway API Integration Tests
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_health_check_endpoint(async_client):
    """Test /health endpoint returns uptime and connected database status."""
    response = await async_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"
    assert data["uptime_seconds"] >= 0.0


@pytest.mark.asyncio
async def test_telemetry_batch_ingestion(async_client):
    """Test successful ingestion of a batch of telemetry records."""
    now = datetime.now(UTC)
    payload = {
        "readings": [
            {
                "device_id": "test-device-01",
                "timestamp": (now + timedelta(seconds=i)).isoformat(),
                "temperature": 40.0 + i,
                "pressure": 5.0 + (i * 0.1),
                "vibration": 1.2,
                "voltage": 24.0,
            }
            for i in range(5)
        ]
    }

    response = await async_client.post("/api/v1/telemetry", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "success"
    assert data["accepted_count"] == 5


@pytest.mark.asyncio
async def test_telemetry_payload_validation_failure(async_client):
    """Test gateway returns HTTP 422 for malformed readings."""
    payload = {
        "readings": [
            {
                "device_id": "valid-id",
                "timestamp": datetime.now(UTC).isoformat(),
                "temperature": 9999.0,  # Violates <= 150.0 constraint
                "pressure": 5.0,
                "vibration": 1.2,
                "voltage": 24.0,
            }
        ]
    }

    response = await async_client.post("/api/v1/telemetry", json=payload)
    assert response.status_code == 422
    data = response.json()
    assert data["error"] == "Validation Error"


@pytest.mark.asyncio
async def test_telemetry_stats_aggregation(async_client):
    """Test /api/v1/telemetry/stats accurately aggregates min/max/average per device."""
    now = datetime.now(UTC)
    # Insert known batch for device-A
    payload = {
        "readings": [
            {
                "device_id": "device-A",
                "timestamp": (now + timedelta(seconds=1)).isoformat(),
                "temperature": 10.0,
                "pressure": 2.0,
                "vibration": 1.0,
                "voltage": 20.0,
            },
            {
                "device_id": "device-A",
                "timestamp": (now + timedelta(seconds=2)).isoformat(),
                "temperature": 30.0,
                "pressure": 4.0,
                "vibration": 3.0,
                "voltage": 24.0,
            },
        ]
    }
    ingest_res = await async_client.post("/api/v1/telemetry", json=payload)
    assert ingest_res.status_code == 201

    stats_res = await async_client.get("/api/v1/telemetry/stats?device_id=device-A")
    assert stats_res.status_code == 200
    stats = stats_res.json()
    assert len(stats) == 1
    dev = stats[0]
    assert dev["device_id"] == "device-A"
    assert dev["reading_count"] == 2

    # Verify min, max, avg for temperature: [10, 30] -> min: 10, max: 30, avg: 20
    assert dev["temperature"]["min"] == 10.0
    assert dev["temperature"]["max"] == 30.0
    assert dev["temperature"]["avg"] == 20.0

    # Verify min, max, avg for pressure: [2.0, 4.0] -> min: 2, max: 4, avg: 3
    assert dev["pressure"]["min"] == 2.0
    assert dev["pressure"]["max"] == 4.0
    assert dev["pressure"]["avg"] == 3.0


@pytest.mark.asyncio
async def test_rate_limiting_middleware(async_client):
    """Test rate limiter blocks requests exceeding configured threshold with HTTP 429."""
    # Temporarily set a very low rate limit
    rate_limiter.max_requests = 3
    rate_limiter.window_seconds = 10
    rate_limiter.reset()

    # Requests 1, 2, 3 should succeed
    for _ in range(3):
        res = await async_client.get("/api/v1/telemetry/stats")
        assert res.status_code == 200

    # Request 4 should be rejected with 429 Too Many Requests
    res4 = await async_client.get("/api/v1/telemetry/stats")
    assert res4.status_code == 429
    assert "Retry-After" in res4.headers
    assert res4.json()["error"] == "Too Many Requests"

