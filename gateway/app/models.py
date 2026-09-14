"""
Data Models and Pydantic V2 Schemas for Telemetry Ingestion.

Defines both the SQLAlchemy ORM layer (for TimescaleDB/PostgreSQL hypertable)
and strict Pydantic V2 validation schemas.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import DateTime, Float, Index, String, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ------------------------------------------------------------------------------
# SQLAlchemy 2.0 ORM Models
# ------------------------------------------------------------------------------
class Base(DeclarativeBase):
    """Base declarative class for ORM entities."""
    pass


class TelemetryRecord(Base):
    """
    SQLAlchemy ORM Model representing a telemetry reading.
    Designed for TimescaleDB Hypertables partitioned on 'timestamp'.
    """

    __tablename__ = "telemetry_readings"

    # Composite primary key consisting of (device_id, timestamp) for hypertable partitioning
    device_id: Mapped[str] = mapped_column(String(64), primary_key=True, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True, nullable=False
    )
    temperature: Mapped[float] = mapped_column(Float, nullable=False)
    pressure: Mapped[float] = mapped_column(Float, nullable=False)
    vibration: Mapped[float] = mapped_column(Float, nullable=False)
    voltage: Mapped[float] = mapped_column(Float, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        Index("idx_telemetry_device_time", "device_id", text("timestamp DESC")),
    )


# ------------------------------------------------------------------------------
# Pydantic V2 Validation Schemas
# ------------------------------------------------------------------------------
class TelemetryReadingSchema(BaseModel):
    """Strict validation schema for a single telemetry reading."""

    model_config = ConfigDict(extra="forbid")

    device_id: Annotated[
        str,
        Field(
            min_length=3,
            max_length=64,
            pattern=r"^[a-zA-Z0-9_\-\.]+$",
            description="Unique device hardware identifier",
            examples=["edge-sensor-01"],
        ),
    ]
    timestamp: Annotated[
        datetime,
        Field(description="UTC timestamp of the sensor measurement"),
    ]
    temperature: Annotated[
        float,
        Field(
            ge=-50.0,
            le=150.0,
            description="Temperature in degrees Celsius [-50.0, 150.0]",
            examples=[42.5],
        ),
    ]
    pressure: Annotated[
        float,
        Field(
            gt=0.0,
            le=1000.0,
            description="Pressure in bar (0.0, 1000.0]",
            examples=[6.2],
        ),
    ]
    vibration: Annotated[
        float,
        Field(
            ge=0.0,
            le=100.0,
            description="RMS Vibration in mm/s [0.0, 100.0]",
            examples=[2.34],
        ),
    ]
    voltage: Annotated[
        float,
        Field(
            gt=0.0,
            le=60.0,
            description="Power rail voltage in Volts (0.0, 60.0]",
            examples=[24.0],
        ),
    ]

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        """Ensure timestamp has timezone info and is not into the distant future."""
        if v.tzinfo is None:
            v = v.replace(tzinfo=UTC)
        else:
            v = v.astimezone(UTC)

        # Allow max 5 minutes clock skew into the future
        max_future = datetime.now(UTC) + timedelta(minutes=5)
        if v > max_future:
            raise ValueError(f"Timestamp {v} is too far in the future (exceeds 5 min skew)")
        return v


class TelemetryBatchRequest(BaseModel):
    """Schema for batch ingestion endpoint."""

    model_config = ConfigDict(extra="forbid")

    readings: Annotated[
        list[TelemetryReadingSchema],
        Field(
            min_length=1,
            max_length=500,
            description="Array of 1 to 500 telemetry readings",
        ),
    ]


class TelemetryIngestResponse(BaseModel):
    """Response returned upon successful telemetry ingestion."""

    status: str = "success"
    accepted_count: int
    ingested_at: datetime


class MetricStats(BaseModel):
    """Summary statistics for an individual sensor metric."""

    min: float
    max: float
    avg: float


class DeviceStatsResponse(BaseModel):
    """Aggregated telemetry metrics for a specific device."""

    device_id: str
    reading_count: int
    temperature: MetricStats
    pressure: MetricStats
    vibration: MetricStats
    voltage: MetricStats
    first_timestamp: datetime | None = None
    last_timestamp: datetime | None = None


class HealthResponse(BaseModel):
    """Service health and readiness check response."""

    status: str
    database: str
    uptime_seconds: float
    server_time: datetime

