"""
Gateway Configuration & Environment Settings.

Uses Pydantic Settings for strictly typed, environment-driven configuration.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewaySettings(BaseSettings):
    """Application settings with environment variable support."""

    APP_NAME: str = "Secure Edge IoT Gateway"
    APP_ENV: str = "development"
    DEBUG: bool = False

    # Server Network Binding
    HOST: str = "0.0.0.0"
    PORT: int = 8443

    # Database Configuration (TimescaleDB / PostgreSQL)
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/telemetry_db",
        description="Async database connection string",
    )
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10
    DB_POOL_TIMEOUT: float = 30.0

    # mTLS Security Paths
    SSL_CA_CERT: str = "certs/ca.crt"
    SSL_SERVER_CERT: str = "certs/server.crt"
    SSL_SERVER_KEY: str = "certs/server.key"
    STRICT_MTLS: bool = True

    # Rate Limiting Configuration
    RATE_LIMIT_REQUESTS: int = 120  # Max requests per window
    RATE_LIMIT_WINDOW_SECONDS: int = 60

    # Ingestion Batch Limits
    MAX_BATCH_SIZE: int = 500

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> GatewaySettings:
    """Return cached instance of GatewaySettings."""
    return GatewaySettings()

