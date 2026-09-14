"""
Database Engine, Session Management, and TimescaleDB Hypertable Setup.

Supports PostgreSQL and TimescaleDB with asyncpg driver, connection pooling,
and graceful schema migrations for partitioned time-series storage.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from gateway.app.config import GatewaySettings, get_settings
from gateway.app.models import Base

logger = logging.getLogger("gateway.database")

# Global engine and session factory references
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine(settings: GatewaySettings | None = None) -> AsyncEngine:
    """Retrieve or initialize the singleton SQLAlchemy async engine."""
    global _engine
    if _engine is None:
        cfg = settings or get_settings()
        engine_kwargs = {
            "echo": cfg.DEBUG,
            "future": True,
        }
        # SQLite in tests doesn't support pool_size / max_overflow
        if not cfg.DATABASE_URL.startswith("sqlite"):
            engine_kwargs.update(
                {
                    "pool_size": cfg.DB_POOL_SIZE,
                    "max_overflow": cfg.DB_MAX_OVERFLOW,
                    "pool_timeout": cfg.DB_POOL_TIMEOUT,
                    "pool_pre_ping": True,
                }
            )
        _engine = create_async_engine(cfg.DATABASE_URL, **engine_kwargs)
    return _engine


def get_session_factory(
    settings: GatewaySettings | None = None,
) -> async_sessionmaker[AsyncSession]:
    """Retrieve or initialize the async sessionmaker factory."""
    global _session_factory
    if _session_factory is None:
        engine = get_engine(settings)
        _session_factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency yielding an async database session per request
    with automatic rollback on error.
    """
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_database(engine: AsyncEngine) -> None:
    """
    Initialize relational schema and TimescaleDB hypertables.
    Idempotent: safe to run repeatedly on startup.
    """
    logger.info("Verifying and initializing database schema...")
    async with engine.begin() as conn:
        # Create base tables defined in models
        await conn.run_sync(Base.metadata.create_all)

        # Attempt to convert telemetry_readings into a TimescaleDB hypertable
        if "postgresql" in str(engine.url):
            try:
                # Enable TimescaleDB extension if not already present
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                # Partition by timestamp column
                await conn.execute(
                    text(
                        """
                        SELECT create_hypertable(
                            'telemetry_readings',
                            'timestamp',
                            if_not_exists => TRUE,
                            migrate_data => TRUE
                        );
                        """
                    )
                )
                logger.info("TimescaleDB hypertable configured for 'telemetry_readings'")
            except Exception as exc:
                logger.warning(
                    "TimescaleDB hypertable setup skipped or unsupported: %s. "
                    "Operating with standard PostgreSQL schema.",
                    exc,
                )
    logger.info("Database schema initialization completed.")


async def close_database() -> None:
    """Dispose of the database connection pool cleanly."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("Database engine connections cleanly closed.")

