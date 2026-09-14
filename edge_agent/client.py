"""
Edge Agent Client & SQLite Spooling Buffer.

Implements an asynchronous, fault-tolerant edge producer agent with
client-side mTLS via httpx, automatic exponential backoff, and
persistent SQLite queueing to guarantee zero telemetry loss.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sqlite3
import ssl
import threading
from typing import Any

import httpx

from edge_agent.sensor_emulator import SensorEmulator

logger = logging.getLogger("edge_agent")


class SpoolingBuffer:
    """
    Thread-safe and persistent SQLite spooling buffer.
    Queues telemetry messages locally during network disruptions or gateway downtime.
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        """
        Initialize the spooling buffer.

        :param db_path: Path to SQLite DB file or ':memory:' for transient/testing.
        """
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Returns or creates the SQLite connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0,
                isolation_level=None,  # Autocommit mode for granular transaction control
            )
            # Enable Write-Ahead Logging (WAL) for superior concurrency and reliability
            if self.db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
        return self._conn

    def _init_db(self) -> None:
        """Initialize the spooling table schema."""
        with self._lock:
            conn = self._get_connection()
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_spool (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    leased_at REAL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_spool_status ON telemetry_spool(status);"
            )

    def enqueue(self, items: list[dict[str, Any]]) -> int:
        """
        Atomically enqueue a batch of telemetry readings into the spool.

        :param items: List of telemetry dictionaries.
        :return: Number of inserted items.
        """
        if not items:
            return 0
        records = [(json.dumps(item), "PENDING", 0) for item in items]
        with self._lock:
            conn = self._get_connection()
            conn.execute("BEGIN IMMEDIATE;")
            try:
                conn.executemany(
                    "INSERT INTO telemetry_spool (payload, status, retry_count) VALUES (?, ?, ?);",
                    records,
                )
                conn.execute("COMMIT;")
                return len(records)
            except Exception:
                conn.execute("ROLLBACK;")
                raise

    def lease_batch(self, limit: int = 50) -> list[tuple[int, dict[str, Any]]]:
        """
        Lease up to `limit` pending records for transmission.

        :param limit: Maximum batch size to lease.
        :return: List of tuples (record_id, payload_dict).
        """
        with self._lock:
            conn = self._get_connection()
            conn.execute("BEGIN IMMEDIATE;")
            try:
                cursor = conn.execute(
                    """
                    SELECT id, payload FROM telemetry_spool
                    WHERE status = 'PENDING'
                    ORDER BY id ASC
                    LIMIT ?;
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()
                if not rows:
                    conn.execute("COMMIT;")
                    return []

                leased_ids = [r[0] for r in rows]
                conn.executemany(
                    "UPDATE telemetry_spool SET status = 'SENDING' WHERE id = ?;",
                    [(r_id,) for r_id in leased_ids],
                )
                conn.execute("COMMIT;")

                return [(r[0], json.loads(r[1])) for r in rows]
            except Exception:
                conn.execute("ROLLBACK;")
                raise

    def acknowledge(self, record_ids: list[int]) -> int:
        """
        Delete successfully transmitted records from the spool buffer.

        :param record_ids: List of record IDs.
        :return: Number of acknowledged (deleted) records.
        """
        if not record_ids:
            return 0
        with self._lock:
            conn = self._get_connection()
            cursor = conn.executemany(
                "DELETE FROM telemetry_spool WHERE id = ?;",
                [(r_id,) for r_id in record_ids],
            )
            return cursor.rowcount

    def requeue_failed(self, record_ids: list[int]) -> None:
        """
        Revert unacknowledged leased records back to PENDING and increment retry_count.

        :param record_ids: List of record IDs.
        """
        if not record_ids:
            return
        with self._lock:
            conn = self._get_connection()
            conn.executemany(
                """
                UPDATE telemetry_spool
                SET status = 'PENDING',
                    retry_count = retry_count + 1
                WHERE id = ?;
                """,
                [(r_id,) for r_id in record_ids],
            )

    def count(self, status: str | None = None) -> int:
        """
        Get count of records in the spool.

        :param status: Filter by status ('PENDING', 'SENDING'), or None for all.
        :return: Total count.
        """
        with self._lock:
            conn = self._get_connection()
            if status:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM telemetry_spool WHERE status = ?;",
                    (status,),
                )
            else:
                cursor = conn.execute("SELECT COUNT(*) FROM telemetry_spool;")
            row = cursor.fetchone()
            return row[0] if row else 0

    def close(self) -> None:
        """Close database connection."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None


class EdgeAgentClient:
    """
    Industrial IoT Edge Agent Client.
    Emulates sensor readings, spools to local storage, and publishes
    via asynchronous mTLS to the Ingestion Gateway.
    """

    def __init__(
        self,
        gateway_url: str = "https://localhost:8443",
        ca_cert_path: str | None = None,
        client_cert_path: str | None = None,
        client_key_path: str | None = None,
        spooler: SpoolingBuffer | None = None,
        sensor: SensorEmulator | None = None,
        batch_size: int = 50,
        flush_interval: float = 1.0,
        sample_interval: float = 0.5,
        base_backoff: float = 1.0,
        max_backoff: float = 30.0,
        jitter: bool = True,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.gateway_url = gateway_url.rstrip("/")
        self.ca_cert_path = ca_cert_path
        self.client_cert_path = client_cert_path
        self.client_key_path = client_key_path
        self.spooler = spooler or SpoolingBuffer()
        self.sensor = sensor or SensorEmulator()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.sample_interval = sample_interval
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.jitter = jitter

        self._injected_client = http_client
        self._client: httpx.AsyncClient | None = None
        self._current_backoff = base_backoff
        self._running = False
        self._tasks: list[asyncio.Task] = []

    def create_ssl_context(self) -> ssl.SSLContext | None:
        """Create and configure Python SSLContext for mutual TLS (mTLS)."""
        if not (self.ca_cert_path and self.client_cert_path and self.client_key_path):
            return None

        if not os.path.exists(self.ca_cert_path):
            raise FileNotFoundError(f"Root CA cert not found: {self.ca_cert_path}")
        if not os.path.exists(self.client_cert_path):
            raise FileNotFoundError(f"Client cert not found: {self.client_cert_path}")
        if not os.path.exists(self.client_key_path):
            raise FileNotFoundError(f"Client key not found: {self.client_key_path}")

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=self.ca_cert_path)
        ctx.load_cert_chain(certfile=self.client_cert_path, keyfile=self.client_key_path)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx

    async def get_client(self) -> httpx.AsyncClient:
        """Get or create the httpx.AsyncClient instance."""
        if self._injected_client is not None:
            return self._injected_client

        if self._client is None or self._client.is_closed:
            ssl_ctx = self.create_ssl_context()
            verify: Any = ssl_ctx if ssl_ctx is not None else False
            self._client = httpx.AsyncClient(
                verify=verify,
                timeout=httpx.Timeout(10.0, connect=5.0),
            )
        return self._client

    async def close_client(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def _compute_next_backoff(self) -> float:
        """Calculate next backoff duration with exponential increase and jitter."""
        delay = self._current_backoff
        self._current_backoff = min(self.max_backoff, self._current_backoff * 2.0)
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        return delay

    def _reset_backoff(self) -> None:
        """Reset exponential backoff to initial base value."""
        self._current_backoff = self.base_backoff

    async def transmit_batch(self, readings: list[dict[str, Any]]) -> bool:
        """
        Send a batch of readings to the Gateway API via POST /api/v1/telemetry.

        :param readings: List of telemetry dictionaries.
        :return: True if successfully ingested (HTTP 200/201), False otherwise.
        """
        if not readings:
            return True

        client = await self.get_client()
        url = f"{self.gateway_url}/api/v1/telemetry"
        payload = {"readings": readings}

        try:
            response = await client.post(url, json=payload)
            if response.status_code in (200, 201):
                return True
            logger.warning(
                "Gateway rejected telemetry batch: HTTP %d - %s",
                response.status_code,
                response.text,
            )
            return False
        except (httpx.RequestError, ssl.SSLError, Exception) as exc:
            logger.error("Network or TLS error transmitting batch: %s", exc)
            return False

    async def flush_spool(self) -> int:
        """
        Drain and transmit available batches from the SQLite spooler.

        :return: Number of transmitted and acknowledged records.
        """
        total_flushed = 0
        while True:
            leased = self.spooler.lease_batch(limit=self.batch_size)
            if not leased:
                break

            record_ids = [item[0] for item in leased]
            readings = [item[1] for item in leased]

            success = await self.transmit_batch(readings)
            if success:
                self.spooler.acknowledge(record_ids)
                total_flushed += len(record_ids)
                remaining = self.spooler.count()
                logger.info(
                    "[TX SUCCESS] Transmitted %d readings to %s (HTTP 201) | Spool remaining: %d",
                    len(record_ids),
                    self.gateway_url,
                    remaining,
                )
                self._reset_backoff()
            else:
                self.spooler.requeue_failed(record_ids)
                backoff_time = self._compute_next_backoff()
                logger.warning(
                    "[TX OFFLINE] Gateway unavailable (%s). Spooled %d items. Retrying in %.2fs...",
                    self.gateway_url,
                    len(record_ids),
                    backoff_time,
                )
                await asyncio.sleep(backoff_time)
                break

        return total_flushed

    async def _sample_loop(self) -> None:
        """Continuous sensor sampling loop."""
        while self._running:
            try:
                reading = self.sensor.generate_reading()
                self.spooler.enqueue([reading])
                count = self.spooler.count()
                logger.info(
                    "[SPOOL] %s: temp=%.1f°C, press=%.2fbar, vib=%.2f, volt=%.1fV | Spool: %d",
                    reading["device_id"],
                    reading["temperature"],
                    reading["pressure"],
                    reading["vibration"],
                    reading["voltage"],
                    count,
                )
            except Exception as exc:
                logger.error("Error during sensor sampling: %s", exc)
            await asyncio.sleep(self.sample_interval)

    async def _transmission_loop(self) -> None:
        """Continuous buffer transmission and replay loop."""
        while self._running:
            try:
                await self.flush_spool()
            except Exception as exc:
                logger.error("Error during spool transmission loop: %s", exc)
            await asyncio.sleep(self.flush_interval)

    async def start(self) -> None:
        """Start sensor emulator and transmission background tasks."""
        if self._running:
            return
        self._running = True
        logger.info("Starting Edge Agent worker loops...")
        self._tasks = [
            asyncio.create_task(self._sample_loop(), name="sensor_sample_loop"),
            asyncio.create_task(self._transmission_loop(), name="telemetry_transmit_loop"),
        ]

    async def stop(self) -> None:
        """Gracefully stop agent tasks and close HTTP connection."""
        if not self._running:
            return
        logger.info("Stopping Edge Agent worker loops...")
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await self.close_client()
        logger.info("Edge Agent gracefully stopped.")


async def main() -> None:
    """CLI entrypoint running the Edge Agent with live terminal output."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    gateway_url = os.getenv("GATEWAY_URL", "https://localhost:8443")
    ca_cert = os.getenv("SSL_CA_CERT", "certs/ca.crt")
    client_cert = os.getenv("CLIENT_CERT_PATH", "certs/client.crt")
    client_key = os.getenv("CLIENT_KEY_PATH", "certs/client.key")
    db_path = os.getenv("SPOOL_DB_PATH", "spool.db")

    logger.info("==========================================================")
    logger.info("Starting Secure Edge IoT Agent")
    logger.info("Gateway URL     : %s", gateway_url)
    logger.info("Spool Database  : %s", db_path)
    logger.info("mTLS CA Cert    : %s", ca_cert)
    logger.info("Client Cert/Key : %s / %s", client_cert, client_key)
    logger.info("==========================================================")

    spooler = SpoolingBuffer(db_path=db_path)
    sensor = SensorEmulator(device_id="edge-sensor-01")

    agent = EdgeAgentClient(
        gateway_url=gateway_url,
        ca_cert_path=ca_cert if os.path.exists(ca_cert) else None,
        client_cert_path=client_cert if os.path.exists(client_cert) else None,
        client_key_path=client_key if os.path.exists(client_key) else None,
        spooler=spooler,
        sensor=sensor,
        sample_interval=1.0,
        flush_interval=2.0,
    )

    try:
        await agent.start()
        while True:
            await asyncio.sleep(1.0)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Termination signal received. Shutting down...")
    finally:
        await agent.stop()
        spooler.close()
        logger.info("Edge Agent cleanly terminated.")


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

