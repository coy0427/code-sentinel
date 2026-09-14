"""
Unit and Integration Tests for Edge Agent & SQLite Spooler.

Tests:
1. Sensor emulation boundaries, physics dynamics, and anomaly generation.
2. SpoolingBuffer atomic enqueue, leasing, acknowledgement, and requeue logic.
3. Offline queueing and replay recovery upon network restoration.
4. Exponential backoff and jitter calculations.
5. Mutual TLS SSLContext initialization.
"""

import os
import ssl
from typing import Any

import httpx
import pytest

from edge_agent.client import EdgeAgentClient, SpoolingBuffer
from edge_agent.sensor_emulator import SensorEmulator


# ------------------------------------------------------------------------------
# 1. Sensor Emulator Tests
# ------------------------------------------------------------------------------
def test_sensor_emulator_reading_structure():
    """Verify generated reading adheres to physical fields and valid bounds."""
    emulator = SensorEmulator(device_id="pump-motor-01")
    reading = emulator.generate_reading()

    assert reading["device_id"] == "pump-motor-01"
    assert "timestamp" in reading
    assert -50.0 <= reading["temperature"] <= 150.0
    assert 0.0 < reading["pressure"] <= 100.0
    assert 0.0 <= reading["vibration"] <= 100.0
    assert 1.0 <= reading["voltage"] <= 60.0


def test_sensor_emulator_batch():
    """Verify batch reading generation produces sequential items."""
    emulator = SensorEmulator(device_id="sensor-batch-01")
    batch = emulator.generate_batch(count=5)

    assert len(batch) == 5
    for item in batch:
        assert item["device_id"] == "sensor-batch-01"


def test_sensor_emulator_anomaly_injection():
    """Verify anomaly generation executes without crashing and alters readings."""
    emulator = SensorEmulator(device_id="anomaly-sensor", anomaly_probability=1.0)
    reading = emulator.generate_reading()
    assert reading["device_id"] == "anomaly-sensor"
    assert isinstance(reading["temperature"], float)


# ------------------------------------------------------------------------------
# 2. SQLite Spooling Buffer Tests
# ------------------------------------------------------------------------------
def test_spooling_buffer_enqueue_and_lease():
    """Test enqueueing items, leasing batches, and state transitions."""
    spooler = SpoolingBuffer(db_path=":memory:")
    items = [
        {"device_id": "sensor-1", "temperature": 40.0 + i}
        for i in range(5)
    ]

    # Enqueue
    inserted = spooler.enqueue(items)
    assert inserted == 5
    assert spooler.count() == 5
    assert spooler.count(status="PENDING") == 5

    # Lease batch of 3
    leased = spooler.lease_batch(limit=3)
    assert len(leased) == 3
    assert spooler.count(status="SENDING") == 3
    assert spooler.count(status="PENDING") == 2

    # Acknowledge the 3 leased items
    leased_ids = [item[0] for item in leased]
    acknowledged = spooler.acknowledge(leased_ids)
    assert acknowledged == 3
    assert spooler.count() == 2

    # Lease remaining 2 items
    remaining = spooler.lease_batch(limit=10)
    assert len(remaining) == 2
    rem_ids = [item[0] for item in remaining]
    spooler.acknowledge(rem_ids)
    assert spooler.count() == 0


def test_spooling_buffer_requeue_on_failure():
    """Verify failed transmissions return items to PENDING with incremented retry count."""
    spooler = SpoolingBuffer(db_path=":memory:")
    spooler.enqueue([{"device_id": "sensor-1", "val": 100}])

    leased = spooler.lease_batch(limit=1)
    record_id, _ = leased[0]
    assert spooler.count(status="SENDING") == 1

    # Simulate transmission failure -> Requeue
    spooler.requeue_failed([record_id])
    assert spooler.count(status="PENDING") == 1
    assert spooler.count(status="SENDING") == 0

    # Verify retry count incremented
    conn = spooler._get_connection()
    cursor = conn.execute("SELECT retry_count FROM telemetry_spool WHERE id = ?", (record_id,))
    row = cursor.fetchone()
    assert row[0] == 1


def test_spooling_buffer_file_persistence(tmp_path):
    """Verify SQLite persistence across connection close and re-open."""
    db_file = str(tmp_path / "test_spool.db")
    spooler1 = SpoolingBuffer(db_path=db_file)
    spooler1.enqueue([{"device_id": "sensor-persist", "temp": 55.5}])
    spooler1.close()

    # Re-open with new instance
    spooler2 = SpoolingBuffer(db_path=db_file)
    assert spooler2.count() == 1
    leased = spooler2.lease_batch(limit=10)
    assert len(leased) == 1
    assert leased[0][1]["device_id"] == "sensor-persist"
    spooler2.acknowledge([leased[0][0]])
    assert spooler2.count() == 0
    spooler2.close()


# ------------------------------------------------------------------------------
# 3. Offline Buffering & Replay Recovery Tests
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_edge_agent_offline_buffering_and_recovery():
    """
    Test edge scenario:
    1. Gateway is offline (returns HTTP 503).
    2. Agent attempts flush -> fails -> records remain safely buffered in SQLite.
    3. Gateway recovers (returns HTTP 201).
    4. Agent flushes -> all buffered telemetry is transmitted with zero loss.
    """
    gateway_online = False
    received_payloads: list[dict[str, Any]] = []

    # Mock handler simulating gateway outage and recovery
    def mock_handler(request: httpx.Request) -> httpx.Response:
        nonlocal gateway_online, received_payloads
        if not gateway_online:
            return httpx.Response(status_code=503, content=b"Gateway Offline")

        import json
        body = json.loads(request.content.decode("utf-8"))
        received_payloads.extend(body["readings"])
        return httpx.Response(
            status_code=201,
            json={"status": "success", "accepted_count": len(body["readings"])},
        )

    transport = httpx.MockTransport(mock_handler)
    mock_client = httpx.AsyncClient(transport=transport)

    spooler = SpoolingBuffer(db_path=":memory:")
    agent = EdgeAgentClient(
        gateway_url="https://gateway.mock:8443",
        spooler=spooler,
        http_client=mock_client,
        base_backoff=0.01,
        max_backoff=0.05,
        jitter=False,
    )

    # 1. Enqueue 10 sensor readings while offline
    test_readings = [
        {"device_id": "edge-01", "temperature": 45.0 + i, "timestamp": f"2026-09-13T12:00:{i:02d}Z"}
        for i in range(10)
    ]
    spooler.enqueue(test_readings)
    assert spooler.count(status="PENDING") == 10

    # 2. Flush while gateway is offline
    gateway_online = False
    flushed_count = await agent.flush_spool()
    assert flushed_count == 0
    # Readings must still exist in the queue
    assert spooler.count() == 10
    assert len(received_payloads) == 0

    # 3. Restore gateway online
    gateway_online = True
    flushed_count = await agent.flush_spool()
    assert flushed_count == 10
    # Queue must now be empty
    assert spooler.count() == 0
    # All readings were received
    assert len(received_payloads) == 10
    assert received_payloads[0]["device_id"] == "edge-01"

    await mock_client.aclose()


# ------------------------------------------------------------------------------
# 4. Exponential Backoff & mTLS Context Tests
# ------------------------------------------------------------------------------
def test_exponential_backoff_calculation():
    """Verify exponential backoff calculation increases and caps at max_backoff."""
    agent = EdgeAgentClient(
        base_backoff=1.0,
        max_backoff=8.0,
        jitter=False,
    )

    d1 = agent._compute_next_backoff()
    assert d1 == 1.0
    d2 = agent._compute_next_backoff()
    assert d2 == 2.0
    d3 = agent._compute_next_backoff()
    assert d3 == 4.0
    d4 = agent._compute_next_backoff()
    assert d4 == 8.0
    d5 = agent._compute_next_backoff()
    assert d5 == 8.0  # Capped at max_backoff

    agent._reset_backoff()
    assert agent._current_backoff == 1.0


def test_mtls_ssl_context_creation():
    """Verify mTLS SSL context loads generated test certs correctly."""
    ca_cert = "certs/ca.crt"
    client_cert = "certs/client.crt"
    client_key = "certs/client.key"

    if not os.path.exists(ca_cert):
        pytest.skip("Test certificates not generated; skipping SSL context test")

    agent = EdgeAgentClient(
        ca_cert_path=ca_cert,
        client_cert_path=client_cert,
        client_key_path=client_key,
    )
    ssl_ctx = agent.create_ssl_context()
    assert ssl_ctx is not None
    assert ssl_ctx.verify_mode == ssl.CERT_REQUIRED

