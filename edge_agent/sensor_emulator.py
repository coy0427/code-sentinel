"""
Industrial Edge Sensor Emulator.

Simulates realistic physical telemetry readings for industrial machinery
(e.g., pumps, compressors, bearings) with sinusoidal cycles and stochastic noise.
"""

from __future__ import annotations

import math
import random
import time
from datetime import UTC, datetime
from typing import Any


class SensorEmulator:
    """Simulates an industrial edge IoT sensor with realistic physical dynamics."""

    def __init__(
        self,
        device_id: str = "edge-sensor-01",
        base_temp: float = 45.0,
        base_pressure: float = 6.2,
        base_vibration: float = 2.5,
        base_voltage: float = 24.0,
        anomaly_probability: float = 0.0,
    ) -> None:
        """
        Initialize the sensor emulator.

        :param device_id: Unique hardware identifier.
        :param base_temp: Nominal temperature in degrees Celsius.
        :param base_pressure: Nominal pressure in bar.
        :param base_vibration: Nominal RMS vibration in mm/s.
        :param base_voltage: Nominal power rail voltage in Volts.
        :param anomaly_probability: Probability of introducing an industrial transient spike.
        """
        self.device_id = device_id
        self.base_temp = base_temp
        self.base_pressure = base_pressure
        self.base_vibration = base_vibration
        self.base_voltage = base_voltage
        self.anomaly_probability = anomaly_probability
        self._step = 0

    def generate_reading(self, timestamp: datetime | None = None) -> dict[str, Any]:
        """
        Generate a single telemetry reading with physically realistic values.

        :param timestamp: Optional explicit timestamp. Defaults to current UTC time.
        :return: Telemetry payload dictionary.
        """
        if timestamp is None:
            now = datetime.now(UTC)
        else:
            now = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=UTC)

        self._step += 1
        t = self._step * 0.1

        # Sinusoidal thermal cycle with slight Gaussian drift
        temp_wave = math.sin(t * 0.05) * 4.0
        temp_noise = random.gauss(0.0, 0.3)
        temperature = round(self.base_temp + temp_wave + temp_noise, 2)
        # Bounded between -50°C and 150°C
        temperature = max(-50.0, min(150.0, temperature))

        # Pressure dynamics with cycle and rapid turbulence
        pressure_wave = math.cos(t * 0.1) * 0.5
        pressure_noise = random.gauss(0.0, 0.05)
        pressure = round(self.base_pressure + pressure_wave + pressure_noise, 3)
        pressure = max(0.1, min(100.0, pressure))

        # Vibration: Mechanical harmonics + random noise
        vib_harmonic = abs(math.sin(t * 0.8)) * 1.2
        vib_noise = abs(random.gauss(0.0, 0.2))
        vibration = round(self.base_vibration + vib_harmonic + vib_noise, 3)
        vibration = max(0.0, min(100.0, vibration))

        # Voltage: DC power supply with micro-ripple
        volt_ripple = math.sin(t * 2.0) * 0.15
        volt_noise = random.gauss(0.0, 0.02)
        voltage = round(self.base_voltage + volt_ripple + volt_noise, 2)
        voltage = max(1.0, min(60.0, voltage))

        # Inject anomaly if triggered
        if self.anomaly_probability > 0 and random.random() < self.anomaly_probability:
            anomaly_type = random.choice(["temp_spike", "vib_spike", "pressure_drop"])
            if anomaly_type == "temp_spike":
                temperature += 25.0
            elif anomaly_type == "vib_spike":
                vibration += 8.0
            elif anomaly_type == "pressure_drop":
                pressure = max(0.1, pressure - 3.0)

        return {
            "device_id": self.device_id,
            "timestamp": now.isoformat(),
            "temperature": temperature,
            "pressure": pressure,
            "vibration": vibration,
            "voltage": voltage,
        }

    def generate_batch(self, count: int) -> list[dict[str, Any]]:
        """Generate a sequential batch of simulated telemetry readings."""
        readings: list[dict[str, Any]] = []
        base_time = time.time()
        for i in range(count):
            reading_time = datetime.fromtimestamp(base_time + i, tz=UTC)
            readings.append(self.generate_reading(timestamp=reading_time))
        return readings

