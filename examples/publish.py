"""publish.py — publish simulated temperature sensor readings to a DataNet channel.

Generates synthetic readings every PUBLISH_INTERVAL seconds and publishes them
as structured JSON payloads.  Demonstrates both async and sync publish patterns.

Usage
-----
::

    python examples/publish.py

Configuration via environment variables:

    DATANET_API_KEY      Your DataNet API key          (default: ak_dev_12345)
    DATANET_CHANNEL      Target channel                (default: project.mall-install.sensor)
    DATANET_API_URL      REST API base URL             (default: https://api.datanet.art)
    DATANET_WS_URL       WebSocket base URL            (default: wss://ws.datanet.art)
    DATANET_INTERVAL     Seconds between readings      (default: 2)
    DATANET_COUNT        Number of readings to publish (default: 0 = unlimited)
    DATANET_PATTERN      "async" or "sync"             (default: async)
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import signal
import sys
import time
from typing import Any

# Allow running this file directly without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datanet import DataNet, MessageMeta

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

API_KEY = os.environ.get("DATANET_API_KEY", "ak_dev_12345")
CHANNEL = os.environ.get("DATANET_CHANNEL", "project.mall-install.sensor")
API_URL = os.environ.get("DATANET_API_URL", "https://api.datanet.art")
WS_URL = os.environ.get("DATANET_WS_URL", "wss://ws.datanet.art")
PUBLISH_INTERVAL = float(os.environ.get("DATANET_INTERVAL", "2"))
PUBLISH_COUNT = int(os.environ.get("DATANET_COUNT", "0"))  # 0 = unlimited

DEVICE_ID = f"sensor-{random.randint(1000, 9999)}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Sensor simulation
# ──────────────────────────────────────────────────────────────────────────────


def _generate_reading(sequence: int) -> dict[str, Any]:
    """Generate a synthetic temperature/humidity/pressure reading.

    Uses a slow sine wave to simulate gradual environmental changes, with a
    small random component for realism.
    """
    phase = (sequence * PUBLISH_INTERVAL) / 120  # 2-minute cycle
    base_temp = 21.0 + 3.0 * math.sin(2 * math.pi * phase)
    base_humidity = 55.0 + 10.0 * math.cos(2 * math.pi * phase)

    return {
        "device_id": DEVICE_ID,
        "sequence": sequence,
        "temperature_c": round(base_temp + random.gauss(0, 0.2), 2),
        "humidity_pct": round(min(100, max(0, base_humidity + random.gauss(0, 0.5))), 2),
        "pressure_hpa": round(1013.25 + random.gauss(0, 1.5), 2),
        "timestamp_ms": int(time.time() * 1000),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Pattern 1 — Async publisher
# ══════════════════════════════════════════════════════════════════════════════


async def run_async() -> None:
    """Publish sensor readings using native async/await."""

    dn = DataNet(api_key=API_KEY, api_url=API_URL, ws_url=WS_URL)
    published = 0
    stop = asyncio.Event()

    @dn.on("connect")
    async def handle_connect() -> None:
        print(f"Connected.  Publishing to: {CHANNEL}")
        print(f"Device ID:  {DEVICE_ID}")
        print(f"Interval:   {PUBLISH_INTERVAL}s")
        print(f"API URL:    {API_URL}")
        print(f"WS URL:     {WS_URL}")
        if PUBLISH_COUNT:
            print(f"Count:      {PUBLISH_COUNT} readings")
        print("\nPress Ctrl-C to stop.\n")

    @dn.on("disconnect")
    async def handle_disconnect() -> None:
        print("Disconnected.")

    @dn.on("error")
    async def handle_error(exc: Exception) -> None:
        print(f"Error: {exc}", file=sys.stderr)

    # Optional: receive our own published messages echoed back
    async def on_echo(data: object, meta: MessageMeta) -> None:
        logger.debug("Echo received from %s: %r", meta.channel, data)

    # Handle Ctrl-C cleanly
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with dn:
        dn.subscribe(CHANNEL, on_echo)  # echo subscription (optional)

        while not stop.is_set():
            reading = _generate_reading(published)
            try:
                await dn.publish(CHANNEL, reading)
                published += 1
                print(
                    f"[{published:>4}] "
                    f"temp={reading['temperature_c']:>5.2f}°C  "
                    f"humidity={reading['humidity_pct']:>5.2f}%  "
                    f"pressure={reading['pressure_hpa']:>7.2f} hPa"
                )
            except Exception as exc:
                print(f"Publish error: {exc}", file=sys.stderr)

            if PUBLISH_COUNT and published >= PUBLISH_COUNT:
                print(f"\nReached {PUBLISH_COUNT} readings. Done.")
                break

            try:
                await asyncio.wait_for(
                    asyncio.shield(stop.wait()), timeout=PUBLISH_INTERVAL
                )
            except asyncio.TimeoutError:
                pass  # interval elapsed; publish next reading

    print(f"\nPublished {published} readings. Goodbye.")


# ══════════════════════════════════════════════════════════════════════════════
# Pattern 2 — Sync publisher (background thread)
# ══════════════════════════════════════════════════════════════════════════════


def run_sync() -> None:
    """Publish sensor readings using the sync background-thread wrapper."""

    dn = DataNet(api_key=API_KEY, api_url=API_URL, ws_url=WS_URL)
    published = 0

    @dn.on("connect")
    async def handle_connect() -> None:
        print(f"Connected.  Publishing to: {CHANNEL}")
        print(f"Device ID:  {DEVICE_ID}")
        print(f"Interval:   {PUBLISH_INTERVAL}s")
        print(f"API URL:    {API_URL}")
        print(f"WS URL:     {WS_URL}")
        print("\nPress Ctrl-C to stop.\n")

    @dn.on("error")
    async def handle_error(exc: Exception) -> None:
        print(f"Error: {exc}", file=sys.stderr)

    dn.connect_sync(timeout=15)

    try:
        while True:
            reading = _generate_reading(published)

            # Schedule the publish coroutine on the SDK's background event loop
            future = asyncio.run_coroutine_threadsafe(
                dn.publish(CHANNEL, reading),
                dn._loop,  # type: ignore[arg-type]
            )
            try:
                future.result(timeout=5)
            except Exception as exc:
                print(f"Publish error: {exc}", file=sys.stderr)
                continue

            published += 1
            print(
                f"[{published:>4}] "
                f"temp={reading['temperature_c']:>5.2f}°C  "
                f"humidity={reading['humidity_pct']:>5.2f}%  "
                f"pressure={reading['pressure_hpa']:>7.2f} hPa"
            )

            if PUBLISH_COUNT and published >= PUBLISH_COUNT:
                print(f"\nReached {PUBLISH_COUNT} readings. Done.")
                break

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\nShutting down…")
    finally:
        dn.disconnect()
        print(f"Published {published} readings. Goodbye.")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pattern = os.environ.get("DATANET_PATTERN", "async").lower()

    if pattern == "sync":
        print("Using sync pattern (background thread).")
        run_sync()
    else:
        print("Using async pattern.")
        asyncio.run(run_async())
