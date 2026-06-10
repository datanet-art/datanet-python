"""publish_p5.py — publish p5-style coordinate payloads to a DataNet channel.

Useful for driving the browser p5 visualizer demo with explicit ``x`` / ``y``
positions. Publishes random coordinate payloads on an interval.

Usage
-----
::

    python examples/publish_p5.py

Configuration via environment variables:

    DATANET_API_KEY      Your DataNet API key          (default: ak_dev_12345)
    DATANET_CHANNEL      Target channel                (default: demo.text.basic)
    DATANET_API_URL      REST API base URL             (default: https://api.datanet.art)
    DATANET_WS_URL       WebSocket base URL            (default: wss://ws.datanet.art)
    DATANET_X_MIN        Minimum X coordinate          (default: 0)
    DATANET_X_MAX        Maximum X coordinate          (default: 1000)
    DATANET_Y_MIN        Minimum Y coordinate          (default: 0)
    DATANET_Y_MAX        Maximum Y coordinate          (default: 450)
    DATANET_INTERVAL     Seconds between publishes     (default: 5)
    DATANET_COUNT        Number of publishes           (default: 10)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import sys

# Allow running this file directly without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from _env import load_example_env
from datanet import DataNet

load_example_env()

API_KEY = os.environ.get("DATANET_API_KEY", "ak_dev_12345")
CHANNEL = os.environ.get("DATANET_CHANNEL", "demo.processing.p5")
API_URL = os.environ.get("DATANET_API_URL", "https://api.datanet.art")
WS_URL = os.environ.get("DATANET_WS_URL", "wss://ws.datanet.art")
X_MIN = float(os.environ.get("DATANET_X_MIN", "0"))
X_MAX = float(os.environ.get("DATANET_X_MAX", "1000"))
Y_MIN = float(os.environ.get("DATANET_Y_MIN", "0"))
Y_MAX = float(os.environ.get("DATANET_Y_MAX", "450"))
PUBLISH_INTERVAL = float(os.environ.get("DATANET_INTERVAL", "5"))
PUBLISH_COUNT = int(os.environ.get("DATANET_COUNT", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main() -> None:
    dn = DataNet(api_key=API_KEY, api_url=API_URL, ws_url=WS_URL)

    @dn.on("connect")
    async def handle_connect() -> None:
        print(f"Connected. Publishing to: {CHANNEL}")
        print(f"Range:   x={X_MIN}..{X_MAX}, y={Y_MIN}..{Y_MAX}")
        print(f"API URL: {API_URL}")
        print(f"WS URL:  {WS_URL}")
        print(f"Count:   {PUBLISH_COUNT}")
        print(f"Delay:   {PUBLISH_INTERVAL}s\n")

    @dn.on("disconnect")
    async def handle_disconnect() -> None:
        print("Disconnected.")

    @dn.on("error")
    async def handle_error(exc: Exception) -> None:
        print(f"Error: {exc}", file=sys.stderr)

    async with dn:
        for i in range(PUBLISH_COUNT):
            payload = {
                "x": round(random.uniform(X_MIN, X_MAX), 2),
                "y": round(random.uniform(Y_MIN, Y_MAX), 2),
            }
            await dn.publish(CHANNEL, payload)
            print(f"published {i + 1}/{PUBLISH_COUNT}: {payload}")
            if i < PUBLISH_COUNT - 1:
                await asyncio.sleep(PUBLISH_INTERVAL)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
