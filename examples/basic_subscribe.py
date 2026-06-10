"""basic_subscribe.py — subscribe to a DataNet channel and print every message.

Shows both the async pattern (recommended) and the sync / background-thread
pattern for scripts that don't manage their own event loop.

Usage
-----
Set your API key and channel, then run::

    python examples/basic_subscribe.py

Or with a custom channel and API key via environment variables::

    DATANET_API_KEY=ak_... DATANET_CHANNEL=project.abc.sensor \\
        python examples/basic_subscribe.py

You can also override the backend URLs for local development::

    DATANET_API_URL=http://localhost:3001 DATANET_WS_URL=ws://localhost:3001 \\
        python examples/basic_subscribe.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time

# Allow running this file directly without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from _env import load_example_env
from datanet import DataNet, MessageMeta

load_example_env()

# ──────────────────────────────────────────────────────────────────────────────
# Configuration — override via environment variables
# ──────────────────────────────────────────────────────────────────────────────

API_KEY = os.environ.get("DATANET_API_KEY", "ak_dev_12345")
CHANNEL = os.environ.get("DATANET_CHANNEL", "project.mall-install.sensor")
API_URL = os.environ.get("DATANET_API_URL", "https://api.datanet.art")
WS_URL = os.environ.get("DATANET_WS_URL", "wss://ws.datanet.art")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


# ══════════════════════════════════════════════════════════════════════════════
# Pattern 1 — Async (recommended for async applications)
# ══════════════════════════════════════════════════════════════════════════════


async def run_async() -> None:
    """Subscribe using the async context manager."""

    async def on_message(data: object, meta: MessageMeta) -> None:
        print(
            f"  [{meta.channel}]"
            f"  from={meta.from_!r}"
            f"  ts={meta.timestamp}"
            f"  data={data!r}"
        )

    dn = DataNet(api_key=API_KEY, api_url=API_URL, ws_url=WS_URL)

    @dn.on("connect")
    async def handle_connect() -> None:
        print(f"Connected!  Listening on channel: {CHANNEL}")
        print(f"API URL: {API_URL}")
        print(f"WS URL:  {WS_URL}")
        print("Press Ctrl-C to stop.\n")

    @dn.on("disconnect")
    async def handle_disconnect() -> None:
        print("Disconnected.")

    @dn.on("error")
    async def handle_error(exc: Exception) -> None:
        print(f"Error: {exc}", file=sys.stderr)

    # Gracefully handle Ctrl-C in async context
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

    async with dn:
        dn.subscribe(CHANNEL, on_message)
        await stop  # wait until Ctrl-C

    print("Goodbye.")


# ══════════════════════════════════════════════════════════════════════════════
# Pattern 2 — Sync / background thread (simpler for scripts)
# ══════════════════════════════════════════════════════════════════════════════


def run_sync() -> None:
    """Subscribe using the sync background-thread helper."""

    dn = DataNet(api_key=API_KEY, api_url=API_URL, ws_url=WS_URL)

    @dn.on("connect")
    async def handle_connect() -> None:
        print(f"Connected!  Listening on channel: {CHANNEL}")
        print(f"API URL: {API_URL}")
        print(f"WS URL:  {WS_URL}")
        print("Press Ctrl-C to stop.\n")

    @dn.on("disconnect")
    async def handle_disconnect() -> None:
        print("Disconnected.")

    @dn.on("error")
    async def handle_error(exc: Exception) -> None:
        print(f"Error: {exc}", file=sys.stderr)

    async def on_message(data: object, meta: MessageMeta) -> None:
        print(
            f"  [{meta.channel}]"
            f"  from={meta.from_!r}"
            f"  ts={meta.timestamp}"
            f"  data={data!r}"
        )

    # connect_sync() blocks until the WebSocket handshake is complete.
    dn.connect_sync(timeout=15)
    dn.subscribe(CHANNEL, on_message)

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nShutting down…")
        dn.disconnect()
        print("Goodbye.")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Select pattern via DATANET_PATTERN env var (default: async)
    pattern = os.environ.get("DATANET_PATTERN", "async").lower()

    if pattern == "sync":
        print("Using sync pattern (background thread).")
        run_sync()
    else:
        print("Using async pattern.")
        asyncio.run(run_async())
