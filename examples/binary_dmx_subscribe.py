"""Subscribe to binary DMX frames from DataNet.

Usage:
    DATANET_API_KEY=ak_... DATANET_BINARY_CHANNEL=project.abc.lighting.dmx \
        python examples/binary_dmx_subscribe.py
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys

# Allow running this file directly without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from _env import load_example_env
from datanet import BinaryMessageMeta, DataNet

load_example_env()

API_KEY = os.environ.get("DATANET_API_KEY", "ak_your_api_key_here")
CHANNEL = os.environ.get(
    "DATANET_BINARY_CHANNEL",
    os.environ.get("DATANET_CHANNEL", "project.your_project_id.lighting.dmx"),
)
API_URL = os.environ.get("DATANET_API_URL", "https://api.datanet.art")
WS_URL = os.environ.get("DATANET_WS_URL", "wss://ws.datanet.art")


async def main() -> None:
    dn = DataNet(
        api_key=API_KEY,
        api_url=API_URL,
        ws_url=WS_URL,
        device_id=os.environ.get("DATANET_DEVICE_ID", "python-dmx-subscriber"),
        client_id=os.environ.get("DATANET_CLIENT_ID", "datanet-python-example"),
    )
    stop = asyncio.Event()

    async def on_dmx(data: bytes, meta: BinaryMessageMeta) -> None:
        preview = " ".join(f"{byte:02x}" for byte in data[:8])
        universe = (meta.metadata or {}).get("universe", "?")
        print(
            f"{len(data)} bytes on {meta.channel} "
            f"ct={meta.content_type} universe={universe}: {preview}"
        )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with dn:
        dn.subscribe_binary(CHANNEL, on_dmx, content_type="binary/dmx")
        print(f"listening on {CHANNEL}; press Ctrl-C to stop")
        await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
