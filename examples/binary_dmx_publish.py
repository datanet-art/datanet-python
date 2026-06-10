"""Publish a binary DMX frame to DataNet.

Usage:
    DATANET_API_KEY=ak_... DATANET_BINARY_CHANNEL=project.abc.lighting.dmx \
        python examples/binary_dmx_publish.py
"""

from __future__ import annotations

import asyncio
import os
import sys

# Allow running this file directly without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datanet import DataNet, build_dmx_frame

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
        device_id=os.environ.get("DATANET_DEVICE_ID", "python-dmx-publisher"),
        client_id=os.environ.get("DATANET_CLIENT_ID", "datanet-python-example"),
    )

    async with dn:
        frame = build_dmx_frame([255, 80, 20, 180], 512)
        await dn.publish_binary(
            CHANNEL,
            frame,
            content_type="binary/dmx",
            metadata={"universe": 1, "format": "dmx512"},
        )
        print(f"sent {len(frame)} DMX bytes to {CHANNEL}")


if __name__ == "__main__":
    asyncio.run(main())
