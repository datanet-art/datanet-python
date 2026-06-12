# DataNet Python SDK

Async Python client for the [DataNet](https://datanet.art) realtime platform.
Supports pub/sub over WebSocket with automatic reconnection and heartbeats.
Payloads can be JSON values and nested data structures, bytes-like binary
payloads, or content-type-labeled formats such as DMX, Art-Net, float vectors,
BLE batches, and compact interaction frames.

## Requirements

- Python 3.11+
- [`aiohttp`](https://docs.aiohttp.org/) >= 3.9
- [`websockets`](https://websockets.readthedocs.io/) >= 12.0

## Installation

Install from PyPI:

```bash
pip install datanet-sdk
```

For local development from this repo:

```bash
pip install -e .[dev]
```

## Quick start

### Async pattern (recommended)

```python
import asyncio
from datanet import DataNet

async def main():
    dn = DataNet(api_key="ak_your_key_here")

    async def on_message(data, meta):
        print(f"[{meta.channel}] {data}")

    async with dn:                              # connects, disconnects on exit
        dn.subscribe("project.abc.sensor", on_message)
        await asyncio.sleep(60)                 # keep running

asyncio.run(main())
```

### Sync / background-thread pattern

For scripts that don't manage their own event loop:

```python
import time
from datanet import DataNet

dn = DataNet(api_key="ak_your_key_here")

@dn.on("connect")
async def handle_connect():
    print("Connected!")

@dn.on("error")
async def handle_error(exc):
    print(f"Error: {exc}")

async def on_sensor(data, meta):
    print(f"sensor reading: {data}")

dn.connect_sync()                   # blocks until connected, runs in bg thread
dn.subscribe("project.abc.sensor", on_sensor)

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    dn.disconnect_sync()
```

## API reference

### `DataNet(api_key, device_id=None, client_id=None, device_name=None, api_url=..., ws_url=..., max_reconnect_attempts=5)`

| Parameter | Default | Description |
|---|---|---|
| `api_key` | — | Your DataNet API key (`ak_...`) |
| `device_id` | — | Stable device identifier for presence/history |
| `client_id` | — | Optional app/client identifier |
| `device_name` | — | Optional display name for dashboards/admin tools |
| `api_url` | `https://api.datanet.art` | REST API base URL |
| `ws_url` | `wss://ws.datanet.art` | WebSocket base URL |
| `max_reconnect_attempts` | `5` | Max consecutive reconnects; `0` = unlimited |

### Payload formats

The Python SDK supports the same protocol classes as the JavaScript SDK:

- JSON scalars, arrays, dictionaries, and nested structures via `publish(...)`.
- Bytes-like values (`bytes`, `bytearray`, `memoryview`) via `publish(...)` auto-detection.
- Explicit binary packets via `publish_binary(...)`.
- DMX frames via `build_dmx_frame(...)` / `publish_dmx(...)`.
- Art-Net ArtDMX packets via `build_art_dmx_packet(...)` / `publish_artnet(...)`.
- Mixed JSON and binary subscriptions via `subscribe_any(...)`.

Binary messages include metadata with the packet: `channel`, `from_`,
`timestamp`, `content_type`, `bytes`, and optional custom `metadata`.
When connected to an older gateway that still emits raw binary WebSocket frames,
the SDK falls back safely and marks those messages with `metadata={"raw": True}`.

```python
from datanet import DataNet, build_dmx_frame

dn = DataNet(api_key="ak_your_key_here")
frame = build_dmx_frame([255, 80, 20, 180], 512)

async with dn:
    await dn.publish_binary(
        "project.abc.lighting.dmx",
        frame,
        content_type="binary/dmx",
        metadata={"universe": 1, "format": "dmx512"},
    )
```

## Local development

When you want to test against a local gateway instead of production, pass the
URLs explicitly:

```python
dn = DataNet(
    api_key="ak_local_key_here",
    api_url="http://localhost:8080",
    ws_url="ws://localhost:8080",
)
```

The bundled examples automatically load configuration from a repo-level `.env`
file if present. Copy the template once, add your real key/channel values, and
keep `.env` local:

```bash
cp .env.example .env
```

The examples also support shell environment overrides, which take precedence
over values in `.env`:

```bash
DATANET_API_KEY='ak_local_key_here' \
DATANET_CHANNEL='demo.text.basic' \
DATANET_API_URL='http://localhost:8080' \
DATANET_WS_URL='ws://localhost:8080' \
python examples/publish.py
```

### Binary examples

The JSON examples use `DATANET_CHANNEL`.

The binary DMX examples use `DATANET_BINARY_CHANNEL` first, then fall back to
`DATANET_CHANNEL` if no binary channel is set. Use the same binary channel for
both publisher and subscriber:

```bash
DATANET_API_KEY='ak_local_key_here' \
DATANET_BINARY_CHANNEL='project.abc.lighting.dmx' \
python examples/binary_dmx_subscribe.py
```

In another terminal:

```bash
DATANET_API_KEY='ak_local_key_here' \
DATANET_BINARY_CHANNEL='project.abc.lighting.dmx' \
python examples/binary_dmx_publish.py
```

To drive the browser p5 visualizer demo directly with pixel coordinates:

```bash
DATANET_API_KEY='ak_local_key_here' \
DATANET_CHANNEL='demo.text.basic' \
DATANET_API_URL='http://localhost:8080' \
DATANET_WS_URL='ws://localhost:8080' \
DATANET_X_MIN='0' \
DATANET_X_MAX='1000' \
DATANET_Y_MIN='0' \
DATANET_Y_MAX='450' \
python examples/publish_p5.py
```

### Methods

| Method | Description |
|---|---|
| `await connect()` | Fetch JWT and open WebSocket |
| `connect_sync(timeout=10)` | Same, but runs in a background thread |
| `await disconnect()` | Close connection and stop run loop |
| `disconnect_sync(timeout=10)` | Close a sync/background-thread connection |
| `subscribe(channel, handler)` | Register an async message handler |
| `unsubscribe(channel, handler=None)` | Remove handler (or all) from channel |
| `await publish(channel, data, content_type=None, metadata=None)` | Send JSON, or auto-detect bytes-like binary data |
| `await publish_binary(channel, data, content_type=..., metadata=None)` | Send binary bytes with content type and metadata |
| `await publish_dmx(channel, values, length=512, metadata=None)` | Publish a clamped DMX frame as `binary/dmx` |
| `await publish_artnet(channel, dmx, ..., metadata=None)` | Publish an ArtDMX packet as `binary/artnet` |
| `subscribe_binary(channel, handler, content_type=None)` | Register an async binary handler |
| `subscribe_any(channel, handler)` | Register an async handler for JSON and binary messages |
| `on(event, handler)` | Register an event handler (decorator or direct) |

### Events

| Event | Handler signature | Fired when |
|---|---|---|
| `"connect"` | `async def()` | WebSocket connection established |
| `"disconnect"` | `async def()` | Connection closed |
| `"error"` | `async def(exc)` | An error occurs |

### `MessageMeta`

Passed as the second argument to every message handler:

```python
@dataclass
class MessageMeta:
    channel: str       # e.g. "project.abc.sensor"
    from_: str         # sender connection ID
    timestamp: int     # Unix timestamp (ms) from the server
```

### `BinaryMessageMeta`

Passed as the second argument to every binary message handler:

```python
@dataclass
class BinaryMessageMeta:
    channel: str
    from_: str
    timestamp: int
    content_type: str
    bytes: int
    metadata: dict | None = None
```

## Reconnection

The SDK reconnects automatically with exponential backoff:

| Attempt | Delay |
|---|---|
| 1st | 1 s |
| 2nd | 2 s |
| 3rd | 4 s |
| 4th | 8 s |
| 5th | 16 s |
| subsequent | capped at 30 s |

All subscriptions are replayed after each reconnect.

## Logging

The SDK uses Python's standard `logging` module under the logger name `datanet.client`.

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Examples

See the [`examples/`](./examples/) directory:

- [`basic_subscribe.py`](./examples/basic_subscribe.py) — subscribe and print messages
- [`publish.py`](./examples/publish.py) — publish simulated sensor readings
- [`publish_p5.py`](./examples/publish_p5.py) — publish p5-style `x` / `y` coordinates
- [`binary_dmx_publish.py`](./examples/binary_dmx_publish.py) — publish binary DMX frames
- [`binary_dmx_subscribe.py`](./examples/binary_dmx_subscribe.py) — subscribe to binary DMX frames

Standalone showcase projects that use the published package live in
[datanet-examples](https://github.com/datanet-art/datanet-examples).

For custom clients or other SDK implementations, see [`PROTOCOL.md`](./PROTOCOL.md).

## Tests

```bash
python -m pip install -e .[dev]
python -m pytest
```

## About

DataNet is developed and supported by [Studio Jordan Shaw](https://www.jordanshaw.com), a creative technology studio building tools for realtime, networked, and physical-digital work.

- DataNet: [datanet.art](https://datanet.art)
- Studio: [jordanshaw.com](https://www.jordanshaw.com)
- Instagram: [@jshaw3](https://www.instagram.com/jshaw3)
- GitHub: [datanet-art](https://github.com/datanet-art)
- Source: [datanet-python](https://github.com/datanet-art/datanet-python)
- Examples: [datanet-examples](https://github.com/datanet-art/datanet-examples)

## License

MIT
