# DataNet WebSocket Protocol

This document describes the SDK-facing WebSocket envelope used by DataNet
clients. SDKs should preserve this contract across languages so JavaScript,
Python, Arduino/ESP32, Processing, TouchDesigner, and bridge tools interoperate.

## Connection

Connect to:

```text
wss://ws.datanet.art/ws
```

Pass the gateway JWT as a WebSocket subprotocol:

```text
Sec-WebSocket-Protocol: bearer, <jwt>
```

The JWT is obtained from `POST https://api.datanet.art/auth/token` using a
project API key.

## Client Envelopes

DataNet supports two transport payload classes:

- JSON values in `d`, including strings, numbers, booleans, arrays, objects,
  and nested data structures.
- Binary bytes in `b64`, labeled by `ct` and optionally described with `meta`.

Subscribe:

```json
{ "op": "sub", "ch": "project.<project-id>.sensor" }
```

Publish JSON:

```json
{
  "op": "pub",
  "ch": "project.<project-id>.sensor",
  "d": { "value": 42 }
}
```

Publish binary:

```json
{
  "op": "pub",
  "ch": "project.<project-id>.lights",
  "bin": true,
  "b64": "AQID",
  "ct": "binary/dmx",
  "meta": {
    "universe": 1,
    "format": "dmx512"
  }
}
```

## Server Publish Envelopes

JSON subscribers receive:

```json
{
  "type": "message",
  "op": "pub",
  "ch": "project.<project-id>.sensor",
  "d": { "value": 42 },
  "ts": 1710000000000,
  "from": "device-1"
}
```

Binary subscribers receive a metadata-bearing envelope:

```json
{
  "type": "message",
  "op": "pub",
  "ch": "project.<project-id>.lights",
  "bin": true,
  "b64": "AQID",
  "ct": "binary/dmx",
  "bytes": 3,
  "ts": 1710000000000,
  "from": "browser-controller",
  "meta": {
    "universe": 1,
    "format": "dmx512"
  }
}
```

SDKs should expose binary messages as bytes plus metadata:

```python
BinaryMessageMeta(
    channel="project.<project-id>.lights",
    from_="browser-controller",
    timestamp=1710000000000,
    content_type="binary/dmx",
    bytes=3,
    metadata={"universe": 1},
)
```

Known binary content types include `binary/dmx`, `binary/dmx-delta`,
`binary/artnet`, `binary/vecf32`, `binary/ble-adv-batch`,
`binary/interaction-batch`, and `application/octet-stream`.

## Raw Binary Compatibility

The preferred server fanout is always the metadata-bearing JSON envelope above.
Some older gateway paths may still deliver raw WebSocket binary frames. Raw
frames do not carry channel, sender, timestamp, content type, or custom
metadata, so SDKs should treat them as a compatibility fallback only.

The Python SDK handles this by first attempting to decode binary WebSocket
frames as UTF-8 JSON envelopes. If decoding or JSON parsing fails, it dispatches
the bytes to registered binary subscribers and marks the metadata as
`{"raw": True}`.

## Error Codes

Gateway errors arrive as `{"type": "error", "error": "<code>", ...}` envelopes:

| `error` | When | Extra fields | Retryable? |
|---|---|---|---|
| `rate_limited` | Publish exceeded a per-connection, per-topic, or per-project msgs/sec or bytes/sec budget | `retry_ms`, `scope` (`"connection"` when the per-connection throttle fired) | Yes — back off for `retry_ms` |
| `device_limit_reached` | Connecting would exceed the plan's active-device cap; sent before the handshake, then the socket is closed | `limit` (the plan's device cap) | No — disconnect another device or upgrade |
| `topic_limit_reached` | The channel exists but is over the plan's channel cap (e.g. after a tier downgrade) | `limit` (the plan's channel cap) | No — remove channels or upgrade |
| `channel_not_provisioned` | The channel has not been created for this project | `channel`, `operation` | No — create the channel first |
| `channel_not_allowed` | The JWT's channel prefixes don't cover this channel | `channel`, `operation` | No |
| `insufficient_scope` | The API key lacks the `pub` or `sub` scope | `required` | No |

The Python SDK surfaces these as `DataNetError` with `code`, `channel`,
`retry_ms`, `scope`, and `limit` attributes. `device_limit_reached` is fatal:
the client stops its reconnect loop and `connect()` / `connect_sync()` raise
the structured error instead of timing out.
