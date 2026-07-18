import base64
import json
import unittest
from unittest.mock import AsyncMock, patch

import datanet.client as client_module
from datanet.client import (
    AnyMessage,
    BinaryMessageMeta,
    DataNet,
    MessageMeta,
    binary_to_base64,
    build_art_dmx_packet,
    build_dmx_frame,
)


class FakeWebSocket:
    def __init__(self):
        self.closed = False
        self.sent = []
        self.incoming = []

    async def send(self, payload):
        self.sent.append(payload)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.incoming:
            return self.incoming.pop(0)
        raise StopAsyncIteration


class FakePostResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def json(self):
        return {"token": "jwt-test"}

    async def text(self):
        return ""


class FakeClientSession:
    calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakePostResponse()


class FakePresenceResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def json(self):
        return {"occupancy": 2, "members": ["one", "two"]}

    async def text(self):
        return ""


class FakePresenceSession:
    calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakePresenceResponse()


class DataNetClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_pub_messages_dispatch_to_matching_handlers(self):
        client = DataNet("ak_test")
        seen = []

        async def on_message(data, meta):
            seen.append((data, meta))

        client.subscribe("project.demo.sensor", on_message)

        await client._handle_message(
            '{"op":"pub","ch":"project.demo.sensor","d":{"value":42},"from":"conn_1","ts":123456}'
        )

        self.assertEqual(len(seen), 1)
        data, meta = seen[0]
        self.assertEqual(data, {"value": 42})
        self.assertIsInstance(meta, MessageMeta)
        self.assertEqual(meta.channel, "project.demo.sensor")
        self.assertEqual(meta.from_, "conn_1")
        self.assertEqual(meta.timestamp, 123456)

    async def test_unsubscribe_removes_specific_handler(self):
        client = DataNet("ak_test")

        async def handler_one(data, meta):
            return None

        async def handler_two(data, meta):
            return None

        client.subscribe("project.demo.sensor", handler_one)
        client.subscribe("project.demo.sensor", handler_two)
        client.unsubscribe("project.demo.sensor", handler_one)

        self.assertEqual(client._subscribers["project.demo.sensor"], [handler_two])

    async def test_non_pub_messages_are_ignored(self):
        client = DataNet("ak_test")
        seen = []

        async def on_message(data, meta):
            seen.append((data, meta))

        client.subscribe("project.demo.sensor", on_message)
        await client._handle_message('{"op":"ack","ch":"project.demo.sensor"}')

        self.assertEqual(seen, [])

    def test_disconnect_sync_without_active_connection_is_safe(self):
        client = DataNet("ak_test")

        client.disconnect_sync()

        self.assertFalse(client.connected)

    async def test_get_presence_uses_current_jwt(self):
        encoded = base64.urlsafe_b64encode(json.dumps({"pid": "project-id"}).encode()).decode().rstrip("=")
        client = DataNet("ak_test", api_url="https://api.example.test")
        client._jwt = f"header.{encoded}.signature"
        FakePresenceSession.calls = []

        with patch.object(client_module.aiohttp, "ClientSession", FakePresenceSession):
            result = await client.get_presence("project.project-id.demo")

        self.assertEqual(result, {"occupancy": 2, "members": ["one", "two"]})
        url, kwargs = FakePresenceSession.calls[0]
        self.assertEqual(url, "https://api.example.test/presence")
        self.assertEqual(kwargs["params"], {"channel": "project.project-id.demo", "projectId": "project-id"})
        self.assertEqual(kwargs["headers"]["Authorization"], f"Bearer {client._jwt}")

    async def test_get_presence_requires_connection(self):
        client = DataNet("ak_test")

        with self.assertRaisesRegex(RuntimeError, "connect before"):
            await client.get_presence("project.demo.room")

    async def test_binary_messages_dispatch_to_binary_and_any_handlers(self):
        client = DataNet("ak_test")
        binary_seen = []
        any_seen = []

        async def on_binary(data, meta):
            binary_seen.append((data, meta))

        async def on_any(message):
            any_seen.append(message)

        client.subscribe_binary("project.demo.dmx", on_binary, content_type="binary/dmx")
        client.subscribe_any("project.demo.dmx", on_any)

        await client._handle_message(
            json.dumps(
                {
                    "op": "pub",
                    "ch": "project.demo.dmx",
                    "bin": True,
                    "b64": "AQID",
                    "ct": "binary/dmx",
                    "bytes": 3,
                    "from": "browser-controller",
                    "ts": 123456,
                    "meta": {"universe": 1},
                }
            )
        )

        self.assertEqual(len(binary_seen), 1)
        data, meta = binary_seen[0]
        self.assertEqual(data, b"\x01\x02\x03")
        self.assertIsInstance(meta, BinaryMessageMeta)
        self.assertEqual(meta.channel, "project.demo.dmx")
        self.assertEqual(meta.from_, "browser-controller")
        self.assertEqual(meta.timestamp, 123456)
        self.assertEqual(meta.content_type, "binary/dmx")
        self.assertEqual(meta.bytes, 3)
        self.assertEqual(meta.metadata, {"universe": 1})

        self.assertEqual(len(any_seen), 1)
        self.assertIsInstance(any_seen[0], AnyMessage)
        self.assertEqual(any_seen[0].kind, "binary")
        self.assertEqual(any_seen[0].data, b"\x01\x02\x03")

    async def test_raw_binary_frames_dispatch_to_binary_handlers(self):
        client = DataNet("ak_test")
        binary_seen = []
        any_seen = []

        async def on_binary(data, meta):
            binary_seen.append((data, meta))

        async def on_any(message):
            any_seen.append(message)

        client.subscribe_binary("project.demo.dmx", on_binary, content_type="binary/dmx")
        client.subscribe_any("project.demo.dmx", on_any)

        await client._handle_raw_binary(b"\xff\x50\x14")

        self.assertEqual(len(binary_seen), 1)
        data, meta = binary_seen[0]
        self.assertEqual(data, b"\xff\x50\x14")
        self.assertIsInstance(meta, BinaryMessageMeta)
        self.assertEqual(meta.channel, "project.demo.dmx")
        self.assertEqual(meta.content_type, "binary/dmx")
        self.assertEqual(meta.bytes, 3)
        self.assertEqual(meta.metadata, {"raw": True})

        self.assertEqual(len(any_seen), 1)
        self.assertEqual(any_seen[0].kind, "binary")
        self.assertEqual(any_seen[0].data, b"\xff\x50\x14")

    async def test_utf8_raw_binary_frames_fall_back_when_not_json(self):
        client = DataNet("ak_test")
        seen = []

        async def on_binary(data, meta):
            seen.append((data, meta))

        client.subscribe_binary("project.demo.raw", on_binary)
        ws = FakeWebSocket()
        ws.incoming.append(b"hello-binary")
        client._ws = ws

        await client._recv_loop()

        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0][0], b"hello-binary")
        self.assertEqual(seen[0][1].metadata, {"raw": True})

    async def test_binary_websocket_json_frames_are_decoded_before_raw_fallback(self):
        client = DataNet("ak_test")
        ws = FakeWebSocket()
        ws.incoming.append(
            json.dumps(
                {
                    "op": "pub",
                    "ch": "project.demo.dmx",
                    "bin": True,
                    "b64": "AQID",
                    "ct": "binary/dmx",
                    "bytes": 3,
                    "meta": {"universe": 1},
                }
            ).encode("utf-8")
        )
        client._ws = ws
        seen = []

        async def on_binary(data, meta):
            seen.append((data, meta))

        client.subscribe_binary("project.demo.dmx", on_binary, content_type="binary/dmx")

        await client._recv_loop()

        self.assertEqual(len(seen), 1)
        data, meta = seen[0]
        self.assertEqual(data, b"\x01\x02\x03")
        self.assertEqual(meta.metadata, {"universe": 1})

    async def test_json_messages_dispatch_to_any_handlers(self):
        client = DataNet("ak_test")
        seen = []

        async def on_any(message):
            seen.append(message)

        client.subscribe_any("project.demo.sensor", on_any)
        await client._handle_message(
            '{"op":"pub","ch":"project.demo.sensor","d":{"value":42},"from":"conn_1","ts":123456}'
        )

        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].kind, "json")
        self.assertEqual(seen[0].data, {"value": 42})
        self.assertIsInstance(seen[0].meta, MessageMeta)

    async def test_publish_binary_sends_metadata_envelope(self):
        client = DataNet("ak_test")
        ws = FakeWebSocket()
        client._ws = ws

        await client.publish_binary(
            "project.demo.dmx",
            b"\x01\x02\x03",
            content_type="binary/dmx",
            metadata={"universe": 1},
        )

        self.assertEqual(
            json.loads(ws.sent[0]),
            {
                "op": "pub",
                "ch": "project.demo.dmx",
                "bin": True,
                "b64": "AQID",
                "ct": "binary/dmx",
                "meta": {"universe": 1},
            },
        )

    async def test_publish_auto_detects_bytes(self):
        client = DataNet("ak_test")
        ws = FakeWebSocket()
        client._ws = ws

        await client.publish(
            "project.demo.dmx",
            bytearray([4, 5]),
            content_type="binary/dmx",
            metadata={"universe": 2},
        )

        self.assertEqual(json.loads(ws.sent[0])["b64"], "BAU=")
        self.assertEqual(json.loads(ws.sent[0])["ct"], "binary/dmx")
        self.assertEqual(json.loads(ws.sent[0])["meta"], {"universe": 2})

    async def test_dmx_and_artnet_helpers(self):
        self.assertEqual(build_dmx_frame([255, 300, -1, 12], 6), b"\xff\xff\x00\x0c\x00\x00")
        packet = build_art_dmx_packet(b"\x01\x02\x03", universe=2, subnet=1, net=3, sequence=9)

        self.assertEqual(packet[:7], b"Art-Net")
        self.assertEqual(packet[8], 0x00)
        self.assertEqual(packet[9], 0x50)
        self.assertEqual(packet[11], 14)
        self.assertEqual(packet[12], 9)
        self.assertEqual(packet[14], 0x12)
        self.assertEqual(packet[15], 3)
        self.assertEqual(packet[16], 0)
        self.assertEqual(packet[17], 3)
        self.assertEqual(packet[18:], b"\x01\x02\x03")

    async def test_binary_base64_helper(self):
        self.assertEqual(binary_to_base64(b"\x00\x01\x02\xff"), "AAEC/w==")

    async def test_auth_payload_includes_device_metadata(self):
        FakeClientSession.calls = []
        client = DataNet(
            "ak_test",
            device_id="python-node",
            client_id="pytest",
            device_name="Python Test Node",
        )

        with patch.object(client_module.aiohttp, "ClientSession", FakeClientSession):
            token = await client._fetch_jwt()

        self.assertEqual(token, "jwt-test")
        _, kwargs = FakeClientSession.calls[0]
        self.assertEqual(
            kwargs["json"],
            {
                "apiKey": "ak_test",
                "deviceId": "python-node",
                "clientId": "pytest",
                "deviceName": "Python Test Node",
            },
        )

    async def test_heartbeat_sends_hb_envelope(self):
        client = DataNet("ak_test")
        ws = FakeWebSocket()
        client._ws = ws
        original_interval = client_module._HEARTBEAT_INTERVAL
        client_module._HEARTBEAT_INTERVAL = 0

        try:
            task = __import__("asyncio").create_task(client._heartbeat_loop())
            while not ws.sent:
                await __import__("asyncio").sleep(0)
            ws.closed = True
            await task
        finally:
            client_module._HEARTBEAT_INTERVAL = original_interval

        self.assertEqual(json.loads(ws.sent[0]), {"op": "hb"})

    async def test_resubscribe_replays_json_binary_and_any_channels(self):
        client = DataNet("ak_test")
        client._send_sub = AsyncMock()

        async def handler(*_):
            return None

        client.subscribe("demo.json", handler)
        client.subscribe_binary("demo.binary", handler)
        client.subscribe_any("demo.any", handler)

        await client._resubscribe_all()

        client._send_sub.assert_any_await("demo.json")
        client._send_sub.assert_any_await("demo.binary")
        client._send_sub.assert_any_await("demo.any")
        self.assertEqual(client._send_sub.await_count, 3)


class FakeHandshakeWebSocket:
    """Minimal ws exposing recv() for handshake tests."""

    def __init__(self, frames):
        self.frames = list(frames)

    async def recv(self):
        if self.frames:
            return self.frames.pop(0)
        raise AssertionError("handshake consumed all frames without resolving")


class GatewayLimitErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_handshake_succeeds_on_connected_message(self):
        client = DataNet("ak_test")
        ws = FakeHandshakeWebSocket([json.dumps({"type": "connected", "userId": "dev-1"})])
        await client._await_handshake(ws)  # must not raise

    async def test_handshake_raises_structured_device_limit_error(self):
        client = DataNet("ak_test")
        client._should_run = True
        errors = []

        async def on_error(exc):
            errors.append(exc)

        client.on("error", on_error)
        ws = FakeHandshakeWebSocket(
            [json.dumps({"type": "error", "error": "device_limit_reached", "limit": 25})]
        )

        with self.assertRaises(client_module.DataNetError) as ctx:
            await client._await_handshake(ws)

        self.assertEqual(ctx.exception.code, "device_limit_reached")
        self.assertEqual(ctx.exception.limit, 25)
        # Fatal: the reconnect loop must stop instead of hammering the gateway.
        self.assertFalse(client._should_run)
        self.assertIs(client._fatal_error, ctx.exception)
        self.assertEqual(len(errors), 1)

    async def test_handshake_skips_binary_noise_before_connected(self):
        client = DataNet("ak_test")
        ws = FakeHandshakeWebSocket(
            [b"\xff\xfe", "not json", json.dumps({"type": "connected"})]
        )
        await client._await_handshake(ws)  # must not raise

    async def test_rate_limited_error_carries_retry_ms_and_scope(self):
        client = DataNet("ak_test")
        errors = []

        async def on_error(exc):
            errors.append(exc)

        client.on("error", on_error)
        await client._handle_message(
            json.dumps(
                {
                    "type": "error",
                    "error": "rate_limited",
                    "retry_ms": 250,
                    "scope": "connection",
                    "channel": "project.p1.sensor",
                }
            )
        )

        self.assertEqual(len(errors), 1)
        error = errors[0]
        self.assertEqual(error.code, "rate_limited")
        self.assertEqual(error.retry_ms, 250)
        self.assertEqual(error.scope, "connection")
        self.assertEqual(error.channel, "project.p1.sensor")
        # Rate limiting is transient — must not stop the client.
        self.assertIsNone(client._fatal_error)

    async def test_topic_limit_reached_error_carries_plan_limit(self):
        client = DataNet("ak_test")
        errors = []

        async def on_error(exc):
            errors.append(exc)

        client.on("error", on_error)
        await client._handle_message(
            json.dumps(
                {
                    "type": "error",
                    "error": "topic_limit_reached",
                    "limit": 240,
                    "channel": "project.p1.one-too-many",
                    "operation": "sub",
                }
            )
        )

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, "topic_limit_reached")
        self.assertEqual(errors[0].limit, 240)
        self.assertEqual(errors[0].channel, "project.p1.one-too-many")
        self.assertIsNone(client._fatal_error)

    async def test_device_limit_error_mid_session_stops_reconnect_loop(self):
        client = DataNet("ak_test")
        client._should_run = True
        errors = []

        async def on_error(exc):
            errors.append(exc)

        client.on("error", on_error)
        await client._handle_message(
            json.dumps({"type": "error", "error": "device_limit_reached", "limit": 5})
        )

        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].limit, 5)
        self.assertFalse(client._should_run)
        self.assertIsNotNone(client._fatal_error)

    async def test_wait_for_connection_raises_fatal_error_immediately(self):
        client = DataNet("ak_test")
        client._fatal_error = client_module.DataNetError(
            "DataNet: device_limit_reached",
            code="device_limit_reached",
            limit=25,
        )

        with self.assertRaises(client_module.DataNetError) as ctx:
            await client._wait_for_connection(timeout=5)

        self.assertEqual(ctx.exception.code, "device_limit_reached")
