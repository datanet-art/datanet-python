import json
import unittest

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

    async def send(self, payload):
        self.sent.append(payload)


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
