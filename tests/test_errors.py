import unittest

from datanet.client import DataNet, DataNetError


class DataNetErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_gateway_limit_errors_are_emitted_as_structured_errors(self):
        client = DataNet("ak_test")
        seen = []

        @client.on("error")
        async def on_error(error):
            seen.append(error)

        await client._handle_message(
            '{"type":"error","error":"rate_limited","retry_ms":250,"scope":"connection","channel":"events"}'
        )

        self.assertEqual(len(seen), 1)
        self.assertIsInstance(seen[0], DataNetError)
        self.assertEqual(seen[0].code, "rate_limited")
        self.assertEqual(seen[0].retry_ms, 250)
        self.assertEqual(seen[0].scope, "connection")
        self.assertEqual(seen[0].channel, "events")


if __name__ == "__main__":
    unittest.main()
