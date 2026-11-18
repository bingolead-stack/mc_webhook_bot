import asyncio
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import app


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        app.DATABASE_PATH = os.path.join(self.tmp.name, "test.sqlite3")
        app.init_db()
        self.start = datetime(2026, 10, 1, tzinfo=timezone.utc)

    def tearDown(self):
        self.tmp.cleanup()

    def send(self, n, o, h, l, c, symbol="EURUSD"):
        candle = app.Candle(symbol=symbol, timestamp=self.start + timedelta(hours=4 * n),
                            open=o, high=h, low=l, close=c)
        return app.process_candle(candle)

    def test_bullish_and_duplicate(self):
        for n in range(18):
            self.send(n, 100 + n, 102 + n, 99 + n, 101 + n)
        self.send(18, 118, 120, 117, 119)
        result = self.send(19, 119, 122, 116, 121)
        self.assertEqual(result["mc"], "bullish")
        self.assertTrue(result["alert_queued"])
        self.assertEqual(self.send(19, 119, 122, 116, 121)["status"], "duplicate_ignored")
        alerts = app.pending_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertIn("EURUSD", alerts[0]["message"])
        self.assertIn("BULLISH MC", alerts[0]["message"])

    def test_worker_sends_symbol_to_telegram(self):
        for n in range(18):
            self.send(n, 100 + n, 102 + n, 99 + n, 101 + n, symbol="btcusdt")
        self.send(18, 118, 120, 117, 119, symbol="btcusdt")
        result = self.send(19, 119, 122, 116, 121, symbol="btcusdt")
        self.assertEqual(result["mc"], "bullish")
        posted = []

        class DummyResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {"ok": True}

        class DummyClient:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def post(self, url, json):
                posted.append({"url": url, "json": json})
                return DummyResponse()

        original_client = app.httpx.AsyncClient
        original_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        original_chat = os.environ.get("TELEGRAM_CHAT_ID")
        app.httpx.AsyncClient = DummyClient
        os.environ["TELEGRAM_BOT_TOKEN"] = "test-token"
        os.environ["TELEGRAM_CHAT_ID"] = "1"

        async def run():
            worker = asyncio.create_task(app.alert_worker())
            try:
                for _ in range(40):
                    await asyncio.sleep(0.05)
                    if posted and not app.pending_alerts():
                        break
            finally:
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass

        try:
            asyncio.run(run())
        finally:
            app.httpx.AsyncClient = original_client
            if original_token is None:
                os.environ.pop("TELEGRAM_BOT_TOKEN", None)
            else:
                os.environ["TELEGRAM_BOT_TOKEN"] = original_token
            if original_chat is None:
                os.environ.pop("TELEGRAM_CHAT_ID", None)
            else:
                os.environ["TELEGRAM_CHAT_ID"] = original_chat

        self.assertEqual(len(posted), 1)
        self.assertTrue(posted[0]["url"].endswith("/sendMessage"))
        self.assertEqual(posted[0]["json"]["chat_id"], "1")
        self.assertIn("BTCUSDT", posted[0]["json"]["text"])
        self.assertIn("BULLISH MC", posted[0]["json"]["text"])
        self.assertEqual(app.pending_alerts(), [])

    def test_bearish(self):
        for n in range(18):
            self.send(n, 200 - n, 202 - n, 198 - n, 199 - n)
        self.send(18, 180, 182, 178, 179)
        result = self.send(19, 179, 183, 175, 177)
        self.assertEqual(result["mc"], "bearish")

    def test_gap_prevents_signal(self):
        for n in range(18):
            self.send(n, 100 + n, 102 + n, 99 + n, 101 + n)
        self.send(18, 118, 120, 117, 119)
        result = self.send(20, 119, 125, 116, 122)
        self.assertIsNone(result["mc"])

    def test_no_history_no_signal(self):
        self.assertIsNone(self.send(0, 100, 102, 99, 101)["mc"])


if __name__ == "__main__":
    unittest.main()
