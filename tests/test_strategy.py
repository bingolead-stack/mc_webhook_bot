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
        self.assertEqual(len(app.pending_alerts()), 1)

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
