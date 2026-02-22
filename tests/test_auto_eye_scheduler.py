from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from auto_eye.scheduler import TimeframeScheduler


class TimeframeSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TimeframeScheduler()

    def test_m15_schedule(self) -> None:
        last = datetime(2026, 2, 21, 10, 14, tzinfo=timezone.utc)
        now_not_due = datetime(2026, 2, 21, 10, 14, 59, tzinfo=timezone.utc)
        now_due = datetime(2026, 2, 21, 10, 15, 0, tzinfo=timezone.utc)

        self.assertFalse(
            self.scheduler.is_due(timeframe="M15", now_utc=now_not_due, last_check_utc=last)
        )
        self.assertTrue(
            self.scheduler.is_due(timeframe="M15", now_utc=now_due, last_check_utc=last)
        )

    def test_h1_schedule(self) -> None:
        last = datetime(2026, 2, 21, 10, 5, tzinfo=timezone.utc)
        now = datetime(2026, 2, 21, 11, 0, tzinfo=timezone.utc)
        self.assertTrue(self.scheduler.is_due(timeframe="H1", now_utc=now, last_check_utc=last))

    def test_h4_schedule(self) -> None:
        last = datetime(2026, 2, 21, 4, 1, tzinfo=timezone.utc)
        now = datetime(2026, 2, 21, 8, 0, tzinfo=timezone.utc)
        self.assertTrue(self.scheduler.is_due(timeframe="H4", now_utc=now, last_check_utc=last))

    def test_d1_schedule(self) -> None:
        last = datetime(2026, 2, 21, 23, 0, tzinfo=timezone.utc)
        now = datetime(2026, 2, 22, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(self.scheduler.is_due(timeframe="D1", now_utc=now, last_check_utc=last))

    def test_w1_schedule(self) -> None:
        last = datetime(2026, 2, 22, 23, 0, tzinfo=timezone.utc)
        now = datetime(2026, 2, 23, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(self.scheduler.is_due(timeframe="W1", now_utc=now, last_check_utc=last))

    def test_monthly_schedule(self) -> None:
        last = datetime(2026, 2, 28, 23, 59, tzinfo=timezone.utc)
        now = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(
            self.scheduler.is_due(timeframe="MN1", now_utc=now, last_check_utc=last)
        )
        self.assertTrue(
            self.scheduler.is_due(timeframe="M1", now_utc=now, last_check_utc=last)
        )


if __name__ == "__main__":
    unittest.main()
