from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config_loader import AutoEyeConfig

from auto_eye.detectors.fractal import FRACTAL_HIGH, FRACTAL_LOW, FractalDetector
from auto_eye.detectors.snr import (
    BREAK_UP_CLOSE,
    ROLE_SUPPORT,
    SNRDetector,
)
from auto_eye.models import OHLCBar, STATUS_INVALIDATED, TrackedElement


def build_config() -> AutoEyeConfig:
    return AutoEyeConfig(
        enabled=True,
        symbols=["EURUSD"],
        timeframes=["M15"],
        elements=["fractal", "snr"],
        history_days=30,
        history_buffer_days=5,
        incremental_bars=500,
        update_interval_seconds=300,
        scheduler_poll_seconds=60,
        output_json="output/auto_eye_zones.json",
        output_csv="output/auto_eye_zones.csv",
        state_json="output/auto_eye_state.json",
        min_gap_points=0.0,
        require_displacement=False,
        displacement_k=1.5,
        atr_period=14,
        median_body_period=20,
        fill_rule="both",
    )


def make_bar(
    index: int,
    *,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
) -> OHLCBar:
    base = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    return OHLCBar(
        time=base + timedelta(minutes=15 * index),
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        tick_volume=100,
    )


class FractalAndSNRDetectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = build_config()
        self.fractal = FractalDetector()
        self.snr = SNRDetector()

    def test_fractal_detects_high_and_low(self) -> None:
        bars = [
            make_bar(0, open_price=9.0, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.4, high_price=12.0, low_price=9.0, close_price=11.0),
            make_bar(2, open_price=10.5, high_price=11.0, low_price=9.5, close_price=10.0),
            make_bar(3, open_price=9.3, high_price=10.0, low_price=7.0, close_price=7.6),
            make_bar(4, open_price=8.0, high_price=9.2, low_price=8.1, close_price=8.4),
        ]
        found = self.fractal.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(found), 2)
        self.assertEqual(found[0].metadata.get("fractal_type"), FRACTAL_HIGH)
        self.assertEqual(found[1].metadata.get("fractal_type"), FRACTAL_LOW)

    def test_snr_appears_only_after_break_close(self) -> None:
        bars = [
            make_bar(0, open_price=8.8, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.2, high_price=12.0, low_price=9.2, close_price=9.15),
            make_bar(2, open_price=10.8, high_price=11.0, low_price=10.2, close_price=8.8),
            make_bar(3, open_price=8.9, high_price=9.1, low_price=8.95, close_price=8.7),
            make_bar(4, open_price=8.8, high_price=9.4, low_price=8.6, close_price=9.2),
        ]
        found = self.snr.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(found), 1)
        item = found[0]
        self.assertEqual(item.metadata.get("role"), ROLE_SUPPORT)
        self.assertEqual(item.metadata.get("break_type"), BREAK_UP_CLOSE)
        self.assertAlmostEqual(float(item.metadata.get("snr_low")), 8.95)
        self.assertAlmostEqual(float(item.metadata.get("snr_high")), 9.15)
        self.assertAlmostEqual(float(item.metadata.get("departure_extreme_price")), 8.95)
        self.assertIsNotNone(item.metadata.get("departure_extreme_time"))
        self.assertIsNotNone(item.metadata.get("departure_range_start_time"))
        self.assertIsNotNone(item.metadata.get("departure_range_end_time"))
        self.assertAlmostEqual(float(item.metadata.get("l_price_used")), 9.15)
        self.assertEqual(item.metadata.get("l_rule_used"), "bullish_C2close")

    def test_snr_status_moves_to_invalidated_after_retest(self) -> None:
        bars = [
            make_bar(0, open_price=8.8, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.2, high_price=12.0, low_price=9.2, close_price=9.15),
            make_bar(2, open_price=10.8, high_price=11.0, low_price=10.2, close_price=8.8),
            make_bar(3, open_price=8.9, high_price=9.1, low_price=8.95, close_price=8.7),
            make_bar(4, open_price=8.8, high_price=9.4, low_price=8.6, close_price=9.2),
            make_bar(5, open_price=9.2, high_price=9.3, low_price=8.98, close_price=9.05),
            make_bar(6, open_price=8.9, high_price=9.1, low_price=8.5, close_price=8.7),
        ]
        found = self.snr.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(found), 1)
        item = found[0]
        self.assertEqual(item.status, STATUS_INVALIDATED)
        self.assertIsNotNone(item.metadata.get("retest_time"))
        self.assertIsNotNone(item.metadata.get("invalidated_time"))

    def test_serialization_roundtrip_for_fractal_and_snr(self) -> None:
        bars = [
            make_bar(0, open_price=8.8, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.2, high_price=12.0, low_price=9.2, close_price=9.15),
            make_bar(2, open_price=10.8, high_price=11.0, low_price=10.2, close_price=8.8),
            make_bar(3, open_price=8.9, high_price=9.1, low_price=8.95, close_price=8.7),
            make_bar(4, open_price=8.8, high_price=9.4, low_price=8.6, close_price=9.2),
        ]
        fractals = self.fractal.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertGreaterEqual(len(fractals), 1)
        fractal_payload = fractals[0].to_dict()
        self.assertIn("fractal_type", fractal_payload)
        restored_fractal = TrackedElement.from_dict(fractal_payload)
        self.assertIsNotNone(restored_fractal)
        assert restored_fractal is not None
        self.assertEqual(restored_fractal.element_type, "fractal")

        snr_items = self.snr.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(snr_items), 1)
        snr_payload = snr_items[0].to_dict()
        self.assertIn("origin_fractal_id", snr_payload)
        restored_snr = TrackedElement.from_dict(snr_payload)
        self.assertIsNotNone(restored_snr)
        assert restored_snr is not None
        self.assertEqual(restored_snr.element_type, "snr")

    def test_snr_migrates_legacy_bounds_to_departure_extreme(self) -> None:
        bars = [
            make_bar(0, open_price=8.8, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.2, high_price=12.0, low_price=9.2, close_price=9.15),
            make_bar(2, open_price=10.8, high_price=11.0, low_price=10.2, close_price=8.8),
            make_bar(3, open_price=8.9, high_price=9.1, low_price=8.95, close_price=8.7),
            make_bar(4, open_price=8.8, high_price=9.4, low_price=8.6, close_price=9.2),
        ]
        current = self.snr.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )[0]

        legacy = TrackedElement.from_dict(current.to_dict())
        self.assertIsNotNone(legacy)
        assert legacy is not None
        legacy.zone_low = 9.0
        legacy.zone_high = 12.0
        legacy.zone_size = 3.0
        legacy.metadata["snr_low"] = 9.0
        legacy.metadata["snr_high"] = 12.0
        legacy.metadata.pop("departure_extreme_price", None)
        legacy.metadata.pop("departure_extreme_time", None)
        legacy.metadata.pop("departure_range_start_time", None)
        legacy.metadata.pop("departure_range_end_time", None)

        self.snr.update_status(
            element=legacy,
            bars=bars,
            config=self.config,
        )
        self.assertAlmostEqual(legacy.zone_low, 8.95)
        self.assertAlmostEqual(legacy.zone_high, 9.15)
        self.assertIsNotNone(legacy.metadata.get("departure_extreme_price"))
        self.assertIsNotNone(legacy.metadata.get("departure_extreme_time"))


if __name__ == "__main__":
    unittest.main()
