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

from auto_eye.detectors.fvg import BEARISH, BULLISH, FVGDetector
from auto_eye.models import OHLCBar, STATUS_MITIGATED_FULL, STATUS_MITIGATED_PARTIAL


def build_config() -> AutoEyeConfig:
    return AutoEyeConfig(
        enabled=True,
        symbols=["EURUSD"],
        timeframes=["M5"],
        elements=["fvg"],
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
        time=base + timedelta(minutes=5 * index),
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        tick_volume=100,
    )


class FVGDetectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.detector = FVGDetector()
        self.config = build_config()

    def test_bullish_detected_only_when_high_c1_less_than_low_c3(self) -> None:
        bars = [
            make_bar(0, open_price=9.4, high_price=10.0, low_price=9.0, close_price=9.8),
            make_bar(1, open_price=9.8, high_price=10.4, low_price=9.6, close_price=10.2),
            make_bar(2, open_price=11.1, high_price=11.4, low_price=11.0, close_price=11.3),
        ]
        result = self.detector.detect(
            symbol="EURUSD",
            timeframe="M5",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].direction, BULLISH)
        self.assertAlmostEqual(result[0].zone_low, 10.0)
        self.assertAlmostEqual(result[0].zone_high, 11.0)

        # Equality must not create FVG because rule is strict (<).
        bars_equal = [
            make_bar(0, open_price=9.4, high_price=10.0, low_price=9.0, close_price=9.8),
            make_bar(1, open_price=9.8, high_price=10.4, low_price=9.6, close_price=10.2),
            make_bar(2, open_price=10.0, high_price=11.0, low_price=10.0, close_price=10.5),
        ]
        result_equal = self.detector.detect(
            symbol="EURUSD",
            timeframe="M5",
            bars=bars_equal,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(result_equal), 0)

    def test_bearish_detected_only_when_low_c1_greater_than_high_c3(self) -> None:
        bars = [
            make_bar(0, open_price=12.2, high_price=12.5, low_price=12.0, close_price=12.1),
            make_bar(1, open_price=12.0, high_price=12.2, low_price=11.6, close_price=11.8),
            make_bar(2, open_price=10.7, high_price=11.0, low_price=10.5, close_price=10.6),
        ]
        result = self.detector.detect(
            symbol="EURUSD",
            timeframe="M5",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].direction, BEARISH)
        self.assertAlmostEqual(result[0].zone_low, 11.0)
        self.assertAlmostEqual(result[0].zone_high, 12.0)

        bars_equal = [
            make_bar(0, open_price=12.2, high_price=12.5, low_price=11.0, close_price=12.1),
            make_bar(1, open_price=12.0, high_price=12.2, low_price=11.6, close_price=11.8),
            make_bar(2, open_price=10.7, high_price=11.0, low_price=10.5, close_price=10.6),
        ]
        result_equal = self.detector.detect(
            symbol="EURUSD",
            timeframe="M5",
            bars=bars_equal,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(result_equal), 0)

    def test_status_switches_to_mitigated_full(self) -> None:
        bars = [
            make_bar(0, open_price=9.4, high_price=10.0, low_price=9.0, close_price=9.8),
            make_bar(1, open_price=9.8, high_price=10.4, low_price=9.6, close_price=10.2),
            make_bar(2, open_price=11.1, high_price=11.4, low_price=11.0, close_price=11.3),
            # Price comes back and fully fills bullish FVG low border.
            make_bar(3, open_price=10.8, high_price=11.1, low_price=9.95, close_price=10.0),
        ]
        detected = self.detector.detect(
            symbol="EURUSD",
            timeframe="M5",
            bars=bars[:3],
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(detected), 1)
        element = detected[0]

        updated = self.detector.update_status(
            element=element,
            bars=bars,
            config=self.config,
        )
        self.assertEqual(updated.status, STATUS_MITIGATED_FULL)
        self.assertIsNotNone(updated.mitigated_time)

    def test_status_switches_to_mitigated_partial(self) -> None:
        bars = [
            make_bar(0, open_price=9.4, high_price=10.0, low_price=9.0, close_price=9.8),
            make_bar(1, open_price=9.8, high_price=10.4, low_price=9.6, close_price=10.2),
            make_bar(2, open_price=11.1, high_price=11.4, low_price=11.0, close_price=11.3),
            # Touches and partially fills, but not fully.
            make_bar(3, open_price=10.9, high_price=11.1, low_price=10.6, close_price=10.8),
        ]
        detected = self.detector.detect(
            symbol="EURUSD",
            timeframe="M5",
            bars=bars[:3],
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(detected), 1)
        element = detected[0]

        updated = self.detector.update_status(
            element=element,
            bars=bars,
            config=self.config,
        )
        self.assertEqual(updated.status, STATUS_MITIGATED_PARTIAL)
        self.assertIsNotNone(updated.touched_time)


    def test_boundary_wick_mitigates_full_even_with_touch_rule(self) -> None:
        self.config.fill_rule = "touch"
        bars = [
            make_bar(0, open_price=9.4, high_price=10.0, low_price=9.0, close_price=9.8),
            make_bar(1, open_price=9.8, high_price=10.4, low_price=9.6, close_price=10.2),
            make_bar(2, open_price=11.1, high_price=11.4, low_price=11.0, close_price=11.3),
            # Bullish FVG lower border (10.0) is broken by wick.
            make_bar(3, open_price=10.8, high_price=11.0, low_price=9.95, close_price=10.6),
        ]
        detected = self.detector.detect(
            symbol="EURUSD",
            timeframe="M5",
            bars=bars[:3],
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(detected), 1)
        element = detected[0]

        updated = self.detector.update_status(
            element=element,
            bars=bars,
            config=self.config,
        )
        self.assertEqual(updated.status, STATUS_MITIGATED_FULL)
        self.assertIsNotNone(updated.mitigated_time)


if __name__ == "__main__":
    unittest.main()
