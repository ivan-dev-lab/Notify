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

from auto_eye.detectors.fractal import FractalDetector
from auto_eye.detectors.rb import RBDetector
from auto_eye.models import STATUS_BROKEN, OHLCBar, TrackedElement


def build_config() -> AutoEyeConfig:
    return AutoEyeConfig(
        enabled=True,
        symbols=["EURUSD"],
        timeframes=["M15"],
        elements=["rb", "fractal"],
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


class RBDetectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = build_config()
        self.fractal = FractalDetector()
        self.rb = RBDetector()

    def test_rb_detects_from_confirmed_fractal(self) -> None:
        bars = [
            make_bar(0, open_price=9.0, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.4, high_price=12.0, low_price=9.0, close_price=11.0),
            make_bar(2, open_price=10.5, high_price=11.0, low_price=9.5, close_price=10.0),
        ]
        fractals = self.fractal.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        found = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(fractals), 1)
        self.assertEqual(len(found), 1)
        item = found[0]
        self.assertEqual(item.element_type, "rb")
        self.assertEqual(item.metadata.get("rb_type"), "high")
        self.assertEqual(item.metadata.get("origin_fractal_id"), fractals[0].id)
        self.assertAlmostEqual(float(item.metadata.get("l_price")), 9.0)
        self.assertAlmostEqual(float(item.metadata.get("extreme_price")), 12.0)
        self.assertAlmostEqual(float(item.metadata.get("rb_low")), 9.0)
        self.assertAlmostEqual(float(item.metadata.get("rb_high")), 12.0)
        self.assertAlmostEqual(float(item.metadata.get("l_price_used")), 9.0)
        self.assertEqual(item.metadata.get("l_rule_used"), "bearish_C1close")

    def test_rb_id_is_stable_between_runs(self) -> None:
        bars = [
            make_bar(0, open_price=9.0, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.4, high_price=12.0, low_price=9.0, close_price=11.0),
            make_bar(2, open_price=10.5, high_price=11.0, low_price=9.5, close_price=10.0),
        ]
        first = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        second = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual([item.id for item in first], [item.id for item in second])

    def test_rb_moves_to_broken_up_on_close_above_zone(self) -> None:
        bars = [
            make_bar(0, open_price=9.0, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.4, high_price=12.0, low_price=9.0, close_price=11.0),
            make_bar(2, open_price=10.5, high_price=11.0, low_price=9.5, close_price=10.0),
            make_bar(3, open_price=11.6, high_price=12.8, low_price=11.2, close_price=12.4),
        ]
        found = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(found), 1)
        item = found[0]
        self.rb.update_status(element=item, bars=bars, config=self.config)
        self.assertEqual(item.status, STATUS_BROKEN)
        self.assertEqual(item.metadata.get("broken_side"), "up")
        self.assertEqual(item.metadata.get("broken_time"), bars[3].time.isoformat())

    def test_rb_moves_to_broken_down_on_close_below_zone(self) -> None:
        bars = [
            make_bar(0, open_price=10.0, high_price=10.5, low_price=9.8, close_price=10.0),
            make_bar(1, open_price=9.9, high_price=10.0, low_price=8.0, close_price=8.5),
            make_bar(2, open_price=8.6, high_price=9.2, low_price=8.4, close_price=8.9),
            make_bar(3, open_price=8.8, high_price=9.0, low_price=7.5, close_price=7.8),
        ]
        found = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(found), 1)
        item = found[0]
        self.assertEqual(item.metadata.get("rb_type"), "low")
        self.assertAlmostEqual(float(item.metadata.get("l_price_used")), 8.5)
        self.assertEqual(item.metadata.get("l_rule_used"), "bullish_C2close")
        self.rb.update_status(element=item, bars=bars, config=self.config)
        self.assertEqual(item.status, STATUS_BROKEN)
        self.assertEqual(item.metadata.get("broken_side"), "down")
        self.assertEqual(item.metadata.get("broken_time"), bars[3].time.isoformat())

    def test_rb_serialization_roundtrip(self) -> None:
        bars = [
            make_bar(0, open_price=9.0, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.4, high_price=12.0, low_price=9.0, close_price=11.0),
            make_bar(2, open_price=10.5, high_price=11.0, low_price=9.5, close_price=10.0),
        ]
        item = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )[0]
        payload = item.to_dict()
        restored = TrackedElement.from_dict(payload)
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.element_type, "rb")
        self.assertEqual(restored.id, item.id)
        self.assertEqual(
            restored.metadata.get("origin_fractal_id"),
            item.metadata.get("origin_fractal_id"),
        )


    def test_rb_bearish_breaks_on_wick_above_upper_border(self) -> None:
        bars = [
            make_bar(0, open_price=9.0, high_price=10.0, low_price=8.0, close_price=9.0),
            make_bar(1, open_price=9.4, high_price=12.0, low_price=9.0, close_price=11.0),
            make_bar(2, open_price=10.5, high_price=11.0, low_price=9.5, close_price=10.0),
            # Upper wick breaks rb_high=12.0, close stays below 12.0.
            make_bar(3, open_price=11.4, high_price=12.2, low_price=11.1, close_price=11.8),
        ]
        found = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(found), 1)
        item = found[0]

        self.rb.update_status(element=item, bars=bars, config=self.config)
        self.assertEqual(item.status, STATUS_BROKEN)
        self.assertEqual(item.metadata.get("broken_side"), "up")
        self.assertEqual(item.metadata.get("broken_time"), bars[3].time.isoformat())

    def test_rb_bullish_breaks_on_wick_below_lower_border(self) -> None:
        bars = [
            make_bar(0, open_price=10.0, high_price=10.5, low_price=9.8, close_price=10.0),
            make_bar(1, open_price=9.9, high_price=10.0, low_price=8.0, close_price=8.5),
            make_bar(2, open_price=8.6, high_price=9.2, low_price=8.4, close_price=8.9),
            # Lower wick breaks rb_low=8.0, close stays above 8.0.
            make_bar(3, open_price=8.2, high_price=8.9, low_price=7.9, close_price=8.1),
        ]
        found = self.rb.detect(
            symbol="EURUSD",
            timeframe="M15",
            bars=bars,
            point_size=0.0001,
            config=self.config,
        )
        self.assertEqual(len(found), 1)
        item = found[0]

        self.rb.update_status(element=item, bars=bars, config=self.config)
        self.assertEqual(item.status, STATUS_BROKEN)
        self.assertEqual(item.metadata.get("broken_side"), "down")
        self.assertEqual(item.metadata.get("broken_time"), bars[3].time.isoformat())


if __name__ == "__main__":
    unittest.main()
