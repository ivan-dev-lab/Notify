from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from auto_eye.models import TrackedElement
from auto_eye.timeframe_files import TimeframeFileStore, TimeframeSnapshot


def make_fvg_element(symbol: str, timeframe: str, index: int) -> TrackedElement:
    base = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    c1 = base + timedelta(minutes=15 * index)
    c2 = c1 + timedelta(minutes=15)
    c3 = c2 + timedelta(minutes=15)
    return TrackedElement(
        id=f"{symbol}-{timeframe}-{index}",
        element_type="fvg",
        symbol=symbol,
        timeframe=timeframe,
        direction="bullish",
        formation_time=c3,
        zone_low=1.1,
        zone_high=1.2,
        zone_size=0.1,
        c1_time=c1,
        c2_time=c2,
        c3_time=c3,
    )


def make_fractal_element(symbol: str, timeframe: str, index: int) -> TrackedElement:
    base = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
    c1 = base + timedelta(minutes=15 * index)
    c2 = c1 + timedelta(minutes=15)
    c3 = c2 + timedelta(minutes=15)
    return TrackedElement(
        id=f"{symbol}-{timeframe}-fractal-{index}",
        element_type="fractal",
        symbol=symbol,
        timeframe=timeframe,
        direction="high",
        formation_time=c3,
        zone_low=1.1,
        zone_high=1.3,
        zone_size=0.2,
        c1_time=c1,
        c2_time=c2,
        c3_time=c3,
        metadata={
            "fractal_type": "high",
            "pivot_time": c2.isoformat(),
            "confirm_time": c3.isoformat(),
            "extreme_price": 1.3,
            "l_price": 1.1,
            "l_alt_price": 1.1,
        },
    )


class TimeframeFileStoreTests(unittest.TestCase):
    def test_save_writes_state_json_per_asset_and_load_reads_timeframe_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_json_path = Path(tmp_dir) / "auto_eye_zones.json"
            store = TimeframeFileStore(base_json_path)

            updated_at = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
            snapshot_m15 = TimeframeSnapshot(
                timeframe="M15",
                initialized=True,
                updated_at_utc=updated_at,
                last_bar_time_by_symbol={
                    "EURUSD": updated_at,
                    "GBPUSD": updated_at,
                },
                elements=[
                    make_fvg_element("EURUSD", "M15", 1),
                    make_fvg_element("GBPUSD", "M15", 2),
                ],
            )
            saved_paths = store.save(snapshot_m15)

            self.assertEqual({path.name for path in saved_paths}, {"EURUSD.json", "GBPUSD.json"})
            self.assertEqual(list(Path(tmp_dir).glob("*.csv")), [])
            self.assertEqual(len(list((Path(tmp_dir) / "State").glob("*.json"))), 2)
            self.assertTrue(all(path.parent.name == "State" for path in saved_paths))

            eur_path = Path(tmp_dir) / "State" / "EURUSD.json"
            with eur_path.open("r", encoding="utf-8") as file:
                eur_payload = json.load(file)
            self.assertEqual(eur_payload.get("symbol"), "EURUSD")
            self.assertIn("M15", eur_payload.get("timeframes", {}))
            m15_payload = eur_payload["timeframes"]["M15"]
            self.assertIn("elements", m15_payload)
            self.assertEqual(len(m15_payload["elements"]["fvg"]), 1)
            self.assertEqual(m15_payload["elements"]["snr"], [])
            self.assertEqual(m15_payload["elements"]["fractals"], [])
            self.assertTrue(m15_payload["state"]["initialized_elements"]["fvg"])

            snapshot_h1 = TimeframeSnapshot(
                timeframe="H1",
                initialized=True,
                updated_at_utc=updated_at + timedelta(hours=1),
                last_bar_time_by_symbol={"EURUSD": updated_at + timedelta(hours=1)},
                elements=[make_fvg_element("EURUSD", "H1", 3)],
            )
            store.save(snapshot_h1)

            loaded_m15 = store.load("M15", ["EURUSD", "GBPUSD"])
            self.assertTrue(loaded_m15.initialized)
            self.assertEqual(loaded_m15.timeframe, "M15")
            self.assertEqual(len(loaded_m15.elements), 2)
            self.assertEqual({item.symbol for item in loaded_m15.elements}, {"EURUSD", "GBPUSD"})
            self.assertEqual({item.timeframe for item in loaded_m15.elements}, {"M15"})
            self.assertEqual({item.element_type for item in loaded_m15.elements}, {"fvg"})

    def test_fractal_store_writes_into_state_without_overwriting_fvg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_json_path = Path(tmp_dir) / "auto_eye_zones.json"
            fvg_store = TimeframeFileStore(base_json_path)
            store = TimeframeFileStore(base_json_path, element_name="fractal")

            updated_at = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
            fvg_snapshot = TimeframeSnapshot(
                timeframe="M15",
                initialized=True,
                updated_at_utc=updated_at,
                last_bar_time_by_symbol={"EURUSD": updated_at},
                elements=[make_fvg_element("EURUSD", "M15", 1)],
            )
            fvg_store.save(fvg_snapshot)

            before = store.load("M15", ["EURUSD"])
            self.assertFalse(before.initialized)
            self.assertEqual(before.last_bar_time_by_symbol, {})
            self.assertEqual(before.elements, [])

            snapshot = TimeframeSnapshot(
                timeframe="M15",
                initialized=True,
                updated_at_utc=updated_at,
                last_bar_time_by_symbol={"EURUSD": updated_at},
                elements=[make_fractal_element("EURUSD", "M15", 1)],
            )
            paths = store.save(snapshot)

            self.assertEqual(len(paths), 1)
            self.assertEqual(paths[0].name, "EURUSD.json")
            self.assertEqual(paths[0].parent.name, "State")

            state_path = Path(tmp_dir) / "State" / "EURUSD.json"
            with state_path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            timeframe_payload = payload["timeframes"]["M15"]
            self.assertEqual(len(timeframe_payload["elements"]["fvg"]), 1)
            self.assertEqual(len(timeframe_payload["elements"]["fractals"]), 1)
            self.assertTrue(timeframe_payload["state"]["initialized_elements"]["fvg"])
            self.assertTrue(timeframe_payload["state"]["initialized_elements"]["fractals"])

            after = store.load("M15", ["EURUSD"])
            self.assertTrue(after.initialized)
            self.assertEqual(len(after.elements), 1)
            self.assertEqual(after.elements[0].element_type, "fractal")


if __name__ == "__main__":
    unittest.main()
