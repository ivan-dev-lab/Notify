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


def make_element(symbol: str, timeframe: str, index: int) -> TrackedElement:
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


class TimeframeFileStoreTests(unittest.TestCase):
    def test_save_writes_one_json_per_asset_and_load_reads_timeframe_slice(self) -> None:
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
                    make_element("EURUSD", "M15", 1),
                    make_element("GBPUSD", "M15", 2),
                ],
            )
            saved_paths = store.save(snapshot_m15)

            self.assertEqual({path.name for path in saved_paths}, {"EURUSD.json", "GBPUSD.json"})
            self.assertEqual(list(Path(tmp_dir).glob("*.csv")), [])
            self.assertEqual(len(list((Path(tmp_dir) / "FVG").glob("*.json"))), 2)

            eur_path = Path(tmp_dir) / "FVG" / "EURUSD.json"
            with eur_path.open("r", encoding="utf-8") as file:
                eur_payload = json.load(file)
            self.assertEqual(eur_payload.get("symbol"), "EURUSD")
            self.assertIn("M15", eur_payload.get("timeframes", {}))

            snapshot_h1 = TimeframeSnapshot(
                timeframe="H1",
                initialized=True,
                updated_at_utc=updated_at + timedelta(hours=1),
                last_bar_time_by_symbol={"EURUSD": updated_at + timedelta(hours=1)},
                elements=[make_element("EURUSD", "H1", 3)],
            )
            store.save(snapshot_h1)

            loaded_m15 = store.load("M15", ["EURUSD", "GBPUSD"])
            self.assertTrue(loaded_m15.initialized)
            self.assertEqual(loaded_m15.timeframe, "M15")
            self.assertEqual(len(loaded_m15.elements), 2)
            self.assertEqual({item.symbol for item in loaded_m15.elements}, {"EURUSD", "GBPUSD"})
            self.assertEqual({item.timeframe for item in loaded_m15.elements}, {"M15"})


if __name__ == "__main__":
    unittest.main()
