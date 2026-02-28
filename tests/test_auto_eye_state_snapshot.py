from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config_loader import (
    AppConfig,
    AutoEyeConfig,
    BrowserConfig,
    LoggingConfig,
    MetaTraderConfig,
    ScraperConfig,
    AutoEyeNotificationsConfig,
    TelegramBacktestConfig,
    TelegramConfig,
)

from auto_eye.state_snapshot import REQUIRED_TIMEFRAMES, STATE_SCHEMA_VERSION, StateSnapshotBuilder


class FakeSource:
    def __init__(self) -> None:
        self._connected = False

    def connect(self) -> None:
        self._connected = True

    def close(self) -> None:
        self._connected = False

    def resolve_symbol(self, asset_or_symbol: str) -> str:
        return str(asset_or_symbol).strip().upper()

    def get_market_quote(self, symbol: str) -> dict[str, object] | None:
        return {
            "price": 6858.4,
            "bid": 6858.3,
            "ask": 6858.5,
            "source": "MT5",
            "tick_time_utc": "2026-02-23T09:22:58+00:00",
        }


def build_config(output_root: Path) -> AppConfig:
    return AppConfig(
        url="",
        browser=BrowserConfig(
            name="chrome",
            headless=True,
            implicit_wait=1,
            page_load_timeout=1,
        ),
        scraper=ScraperConfig(
            assets=["SPX500"],
            output_json=str(output_root / "forex_quotes.json"),
            symbol_map={},
        ),
        metatrader=MetaTraderConfig(
            login=0,
            password="",
            server="",
            terminal_path="",
            timeout_ms=1000,
        ),
        telegram=TelegramConfig(
            bot_token="",
            check_interval_seconds=10,
            alerts_json=str(output_root / "alerts.json"),
            allowed_user_ids=[],
            auto_eye_notifications=AutoEyeNotificationsConfig(
                enabled=False,
                timeframes=[],
                elements=[],
                state_dir="",
                seen_ids_json=str(output_root / "auto_eye_notified_elements.json"),
            ),
            backtest=TelegramBacktestConfig(
                enabled=False,
                allowed_user_ids=[],
                max_interval_hours=24,
                warmup_bars=100,
                max_proposals_to_send=5,
            ),
        ),
        logging=LoggingConfig(
            level="INFO",
            file=str(output_root / "notify.log"),
            max_bytes=1000,
            backup_count=1,
        ),
        auto_eye=AutoEyeConfig(
            enabled=True,
            symbols=["SPX500"],
            timeframes=list(REQUIRED_TIMEFRAMES),
            elements=["fvg", "fractal", "snr", "rb"],
            history_days=30,
            history_buffer_days=5,
            incremental_bars=500,
            update_interval_seconds=300,
            scheduler_poll_seconds=60,
            output_json=str(output_root / "auto_eye_zones.json"),
            output_csv=str(output_root / "auto_eye_zones.csv"),
            state_json=str(output_root / "auto_eye_state.json"),
            min_gap_points=0.0,
            require_displacement=False,
            displacement_k=1.5,
            atr_period=14,
            median_body_period=20,
            fill_rule="both",
            snr_departure_start="pivot",
            snr_include_break_candle=False,
        ),
    )


class StateSnapshotBuilderTests(unittest.TestCase):
    def test_normalizes_existing_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_path = Path(tmp_dir) / "Exchange" / "State" / "SPX500.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            existing_state = {
                "symbol": "SPX500",
                "updated_at_utc": "2026-02-23T09:20:00+00:00",
                "market": {
                    "price": 6800.0,
                    "tick_time_utc": "2026-02-23T09:20:00+00:00",
                    "source": "MT5",
                },
                "timeframes": {
                    "H4": {
                        "initialized": True,
                        "updated_at_utc": "2026-02-23T09:20:00+00:00",
                        "last_bar_time_utc": "2026-02-23T08:00:00+00:00",
                        "elements": {
                            "snr": [
                                {
                                    "id": "snr-1",
                                    "element_type": "snr",
                                    "symbol": "SPX500",
                                    "timeframe": "H4",
                                    "break_time_utc": "2026-02-23T04:00:00+00:00",
                                    "snr_low": 6859.1,
                                    "snr_high": 6865.57,
                                    "status": "active",
                                },
                                {
                                    "id": "snr-2",
                                    "element_type": "snr",
                                    "symbol": "SPX500",
                                    "timeframe": "H4",
                                    "break_time_utc": "2026-02-22T04:00:00+00:00",
                                    "snr_low": 6800.0,
                                    "snr_high": 6810.0,
                                    "status": "invalidated",
                                }
                            ]
                        },
                    }
                },
                "derived": {
                    "htf_bias": {"direction": "bullish", "reason_ids": ["snr:snr-1"]},
                    "global_blocks": {"no_long": [], "no_short": []},
                },
                "scenarios": {"transition": [{"id": "t-1"}], "deals": []},
            }
            with state_path.open("w", encoding="utf-8") as file:
                json.dump(existing_state, file, ensure_ascii=False, indent=2)

            builder = StateSnapshotBuilder(config=config, source=FakeSource())
            report_first = builder.build_all()
            self.assertEqual(report_first.symbols_processed, 1)
            self.assertEqual(report_first.files_updated, 1)

            with state_path.open("r", encoding="utf-8") as file:
                state = json.load(file)

            self.assertEqual(state.get("schema_version"), STATE_SCHEMA_VERSION)
            self.assertEqual(state.get("symbol"), "SPX500")
            self.assertEqual(state.get("market", {}).get("price"), 6858.4)
            self.assertEqual(
                set(REQUIRED_TIMEFRAMES).issubset(set(state.get("timeframes", {}).keys())),
                True,
            )

            h4 = state["timeframes"]["H4"]
            self.assertTrue(h4.get("initialized"))
            self.assertIn("fvg", h4["elements"])
            self.assertIn("snr", h4["elements"])
            self.assertIn("fractals", h4["elements"])
            self.assertIn("rb", h4["elements"])
            self.assertEqual(h4["elements"]["snr"][0]["id"], "snr-1")
            self.assertEqual(len(h4["elements"]["snr"]), 1)

            self.assertNotIn("derived", state)
            self.assertNotIn("scenarios", state)

            schema_path = Path(tmp_dir) / "Exchange" / "State" / "schema_version.json"
            self.assertTrue(schema_path.exists())
            self.assertTrue((Path(tmp_dir) / "Exchange" / "Actions").exists())
            self.assertTrue((Path(tmp_dir) / "Exchange" / "Decisions").exists())

            report_second = builder.build_all()
            self.assertEqual(report_second.files_updated, 0)
            self.assertEqual(report_second.files_unchanged, 1)


if __name__ == "__main__":
    unittest.main()



