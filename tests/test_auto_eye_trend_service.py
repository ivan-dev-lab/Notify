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

from config_loader import (  # noqa: E402
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

from auto_eye.exporters import state_json_path, trend_json_path  # noqa: E402
from auto_eye.trend_service import TrendSnapshotBuilder  # noqa: E402


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
            timeframes=["M5", "M15", "H1"],
            elements=["fvg", "snr", "fractal", "rb"],
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


def write_state(config: AppConfig, symbol: str, payload: dict[str, object]) -> Path:
    state_path = state_json_path(Path(config.auto_eye.output_json), symbol)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with state_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    return state_path


def read_trend(config: AppConfig, symbol: str) -> dict[str, object]:
    trend_path = trend_json_path(Path(config.auto_eye.output_json), symbol)
    with trend_path.open("r", encoding="utf-8") as file:
        return json.load(file)


class TrendSnapshotBuilderTests(unittest.TestCase):
    def test_uses_latest_h1_signal_across_fvg_and_snr(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "fvg-old",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T09:00:00+00:00",
                                }
                            ],
                            "snr": [
                                {
                                    "id": "snr-new",
                                    "role": "resistance",
                                    "break_type": "break_down_close",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T10:00:00+00:00",
                                }
                            ],
                            "fractals": [],
                            "rb": [],
                        }
                    }
                },
            }
            write_state(config, "SPX500", state_payload)

            builder = TrendSnapshotBuilder(config=config)
            report = builder.build_all()
            self.assertEqual(report.files_updated, 1)

            trend = read_trend(config, "SPX500")
            trend_block = trend["trend"]
            self.assertEqual(trend_block["direction"], "bearish")
            source = trend_block["source_signal"]
            self.assertEqual(source["type"], "snr")
            self.assertEqual(source["polarity"], "negative")
            self.assertEqual(source["element_id"], "snr-new")

    def test_returns_neutral_without_valid_h1_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "fvg-invalid",
                                    "direction": "bullish",
                                    "status": "mitigated_full",
                                    "formation_time_utc": "2026-02-27T09:00:00+00:00",
                                }
                            ],
                            "snr": [
                                {
                                    "id": "snr-invalid",
                                    "role": "support",
                                    "break_type": "break_up_close",
                                    "status": "invalidated",
                                    "break_time_utc": "2026-02-27T10:00:00+00:00",
                                }
                            ],
                            "fractals": [],
                            "rb": [],
                        }
                    }
                },
            }
            write_state(config, "SPX500", state_payload)

            builder = TrendSnapshotBuilder(config=config)
            builder.build_all()

            trend = read_trend(config, "SPX500")
            self.assertEqual(trend["trend"]["direction"], "neutral")
            self.assertIsNone(trend["trend"]["source_signal"])

    def test_rewrites_only_when_material_fields_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            first_state = {
                "symbol": "SPX500",
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "fvg-1",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T09:00:00+00:00",
                                }
                            ],
                            "snr": [],
                            "fractals": [],
                            "rb": [],
                        }
                    }
                },
            }
            write_state(config, "SPX500", first_state)

            builder = TrendSnapshotBuilder(config=config)
            first_report = builder.build_all()
            self.assertEqual(first_report.files_updated, 1)

            second_report = builder.build_all()
            self.assertEqual(second_report.files_updated, 0)
            self.assertEqual(second_report.files_unchanged, 1)

            updated_state_same_direction = {
                "symbol": "SPX500",
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "fvg-2",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                }
                            ],
                            "snr": [],
                            "fractals": [],
                            "rb": [],
                        }
                    }
                },
            }
            write_state(config, "SPX500", updated_state_same_direction)

            third_report = builder.build_all()
            self.assertEqual(third_report.files_updated, 1)

            trend = read_trend(config, "SPX500")
            self.assertEqual(trend["trend"]["direction"], "bullish")
            self.assertEqual(trend["trend"]["source_signal"]["element_id"], "fvg-2")
            self.assertEqual(len(trend["history"]), 0)

    def test_appends_history_when_direction_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            bullish_state = {
                "symbol": "SPX500",
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T09:00:00+00:00",
                                }
                            ],
                            "snr": [],
                            "fractals": [],
                            "rb": [],
                        }
                    }
                },
            }
            write_state(config, "SPX500", bullish_state)

            builder = TrendSnapshotBuilder(config=config)
            builder.build_all()

            bearish_state = {
                "symbol": "SPX500",
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "fvg-bear",
                                    "direction": "bearish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                }
                            ],
                            "snr": [],
                            "fractals": [],
                            "rb": [],
                        }
                    }
                },
            }
            write_state(config, "SPX500", bearish_state)
            builder.build_all()

            trend = read_trend(config, "SPX500")
            self.assertEqual(trend["trend"]["direction"], "bearish")
            self.assertEqual(len(trend["history"]), 1)
            self.assertEqual(trend["history"][0]["direction"], "bearish")
            self.assertEqual(
                trend["history"][0]["source_signal"]["element_id"],
                "fvg-bear",
            )


if __name__ == "__main__":
    unittest.main()

