from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
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

from auto_eye.exporters import scenario_json_path, state_json_path, trend_json_path  # noqa: E402
from auto_eye.scenario_service import ScenarioSnapshotBuilder  # noqa: E402


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
            timeframes=["M5", "H1"],
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


def write_state(config: AppConfig, symbol: str, payload: dict[str, object]) -> None:
    path = state_json_path(Path(config.auto_eye.output_json), symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def write_trend(config: AppConfig, symbol: str, direction: str) -> None:
    path = trend_json_path(Path(config.auto_eye.output_json), symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "1.0.0",
        "symbol": symbol,
        "updated_at_utc": "2026-02-27T12:00:00+00:00",
        "trend": {
            "timeframe": "H1",
            "direction": direction,
            "determined_at_utc": "2026-02-27T12:00:00+00:00",
            "source_signal": {
                "type": "snr",
                "polarity": "positive" if direction == "bullish" else "negative",
                "signal_time_utc": "2026-02-27T11:00:00+00:00",
                "element_id": "trend-source",
            },
        },
        "history": [],
    }
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def read_scenarios(config: AppConfig, symbol: str) -> dict[str, object]:
    path = scenario_json_path(Path(config.auto_eye.output_json), symbol)
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


class ScenarioSnapshotBuilderTests(unittest.TestCase):
    def test_creates_trend_continuation_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 101.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "h1-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                    "fvg_low": 100.0,
                                    "fvg_high": 102.0,
                                }
                            ],
                            "snr": [
                                {
                                    "id": "h1-snr-target",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                                    "snr_low": 103.0,
                                    "snr_high": 104.0,
                                }
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:05:00+00:00",
                                    "fvg_low": 100.8,
                                    "fvg_high": 101.2,
                                }
                            ],
                            "snr": [],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            report = builder.build_all()

            self.assertEqual(report.files_updated, 1)
            self.assertEqual(report.scenarios_created, 1)

            scenarios = read_scenarios(config, "SPX500")
            active = scenarios["active"]
            self.assertEqual(len(active), 1)
            scenario = active[0]
            self.assertEqual(scenario["scenario_type"], "trend_continuation")
            self.assertEqual(scenario["direction"], "long")
            self.assertEqual(scenario["htf_anchor"]["type"], "h1_fvg")
            self.assertEqual(scenario["htf_anchor"]["element_id"], "h1-fvg-bull")
            self.assertAlmostEqual(scenario["sl"]["price"], 100.0)
            self.assertEqual(scenario["tp"]["target_element"]["type"], "h1_snr")
            self.assertEqual(scenario["tp"]["target_element"]["id"], "h1-snr-target")

    def test_creates_reversal_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 109.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [],
                            "snr": [
                                {
                                    "id": "h1-opposite-snr",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T10:00:00+00:00",
                                    "snr_low": 108.0,
                                    "snr_high": 110.0,
                                },
                                {
                                    "id": "h1-target-support",
                                    "role": "support",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T09:00:00+00:00",
                                    "snr_low": 105.0,
                                    "snr_high": 106.0,
                                },
                            ],
                            "rb": [
                                {
                                    "id": "h1-counter-rb",
                                    "rb_type": "high",
                                    "status": "active",
                                    "confirm_time_utc": "2026-02-27T11:00:00+00:00",
                                    "rb_low": 107.0,
                                    "rb_high": 109.0,
                                }
                            ],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [],
                            "snr": [
                                {
                                    "id": "m5-snr-bear",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T11:05:00+00:00",
                                    "snr_low": 108.2,
                                    "snr_high": 108.8,
                                }
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            report = builder.build_all()

            self.assertEqual(report.scenarios_created, 1)
            scenarios = read_scenarios(config, "SPX500")
            scenario = scenarios["active"][0]
            self.assertEqual(scenario["scenario_type"], "reversal_at_opposite")
            self.assertEqual(scenario["direction"], "short")
            self.assertEqual(scenario["htf_anchor"]["type"], "h1_rb")
            self.assertEqual(scenario["htf_anchor"]["element_id"], "h1-counter-rb")
            self.assertIn("opposite_touch", scenario["metadata"])

    def test_deduplicates_same_signal_on_next_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 101.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "h1-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                    "fvg_low": 100.0,
                                    "fvg_high": 102.0,
                                }
                            ],
                            "snr": [
                                {
                                    "id": "h1-snr-target",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                                    "snr_low": 103.0,
                                    "snr_high": 104.0,
                                }
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:05:00+00:00",
                                    "fvg_low": 100.8,
                                    "fvg_high": 101.2,
                                }
                            ],
                            "snr": [],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            first_report = builder.build_all()
            second_report = builder.build_all()

            self.assertEqual(first_report.scenarios_created, 1)
            self.assertEqual(second_report.scenarios_created, 0)
            self.assertEqual(second_report.files_updated, 0)
            self.assertEqual(second_report.files_unchanged, 1)

    def test_expires_old_pending_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 101.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "h1-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                    "fvg_low": 100.0,
                                    "fvg_high": 102.0,
                                }
                            ],
                            "snr": [
                                {
                                    "id": "h1-snr-target",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                                    "snr_low": 103.0,
                                    "snr_high": 104.0,
                                }
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:05:00+00:00",
                                    "fvg_low": 100.8,
                                    "fvg_high": 101.2,
                                }
                            ],
                            "snr": [],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            builder.build_all()

            path = scenario_json_path(Path(config.auto_eye.output_json), "SPX500")
            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            payload["active"][0]["expires_at_utc"] = "2000-01-01T00:00:00+00:00"
            with path.open("w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)

            second_report = builder.build_all()
            self.assertEqual(second_report.scenarios_expired, 1)

            scenarios = read_scenarios(config, "SPX500")
            self.assertEqual(len(scenarios["active"]), 0)
            self.assertEqual(len(scenarios["history"]), 1)
            self.assertEqual(scenarios["history"][0]["status"], "expired")


    def test_prefers_smallest_m5_snr_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 101.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "h1-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                    "fvg_low": 100.0,
                                    "fvg_high": 102.0,
                                }
                            ],
                            "snr": [
                                {
                                    "id": "h1-snr-target",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                                    "snr_low": 103.0,
                                    "snr_high": 104.0,
                                }
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-late",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:07:00+00:00",
                                    "fvg_low": 100.7,
                                    "fvg_high": 101.4,
                                }
                            ],
                            "snr": [
                                {
                                    "id": "m5-snr-large",
                                    "role": "support",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T10:06:00+00:00",
                                    "snr_low": 100.7,
                                    "snr_high": 101.3,
                                },
                                {
                                    "id": "m5-snr-small",
                                    "role": "support",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T10:05:00+00:00",
                                    "snr_low": 100.95,
                                    "snr_high": 101.0,
                                },
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            report = builder.build_all()

            self.assertEqual(report.scenarios_created, 1)
            scenarios = read_scenarios(config, "SPX500")
            scenario = scenarios["active"][0]
            self.assertEqual(scenario["ltf_confirmation"]["type"], "m5_snr")
            self.assertEqual(scenario["ltf_confirmation"]["element_id"], "m5-snr-small")

    def test_uses_retest_time_for_start_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 103.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [],
                            "snr": [
                                {
                                    "id": "h1-support-retested",
                                    "role": "support",
                                    "status": "retested",
                                    "break_time_utc": "2026-02-27T10:00:00+00:00",
                                    "retest_time_utc": "2026-02-27T10:30:00+00:00",
                                    "snr_low": 100.0,
                                    "snr_high": 101.0,
                                },
                                {
                                    "id": "h1-target-resistance",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T09:00:00+00:00",
                                    "snr_low": 104.0,
                                    "snr_high": 105.0,
                                },
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-before-retest",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:20:00+00:00",
                                    "fvg_low": 102.0,
                                    "fvg_high": 102.4,
                                },
                                {
                                    "id": "m5-fvg-after-retest",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:35:00+00:00",
                                    "fvg_low": 102.5,
                                    "fvg_high": 102.9,
                                },
                            ],
                            "snr": [],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            report = builder.build_all()

            self.assertEqual(report.scenarios_created, 1)
            scenarios = read_scenarios(config, "SPX500")
            scenario = scenarios["active"][0]
            self.assertEqual(
                scenario["htf_anchor"]["element_id"],
                "h1-support-retested",
            )
            self.assertEqual(
                scenario["ltf_confirmation"]["element_id"],
                "m5-fvg-after-retest",
            )


    def test_expires_when_scenario_reference_missing_in_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 101.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "h1-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                    "fvg_low": 100.0,
                                    "fvg_high": 102.0,
                                }
                            ],
                            "snr": [
                                {
                                    "id": "h1-snr-target",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                                    "snr_low": 103.0,
                                    "snr_high": 104.0,
                                }
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:05:00+00:00",
                                    "fvg_low": 100.8,
                                    "fvg_high": 101.2,
                                }
                            ],
                            "snr": [],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            first_report = builder.build_all()
            self.assertEqual(first_report.scenarios_created, 1)

            state_payload["timeframes"]["M5"]["elements"]["fvg"] = []
            write_state(config, "SPX500", state_payload)

            second_report = builder.build_all()
            self.assertEqual(second_report.scenarios_expired, 1)

            scenarios = read_scenarios(config, "SPX500")
            self.assertEqual(len(scenarios["active"]), 0)
            self.assertEqual(len(scenarios["history"]), 1)
            self.assertEqual(scenarios["history"][0]["status"], "expired")
            self.assertEqual(
                scenarios["history"][0]["metadata"]["expired_reason"],
                "missing_state_element",
            )

    def test_prefers_closest_h1_anchor_to_price(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 103.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [],
                            "snr": [
                                {
                                    "id": "h1-support-far-new",
                                    "role": "support",
                                    "status": "retested",
                                    "break_time_utc": "2026-02-27T09:00:00+00:00",
                                    "retest_time_utc": "2026-02-27T11:30:00+00:00",
                                    "snr_low": 99.0,
                                    "snr_high": 100.0,
                                },
                                {
                                    "id": "h1-support-near-old",
                                    "role": "support",
                                    "status": "retested",
                                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                                    "retest_time_utc": "2026-02-27T10:30:00+00:00",
                                    "snr_low": 102.8,
                                    "snr_high": 103.2,
                                },
                                {
                                    "id": "h1-resistance-target",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T07:00:00+00:00",
                                    "snr_low": 104.0,
                                    "snr_high": 105.0,
                                },
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T11:40:00+00:00",
                                    "fvg_low": 102.9,
                                    "fvg_high": 103.1,
                                }
                            ],
                            "snr": [],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            report = builder.build_all()

            self.assertEqual(report.scenarios_created, 1)
            scenarios = read_scenarios(config, "SPX500")
            scenario = scenarios["active"][0]
            self.assertEqual(
                scenario["htf_anchor"]["element_id"],
                "h1-support-near-old",
            )

    def test_does_not_duplicate_same_htf_ltf_pair_when_price_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            state_payload = {
                "symbol": "SPX500",
                "market": {"price": 101.0},
                "timeframes": {
                    "H1": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "h1-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:00:00+00:00",
                                    "fvg_low": 100.0,
                                    "fvg_high": 102.0,
                                }
                            ],
                            "snr": [
                                {
                                    "id": "h1-snr-target-a",
                                    "role": "resistance",
                                    "status": "active",
                                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                                    "snr_low": 103.0,
                                    "snr_high": 104.0,
                                }
                            ],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                    "M5": {
                        "elements": {
                            "fvg": [
                                {
                                    "id": "m5-fvg-bull",
                                    "direction": "bullish",
                                    "status": "active",
                                    "formation_time_utc": "2026-02-27T10:05:00+00:00",
                                    "fvg_low": 100.8,
                                    "fvg_high": 101.2,
                                }
                            ],
                            "snr": [],
                            "rb": [],
                            "fractals": [],
                        }
                    },
                },
            }
            write_state(config, "SPX500", state_payload)
            write_trend(config, "SPX500", "bullish")

            builder = ScenarioSnapshotBuilder(config=config)
            first_report = builder.build_all()
            self.assertEqual(first_report.scenarios_created, 1)

            scenarios_before = read_scenarios(config, "SPX500")
            self.assertEqual(len(scenarios_before["active"]), 1)
            initial_id = scenarios_before["active"][0]["scenario_id"]

            # Change market/TP conditions, but keep same HTF/LTF anchors.
            state_payload["market"]["price"] = 101.7
            state_payload["timeframes"]["H1"]["elements"]["snr"] = [
                {
                    "id": "h1-snr-target-a",
                    "role": "resistance",
                    "status": "active",
                    "break_time_utc": "2026-02-27T08:00:00+00:00",
                    "snr_low": 103.0,
                    "snr_high": 104.0,
                },
                {
                    "id": "h1-snr-target-b",
                    "role": "resistance",
                    "status": "active",
                    "break_time_utc": "2026-02-27T09:00:00+00:00",
                    "snr_low": 102.2,
                    "snr_high": 102.8,
                },
            ]
            write_state(config, "SPX500", state_payload)

            second_report = builder.build_all()
            self.assertEqual(second_report.scenarios_created, 0)

            scenarios_after = read_scenarios(config, "SPX500")
            self.assertEqual(len(scenarios_after["active"]), 1)
            self.assertEqual(scenarios_after["active"][0]["scenario_id"], initial_id)
            self.assertEqual(
                scenarios_after["active"][0]["htf_anchor"]["element_id"],
                "h1-fvg-bull",
            )
            self.assertEqual(
                scenarios_after["active"][0]["ltf_confirmation"]["element_id"],
                "m5-fvg-bull",
            )

    def test_collapse_overlapping_snr_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)
            builder = ScenarioSnapshotBuilder(config=config)

            base = datetime(2026, 2, 27, 10, 0, tzinfo=timezone.utc)
            elements = [
                {
                    "id": "snr-overlap-old",
                    "type": "snr",
                    "zone_low": 100.0,
                    "zone_high": 101.0,
                    "zone_size": 1.0,
                    "signal_dt": base,
                    "start_dt": base,
                },
                {
                    "id": "snr-overlap-new",
                    "type": "snr",
                    "zone_low": 100.2,
                    "zone_high": 101.2,
                    "zone_size": 1.0,
                    "signal_dt": base.replace(hour=11),
                    "start_dt": base.replace(hour=11),
                },
                {
                    "id": "snr-standalone",
                    "type": "snr",
                    "zone_low": 103.0,
                    "zone_high": 104.0,
                    "zone_size": 1.0,
                    "signal_dt": base,
                    "start_dt": base,
                },
                {
                    "id": "fvg-other",
                    "type": "fvg",
                    "zone_low": 100.5,
                    "zone_high": 100.9,
                    "zone_size": 0.4,
                    "signal_dt": base,
                    "start_dt": base,
                },
            ]

            collapsed = builder._collapse_overlapping_snr(
                elements=elements,
                price=100.8,
                prefer_smallest_zone=False,
            )
            snr_ids = {item["id"] for item in collapsed if item["type"] == "snr"}
            self.assertSetEqual(snr_ids, {"snr-overlap-new", "snr-standalone"})

if __name__ == "__main__":
    unittest.main()


