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

from auto_eye.backtest_service import BacktestScenarioRunner  # noqa: E402
from auto_eye.models import (  # noqa: E402
    OHLCBar,
    STATUS_ACTIVE,
    STATUS_RETESTED,
    TrackedElement,
    datetime_to_iso,
)


class FakeSource:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._connected = False

    def connect(self) -> None:
        self._connected = True

    def close(self) -> None:
        self._connected = False

    def resolve_symbol(self, raw: str) -> str:
        return str(raw).strip().upper()

    def get_point_size(self, symbol: str) -> float:
        _ = symbol
        return 0.01

    def fetch_range(
        self,
        *,
        symbol: str,
        timeframe_code: str,
        start_time_utc: datetime,
        end_time_utc: datetime,
    ) -> list[OHLCBar] | None:
        _ = symbol
        _ = start_time_utc
        _ = end_time_utc
        base = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

        if timeframe_code == "M5":
            return [
                OHLCBar(time=base, open=100.2, high=100.8, low=99.9, close=100.4),
                OHLCBar(
                    time=base.replace(minute=5),
                    open=100.4,
                    high=100.9,
                    low=100.1,
                    close=100.6,
                ),
                OHLCBar(
                    time=base.replace(minute=10),
                    open=100.6,
                    high=101.0,
                    low=100.4,
                    close=100.7,
                ),
            ]

        if timeframe_code == "H1":
            return [
                OHLCBar(
                    time=datetime(2025, 12, 31, 22, 0, tzinfo=timezone.utc),
                    open=100.0,
                    high=101.0,
                    low=99.5,
                    close=100.0,
                ),
                OHLCBar(
                    time=datetime(2025, 12, 31, 23, 0, tzinfo=timezone.utc),
                    open=100.0,
                    high=101.0,
                    low=99.5,
                    close=100.2,
                ),
                OHLCBar(
                    time=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                    open=100.2,
                    high=101.2,
                    low=99.8,
                    close=100.6,
                ),
            ]

        return []


class FakeSNRDetector:
    element_type = "snr"

    def detect(self, *, symbol, timeframe, bars, point_size, config):
        _ = point_size
        _ = config
        if len(bars) < 3:
            return []

        if timeframe == "H1":
            ts = bars[2].time
            return [
                TrackedElement(
                    id="h1-snr-support",
                    element_type="snr",
                    symbol=symbol,
                    timeframe=timeframe,
                    direction="support",
                    formation_time=ts,
                    zone_low=100.0,
                    zone_high=101.0,
                    zone_size=1.0,
                    c1_time=ts,
                    c2_time=ts,
                    c3_time=ts,
                    status=STATUS_RETESTED,
                    touched_time=ts,
                    metadata={
                        "role": "support",
                        "break_type": "break_up_close",
                        "break_time": datetime_to_iso(ts),
                        "snr_low": 100.0,
                        "snr_high": 101.0,
                        "retest_time": datetime_to_iso(ts),
                        "origin_fractal_id": "f1",
                    },
                )
            ]

        if timeframe == "M5":
            trigger = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
            if bars[-1].time < trigger:
                return []
            return [
                TrackedElement(
                    id="m5-snr-confirm",
                    element_type="snr",
                    symbol=symbol,
                    timeframe=timeframe,
                    direction="support",
                    formation_time=trigger,
                    zone_low=100.55,
                    zone_high=100.60,
                    zone_size=0.05,
                    c1_time=trigger,
                    c2_time=trigger,
                    c3_time=trigger,
                    status=STATUS_ACTIVE,
                    metadata={
                        "role": "support",
                        "break_type": "break_up_close",
                        "break_time": datetime_to_iso(trigger),
                        "snr_low": 100.55,
                        "snr_high": 100.60,
                        "origin_fractal_id": "f2",
                    },
                )
            ]

        return []

    def update_status(self, *, element, bars, config):
        _ = bars
        _ = config
        return element


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
            elements=["snr"],
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


class BacktestScenarioRunnerTests(unittest.TestCase):
    def test_backtest_generates_single_proposal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "Speculator" / "output"
            output_root.mkdir(parents=True, exist_ok=True)
            config = build_config(output_root)

            runner = BacktestScenarioRunner(
                config=config,
                detectors={"snr": FakeSNRDetector()},
                source=FakeSource(config),
            )

            report = runner.run(
                start_time_utc=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                end_time_utc=datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
                symbols=["SPX500"],
                run_id="unit_backtest",
                warmup_bars=10,
            )

            self.assertEqual(report.proposals_created, 1)
            self.assertEqual(report.errors, [])

            proposals_path = report.output_dir / "proposals.jsonl"
            self.assertTrue(proposals_path.exists())
            lines = proposals_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 1)

            proposal = json.loads(lines[0])
            self.assertEqual(proposal["run_id"], "unit_backtest")
            self.assertEqual(proposal["symbol"], "SPX500")


if __name__ == "__main__":
    unittest.main()

