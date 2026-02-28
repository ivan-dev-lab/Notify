from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

from app_logging import configure_logging
from auto_eye.backtest_service import BacktestScenarioRunner
from auto_eye.detectors.base import MarketElementDetector
from auto_eye.detectors.registry import build_detectors
from auto_eye.models import datetime_from_iso
from auto_eye.scenario_service import ScenarioSnapshotBuilder
from auto_eye.state_snapshot import StateSnapshotBuilder
from auto_eye.timeframe_service import TimeframeUpdateReport, TimeframeUpdateService
from auto_eye.trend_service import TrendSnapshotBuilder
from config_loader import load_config

logger = logging.getLogger(__name__)


def summarize_reports(reports: list[TimeframeUpdateReport]) -> dict[str, object]:
    return {
        "timeframes_processed": len(reports),
        "files_updated": sum(1 for report in reports if report.file_updated),
        "timeframes_skipped_no_data": sum(
            1 for report in reports if report.skipped_no_data
        ),
        "new_elements": sum(report.new_count for report in reports),
        "status_updates": sum(report.status_updated_count for report in reports),
        "active_total": sum(report.total_active for report in reports),
    }


def merge_state_summary(
    payload: dict[str, object],
    *,
    state_files_updated: int,
    state_files_unchanged: int,
    state_errors: list[str],
) -> dict[str, object]:
    merged = dict(payload)
    merged["state_files_updated"] = state_files_updated
    merged["state_files_unchanged"] = state_files_unchanged
    merged["state_errors"] = state_errors
    return merged


def merge_trend_summary(
    payload: dict[str, object],
    *,
    trend_files_updated: int,
    trend_files_unchanged: int,
    trend_errors: list[str],
) -> dict[str, object]:
    merged = dict(payload)
    merged["trend_files_updated"] = trend_files_updated
    merged["trend_files_unchanged"] = trend_files_unchanged
    merged["trend_errors"] = trend_errors
    return merged


def merge_scenario_summary(
    payload: dict[str, object],
    *,
    scenario_files_updated: int,
    scenario_files_unchanged: int,
    scenarios_created: int,
    scenarios_expired: int,
    scenario_errors: list[str],
) -> dict[str, object]:
    merged = dict(payload)
    merged["scenario_files_updated"] = scenario_files_updated
    merged["scenario_files_unchanged"] = scenario_files_unchanged
    merged["scenarios_created"] = scenarios_created
    merged["scenarios_expired"] = scenarios_expired
    merged["scenario_errors"] = scenario_errors
    return merged


def build_services(
    detector_map: dict[str, MarketElementDetector],
    *,
    config,
) -> list[tuple[str, TimeframeUpdateService]]:
    services: list[tuple[str, TimeframeUpdateService]] = []
    for name, detector in detector_map.items():
        services.append(
            (
                name,
                TimeframeUpdateService(config=config, detectors={name: detector}),
            )
        )
    return services


def run_once(config_path: Path, *, force_full_scan: bool = False) -> dict[str, object]:
    config = load_config(config_path)
    log_path = configure_logging(
        level=config.logging.level,
        file_path=config.logging.file,
        max_bytes=config.logging.max_bytes,
        backup_count=config.logging.backup_count,
    )
    logger.info("Logging initialized: %s", log_path)
    logger.info("Starting auto-eye run with config: %s", config_path)

    detectors = build_detectors(config.auto_eye.elements)
    services = build_services(detectors, config=config)
    if not services:
        raise RuntimeError("No detectors enabled in auto_eye.elements")

    all_reports: list[TimeframeUpdateReport] = []
    for detector_name, service in services:
        reports = service.run_all(force=True)
        all_reports.extend(reports)
        for report in reports:
            logger.info(
                "DET=%s TF=%s file_updated=%s new=%s status_updated=%s active=%s total=%s msg=%s",
                detector_name,
                report.timeframe,
                report.file_updated,
                report.new_count,
                report.status_updated_count,
                report.total_active,
                report.total_elements,
                report.message,
            )

    payload = summarize_reports(all_reports)
    payload["detectors_processed"] = [name for name, _ in services]

    state_builder = StateSnapshotBuilder(config=config)
    state_report = state_builder.build_all(force_write=False)
    payload = merge_state_summary(
        payload,
        state_files_updated=state_report.files_updated,
        state_files_unchanged=state_report.files_unchanged,
        state_errors=state_report.errors,
    )

    trend_builder = TrendSnapshotBuilder(config=config)
    trend_report = trend_builder.build_all(force_write=False)
    payload = merge_trend_summary(
        payload,
        trend_files_updated=trend_report.files_updated,
        trend_files_unchanged=trend_report.files_unchanged,
        trend_errors=trend_report.errors,
    )

    scenario_builder = ScenarioSnapshotBuilder(config=config)
    scenario_report = scenario_builder.build_all(force_write=False)
    payload = merge_scenario_summary(
        payload,
        scenario_files_updated=scenario_report.files_updated,
        scenario_files_unchanged=scenario_report.files_unchanged,
        scenarios_created=scenario_report.scenarios_created,
        scenarios_expired=scenario_report.scenarios_expired,
        scenario_errors=scenario_report.errors,
    )

    logger.info(
        "Auto-eye run completed: detectors=%s processed=%s updated_files=%s state_updated=%s trend_updated=%s scenario_updated=%s new=%s status_updates=%s scenarios_created=%s scenarios_expired=%s",
        ",".join(payload["detectors_processed"]),
        payload["timeframes_processed"],
        payload["files_updated"],
        payload["state_files_updated"],
        payload["trend_files_updated"],
        payload["scenario_files_updated"],
        payload["new_elements"],
        payload["status_updates"],
        payload["scenarios_created"],
        payload["scenarios_expired"],
    )
    return payload


def run_loop(config_path: Path, *, force_full_scan: bool = False) -> None:
    config = load_config(config_path)
    log_path = configure_logging(
        level=config.logging.level,
        file_path=config.logging.file,
        max_bytes=config.logging.max_bytes,
        backup_count=config.logging.backup_count,
    )
    logger.info("Logging initialized: %s", log_path)
    logger.info("Starting auto-eye loop with config: %s", config_path)

    detectors = build_detectors(config.auto_eye.elements)
    services = build_services(detectors, config=config)
    if not services:
        raise RuntimeError("No detectors enabled in auto_eye.elements")

    poll_seconds = max(10, config.auto_eye.scheduler_poll_seconds)
    logger.info(
        "Auto-eye scheduler started: poll=%s sec, timeframes=%s detectors=%s",
        poll_seconds,
        ", ".join(config.auto_eye.timeframes),
        ",".join(name for name, _ in services),
    )

    if force_full_scan:
        try:
            initial_reports: list[TimeframeUpdateReport] = []
            for _, service in services:
                initial_reports.extend(service.run_all(force=True))
            summary = summarize_reports(initial_reports)
            state_builder = StateSnapshotBuilder(config=config)
            state_report = state_builder.build_all(force_write=False)
            trend_builder = TrendSnapshotBuilder(config=config)
            trend_report = trend_builder.build_all(force_write=False)
            scenario_builder = ScenarioSnapshotBuilder(config=config)
            scenario_report = scenario_builder.build_all(force_write=False)
            logger.info(
                "Initial full scan done: processed=%s updated_files=%s state_updated=%s trend_updated=%s scenario_updated=%s new=%s status_updates=%s scenarios_created=%s scenarios_expired=%s",
                summary["timeframes_processed"],
                summary["files_updated"],
                state_report.files_updated,
                trend_report.files_updated,
                scenario_report.files_updated,
                summary["new_elements"],
                summary["status_updates"],
                scenario_report.scenarios_created,
                scenario_report.scenarios_expired,
            )
        except Exception:
            logger.exception("Initial full scan failed")

    while True:
        try:
            reports: list[TimeframeUpdateReport] = []
            for _, service in services:
                reports.extend(service.run_due())
            if len(reports) > 0:
                summary = summarize_reports(reports)
                state_builder = StateSnapshotBuilder(config=config)
                state_report = state_builder.build_all(force_write=False)
                trend_builder = TrendSnapshotBuilder(config=config)
                trend_report = trend_builder.build_all(force_write=False)
                scenario_builder = ScenarioSnapshotBuilder(config=config)
                scenario_report = scenario_builder.build_all(force_write=False)
                logger.info(
                    "Scheduler cycle: processed=%s updated_files=%s state_updated=%s trend_updated=%s scenario_updated=%s new=%s status_updates=%s scenarios_created=%s scenarios_expired=%s",
                    summary["timeframes_processed"],
                    summary["files_updated"],
                    state_report.files_updated,
                    trend_report.files_updated,
                    scenario_report.files_updated,
                    summary["new_elements"],
                    summary["status_updates"],
                    scenario_report.scenarios_created,
                    scenario_report.scenarios_expired,
                )
        except Exception:
            logger.exception("Auto-eye loop iteration failed")
        time.sleep(poll_seconds)


def run_backtest(
    config_path: Path,
    *,
    start_time_utc: datetime,
    end_time_utc: datetime | None,
    run_id: str | None,
    symbols: list[str] | None,
    warmup_bars: int,
) -> dict[str, object]:
    config = load_config(config_path)
    log_path = configure_logging(
        level=config.logging.level,
        file_path=config.logging.file,
        max_bytes=config.logging.max_bytes,
        backup_count=config.logging.backup_count,
    )
    logger.info("Logging initialized: %s", log_path)
    logger.info(
        "Starting backtest with config: %s start=%s end=%s",
        config_path,
        start_time_utc.isoformat(),
        end_time_utc.isoformat() if end_time_utc else "now",
    )

    detectors = build_detectors(config.auto_eye.elements)
    runner = BacktestScenarioRunner(config=config, detectors=detectors)
    report = runner.run(
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        symbols=symbols,
        run_id=run_id,
        warmup_bars=warmup_bars,
    )

    payload = {
        "run_id": report.run_id,
        "output_dir": str(report.output_dir),
        "symbols_processed": report.symbols_processed,
        "steps_processed": report.steps_processed,
        "proposals_created": report.proposals_created,
        "scenarios_expired": report.scenarios_expired,
        "events_written": report.events_written,
        "errors": report.errors,
    }
    logger.info(
        "Backtest completed: run_id=%s output=%s proposals=%s errors=%s",
        report.run_id,
        report.output_dir,
        report.proposals_created,
        len(report.errors),
    )
    return payload


def parse_utc_datetime(value: str, *, arg_name: str) -> datetime:
    parsed = datetime_from_iso(value)
    if parsed is None:
        raise ValueError(
            f"Invalid datetime for {arg_name}: '{value}'. Use ISO-8601, e.g. 2026-02-20T12:00:00+00:00"
        )
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "AutoEye market-structure scanner. "
            "Independent from bot, but exports machine-readable JSON."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/site_config.yaml"),
        help="Path to YAML config",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously using per-timeframe schedule",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Force full history scan on first iteration",
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        help="Run event-driven scenario backtest/replay",
    )
    parser.add_argument(
        "--start-utc",
        type=str,
        default="",
        help="Backtest start time in ISO-8601 UTC/offset format",
    )
    parser.add_argument(
        "--end-utc",
        type=str,
        default="",
        help="Backtest end time in ISO-8601 UTC/offset format (optional)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default="",
        help="Optional run id for Exchange/Backtests/<run_id>",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Optional comma-separated symbols for backtest",
    )
    parser.add_argument(
        "--warmup-bars",
        type=int,
        default=500,
        help="Warmup bars for backtest replay",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.backtest:
        if not str(args.start_utc).strip():
            raise SystemExit("--start-utc is required in --backtest mode")

        start_time = parse_utc_datetime(str(args.start_utc).strip(), arg_name="--start-utc")
        end_time = None
        if str(args.end_utc).strip():
            end_time = parse_utc_datetime(str(args.end_utc).strip(), arg_name="--end-utc")

        symbols: list[str] | None = None
        if str(args.symbols).strip():
            symbols = [item.strip() for item in str(args.symbols).split(",") if item.strip()]

        run_backtest(
            args.config,
            start_time_utc=start_time,
            end_time_utc=end_time,
            run_id=str(args.run_id).strip() or None,
            symbols=symbols,
            warmup_bars=max(20, int(args.warmup_bars)),
        )
    elif args.loop:
        run_loop(args.config, force_full_scan=bool(args.full_scan))
    else:
        run_once(args.config, force_full_scan=bool(args.full_scan))
