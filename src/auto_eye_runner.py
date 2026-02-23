from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from app_logging import configure_logging
from auto_eye.detectors.base import MarketElementDetector
from auto_eye.detectors.registry import build_detectors
from auto_eye.state_snapshot import StateSnapshotBuilder
from auto_eye.timeframe_service import TimeframeUpdateReport, TimeframeUpdateService
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

    logger.info(
        "Auto-eye run completed: detectors=%s processed=%s updated_files=%s state_updated=%s new=%s status_updates=%s",
        ",".join(payload["detectors_processed"]),
        payload["timeframes_processed"],
        payload["files_updated"],
        payload["state_files_updated"],
        payload["new_elements"],
        payload["status_updates"],
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
            logger.info(
                "Initial full scan done: processed=%s updated_files=%s state_updated=%s new=%s status_updates=%s",
                summary["timeframes_processed"],
                summary["files_updated"],
                state_report.files_updated,
                summary["new_elements"],
                summary["status_updates"],
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
                logger.info(
                    "Scheduler cycle: processed=%s updated_files=%s state_updated=%s new=%s status_updates=%s",
                    summary["timeframes_processed"],
                    summary["files_updated"],
                    state_report.files_updated,
                    summary["new_elements"],
                    summary["status_updates"],
                )
        except Exception:
            logger.exception("Auto-eye loop iteration failed")
        time.sleep(poll_seconds)


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
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.loop:
        run_loop(args.config, force_full_scan=bool(args.full_scan))
    else:
        run_once(args.config, force_full_scan=bool(args.full_scan))
