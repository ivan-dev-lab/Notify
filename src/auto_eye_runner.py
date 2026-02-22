from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from app_logging import configure_logging
from auto_eye.detectors.registry import build_detectors
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
        "new_fvg": sum(report.new_count for report in reports),
        "status_updates": sum(report.status_updated_count for report in reports),
        "active_total": sum(report.total_active for report in reports),
    }


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
    service = TimeframeUpdateService(config=config, detectors=detectors)

    reports = service.run_all(force=True)
    for report in reports:
        logger.info(
            "TF=%s file_updated=%s new=%s status_updated=%s active=%s total=%s msg=%s",
            report.timeframe,
            report.file_updated,
            report.new_count,
            report.status_updated_count,
            report.total_active,
            report.total_elements,
            report.message,
        )
    payload = summarize_reports(reports)

    logger.info(
        "Auto-eye run completed: processed=%s updated_files=%s new=%s status_updates=%s",
        payload["timeframes_processed"],
        payload["files_updated"],
        payload["new_fvg"],
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
    service = TimeframeUpdateService(config=config, detectors=detectors)

    poll_seconds = max(10, config.auto_eye.scheduler_poll_seconds)
    logger.info(
        "Auto-eye scheduler started: poll=%s sec, timeframes=%s",
        poll_seconds,
        ", ".join(config.auto_eye.timeframes),
    )

    if force_full_scan:
        try:
            initial_reports = service.run_all(force=True)
            summary = summarize_reports(initial_reports)
            logger.info(
                "Initial full scan done: processed=%s updated_files=%s new=%s status_updates=%s",
                summary["timeframes_processed"],
                summary["files_updated"],
                summary["new_fvg"],
                summary["status_updates"],
            )
        except Exception:
            logger.exception("Initial full scan failed")

    while True:
        try:
            reports = service.run_due()
            if reports:
                summary = summarize_reports(reports)
                logger.info(
                    "Scheduler cycle: processed=%s updated_files=%s new=%s status_updates=%s",
                    summary["timeframes_processed"],
                    summary["files_updated"],
                    summary["new_fvg"],
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
