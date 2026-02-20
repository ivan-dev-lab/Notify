from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from app_logging import configure_logging
from auto_eye.detectors.registry import build_detectors
from auto_eye.engine import AutoEyeEngine
from config_loader import load_config

logger = logging.getLogger(__name__)


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
    engine = AutoEyeEngine(config=config, detectors=detectors)
    payload = engine.run_once(force_full_scan=force_full_scan)

    logger.info(
        "Auto-eye run completed: elements=%s errors=%s",
        payload.get("count"),
        len(payload.get("errors", [])),
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
    engine = AutoEyeEngine(config=config, detectors=detectors)

    interval = max(10, config.auto_eye.update_interval_seconds)
    first_run = True
    while True:
        try:
            engine.run_once(force_full_scan=force_full_scan and first_run)
        except Exception:
            logger.exception("Auto-eye loop iteration failed")
        first_run = False
        time.sleep(interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "AutoEye market-structure scanner. "
            "Independent from bot, but exports machine-readable JSON/CSV."
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
        help="Run continuously using auto_eye.update_interval_seconds",
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
