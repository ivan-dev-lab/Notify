from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def resolve_log_path(path: str) -> Path:
    log_path = Path(path)
    if log_path.is_absolute():
        return log_path
    return Path.cwd() / log_path


def configure_logging(
    *,
    level: str = "INFO",
    file_path: str = "logs/notify.log",
    max_bytes: int = 5_000_000,
    backup_count: int = 5,
) -> Path:
    log_level_name = level.upper().strip() or "INFO"
    log_level = getattr(logging, log_level_name, logging.INFO)

    log_path = resolve_log_path(file_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max(1, int(max_bytes)),
        backupCount=max(1, int(backup_count)),
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    return log_path
