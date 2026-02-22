from __future__ import annotations

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


def resolve_output_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def sanitize_asset_filename(asset: str) -> str:
    normalized = str(asset).strip()
    if not normalized:
        return "UNKNOWN"
    normalized = re.sub(r'[\\/:*?"<>|]+', "_", normalized)
    normalized = re.sub(r"\s+", "_", normalized)
    return normalized


def asset_json_path(base_json_path: Path, asset: str) -> Path:
    return base_json_path.parent / f"{sanitize_asset_filename(asset)}.json"


def export_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    logger.info("AutoEye JSON exported: %s", path)
