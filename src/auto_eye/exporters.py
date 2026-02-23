from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from pathlib import Path

logger = logging.getLogger(__name__)


def resolve_output_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def _normalize_base_json_path(base_json_path: Path) -> Path:
    if base_json_path.is_absolute():
        return base_json_path
    return (Path.cwd() / base_json_path).resolve()


def sanitize_asset_filename(asset: str) -> str:
    normalized = str(asset).strip()
    if not normalized:
        return "UNKNOWN"
    normalized = re.sub(r'[\\/:*?"<>|]+', "_", normalized)
    normalized = re.sub(r"\s+", "_", normalized)
    return normalized


def sanitize_element_folder(element_name: str) -> str:
    normalized = str(element_name).strip().upper()
    if not normalized:
        return "FVG"
    if normalized in {"FVG"}:
        return "FVG"
    if normalized in {"FRACTAL", "FRACTALS"}:
        return "Fractals"
    if normalized in {"SNR"}:
        return "SNR"
    normalized = re.sub(r'[\\/:*?"<>|]+', "_", normalized)
    normalized = re.sub(r"\s+", "_", normalized)
    return normalized


def resolve_storage_element_name(element_names: Iterable[str]) -> str:
    for item in element_names:
        text = str(item).strip()
        if text:
            return sanitize_element_folder(text)
    return "FVG"


def asset_json_path(base_json_path: Path, asset: str, *, element_name: str = "FVG") -> Path:
    folder = sanitize_element_folder(element_name)
    return base_json_path.parent / folder / f"{sanitize_asset_filename(asset)}.json"


def exchange_base_path(base_json_path: Path) -> Path:
    normalized = _normalize_base_json_path(base_json_path)
    output_dir = normalized.parent
    if output_dir.name.lower() == "output":
        # .../<Trading>/<Project>/output/<file>.json -> .../<Trading>/Exchange
        # Also handles .../<Trading>/<Project>/src/output/<file>.json.
        project_dir = output_dir.parent
        if project_dir.name.lower() == "src":
            project_dir = project_dir.parent
        trading_dir = project_dir.parent
    else:
        # Fallback for non-standard layout.
        trading_dir = output_dir.parent
    return trading_dir / "Exchange"


def ensure_exchange_structure(base_json_path: Path) -> dict[str, Path]:
    exchange_dir = exchange_base_path(base_json_path)
    paths = {
        "exchange": exchange_dir,
        "state": exchange_dir / "State",
        "actions": exchange_dir / "Actions",
        "decisions": exchange_dir / "Decisions",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def state_json_path(base_json_path: Path, asset: str) -> Path:
    dirs = ensure_exchange_structure(base_json_path)
    return dirs["state"] / f"{sanitize_asset_filename(asset)}.json"


def export_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    logger.info("AutoEye JSON exported: %s", path)

