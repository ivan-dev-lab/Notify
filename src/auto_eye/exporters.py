from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

from auto_eye.models import TrackedElement

logger = logging.getLogger(__name__)


def resolve_output_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def export_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
    logger.info("AutoEye JSON exported: %s", path)


def export_csv(path: Path, elements: list[TrackedElement]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "id",
                "element_type",
                "symbol",
                "timeframe",
                "direction",
                "formation_time",
                "fvg_low",
                "fvg_high",
                "gap_size",
                "c1_time",
                "c2_time",
                "c3_time",
                "status",
                "touched_time",
                "mitigated_time",
                "fill_price",
                "fill_percent",
                "metadata",
            ],
        )
        writer.writeheader()
        for element in elements:
            row = element.to_dict()
            row["metadata"] = json.dumps(row.get("metadata", {}), ensure_ascii=False)
            writer.writerow(row)

    logger.info("AutoEye CSV exported: %s", path)
