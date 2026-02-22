from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from auto_eye.exporters import asset_json_path, resolve_storage_element_name
from auto_eye.models import TrackedElement, datetime_from_iso, datetime_to_iso

logger = logging.getLogger(__name__)


@dataclass
class TimeframeSnapshot:
    timeframe: str
    initialized: bool
    updated_at_utc: datetime | None
    last_bar_time_by_symbol: dict[str, datetime]
    elements: list[TrackedElement]

    @classmethod
    def empty(cls, timeframe: str) -> TimeframeSnapshot:
        return cls(
            timeframe=timeframe,
            initialized=False,
            updated_at_utc=None,
            last_bar_time_by_symbol={},
            elements=[],
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "timeframe": self.timeframe,
            "initialized": bool(self.initialized),
            "updated_at_utc": datetime_to_iso(self.updated_at_utc),
            "last_bar_time_by_symbol": {
                symbol: datetime_to_iso(value)
                for symbol, value in self.last_bar_time_by_symbol.items()
            },
            "elements": [element.to_dict() for element in self.elements],
        }


class TimeframeFileStore:
    def __init__(
        self,
        base_json_path: Path,
        *,
        element_name: str = "FVG",
    ) -> None:
        self.base_json_path = base_json_path
        self.element_name = resolve_storage_element_name([element_name])

    def json_path_for_symbol(self, symbol: str) -> Path:
        return asset_json_path(
            self.base_json_path,
            symbol,
            element_name=self.element_name,
        )

    def load(self, timeframe: str, symbols: list[str]) -> TimeframeSnapshot:
        normalized_timeframe = timeframe.strip().upper()
        last_bar_time_by_symbol: dict[str, datetime] = {}
        elements: list[TrackedElement] = []
        updated_candidates: list[datetime] = []
        initialized = False

        for symbol in symbols:
            path = self.json_path_for_symbol(symbol)
            raw_asset = self._read_asset_payload(path)
            if raw_asset is None:
                continue

            raw_timeframes = raw_asset.get("timeframes")
            if not isinstance(raw_timeframes, dict):
                continue
            raw_timeframe = raw_timeframes.get(normalized_timeframe)
            if not isinstance(raw_timeframe, dict):
                continue

            initialized = True

            updated_at = datetime_from_iso(str(raw_timeframe.get("updated_at_utc") or ""))
            if updated_at is not None:
                updated_candidates.append(updated_at)

            last_bar = datetime_from_iso(str(raw_timeframe.get("last_bar_time") or ""))
            if last_bar is not None:
                last_bar_time_by_symbol[symbol] = last_bar

            raw_elements = raw_timeframe.get("elements", [])
            if isinstance(raw_elements, list):
                for item in raw_elements:
                    if not isinstance(item, dict):
                        continue
                    parsed = TrackedElement.from_dict(item)
                    if parsed is None:
                        continue
                    if parsed.symbol != symbol:
                        continue
                    if parsed.timeframe.upper() != normalized_timeframe:
                        continue
                    elements.append(parsed)

        updated_at_utc = max(updated_candidates) if updated_candidates else None
        return TimeframeSnapshot(
            timeframe=normalized_timeframe,
            initialized=initialized,
            updated_at_utc=updated_at_utc,
            last_bar_time_by_symbol=last_bar_time_by_symbol,
            elements=elements,
        )

    def save(self, snapshot: TimeframeSnapshot) -> list[Path]:
        timeframe = snapshot.timeframe.strip().upper()
        elements_by_symbol: dict[str, list[TrackedElement]] = {}
        for element in snapshot.elements:
            if element.timeframe.upper() != timeframe:
                continue
            elements_by_symbol.setdefault(element.symbol, []).append(element)

        symbols = set(snapshot.last_bar_time_by_symbol.keys()) | set(elements_by_symbol.keys())
        saved_paths: list[Path] = []

        for symbol in sorted(symbols):
            path = self.json_path_for_symbol(symbol)
            raw_asset = self._read_asset_payload(path)
            if raw_asset is None:
                raw_asset = {
                    "symbol": symbol,
                    "updated_at_utc": None,
                    "timeframes": {},
                }

            raw_timeframes = raw_asset.get("timeframes")
            if not isinstance(raw_timeframes, dict):
                raw_timeframes = {}
                raw_asset["timeframes"] = raw_timeframes

            symbol_elements = sorted(
                elements_by_symbol.get(symbol, []),
                key=lambda item: (item.c3_time, item.id),
            )
            raw_timeframes[timeframe] = {
                "initialized": True,
                "updated_at_utc": datetime_to_iso(snapshot.updated_at_utc),
                "last_bar_time": datetime_to_iso(snapshot.last_bar_time_by_symbol.get(symbol)),
                "elements": [element.to_dict() for element in symbol_elements],
            }

            raw_asset["symbol"] = symbol
            raw_asset["updated_at_utc"] = datetime_to_iso(snapshot.updated_at_utc)

            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as file:
                json.dump(raw_asset, file, ensure_ascii=False, indent=2)
            saved_paths.append(path)

        return saved_paths

    @staticmethod
    def _read_asset_payload(path: Path) -> dict[str, object] | None:
        if not path.exists():
            return None
        if path.stat().st_size == 0:
            logger.warning("Empty AutoEye asset file, rebuilding: %s", path)
            return None

        with path.open("r", encoding="utf-8") as file:
            raw = json.load(file)
        if not isinstance(raw, dict):
            logger.warning("Invalid AutoEye asset file format, rebuilding: %s", path)
            return None
        return raw
