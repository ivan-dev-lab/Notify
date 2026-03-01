from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from auto_eye.exporters import resolve_storage_element_name, state_json_path
from auto_eye.models import TrackedElement, datetime_from_iso, datetime_to_iso

logger = logging.getLogger(__name__)

STATE_SCHEMA_VERSION = "1.0.0"
REQUIRED_STATE_TIMEFRAMES = ["M5", "M15", "H1", "H4", "D1", "W1", "MN1"]
STATE_ELEMENT_KEYS = ("fvg", "snr", "fractals", "rb")


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
        self.storage_element_name = resolve_storage_element_name([element_name])
        self.state_element_key = self._resolve_state_element_key(self.storage_element_name)

    def json_path_for_symbol(self, symbol: str) -> Path:
        return state_json_path(self.base_json_path, symbol)

    def load(self, timeframe: str, symbols: list[str]) -> TimeframeSnapshot:
        normalized_timeframe = timeframe.strip().upper()
        last_bar_time_by_symbol: dict[str, datetime] = {}
        elements: list[TrackedElement] = []
        updated_candidates: list[datetime] = []
        initialized = False

        for symbol in symbols:
            path = self.json_path_for_symbol(symbol)
            raw_state = self._read_state_payload(path)
            if raw_state is None:
                continue

            raw_timeframes = raw_state.get("timeframes")
            if not isinstance(raw_timeframes, dict):
                continue
            raw_timeframe = raw_timeframes.get(normalized_timeframe)
            if not isinstance(raw_timeframe, dict):
                continue

            updated_at = datetime_from_iso(str(raw_timeframe.get("updated_at_utc") or ""))
            if updated_at is not None:
                updated_candidates.append(updated_at)

            state_block = raw_timeframe.get("state")
            has_element_tracking = isinstance(state_block, dict)
            last_bar = self._resolve_last_bar(raw_timeframe, has_element_tracking)
            if last_bar is not None:
                last_bar_time_by_symbol[symbol] = last_bar

            if has_element_tracking:
                initialized_map = state_block.get("initialized_elements")
                if isinstance(initialized_map, dict) and bool(
                    initialized_map.get(self.state_element_key)
                ):
                    initialized = True
            elif bool(raw_timeframe.get("initialized")):
                initialized = True

            for parsed in self._read_timeframe_elements(
                symbol=symbol,
                timeframe=normalized_timeframe,
                raw_timeframe=raw_timeframe,
            ):
                elements.append(parsed)

        deduped: dict[str, TrackedElement] = {}
        for element in elements:
            deduped[element.id] = element
        sorted_elements = sorted(
            deduped.values(),
            key=lambda item: (item.symbol, item.c3_time, item.id),
        )

        updated_at_utc = max(updated_candidates) if updated_candidates else None
        return TimeframeSnapshot(
            timeframe=normalized_timeframe,
            initialized=initialized,
            updated_at_utc=updated_at_utc,
            last_bar_time_by_symbol=last_bar_time_by_symbol,
            elements=sorted_elements,
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
            raw_state = self._read_state_payload(path)
            state_payload = self._ensure_state_payload(
                raw_state=raw_state,
                symbol=symbol,
                now_utc=snapshot.updated_at_utc,
            )
            raw_timeframes = state_payload["timeframes"]
            raw_timeframe = raw_timeframes.get(timeframe)
            if not isinstance(raw_timeframe, dict):
                raw_timeframe = self._empty_timeframe_payload()
                raw_timeframes[timeframe] = raw_timeframe

            raw_elements = raw_timeframe.get("elements")
            if not isinstance(raw_elements, dict):
                raw_elements = self._empty_elements_payload()
                raw_timeframe["elements"] = raw_elements
            for key in STATE_ELEMENT_KEYS:
                raw_elements.setdefault(key, [])

            symbol_elements = sorted(
                elements_by_symbol.get(symbol, []),
                key=lambda item: (item.c3_time, item.id),
            )
            raw_elements[self.state_element_key] = [
                self._tracked_to_state_element(item) for item in symbol_elements
            ]

            raw_timeframe["initialized"] = bool(raw_timeframe.get("initialized")) or bool(
                snapshot.initialized
            )
            raw_timeframe["updated_at_utc"] = datetime_to_iso(snapshot.updated_at_utc)

            last_bar = snapshot.last_bar_time_by_symbol.get(symbol)
            if last_bar is not None:
                raw_timeframe["last_bar_time_utc"] = datetime_to_iso(last_bar)
            else:
                raw_timeframe.setdefault("last_bar_time_utc", None)

            state_block = raw_timeframe.get("state")
            if not isinstance(state_block, dict):
                state_block = {}
            initialized_elements = state_block.get("initialized_elements")
            if not isinstance(initialized_elements, dict):
                initialized_elements = {}
            initialized_elements[self.state_element_key] = bool(snapshot.initialized)
            state_block["initialized_elements"] = initialized_elements

            last_bar_by_element = state_block.get("last_bar_time_by_element_utc")
            if not isinstance(last_bar_by_element, dict):
                last_bar_by_element = {}
            if last_bar is not None:
                last_bar_by_element[self.state_element_key] = datetime_to_iso(last_bar)
            state_block["last_bar_time_by_element_utc"] = last_bar_by_element
            raw_timeframe["state"] = state_block

            state_payload["symbol"] = symbol
            state_payload["updated_at_utc"] = datetime_to_iso(snapshot.updated_at_utc)

            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as file:
                json.dump(state_payload, file, ensure_ascii=False, indent=2)
            saved_paths.append(path)

        return saved_paths

    @staticmethod
    def _resolve_state_element_key(storage_element_name: str) -> str:
        normalized = str(storage_element_name).strip().lower()
        if normalized in {"fvg"}:
            return "fvg"
        if normalized in {"snr"}:
            return "snr"
        if normalized in {"fractal", "fractals"}:
            return "fractals"
        if normalized in {"rb", "rangeblock", "range_block"}:
            return "rb"
        return normalized

    def _resolve_last_bar(
        self,
        raw_timeframe: dict[str, object],
        has_element_tracking: bool,
    ) -> datetime | None:
        if has_element_tracking:
            state_block = raw_timeframe.get("state")
            if isinstance(state_block, dict):
                last_bar_by_element = state_block.get("last_bar_time_by_element_utc")
                if isinstance(last_bar_by_element, dict):
                    value = datetime_from_iso(
                        str(last_bar_by_element.get(self.state_element_key) or "")
                    )
                    if value is not None:
                        return value
            return None
        return datetime_from_iso(
            str(raw_timeframe.get("last_bar_time_utc") or raw_timeframe.get("last_bar_time") or "")
        )

    def _read_timeframe_elements(
        self,
        *,
        symbol: str,
        timeframe: str,
        raw_timeframe: dict[str, object],
    ) -> list[TrackedElement]:
        raw_elements_block = raw_timeframe.get("elements")
        if isinstance(raw_elements_block, dict):
            raw_items = raw_elements_block.get(self.state_element_key, [])
            if self.state_element_key == "fractals" and not isinstance(raw_items, list):
                raw_items = raw_elements_block.get("fractal", [])
        else:
            raw_items = raw_elements_block

        if not isinstance(raw_items, list):
            return []

        parsed_elements: list[TrackedElement] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            parsed = self._parse_state_element(item)
            if parsed is None:
                continue
            if parsed.symbol != symbol:
                continue
            if parsed.timeframe.upper() != timeframe:
                continue
            parsed_elements.append(parsed)
        return parsed_elements

    @staticmethod
    def _read_state_payload(path: Path) -> dict[str, object] | None:
        if not path.exists():
            return None
        if path.stat().st_size == 0:
            logger.warning("Empty State file, rebuilding: %s", path)
            return None

        with path.open("r", encoding="utf-8") as file:
            raw = json.load(file)
        if not isinstance(raw, dict):
            logger.warning("Invalid State file format, rebuilding: %s", path)
            return None
        return raw

    @staticmethod
    def _empty_elements_payload() -> dict[str, list[dict[str, object]]]:
        return {
            "fvg": [],
            "snr": [],
            "fractals": [],
            "rb": [],
        }

    @classmethod
    def _empty_timeframe_payload(cls) -> dict[str, object]:
        return {
            "initialized": False,
            "updated_at_utc": None,
            "last_bar_time_utc": None,
            "elements": cls._empty_elements_payload(),
            "state": {
                "initialized_elements": {},
                "last_bar_time_by_element_utc": {},
            },
        }

    @classmethod
    def _ensure_state_payload(
        cls,
        *,
        raw_state: dict[str, object] | None,
        symbol: str,
        now_utc: datetime | None,
    ) -> dict[str, object]:
        state: dict[str, object] = {}
        if isinstance(raw_state, dict):
            state = dict(raw_state)

        state["schema_version"] = str(state.get("schema_version") or STATE_SCHEMA_VERSION)
        state["symbol"] = symbol
        state.setdefault("updated_at_utc", datetime_to_iso(now_utc))
        state.pop("derived", None)
        state.pop("scenarios", None)

        market = state.get("market")
        if not isinstance(market, dict):
            market = {}
        market.setdefault("price", 0.0)
        market.setdefault("bid", None)
        market.setdefault("ask", None)
        market.setdefault("source", "MT5")
        market.setdefault(
            "tick_time_utc",
            datetime_to_iso(now_utc) or datetime.now(timezone.utc).isoformat(),
        )
        state["market"] = market

        raw_timeframes = state.get("timeframes")
        if not isinstance(raw_timeframes, dict):
            raw_timeframes = {}

        for timeframe in REQUIRED_STATE_TIMEFRAMES:
            timeframe_payload = raw_timeframes.get(timeframe)
            raw_timeframes[timeframe] = cls._ensure_timeframe_payload(timeframe_payload)

        for timeframe, timeframe_payload in list(raw_timeframes.items()):
            if timeframe in REQUIRED_STATE_TIMEFRAMES:
                continue
            raw_timeframes[timeframe] = cls._ensure_timeframe_payload(timeframe_payload)

        state["timeframes"] = raw_timeframes
        return state

    @classmethod
    def _ensure_timeframe_payload(cls, raw_timeframe: object) -> dict[str, object]:
        if not isinstance(raw_timeframe, dict):
            return cls._empty_timeframe_payload()

        timeframe_payload = dict(raw_timeframe)
        timeframe_payload["initialized"] = bool(timeframe_payload.get("initialized"))
        timeframe_payload.setdefault("updated_at_utc", None)
        timeframe_payload["last_bar_time_utc"] = (
            timeframe_payload.get("last_bar_time_utc") or timeframe_payload.get("last_bar_time")
        )
        timeframe_payload.pop("last_bar_time", None)

        raw_elements = timeframe_payload.get("elements")
        elements_payload = cls._normalize_elements_block(raw_elements)
        timeframe_payload["elements"] = elements_payload

        state_block = timeframe_payload.get("state")
        if not isinstance(state_block, dict):
            state_block = {}
        initialized_elements = state_block.get("initialized_elements")
        if not isinstance(initialized_elements, dict):
            initialized_elements = {}
        last_bar_by_element = state_block.get("last_bar_time_by_element_utc")
        if not isinstance(last_bar_by_element, dict):
            last_bar_by_element = {}
        state_block["initialized_elements"] = initialized_elements
        state_block["last_bar_time_by_element_utc"] = last_bar_by_element
        timeframe_payload["state"] = state_block
        return timeframe_payload

    @classmethod
    def _normalize_elements_block(cls, raw_elements: object) -> dict[str, list[dict[str, object]]]:
        if isinstance(raw_elements, dict):
            normalized: dict[str, list[dict[str, object]]] = {}
            for key in STATE_ELEMENT_KEYS:
                if key == "fractals":
                    value = raw_elements.get("fractals")
                    if not isinstance(value, list):
                        value = raw_elements.get("fractal")
                else:
                    value = raw_elements.get(key)
                if isinstance(value, list):
                    normalized[key] = [item for item in value if isinstance(item, dict)]
                else:
                    normalized[key] = []
            return normalized

        if isinstance(raw_elements, list):
            converted = cls._empty_elements_payload()
            for item in raw_elements:
                if not isinstance(item, dict):
                    continue
                raw_type = str(item.get("element_type") or "").strip().lower()
                if raw_type == "fractal":
                    converted["fractals"].append(item)
                elif raw_type in {"fvg", "snr", "rb"}:
                    converted[raw_type].append(item)
            return converted

        return cls._empty_elements_payload()

    @staticmethod
    def _tracked_to_state_element(element: TrackedElement) -> dict[str, object]:
        raw = element.to_dict()
        element_type = str(raw.get("element_type") or element.element_type).strip().lower()
        if element_type == "fractal":
            return {
                "id": raw.get("id"),
                "element_type": "fractal",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "fractal_type": raw.get("fractal_type"),
                "pivot_time_utc": raw.get("pivot_time"),
                "confirm_time_utc": raw.get("confirm_time"),
                "formation_time_utc": raw.get("confirm_time"),
                "c1_time_utc": raw.get("c1_time"),
                "c2_time_utc": raw.get("c2_time"),
                "c3_time_utc": raw.get("c3_time"),
                "extreme_price": raw.get("extreme_price"),
                "l_price": raw.get("l_price"),
                "l_alt_price": raw.get("l_alt_price"),
                "l_price_bearish": raw.get("l_price_bearish"),
                "l_alt_bearish": raw.get("l_alt_bearish"),
                "l_price_bullish": raw.get("l_price_bullish"),
                "l_alt_bullish": raw.get("l_alt_bullish"),
                "status": raw.get("status"),
                "broken_time_utc": raw.get("broken_time"),
                "broken_side": raw.get("broken_side"),
                "metadata": raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {},
            }
        if element_type == "snr":
            metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            break_time = raw.get("break_time")
            return {
                "id": raw.get("id"),
                "element_type": "snr",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "origin_fractal_id": raw.get("origin_fractal_id"),
                "role": raw.get("role"),
                "break_type": raw.get("break_type"),
                "break_time_utc": break_time,
                "formation_time_utc": break_time,
                "break_close": raw.get("break_close"),
                "l_price": raw.get("l_price"),
                "l_alt_price": raw.get("l_alt_price"),
                "l_price_bearish": raw.get("l_price_bearish"),
                "l_alt_bearish": raw.get("l_alt_bearish"),
                "l_price_bullish": raw.get("l_price_bullish"),
                "l_alt_bullish": raw.get("l_alt_bullish"),
                "l_price_used": raw.get("l_price_used"),
                "l_rule_used": raw.get("l_rule_used"),
                "extreme_price": raw.get("extreme_price"),
                "departure_extreme_price": raw.get("departure_extreme_price")
                or metadata.get("departure_extreme_price"),
                "departure_extreme_time_utc": raw.get("departure_extreme_time")
                or metadata.get("departure_extreme_time"),
                "departure_range_start_time_utc": raw.get("departure_range_start_time")
                or metadata.get("departure_range_start_time"),
                "departure_range_end_time_utc": raw.get("departure_range_end_time")
                or metadata.get("departure_range_end_time"),
                "snr_low": raw.get("snr_low"),
                "snr_high": raw.get("snr_high"),
                "invalid_calc": raw.get("invalid_calc"),
                "invalid_calc_reason": raw.get("invalid_calc_reason"),
                "status": raw.get("status"),
                "retest_time_utc": raw.get("retest_time"),
                "invalidated_time_utc": raw.get("invalidated_time"),
                "metadata": metadata,
            }
        if element_type == "rb":
            metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            confirm_time = raw.get("confirm_time")
            return {
                "id": raw.get("id"),
                "element_type": "rb",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "rb_type": raw.get("rb_type"),
                "origin_fractal_id": raw.get("origin_fractal_id"),
                "pivot_time_utc": raw.get("pivot_time"),
                "confirm_time_utc": confirm_time,
                "formation_time_utc": confirm_time,
                "c1_time_utc": raw.get("c1_time"),
                "c2_time_utc": raw.get("c2_time"),
                "c3_time_utc": raw.get("c3_time"),
                "l_price": raw.get("l_price"),
                "l_alt_price": raw.get("l_alt_price"),
                "l_price_bearish": raw.get("l_price_bearish"),
                "l_alt_bearish": raw.get("l_alt_bearish"),
                "l_price_bullish": raw.get("l_price_bullish"),
                "l_alt_bullish": raw.get("l_alt_bullish"),
                "l_price_used": raw.get("l_price_used"),
                "l_rule_used": raw.get("l_rule_used"),
                "line_used": raw.get("line_used"),
                "line_rule_used": raw.get("line_rule_used"),
                "extreme_price": raw.get("extreme_price"),
                "rb_low": raw.get("rb_low"),
                "rb_high": raw.get("rb_high"),
                "status": raw.get("status"),
                "broken_time_utc": raw.get("broken_time"),
                "broken_side": raw.get("broken_side"),
                "metadata": metadata,
            }
        return {
            "id": raw.get("id"),
            "element_type": "fvg",
            "symbol": raw.get("symbol"),
            "timeframe": raw.get("timeframe"),
            "direction": raw.get("direction"),
            "formation_time_utc": raw.get("formation_time"),
            "fvg_low": raw.get("fvg_low"),
            "fvg_high": raw.get("fvg_high"),
            "gap_size": raw.get("gap_size"),
            "c1_time_utc": raw.get("c1_time"),
            "c2_time_utc": raw.get("c2_time"),
            "c3_time_utc": raw.get("c3_time"),
            "status": raw.get("status"),
            "touched_time_utc": raw.get("touched_time"),
            "mitigated_time_utc": raw.get("mitigated_time"),
            "fill_price": raw.get("fill_price"),
            "fill_percent": raw.get("fill_percent"),
            "metadata": raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {},
        }

    @staticmethod
    def _parse_state_element(raw: dict[str, object]) -> TrackedElement | None:
        element_type = str(raw.get("element_type") or "").strip().lower()
        if element_type == "fractal":
            converted: dict[str, Any] = {
                "id": raw.get("id"),
                "element_type": "fractal",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "fractal_type": raw.get("fractal_type"),
                "pivot_time": raw.get("pivot_time_utc") or raw.get("pivot_time"),
                "confirm_time": raw.get("confirm_time_utc") or raw.get("confirm_time"),
                "c1_time": raw.get("c1_time_utc") or raw.get("c1_time"),
                "c2_time": raw.get("c2_time_utc") or raw.get("c2_time"),
                "c3_time": raw.get("c3_time_utc") or raw.get("c3_time"),
                "extreme_price": raw.get("extreme_price"),
                "l_price": raw.get("l_price"),
                "l_alt_price": raw.get("l_alt_price"),
                "l_price_bearish": raw.get("l_price_bearish"),
                "l_alt_bearish": raw.get("l_alt_bearish"),
                "l_price_bullish": raw.get("l_price_bullish"),
                "l_alt_bullish": raw.get("l_alt_bullish"),
                "status": raw.get("status"),
                "broken_time": raw.get("broken_time_utc") or raw.get("broken_time"),
                "broken_side": raw.get("broken_side"),
                "metadata": raw.get("metadata"),
            }
            return TrackedElement.from_dict(converted)

        if element_type == "snr":
            converted = {
                "id": raw.get("id"),
                "element_type": "snr",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "origin_fractal_id": raw.get("origin_fractal_id"),
                "role": raw.get("role"),
                "break_type": raw.get("break_type"),
                "break_time": raw.get("break_time_utc") or raw.get("break_time"),
                "break_close": raw.get("break_close"),
                "l_price": raw.get("l_price"),
                "l_alt_price": raw.get("l_alt_price"),
                "l_price_bearish": raw.get("l_price_bearish"),
                "l_alt_bearish": raw.get("l_alt_bearish"),
                "l_price_bullish": raw.get("l_price_bullish"),
                "l_alt_bullish": raw.get("l_alt_bullish"),
                "l_price_used": raw.get("l_price_used"),
                "l_rule_used": raw.get("l_rule_used"),
                "extreme_price": raw.get("extreme_price"),
                "departure_extreme_price": raw.get("departure_extreme_price"),
                "departure_extreme_time": raw.get("departure_extreme_time_utc")
                or raw.get("departure_extreme_time"),
                "departure_range_start_time": raw.get("departure_range_start_time_utc")
                or raw.get("departure_range_start_time"),
                "departure_range_end_time": raw.get("departure_range_end_time_utc")
                or raw.get("departure_range_end_time"),
                "snr_low": raw.get("snr_low"),
                "snr_high": raw.get("snr_high"),
                "invalid_calc": raw.get("invalid_calc"),
                "invalid_calc_reason": raw.get("invalid_calc_reason"),
                "status": raw.get("status"),
                "retest_time": raw.get("retest_time_utc") or raw.get("retest_time"),
                "invalidated_time": raw.get("invalidated_time_utc")
                or raw.get("invalidated_time"),
                "metadata": raw.get("metadata"),
            }
            return TrackedElement.from_dict(converted)

        if element_type == "rb":
            converted = {
                "id": raw.get("id"),
                "element_type": "rb",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "rb_type": raw.get("rb_type"),
                "origin_fractal_id": raw.get("origin_fractal_id"),
                "pivot_time": raw.get("pivot_time_utc") or raw.get("pivot_time"),
                "confirm_time": raw.get("confirm_time_utc")
                or raw.get("confirm_time")
                or raw.get("formation_time_utc")
                or raw.get("formation_time"),
                "c1_time": raw.get("c1_time_utc") or raw.get("c1_time"),
                "c2_time": raw.get("c2_time_utc") or raw.get("c2_time"),
                "c3_time": raw.get("c3_time_utc") or raw.get("c3_time"),
                "l_price": raw.get("l_price"),
                "l_alt_price": raw.get("l_alt_price"),
                "l_price_bearish": raw.get("l_price_bearish"),
                "l_alt_bearish": raw.get("l_alt_bearish"),
                "l_price_bullish": raw.get("l_price_bullish"),
                "l_alt_bullish": raw.get("l_alt_bullish"),
                "l_price_used": raw.get("l_price_used") or raw.get("line_used"),
                "l_rule_used": raw.get("l_rule_used") or raw.get("line_rule_used"),
                "line_used": raw.get("line_used") or raw.get("l_price_used"),
                "line_rule_used": raw.get("line_rule_used") or raw.get("l_rule_used"),
                "extreme_price": raw.get("extreme_price"),
                "rb_low": raw.get("rb_low"),
                "rb_high": raw.get("rb_high"),
                "status": raw.get("status"),
                "broken_time": raw.get("broken_time_utc") or raw.get("broken_time"),
                "broken_side": raw.get("broken_side"),
                "metadata": raw.get("metadata"),
            }
            return TrackedElement.from_dict(converted)

        converted = {
            "id": raw.get("id"),
            "element_type": "fvg",
            "symbol": raw.get("symbol"),
            "timeframe": raw.get("timeframe"),
            "direction": raw.get("direction"),
            "formation_time": raw.get("formation_time_utc") or raw.get("formation_time"),
            "fvg_low": raw.get("fvg_low"),
            "fvg_high": raw.get("fvg_high"),
            "gap_size": raw.get("gap_size"),
            "c1_time": raw.get("c1_time_utc") or raw.get("c1_time"),
            "c2_time": raw.get("c2_time_utc") or raw.get("c2_time"),
            "c3_time": raw.get("c3_time_utc") or raw.get("c3_time"),
            "status": raw.get("status"),
            "touched_time": raw.get("touched_time_utc") or raw.get("touched_time"),
            "mitigated_time": raw.get("mitigated_time_utc") or raw.get("mitigated_time"),
            "fill_price": raw.get("fill_price"),
            "fill_percent": raw.get("fill_percent"),
            "metadata": raw.get("metadata"),
        }
        return TrackedElement.from_dict(converted)
