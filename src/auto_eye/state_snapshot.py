from __future__ import annotations

import json
import logging
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from config_loader import AppConfig

from auto_eye.exporters import ensure_exchange_structure, resolve_output_path, state_json_path
from auto_eye.models import (
    STATUS_BROKEN,
    STATUS_EXPIRED,
    STATUS_INVALIDATED,
    STATUS_MITIGATED_FULL,
)
from auto_eye.mt5_source import MT5BarsSource
from auto_eye.timeframe_files import REQUIRED_STATE_TIMEFRAMES, STATE_SCHEMA_VERSION

logger = logging.getLogger(__name__)

REQUIRED_TIMEFRAMES = list(REQUIRED_STATE_TIMEFRAMES)


@dataclass
class StateSnapshotReport:
    symbols_processed: int
    files_updated: int
    files_unchanged: int
    errors: list[str]


class StateSnapshotBuilder:
    def __init__(
        self,
        *,
        config: AppConfig,
        source: MT5BarsSource | None = None,
    ) -> None:
        self.config = config
        self.source = source or MT5BarsSource(config)
        self.base_json_path = resolve_output_path(config.auto_eye.output_json)
        self.state_dir = ensure_exchange_structure(self.base_json_path)["state"]

    def build_all(self, *, force_write: bool = False) -> StateSnapshotReport:
        symbols = self._resolve_symbols()
        if len(symbols) == 0:
            return StateSnapshotReport(
                symbols_processed=0,
                files_updated=0,
                files_unchanged=0,
                errors=[],
            )

        errors: list[str] = []
        files_updated = 0
        files_unchanged = 0
        now_utc = datetime.now(timezone.utc)

        self.source.connect()
        try:
            for symbol in symbols:
                try:
                    existing = self._load_existing_state(symbol)
                    payload = self._build_symbol_state(
                        symbol=symbol,
                        existing=existing,
                        now_utc=now_utc,
                    )
                    if force_write or self._has_changes(existing=existing, new_payload=payload):
                        self._save_state(symbol=symbol, payload=payload)
                        files_updated += 1
                    else:
                        files_unchanged += 1
                except Exception as error:  # pragma: no cover - runtime safety
                    message = f"{symbol}: {error}"
                    errors.append(message)
                    logger.exception("Failed to normalize state snapshot for %s", symbol)
        finally:
            self.source.close()

        self._save_schema_version(now_utc=now_utc)
        return StateSnapshotReport(
            symbols_processed=len(symbols),
            files_updated=files_updated,
            files_unchanged=files_unchanged,
            errors=errors,
        )

    def _resolve_symbols(self) -> list[str]:
        symbols: list[str] = []
        for raw in self.config.auto_eye.symbols:
            symbol = self.source.resolve_symbol(raw)
            if symbol and symbol not in symbols:
                symbols.append(symbol)
        return symbols

    def _build_symbol_state(
        self,
        *,
        symbol: str,
        existing: dict[str, object] | None,
        now_utc: datetime,
    ) -> dict[str, object]:
        state: dict[str, object] = {}
        if isinstance(existing, dict):
            state = deepcopy(existing)

        state["schema_version"] = str(state.get("schema_version") or STATE_SCHEMA_VERSION)
        state["symbol"] = symbol
        state["updated_at_utc"] = now_utc.isoformat()
        state["market"] = self._build_market(symbol=symbol, existing=existing, now_utc=now_utc)
        state["timeframes"] = self._normalize_timeframes(state.get("timeframes"))
        state.pop("derived", None)
        state.pop("scenarios", None)
        return state

    def _normalize_timeframes(self, raw_timeframes: object) -> dict[str, object]:
        normalized: dict[str, object] = {}
        if isinstance(raw_timeframes, dict):
            for timeframe, payload in raw_timeframes.items():
                normalized[str(timeframe).strip().upper()] = self._normalize_timeframe_payload(
                    payload
                )

        for timeframe in REQUIRED_TIMEFRAMES:
            if timeframe not in normalized:
                normalized[timeframe] = self._empty_timeframe_payload()
        return normalized

    @classmethod
    def _normalize_timeframe_payload(cls, raw_payload: object) -> dict[str, object]:
        if not isinstance(raw_payload, dict):
            return cls._empty_timeframe_payload()

        payload = deepcopy(raw_payload)
        payload["initialized"] = bool(payload.get("initialized"))
        payload["updated_at_utc"] = payload.get("updated_at_utc")
        payload["last_bar_time_utc"] = payload.get("last_bar_time_utc") or payload.get(
            "last_bar_time"
        )
        payload.pop("last_bar_time", None)

        payload["elements"] = cls._normalize_elements(payload.get("elements"))
        payload["state"] = cls._normalize_state_block(payload.get("state"))
        return payload

    @classmethod
    def _normalize_elements(cls, raw_elements: object) -> dict[str, list[object]]:
        if not isinstance(raw_elements, dict):
            return cls._empty_elements()

        elements = cls._empty_elements()
        for key in ("fvg", "snr", "fractals", "rb"):
            if key == "fractals":
                value = raw_elements.get("fractals")
                if not isinstance(value, list):
                    value = raw_elements.get("fractal")
            else:
                value = raw_elements.get(key)
            if isinstance(value, list):
                elements[key] = [
                    item
                    for item in value
                    if isinstance(item, dict) and cls._is_actual_raw_element(item)
                ]
        return elements

    @staticmethod
    def _is_actual_raw_element(item: dict[str, object]) -> bool:
        status = str(item.get("status") or "").strip().lower()
        if not status:
            return True
        return status not in {
            STATUS_INVALIDATED,
            STATUS_MITIGATED_FULL,
            STATUS_BROKEN,
            STATUS_EXPIRED,
        }

    @staticmethod
    def _normalize_state_block(raw_state: object) -> dict[str, object]:
        if not isinstance(raw_state, dict):
            return {
                "initialized_elements": {},
                "last_bar_time_by_element_utc": {},
            }

        state = deepcopy(raw_state)
        initialized_elements = state.get("initialized_elements")
        if not isinstance(initialized_elements, dict):
            initialized_elements = {}
        last_bar_by_element = state.get("last_bar_time_by_element_utc")
        if not isinstance(last_bar_by_element, dict):
            last_bar_by_element = {}
        state["initialized_elements"] = initialized_elements
        state["last_bar_time_by_element_utc"] = last_bar_by_element
        return state

    @classmethod
    def _empty_timeframe_payload(cls) -> dict[str, object]:
        return {
            "initialized": False,
            "updated_at_utc": None,
            "last_bar_time_utc": None,
            "elements": cls._empty_elements(),
            "state": {
                "initialized_elements": {},
                "last_bar_time_by_element_utc": {},
            },
        }

    @staticmethod
    def _empty_elements() -> dict[str, list[object]]:
        return {
            "fvg": [],
            "snr": [],
            "fractals": [],
            "rb": [],
        }

    def _build_market(
        self,
        *,
        symbol: str,
        existing: dict[str, object] | None,
        now_utc: datetime,
    ) -> dict[str, object]:
        quote = self.source.get_market_quote(symbol)
        if quote is not None:
            return quote

        if isinstance(existing, dict):
            old_market = existing.get("market")
            if isinstance(old_market, dict):
                price = old_market.get("price")
                tick_time = old_market.get("tick_time_utc")
                if isinstance(price, (int, float)) and tick_time:
                    return old_market

        return {
            "price": 0.0,
            "bid": None,
            "ask": None,
            "source": "MT5",
            "tick_time_utc": now_utc.isoformat(),
        }

    def _load_existing_state(self, symbol: str) -> dict[str, object] | None:
        path = state_json_path(self.base_json_path, symbol)
        if not path.exists() or path.stat().st_size == 0:
            return None
        with path.open("r", encoding="utf-8") as file:
            raw = json.load(file)
        if not isinstance(raw, dict):
            return None
        return raw

    @staticmethod
    def _has_changes(
        *,
        existing: dict[str, object] | None,
        new_payload: dict[str, object],
    ) -> bool:
        if existing is None:
            return True
        old_norm = StateSnapshotBuilder._normalize_for_compare(existing)
        new_norm = StateSnapshotBuilder._normalize_for_compare(new_payload)
        return old_norm != new_norm

    @staticmethod
    def _normalize_for_compare(payload: dict[str, object]) -> dict[str, object]:
        normalized = deepcopy(payload)
        normalized.pop("updated_at_utc", None)
        return normalized

    def _save_state(self, *, symbol: str, payload: dict[str, object]) -> None:
        path = state_json_path(self.base_json_path, symbol)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        logger.info("State snapshot updated: %s", path)

    def _save_schema_version(self, *, now_utc: datetime) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        schema_path = self.state_dir / "schema_version.json"
        payload = {
            "schema_version": STATE_SCHEMA_VERSION,
            "updated_at_utc": now_utc.isoformat(),
            "notes": "State schema for market elements only",
        }
        if schema_path.exists() and schema_path.stat().st_size > 0:
            try:
                with schema_path.open("r", encoding="utf-8") as file:
                    old = json.load(file)
                if isinstance(old, dict) and old.get("schema_version") == STATE_SCHEMA_VERSION:
                    return
            except Exception:  # pragma: no cover - runtime resilience
                pass

        with schema_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

