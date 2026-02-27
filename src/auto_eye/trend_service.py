from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config_loader import AppConfig

from auto_eye.exporters import ensure_exchange_structure, resolve_output_path, trend_json_path
from auto_eye.models import datetime_from_iso, datetime_to_iso

logger = logging.getLogger(__name__)

TREND_SCHEMA_VERSION = "1.0.0"
TREND_TIMEFRAME = "H1"
TREND_HISTORY_LIMIT = 50
VALID_FVG_STATUSES = {"active", "touched"}
VALID_SNR_STATUSES = {"active", "retested"}


@dataclass
class TrendSnapshotReport:
    symbols_processed: int
    files_updated: int
    files_unchanged: int
    errors: list[str]


class TrendSnapshotBuilder:
    def __init__(
        self,
        *,
        config: AppConfig,
        history_limit: int = TREND_HISTORY_LIMIT,
    ) -> None:
        self.config = config
        self.history_limit = max(1, int(history_limit))
        self.base_json_path = resolve_output_path(config.auto_eye.output_json)
        exchange_paths = ensure_exchange_structure(self.base_json_path)
        self.state_dir = exchange_paths["state"]
        self.trend_dir = exchange_paths["trends"]

    def build_all(self, *, force_write: bool = False) -> TrendSnapshotReport:
        state_files = self._discover_state_files()
        if len(state_files) == 0:
            return TrendSnapshotReport(
                symbols_processed=0,
                files_updated=0,
                files_unchanged=0,
                errors=[],
            )

        now_utc = datetime.now(timezone.utc)
        files_updated = 0
        files_unchanged = 0
        errors: list[str] = []

        for state_path in state_files:
            symbol = state_path.stem
            try:
                state_payload = self._load_json(state_path)
                state_symbol = str(state_payload.get("symbol") or "").strip()
                if state_symbol:
                    symbol = state_symbol

                trend_path = trend_json_path(self.base_json_path, symbol)
                existing_payload = self._load_optional_json(trend_path)
                next_payload = self._build_trend_payload(
                    symbol=symbol,
                    state_payload=state_payload,
                    existing_payload=existing_payload,
                    now_utc=now_utc,
                )

                if force_write or self._should_write(existing_payload, next_payload):
                    self._save_atomic(trend_path, next_payload)
                    files_updated += 1
                else:
                    files_unchanged += 1
            except Exception as error:  # pragma: no cover - runtime safety
                errors.append(f"{symbol}: {error}")
                logger.exception("Failed to update trend snapshot for %s", symbol)

        return TrendSnapshotReport(
            symbols_processed=len(state_files),
            files_updated=files_updated,
            files_unchanged=files_unchanged,
            errors=errors,
        )

    def _discover_state_files(self) -> list[Path]:
        if not self.state_dir.exists():
            return []
        files = [
            path
            for path in self.state_dir.glob("*.json")
            if path.name.lower() != "schema_version.json"
        ]
        files.sort(key=lambda path: path.name)
        return files

    def _build_trend_payload(
        self,
        *,
        symbol: str,
        state_payload: dict[str, Any],
        existing_payload: dict[str, Any] | None,
        now_utc: datetime,
    ) -> dict[str, Any]:
        source_signal = self._resolve_latest_h1_signal(state_payload)
        direction = self._direction_from_signal(source_signal)
        now_iso = datetime_to_iso(now_utc)

        history = self._build_history(
            existing_payload=existing_payload,
            new_direction=direction,
            source_signal=source_signal,
            changed_at_utc=now_iso,
        )

        trend_block: dict[str, Any] = {
            "timeframe": TREND_TIMEFRAME,
            "direction": direction,
            "determined_at_utc": now_iso,
            "source_signal": self._strip_internal_fields(source_signal),
        }

        return {
            "schema_version": TREND_SCHEMA_VERSION,
            "symbol": symbol,
            "updated_at_utc": now_iso,
            "trend": trend_block,
            "history": history,
        }

    def _build_history(
        self,
        *,
        existing_payload: dict[str, Any] | None,
        new_direction: str,
        source_signal: dict[str, Any] | None,
        changed_at_utc: str | None,
    ) -> list[dict[str, Any]]:
        existing_history: list[dict[str, Any]] = []
        old_direction: str | None = None

        if isinstance(existing_payload, dict):
            raw_history = existing_payload.get("history")
            if isinstance(raw_history, list):
                existing_history = [
                    item for item in raw_history if isinstance(item, dict)
                ]
            old_direction = self._extract_direction(existing_payload)

        if old_direction is None or old_direction == new_direction:
            return existing_history[-self.history_limit :]

        appended = list(existing_history)
        appended.append(
            {
                "changed_at_utc": changed_at_utc,
                "direction": new_direction,
                "source_signal": self._strip_internal_fields(source_signal),
            }
        )
        return appended[-self.history_limit :]

    def _resolve_latest_h1_signal(
        self,
        state_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        raw_timeframes = state_payload.get("timeframes")
        if not isinstance(raw_timeframes, dict):
            return None

        h1_payload = raw_timeframes.get(TREND_TIMEFRAME)
        if not isinstance(h1_payload, dict):
            return None

        raw_elements = h1_payload.get("elements")
        if not isinstance(raw_elements, dict):
            return None

        signals: list[dict[str, Any]] = []

        for item in raw_elements.get("fvg", []):
            signal = self._signal_from_fvg(item)
            if signal is not None:
                signals.append(signal)

        for item in raw_elements.get("snr", []):
            signal = self._signal_from_snr(item)
            if signal is not None:
                signals.append(signal)

        if len(signals) == 0:
            return None

        signals.sort(
            key=lambda signal: (
                signal["_signal_time"],
                str(signal.get("element_id") or ""),
                str(signal.get("type") or ""),
            )
        )
        return signals[-1]

    @staticmethod
    def _signal_from_fvg(raw: object) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None

        status = str(raw.get("status") or "").strip().lower()
        if status not in VALID_FVG_STATUSES:
            return None

        direction = str(raw.get("direction") or "").strip().lower()
        if direction == "bullish":
            polarity = "positive"
        elif direction == "bearish":
            polarity = "negative"
        else:
            return None

        signal_time = TrendSnapshotBuilder._parse_signal_time(
            raw,
            "formation_time_utc",
            "formation_time",
            "c3_time_utc",
            "c3_time",
        )
        if signal_time is None:
            return None

        element_id = str(raw.get("id") or "").strip()
        if not element_id:
            return None

        return {
            "type": "fvg",
            "polarity": polarity,
            "signal_time_utc": datetime_to_iso(signal_time),
            "element_id": element_id,
            "_signal_time": signal_time,
        }

    @staticmethod
    def _signal_from_snr(raw: object) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None

        status = str(raw.get("status") or "").strip().lower()
        if status not in VALID_SNR_STATUSES:
            return None

        role = str(raw.get("role") or "").strip().lower()
        break_type = str(raw.get("break_type") or "").strip().lower()

        if role == "support" or break_type == "break_up_close":
            polarity = "positive"
        elif role == "resistance" or break_type == "break_down_close":
            polarity = "negative"
        else:
            return None

        signal_time = TrendSnapshotBuilder._parse_signal_time(
            raw,
            "break_time_utc",
            "break_time",
            "formation_time_utc",
            "formation_time",
        )
        if signal_time is None:
            return None

        element_id = str(raw.get("id") or "").strip()
        if not element_id:
            return None

        return {
            "type": "snr",
            "polarity": polarity,
            "signal_time_utc": datetime_to_iso(signal_time),
            "element_id": element_id,
            "_signal_time": signal_time,
        }

    @staticmethod
    def _parse_signal_time(raw: dict[str, Any], *keys: str) -> datetime | None:
        for key in keys:
            parsed = datetime_from_iso(str(raw.get(key) or ""))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _direction_from_signal(signal: dict[str, Any] | None) -> str:
        if not isinstance(signal, dict):
            return "neutral"
        polarity = str(signal.get("polarity") or "").strip().lower()
        if polarity == "positive":
            return "bullish"
        if polarity == "negative":
            return "bearish"
        return "neutral"

    @staticmethod
    def _extract_direction(payload: dict[str, Any] | None) -> str | None:
        if not isinstance(payload, dict):
            return None
        trend = payload.get("trend")
        if not isinstance(trend, dict):
            return None
        direction = str(trend.get("direction") or "").strip().lower()
        if direction in {"bullish", "bearish", "neutral"}:
            return direction
        return None

    @staticmethod
    def _extract_material_fields(payload: dict[str, Any] | None) -> tuple[str | None, str | None, str | None]:
        if not isinstance(payload, dict):
            return None, None, None

        trend = payload.get("trend")
        if not isinstance(trend, dict):
            return None, None, None

        direction = str(trend.get("direction") or "").strip().lower() or None
        source_signal = trend.get("source_signal")
        if not isinstance(source_signal, dict):
            return direction, None, None

        element_id = str(source_signal.get("element_id") or "").strip() or None
        signal_time_utc = str(source_signal.get("signal_time_utc") or "").strip() or None
        return direction, element_id, signal_time_utc

    @classmethod
    def _should_write(
        cls,
        existing_payload: dict[str, Any] | None,
        next_payload: dict[str, Any],
    ) -> bool:
        if not isinstance(existing_payload, dict):
            return True

        old_direction, old_element_id, old_signal_time = cls._extract_material_fields(
            existing_payload
        )
        new_direction, new_element_id, new_signal_time = cls._extract_material_fields(
            next_payload
        )

        return (
            old_direction != new_direction
            or old_element_id != new_element_id
            or old_signal_time != new_signal_time
        )

    @staticmethod
    def _strip_internal_fields(source_signal: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(source_signal, dict):
            return None
        cleaned = {
            key: value
            for key, value in source_signal.items()
            if not str(key).startswith("_")
        }
        if len(cleaned) == 0:
            return None
        return cleaned

    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as file:
            raw = json.load(file)
        if not isinstance(raw, dict):
            raise RuntimeError(f"Invalid JSON object: {path}")
        return raw

    @classmethod
    def _load_optional_json(cls, path: Path) -> dict[str, Any] | None:
        if not path.exists() or path.stat().st_size == 0:
            return None
        return cls._load_json(path)

    @staticmethod
    def _save_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.parent / f"{path.name}.tmp"
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        temp_path.replace(path)
        logger.info("Trend snapshot updated: %s", path)
