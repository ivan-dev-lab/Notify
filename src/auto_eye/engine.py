from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from config_loader import AppConfig

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.models import AutoEyeState, TrackedElement, datetime_to_iso
from auto_eye.mt5_source import MT5BarsSource
from auto_eye.state_store import AutoEyeStateStore, resolve_path
from auto_eye.timeframes import normalize_timeframes

logger = logging.getLogger(__name__)


class AutoEyeEngine:
    def __init__(
        self,
        *,
        config: AppConfig,
        detectors: dict[str, MarketElementDetector],
        source: MT5BarsSource | None = None,
        state_store: AutoEyeStateStore | None = None,
    ) -> None:
        self.config = config
        self.detectors = detectors
        self.source = source or MT5BarsSource(config)
        self.state_store = state_store or AutoEyeStateStore(
            resolve_path(config.auto_eye.state_json)
        )

    def run_once(self, *, force_full_scan: bool = False) -> dict[str, object]:
        auto_eye_cfg = self.config.auto_eye
        if not auto_eye_cfg.enabled:
            raise RuntimeError("auto_eye.enabled=false in config")

        if not self.detectors:
            raise RuntimeError("No detectors enabled in auto_eye.elements")

        now_utc = datetime.now(timezone.utc)
        state = self.state_store.load()
        symbols = self._resolve_symbols()
        timeframes = normalize_timeframes(auto_eye_cfg.timeframes)
        enabled_types = set(self.detectors.keys())
        history_cutoff = now_utc - timedelta(
            days=auto_eye_cfg.history_days + auto_eye_cfg.history_buffer_days
        )

        logger.info(
            "AutoEye run started: symbols=%s timeframes=%s detectors=%s force_full_scan=%s",
            len(symbols),
            len(timeframes),
            ",".join(sorted(enabled_types)),
            force_full_scan,
        )

        processed_keys: set[str] = set()
        processed_elements: list[TrackedElement] = []
        errors: list[str] = []

        self.source.connect()
        try:
            for symbol in symbols:
                for timeframe in timeframes:
                    key = self._build_key(symbol, timeframe)
                    processed_keys.add(key)

                    try:
                        key_elements = [
                            element
                            for element in state.elements
                            if (
                                element.symbol == symbol
                                and element.timeframe == timeframe
                                and element.element_type in enabled_types
                            )
                        ]

                        last_bar_time = state.last_bar_time_by_key.get(key)
                        should_full_scan = force_full_scan or last_bar_time is None
                        if should_full_scan:
                            bars = self.source.fetch_history(
                                symbol=symbol,
                                timeframe_code=timeframe,
                                history_days=auto_eye_cfg.history_days,
                                history_buffer_days=auto_eye_cfg.history_buffer_days,
                            )
                        else:
                            bars = self.source.fetch_incremental(
                                symbol=symbol,
                                timeframe_code=timeframe,
                                last_bar_time=last_bar_time,
                                incremental_bars=auto_eye_cfg.incremental_bars,
                                history_days=auto_eye_cfg.history_days,
                                history_buffer_days=auto_eye_cfg.history_buffer_days,
                            )

                        if bars is None:
                            raise RuntimeError(
                                f"No bars returned from MT5 for {symbol} {timeframe}"
                            )

                        if len(bars) < 3:
                            logger.warning(
                                "Not enough bars for %s %s: %s",
                                symbol,
                                timeframe,
                                len(bars),
                            )
                            processed_elements.extend(key_elements)
                            continue

                        point_size = self.source.get_point_size(symbol)
                        updated_key_elements = self._process_key_elements(
                            symbol=symbol,
                            timeframe=timeframe,
                            bars=bars,
                            point_size=point_size,
                            existing=key_elements,
                        )
                        updated_key_elements = [
                            element
                            for element in updated_key_elements
                            if element.formation_time >= history_cutoff
                        ]
                        processed_elements.extend(updated_key_elements)
                        state.last_bar_time_by_key[key] = bars[-1].time

                    except Exception as error:  # pragma: no cover - runtime safety
                        error_message = f"{symbol} {timeframe}: {error}"
                        errors.append(error_message)
                        logger.exception(
                            "AutoEye failed for symbol=%s timeframe=%s",
                            symbol,
                            timeframe,
                        )
                        # Keep previous state for this key if update failed.
                        fallback_elements = [
                            element
                            for element in state.elements
                            if (
                                element.symbol == symbol
                                and element.timeframe == timeframe
                                and element.element_type in enabled_types
                            )
                        ]
                        processed_elements.extend(fallback_elements)
        finally:
            self.source.close()

        preserved_elements = [
            element
            for element in state.elements
            if (
                element.element_type not in enabled_types
                or self._build_key(element.symbol, element.timeframe) not in processed_keys
            )
        ]

        state.elements = self._deduplicate_elements(preserved_elements + processed_elements)
        state.updated_at_utc = now_utc
        self.state_store.save(state)

        exported_payload = self._build_export_payload(
            now_utc=now_utc,
            state=state,
            symbols=symbols,
            timeframes=timeframes,
            errors=errors,
        )
        return exported_payload

    def _process_key_elements(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: list,
        point_size: float,
        existing: list[TrackedElement],
    ) -> list[TrackedElement]:
        key_result: list[TrackedElement] = []

        for detector_name, detector in self.detectors.items():
            existing_by_id: dict[str, TrackedElement] = {
                element.id: element
                for element in existing
                if element.element_type == detector_name
            }
            detected = detector.detect(
                symbol=symbol,
                timeframe=timeframe,
                bars=bars,
                point_size=point_size,
                config=self.config.auto_eye,
            )
            for element in detected:
                matched_id = self._find_matching_existing_id(
                    candidate=element,
                    existing=list(existing_by_id.values()),
                )
                if matched_id is not None and matched_id != element.id:
                    element.id = matched_id
                if element.id not in existing_by_id:
                    existing_by_id[element.id] = element

            for element in existing_by_id.values():
                detector.update_status(
                    element=element,
                    bars=bars,
                    config=self.config.auto_eye,
                )

            key_result.extend(existing_by_id.values())

        key_result.sort(key=lambda element: (element.c3_time, element.id))
        return key_result

    def _build_export_payload(
        self,
        *,
        now_utc: datetime,
        state: AutoEyeState,
        symbols: list[str],
        timeframes: list[str],
        errors: list[str],
    ) -> dict[str, object]:
        element_rows = [element.to_dict() for element in state.elements]
        return {
            "generated_at_utc": now_utc.isoformat(),
            "source": "metatrader5",
            "symbols": symbols,
            "timeframes": timeframes,
            "enabled_elements": sorted(self.detectors.keys()),
            "count": len(state.elements),
            "errors": errors,
            "elements": element_rows,
        }

    def _export_payload(
        self,
        payload: dict[str, object],
        elements: list[TrackedElement],
    ) -> None:
        _ = payload
        _ = elements
        logger.debug(
            "Legacy per-element export is disabled. Use ../Exchange/State/<SYMBOL>.json as source of truth."
        )

    def _resolve_symbols(self) -> list[str]:
        symbols: list[str] = []
        for raw in self.config.auto_eye.symbols:
            resolved = self.source.resolve_symbol(raw)
            if resolved and resolved not in symbols:
                symbols.append(resolved)
        return symbols

    @staticmethod
    def _build_key(symbol: str, timeframe: str) -> str:
        return f"{symbol}|{timeframe}"

    @staticmethod
    def _deduplicate_elements(elements: list[TrackedElement]) -> list[TrackedElement]:
        deduplicated: dict[str, TrackedElement] = {}
        for element in elements:
            deduplicated[element.id] = element
        values = list(deduplicated.values())
        values.sort(key=lambda item: (item.symbol, item.timeframe, item.c3_time, item.id))
        return values

    @staticmethod
    def _find_matching_existing_id(
        *,
        candidate: TrackedElement,
        existing: list[TrackedElement],
    ) -> str | None:
        candidate_key = AutoEyeEngine._snr_identity_key(candidate)
        if candidate_key is None:
            return None
        for item in existing:
            if item.element_type != candidate.element_type:
                continue
            if AutoEyeEngine._snr_identity_key(item) == candidate_key:
                return item.id
        return None

    @staticmethod
    def _snr_identity_key(element: TrackedElement) -> str | None:
        if element.element_type != "snr":
            return None
        origin = str(element.metadata.get("origin_fractal_id") or "").strip()
        break_time = str(
            element.metadata.get("break_time") or datetime_to_iso(element.c3_time) or ""
        ).strip()
        role = str(element.metadata.get("role") or element.direction or "").strip()
        break_type = str(element.metadata.get("break_type") or "").strip()
        if not origin or not break_time or not role:
            return None
        return f"{origin}|{break_time}|{role}|{break_type}"

