from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config_loader import AppConfig

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.exporters import ensure_exchange_structure, resolve_output_path
from auto_eye.models import OHLCBar, TrackedElement, datetime_from_iso, datetime_to_iso
from auto_eye.mt5_source import MT5BarsSource
from auto_eye.scenario_service import ScenarioSnapshotBuilder
from auto_eye.timeframes import timeframe_to_seconds

logger = logging.getLogger(__name__)

VALID_FVG_STATUSES = {"active", "touched"}
VALID_SNR_STATUSES = {"active", "retested"}


@dataclass
class BacktestRunReport:
    run_id: str
    output_dir: Path
    symbols_processed: int
    steps_processed: int
    proposals_created: int
    scenarios_expired: int
    events_written: int
    errors: list[str]


class BacktestScenarioRunner:
    def __init__(
        self,
        *,
        config: AppConfig,
        detectors: dict[str, MarketElementDetector],
        source: MT5BarsSource | None = None,
        scenario_builder: ScenarioSnapshotBuilder | None = None,
    ) -> None:
        self.config = config
        self.detectors = detectors
        self.source = source or MT5BarsSource(config)
        self.scenario_builder = scenario_builder or ScenarioSnapshotBuilder(config=config)
        self.base_json_path = resolve_output_path(config.auto_eye.output_json)

    def run(
        self,
        *,
        start_time_utc: datetime,
        end_time_utc: datetime | None = None,
        symbols: list[str] | None = None,
        run_id: str | None = None,
        warmup_bars: int = 500,
    ) -> BacktestRunReport:
        if not self.detectors:
            raise RuntimeError("No detectors configured in auto_eye.elements")

        start_utc = start_time_utc.astimezone(timezone.utc)
        end_utc = end_time_utc.astimezone(timezone.utc) if end_time_utc is not None else datetime.now(timezone.utc)
        if end_utc <= start_utc:
            raise ValueError("end_time_utc must be greater than start_time_utc")

        normalized_symbols = self._resolve_symbols(symbols)
        if len(normalized_symbols) == 0:
            raise RuntimeError("No symbols resolved for backtest")

        backtest_run_id = self._resolve_run_id(run_id, start_utc, end_utc, normalized_symbols)
        output_dir = ensure_exchange_structure(self.base_json_path)["backtests"] / backtest_run_id
        output_dir.mkdir(parents=True, exist_ok=True)

        proposals_path = output_dir / "proposals.jsonl"
        events_path = output_dir / "events.jsonl"
        summary_path = output_dir / "summary.json"

        logger.info(
            "Backtest started: run_id=%s symbols=%s start=%s end=%s",
            backtest_run_id,
            len(normalized_symbols),
            datetime_to_iso(start_utc),
            datetime_to_iso(end_utc),
        )

        all_proposals: list[dict[str, Any]] = []
        all_events: list[dict[str, Any]] = []
        errors: list[str] = []
        steps_processed = 0
        proposals_created = 0
        scenarios_expired = 0

        self.source.connect()
        try:
            for symbol in normalized_symbols:
                try:
                    report = self._run_symbol(
                        symbol=symbol,
                        start_time_utc=start_utc,
                        end_time_utc=end_utc,
                        run_id=backtest_run_id,
                        warmup_bars=warmup_bars,
                    )
                    steps_processed += report["steps_processed"]
                    proposals_created += report["proposals_created"]
                    scenarios_expired += report["scenarios_expired"]
                    all_proposals.extend(report["proposals"])
                    all_events.extend(report["events"])
                except Exception as error:  # pragma: no cover - runtime safety
                    errors.append(f"{symbol}: {error}")
                    logger.exception("Backtest failed for %s", symbol)
        finally:
            self.source.close()

        self._write_jsonl(proposals_path, all_proposals)
        self._write_jsonl(events_path, all_events)

        summary_payload = {
            "run_id": backtest_run_id,
            "started_at_utc": datetime_to_iso(datetime.now(timezone.utc)),
            "start_time_utc": datetime_to_iso(start_utc),
            "end_time_utc": datetime_to_iso(end_utc),
            "symbols": normalized_symbols,
            "symbols_processed": len(normalized_symbols),
            "steps_processed": steps_processed,
            "proposals_created": proposals_created,
            "scenarios_expired": scenarios_expired,
            "events_written": len(all_events),
            "errors": errors,
        }
        with summary_path.open("w", encoding="utf-8") as file:
            json.dump(summary_payload, file, ensure_ascii=False, indent=2)

        logger.info(
            "Backtest completed: run_id=%s proposals=%s events=%s errors=%s",
            backtest_run_id,
            proposals_created,
            len(all_events),
            len(errors),
        )

        return BacktestRunReport(
            run_id=backtest_run_id,
            output_dir=output_dir,
            symbols_processed=len(normalized_symbols),
            steps_processed=steps_processed,
            proposals_created=proposals_created,
            scenarios_expired=scenarios_expired,
            events_written=len(all_events),
            errors=errors,
        )

    def _run_symbol(
        self,
        *,
        symbol: str,
        start_time_utc: datetime,
        end_time_utc: datetime,
        run_id: str,
        warmup_bars: int,
    ) -> dict[str, Any]:
        m5_seconds = timeframe_to_seconds("M5")
        h1_seconds = timeframe_to_seconds("H1")

        warmup_start_m5 = start_time_utc - timedelta(seconds=m5_seconds * max(3, warmup_bars))
        warmup_start_h1 = start_time_utc - timedelta(seconds=h1_seconds * max(3, warmup_bars // 12))

        m5_bars = self.source.fetch_range(
            symbol=symbol,
            timeframe_code="M5",
            start_time_utc=warmup_start_m5,
            end_time_utc=end_time_utc,
        )
        h1_bars = self.source.fetch_range(
            symbol=symbol,
            timeframe_code="H1",
            start_time_utc=warmup_start_h1,
            end_time_utc=end_time_utc,
        )

        if m5_bars is None or h1_bars is None:
            raise RuntimeError("MT5 returned None for requested backtest range")

        if len(m5_bars) < 3 or len(h1_bars) < 3:
            return {
                "steps_processed": 0,
                "proposals_created": 0,
                "scenarios_expired": 0,
                "proposals": [],
                "events": [
                    {
                        "event": "symbol_skipped",
                        "run_id": run_id,
                        "symbol": symbol,
                        "reason": "not_enough_bars",
                        "m5_bars": len(m5_bars),
                        "h1_bars": len(h1_bars),
                    }
                ],
            }

        point_size = self.source.get_point_size(symbol)
        previous_payload: dict[str, Any] | None = None
        previous_trend: str | None = None

        h1_index = -1
        steps_processed = 0
        proposals_created = 0
        scenarios_expired = 0
        proposals: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []

        for idx, m5_bar in enumerate(m5_bars):
            step_time = m5_bar.time
            if step_time < start_time_utc:
                continue
            if step_time > end_time_utc:
                break

            while h1_index + 1 < len(h1_bars) and h1_bars[h1_index + 1].time <= step_time:
                h1_index += 1
            if h1_index < 0:
                continue

            m5_slice = m5_bars[: idx + 1]
            h1_slice = h1_bars[: h1_index + 1]
            if len(m5_slice) < 3 or len(h1_slice) < 3:
                continue

            state_payload = self._build_state_snapshot(
                symbol=symbol,
                m5_bars=m5_slice,
                h1_bars=h1_slice,
                point_size=point_size,
                now_utc=step_time,
            )
            trend_payload = self._build_trend_snapshot(
                symbol=symbol,
                state_payload=state_payload,
                now_utc=step_time,
            )
            current_trend = self._trend_direction(trend_payload)
            if current_trend != previous_trend:
                previous_trend = current_trend
                events.append(
                    {
                        "event": "trend_changed",
                        "run_id": run_id,
                        "symbol": symbol,
                        "time_utc": datetime_to_iso(step_time),
                        "trend": current_trend,
                    }
                )

            old_active_ids = self._scenario_ids(previous_payload, key="active")
            old_history_ids = self._scenario_ids(previous_payload, key="history")

            next_payload, _, _ = self.scenario_builder.build_symbol_snapshot(
                symbol=symbol,
                state_payload=state_payload,
                trend_payload=trend_payload,
                existing_payload=previous_payload,
                now_utc=step_time,
            )

            new_active = self._index_by_id(next_payload.get("active"))
            new_history = self._index_by_id(next_payload.get("history"))

            created_ids = [scenario_id for scenario_id in new_active if scenario_id not in old_active_ids]
            for scenario_id in created_ids:
                scenario = new_active[scenario_id]
                proposals_created += 1
                proposals.append(self._proposal_record(run_id=run_id, symbol=symbol, scenario=scenario))
                events.append(
                    {
                        "event": "scenario_created",
                        "run_id": run_id,
                        "symbol": symbol,
                        "time_utc": datetime_to_iso(step_time),
                        "scenario_id": scenario_id,
                        "scenario_type": scenario.get("scenario_type"),
                        "direction": scenario.get("direction"),
                    }
                )

            expired_ids = [
                scenario_id
                for scenario_id, scenario in new_history.items()
                if scenario_id not in old_history_ids
                and str(scenario.get("status") or "").strip().lower() == "expired"
            ]
            if len(expired_ids) > 0:
                scenarios_expired += len(expired_ids)
                for scenario_id in expired_ids:
                    events.append(
                        {
                            "event": "scenario_expired",
                            "run_id": run_id,
                            "symbol": symbol,
                            "time_utc": datetime_to_iso(step_time),
                            "scenario_id": scenario_id,
                        }
                    )

            previous_payload = next_payload
            steps_processed += 1

        return {
            "steps_processed": steps_processed,
            "proposals_created": proposals_created,
            "scenarios_expired": scenarios_expired,
            "proposals": proposals,
            "events": events,
        }
    def _build_state_snapshot(
        self,
        *,
        symbol: str,
        m5_bars: list[OHLCBar],
        h1_bars: list[OHLCBar],
        point_size: float,
        now_utc: datetime,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "updated_at_utc": datetime_to_iso(now_utc),
            "market": {
                "price": float(m5_bars[-1].close),
                "source": "MT5-backtest",
                "tick_time_utc": datetime_to_iso(now_utc),
            },
            "timeframes": {
                "M5": {
                    "elements": self._build_tf_elements(
                        symbol=symbol,
                        timeframe="M5",
                        bars=m5_bars,
                        point_size=point_size,
                    )
                },
                "H1": {
                    "elements": self._build_tf_elements(
                        symbol=symbol,
                        timeframe="H1",
                        bars=h1_bars,
                        point_size=point_size,
                    )
                },
            },
        }

    def _build_tf_elements(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: list[OHLCBar],
        point_size: float,
    ) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, dict[str, dict[str, Any]]] = {
            "fvg": {},
            "snr": {},
            "rb": {},
            "fractals": {},
        }

        for detector_name, detector in self.detectors.items():
            detected = detector.detect(
                symbol=symbol,
                timeframe=timeframe,
                bars=bars,
                point_size=point_size,
                config=self.config.auto_eye,
            )
            for item in detected:
                detector.update_status(
                    element=item,
                    bars=bars,
                    config=self.config.auto_eye,
                )

                converted = self._tracked_to_state(item)
                if converted is None:
                    continue
                target_key = self._state_key_for_detector(detector_name)
                if target_key not in grouped:
                    continue
                item_id = str(converted.get("id") or "").strip()
                if not item_id:
                    continue
                grouped[target_key][item_id] = converted

        return {
            key: list(sorted(values.values(), key=self._element_sort_key))
            for key, values in grouped.items()
        }

    @staticmethod
    def _state_key_for_detector(detector_name: str) -> str:
        normalized = detector_name.strip().lower()
        if normalized == "fractal":
            return "fractals"
        if normalized in {"fvg", "snr", "rb"}:
            return normalized
        return normalized

    @staticmethod
    def _element_sort_key(item: dict[str, Any]) -> tuple[str, str]:
        for key in (
            "formation_time_utc",
            "break_time_utc",
            "confirm_time_utc",
            "c3_time_utc",
            "pivot_time_utc",
        ):
            value = str(item.get(key) or "")
            if value:
                return value, str(item.get("id") or "")
        return "", str(item.get("id") or "")

    @staticmethod
    def _tracked_to_state(item: TrackedElement) -> dict[str, Any] | None:
        raw = item.to_dict()
        element_type = str(raw.get("element_type") or "").strip().lower()

        if element_type == "fvg":
            return {
                "id": raw.get("id"),
                "element_type": "fvg",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "direction": raw.get("direction"),
                "formation_time_utc": raw.get("formation_time"),
                "c3_time_utc": raw.get("c3_time"),
                "fvg_low": raw.get("fvg_low"),
                "fvg_high": raw.get("fvg_high"),
                "status": raw.get("status"),
                "touched_time_utc": raw.get("touched_time"),
                "mitigated_time_utc": raw.get("mitigated_time"),
            }

        if element_type == "snr":
            return {
                "id": raw.get("id"),
                "element_type": "snr",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "role": raw.get("role"),
                "break_type": raw.get("break_type"),
                "break_time_utc": raw.get("break_time"),
                "snr_low": raw.get("snr_low"),
                "snr_high": raw.get("snr_high"),
                "status": raw.get("status"),
                "retest_time_utc": raw.get("retest_time"),
                "invalidated_time_utc": raw.get("invalidated_time"),
            }

        if element_type == "rb":
            return {
                "id": raw.get("id"),
                "element_type": "rb",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "rb_type": raw.get("rb_type"),
                "confirm_time_utc": raw.get("confirm_time"),
                "rb_low": raw.get("rb_low"),
                "rb_high": raw.get("rb_high"),
                "status": raw.get("status"),
                "broken_time_utc": raw.get("broken_time"),
            }

        if element_type == "fractal":
            return {
                "id": raw.get("id"),
                "element_type": "fractal",
                "symbol": raw.get("symbol"),
                "timeframe": raw.get("timeframe"),
                "fractal_type": raw.get("fractal_type"),
                "confirm_time_utc": raw.get("confirm_time"),
                "extreme_price": raw.get("extreme_price"),
                "l_price": raw.get("l_price"),
            }

        return None

    def _build_trend_snapshot(
        self,
        *,
        symbol: str,
        state_payload: dict[str, Any],
        now_utc: datetime,
    ) -> dict[str, Any]:
        source_signal = self._resolve_latest_h1_signal(state_payload)
        direction = self._direction_from_signal(source_signal)
        now_iso = datetime_to_iso(now_utc)

        trend_block: dict[str, Any] = {
            "timeframe": "H1",
            "direction": direction,
            "determined_at_utc": now_iso,
            "source_signal": self._strip_internal_fields(source_signal),
        }
        return {
            "schema_version": "1.0.0",
            "symbol": symbol,
            "updated_at_utc": now_iso,
            "trend": trend_block,
            "history": [],
        }

    def _resolve_latest_h1_signal(
        self,
        state_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        raw_timeframes = state_payload.get("timeframes")
        if not isinstance(raw_timeframes, dict):
            return None

        h1_payload = raw_timeframes.get("H1")
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

        signal_time = BacktestScenarioRunner._parse_signal_time(
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

        signal_time = BacktestScenarioRunner._parse_signal_time(
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
    def _trend_direction(trend_payload: dict[str, Any]) -> str:
        trend = trend_payload.get("trend") if isinstance(trend_payload, dict) else None
        if not isinstance(trend, dict):
            return "neutral"
        direction = str(trend.get("direction") or "").strip().lower()
        if direction in {"bullish", "bearish", "neutral"}:
            return direction
        return "neutral"

    @staticmethod
    def _proposal_record(
        *,
        run_id: str,
        symbol: str,
        scenario: dict[str, Any],
    ) -> dict[str, Any]:
        htf_anchor = scenario.get("htf_anchor")
        if not isinstance(htf_anchor, dict):
            htf_anchor = {}
        ltf_confirmation = scenario.get("ltf_confirmation")
        if not isinstance(ltf_confirmation, dict):
            ltf_confirmation = {}
        entry = scenario.get("entry")
        if not isinstance(entry, dict):
            entry = {}
        sl = scenario.get("sl")
        if not isinstance(sl, dict):
            sl = {}
        tp = scenario.get("tp")
        if not isinstance(tp, dict):
            tp = None

        target_id = None
        if isinstance(tp, dict):
            target = tp.get("target_element")
            if isinstance(target, dict):
                target_id = target.get("id")

        return {
            "run_id": run_id,
            "created_at_utc": scenario.get("created_at_utc"),
            "symbol": symbol,
            "scenario_id": scenario.get("scenario_id"),
            "scenario_type": scenario.get("scenario_type"),
            "direction": scenario.get("direction"),
            "trend_at_creation": scenario.get("trend_at_creation"),
            "htf_anchor_id": htf_anchor.get("element_id"),
            "ltf_confirmation_id": ltf_confirmation.get("element_id"),
            "entry": {
                "type": entry.get("type"),
                "price": entry.get("price"),
                "zone": entry.get("zone"),
            },
            "sl": {"price": sl.get("price")},
            "tp": None if tp is None else {"price": tp.get("price"), "target_id": target_id},
        }

    @staticmethod
    def _index_by_id(raw_items: Any) -> dict[str, dict[str, Any]]:
        if not isinstance(raw_items, list):
            return {}
        out: dict[str, dict[str, Any]] = {}
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            scenario_id = str(item.get("scenario_id") or "").strip()
            if not scenario_id:
                continue
            out[scenario_id] = item
        return out

    @staticmethod
    def _scenario_ids(payload: dict[str, Any] | None, *, key: str) -> set[str]:
        if not isinstance(payload, dict):
            return set()
        return set(BacktestScenarioRunner._index_by_id(payload.get(key)).keys())

    def _resolve_symbols(self, symbols: list[str] | None) -> list[str]:
        raw_symbols = symbols if symbols is not None else list(self.config.auto_eye.symbols)
        resolved: list[str] = []
        for item in raw_symbols:
            symbol = self.source.resolve_symbol(str(item).strip())
            if symbol and symbol not in resolved:
                resolved.append(symbol)
        return resolved

    @staticmethod
    def _resolve_run_id(
        run_id: str | None,
        start_time_utc: datetime,
        end_time_utc: datetime,
        symbols: list[str],
    ) -> str:
        if run_id is not None and str(run_id).strip():
            return str(run_id).strip()

        symbol_part = "multi" if len(symbols) != 1 else symbols[0]
        start_part = start_time_utc.strftime("%Y%m%dT%H%M%SZ")
        end_part = end_time_utc.strftime("%Y%m%dT%H%M%SZ")
        return f"{symbol_part}_{start_part}_{end_part}"

    @staticmethod
    def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row, ensure_ascii=False))
                file.write("\n")
