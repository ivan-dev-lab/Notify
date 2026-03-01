from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config_loader import AppConfig

from auto_eye.exporters import (
    ensure_exchange_structure,
    resolve_output_path,
    scenario_json_path,
    trend_json_path,
)
from auto_eye.models import datetime_from_iso, datetime_to_iso

logger = logging.getLogger(__name__)

SCENARIO_SCHEMA_VERSION = "1.0.0"
H1 = "H1"
M5 = "M5"

BULLISH = "bullish"
BEARISH = "bearish"
NEUTRAL = "neutral"

SCENARIO_TYPE_A = "trend_continuation"
SCENARIO_TYPE_B = "reversal_at_opposite"

PENDING = "pending"
APPROVED = "approved"
EXPIRED = "expired"
ACTIVE_SCENARIO_STATUSES = {PENDING, APPROVED}

VALID_FVG_STATUSES = {"active", "touched", "mitigated_partial"}
VALID_SNR_STATUSES = {"active", "retested"}
VALID_RB_STATUSES = {"active"}
INVALID_ANCHOR_STATUSES = {"invalidated", "mitigated_full", "broken", "expired"}


@dataclass
class ScenarioSnapshotReport:
    symbols_processed: int
    files_updated: int
    files_unchanged: int
    scenarios_created: int
    scenarios_expired: int
    errors: list[str]


class ScenarioSnapshotBuilder:
    def __init__(
        self,
        *,
        config: AppConfig,
        expiry_hours: int = 12,
        tp_prefer_zones: bool = True,
        require_tp: bool = False,
    ) -> None:
        self.config = config
        self.expiry_hours = max(1, int(expiry_hours))
        self.tp_prefer_zones = bool(tp_prefer_zones)
        self.require_tp = bool(require_tp)

        self.base_json_path = resolve_output_path(config.auto_eye.output_json)
        exchange_paths = ensure_exchange_structure(self.base_json_path)
        self.state_dir = exchange_paths["state"]

    def build_all(self, *, force_write: bool = False) -> ScenarioSnapshotReport:
        state_files = self._discover_state_files()
        if len(state_files) == 0:
            return ScenarioSnapshotReport(
                symbols_processed=0,
                files_updated=0,
                files_unchanged=0,
                scenarios_created=0,
                scenarios_expired=0,
                errors=[],
            )

        now_utc = datetime.now(timezone.utc)
        files_updated = 0
        files_unchanged = 0
        scenarios_created = 0
        scenarios_expired = 0
        errors: list[str] = []

        for state_path in state_files:
            symbol = state_path.stem
            try:
                state_payload = self._load_json(state_path)
                state_symbol = str(state_payload.get("symbol") or "").strip()
                if state_symbol:
                    symbol = state_symbol

                trend_payload = self._load_optional_json(
                    trend_json_path(self.base_json_path, symbol)
                )
                scenario_path = scenario_json_path(self.base_json_path, symbol)
                existing_payload = self._load_optional_json(scenario_path)

                next_payload, created_count, expired_count = self._build_symbol_payload(
                    symbol=symbol,
                    state_payload=state_payload,
                    trend_payload=trend_payload,
                    existing_payload=existing_payload,
                    now_utc=now_utc,
                )
                scenarios_created += created_count
                scenarios_expired += expired_count

                if force_write or self._should_write(existing_payload, next_payload):
                    self._save_json(scenario_path, next_payload)
                    files_updated += 1
                else:
                    files_unchanged += 1
            except Exception as error:  # pragma: no cover - runtime safety
                errors.append(f"{symbol}: {error}")
                logger.exception("Failed to update scenarios for %s", symbol)

        return ScenarioSnapshotReport(
            symbols_processed=len(state_files),
            files_updated=files_updated,
            files_unchanged=files_unchanged,
            scenarios_created=scenarios_created,
            scenarios_expired=scenarios_expired,
            errors=errors,
        )

    def build_symbol_snapshot(
        self,
        *,
        symbol: str,
        state_payload: dict[str, Any],
        trend_payload: dict[str, Any] | None,
        existing_payload: dict[str, Any] | None,
        now_utc: datetime,
    ) -> tuple[dict[str, Any], int, int]:
        return self._build_symbol_payload(
            symbol=symbol,
            state_payload=state_payload,
            trend_payload=trend_payload,
            existing_payload=existing_payload,
            now_utc=now_utc,
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

    def _build_symbol_payload(
        self,
        *,
        symbol: str,
        state_payload: dict[str, Any],
        trend_payload: dict[str, Any] | None,
        existing_payload: dict[str, Any] | None,
        now_utc: datetime,
    ) -> tuple[dict[str, Any], int, int]:
        active, history = self._extract_existing(existing_payload)
        state_index = self._index_state_elements(state_payload)

        active, history, expired_count = self._expire_scenarios(
            active=active,
            history=history,
            state_index=state_index,
            now_utc=now_utc,
        )

        known_ids = {
            str(item.get("scenario_id") or "").strip()
            for item in [*active, *history]
            if str(item.get("scenario_id") or "").strip()
        }

        trend_direction = self._resolve_trend_direction(trend_payload)
        created_count = 0
        if trend_direction in {BULLISH, BEARISH}:
            for candidate in self._generate_scenarios(
                symbol=symbol,
                state_payload=state_payload,
                trend_direction=trend_direction,
                now_utc=now_utc,
            ):
                if self._scenario_has_missing_references(
                    scenario=candidate,
                    state_index=state_index,
                ):
                    continue
                scenario_id = str(candidate.get("scenario_id") or "").strip()
                if not scenario_id or scenario_id in known_ids:
                    continue
                active.append(candidate)
                known_ids.add(scenario_id)
                created_count += 1

        active.sort(key=self._scenario_sort_key)
        history.sort(key=self._scenario_sort_key)

        payload: dict[str, Any] = {
            "schema_version": SCENARIO_SCHEMA_VERSION,
            "symbol": symbol,
            "updated_at_utc": datetime_to_iso(now_utc),
            "active": active,
            "history": history,
        }
        return payload, created_count, expired_count

    @staticmethod
    def _extract_existing(
        payload: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not isinstance(payload, dict):
            return [], []

        active_raw = payload.get("active")
        history_raw = payload.get("history")

        active = [item for item in active_raw if isinstance(item, dict)] if isinstance(active_raw, list) else []
        history = [item for item in history_raw if isinstance(item, dict)] if isinstance(history_raw, list) else []
        return list(active), list(history)

    def _expire_scenarios(
        self,
        *,
        active: list[dict[str, Any]],
        history: list[dict[str, Any]],
        state_index: dict[tuple[str, str], str],
        now_utc: datetime,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
        next_active: list[dict[str, Any]] = []
        next_history = list(history)
        expired_count = 0

        for scenario in active:
            status = str(scenario.get("status") or "").strip().lower()
            if status not in ACTIVE_SCENARIO_STATUSES:
                next_history.append(scenario)
                continue

            expired_reason = self._is_scenario_expired(
                scenario=scenario,
                state_index=state_index,
                now_utc=now_utc,
            )
            if expired_reason is None:
                next_active.append(scenario)
                continue

            expired_count += 1
            updated = dict(scenario)
            updated["status"] = EXPIRED
            updated["updated_at_utc"] = datetime_to_iso(now_utc)
            metadata = updated.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            metadata["expired_reason"] = expired_reason
            updated["metadata"] = metadata
            next_history.append(updated)

        return next_active, next_history, expired_count

    def _is_scenario_expired(
        self,
        *,
        scenario: dict[str, Any],
        state_index: dict[tuple[str, str], str],
        now_utc: datetime,
    ) -> str | None:
        expires_at = datetime_from_iso(str(scenario.get("expires_at_utc") or ""))
        if expires_at is not None and expires_at <= now_utc:
            return "time"

        if self._scenario_has_missing_references(
            scenario=scenario,
            state_index=state_index,
        ):
            return "missing_state_element"

        if self._anchor_became_invalid(scenario=scenario, state_index=state_index):
            return "anchor_invalidated"

        return None

    @staticmethod
    def _anchor_became_invalid(
        *,
        scenario: dict[str, Any],
        state_index: dict[tuple[str, str], str],
    ) -> bool:
        anchor = scenario.get("htf_anchor")
        if not isinstance(anchor, dict):
            return False

        anchor_type = str(anchor.get("type") or "").strip().lower()
        anchor_id = str(anchor.get("element_id") or "").strip()
        if not anchor_type or not anchor_id:
            return False

        status = state_index.get((anchor_type, anchor_id))
        if status is None:
            return False
        return status in INVALID_ANCHOR_STATUSES

    def _index_state_elements(self, state_payload: dict[str, Any]) -> dict[tuple[str, str], str]:
        index: dict[tuple[str, str], str] = {}
        for timeframe in (H1, M5):
            for element_type in ("fvg", "snr", "rb", "fractals"):
                for element in self._state_tf_elements(state_payload, timeframe, element_type):
                    parsed = self._normalize_element(timeframe, element_type, element)
                    if parsed is None:
                        continue
                    index[(parsed["label"], parsed["id"])] = parsed["status"]
        return index

    def _scenario_has_missing_references(
        self,
        *,
        scenario: dict[str, Any],
        state_index: dict[tuple[str, str], str],
    ) -> bool:
        required_pairs = [
            self._extract_reference_pair(scenario.get("htf_anchor")),
            self._extract_reference_pair(scenario.get("ltf_confirmation")),
        ]
        for pair in required_pairs:
            if pair is None or pair not in state_index:
                return True

        tp = scenario.get("tp")
        if isinstance(tp, dict):
            target = self._extract_reference_pair(tp.get("target_element"))
            if target is not None and target not in state_index:
                return True

        metadata = scenario.get("metadata")
        if isinstance(metadata, dict):
            opposite_touch = self._extract_reference_pair(metadata.get("opposite_touch"))
            if opposite_touch is not None and opposite_touch not in state_index:
                return True

        evidence_ids = scenario.get("evidence_ids")
        if isinstance(evidence_ids, list):
            for raw_item in evidence_ids:
                text = str(raw_item or "").strip()
                if ":" not in text:
                    continue
                ref_type, ref_id = text.split(":", 1)
                pair = (ref_type.strip().lower(), ref_id.strip())
                if not pair[0] or not pair[1]:
                    continue
                if pair not in state_index:
                    return True

        return False

    @staticmethod
    def _extract_reference_pair(raw: object) -> tuple[str, str] | None:
        if not isinstance(raw, dict):
            return None
        ref_type = str(raw.get("type") or "").strip().lower()
        ref_id = str(raw.get("element_id") or raw.get("id") or "").strip()
        if not ref_type or not ref_id:
            return None
        return ref_type, ref_id

    def _generate_scenarios(
        self,
        *,
        symbol: str,
        state_payload: dict[str, Any],
        trend_direction: str,
        now_utc: datetime,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []

        scenario_a = self._build_scenario_a(
            symbol=symbol,
            state_payload=state_payload,
            trend_direction=trend_direction,
            now_utc=now_utc,
        )
        if scenario_a is not None:
            out.append(scenario_a)

        scenario_b = self._build_scenario_b(
            symbol=symbol,
            state_payload=state_payload,
            trend_direction=trend_direction,
            now_utc=now_utc,
        )
        if scenario_b is not None:
            out.append(scenario_b)

        return out

    def _build_scenario_a(
        self,
        *,
        symbol: str,
        state_payload: dict[str, Any],
        trend_direction: str,
        now_utc: datetime,
    ) -> dict[str, Any] | None:
        price = self._price(state_payload)
        if price is None:
            return None

        h1_anchor = self._select_start_element(
            elements=self._collect_h1_inefficiencies(state_payload, trend_direction),
            price=price,
            now_utc=now_utc,
            require_interaction=True,
        )
        if h1_anchor is None:
            return None

        m5_confirmation = self._select_m5_confirmation(
            confirmations=self._collect_m5_confirmations(state_payload, trend_direction),
            min_signal_time=h1_anchor["start_dt"],
        )
        if m5_confirmation is None:
            return None

        trade_direction = "long" if trend_direction == BULLISH else "short"
        sl_price = h1_anchor["zone_low"] if trade_direction == "long" else h1_anchor["zone_high"]
        tp_payload = self._choose_take_profit(
            state_payload=state_payload,
            trade_direction=trade_direction,
            entry_price=price,
            exclude_element_ids={h1_anchor["id"]},
        )
        if tp_payload is None and self.require_tp:
            return None

        metadata = {
            "mode": "live",
            "start": {
                "anchor_start_time_utc": h1_anchor.get("start_time_utc"),
                "anchor_interaction_time_utc": h1_anchor.get("interaction_time_utc"),
            },
        }

        scenario = self._build_base_scenario(
            symbol=symbol,
            trend_direction=trend_direction,
            scenario_type=SCENARIO_TYPE_A,
            trade_direction=trade_direction,
            h1_anchor=h1_anchor,
            m5_confirmation=m5_confirmation,
            entry_price=price,
            sl_price=sl_price,
            tp_payload=tp_payload,
            evidence_ids=[
                f"{h1_anchor['label']}:{h1_anchor['id']}",
                f"{m5_confirmation['label']}:{m5_confirmation['id']}",
            ],
            metadata=metadata,
            now_utc=now_utc,
        )
        scenario["scenario_id"] = self._build_scenario_id(scenario)
        return scenario
    def _build_scenario_b(
        self,
        *,
        symbol: str,
        state_payload: dict[str, Any],
        trend_direction: str,
        now_utc: datetime,
    ) -> dict[str, Any] | None:
        price = self._price(state_payload)
        if price is None:
            return None

        counter_direction = BEARISH if trend_direction == BULLISH else BULLISH

        opposite_touch = self._select_start_element(
            elements=self._collect_h1_inefficiencies(state_payload, counter_direction),
            price=price,
            now_utc=now_utc,
            require_interaction=True,
        )
        if opposite_touch is None:
            return None

        counter_anchor = self._select_start_element(
            elements=self._collect_h1_counter_anchors(
                state_payload=state_payload,
                counter_direction=counter_direction,
                min_signal_time=opposite_touch["start_dt"],
            ),
            price=price,
            now_utc=now_utc,
            require_interaction=False,
        )
        if counter_anchor is None:
            return None

        m5_confirmation = self._select_m5_confirmation(
            confirmations=self._collect_m5_confirmations(state_payload, counter_direction),
            min_signal_time=counter_anchor["start_dt"],
        )
        if m5_confirmation is None:
            return None

        trade_direction = "long" if counter_direction == BULLISH else "short"
        sl_price = (
            counter_anchor["zone_low"]
            if trade_direction == "long"
            else counter_anchor["zone_high"]
        )
        tp_payload = self._choose_take_profit(
            state_payload=state_payload,
            trade_direction=trade_direction,
            entry_price=price,
            exclude_element_ids={counter_anchor["id"]},
        )
        if tp_payload is None and self.require_tp:
            return None

        metadata = {
            "mode": "live",
            "start": {
                "opposite_touch_time_utc": opposite_touch.get("start_time_utc"),
                "counter_anchor_start_time_utc": counter_anchor.get("start_time_utc"),
                "counter_anchor_interaction_time_utc": counter_anchor.get(
                    "interaction_time_utc"
                ),
            },
            "opposite_touch": {
                "type": opposite_touch["label"],
                "element_id": opposite_touch["id"],
                "signal_time_utc": opposite_touch["signal_time_utc"],
            },
        }

        scenario = self._build_base_scenario(
            symbol=symbol,
            trend_direction=trend_direction,
            scenario_type=SCENARIO_TYPE_B,
            trade_direction=trade_direction,
            h1_anchor=counter_anchor,
            m5_confirmation=m5_confirmation,
            entry_price=price,
            sl_price=sl_price,
            tp_payload=tp_payload,
            evidence_ids=[
                f"{opposite_touch['label']}:{opposite_touch['id']}",
                f"{counter_anchor['label']}:{counter_anchor['id']}",
                f"{m5_confirmation['label']}:{m5_confirmation['id']}",
            ],
            metadata=metadata,
            now_utc=now_utc,
        )
        scenario["scenario_id"] = self._build_scenario_id(scenario)
        return scenario

    def _build_base_scenario(
        self,
        *,
        symbol: str,
        trend_direction: str,
        scenario_type: str,
        trade_direction: str,
        h1_anchor: dict[str, Any],
        m5_confirmation: dict[str, Any],
        entry_price: float,
        sl_price: float,
        tp_payload: dict[str, Any] | None,
        evidence_ids: list[str],
        metadata: dict[str, Any],
        now_utc: datetime,
    ) -> dict[str, Any]:
        now_iso = datetime_to_iso(now_utc)
        return {
            "scenario_id": "",
            "symbol": symbol,
            "created_at_utc": now_iso,
            "updated_at_utc": now_iso,
            "trend_at_creation": trend_direction,
            "scenario_type": scenario_type,
            "direction": trade_direction,
            "status": PENDING,
            "htf_anchor": {
                "type": h1_anchor["label"],
                "element_id": h1_anchor["id"],
                "zone": [h1_anchor["zone_low"], h1_anchor["zone_high"]],
                "signal_time_utc": h1_anchor["signal_time_utc"],
            },
            "ltf_confirmation": {
                "type": m5_confirmation["label"],
                "element_id": m5_confirmation["id"],
                "signal_time_utc": m5_confirmation["signal_time_utc"],
            },
            "entry": {
                "type": "market",
                "price": entry_price,
                "zone": [m5_confirmation["zone_low"], m5_confirmation["zone_high"]],
            },
            "sl": {
                "price": sl_price,
                "rule": "behind_anchor",
            },
            "tp": tp_payload,
            "evidence_ids": evidence_ids,
            "expires_at_utc": datetime_to_iso(
                now_utc + timedelta(hours=self.expiry_hours)
            ),
            "metadata": metadata,
        }

    @staticmethod
    def _build_scenario_id(scenario: dict[str, Any]) -> str:
        anchor = scenario.get("htf_anchor")
        if not isinstance(anchor, dict):
            anchor = {}
        confirmation = scenario.get("ltf_confirmation")
        if not isinstance(confirmation, dict):
            confirmation = {}
        entry = scenario.get("entry")
        if not isinstance(entry, dict):
            entry = {}
        sl = scenario.get("sl")
        if not isinstance(sl, dict):
            sl = {}
        tp = scenario.get("tp")
        if not isinstance(tp, dict):
            tp = {}

        entry_zone = entry.get("zone") if isinstance(entry.get("zone"), list) else []
        entry_low = ScenarioSnapshotBuilder._safe_float(entry_zone[0], fallback=0.0) if len(entry_zone) > 0 else 0.0
        entry_high = ScenarioSnapshotBuilder._safe_float(entry_zone[1], fallback=0.0) if len(entry_zone) > 1 else 0.0

        target_element = tp.get("target_element")
        if not isinstance(target_element, dict):
            target_element = {}

        seed = "|".join(
            [
                str(scenario.get("symbol") or "").strip(),
                str(scenario.get("scenario_type") or "").strip(),
                str(scenario.get("direction") or "").strip(),
                str(anchor.get("element_id") or "").strip(),
                str(confirmation.get("element_id") or "").strip(),
                f"{entry_low:.10f}",
                f"{entry_high:.10f}",
                f"{ScenarioSnapshotBuilder._safe_float(sl.get('price'), fallback=0.0):.10f}",
                f"{ScenarioSnapshotBuilder._safe_float(tp.get('price'), fallback=0.0):.10f}",
                str(target_element.get("id") or "").strip(),
            ]
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()

    def _choose_take_profit(
        self,
        *,
        state_payload: dict[str, Any],
        trade_direction: str,
        entry_price: float,
        exclude_element_ids: set[str],
    ) -> dict[str, Any] | None:
        candidates = self._collect_h1_candidates(state_payload)

        ranked: list[tuple[float, dict[str, Any], float]] = []
        for candidate in candidates:
            candidate_id = str(candidate.get("id") or "")
            if candidate_id in exclude_element_ids:
                continue

            candidate_level = self._candidate_level(candidate, trade_direction)
            if candidate_level is None:
                continue

            if trade_direction == "long":
                if candidate_level <= entry_price:
                    continue
                distance = candidate_level - entry_price
            else:
                if candidate_level >= entry_price:
                    continue
                distance = entry_price - candidate_level

            ranked.append((distance, candidate, candidate_level))

        if len(ranked) == 0:
            return None

        if self.tp_prefer_zones:
            zone_only = [item for item in ranked if item[1]["type"] in {"fvg", "snr", "rb"}]
            if len(zone_only) > 0:
                ranked = zone_only

        ranked.sort(
            key=lambda item: (
                item[0],
                item[1]["signal_dt"],
                item[1]["id"],
            )
        )
        _, winner, level = ranked[0]
        return {
            "price": level,
            "target_element": {
                "type": winner["label"],
                "id": winner["id"],
            },
        }

    @staticmethod
    def _candidate_level(candidate: dict[str, Any], trade_direction: str) -> float | None:
        if candidate["type"] in {"fvg", "snr", "rb"}:
            if trade_direction == "long":
                return candidate["zone_low"]
            return candidate["zone_high"]
        if candidate["type"] == "fractal":
            return candidate.get("level")
        return None

    def _collect_h1_candidates(self, state_payload: dict[str, Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for element_type in ("fvg", "snr", "rb", "fractals"):
            for raw in self._state_tf_elements(state_payload, H1, element_type):
                parsed = self._normalize_element(H1, element_type, raw)
                if parsed is None:
                    continue
                if parsed["status"] in {"invalidated", "mitigated_full", "broken", "expired"}:
                    continue
                if parsed["type"] == "fvg" and parsed["status"] not in VALID_FVG_STATUSES:
                    continue
                if parsed["type"] == "snr" and parsed["status"] not in VALID_SNR_STATUSES:
                    continue
                if parsed["type"] == "rb" and parsed["status"] not in VALID_RB_STATUSES:
                    continue
                out.append(parsed)
        return out

    def _collect_h1_inefficiencies(
        self,
        state_payload: dict[str, Any],
        direction: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for element_type in ("fvg", "snr"):
            for raw in self._state_tf_elements(state_payload, H1, element_type):
                parsed = self._normalize_element(H1, element_type, raw)
                if parsed is None or parsed["direction"] != direction:
                    continue
                if parsed["type"] == "fvg" and parsed["status"] not in VALID_FVG_STATUSES:
                    continue
                if parsed["type"] == "snr" and parsed["status"] not in VALID_SNR_STATUSES:
                    continue
                out.append(parsed)
        return out

    def _collect_h1_counter_anchors(
        self,
        *,
        state_payload: dict[str, Any],
        counter_direction: str,
        min_signal_time: datetime,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for element_type in ("fvg", "snr", "rb"):
            for raw in self._state_tf_elements(state_payload, H1, element_type):
                parsed = self._normalize_element(H1, element_type, raw)
                if parsed is None or parsed["direction"] != counter_direction:
                    continue
                if parsed["signal_dt"] < min_signal_time:
                    continue
                if parsed["type"] == "fvg" and parsed["status"] not in VALID_FVG_STATUSES:
                    continue
                if parsed["type"] == "snr" and parsed["status"] not in VALID_SNR_STATUSES:
                    continue
                if parsed["type"] == "rb" and parsed["status"] not in VALID_RB_STATUSES:
                    continue
                out.append(parsed)
        return out

    def _collect_m5_confirmations(
        self,
        state_payload: dict[str, Any],
        direction: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for element_type in ("fvg", "snr"):
            for raw in self._state_tf_elements(state_payload, M5, element_type):
                parsed = self._normalize_element(M5, element_type, raw)
                if parsed is None or parsed["direction"] != direction:
                    continue
                if parsed["type"] == "fvg" and parsed["status"] not in VALID_FVG_STATUSES:
                    continue
                if parsed["type"] == "snr" and parsed["status"] not in VALID_SNR_STATUSES:
                    continue
                out.append(parsed)
        return out

    def _select_start_element(
        self,
        *,
        elements: list[dict[str, Any]],
        price: float,
        now_utc: datetime,
        require_interaction: bool,
    ) -> dict[str, Any] | None:
        prepared: list[dict[str, Any]] = []
        for item in elements:
            if item["signal_dt"] > now_utc:
                continue
            interaction_dt = self._interaction_time(item=item, price=price, now_utc=now_utc)
            if require_interaction and interaction_dt is None:
                continue
            start_dt = interaction_dt or item["signal_dt"]
            candidate = dict(item)
            candidate["interaction_dt"] = interaction_dt
            candidate["interaction_time_utc"] = datetime_to_iso(interaction_dt)
            candidate["start_dt"] = start_dt
            candidate["start_time_utc"] = datetime_to_iso(start_dt)
            prepared.append(candidate)

        if len(prepared) == 0:
            return None

        prepared = self._collapse_overlapping_snr(
            elements=prepared,
            price=price,
            prefer_smallest_zone=False,
        )
        prepared.sort(
            key=lambda item: (
                self._zone_distance_to_price(item=item, price=price),
                self._dt_sort_desc(item.get("start_dt")),
                self._dt_sort_desc(item.get("signal_dt")),
                item["zone_size"],
                item["id"],
            )
        )
        return prepared[0]

    def _select_m5_confirmation(
        self,
        *,
        confirmations: list[dict[str, Any]],
        min_signal_time: datetime,
    ) -> dict[str, Any] | None:
        eligible = [
            item
            for item in confirmations
            if item["signal_dt"] >= min_signal_time
        ]
        if len(eligible) == 0:
            return None
        eligible = self._collapse_overlapping_snr(
            elements=eligible,
            price=None,
            prefer_smallest_zone=True,
        )

        snr_candidates = [item for item in eligible if item["type"] == "snr"]
        if len(snr_candidates) > 0:
            min_size = min(item["zone_size"] for item in snr_candidates)
            smallest = [
                item
                for item in snr_candidates
                if abs(item["zone_size"] - min_size) <= 1e-12
            ]
            smallest.sort(
                key=lambda item: (
                    item["signal_dt"],
                    item["id"],
                )
            )
            return smallest[-1]

        eligible.sort(
            key=lambda item: (
                item["signal_dt"],
                item["id"],
                item["label"],
            )
        )
        return eligible[-1]

    def _collapse_overlapping_snr(
        self,
        *,
        elements: list[dict[str, Any]],
        price: float | None,
        prefer_smallest_zone: bool,
    ) -> list[dict[str, Any]]:
        snr_items = [item for item in elements if item.get("type") == "snr"]
        if len(snr_items) <= 1:
            return elements

        non_snr = [item for item in elements if item.get("type") != "snr"]
        if prefer_smallest_zone:
            snr_items.sort(
                key=lambda item: (
                    item["zone_size"],
                    self._dt_sort_desc(item.get("signal_dt")),
                    self._zone_distance_to_price(item=item, price=price),
                    item["id"],
                )
            )
        else:
            snr_items.sort(
                key=lambda item: (
                    self._zone_distance_to_price(item=item, price=price),
                    self._dt_sort_desc(item.get("start_dt")),
                    self._dt_sort_desc(item.get("signal_dt")),
                    item["zone_size"],
                    item["id"],
                )
            )

        selected: list[dict[str, Any]] = []
        for candidate in snr_items:
            if any(
                self._zones_overlap(candidate, existing)
                for existing in selected
            ):
                continue
            selected.append(candidate)

        return [*non_snr, *selected]

    @staticmethod
    def _zones_overlap(first: dict[str, Any], second: dict[str, Any]) -> bool:
        first_low = float(first["zone_low"])
        first_high = float(first["zone_high"])
        second_low = float(second["zone_low"])
        second_high = float(second["zone_high"])
        return min(first_high, second_high) >= max(first_low, second_low)

    @staticmethod
    def _zone_distance_to_price(*, item: dict[str, Any], price: float | None) -> float:
        if price is None:
            return 0.0
        low = float(item["zone_low"])
        high = float(item["zone_high"])
        if low <= price <= high:
            return 0.0
        return min(abs(price - low), abs(price - high))

    @staticmethod
    def _dt_sort_desc(value: object) -> float:
        if isinstance(value, datetime):
            return -value.timestamp()
        return float("inf")

    @staticmethod
    def _interaction_time(
        *,
        item: dict[str, Any],
        price: float,
        now_utc: datetime,
    ) -> datetime | None:
        del now_utc

        interaction_dt = item.get("interaction_dt")
        if isinstance(interaction_dt, datetime):
            return interaction_dt

        if item["zone_low"] <= price <= item["zone_high"]:
            return item["signal_dt"]

        return None
    def _normalize_element(
        self,
        timeframe: str,
        element_type: str,
        raw: dict[str, Any],
    ) -> dict[str, Any] | None:
        normalized_type = element_type.strip().lower()
        if normalized_type == "fractals":
            normalized_type = "fractal"

        if normalized_type == "fvg":
            return self._normalize_fvg(timeframe, raw)
        if normalized_type == "snr":
            return self._normalize_snr(timeframe, raw)
        if normalized_type == "rb":
            return self._normalize_rb(timeframe, raw)
        if normalized_type == "fractal":
            return self._normalize_fractal(timeframe, raw)
        return None

    def _normalize_fvg(self, timeframe: str, raw: dict[str, Any]) -> dict[str, Any] | None:
        element_id = str(raw.get("id") or "").strip()
        direction = str(raw.get("direction") or "").strip().lower()
        signal_time = self._signal_time(raw, "formation_time_utc", "formation_time", "c3_time_utc", "c3_time")
        interaction_time = self._signal_time(raw, "touched_time_utc", "touched_time")
        low = self._safe_float(raw.get("fvg_low"), fallback=0.0)
        high = self._safe_float(raw.get("fvg_high"), fallback=0.0)
        if not element_id or direction not in {BULLISH, BEARISH} or signal_time is None:
            return None
        zone_low = min(low, high)
        zone_high = max(low, high)
        return {
            "id": element_id,
            "type": "fvg",
            "label": f"{timeframe.lower()}_fvg",
            "status": self._safe_status(raw.get("status")),
            "direction": direction,
            "signal_dt": signal_time,
            "signal_time_utc": datetime_to_iso(signal_time),
            "interaction_dt": interaction_time,
            "zone_low": zone_low,
            "zone_high": zone_high,
            "zone_size": max(0.0, zone_high - zone_low),
        }

    def _normalize_snr(self, timeframe: str, raw: dict[str, Any]) -> dict[str, Any] | None:
        element_id = str(raw.get("id") or "").strip()
        direction = self._parse_direction_from_snr(raw)
        signal_time = self._signal_time(raw, "break_time_utc", "break_time", "formation_time_utc", "formation_time")
        interaction_time = self._signal_time(raw, "retest_time_utc", "retest_time")
        low = self._safe_float(raw.get("snr_low"), fallback=0.0)
        high = self._safe_float(raw.get("snr_high"), fallback=0.0)
        if not element_id or direction is None or signal_time is None:
            return None
        zone_low = min(low, high)
        zone_high = max(low, high)
        return {
            "id": element_id,
            "type": "snr",
            "label": f"{timeframe.lower()}_snr",
            "status": self._safe_status(raw.get("status")),
            "direction": direction,
            "signal_dt": signal_time,
            "signal_time_utc": datetime_to_iso(signal_time),
            "interaction_dt": interaction_time,
            "zone_low": zone_low,
            "zone_high": zone_high,
            "zone_size": max(0.0, zone_high - zone_low),
        }

    def _normalize_rb(self, timeframe: str, raw: dict[str, Any]) -> dict[str, Any] | None:
        element_id = str(raw.get("id") or "").strip()
        direction = self._parse_direction_from_rb(raw)
        signal_time = self._signal_time(raw, "confirm_time_utc", "confirm_time", "formation_time_utc", "formation_time")
        low = self._safe_float(raw.get("rb_low"), fallback=0.0)
        high = self._safe_float(raw.get("rb_high"), fallback=0.0)
        if not element_id or direction is None or signal_time is None:
            return None
        zone_low = min(low, high)
        zone_high = max(low, high)
        return {
            "id": element_id,
            "type": "rb",
            "label": f"{timeframe.lower()}_rb",
            "status": self._safe_status(raw.get("status")),
            "direction": direction,
            "signal_dt": signal_time,
            "signal_time_utc": datetime_to_iso(signal_time),
            "interaction_dt": None,
            "zone_low": zone_low,
            "zone_high": zone_high,
            "zone_size": max(0.0, zone_high - zone_low),
        }

    def _normalize_fractal(self, timeframe: str, raw: dict[str, Any]) -> dict[str, Any] | None:
        element_id = str(raw.get("id") or "").strip()
        signal_time = self._signal_time(raw, "confirm_time_utc", "confirm_time", "formation_time_utc", "formation_time")
        level = self._safe_float(raw.get("extreme_price"), fallback=None)
        if level is None:
            level = self._safe_float(raw.get("l_price"), fallback=None)
        if not element_id or signal_time is None or level is None:
            return None
        return {
            "id": element_id,
            "type": "fractal",
            "label": f"{timeframe.lower()}_fractal",
            "status": self._safe_status(raw.get("status")),
            "direction": None,
            "signal_dt": signal_time,
            "signal_time_utc": datetime_to_iso(signal_time),
            "interaction_dt": None,
            "zone_low": level,
            "zone_high": level,
            "zone_size": 0.0,
            "level": level,
        }

    @staticmethod
    def _signal_time(raw: dict[str, Any], *keys: str) -> datetime | None:
        for key in keys:
            parsed = datetime_from_iso(str(raw.get(key) or ""))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _parse_direction_from_snr(raw: dict[str, Any]) -> str | None:
        role = str(raw.get("role") or "").strip().lower()
        break_type = str(raw.get("break_type") or "").strip().lower()
        if role == "support" or break_type == "break_up_close":
            return BULLISH
        if role == "resistance" or break_type == "break_down_close":
            return BEARISH
        return None

    @staticmethod
    def _parse_direction_from_rb(raw: dict[str, Any]) -> str | None:
        rb_type = str(raw.get("rb_type") or raw.get("direction") or "").strip().lower()
        if rb_type == "low":
            return BULLISH
        if rb_type == "high":
            return BEARISH
        return None

    @staticmethod
    def _resolve_trend_direction(payload: dict[str, Any] | None) -> str:
        if not isinstance(payload, dict):
            return NEUTRAL
        trend = payload.get("trend")
        if not isinstance(trend, dict):
            return NEUTRAL
        direction = str(trend.get("direction") or "").strip().lower()
        if direction in {BULLISH, BEARISH, NEUTRAL}:
            return direction
        return NEUTRAL

    @staticmethod
    def _price(state_payload: dict[str, Any]) -> float | None:
        market = state_payload.get("market")
        if not isinstance(market, dict):
            return None
        return ScenarioSnapshotBuilder._safe_float(market.get("price"), fallback=None)

    @staticmethod
    def _state_tf_payload(state_payload: dict[str, Any], timeframe: str) -> dict[str, Any]:
        raw_timeframes = state_payload.get("timeframes")
        if not isinstance(raw_timeframes, dict):
            return {}
        tf_payload = raw_timeframes.get(timeframe)
        if not isinstance(tf_payload, dict):
            return {}
        return tf_payload

    @classmethod
    def _state_tf_elements(
        cls,
        state_payload: dict[str, Any],
        timeframe: str,
        element_type: str,
    ) -> list[dict[str, Any]]:
        tf_payload = cls._state_tf_payload(state_payload, timeframe)
        raw_elements = tf_payload.get("elements")
        if not isinstance(raw_elements, dict):
            return []

        key = element_type
        if key == "fractals":
            items = raw_elements.get("fractals")
            if not isinstance(items, list):
                items = raw_elements.get("fractal")
        else:
            items = raw_elements.get(key)

        if not isinstance(items, list):
            return []
        return [item for item in items if isinstance(item, dict)]

    @staticmethod
    def _safe_float(value: object, *, fallback: float | None = 0.0) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def _safe_status(value: object) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _scenario_sort_key(item: dict[str, Any]) -> tuple[str, str]:
        created = str(item.get("created_at_utc") or "")
        scenario_id = str(item.get("scenario_id") or "")
        return created, scenario_id

    @classmethod
    def _should_write(
        cls,
        existing_payload: dict[str, Any] | None,
        next_payload: dict[str, Any],
    ) -> bool:
        if not isinstance(existing_payload, dict):
            return True
        return cls._normalize_for_compare(existing_payload) != cls._normalize_for_compare(
            next_payload
        )

    @staticmethod
    def _normalize_for_compare(payload: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in payload.items():
            if key == "updated_at_utc":
                continue
            normalized[key] = value
        return normalized

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
    def _save_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.parent / f"{path.name}.tmp"
        with temp.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        temp.replace(path)
        logger.info("Scenario snapshot updated: %s", path)

