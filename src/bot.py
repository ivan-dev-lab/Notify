# проблема: не всегда элементы есть в STATE
# в целом норм, но следить за перекрытием SNR
# использовать более актуальные опорные области - которые ближе всего к цене - сортировка

from __future__ import annotations

import argparse
import asyncio
import contextlib
import html
import json
import logging
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
import yaml

from app_logging import configure_logging
from auto_eye.exporters import ensure_exchange_structure
from config_loader import AppConfig, load_config
from main import QuotesMap, collect_quotes, resolve_output_path, save_quotes

CALLBACK_REFRESH = "refresh"
CALLBACK_MENU_ALERTS = "menu_alerts"
CALLBACK_MENU_HOME = "menu_home"
CALLBACK_MENU_DELETE = "menu_delete"
CALLBACK_MENU_BACKTEST = "menu_backtest"
CALLBACK_CANCEL = "cancel"
CALLBACK_NOOP = "noop"
CALLBACK_BACKTEST_CANCEL = "bt_cancel"

CALLBACK_ALERT_ASSET_PREFIX = "alerts_asset|"
CALLBACK_PRICE_SET_PREFIX = "price_set|"
CALLBACK_TIME_QUICK_PREFIX = "time_q|"
CALLBACK_TIME_CUSTOM_PREFIX = "time_c|"
CALLBACK_DELETE_ASSET_PREFIX = "del_asset|"
CALLBACK_DELETE_ONE_HOME_PREFIX = "del_h|"
CALLBACK_DELETE_ONE_ASSET_PREFIX = "del_a|"
CALLBACK_DELETE_APPLY_ASSET_PREFIX = "del_apply|"
CALLBACK_PRICE_CROSS_MENU_PREFIX = "pc_menu|"
CALLBACK_PRICE_TIME_MENU_PREFIX = "pt_menu|"
CALLBACK_PRICE_TIME_CANDLE_MENU_PREFIX = "pt_candle|"
CALLBACK_PRICE_TIME_DIR_PREFIX = "pt_dir|"
CALLBACK_PRICE_TIME_TF_PREFIX = "pt_tf|"
CALLBACK_TIME_CANDLE_MENU_PREFIX = "tcm|"
CALLBACK_BACK_ASSET_PREFIX = "back_asset|"
CALLBACK_ALERT_DELETE_MESSAGE = "alert_delete_msg"
CALLBACK_EDIT_ALERT_MENU_PREFIX = "edit_menu|"
CALLBACK_EDIT_ALERT_PICK_PREFIX = "edit_pick|"
CALLBACK_EDIT_TYPE_PREFIX = "edit_type|"
CALLBACK_EDIT_KEEP_PREFIX = "edit_keep|"
CALLBACK_EDIT_CHANGE_PREFIX = "edit_change|"
CALLBACK_EDIT_SET_DIR_PREFIX = "edit_set_dir|"
CALLBACK_EDIT_SET_TF_PREFIX = "edit_set_tf|"
CALLBACK_EDIT_BACK = "edit_back"
CALLBACK_BACKTEST_ASSET_PREFIX = "bt_asset|"
CALLBACK_BACKTEST_RANGE_PREFIX = "bt_range|"
CALLBACK_BACKTEST_CUSTOM_PREFIX = "bt_custom|"
CALLBACK_BACKTEST_BACK_PREFIX = "bt_back|"

ALERT_KIND_PRICE = "price"
ALERT_KIND_TIME = "time"
ALERT_KIND_PRICE_TIME = "price_time"

EDIT_TYPE_PRICE_CROSS = "price_cross"
EDIT_TYPE_PRICE_HOLD = "price_hold"
EDIT_TYPE_PRICE_CANDLE = "price_candle"
EDIT_TYPE_TIME_CANDLE = "time_candle"
EDIT_TYPE_TIME_CUSTOM = "time_custom"

DIRECTION_ABOVE = "above"
DIRECTION_BELOW = "below"
PRICE_TIME_MODE_HOLD = "hold"
PRICE_TIME_MODE_CANDLE_CLOSE = "candle_close"
CROSS_TOP_DOWN = "cross_top_down"
CROSS_BOTTOM_UP = "cross_bottom_up"

TIMEFRAME_M15 = "m15"
TIMEFRAME_H1 = "h1"
TIMEFRAME_H4 = "h4"
TIMEFRAME_D1 = "d1"
TIMEFRAME_W1 = "w1"
# "m1" here means monthly timeframe label M1 in TradingView context.
TIMEFRAME_M1 = "m1"

HOLD_TIMEFRAME_MINUTES = {
    TIMEFRAME_M15: 15,
    TIMEFRAME_H1: 60,
    TIMEFRAME_H4: 240,
}

USER_TIMEZONE = timezone(timedelta(hours=5))
USER_TIMEZONE_LABEL = "GMT+5"
MAX_ALERT_MESSAGE_LENGTH = 300

HHMM_PATTERN = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*$")
FULL_DATETIME_PATTERN = re.compile(
    r"^\s*(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})\s*$"
)
DMY_DATETIME_PATTERN = re.compile(
    r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})\s*$"
)
BACKTEST_INTERVAL_PATTERN = re.compile(r"^\s*(.+?)\s+[-–—]\s+(.+?)\s*$")

PREFERRED_GROUP_ORDER = [
    "INDICES",
    "GBP/N*",
    "USD/N*",
    "EUR/N*",
    "AUD/N*",
    "NZD/N*",
]

TIMEFRAME_RULES_PATH = Path("config/timeframe_rules.yaml")

AUTO_EYE_ELEMENT_KEY_MAP = {
    "fvg": "fvg",
    "snr": "snr",
    "fractal": "fractals",
    "fractals": "fractals",
    "rb": "rb",
}
AUTO_EYE_ELEMENT_LABELS = {
    "fvg": "FVG",
    "snr": "SNR",
    "fractals": "Fractal",
    "rb": "RB",
}
AUTO_EYE_FORMATION_TIME_KEYS = (
    "formation_time_utc",
    "formation_time",
    "break_time",
    "confirm_time",
    "c3_time_utc",
    "c3_time",
)
AUTO_EYE_H1_TIMEFRAME = "H1"
AUTO_EYE_VALID_STATUSES = {
    "fvg": {"active", "touched", "mitigated_partial"},
    "snr": {"active", "retested"},
    "rb": {"active"},
}
AUTO_EYE_MONITORED_H1_ELEMENTS = ("fvg", "snr", "rb")
AUTO_EYE_NEAR_PRICE_RATIO = 0.0008
AUTO_EYE_NEAR_ZONE_FACTOR = 0.5
AUTO_EYE_NEAR_MIN_DISTANCE = 1e-6

AUTO_EYE_INDEX_SPECS: dict[str, tuple[str, float]] = {
    "GER40": ("EUR", 25.0),
    "SPX500": ("USD", 50.0),
    "NAS100": ("USD", 50.0),
    "NDX100": ("USD", 50.0),
}
AUTO_EYE_CONTRACT_SIZES: dict[str, float] = {
    "XAUUSD": 100.0,
    "XAGUSD": 5000.0,
    "BTCUSDT": 1.0,
    "ETHUSDT": 1.0,
}
AUTO_EYE_DEFAULT_CONTRACT_SIZE = 100000.0

logger = logging.getLogger(__name__)


@dataclass
class AlertRule:
    user_id: int
    asset: str
    kind: str
    created_at_utc: str
    direction: str | None = None
    target: float | None = None
    trigger_at_utc: str | None = None
    delay_minutes: int | None = None
    price_time_mode: str | None = None
    timeframe_code: str | None = None
    condition_started_at_utc: str | None = None
    message_text: str | None = None


@dataclass
class TriggeredAlert:
    alert: AlertRule
    current_value_text: str


@dataclass
class TimeframeRules:
    h4_start_minutes_by_group: dict[str, int]
    indices_symbols: set[str]
    crypto_prefixes: list[str]
    default_group: str


@dataclass
class AssetDeleteSelectionState:
    asset: str
    selected_selectors: set[str]


@dataclass
class AutoEyeElementEvent:
    dedupe_key: str
    symbol: str
    timeframe: str
    element_key: str
    element_id: str
    direction: str
    status: str
    formation_time_utc: str | None
    zone_low: float | None
    zone_high: float | None
    price: float
    location: str
    distance_to_zone: float
    trend_direction: str
    recommendation: str
    trade_direction: str
    entry_price: float
    sl_price: float
    tp_price: float | None
    rr_ratio: float | None
    profit_quote_per_lot: float | None
    loss_quote_per_lot: float
    quote_currency: str | None
    tp_target_type: str | None
    tp_target_id: str | None


@dataclass
class BotState:
    config: AppConfig
    alert_store: "AlertStore"
    timeframe_rules: TimeframeRules
    scrape_lock: asyncio.Lock
    pending_inputs: dict[int, dict[str, object]]
    asset_delete_selection: dict[int, AssetDeleteSelectionState]
    alert_edit_sessions: dict[int, dict[str, object]]
    last_quotes: QuotesMap
    dashboard_message_ids: dict[int, tuple[int, int]]
    auto_eye_state_dir: Path
    auto_eye_seen_store: "AutoEyeSeenStore"
    backtest_tasks: dict[int, asyncio.Task]
    periodic_task: asyncio.Task | None = None


class AutoEyeSeenStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.initialized = False
        self.seen_ids: set[str] = set()
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            logger.info("Auto-eye notify store not found, starting empty: %s", self.path)
            return

        with self.path.open("r", encoding="utf-8") as file:
            raw = json.load(file)

        raw_active = raw.get("active_keys")
        if isinstance(raw_active, list):
            self.initialized = bool(raw.get("initialized", False))
            self.seen_ids = {
                str(item).strip() for item in raw_active if str(item).strip()
            }
        else:
            # Legacy format (seen_ids for "new elements") is not compatible with
            # proximity alerts; bootstrap fresh active snapshot.
            self.initialized = False
            self.seen_ids = set()

        logger.info(
            "Loaded auto-eye notify store: initialized=%s active=%s path=%s",
            self.initialized,
            len(self.seen_ids),
            self.path,
        )

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "initialized": self.initialized,
            "active_keys": sorted(self.seen_ids),
            "seen_ids": sorted(self.seen_ids),
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        with self.path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

    def register_snapshot(self, current_keys: set[str]) -> set[str]:
        normalized = {key.strip() for key in current_keys if key.strip()}

        if not self.initialized:
            self.initialized = True
            self.seen_ids = set(normalized)
            self.save()
            logger.info(
                "Auto-eye notify store initialized with %s active keys", len(self.seen_ids)
            )
            return set(normalized)

        new_keys = normalized - self.seen_ids
        if normalized != self.seen_ids:
            self.seen_ids = set(normalized)
            self.save()

        return new_keys


class AlertStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.alerts: list[AlertRule] = []
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            self.alerts = []
            logger.info("Alerts store not found, starting empty: %s", self.path)
            return

        with self.path.open("r", encoding="utf-8") as file:
            raw = json.load(file)

        loaded: list[AlertRule] = []
        for item in raw.get("alerts", []):
            parsed = self._parse_alert(item)
            if parsed is None:
                logger.warning("Skipping invalid alert entry: %s", item)
                continue
            loaded.append(parsed)

        self.alerts = loaded
        logger.info("Loaded %s alerts from %s", len(self.alerts), self.path)

    def _parse_alert(self, item: dict[str, object]) -> AlertRule | None:
        try:
            raw_user_id = item.get("user_id", item.get("chat_id"))
            user_id = int(raw_user_id)
            asset = str(item["asset"])
            created_at_utc = str(item.get("created_at_utc", ""))
        except (KeyError, TypeError, ValueError):
            return None

        raw_message = item.get("message_text", item.get("message"))
        message_text = normalize_alert_message_value(raw_message)

        if user_id <= 0:
            return None

        kind = str(item.get("kind", "")).strip().lower()

        if kind == ALERT_KIND_PRICE:
            direction = str(item.get("direction", "")).strip().lower()
            target_raw = item.get("target")
            if direction not in {
                DIRECTION_ABOVE,
                DIRECTION_BELOW,
                CROSS_TOP_DOWN,
                CROSS_BOTTOM_UP,
            }:
                return None
            try:
                target = float(target_raw)
            except (TypeError, ValueError):
                return None

            return AlertRule(
                user_id=user_id,
                asset=asset,
                kind=ALERT_KIND_PRICE,
                direction=direction,
                target=target,
                created_at_utc=created_at_utc,
                message_text=message_text,
            )

        if kind == ALERT_KIND_TIME:
            trigger_at_utc = str(item.get("trigger_at_utc", "")).strip()
            delay_raw = item.get("delay_minutes", 0)
            try:
                delay_minutes = int(delay_raw)
            except (TypeError, ValueError):
                return None

            if not trigger_at_utc or delay_minutes <= 0:
                return None

            return AlertRule(
                user_id=user_id,
                asset=asset,
                kind=ALERT_KIND_TIME,
                trigger_at_utc=trigger_at_utc,
                delay_minutes=delay_minutes,
                created_at_utc=created_at_utc,
                message_text=message_text,
            )

        if kind == ALERT_KIND_PRICE_TIME:
            direction = str(item.get("direction", "")).strip().lower()
            target_raw = item.get("target")
            mode = str(item.get("price_time_mode", "")).strip().lower()
            timeframe_code = str(item.get("timeframe_code", "")).strip().lower()
            trigger_at_utc = str(item.get("trigger_at_utc", "")).strip() or None
            condition_started_at_utc = (
                str(item.get("condition_started_at_utc", "")).strip() or None
            )

            if direction not in {
                DIRECTION_ABOVE,
                DIRECTION_BELOW,
                CROSS_TOP_DOWN,
                CROSS_BOTTOM_UP,
            }:
                return None
            try:
                target = float(target_raw)
            except (TypeError, ValueError):
                return None

            if mode == PRICE_TIME_MODE_HOLD:
                delay_raw = item.get("delay_minutes", 0)
                try:
                    delay_minutes = int(delay_raw)
                except (TypeError, ValueError):
                    return None

                if delay_minutes <= 0 or not is_supported_hold_timeframe(timeframe_code):
                    return None

                return AlertRule(
                    user_id=user_id,
                    asset=asset,
                    kind=ALERT_KIND_PRICE_TIME,
                    direction=direction,
                    target=target,
                    delay_minutes=delay_minutes,
                    price_time_mode=PRICE_TIME_MODE_HOLD,
                    timeframe_code=timeframe_code,
                    condition_started_at_utc=condition_started_at_utc,
                    created_at_utc=created_at_utc,
                    message_text=message_text,
                )

            if mode == PRICE_TIME_MODE_CANDLE_CLOSE:
                if not is_supported_candle_timeframe(timeframe_code):
                    return None

                return AlertRule(
                    user_id=user_id,
                    asset=asset,
                    kind=ALERT_KIND_PRICE_TIME,
                    direction=direction,
                    target=target,
                    trigger_at_utc=trigger_at_utc,
                    price_time_mode=PRICE_TIME_MODE_CANDLE_CLOSE,
                    timeframe_code=timeframe_code,
                    created_at_utc=created_at_utc,
                    message_text=message_text,
                )

            return None

        # Backward compatibility with old price schema (no kind field).
        direction = str(item.get("direction", "")).strip().lower()
        target_raw = item.get("target")
        if direction in {
            DIRECTION_ABOVE,
            DIRECTION_BELOW,
            CROSS_TOP_DOWN,
            CROSS_BOTTOM_UP,
        } and target_raw is not None:
            try:
                target = float(target_raw)
            except (TypeError, ValueError):
                return None

            return AlertRule(
                user_id=user_id,
                asset=asset,
                kind=ALERT_KIND_PRICE,
                direction=direction,
                target=target,
                created_at_utc=created_at_utc,
                message_text=message_text,
            )

        return None

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "alerts": [asdict(alert) for alert in self.alerts],
        }
        with self.path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        logger.info("Saved %s alerts to %s", len(self.alerts), self.path)

    def list_for_user(self, user_id: int) -> list[AlertRule]:
        return [alert for alert in self.alerts if alert.user_id == user_id]

    def list_for_user_asset(self, user_id: int, asset: str) -> list[AlertRule]:
        return [
            alert
            for alert in self.alerts
            if alert.user_id == user_id and alert.asset == asset
        ]

    def upsert_price(
        self,
        user_id: int,
        asset: str,
        direction: str,
        target: float,
        *,
        message_text: str | None = None,
    ) -> None:
        normalized_message = normalize_alert_message_value(message_text)
        self.alerts = [
            alert
            for alert in self.alerts
            if not (
                alert.user_id == user_id
                and alert.asset == asset
                and alert.kind == ALERT_KIND_PRICE
                and alert.direction == direction
                and float(alert.target or 0.0) == float(target)
            )
        ]

        self.alerts.append(
            AlertRule(
                user_id=user_id,
                asset=asset,
                kind=ALERT_KIND_PRICE,
                direction=direction,
                target=target,
                created_at_utc=datetime.now(timezone.utc).isoformat(),
                message_text=normalized_message,
            )
        )

        logger.info(
            "Upsert price alert user_id=%s asset=%s direction=%s target=%s",
            user_id,
            asset,
            direction,
            target,
        )
        self.save()

    def add_time(
        self,
        user_id: int,
        asset: str,
        trigger_at_utc: datetime,
        delay_minutes: int,
        *,
        message_text: str | None = None,
    ) -> None:
        normalized_message = normalize_alert_message_value(message_text)
        trigger_iso = trigger_at_utc.astimezone(timezone.utc).isoformat()

        self.alerts = [
            alert
            for alert in self.alerts
            if not (
                alert.user_id == user_id
                and alert.asset == asset
                and alert.kind == ALERT_KIND_TIME
                and alert.trigger_at_utc == trigger_iso
            )
        ]

        self.alerts.append(
            AlertRule(
                user_id=user_id,
                asset=asset,
                kind=ALERT_KIND_TIME,
                trigger_at_utc=trigger_iso,
                delay_minutes=max(1, int(delay_minutes)),
                created_at_utc=datetime.now(timezone.utc).isoformat(),
                message_text=normalized_message,
            )
        )

        logger.info(
            "Add time alert user_id=%s asset=%s trigger_at_utc=%s delay_minutes=%s",
            user_id,
            asset,
            trigger_iso,
            delay_minutes,
        )
        self.save()

    def add_price_time(
        self,
        user_id: int,
        asset: str,
        direction: str,
        target: float,
        mode: str,
        timeframe_code: str,
        *,
        delay_minutes: int | None = None,
        trigger_at_utc: datetime | None = None,
        message_text: str | None = None,
    ) -> None:
        normalized_mode = mode.strip().lower()
        normalized_timeframe = timeframe_code.strip().lower()
        normalized_message = normalize_alert_message_value(message_text)

        existing_filtered: list[AlertRule] = []
        for alert in self.alerts:
            same_rule = (
                alert.user_id == user_id
                and alert.asset == asset
                and alert.kind == ALERT_KIND_PRICE_TIME
                and alert.direction == direction
                and float(alert.target or 0.0) == float(target)
                and alert.price_time_mode == normalized_mode
                and (alert.timeframe_code or "").lower() == normalized_timeframe
            )
            if same_rule:
                continue
            existing_filtered.append(alert)
        self.alerts = existing_filtered

        trigger_iso: str | None = None
        if trigger_at_utc is not None:
            trigger_iso = trigger_at_utc.astimezone(timezone.utc).isoformat()

        self.alerts.append(
            AlertRule(
                user_id=user_id,
                asset=asset,
                kind=ALERT_KIND_PRICE_TIME,
                direction=direction,
                target=target,
                delay_minutes=delay_minutes,
                trigger_at_utc=trigger_iso,
                price_time_mode=normalized_mode,
                timeframe_code=normalized_timeframe,
                condition_started_at_utc=None,
                created_at_utc=datetime.now(timezone.utc).isoformat(),
                message_text=normalized_message,
            )
        )

        logger.info(
            "Add price-time alert user_id=%s asset=%s direction=%s target=%s mode=%s timeframe=%s delay_minutes=%s trigger_at_utc=%s",
            user_id,
            asset,
            direction,
            target,
            normalized_mode,
            normalized_timeframe,
            delay_minutes,
            trigger_iso,
        )
        self.save()

    def remove_asset_alerts(self, user_id: int, asset: str) -> int:
        before = len(self.alerts)
        self.alerts = [
            alert
            for alert in self.alerts
            if not (alert.user_id == user_id and alert.asset == asset)
        ]

        removed = before - len(self.alerts)
        if removed:
            logger.info("Removed %s alerts for user_id=%s asset=%s", removed, user_id, asset)
            self.save()
        else:
            logger.info("No alerts to remove for user_id=%s asset=%s", user_id, asset)
        return removed

    def remove_one(
        self, user_id: int, asset: str, kind: str, created_at_utc: str
    ) -> bool:
        before = len(self.alerts)
        self.alerts = [
            alert
            for alert in self.alerts
            if not (
                alert.user_id == user_id
                and alert.asset == asset
                and alert.kind == kind
                and alert.created_at_utc == created_at_utc
            )
        ]

        removed = before - len(self.alerts)
        if removed > 0:
            logger.info(
                "Removed %s alerts for user_id=%s asset=%s kind=%s created_at_utc=%s",
                removed,
                user_id,
                asset,
                kind,
                created_at_utc,
            )
            self.save()
            return True

        logger.info(
            "No alert found to remove for user_id=%s asset=%s kind=%s created_at_utc=%s",
            user_id,
            asset,
            kind,
            created_at_utc,
        )
        return False

    def consume_triggered(
        self, quotes: QuotesMap, previous_quotes: QuotesMap | None = None
    ) -> list[TriggeredAlert]:
        now_utc = datetime.now(timezone.utc)
        triggered: list[TriggeredAlert] = []
        active: list[AlertRule] = []
        has_state_changes = False
        prev_quotes = previous_quotes or {}

        for alert in self.alerts:
            if alert.kind == ALERT_KIND_PRICE:
                record = quotes.get(alert.asset, {})
                current_text = str(record.get("value") or "").strip()
                current_value = parse_price(current_text)
                previous_text = str(prev_quotes.get(alert.asset, {}).get("value") or "").strip()
                previous_value = parse_price(previous_text)

                if current_value is None or alert.target is None or alert.direction is None:
                    active.append(alert)
                    continue

                if alert.direction in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}:
                    if (
                        previous_value is not None
                        and is_cross_triggered(
                            previous_value,
                            current_value,
                            alert.direction,
                            alert.target,
                        )
                    ):
                        triggered.append(
                            TriggeredAlert(alert=alert, current_value_text=current_text)
                        )
                        continue
                elif compare_by_direction(current_value, alert.direction, alert.target):
                    triggered.append(
                        TriggeredAlert(alert=alert, current_value_text=current_text)
                    )
                    continue

                active.append(alert)
                continue

            if alert.kind == ALERT_KIND_TIME:
                if not alert.trigger_at_utc:
                    continue

                try:
                    trigger_at = datetime.fromisoformat(alert.trigger_at_utc)
                except ValueError:
                    logger.warning("Invalid trigger_at_utc in alert: %s", alert)
                    continue

                if trigger_at.tzinfo is None:
                    trigger_at = trigger_at.replace(tzinfo=timezone.utc)
                else:
                    trigger_at = trigger_at.astimezone(timezone.utc)

                if now_utc >= trigger_at:
                    current_text = str(
                        quotes.get(alert.asset, {}).get("value") or "n/a"
                    ).strip()
                    triggered.append(
                        TriggeredAlert(alert=alert, current_value_text=current_text)
                    )
                    continue

                active.append(alert)
                continue

            if alert.kind == ALERT_KIND_PRICE_TIME:
                record = quotes.get(alert.asset, {})
                current_text = str(record.get("value") or "").strip()
                current_value = parse_price(current_text)
                previous_text = str(prev_quotes.get(alert.asset, {}).get("value") or "").strip()
                previous_value = parse_price(previous_text)

                if (
                    current_value is None
                    or alert.target is None
                    or alert.direction
                    not in {
                        DIRECTION_ABOVE,
                        DIRECTION_BELOW,
                        CROSS_TOP_DOWN,
                        CROSS_BOTTOM_UP,
                    }
                    or not alert.price_time_mode
                    or not alert.timeframe_code
                ):
                    active.append(alert)
                    continue

                condition_met = compare_by_direction(current_value, alert.direction, alert.target)
                cross_met = (
                    previous_value is not None
                    and is_cross_triggered(
                        previous_value,
                        current_value,
                        alert.direction,
                        alert.target,
                    )
                )

                if alert.price_time_mode == PRICE_TIME_MODE_HOLD:
                    hold_minutes = max(1, int(alert.delay_minutes or 0))

                    is_price_condition_met = (
                        cross_met
                        if alert.direction in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}
                        else condition_met
                    )

                    if is_price_condition_met:
                        started_at = parse_utc_iso(alert.condition_started_at_utc or "")
                        if started_at is None:
                            alert.condition_started_at_utc = now_utc.isoformat()
                            has_state_changes = True
                            active.append(alert)
                            continue

                        if now_utc >= started_at + timedelta(minutes=hold_minutes):
                            triggered.append(
                                TriggeredAlert(alert=alert, current_value_text=current_text)
                            )
                            continue

                        active.append(alert)
                        continue

                    if alert.condition_started_at_utc:
                        alert.condition_started_at_utc = None
                        has_state_changes = True
                    active.append(alert)
                    continue

                if alert.price_time_mode == PRICE_TIME_MODE_CANDLE_CLOSE:
                    trigger_at = parse_utc_iso(alert.trigger_at_utc or "")
                    if trigger_at is None:
                        logger.warning("Invalid trigger_at_utc in price_time alert: %s", alert)
                        active.append(alert)
                        continue

                    if now_utc >= trigger_at:
                        is_price_condition_met = compare_candle_close_condition(
                            current_value,
                            alert.direction,
                            alert.target,
                        )
                        if is_price_condition_met:
                            triggered.append(
                                TriggeredAlert(alert=alert, current_value_text=current_text)
                            )
                            continue

                        next_trigger = advance_candle_close_utc(
                            trigger_at,
                            alert.timeframe_code,
                        )
                        while next_trigger is not None and next_trigger <= now_utc:
                            next_trigger = advance_candle_close_utc(
                                next_trigger,
                                alert.timeframe_code,
                            )
                        if next_trigger is None:
                            active.append(alert)
                            continue

                        logger.info(
                            "Price-time candle check not met user_id=%s asset=%s tf=%s direction=%s target=%s current=%s trigger_at_utc=%s -> next_trigger_utc=%s",
                            alert.user_id,
                            alert.asset,
                            alert.timeframe_code,
                            alert.direction,
                            alert.target,
                            current_text,
                            trigger_at.isoformat(),
                            next_trigger.isoformat(),
                        )
                        alert.trigger_at_utc = next_trigger.isoformat()
                        has_state_changes = True
                        active.append(alert)
                        continue

                    active.append(alert)
                    continue

                active.append(alert)
                continue

            active.append(alert)

        if len(active) != len(self.alerts) or has_state_changes:
            self.alerts = active
            self.save()

        if triggered:
            logger.info("Triggered %s alerts", len(triggered))
        return triggered


def normalize_auto_eye_timeframe(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized == "M1":
        return "MN1"
    return normalized


def normalize_auto_eye_element_key(value: str) -> str | None:
    normalized = str(value).strip().lower()
    return AUTO_EYE_ELEMENT_KEY_MAP.get(normalized)


def resolve_auto_eye_state_dir(config: AppConfig) -> Path:
    custom_state_dir = str(config.telegram.auto_eye_notifications.state_dir).strip()
    if custom_state_dir:
        path = Path(custom_state_dir)
        if path.is_absolute():
            return path
        return Path.cwd() / path

    auto_eye_output_path = resolve_output_path(config.auto_eye.output_json)
    return ensure_exchange_structure(auto_eye_output_path)["state"]


def parse_auto_eye_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        normalized = value.strip().replace(",", ".")
        if not normalized:
            return None
        try:
            return float(normalized)
        except ValueError:
            return None

    return None


def parse_auto_eye_zone(
    raw_item: dict[str, object],
    element_key: str,
) -> tuple[float | None, float | None]:
    if element_key == "fvg":
        return (
            parse_auto_eye_float(raw_item.get("fvg_low")),
            parse_auto_eye_float(raw_item.get("fvg_high")),
        )

    if element_key == "snr":
        return (
            parse_auto_eye_float(raw_item.get("snr_low")),
            parse_auto_eye_float(raw_item.get("snr_high")),
        )

    if element_key == "rb":
        return (
            parse_auto_eye_float(raw_item.get("rb_low")),
            parse_auto_eye_float(raw_item.get("rb_high")),
        )

    if element_key == "fractals":
        extreme = parse_auto_eye_float(raw_item.get("extreme_price"))
        return extreme, extreme

    return (
        parse_auto_eye_float(raw_item.get("zone_low")),
        parse_auto_eye_float(raw_item.get("zone_high")),
    )


def parse_auto_eye_signal_time(raw_item: dict[str, object]) -> str | None:
    for key in AUTO_EYE_FORMATION_TIME_KEYS:
        value = str(raw_item.get(key) or "").strip()
        if value:
            return value
    return None


def parse_auto_eye_direction(
    *,
    element_key: str,
    raw_item: dict[str, object],
) -> str | None:
    if element_key == "fvg":
        direction = str(raw_item.get("direction") or "").strip().lower()
        if direction in {"bullish", "bearish"}:
            return direction
        return None

    if element_key == "snr":
        role = str(raw_item.get("role") or "").strip().lower()
        break_type = str(raw_item.get("break_type") or "").strip().lower()
        if break_type == "break_up_close":
            return "bullish"
        if break_type == "break_down_close":
            return "bearish"
        if role == "support":
            return "bullish"
        if role == "resistance":
            return "bearish"
        return None

    if element_key == "rb":
        rb_type = str(raw_item.get("rb_type") or raw_item.get("direction") or "").strip().lower()
        if rb_type == "low":
            return "bullish"
        if rb_type == "high":
            return "bearish"
        if rb_type in {"bullish", "bearish"}:
            return rb_type
        return None

    return None


def auto_eye_is_valid_h1_status(*, element_key: str, status: str) -> bool:
    allowed = AUTO_EYE_VALID_STATUSES.get(element_key)
    if allowed is None:
        return False
    return status in allowed


def auto_eye_symbol_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def resolve_quote_price_for_symbol(quotes: QuotesMap, symbol: str) -> float | None:
    symbol_key = auto_eye_symbol_key(symbol)
    for asset, quote in quotes.items():
        if not isinstance(quote, dict):
            continue
        asset_key = auto_eye_symbol_key(asset)
        pair_key = auto_eye_symbol_key(str(quote.get("pair") or ""))
        if symbol_key not in {asset_key, pair_key}:
            continue
        price = parse_auto_eye_float(quote.get("value"))
        if price is not None:
            return price
    return None


def resolve_symbol_price(
    *,
    state: BotState,
    symbol: str,
    raw_payload: dict[str, object],
) -> float | None:
    quote_price = resolve_quote_price_for_symbol(state.last_quotes, symbol)
    if quote_price is not None:
        return quote_price

    market = raw_payload.get("market")
    if isinstance(market, dict):
        return parse_auto_eye_float(market.get("price"))
    return None


def resolve_trend_direction(*, trend_dir: Path, symbol: str) -> str:
    path = trend_dir / f"{symbol}.json"
    if not path.exists():
        return "neutral"
    try:
        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except Exception:
        return "neutral"
    if not isinstance(payload, dict):
        return "neutral"
    trend = payload.get("trend")
    if not isinstance(trend, dict):
        return "neutral"
    direction = str(trend.get("direction") or "").strip().lower()
    if direction in {"bullish", "bearish", "neutral"}:
        return direction
    return "neutral"


def auto_eye_near_threshold(*, price: float, zone_low: float, zone_high: float) -> float:
    zone_size = max(0.0, abs(zone_high - zone_low))
    by_zone = zone_size * AUTO_EYE_NEAR_ZONE_FACTOR
    by_price = abs(price) * AUTO_EYE_NEAR_PRICE_RATIO
    return max(AUTO_EYE_NEAR_MIN_DISTANCE, by_zone, by_price)


def classify_price_location(
    *,
    price: float,
    zone_low: float,
    zone_high: float,
) -> tuple[str | None, float]:
    low = min(zone_low, zone_high)
    high = max(zone_low, zone_high)
    if low <= price <= high:
        return "inside", 0.0

    threshold = auto_eye_near_threshold(price=price, zone_low=low, zone_high=high)
    if price < low:
        distance = low - price
        if distance <= threshold:
            return "near_below", distance
        return None, distance

    distance = price - high
    if distance <= threshold:
        return "near_above", distance
    return None, distance


def parse_auto_eye_pair(symbol: str) -> tuple[str, str | None]:
    normalized = auto_eye_symbol_key(symbol)
    if normalized in AUTO_EYE_INDEX_SPECS:
        quote, _ = AUTO_EYE_INDEX_SPECS[normalized]
        return normalized, quote
    if len(normalized) == 6 and normalized.isalpha():
        return normalized[:3], normalized[3:]
    if normalized.endswith("USDT"):
        return normalized[:-4], "USDT"
    if normalized.endswith("USD"):
        return normalized[:-3], "USD"
    return normalized, None


def get_auto_eye_contract_size(symbol: str) -> float:
    normalized = auto_eye_symbol_key(symbol)
    if normalized in AUTO_EYE_INDEX_SPECS:
        return AUTO_EYE_INDEX_SPECS[normalized][1]
    return AUTO_EYE_CONTRACT_SIZES.get(normalized, AUTO_EYE_DEFAULT_CONTRACT_SIZE)


def choose_nearest_h1_tp(
    *,
    entry_price: float,
    trade_direction: str,
    current_element_id: str,
    targets: list[dict[str, object]],
) -> tuple[float | None, str | None, str | None]:
    best_level: float | None = None
    best_type: str | None = None
    best_id: str | None = None
    best_distance: float | None = None

    for target in targets:
        target_id = str(target.get("id") or "").strip()
        if not target_id or target_id == current_element_id:
            continue

        zone_low = parse_auto_eye_float(target.get("zone_low"))
        zone_high = parse_auto_eye_float(target.get("zone_high"))
        if zone_low is None or zone_high is None:
            continue
        low = min(zone_low, zone_high)
        high = max(zone_low, zone_high)

        if trade_direction == "long":
            level = low
            if level <= entry_price:
                continue
            distance = level - entry_price
        else:
            level = high
            if level >= entry_price:
                continue
            distance = entry_price - level

        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_level = level
            best_type = str(target.get("element_key") or "").strip().lower()
            best_id = target_id

    return best_level, best_type, best_id


def build_auto_eye_trade_plan(
    *,
    symbol: str,
    entry_price: float,
    zone_low: float,
    zone_high: float,
    direction: str,
    current_element_id: str,
    tp_targets: list[dict[str, object]],
) -> dict[str, object]:
    trade_direction = "long" if direction == "bullish" else "short"
    low = min(zone_low, zone_high)
    high = max(zone_low, zone_high)
    sl_price = low if trade_direction == "long" else high
    tp_price, tp_target_type, tp_target_id = choose_nearest_h1_tp(
        entry_price=entry_price,
        trade_direction=trade_direction,
        current_element_id=current_element_id,
        targets=tp_targets,
    )

    risk_abs = abs(entry_price - sl_price)
    reward_abs = abs(tp_price - entry_price) if tp_price is not None else None
    rr_ratio: float | None = None
    if reward_abs is not None and risk_abs > 0:
        rr_ratio = reward_abs / risk_abs

    contract_size = get_auto_eye_contract_size(symbol)
    _, quote_currency = parse_auto_eye_pair(symbol)

    if trade_direction == "long":
        loss_quote = max(0.0, (entry_price - sl_price) * contract_size)
        profit_quote = (
            max(0.0, (tp_price - entry_price) * contract_size)
            if tp_price is not None
            else None
        )
    else:
        loss_quote = max(0.0, (sl_price - entry_price) * contract_size)
        profit_quote = (
            max(0.0, (entry_price - tp_price) * contract_size)
            if tp_price is not None
            else None
        )

    return {
        "trade_direction": trade_direction,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "rr_ratio": rr_ratio,
        "profit_quote_per_lot": profit_quote,
        "loss_quote_per_lot": loss_quote,
        "quote_currency": quote_currency,
        "tp_target_type": tp_target_type,
        "tp_target_id": tp_target_id,
    }


def build_auto_eye_recommendation(*, trend_direction: str, zone_direction: str) -> str:
    if trend_direction not in {"bullish", "bearish"}:
        return (
            "Тренд H1 нейтрален: ждите M5 реакцию; "
            "для разворота дождитесь появления новой H1 опорной области."
        )
    if trend_direction == zone_direction:
        return "Сделка по тренду: дождитесь M5 реакции в текущей H1 зоне."
    return (
        "Контртренд/разворот: дождитесь подтверждающей H1 опорной области "
        "и только затем ищите M5 реакцию."
    )


def collect_new_auto_eye_events(state: BotState) -> list[AutoEyeElementEvent]:
    config = state.config.telegram.auto_eye_notifications
    if not config.enabled:
        return []

    if not state.auto_eye_state_dir.exists():
        logger.debug("Auto-eye state dir is missing: %s", state.auto_eye_state_dir)
        return []

    trend_dir = state.auto_eye_state_dir.parent / "Trends"
    events_by_key: dict[str, AutoEyeElementEvent] = {}

    for state_file in sorted(state.auto_eye_state_dir.glob("*.json")):
        if state_file.name.lower() == "schema_version.json":
            continue

        try:
            with state_file.open("r", encoding="utf-8") as file:
                raw_payload = json.load(file)
        except Exception as error:
            logger.warning("Failed to read auto-eye state file %s: %s", state_file, error)
            continue

        if not isinstance(raw_payload, dict):
            continue

        symbol = str(raw_payload.get("symbol") or state_file.stem).strip().upper()
        price = resolve_symbol_price(state=state, symbol=symbol, raw_payload=raw_payload)
        if price is None or price <= 0:
            continue

        trend_direction = resolve_trend_direction(trend_dir=trend_dir, symbol=symbol)

        raw_timeframes = raw_payload.get("timeframes")
        if not isinstance(raw_timeframes, dict):
            continue
        raw_h1 = raw_timeframes.get(AUTO_EYE_H1_TIMEFRAME)
        if not isinstance(raw_h1, dict):
            continue
        raw_elements_block = raw_h1.get("elements")
        if not isinstance(raw_elements_block, dict):
            continue

        h1_zones: list[dict[str, object]] = []
        for element_key in AUTO_EYE_MONITORED_H1_ELEMENTS:
            raw_items = raw_elements_block.get(element_key)
            if not isinstance(raw_items, list):
                continue

            for raw_item in raw_items:
                if not isinstance(raw_item, dict):
                    continue
                element_id = str(raw_item.get("id") or "").strip()
                if not element_id:
                    continue
                status = str(raw_item.get("status") or "").strip().lower()
                if not auto_eye_is_valid_h1_status(element_key=element_key, status=status):
                    continue
                direction = parse_auto_eye_direction(element_key=element_key, raw_item=raw_item)
                if direction not in {"bullish", "bearish"}:
                    continue
                zone_low, zone_high = parse_auto_eye_zone(raw_item, element_key)
                if zone_low is None or zone_high is None:
                    continue
                signal_time_utc = parse_auto_eye_signal_time(raw_item)
                h1_zones.append(
                    {
                        "symbol": symbol,
                        "timeframe": AUTO_EYE_H1_TIMEFRAME,
                        "element_key": element_key,
                        "id": element_id,
                        "direction": direction,
                        "status": status,
                        "zone_low": min(zone_low, zone_high),
                        "zone_high": max(zone_low, zone_high),
                        "formation_time_utc": signal_time_utc,
                    }
                )

        if len(h1_zones) == 0:
            continue

        best_event: AutoEyeElementEvent | None = None
        for zone in h1_zones:
            zone_low = float(zone["zone_low"])
            zone_high = float(zone["zone_high"])
            location, distance = classify_price_location(
                price=price,
                zone_low=zone_low,
                zone_high=zone_high,
            )
            if location is None:
                continue

            recommendation = build_auto_eye_recommendation(
                trend_direction=trend_direction,
                zone_direction=str(zone["direction"]),
            )
            trade_plan = build_auto_eye_trade_plan(
                symbol=symbol,
                entry_price=price,
                zone_low=zone_low,
                zone_high=zone_high,
                direction=str(zone["direction"]),
                current_element_id=str(zone["id"]),
                tp_targets=h1_zones,
            )
            dedupe_key = (
                f"{symbol}|{AUTO_EYE_H1_TIMEFRAME}|{zone['element_key']}|{zone['id']}|{location}"
            )

            event = AutoEyeElementEvent(
                dedupe_key=dedupe_key,
                symbol=symbol,
                timeframe=AUTO_EYE_H1_TIMEFRAME,
                element_key=str(zone["element_key"]),
                element_id=str(zone["id"]),
                direction=str(zone["direction"]),
                status=str(zone["status"]),
                formation_time_utc=(
                    str(zone.get("formation_time_utc"))
                    if zone.get("formation_time_utc")
                    else None
                ),
                zone_low=zone_low,
                zone_high=zone_high,
                price=price,
                location=location,
                distance_to_zone=distance,
                trend_direction=trend_direction,
                recommendation=recommendation,
                trade_direction=str(trade_plan["trade_direction"]),
                entry_price=float(trade_plan["entry_price"]),
                sl_price=float(trade_plan["sl_price"]),
                tp_price=(
                    float(trade_plan["tp_price"])
                    if isinstance(trade_plan["tp_price"], (int, float))
                    else None
                ),
                rr_ratio=(
                    float(trade_plan["rr_ratio"])
                    if isinstance(trade_plan["rr_ratio"], (int, float))
                    else None
                ),
                profit_quote_per_lot=(
                    float(trade_plan["profit_quote_per_lot"])
                    if isinstance(trade_plan["profit_quote_per_lot"], (int, float))
                    else None
                ),
                loss_quote_per_lot=float(trade_plan["loss_quote_per_lot"]),
                quote_currency=(
                    str(trade_plan["quote_currency"])
                    if trade_plan.get("quote_currency")
                    else None
                ),
                tp_target_type=(
                    str(trade_plan["tp_target_type"])
                    if trade_plan.get("tp_target_type")
                    else None
                ),
                tp_target_id=(
                    str(trade_plan["tp_target_id"])
                    if trade_plan.get("tp_target_id")
                    else None
                ),
            )

            if best_event is None:
                best_event = event
                continue

            current_signal = parse_utc_iso(event.formation_time_utc or "")
            best_signal = parse_utc_iso(best_event.formation_time_utc or "")
            current_sort = (
                0 if event.location == "inside" else 1,
                event.distance_to_zone,
                -(current_signal.timestamp() if current_signal else 0.0),
                event.element_id,
            )
            best_sort = (
                0 if best_event.location == "inside" else 1,
                best_event.distance_to_zone,
                -(best_signal.timestamp() if best_signal else 0.0),
                best_event.element_id,
            )
            if current_sort < best_sort:
                best_event = event

        if best_event is not None:
            events_by_key[best_event.dedupe_key] = best_event

    if not events_by_key:
        return []

    new_keys = state.auto_eye_seen_store.register_snapshot(set(events_by_key.keys()))
    if not new_keys:
        return []

    new_events = [events_by_key[key] for key in new_keys if key in events_by_key]
    new_events.sort(
        key=lambda event: (
            event.symbol,
            event.location,
            event.distance_to_zone,
            event.element_key,
            event.element_id,
        )
    )
    return new_events


def auto_eye_direction_label(direction: str) -> str:
    mapping = {
        "bullish": "bullish",
        "bearish": "bearish",
        "support": "support",
        "resistance": "resistance",
    }
    return mapping.get(direction.lower(), direction)


def auto_eye_location_label(event: AutoEyeElementEvent) -> str:
    if event.location == "inside":
        return "цена внутри зоны"
    if event.location == "near_above":
        return f"цена выше зоны, рядом ({format_target(event.distance_to_zone)})"
    if event.location == "near_below":
        return f"цена ниже зоны, рядом ({format_target(event.distance_to_zone)})"
    return event.location


def render_auto_eye_event_text(event: AutoEyeElementEvent) -> str:
    element_label = AUTO_EYE_ELEMENT_LABELS.get(event.element_key, event.element_key.upper())
    title = (
        "<b>H1 опорная область: цена в зоне</b>"
        if event.location == "inside"
        else "<b>H1 опорная область: цена рядом</b>"
    )

    lines = [
        title,
        f"<b>Актив:</b> <code>{html.escape(event.symbol)}</code>",
        f"<b>Тренд H1:</b> <b>{html.escape(event.trend_direction)}</b>",
        f"<b>Текущая цена:</b> <b>{format_target(event.price)}</b>",
        f"<b>Местонахождение:</b> <b>{html.escape(auto_eye_location_label(event))}</b>",
        f"<b>H1 зона:</b> <b>{html.escape(element_label)}</b> / <b>{html.escape(auto_eye_direction_label(event.direction))}</b>",
    ]

    if event.zone_low is not None and event.zone_high is not None:
        lines.append(
            f"<b>Границы:</b> <b>{format_target(event.zone_low)} - {format_target(event.zone_high)}</b>"
        )

    if event.formation_time_utc:
        lines.append(
            "<b>Сигнал зоны:</b> "
            f"<b>{html.escape(format_local_datetime(event.formation_time_utc))}</b>"
        )

    lines.append(f"<b>Рекомендация:</b> {html.escape(event.recommendation)}")

    lines.append("")
    lines.append("<b>Расчёт сделки (черновик)</b>")
    lines.append(
        f"<b>Направление:</b> <b>{'BUY' if event.trade_direction == 'long' else 'SELL'}</b>"
    )
    lines.append(f"<b>Entry:</b> <b>{format_target(event.entry_price)}</b>")
    lines.append(f"<b>SL (за H1 зоной):</b> <b>{format_target(event.sl_price)}</b>")
    if event.tp_price is not None:
        lines.append(f"<b>TP (ближайший H1 RB/SNR/FVG):</b> <b>{format_target(event.tp_price)}</b>")
    else:
        lines.append("<b>TP (ближайший H1 RB/SNR/FVG):</b> <b>не найден</b>")

    if event.rr_ratio is not None:
        lines.append(f"<b>R:R:</b> <b>{event.rr_ratio:.2f}</b>")
    if event.quote_currency:
        quote = html.escape(event.quote_currency)
        if event.profit_quote_per_lot is not None:
            lines.append(
                f"<b>Потенциал на 1 lot:</b> <b>{event.profit_quote_per_lot:.2f} {quote}</b>"
            )
        lines.append(
            f"<b>Риск на 1 lot:</b> <b>{event.loss_quote_per_lot:.2f} {quote}</b>"
        )

    if event.tp_target_type and event.tp_target_id:
        tp_type = AUTO_EYE_ELEMENT_LABELS.get(event.tp_target_type, event.tp_target_type.upper())
        lines.append(
            f"<b>TP-ориентир:</b> <b>{html.escape(tp_type)}</b> "
            f"<code>{html.escape(event.tp_target_id)}</code>"
        )

    lines.append(f"<b>ID:</b> <code>{html.escape(event.element_id)}</code>")
    return "\n".join(lines)


async def send_auto_eye_notifications(bot: Bot, state: BotState) -> int:
    if not state.config.telegram.auto_eye_notifications.enabled:
        return 0

    new_events = await asyncio.to_thread(collect_new_auto_eye_events, state)
    if not new_events:
        return 0

    recipients = list(state.config.telegram.allowed_user_ids)
    sent_count = 0

    for event in new_events:
        text = render_auto_eye_event_text(event)
        for user_id in recipients:
            try:
                await bot.send_message(chat_id=user_id, text=text)
                sent_count += 1
            except Exception:
                logger.exception(
                    "Failed to send auto-eye notification user_id=%s event=%s",
                    user_id,
                    event.dedupe_key,
                )

    logger.info(
        "Auto-eye notifications sent: events=%s recipients=%s messages=%s",
        len(new_events),
        len(recipients),
        sent_count,
    )
    return sent_count


def direction_label(direction: str) -> str:
    if direction == DIRECTION_ABOVE:
        return "≥"
    if direction == DIRECTION_BELOW:
        return "≤"
    if direction == CROSS_TOP_DOWN:
        return "сверху вниз"
    if direction == CROSS_BOTTOM_UP:
        return "снизу вверх"
    return direction


def direction_human(direction: str) -> str:
    if direction == DIRECTION_ABOVE:
        return "выше или равна"
    if direction == DIRECTION_BELOW:
        return "ниже или равна"
    if direction == CROSS_TOP_DOWN:
        return "пересекла сверху вниз"
    if direction == CROSS_BOTTOM_UP:
        return "пересекла снизу вверх"
    return direction


def compare_by_direction(current_value: float, direction: str, target: float) -> bool:
    if direction == DIRECTION_ABOVE:
        return current_value >= target
    if direction == DIRECTION_BELOW:
        return current_value <= target
    return False


def compare_candle_close_condition(current_value: float, direction: str, target: float) -> bool:
    if direction == CROSS_TOP_DOWN:
        return current_value <= target
    if direction == CROSS_BOTTOM_UP:
        return current_value >= target
    return compare_by_direction(current_value, direction, target)


def is_cross_triggered(
    previous_value: float,
    current_value: float,
    direction: str,
    target: float,
) -> bool:
    if direction == CROSS_TOP_DOWN:
        return previous_value > target and current_value <= target
    if direction == CROSS_BOTTOM_UP:
        return previous_value < target and current_value >= target
    return False


def parse_price(text: str) -> float | None:
    normalized = text.strip().replace(" ", "").replace(",", ".")
    normalized = re.sub(r"[^0-9.\-]", "", normalized)

    if not normalized or normalized in {"-", ".", "-."}:
        return None

    if normalized.count(".") > 1:
        return None

    try:
        return float(normalized)
    except ValueError:
        return None


def normalize_alert_message_value(value: object | None) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized


def parse_user_alert_message_input(text: str) -> tuple[str | None, str | None]:
    raw = (text or "").strip()
    if raw in {"-", "—"}:
        return None, None
    if not raw:
        return None, None
    if len(raw) > MAX_ALERT_MESSAGE_LENGTH:
        return None, (
            "Сообщение слишком длинное. "
            f"Максимум {MAX_ALERT_MESSAGE_LENGTH} символов."
        )
    return raw, None


def format_alert_message_preview(message_text: str | None) -> str:
    normalized = normalize_alert_message_value(message_text)
    if not normalized:
        return ""

    compact = " ".join(normalized.split())
    if len(compact) > 80:
        compact = compact[:77].rstrip() + "..."

    return f" | 💬 <i>{html.escape(compact)}</i>"


def format_alert_message_block(message_text: str | None) -> str:
    normalized = normalize_alert_message_value(message_text)
    if not normalized:
        return ""
    return f"\n<b>Сообщение:</b> {html.escape(normalized)}"


def format_target(target: float) -> str:
    return f"{target:.6f}".rstrip("0").rstrip(".")


def parse_utc_iso(value: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_local_datetime(utc_iso: str | None) -> str:
    if not utc_iso:
        return "unknown"

    dt = parse_utc_iso(utc_iso)
    if dt is None:
        return "unknown"

    local_dt = dt.astimezone(USER_TIMEZONE)
    return local_dt.strftime("%d.%m.%Y %H:%M") + f" {USER_TIMEZONE_LABEL}"


def render_alert_line(alert: AlertRule) -> str:
    message_suffix = format_alert_message_preview(alert.message_text)

    if alert.kind == ALERT_KIND_PRICE:
        if alert.target is None or alert.direction is None:
            return f"• <code>{html.escape(alert.asset)}</code>: некорректный ценовой алерт"
        return (
            f"• <code>{html.escape(alert.asset)}</code>: "
            f"{direction_label(alert.direction)} <b>{format_target(alert.target)}</b>"
            f"{message_suffix}"
        )

    if alert.kind == ALERT_KIND_TIME:
        when = format_local_datetime(alert.trigger_at_utc)
        return (
            f"• <code>{html.escape(alert.asset)}</code>: по времени "
            f"<b>{html.escape(when)}</b>{message_suffix}"
        )

    if alert.kind == ALERT_KIND_PRICE_TIME:
        if alert.target is None or alert.direction is None:
            return f"• <code>{html.escape(alert.asset)}</code>: некорректный price+time алерт"

        mode = alert.price_time_mode or ""
        tf = timeframe_label(alert.timeframe_code or "")
        condition = (
            f"{direction_label(alert.direction)} <b>{format_target(alert.target)}</b>"
        )
        if mode == PRICE_TIME_MODE_HOLD:
            next_check = "после начала удержания"
            started_at = parse_utc_iso(alert.condition_started_at_utc or "")
            if started_at is not None and alert.delay_minutes:
                hold_trigger = started_at + timedelta(minutes=max(1, int(alert.delay_minutes)))
                next_check = format_local_datetime(hold_trigger.isoformat())
            return (
                f"• <code>{html.escape(alert.asset)}</code>: "
                f"удержание {html.escape(tf)} при {condition} "
                f"(след. проверка: <b>{html.escape(next_check)}</b>)"
                f"{message_suffix}"
            )
        if mode == PRICE_TIME_MODE_CANDLE_CLOSE:
            next_when = format_local_datetime(alert.trigger_at_utc)
            return (
                f"• <code>{html.escape(alert.asset)}</code>: "
                f"закрытие {html.escape(tf)} при {condition} "
                f"(след. проверка: <b>{html.escape(next_when)}</b>)"
                f"{message_suffix}"
            )
        return f"• <code>{html.escape(alert.asset)}</code>: price+time {condition}{message_suffix}"

    return f"• <code>{html.escape(alert.asset)}</code>: неизвестный алерт"


def alert_sort_key(alert: AlertRule) -> tuple[str, str, str]:
    return (alert.asset, alert.kind, alert.created_at_utc)


def build_alert_selector(alert: AlertRule) -> str:
    return f"{alert.asset}|{alert.kind}|{alert.created_at_utc}"


def parse_alert_selector(selector: str) -> tuple[str, str, str] | None:
    parts = selector.split("|", maxsplit=2)
    if len(parts) != 3:
        return None

    asset, kind, created_at_utc = parts
    if not asset or kind not in {ALERT_KIND_PRICE, ALERT_KIND_TIME, ALERT_KIND_PRICE_TIME}:
        return None

    return asset, kind, created_at_utc


def format_alert_button_text(alert: AlertRule, *, include_asset: bool) -> str:
    prefix = f"{alert.asset} " if include_asset else ""

    if alert.kind == ALERT_KIND_PRICE:
        direction = direction_label(alert.direction or "")
        value = format_target(alert.target or 0.0)
        return f"❌ {prefix}{direction} {value}".strip()

    if alert.kind == ALERT_KIND_TIME:
        local_time = format_local_datetime(alert.trigger_at_utc)
        return f"❌ {prefix}{local_time}".strip()

    if alert.kind == ALERT_KIND_PRICE_TIME:
        direction = direction_label(alert.direction or "")
        value = format_target(alert.target or 0.0)
        tf_label = timeframe_label(alert.timeframe_code or "")
        mode_label = (
            "удержание"
            if alert.price_time_mode == PRICE_TIME_MODE_HOLD
            else "закрытие"
        )
        return f"❌ {prefix}{mode_label} {tf_label}: {direction} {value}".strip()

    return f"❌ {prefix}unknown".strip()


def timeframe_label(timeframe_code: str) -> str:
    mapping = {
        TIMEFRAME_M15: "M15",
        TIMEFRAME_H1: "H1",
        TIMEFRAME_H4: "H4",
        TIMEFRAME_D1: "D1",
        TIMEFRAME_W1: "W1",
        TIMEFRAME_M1: "M1",
    }
    return mapping.get(timeframe_code.lower(), timeframe_code.upper())


def is_supported_candle_timeframe(timeframe_code: str) -> bool:
    return timeframe_code in {
        TIMEFRAME_M15,
        TIMEFRAME_H1,
        TIMEFRAME_H4,
        TIMEFRAME_D1,
        TIMEFRAME_W1,
        TIMEFRAME_M1,
    }


def is_supported_hold_timeframe(timeframe_code: str) -> bool:
    return timeframe_code in HOLD_TIMEFRAME_MINUTES


def parse_hhmm_to_minutes(value: str) -> int:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", value)
    if match is None:
        raise ValueError(f"Invalid HH:MM value: {value!r}")

    hour = int(match.group(1))
    minute = int(match.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Invalid HH:MM value: {value!r}")

    return hour * 60 + minute


def load_timeframe_rules(path: Path) -> TimeframeRules:
    defaults_h4 = {
        "forex": parse_hhmm_to_minutes("03:00"),
        "indices": parse_hhmm_to_minutes("03:00"),
        "crypto": parse_hhmm_to_minutes("01:00"),
    }
    default_indices = {"GER40", "SPX500", "NDX100"}
    default_crypto_prefixes = [
        "BTC",
        "ETH",
        "SOL",
        "XRP",
        "LTC",
        "ADA",
        "DOGE",
        "BNB",
        "DOT",
        "AVAX",
        "TRX",
        "LINK",
    ]
    default_group = "forex"

    if not path.exists():
        logger.warning("Timeframe rules config not found, using defaults: %s", path)
        return TimeframeRules(
            h4_start_minutes_by_group=defaults_h4,
            indices_symbols=default_indices,
            crypto_prefixes=default_crypto_prefixes,
            default_group=default_group,
        )

    with path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    h4_raw = raw.get("h4_first_candle_start", {})
    h4_by_group = dict(defaults_h4)
    if isinstance(h4_raw, dict):
        for group_name, start_value in h4_raw.items():
            if not isinstance(group_name, str):
                continue
            try:
                h4_by_group[group_name.strip().lower()] = parse_hhmm_to_minutes(
                    str(start_value)
                )
            except ValueError:
                logger.warning(
                    "Invalid h4 start value for group %s: %s",
                    group_name,
                    start_value,
                )

    asset_groups_raw = raw.get("asset_groups", {})
    indices_symbols = set(default_indices)
    crypto_prefixes = list(default_crypto_prefixes)

    if isinstance(asset_groups_raw, dict):
        indices_list = asset_groups_raw.get("indices_symbols", [])
        if isinstance(indices_list, list):
            parsed_indices = {
                str(symbol).strip().upper() for symbol in indices_list if str(symbol).strip()
            }
            if parsed_indices:
                indices_symbols = parsed_indices

        crypto_list = asset_groups_raw.get("crypto_prefixes", [])
        if isinstance(crypto_list, list):
            parsed_prefixes = [
                str(prefix).strip().upper()
                for prefix in crypto_list
                if str(prefix).strip()
            ]
            if parsed_prefixes:
                crypto_prefixes = parsed_prefixes

        group_value = str(asset_groups_raw.get("default", "")).strip().lower()
        if group_value:
            default_group = group_value

    logger.info(
        "Loaded timeframe rules: h4_groups=%s indices=%s crypto_prefixes=%s default=%s",
        sorted(h4_by_group.keys()),
        len(indices_symbols),
        len(crypto_prefixes),
        default_group,
    )

    return TimeframeRules(
        h4_start_minutes_by_group=h4_by_group,
        indices_symbols=indices_symbols,
        crypto_prefixes=crypto_prefixes,
        default_group=default_group,
    )


def normalize_symbol(asset: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", asset.upper())


def detect_asset_market_group(asset: str, rules: TimeframeRules) -> str:
    symbol = normalize_symbol(asset)

    if symbol in rules.indices_symbols:
        return "indices"

    for prefix in rules.crypto_prefixes:
        if symbol.startswith(prefix):
            return "crypto"

    if re.fullmatch(r"[A-Z]{6}", symbol):
        return "forex"

    return rules.default_group


def next_interval_close(now_local: datetime, *, interval_minutes: int) -> datetime:
    midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    current_minutes = now_local.hour * 60 + now_local.minute
    next_minutes = ((current_minutes // interval_minutes) + 1) * interval_minutes
    days_add, minute_of_day = divmod(next_minutes, 24 * 60)
    return midnight + timedelta(days=days_add, minutes=minute_of_day)


def next_h4_close_for_group(now_local: datetime, h4_start_minutes: int) -> datetime:
    midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    current_minutes = now_local.hour * 60 + now_local.minute
    step = 4 * 60

    if current_minutes < h4_start_minutes:
        next_minutes = h4_start_minutes
    else:
        passed = current_minutes - h4_start_minutes
        next_minutes = h4_start_minutes + ((passed // step) + 1) * step

    days_add, minute_of_day = divmod(next_minutes, 24 * 60)
    return midnight + timedelta(days=days_add, minutes=minute_of_day)


def next_d1_close(now_local: datetime) -> datetime:
    midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    candidate = midnight + timedelta(days=1)
    if candidate <= now_local:
        candidate += timedelta(days=1)
    return candidate


def next_w1_close(now_local: datetime) -> datetime:
    midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    days_until_monday = (7 - midnight.weekday()) % 7
    candidate = midnight + timedelta(days=days_until_monday)
    if candidate <= now_local:
        candidate += timedelta(days=7)
    return candidate


def next_m1_close(now_local: datetime) -> datetime:
    year = now_local.year
    month = now_local.month

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

    candidate = datetime(year, month, 1, tzinfo=now_local.tzinfo)
    if candidate <= now_local:
        if month == 12:
            candidate = datetime(year + 1, 1, 1, tzinfo=now_local.tzinfo)
        else:
            candidate = datetime(year, month + 1, 1, tzinfo=now_local.tzinfo)
    return candidate


def compute_timeframe_trigger_utc(
    state: BotState,
    asset: str,
    timeframe_code: str,
    *,
    now_utc: datetime | None = None,
) -> tuple[datetime, int, str]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(USER_TIMEZONE)
    normalized_timeframe = timeframe_code.lower()
    group = detect_asset_market_group(asset, state.timeframe_rules)

    if normalized_timeframe == TIMEFRAME_M15:
        trigger_local = next_interval_close(now_local, interval_minutes=15)
    elif normalized_timeframe == TIMEFRAME_H1:
        trigger_local = next_interval_close(now_local, interval_minutes=60)
    elif normalized_timeframe == TIMEFRAME_H4:
        h4_start = state.timeframe_rules.h4_start_minutes_by_group.get(
            group,
            state.timeframe_rules.h4_start_minutes_by_group.get(
                state.timeframe_rules.default_group,
                parse_hhmm_to_minutes("03:00"),
            ),
        )
        trigger_local = next_h4_close_for_group(now_local, h4_start)
    elif normalized_timeframe == TIMEFRAME_D1:
        trigger_local = next_d1_close(now_local)
    elif normalized_timeframe == TIMEFRAME_W1:
        trigger_local = next_w1_close(now_local)
    elif normalized_timeframe == TIMEFRAME_M1:
        trigger_local = next_m1_close(now_local)
    else:
        raise ValueError(f"Unsupported timeframe code: {timeframe_code}")

    trigger_utc = trigger_local.astimezone(timezone.utc)
    delay_minutes = max(1, math.ceil((trigger_utc - now_utc).total_seconds() / 60))
    return trigger_utc, delay_minutes, group


def advance_candle_close_utc(
    previous_trigger_utc: datetime,
    timeframe_code: str | None,
) -> datetime | None:
    if timeframe_code is None:
        return None

    code = timeframe_code.lower()
    if code == TIMEFRAME_M15:
        return previous_trigger_utc + timedelta(minutes=15)
    if code == TIMEFRAME_H1:
        return previous_trigger_utc + timedelta(hours=1)
    if code == TIMEFRAME_H4:
        return previous_trigger_utc + timedelta(hours=4)
    if code == TIMEFRAME_D1:
        return previous_trigger_utc + timedelta(days=1)
    if code == TIMEFRAME_W1:
        return previous_trigger_utc + timedelta(days=7)
    if code == TIMEFRAME_M1:
        after_prev_local = (previous_trigger_utc + timedelta(seconds=1)).astimezone(
            USER_TIMEZONE
        )
        return next_m1_close(after_prev_local).astimezone(timezone.utc)
    return None


def classify_asset_group(asset: str) -> str:
    normalized = asset.strip().upper()
    if re.fullmatch(r"[A-Z]{6}", normalized):
        return f"{normalized[:3]}/N*"
    return "INDICES"


def group_assets_for_ui(assets: list[str]) -> list[tuple[str, list[str]]]:
    grouped: dict[str, list[str]] = {}
    for asset in assets:
        name = classify_asset_group(asset)
        grouped.setdefault(name, []).append(asset)

    ordered: list[tuple[str, list[str]]] = []
    for name in PREFERRED_GROUP_ORDER:
        items = grouped.pop(name, [])
        if items:
            ordered.append((name, items))

    for name in sorted(grouped):
        ordered.append((name, grouped[name]))

    return ordered


def render_grouped_quotes(config: AppConfig, quotes: QuotesMap) -> list[str]:
    lines: list[str] = []
    grouped = group_assets_for_ui(get_display_assets(config, quotes))
    if not grouped:
        return ["• нет данных"]

    for group_name, group_assets in grouped:
        lines.append(f"<b>{html.escape(group_name)}</b>")
        for asset in group_assets:
            value = str(quotes.get(asset, {}).get("value") or "n/a")
            lines.append(
                f"• <code>{html.escape(asset)}</code>: "
                f"<b>{html.escape(value)}</b>"
            )
        lines.append("")

    if lines and lines[-1] == "":
        lines.pop()

    return lines


def load_cached_quotes(path: Path) -> QuotesMap:
    if not path.exists():
        logger.info("Cached quotes file not found: %s", path)
        return {}

    with path.open("r", encoding="utf-8") as file:
        raw = json.load(file)

    quotes = raw.get("quotes", {})
    if isinstance(quotes, dict):
        logger.info("Loaded cached quotes: %s items", len(quotes))
        return quotes

    logger.warning("Cached quotes format invalid in %s", path)
    return {}


def build_home_keyboard(
    has_alerts: bool,
    *,
    has_backtest: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="Обновить и проверить", callback_data=CALLBACK_REFRESH)],
        [InlineKeyboardButton(text="Меню алертов", callback_data=CALLBACK_MENU_ALERTS)],
    ]
    if has_backtest:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Бектест сценариев",
                    callback_data=CALLBACK_MENU_BACKTEST,
                )
            ]
        )
    if has_alerts:
        rows.append([InlineKeyboardButton(text="Удалить алерт", callback_data=CALLBACK_MENU_DELETE)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_alerts_menu_keyboard(assets: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    for group, group_assets in group_assets_for_ui(assets):
        rows.append(
            [InlineKeyboardButton(text=f"▾ {group} ▾", callback_data=CALLBACK_NOOP)]
        )
        group_row: list[InlineKeyboardButton] = []
        for asset in group_assets:
            group_row.append(
                InlineKeyboardButton(
                    text=asset,
                    callback_data=f"{CALLBACK_ALERT_ASSET_PREFIX}{asset}",
                )
            )
            if len(group_row) == 2:
                rows.append(group_row)
                group_row = []

        if group_row:
            rows.append(group_row)

    rows.append([InlineKeyboardButton(text="Назад", callback_data=CALLBACK_MENU_HOME)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_backtest_assets_keyboard(assets: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    for group, group_assets in group_assets_for_ui(assets):
        rows.append(
            [InlineKeyboardButton(text=f"▾ {group} ▾", callback_data=CALLBACK_NOOP)]
        )
        group_row: list[InlineKeyboardButton] = []
        for asset in group_assets:
            group_row.append(
                InlineKeyboardButton(
                    text=asset,
                    callback_data=f"{CALLBACK_BACKTEST_ASSET_PREFIX}{asset}",
                )
            )
            if len(group_row) == 2:
                rows.append(group_row)
                group_row = []

        if group_row:
            rows.append(group_row)

    rows.append([InlineKeyboardButton(text="Назад", callback_data=CALLBACK_MENU_HOME)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_backtest_period_keyboard(asset: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Последние 6 часов",
                    callback_data=f"{CALLBACK_BACKTEST_RANGE_PREFIX}{asset}|6",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Последние 24 часа",
                    callback_data=f"{CALLBACK_BACKTEST_RANGE_PREFIX}{asset}|24",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Последние 7 дней",
                    callback_data=f"{CALLBACK_BACKTEST_RANGE_PREFIX}{asset}|168",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Свой интервал",
                    callback_data=f"{CALLBACK_BACKTEST_CUSTOM_PREFIX}{asset}",
                )
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=CALLBACK_MENU_BACKTEST),
                InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_BACKTEST_CANCEL),
            ],
        ]
    )


def build_backtest_input_keyboard(asset: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Назад",
                    callback_data=f"{CALLBACK_BACKTEST_BACK_PREFIX}{asset}",
                ),
                InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_BACKTEST_CANCEL),
            ]
        ]
    )


def build_asset_alert_keyboard(asset: str, asset_alerts: list[AlertRule]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="Пересечение цены",
                callback_data=f"{CALLBACK_PRICE_CROSS_MENU_PREFIX}{asset}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="Удержание по времени",
                callback_data=f"{CALLBACK_PRICE_TIME_MENU_PREFIX}{asset}",
            ),
        ],
        [
            InlineKeyboardButton(
                text="Таймер: закрытие свечи",
                callback_data=f"{CALLBACK_TIME_CANDLE_MENU_PREFIX}{asset}",
            )
        ],
        [
            InlineKeyboardButton(
                text="Точное время",
                callback_data=f"{CALLBACK_TIME_CUSTOM_PREFIX}{asset}",
            )
        ],
    ]

    rows.append(
        [
            InlineKeyboardButton(
                text="Удалить алерт",
                callback_data=f"{CALLBACK_DELETE_ASSET_PREFIX}{asset}",
            )
        ]
    )
    if asset_alerts:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Редактировать алерт",
                    callback_data=f"{CALLBACK_EDIT_ALERT_MENU_PREFIX}{asset}",
                )
            ]
        )

    rows.append([InlineKeyboardButton(text="Назад к меню", callback_data=CALLBACK_MENU_ALERTS)])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_delete_alerts_keyboard(alerts: list[AlertRule]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    for alert in sorted(alerts, key=alert_sort_key):
        rows.append(
            [
                InlineKeyboardButton(
                    text=format_alert_button_text(alert, include_asset=True),
                    callback_data=f"{CALLBACK_DELETE_ONE_HOME_PREFIX}{build_alert_selector(alert)}",
                )
            ]
        )

    rows.append([InlineKeyboardButton(text="Назад", callback_data=CALLBACK_MENU_HOME)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_asset_delete_select_keyboard(
    asset: str,
    asset_alerts: list[AlertRule],
    selected_selectors: set[str],
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    for alert in sorted(asset_alerts, key=alert_sort_key):
        selector = build_alert_selector(alert)
        checked = "☑" if selector in selected_selectors else "☐"
        text = format_alert_button_text(alert, include_asset=False)
        if text.startswith("❌ "):
            text = text[2:]
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{checked} {text}",
                    callback_data=f"{CALLBACK_DELETE_ONE_ASSET_PREFIX}{selector}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                text=f"Удалить выбранные ({len(selected_selectors)})",
                callback_data=f"{CALLBACK_DELETE_APPLY_ASSET_PREFIX}{asset}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(text="Назад", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
            InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_cancel_keyboard(asset: str | None = None) -> InlineKeyboardMarkup:
    if asset:
        callback_data = f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"
    else:
        callback_data = CALLBACK_CANCEL
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=callback_data)]]
    )


def build_input_step_keyboard(asset: str, back_callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Назад", callback_data=back_callback),
                InlineKeyboardButton(text="Отмена", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
            ]
        ]
    )


def build_price_cross_direction_keyboard(asset: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Сверху вниз ⬇️",
                    callback_data=f"{CALLBACK_PRICE_SET_PREFIX}{asset}|{CROSS_TOP_DOWN}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Снизу вверх ⬆️",
                    callback_data=f"{CALLBACK_PRICE_SET_PREFIX}{asset}|{CROSS_BOTTOM_UP}",
                )
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
                InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
            ],
        ]
    )


def build_price_time_mode_keyboard(asset: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Закрытие свечи",
                    callback_data=f"{CALLBACK_PRICE_TIME_CANDLE_MENU_PREFIX}{asset}",
                )
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
                InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
            ],
        ]
    )


def build_price_time_direction_keyboard(asset: str, *, back_callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Сверху вниз ⬇️",
                    callback_data=f"{CALLBACK_PRICE_TIME_DIR_PREFIX}{asset}|{CROSS_TOP_DOWN}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Снизу вверх ⬆️",
                    callback_data=f"{CALLBACK_PRICE_TIME_DIR_PREFIX}{asset}|{CROSS_BOTTOM_UP}",
                )
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=back_callback),
                InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
            ],
        ]
    )


def build_price_time_tf_keyboard(
    asset: str,
    cross_direction: str,
    *,
    back_callback: str,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="M15",
                    callback_data=f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset}|{cross_direction}|{TIMEFRAME_M15}",
                ),
                InlineKeyboardButton(
                    text="H1",
                    callback_data=f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset}|{cross_direction}|{TIMEFRAME_H1}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="H4",
                    callback_data=f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset}|{cross_direction}|{TIMEFRAME_H4}",
                ),
                InlineKeyboardButton(
                    text="D1",
                    callback_data=f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset}|{cross_direction}|{TIMEFRAME_D1}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="W1",
                    callback_data=f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset}|{cross_direction}|{TIMEFRAME_W1}",
                ),
                InlineKeyboardButton(
                    text="M1",
                    callback_data=f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset}|{cross_direction}|{TIMEFRAME_M1}",
                ),
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=back_callback),
                InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
            ],
        ]
    )


def build_time_candle_tf_keyboard(asset: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="M15",
                    callback_data=f"{CALLBACK_TIME_QUICK_PREFIX}{asset}|{TIMEFRAME_M15}",
                ),
                InlineKeyboardButton(
                    text="H1",
                    callback_data=f"{CALLBACK_TIME_QUICK_PREFIX}{asset}|{TIMEFRAME_H1}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="H4",
                    callback_data=f"{CALLBACK_TIME_QUICK_PREFIX}{asset}|{TIMEFRAME_H4}",
                )
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
                InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
            ],
        ]
    )


def build_extend_keyboard(alert: AlertRule) -> InlineKeyboardMarkup | None:
    _ = alert
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Удалить сообщение",
                    callback_data=CALLBACK_ALERT_DELETE_MESSAGE,
                )
            ]
        ]
    )


def build_edit_alert_select_keyboard(asset: str, asset_alerts: list[AlertRule]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for alert in sorted(asset_alerts, key=alert_sort_key):
        rows.append(
            [
                InlineKeyboardButton(
                    text=format_alert_button_text(alert, include_asset=False),
                    callback_data=f"{CALLBACK_EDIT_ALERT_PICK_PREFIX}{build_alert_selector(alert)}",
                )
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(text="Назад", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
            InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_edit_type_keyboard(asset: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="Пересечение цены",
                callback_data=f"{CALLBACK_EDIT_TYPE_PREFIX}{EDIT_TYPE_PRICE_CROSS}",
            )
        ],
        [
            InlineKeyboardButton(
                text="Цена + удержание",
                callback_data=f"{CALLBACK_EDIT_TYPE_PREFIX}{EDIT_TYPE_PRICE_HOLD}",
            )
        ],
        [
            InlineKeyboardButton(
                text="Цена + закрытие свечи",
                callback_data=f"{CALLBACK_EDIT_TYPE_PREFIX}{EDIT_TYPE_PRICE_CANDLE}",
            )
        ],
        [
            InlineKeyboardButton(
                text="Таймер: закрытие свечи",
                callback_data=f"{CALLBACK_EDIT_TYPE_PREFIX}{EDIT_TYPE_TIME_CANDLE}",
            )
        ],
        [
            InlineKeyboardButton(
                text="Точное время",
                callback_data=f"{CALLBACK_EDIT_TYPE_PREFIX}{EDIT_TYPE_TIME_CUSTOM}",
            )
        ],
        [
            InlineKeyboardButton(text="Назад", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
            InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_edit_keep_change_keyboard(asset: str, field: str, current_value: str) -> InlineKeyboardMarkup:
    compact_value = " ".join(current_value.split())
    if len(compact_value) > 32:
        compact_value = compact_value[:29].rstrip() + "..."

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"Оставить: {compact_value}",
                    callback_data=f"{CALLBACK_EDIT_KEEP_PREFIX}{field}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Изменить",
                    callback_data=f"{CALLBACK_EDIT_CHANGE_PREFIX}{field}",
                )
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=CALLBACK_EDIT_BACK),
                InlineKeyboardButton(text="Отмена", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
            ],
        ]
    )


def build_edit_direction_keyboard(asset: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Сверху вниз ⬇️",
                    callback_data=f"{CALLBACK_EDIT_SET_DIR_PREFIX}{CROSS_TOP_DOWN}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Снизу вверх ⬆️",
                    callback_data=f"{CALLBACK_EDIT_SET_DIR_PREFIX}{CROSS_BOTTOM_UP}",
                )
            ],
            [
                InlineKeyboardButton(text="Назад", callback_data=CALLBACK_EDIT_BACK),
                InlineKeyboardButton(text="Отмена", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
            ],
        ]
    )


def build_edit_timeframe_keyboard(
    asset: str,
    timeframe_codes: list[str],
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    current_row: list[InlineKeyboardButton] = []
    for code in timeframe_codes:
        current_row.append(
            InlineKeyboardButton(
                text=timeframe_label(code),
                callback_data=f"{CALLBACK_EDIT_SET_TF_PREFIX}{code}",
            )
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)

    rows.append(
        [
            InlineKeyboardButton(text="Назад", callback_data=CALLBACK_EDIT_BACK),
            InlineKeyboardButton(text="Отмена", callback_data=f"{CALLBACK_BACK_ASSET_PREFIX}{asset}"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_display_assets(config: AppConfig, quotes: QuotesMap) -> list[str]:
    assets = [asset for asset in config.scraper.assets if asset]
    known = set(assets)

    extras = sorted(asset for asset in quotes.keys() if asset and asset not in known)
    assets.extend(extras)
    return assets


def get_backtest_assets(config: AppConfig, quotes: QuotesMap) -> list[str]:
    if config.auto_eye.symbols:
        source = list(config.auto_eye.symbols)
    else:
        source = get_display_assets(config, quotes)

    assets: list[str] = []
    seen: set[str] = set()
    for item in source:
        asset = str(item).strip().upper()
        if not asset or asset in seen:
            continue
        seen.add(asset)
        assets.append(asset)
    return assets


def render_backtest_assets_menu_text() -> str:
    return (
        "<b>Бектест сценариев</b>\n\n"
        "Выберите актив. После этого выберите период или задайте свой интервал."
    )


def render_backtest_period_menu_text(asset: str) -> str:
    return (
        f"<b>Бектест: {html.escape(asset)}</b>\n\n"
        "Выберите период:\n"
        "• быстрый диапазон, или\n"
        "• свой интервал в формате <code>dd.mm.yyyy hh:mm - dd.mm.yyyy hh:mm</code>"
    )


def render_dashboard_text(config: AppConfig, quotes: QuotesMap) -> str:
    lines: list[str] = []
    lines.extend(render_grouped_quotes(config, quotes))
    return "\n".join(lines)


def render_alerts_menu_text(chat_alerts: list[AlertRule]) -> str:
    lines = ["<b>Управление алертами</b>", "", "Выберите актив из списка ниже.", ""]
    lines.append("<b>Текущие алерты</b>")

    if not chat_alerts:
        lines.append("• нет")
    else:
        for alert in sorted(chat_alerts, key=alert_sort_key):
            lines.append(render_alert_line(alert))

    lines.append("")
    lines.append(
        "<i>Время пользовательского ввода: "
        f"{html.escape(USER_TIMEZONE_LABEL)} | формат: dd.mm.yyyy hh:mm</i>"
    )
    return "\n".join(lines)


def render_asset_menu_text(asset: str, asset_alerts: list[AlertRule]) -> str:
    lines = [f"<b>{html.escape(asset)}</b>", ""]

    if not asset_alerts:
        lines.append("• Активных алертов по активу нет.")
    else:
        lines.append("<b>Активные алерты</b>")
        for alert in sorted(asset_alerts, key=alert_sort_key):
            lines.append(render_alert_line(alert))

    lines.append("")
    lines.append("<i>Выберите действие ниже.</i>")
    return "\n".join(lines)


def render_asset_delete_select_text(
    asset: str,
    asset_alerts: list[AlertRule],
    selected_count: int,
) -> str:
    lines = [
        f"<b>{html.escape(asset)}</b>",
        "",
        "<b>Удаление алертов</b>",
    ]
    if not asset_alerts:
        lines.append("• Активных алертов по активу нет.")
    else:
        lines.append("Выберите один или несколько алертов в списке ниже.")
        lines.append(f"<i>Выбрано: {selected_count}</i>")

    return "\n".join(lines)


def render_delete_menu_text(alerts: list[AlertRule]) -> str:
    lines = ["<b>Удаление алертов</b>", "", "Выберите алерт для удаления.", ""]

    if not alerts:
        lines.append("• У вас нет активных алертов.")
    else:
        for alert in sorted(alerts, key=alert_sort_key):
            lines.append(render_alert_line(alert))

    return "\n".join(lines)


def render_edit_alert_select_text(asset: str, asset_alerts: list[AlertRule]) -> str:
    lines = [
        f"<b>{html.escape(asset)}</b>",
        "",
        "<b>Редактирование алерта</b>",
    ]
    if not asset_alerts:
        lines.append("• Активных алертов по активу нет.")
    else:
        lines.append("Выберите алерт, который нужно изменить.")
    return "\n".join(lines)


def edit_type_label(edit_type: str) -> str:
    mapping = {
        EDIT_TYPE_PRICE_CROSS: "Пересечение цены",
        EDIT_TYPE_PRICE_HOLD: "Цена + удержание",
        EDIT_TYPE_PRICE_CANDLE: "Цена + закрытие свечи",
        EDIT_TYPE_TIME_CANDLE: "Таймер: закрытие свечи",
        EDIT_TYPE_TIME_CUSTOM: "Точное время",
    }
    return mapping.get(edit_type, edit_type)


def find_user_alert_by_selector(
    state: BotState,
    user_id: int,
    selector: str,
) -> AlertRule | None:
    parsed = parse_alert_selector(selector)
    if parsed is None:
        return None

    asset, kind, created_at_utc = parsed
    for alert in state.alert_store.list_for_user(user_id):
        if (
            alert.asset == asset
            and alert.kind == kind
            and alert.created_at_utc == created_at_utc
        ):
            return alert
    return None


def infer_quick_timeframe_from_alert(alert: AlertRule) -> str | None:
    if alert.kind == ALERT_KIND_PRICE_TIME and alert.timeframe_code in {
        TIMEFRAME_M15,
        TIMEFRAME_H1,
        TIMEFRAME_H4,
    }:
        return alert.timeframe_code

    if alert.kind == ALERT_KIND_TIME:
        mapping = {
            15: TIMEFRAME_M15,
            60: TIMEFRAME_H1,
            240: TIMEFRAME_H4,
        }
        return mapping.get(int(alert.delay_minutes or 0))

    return None


def get_edit_type_required_fields(edit_type: str) -> list[str]:
    if edit_type == EDIT_TYPE_PRICE_CROSS:
        return ["direction", "target", "message"]
    if edit_type == EDIT_TYPE_PRICE_HOLD:
        return ["direction", "target", "timeframe", "message"]
    if edit_type == EDIT_TYPE_PRICE_CANDLE:
        return ["direction", "target", "timeframe", "message"]
    if edit_type == EDIT_TYPE_TIME_CANDLE:
        return ["timeframe", "message"]
    if edit_type == EDIT_TYPE_TIME_CUSTOM:
        return ["trigger_at_utc", "message"]
    return []


def get_edit_timeframe_options(edit_type: str) -> list[str]:
    if edit_type == EDIT_TYPE_PRICE_HOLD:
        return [TIMEFRAME_M15, TIMEFRAME_H1, TIMEFRAME_H4]
    if edit_type == EDIT_TYPE_PRICE_CANDLE:
        return [
            TIMEFRAME_M15,
            TIMEFRAME_H1,
            TIMEFRAME_H4,
            TIMEFRAME_D1,
            TIMEFRAME_W1,
            TIMEFRAME_M1,
        ]
    if edit_type == EDIT_TYPE_TIME_CANDLE:
        return [TIMEFRAME_M15, TIMEFRAME_H1, TIMEFRAME_H4]
    return []


def get_original_edit_field_value(
    original_alert: AlertRule,
    edit_type: str,
    field: str,
) -> object | None:
    if field == "direction":
        if original_alert.kind in {ALERT_KIND_PRICE, ALERT_KIND_PRICE_TIME}:
            return original_alert.direction
        return None

    if field == "target":
        if original_alert.kind in {ALERT_KIND_PRICE, ALERT_KIND_PRICE_TIME}:
            return original_alert.target
        return None

    if field == "timeframe":
        options = set(get_edit_timeframe_options(edit_type))
        if original_alert.kind == ALERT_KIND_PRICE_TIME and original_alert.timeframe_code in options:
            return original_alert.timeframe_code
        inferred = infer_quick_timeframe_from_alert(original_alert)
        if inferred in options:
            return inferred
        return None

    if field == "trigger_at_utc":
        if original_alert.kind == ALERT_KIND_TIME and original_alert.trigger_at_utc:
            return original_alert.trigger_at_utc
        return None

    if field == "message":
        return normalize_alert_message_value(original_alert.message_text)

    return None


def snapshot_edit_session(session: dict[str, object]) -> dict[str, object]:
    return {
        "target_type": session.get("target_type"),
        "direction": session.get("direction"),
        "target": session.get("target"),
        "timeframe_code": session.get("timeframe_code"),
        "trigger_at_utc": session.get("trigger_at_utc"),
        "message": session.get("message"),
        "step": session.get("step"),
        "field": session.get("field"),
    }


def push_edit_session_history(session: dict[str, object]) -> None:
    history_raw = session.get("history")
    if not isinstance(history_raw, list):
        history_raw = []
        session["history"] = history_raw
    history_raw.append(snapshot_edit_session(session))


def pop_edit_session_history(session: dict[str, object]) -> bool:
    history_raw = session.get("history")
    if not isinstance(history_raw, list) or not history_raw:
        return False

    snapshot = history_raw.pop()
    for key, value in snapshot.items():
        session[key] = value
    return True


def set_edit_step(session: dict[str, object], step: str, field: str | None = None) -> None:
    session["step"] = step
    session["field"] = field


def get_next_unset_edit_field(session: dict[str, object]) -> str | None:
    edit_type = str(session.get("target_type") or "")
    if not edit_type:
        return None

    for field in get_edit_type_required_fields(edit_type):
        if field == "direction" and not session.get("direction"):
            return field
        if field == "target" and session.get("target") is None:
            return field
        if field == "timeframe" and not session.get("timeframe_code"):
            return field
        if field == "trigger_at_utc" and not session.get("trigger_at_utc"):
            return field
        if field == "message" and session.get("message") is None:
            return field
    return None


def choose_edit_input_step(field: str) -> str:
    if field == "direction":
        return "choose_direction"
    if field == "timeframe":
        return "choose_timeframe"
    if field == "target":
        return "input_target"
    if field == "trigger_at_utc":
        return "input_time"
    if field == "message":
        return "input_message"
    return "choose_type"


def advance_edit_session_step(session: dict[str, object]) -> None:
    next_field = get_next_unset_edit_field(session)
    if next_field is None:
        set_edit_step(session, "review")
        return

    original_alert = session.get("original_alert")
    if not isinstance(original_alert, AlertRule):
        set_edit_step(session, "choose_type")
        return

    edit_type = str(session.get("target_type") or "")
    original_value = get_original_edit_field_value(original_alert, edit_type, next_field)
    if original_value is None:
        set_edit_step(session, choose_edit_input_step(next_field), next_field)
        return

    set_edit_step(session, "ask_keep_change", next_field)


def render_edit_session_text(session: dict[str, object]) -> str:
    asset = str(session.get("asset") or "")
    step = str(session.get("step") or "")
    original_alert = session.get("original_alert")
    target_type = str(session.get("target_type") or "")

    lines = [f"<b>{html.escape(asset)}</b>", "", "<b>Редактирование алерта</b>"]
    if isinstance(original_alert, AlertRule):
        lines.append(f"Текущий: {render_alert_line(original_alert)}")

    if step == "choose_type":
        lines.append("")
        lines.append("Выберите новый тип алерта.")
        return "\n".join(lines)

    if step == "ask_keep_change":
        field = str(session.get("field") or "")
        value = get_original_edit_field_value(
            original_alert,
            target_type,
            field,
        ) if isinstance(original_alert, AlertRule) else None
        if field == "direction":
            field_text = "направление"
            value_text = direction_label(str(value or ""))
        elif field == "target":
            field_text = "уровень цены"
            value_text = format_target(float(value)) if isinstance(value, (int, float)) else "-"
        elif field == "timeframe":
            field_text = "таймфрейм"
            value_text = timeframe_label(str(value or ""))
        elif field == "trigger_at_utc":
            field_text = "время"
            value_text = format_local_datetime(str(value or ""))
        elif field == "message":
            field_text = "сообщение"
            value_text = " ".join(str(value).split())
            if len(value_text) > 70:
                value_text = value_text[:67].rstrip() + "..."
        else:
            field_text = field
            value_text = str(value or "-")
        lines.append("")
        lines.append(
            f"Для поля <b>{html.escape(field_text)}</b> оставить текущее значение "
            f"<b>{html.escape(value_text)}</b> или изменить?"
        )
        return "\n".join(lines)

    if step == "choose_direction":
        lines.append("")
        lines.append("Выберите направление пересечения.")
        return "\n".join(lines)

    if step == "choose_timeframe":
        lines.append("")
        lines.append("Выберите таймфрейм.")
        return "\n".join(lines)

    if step == "input_target":
        lines.append("")
        lines.append("Введите новый уровень цены. Пример: <code>1.2456</code>")
        return "\n".join(lines)

    if step == "input_time":
        lines.append("")
        lines.append(
            "Введите время в зоне "
            f"<b>{html.escape(USER_TIMEZONE_LABEL)}</b> в формате "
            "<code>dd.mm.yyyy HH:MM</code>."
        )
        return "\n".join(lines)

    if step == "input_message":
        lines.append("")
        lines.append(
            "Введите сообщение к алерту или <code>-</code>, "
            "если сообщение не нужно."
        )
        return "\n".join(lines)

    if step == "review":
        lines.append("")
        lines.append(f"Новый тип: <b>{html.escape(edit_type_label(target_type))}</b>")
        if session.get("direction"):
            lines.append(f"Направление: <b>{html.escape(direction_label(str(session.get('direction'))))}</b>")
        if session.get("target") is not None:
            lines.append(f"Уровень: <b>{format_target(float(session.get('target') or 0.0))}</b>")
        if session.get("timeframe_code"):
            lines.append(f"TF: <b>{html.escape(timeframe_label(str(session.get('timeframe_code'))))}</b>")
        if session.get("trigger_at_utc"):
            lines.append(
                "Время: "
                f"<b>{html.escape(format_local_datetime(str(session.get('trigger_at_utc'))))}</b>"
            )
        message_preview = normalize_alert_message_value(session.get("message"))
        if message_preview:
            compact = " ".join(message_preview.split())
            if len(compact) > 120:
                compact = compact[:117].rstrip() + "..."
            lines.append(f"Сообщение: <i>{html.escape(compact)}</i>")
        else:
            lines.append("Сообщение: <i>без сообщения</i>")
        lines.append("")
        lines.append("Подтверждение не требуется, нажмите любую кнопку шага назад/отмены или отправьте значение, если нужно изменить.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Ошибка шага редактирования.")
    return "\n".join(lines)


def build_edit_session_keyboard(session: dict[str, object]) -> InlineKeyboardMarkup:
    asset = str(session.get("asset") or "")
    step = str(session.get("step") or "")
    field = str(session.get("field") or "")
    target_type = str(session.get("target_type") or "")

    if step == "choose_type":
        return build_edit_type_keyboard(asset)

    if step == "ask_keep_change":
        original_alert = session.get("original_alert")
        value = get_original_edit_field_value(
            original_alert,
            target_type,
            field,
        ) if isinstance(original_alert, AlertRule) else None
        if field == "direction":
            value_text = direction_label(str(value or ""))
        elif field == "target":
            value_text = format_target(float(value)) if isinstance(value, (int, float)) else "-"
        elif field == "timeframe":
            value_text = timeframe_label(str(value or ""))
        elif field == "trigger_at_utc":
            value_text = format_local_datetime(str(value or ""))
        else:
            value_text = str(value or "-")
        return build_edit_keep_change_keyboard(asset, field, value_text)

    if step == "choose_direction":
        return build_edit_direction_keyboard(asset)

    if step == "choose_timeframe":
        return build_edit_timeframe_keyboard(asset, get_edit_timeframe_options(target_type))

    if step in {"input_target", "input_time", "input_message"}:
        return build_input_step_keyboard(asset, CALLBACK_EDIT_BACK)

    return build_edit_type_keyboard(asset)


def set_pending_for_edit_step(
    state: BotState,
    user_id: int,
    session: dict[str, object],
) -> None:
    step = str(session.get("step") or "")
    asset = str(session.get("asset") or "")
    if step == "input_target":
        state.pending_inputs[user_id] = {
            "type": "edit_target_input",
            "asset": asset,
            "back_callback": CALLBACK_EDIT_BACK,
        }
        return
    if step == "input_time":
        state.pending_inputs[user_id] = {
            "type": "edit_time_input",
            "asset": asset,
            "back_callback": CALLBACK_EDIT_BACK,
        }
        return
    if step == "input_message":
        state.pending_inputs[user_id] = {
            "type": "edit_message_input",
            "asset": asset,
            "back_callback": CALLBACK_EDIT_BACK,
        }
        return

    waiting = state.pending_inputs.get(user_id)
    if waiting is None:
        return
    if str(waiting.get("type", "")) in {
        "edit_target_input",
        "edit_time_input",
        "edit_message_input",
    }:
        state.pending_inputs.pop(user_id, None)


def apply_edit_session(state: BotState, user_id: int) -> tuple[bool, str, str]:
    session = state.alert_edit_sessions.get(user_id)
    if session is None:
        return False, "Сессия редактирования не найдена.", ""

    asset = str(session.get("asset") or "")
    selector = str(session.get("selector") or "")
    parsed = parse_alert_selector(selector)
    if parsed is None:
        return False, "Не удалось определить исходный алерт.", asset

    edit_type = str(session.get("target_type") or "")
    direction = str(session.get("direction") or "")
    target_raw = session.get("target")
    timeframe_code = str(session.get("timeframe_code") or "")
    trigger_raw = str(session.get("trigger_at_utc") or "")
    message_value = normalize_alert_message_value(session.get("message"))

    target_value = float(target_raw) if isinstance(target_raw, (int, float)) else None

    if edit_type in {EDIT_TYPE_PRICE_CROSS, EDIT_TYPE_PRICE_HOLD, EDIT_TYPE_PRICE_CANDLE}:
        if direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP, DIRECTION_ABOVE, DIRECTION_BELOW}:
            return False, "Некорректное направление.", asset
        if target_value is None:
            return False, "Некорректный уровень цены.", asset

    if edit_type in {EDIT_TYPE_PRICE_HOLD, EDIT_TYPE_PRICE_CANDLE, EDIT_TYPE_TIME_CANDLE}:
        options = set(get_edit_timeframe_options(edit_type))
        if timeframe_code not in options:
            return False, "Некорректный таймфрейм.", asset

    if edit_type == EDIT_TYPE_TIME_CUSTOM:
        trigger_at_utc = parse_utc_iso(trigger_raw)
        if trigger_at_utc is None:
            return False, "Некорректное время.", asset
        if trigger_at_utc <= datetime.now(timezone.utc):
            return False, "Время алерта уже в прошлом. Выберите новое.", asset

    old_asset, old_kind, old_created_at = parsed

    if edit_type == EDIT_TYPE_PRICE_CROSS:
        state.alert_store.upsert_price(
            user_id,
            asset,
            direction,
            float(target_value),
            message_text=message_value,
        )
    elif edit_type == EDIT_TYPE_PRICE_HOLD:
        hold_minutes = HOLD_TIMEFRAME_MINUTES[timeframe_code]
        state.alert_store.add_price_time(
            user_id=user_id,
            asset=asset,
            direction=direction,
            target=float(target_value),
            mode=PRICE_TIME_MODE_HOLD,
            timeframe_code=timeframe_code,
            delay_minutes=hold_minutes,
            message_text=message_value,
        )
    elif edit_type == EDIT_TYPE_PRICE_CANDLE:
        trigger_at_utc, _, _ = compute_timeframe_trigger_utc(state, asset, timeframe_code)
        state.alert_store.add_price_time(
            user_id=user_id,
            asset=asset,
            direction=direction,
            target=float(target_value),
            mode=PRICE_TIME_MODE_CANDLE_CLOSE,
            timeframe_code=timeframe_code,
            trigger_at_utc=trigger_at_utc,
            message_text=message_value,
        )
    elif edit_type == EDIT_TYPE_TIME_CANDLE:
        trigger_at_utc, delay, _ = compute_timeframe_trigger_utc(state, asset, timeframe_code)
        state.alert_store.add_time(
            user_id=user_id,
            asset=asset,
            trigger_at_utc=trigger_at_utc,
            delay_minutes=delay,
            message_text=message_value,
        )
    elif edit_type == EDIT_TYPE_TIME_CUSTOM:
        trigger_at_utc = parse_utc_iso(trigger_raw)
        if trigger_at_utc is None:
            return False, "Некорректное время.", asset
        delay = max(
            1,
            math.ceil((trigger_at_utc - datetime.now(timezone.utc)).total_seconds() / 60),
        )
        state.alert_store.add_time(
            user_id=user_id,
            asset=asset,
            trigger_at_utc=trigger_at_utc,
            delay_minutes=delay,
            message_text=message_value,
        )
    else:
        return False, "Неизвестный тип редактирования.", asset

    state.alert_store.remove_one(user_id, old_asset, old_kind, old_created_at)
    state.alert_edit_sessions.pop(user_id, None)
    state.pending_inputs.pop(user_id, None)
    return True, "Алерт обновлен.", asset


def is_user_allowed(state: BotState, user_id: int) -> bool:
    allowed = state.config.telegram.allowed_user_ids
    if not allowed:
        return True
    return user_id in allowed


def is_backtest_user_allowed(state: BotState, user_id: int) -> bool:
    backtest_cfg = state.config.telegram.backtest
    if not backtest_cfg.enabled:
        return False

    if not is_user_allowed(state, user_id):
        return False

    allowed = backtest_cfg.allowed_user_ids
    if not allowed:
        return False

    return user_id in allowed


async def ensure_message_allowed(state: BotState, message: Message) -> bool:
    user_id = message.from_user.id if message.from_user is not None else message.chat.id
    if is_user_allowed(state, user_id):
        return True

    logger.warning(
        "Unauthorized message dropped user_id=%s chat_id=%s",
        user_id,
        message.chat.id,
    )
    with contextlib.suppress(TelegramBadRequest):
        await message.delete()
    return False


async def ensure_callback_allowed(state: BotState, query: CallbackQuery) -> bool:
    user_id = query.from_user.id
    if is_user_allowed(state, user_id):
        return True

    logger.warning("Unauthorized callback dropped user_id=%s data=%s", user_id, query.data)
    with contextlib.suppress(TelegramBadRequest):
        await query.answer("Доступ запрещен", show_alert=False)
    return False


def get_user_id_from_query(query: CallbackQuery) -> int:
    return query.from_user.id


def get_user_id_from_message(message: Message) -> int:
    if message.from_user is not None:
        return message.from_user.id
    return message.chat.id


async def safe_edit_message(
    query: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup
) -> None:
    if query.message is None:
        return

    try:
        await query.message.edit_text(text=text, reply_markup=reply_markup)
    except TelegramBadRequest as error:
        if "message is not modified" not in str(error).lower():
            raise


def parse_custom_time_to_utc(text: str) -> tuple[datetime, int] | None:
    raw = text.strip()
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(USER_TIMEZONE)

    hhmm_match = HHMM_PATTERN.fullmatch(raw)
    if hhmm_match:
        hour = int(hhmm_match.group(1))
        minute = int(hhmm_match.group(2))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None

        trigger_local = now_local.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0,
        )
        if trigger_local <= now_local:
            trigger_local += timedelta(days=1)

        trigger_utc = trigger_local.astimezone(timezone.utc)
        delay_minutes = max(1, math.ceil((trigger_utc - now_utc).total_seconds() / 60))
        return trigger_utc, delay_minutes

    dmy_match = DMY_DATETIME_PATTERN.fullmatch(raw)
    if dmy_match:
        day = int(dmy_match.group(1))
        month = int(dmy_match.group(2))
        year = int(dmy_match.group(3))
        hour = int(dmy_match.group(4))
        minute = int(dmy_match.group(5))

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None

        try:
            trigger_local = datetime(
                year,
                month,
                day,
                hour,
                minute,
                tzinfo=USER_TIMEZONE,
            )
        except ValueError:
            return None

        trigger_utc = trigger_local.astimezone(timezone.utc)
        if trigger_utc <= now_utc:
            return None

        delay_minutes = max(1, math.ceil((trigger_utc - now_utc).total_seconds() / 60))
        return trigger_utc, delay_minutes

    full_match = FULL_DATETIME_PATTERN.fullmatch(raw)
    if full_match:
        year = int(full_match.group(1))
        month = int(full_match.group(2))
        day = int(full_match.group(3))
        hour = int(full_match.group(4))
        minute = int(full_match.group(5))

        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None

        try:
            trigger_local = datetime(
                year,
                month,
                day,
                hour,
                minute,
                tzinfo=USER_TIMEZONE,
            )
        except ValueError:
            return None

        trigger_utc = trigger_local.astimezone(timezone.utc)
        if trigger_utc <= now_utc:
            return None

        delay_minutes = max(1, math.ceil((trigger_utc - now_utc).total_seconds() / 60))
        return trigger_utc, delay_minutes

    return None


def parse_local_datetime_input(text: str) -> datetime | None:
    raw = text.strip()

    dmy_match = DMY_DATETIME_PATTERN.fullmatch(raw)
    if dmy_match:
        day = int(dmy_match.group(1))
        month = int(dmy_match.group(2))
        year = int(dmy_match.group(3))
        hour = int(dmy_match.group(4))
        minute = int(dmy_match.group(5))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        with contextlib.suppress(ValueError):
            return datetime(year, month, day, hour, minute, tzinfo=USER_TIMEZONE)
        return None

    full_match = FULL_DATETIME_PATTERN.fullmatch(raw)
    if full_match:
        year = int(full_match.group(1))
        month = int(full_match.group(2))
        day = int(full_match.group(3))
        hour = int(full_match.group(4))
        minute = int(full_match.group(5))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        with contextlib.suppress(ValueError):
            return datetime(year, month, day, hour, minute, tzinfo=USER_TIMEZONE)
        return None

    return None


def parse_backtest_interval_input(text: str) -> tuple[datetime, datetime] | None:
    raw = text.strip()
    match = BACKTEST_INTERVAL_PATTERN.fullmatch(raw)
    if match is None:
        return None

    left_raw = str(match.group(1) or "").strip()
    right_raw = str(match.group(2) or "").strip()
    start_local = parse_local_datetime_input(left_raw)
    end_local = parse_local_datetime_input(right_raw)
    if start_local is None or end_local is None:
        return None

    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)
    if end_utc <= start_utc:
        return None

    return start_utc, end_utc


def load_jsonl_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []

    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            payload = line.strip()
            if not payload:
                continue
            with contextlib.suppress(json.JSONDecodeError):
                parsed = json.loads(payload)
                if isinstance(parsed, dict):
                    rows.append(parsed)
    return rows


def resolve_backtest_log_path(config: AppConfig) -> Path:
    raw_value = str(config.telegram.backtest.decisions_log_file).strip()
    if not raw_value:
        raw_value = "logs/backtest_decisions.log"
    return resolve_output_path(raw_value)


def format_backtest_event_line(event: dict[str, object]) -> str:
    event_type = str(event.get("event") or "unknown").strip().lower()
    time_utc = str(event.get("time_utc") or "")
    time_local = format_local_datetime(time_utc) if time_utc else "unknown"

    if event_type == "trend_changed":
        trend = str(event.get("trend") or "").strip().lower()
        return f"[{time_local}] trend_changed -> {trend or 'unknown'}"

    if event_type == "scenario_created":
        scenario_id = str(event.get("scenario_id") or "").strip()
        scenario_type = str(event.get("scenario_type") or "").strip()
        direction = str(event.get("direction") or "").strip()
        return (
            f"[{time_local}] scenario_created id={scenario_id or 'n/a'} "
            f"type={scenario_type or 'n/a'} direction={direction or 'n/a'}"
        )

    if event_type == "scenario_expired":
        scenario_id = str(event.get("scenario_id") or "").strip()
        return f"[{time_local}] scenario_expired id={scenario_id or 'n/a'}"

    if event_type == "symbol_skipped":
        reason = str(event.get("reason") or "").strip()
        m5_bars = str(event.get("m5_bars") or "")
        h1_bars = str(event.get("h1_bars") or "")
        return (
            f"[{time_local}] symbol_skipped reason={reason or 'n/a'} "
            f"m5_bars={m5_bars or 'n/a'} h1_bars={h1_bars or 'n/a'}"
        )

    if event_type == "h1_aggregated_from_m5":
        h1_bars = str(event.get("h1_bars") or "")
        return f"[{time_local}] h1_aggregated_from_m5 h1_bars={h1_bars or 'n/a'}"

    return f"[{time_local}] {event_type}"


def write_backtest_decision_log(
    config: AppConfig,
    *,
    user_id: int,
    asset: str,
    start_utc: datetime,
    end_utc: datetime,
    report: object,
    proposals: list[dict[str, object]],
    events: list[dict[str, object]],
) -> Path:
    path = resolve_backtest_log_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)

    run_id = str(getattr(report, "run_id", "n/a"))
    output_dir = str(getattr(report, "output_dir", ""))
    steps_processed = int(getattr(report, "steps_processed", 0) or 0)
    proposals_created = int(getattr(report, "proposals_created", 0) or 0)
    scenarios_expired = int(getattr(report, "scenarios_expired", 0) or 0)
    events_written = int(getattr(report, "events_written", 0) or 0)

    lines: list[str] = []
    lines.append("=" * 100)
    lines.append(f"created_at_utc={datetime.now(timezone.utc).isoformat()}")
    lines.append(f"run_id={run_id}")
    lines.append(f"user_id={user_id}")
    lines.append(f"asset={asset}")
    lines.append(f"interval_utc={start_utc.isoformat()} -> {end_utc.isoformat()}")
    lines.append(
        "interval_local="
        f"{format_local_datetime(start_utc.isoformat())} -> "
        f"{format_local_datetime(end_utc.isoformat())}"
    )
    lines.append(f"output_dir={output_dir}")
    lines.append(
        f"summary: steps={steps_processed} proposals={proposals_created} "
        f"expired={scenarios_expired} events={events_written}"
    )

    if len(proposals) > 0:
        lines.append("proposals:")
        for idx, proposal in enumerate(proposals, start=1):
            scenario_id = str(proposal.get("scenario_id") or "").strip() or "n/a"
            scenario_type = str(proposal.get("scenario_type") or "").strip() or "n/a"
            direction = str(proposal.get("direction") or "").strip() or "n/a"
            created_at = str(proposal.get("created_at_utc") or "")
            lines.append(
                f"  {idx}. id={scenario_id} type={scenario_type} "
                f"direction={direction} created={created_at or 'n/a'}"
            )

    if len(events) > 0:
        lines.append("events:")
        for event in events:
            lines.append(f"  {format_backtest_event_line(event)}")

    lines.append("")

    with path.open("a", encoding="utf-8") as file:
        file.write("\n".join(lines))

    return path


def run_backtest_for_asset_sync(
    config: AppConfig,
    *,
    asset: str,
    start_utc: datetime,
    end_utc: datetime,
    warmup_bars: int,
) -> tuple[object, list[dict[str, object]], list[dict[str, object]]]:
    from auto_eye.backtest_service import BacktestScenarioRunner
    from auto_eye.detectors.registry import build_detectors

    detectors = build_detectors(config.auto_eye.elements)
    runner = BacktestScenarioRunner(config=config, detectors=detectors)
    report = runner.run(
        start_time_utc=start_utc,
        end_time_utc=end_utc,
        symbols=[asset],
        warmup_bars=warmup_bars,
    )
    proposals = load_jsonl_rows(report.output_dir / "proposals.jsonl")
    events = load_jsonl_rows(report.output_dir / "events.jsonl")
    return report, proposals, events


def format_backtest_price(value: object) -> str:
    parsed = parse_auto_eye_float(value)
    if parsed is None:
        return "n/a"
    return format_target(parsed)


def render_backtest_proposal_text(
    proposal: dict[str, object],
    *,
    index: int,
    total: int,
) -> str:
    scenario_type = str(proposal.get("scenario_type") or "").strip().lower()
    scenario_type_label = {
        "trend_continuation": "Продолжение тренда",
        "reversal_at_opposite": "Разворот от противоположной области",
    }.get(scenario_type, scenario_type or "unknown")

    created_at = str(proposal.get("created_at_utc") or "")
    symbol = str(proposal.get("symbol") or "")
    direction = str(proposal.get("direction") or "")
    scenario_id = str(proposal.get("scenario_id") or "")

    entry = proposal.get("entry") if isinstance(proposal.get("entry"), dict) else {}
    sl = proposal.get("sl") if isinstance(proposal.get("sl"), dict) else {}
    tp = proposal.get("tp") if isinstance(proposal.get("tp"), dict) else None

    lines = [
        f"<b>Сценарий {index}/{total}</b>",
        f"<b>Актив:</b> <code>{html.escape(symbol)}</code>",
        f"<b>Тип:</b> <b>{html.escape(scenario_type_label)}</b>",
        f"<b>Направление:</b> <b>{html.escape(direction or 'n/a')}</b>",
        f"<b>Время:</b> <b>{html.escape(format_local_datetime(created_at))}</b>",
        f"<b>Entry:</b> <b>{html.escape(format_backtest_price(entry.get('price')))}</b>",
        f"<b>SL:</b> <b>{html.escape(format_backtest_price(sl.get('price')))}</b>",
    ]

    if isinstance(tp, dict):
        lines.append(
            f"<b>TP:</b> <b>{html.escape(format_backtest_price(tp.get('price')))}</b>"
        )
    else:
        lines.append("<b>TP:</b> <b>n/a</b>")

    if scenario_id:
        lines.append(f"<b>ID:</b> <code>{html.escape(scenario_id)}</code>")

    return "\n".join(lines)


async def _safe_send_backtest_message(
    bot: Bot,
    *,
    user_id: int,
    text: str,
    context: str,
) -> bool:
    try:
        await bot.send_message(chat_id=user_id, text=text)
        return True
    except Exception:
        logger.warning(
            "Backtest notification send failed user_id=%s context=%s",
            user_id,
            context,
            exc_info=True,
        )
        return False


async def execute_backtest_and_notify(
    bot: Bot,
    state: BotState,
    *,
    user_id: int,
    asset: str,
    start_utc: datetime,
    end_utc: datetime,
) -> None:
    backtest_cfg = state.config.telegram.backtest
    send_failed = False

    async def send_text(text: str, *, context: str) -> None:
        nonlocal send_failed
        ok = await _safe_send_backtest_message(
            bot,
            user_id=user_id,
            text=text,
            context=context,
        )
        if not ok:
            send_failed = True

    try:
        await send_text(
            (
                "<b>Бектест запущен</b>\n"
                f"<b>Актив:</b> <code>{html.escape(asset)}</code>\n"
                f"<b>Период:</b> <b>{html.escape(format_local_datetime(start_utc.isoformat()))}</b>"
                " - "
                f"<b>{html.escape(format_local_datetime(end_utc.isoformat()))}</b>"
            ),
            context="start",
        )

        async with state.scrape_lock:
            report, proposals, events = await asyncio.to_thread(
                run_backtest_for_asset_sync,
                state.config,
                asset=asset,
                start_utc=start_utc,
                end_utc=end_utc,
                warmup_bars=backtest_cfg.warmup_bars,
            )

        log_path = await asyncio.to_thread(
            write_backtest_decision_log,
            state.config,
            user_id=user_id,
            asset=asset,
            start_utc=start_utc,
            end_utc=end_utc,
            report=report,
            proposals=proposals,
            events=events,
        )
    except Exception:
        logger.exception(
            "Backtest task failed user_id=%s asset=%s start=%s end=%s",
            user_id,
            asset,
            start_utc.isoformat(),
            end_utc.isoformat(),
        )
        await send_text(
            (
                "<b>Бектест завершился с ошибкой.</b>\n"
                "Проверьте логи и параметры периода."
            ),
            context="run_failed",
        )
        return
    finally:
        current = state.backtest_tasks.get(user_id)
        if current is asyncio.current_task():
            state.backtest_tasks.pop(user_id, None)

    proposals.sort(key=lambda row: str(row.get("created_at_utc") or ""))
    total = len(proposals)
    max_send = max(1, backtest_cfg.max_proposals_to_send)
    send_rows = proposals[:max_send]

    if total == 0:
        await send_text(
            (
                "<b>Бектест завершен</b>\n"
                f"<code>{html.escape(asset)}</code>: возможных сценариев не найдено.\n"
                f"<b>Лог решений:</b> <code>{html.escape(str(log_path))}</code>"
            ),
            context="no_proposals",
        )
        if send_failed:
            logger.warning(
                "Backtest completed with notification delivery issues user_id=%s asset=%s run_id=%s",
                user_id,
                asset,
                getattr(report, "run_id", "n/a"),
            )
        return

    for idx, row in enumerate(send_rows, start=1):
        await send_text(
            render_backtest_proposal_text(row, index=idx, total=total),
            context=f"proposal_{idx}",
        )

    omitted = total - len(send_rows)
    summary = (
        "<b>Бектест завершен</b>\n"
        f"<b>Run ID:</b> <code>{html.escape(str(getattr(report, 'run_id', 'n/a')))}</code>\n"
        f"<b>Найдено сценариев:</b> <b>{total}</b>\n"
        f"<b>Лог решений:</b> <code>{html.escape(str(log_path))}</code>"
    )
    if omitted > 0:
        summary += f"\n<b>Показано:</b> {len(send_rows)} (скрыто: {omitted})"

    await send_text(summary, context="summary")

    if send_failed:
        logger.warning(
            "Backtest completed with notification delivery issues user_id=%s asset=%s run_id=%s",
            user_id,
            asset,
            getattr(report, "run_id", "n/a"),
        )


async def start_backtest_task(
    bot: Bot,
    state: BotState,
    *,
    user_id: int,
    asset: str,
    start_utc: datetime,
    end_utc: datetime,
) -> tuple[bool, str]:
    if not is_backtest_user_allowed(state, user_id):
        return False, "Доступ к бектесту не разрешен."

    existing = state.backtest_tasks.get(user_id)
    if existing is not None and not existing.done():
        return False, "Бектест уже выполняется. Дождитесь завершения текущего запуска."

    normalized_asset = asset.strip().upper()
    available_assets = set(get_backtest_assets(state.config, state.last_quotes))
    if normalized_asset not in available_assets:
        return False, "Этот актив недоступен для бектеста."

    now_utc = datetime.now(timezone.utc)
    if start_utc >= now_utc:
        return False, "Начало интервала должно быть в прошлом."
    if end_utc > now_utc + timedelta(minutes=1):
        return False, "Конец интервала не должен быть в будущем."

    max_hours = max(1, int(state.config.telegram.backtest.max_interval_hours))
    duration_hours = (end_utc - start_utc).total_seconds() / 3600.0
    if duration_hours > max_hours:
        return (
            False,
            f"Интервал слишком большой. Максимум: {max_hours} ч.",
        )

    task = asyncio.create_task(
        execute_backtest_and_notify(
            bot,
            state,
            user_id=user_id,
            asset=normalized_asset,
            start_utc=start_utc,
            end_utc=end_utc,
        )
    )
    state.backtest_tasks[user_id] = task
    return True, "Бектест запущен. Результаты придут отдельными сообщениями."


async def refresh_quotes_and_alerts(
    bot: Bot,
    state: BotState,
    *,
    process_alerts: bool,
) -> QuotesMap:
    logger.info("Refreshing quotes (process_alerts=%s)", process_alerts)

    triggered: list[TriggeredAlert] = []
    async with state.scrape_lock:
        previous_quotes = state.last_quotes
        quotes = await asyncio.to_thread(collect_quotes, state.config, False)
        await asyncio.to_thread(save_quotes, state.config, quotes)

        if process_alerts:
            triggered = state.alert_store.consume_triggered(quotes, previous_quotes)

        state.last_quotes = quotes

    for event in triggered:
        alert = event.alert
        if alert.kind == ALERT_KIND_PRICE:
            text = (
                "<b>Сработал алерт</b>\n"
                "<b>Тип:</b> цена\n"
                f"<b>Актив:</b> <code>{html.escape(alert.asset)}</code>\n"
                f"<b>Условие:</b> {direction_label(alert.direction or '')} "
                f"<b>{format_target(alert.target or 0.0)}</b>\n"
                f"<b>Текущая цена:</b> <b>{html.escape(event.current_value_text)}</b>"
            )
        elif alert.kind == ALERT_KIND_TIME:
            text = (
                "<b>Сработал алерт</b>\n"
                "<b>Тип:</b> время\n"
                f"<b>Актив:</b> <code>{html.escape(alert.asset)}</code>\n"
                f"<b>Запланировано:</b> "
                f"<b>{html.escape(format_local_datetime(alert.trigger_at_utc))}</b>\n"
                f"<b>Текущая цена:</b> <b>{html.escape(event.current_value_text)}</b>"
            )
        else:
            mode = alert.price_time_mode or ""
            mode_text = (
                "удержание"
                if mode == PRICE_TIME_MODE_HOLD
                else "закрытие свечи"
            )
            tf = timeframe_label(alert.timeframe_code or "")
            text = (
                "<b>Сработал алерт</b>\n"
                "<b>Тип:</b> цена + время\n"
                f"<b>Актив:</b> <code>{html.escape(alert.asset)}</code>\n"
                f"<b>Режим:</b> {html.escape(mode_text)} {html.escape(tf)}\n"
                f"<b>Условие:</b> {direction_label(alert.direction or '')} "
                f"<b>{format_target(alert.target or 0.0)}</b>\n"
                f"<b>Текущая цена:</b> <b>{html.escape(event.current_value_text)}</b>"
            )

        try:
            text_with_message = text + format_alert_message_block(alert.message_text)
            await bot.send_message(
                chat_id=alert.user_id,
                text=text_with_message,
                reply_markup=build_extend_keyboard(alert),
            )
            logger.info(
                "Sent triggered alert user_id=%s kind=%s asset=%s current=%s",
                alert.user_id,
                alert.kind,
                alert.asset,
                event.current_value_text,
            )
        except Exception:
            logger.exception(
                "Failed to send alert message to user_id=%s", alert.user_id
            )

    logger.info("Refresh finished, quotes=%s triggered=%s", len(quotes), len(triggered))
    return quotes


async def send_dashboard_message(
    message: Message,
    state: BotState,
    *,
    quotes: QuotesMap | None = None,
) -> None:
    if quotes is None:
        quotes = state.last_quotes

    user_id = get_user_id_from_message(message)
    alerts = state.alert_store.list_for_user(user_id)
    text = render_dashboard_text(
        state.config,
        quotes,
    )
    sent = await message.answer(
        text=text,
        reply_markup=build_home_keyboard(
            has_alerts=bool(alerts),
            has_backtest=is_backtest_user_allowed(state, user_id),
        ),
    )
    state.dashboard_message_ids[user_id] = (sent.chat.id, sent.message_id)


async def send_alerts_menu_message(message: Message, state: BotState) -> None:
    user_id = get_user_id_from_message(message)
    chat_alerts = state.alert_store.list_for_user(user_id)
    assets_for_menu = get_display_assets(state.config, state.last_quotes)

    await message.answer(
        text=render_alerts_menu_text(chat_alerts),
        reply_markup=build_alerts_menu_keyboard(assets_for_menu),
    )


async def send_backtest_assets_menu_message(message: Message, state: BotState) -> None:
    assets = get_backtest_assets(state.config, state.last_quotes)
    await message.answer(
        text=render_backtest_assets_menu_text(),
        reply_markup=build_backtest_assets_keyboard(assets),
    )


async def send_backtest_period_menu_message(
    message: Message,
    state: BotState,
    asset: str,
) -> None:
    _ = state
    await message.answer(
        text=render_backtest_period_menu_text(asset),
        reply_markup=build_backtest_period_keyboard(asset),
    )


async def edit_backtest_assets_menu_message(query: CallbackQuery, state: BotState) -> None:
    assets = get_backtest_assets(state.config, state.last_quotes)
    await safe_edit_message(
        query,
        text=render_backtest_assets_menu_text(),
        reply_markup=build_backtest_assets_keyboard(assets),
    )


async def edit_backtest_period_menu_message(
    query: CallbackQuery,
    state: BotState,
    asset: str,
) -> None:
    _ = state
    await safe_edit_message(
        query,
        text=render_backtest_period_menu_text(asset),
        reply_markup=build_backtest_period_keyboard(asset),
    )


async def send_asset_alert_message(message: Message, state: BotState, asset: str) -> None:
    user_id = get_user_id_from_message(message)
    asset_alerts = state.alert_store.list_for_user_asset(user_id, asset)
    await message.answer(
        text=render_asset_menu_text(asset, asset_alerts),
        reply_markup=build_asset_alert_keyboard(asset, asset_alerts),
    )


async def edit_dashboard_message(
    query: CallbackQuery,
    state: BotState,
    *,
    quotes: QuotesMap | None = None,
) -> None:
    if quotes is None:
        quotes = state.last_quotes

    user_id = get_user_id_from_query(query)
    alerts = state.alert_store.list_for_user(user_id)
    text = render_dashboard_text(
        state.config,
        quotes,
    )
    await safe_edit_message(
        query,
        text=text,
        reply_markup=build_home_keyboard(
            has_alerts=bool(alerts),
            has_backtest=is_backtest_user_allowed(state, user_id),
        ),
    )
    if query.message is not None:
        state.dashboard_message_ids[user_id] = (
            query.message.chat.id,
            query.message.message_id,
        )


async def edit_alerts_menu_message(query: CallbackQuery, state: BotState) -> None:
    user_id = get_user_id_from_query(query)
    chat_alerts = state.alert_store.list_for_user(user_id)
    text = render_alerts_menu_text(chat_alerts)
    assets_for_menu = get_display_assets(state.config, state.last_quotes)
    await safe_edit_message(
        query,
        text=text,
        reply_markup=build_alerts_menu_keyboard(assets_for_menu),
    )


async def edit_delete_menu_message(query: CallbackQuery, state: BotState) -> None:
    user_id = get_user_id_from_query(query)
    alerts = state.alert_store.list_for_user(user_id)
    await safe_edit_message(
        query,
        text=render_delete_menu_text(alerts),
        reply_markup=build_delete_alerts_keyboard(alerts),
    )


async def edit_asset_alert_message(query: CallbackQuery, state: BotState, asset: str) -> None:
    user_id = get_user_id_from_query(query)
    asset_alerts = state.alert_store.list_for_user_asset(user_id, asset)
    await safe_edit_message(
        query,
        text=render_asset_menu_text(asset, asset_alerts),
        reply_markup=build_asset_alert_keyboard(asset, asset_alerts),
    )


def get_asset_delete_selection(
    state: BotState,
    user_id: int,
    asset: str,
) -> tuple[list[AlertRule], set[str]]:
    asset_alerts = state.alert_store.list_for_user_asset(user_id, asset)
    valid_selectors = {build_alert_selector(alert) for alert in asset_alerts}

    existing = state.asset_delete_selection.get(user_id)
    selected: set[str] = set()
    if existing is not None and existing.asset == asset:
        selected = {item for item in existing.selected_selectors if item in valid_selectors}

    state.asset_delete_selection[user_id] = AssetDeleteSelectionState(
        asset=asset,
        selected_selectors=selected,
    )
    return asset_alerts, selected


async def edit_asset_delete_select_message(query: CallbackQuery, state: BotState, asset: str) -> None:
    user_id = get_user_id_from_query(query)
    asset_alerts, selected = get_asset_delete_selection(state, user_id, asset)
    await safe_edit_message(
        query,
        text=render_asset_delete_select_text(asset, asset_alerts, len(selected)),
        reply_markup=build_asset_delete_select_keyboard(asset, asset_alerts, selected),
    )


async def edit_asset_edit_select_message(query: CallbackQuery, state: BotState, asset: str) -> None:
    user_id = get_user_id_from_query(query)
    asset_alerts = state.alert_store.list_for_user_asset(user_id, asset)
    await safe_edit_message(
        query,
        text=render_edit_alert_select_text(asset, asset_alerts),
        reply_markup=build_edit_alert_select_keyboard(asset, asset_alerts),
    )


async def edit_alert_edit_session_message(query: CallbackQuery, state: BotState, user_id: int) -> None:
    session = state.alert_edit_sessions.get(user_id)
    if session is None:
        await edit_alerts_menu_message(query, state)
        return

    set_pending_for_edit_step(state, user_id, session)
    await safe_edit_message(
        query,
        text=render_edit_session_text(session),
        reply_markup=build_edit_session_keyboard(session),
    )


async def send_alert_edit_session_message(message: Message, state: BotState, user_id: int) -> None:
    session = state.alert_edit_sessions.get(user_id)
    if session is None:
        await send_alerts_menu_message(message, state)
        return

    set_pending_for_edit_step(state, user_id, session)
    await message.answer(
        text=render_edit_session_text(session),
        reply_markup=build_edit_session_keyboard(session),
    )


async def continue_alert_edit_flow_query(
    query: CallbackQuery,
    state: BotState,
    user_id: int,
) -> None:
    session = state.alert_edit_sessions.get(user_id)
    if session is None:
        await edit_alerts_menu_message(query, state)
        return

    if str(session.get("step") or "") == "review":
        success, msg, asset = apply_edit_session(state, user_id)
        await query.answer(msg, show_alert=False)
        if success:
            await edit_asset_alert_message(query, state, asset)
            return
        current = state.alert_edit_sessions.get(user_id)
        if current is not None and str(current.get("target_type") or "") == EDIT_TYPE_TIME_CUSTOM:
            set_edit_step(current, "input_time", "trigger_at_utc")
        await edit_alert_edit_session_message(query, state, user_id)
        return

    await edit_alert_edit_session_message(query, state, user_id)


async def continue_alert_edit_flow_message(
    message: Message,
    state: BotState,
    user_id: int,
) -> None:
    session = state.alert_edit_sessions.get(user_id)
    if session is None:
        await send_alerts_menu_message(message, state)
        return

    if str(session.get("step") or "") == "review":
        success, msg, asset = apply_edit_session(state, user_id)
        await message.answer(msg)
        if success:
            await send_asset_alert_message(message, state, asset)
            return
        current = state.alert_edit_sessions.get(user_id)
        if current is not None and str(current.get("target_type") or "") == EDIT_TYPE_TIME_CUSTOM:
            set_edit_step(current, "input_time", "trigger_at_utc")
        await send_alert_edit_session_message(message, state, user_id)
        return

    await send_alert_edit_session_message(message, state, user_id)


def build_router(state: BotState) -> Router:
    router = Router()

    @router.message(CommandStart())
    async def start_handler(message: Message) -> None:
        if not await ensure_message_allowed(state, message):
            return

        user_id = get_user_id_from_message(message)
        logger.info("/start from user_id=%s chat_id=%s", user_id, message.chat.id)

        quotes = state.last_quotes
        if not quotes:
            try:
                quotes = await refresh_quotes_and_alerts(
                    message.bot, state, process_alerts=False
                )
            except Exception:
                logger.exception("Quote refresh failed on /start")
                await message.answer("<b>Не удалось загрузить котировки.</b>")
                return

        await send_dashboard_message(message, state, quotes=quotes)

    @router.message(Command("backtest"))
    async def backtest_command_handler(message: Message) -> None:
        if not await ensure_message_allowed(state, message):
            return

        user_id = get_user_id_from_message(message)
        if not is_backtest_user_allowed(state, user_id):
            await message.answer("Доступ к бектесту не разрешен.")
            return

        state.pending_inputs.pop(user_id, None)
        await send_backtest_assets_menu_message(message, state)
    @router.callback_query(F.data == CALLBACK_NOOP)
    async def noop_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return
        await query.answer()

    @router.callback_query(F.data == CALLBACK_MENU_BACKTEST)
    async def menu_backtest_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        if not is_backtest_user_allowed(state, user_id):
            await query.answer("Нет доступа к бектесту", show_alert=False)
            return

        state.pending_inputs.pop(user_id, None)
        await query.answer()
        await edit_backtest_assets_menu_message(query, state)

    @router.callback_query(F.data.startswith(CALLBACK_BACKTEST_ASSET_PREFIX))
    async def backtest_asset_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        if not is_backtest_user_allowed(state, user_id):
            await query.answer("Нет доступа к бектесту", show_alert=False)
            return

        data = str(query.data or "")
        asset = data.removeprefix(CALLBACK_BACKTEST_ASSET_PREFIX).strip().upper()
        if not asset:
            await query.answer()
            await edit_backtest_assets_menu_message(query, state)
            return

        state.pending_inputs.pop(user_id, None)
        await query.answer()
        await edit_backtest_period_menu_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_BACKTEST_BACK_PREFIX))
    async def backtest_back_to_period_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        if not is_backtest_user_allowed(state, user_id):
            await query.answer("Нет доступа к бектесту", show_alert=False)
            return

        data = str(query.data or "")
        asset = data.removeprefix(CALLBACK_BACKTEST_BACK_PREFIX).strip().upper()
        if not asset:
            await query.answer()
            await edit_backtest_assets_menu_message(query, state)
            return

        state.pending_inputs.pop(user_id, None)
        await query.answer()
        await edit_backtest_period_menu_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_BACKTEST_RANGE_PREFIX))
    async def backtest_quick_range_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        if not is_backtest_user_allowed(state, user_id):
            await query.answer("Нет доступа к бектесту", show_alert=False)
            return

        data = str(query.data or "")
        payload = data.removeprefix(CALLBACK_BACKTEST_RANGE_PREFIX)
        parts = payload.split("|", maxsplit=1)
        if len(parts) != 2:
            await query.answer()
            await edit_backtest_assets_menu_message(query, state)
            return

        asset = parts[0].strip().upper()
        try:
            hours = int(parts[1])
        except ValueError:
            await query.answer()
            await edit_backtest_period_menu_message(query, state, asset)
            return

        if not asset or hours <= 0:
            await query.answer()
            await edit_backtest_assets_menu_message(query, state)
            return

        end_utc = datetime.now(timezone.utc)
        start_utc = end_utc - timedelta(hours=hours)
        ok, response_text = await start_backtest_task(
            query.bot,
            state,
            user_id=user_id,
            asset=asset,
            start_utc=start_utc,
            end_utc=end_utc,
        )
        await query.answer(response_text if not ok else "Бектест запущен", show_alert=not ok)
        if ok:
            await edit_backtest_period_menu_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_BACKTEST_CUSTOM_PREFIX))
    async def backtest_custom_range_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        if not is_backtest_user_allowed(state, user_id):
            await query.answer("Нет доступа к бектесту", show_alert=False)
            return

        data = str(query.data or "")
        asset = data.removeprefix(CALLBACK_BACKTEST_CUSTOM_PREFIX).strip().upper()
        if not asset:
            await query.answer()
            await edit_backtest_assets_menu_message(query, state)
            return

        state.pending_inputs[user_id] = {
            "type": "backtest_interval_input",
            "asset": asset,
        }

        await query.answer()
        await safe_edit_message(
            query,
            text=(
                f"<b>Бектест: {html.escape(asset)}</b>\n"
                "Введите интервал в <b>GMT+5</b>:\n"
                "<code>dd.mm.yyyy hh:mm - dd.mm.yyyy hh:mm</code>\n"
                "или\n"
                "<code>yyyy-mm-dd hh:mm - yyyy-mm-dd hh:mm</code>"
            ),
            reply_markup=build_backtest_input_keyboard(asset),
        )

    @router.callback_query(F.data == CALLBACK_BACKTEST_CANCEL)
    async def backtest_cancel_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        state.pending_inputs.pop(user_id, None)
        await query.answer()
        await edit_dashboard_message(query, state)
    @router.callback_query(F.data == CALLBACK_REFRESH)
    async def refresh_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        logger.info("Manual refresh requested by user_id=%s", user_id)
        await query.answer()

        try:
            quotes = await refresh_quotes_and_alerts(query.bot, state, process_alerts=True)
            await send_auto_eye_notifications(query.bot, state)
        except Exception:
            logger.exception("Manual refresh failed")
            user_id = get_user_id_from_query(query)
            has_alerts = bool(state.alert_store.list_for_user(user_id))
            await safe_edit_message(
                query,
                text="<b>Не удалось обновить котировки.</b>",
                reply_markup=build_home_keyboard(
                    has_alerts=has_alerts,
                    has_backtest=is_backtest_user_allowed(state, user_id),
                ),
            )
            return

        await edit_dashboard_message(query, state, quotes=quotes)

    @router.callback_query(F.data == CALLBACK_ALERT_DELETE_MESSAGE)
    async def alert_delete_message_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        if query.message is None:
            await query.answer()
            return

        with contextlib.suppress(TelegramBadRequest):
            await query.message.delete()
        await query.answer("Сообщение удалено")

    @router.callback_query(F.data == CALLBACK_MENU_HOME)
    async def menu_home_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        logger.info("Home menu opened by user_id=%s", get_user_id_from_query(query))
        await query.answer()
        user_id = get_user_id_from_query(query)
        state.pending_inputs.pop(user_id, None)
        state.asset_delete_selection.pop(user_id, None)
        state.alert_edit_sessions.pop(user_id, None)
        await edit_dashboard_message(query, state)

    @router.callback_query(F.data == CALLBACK_MENU_ALERTS)
    async def menu_alerts_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        logger.info("Alerts menu opened by user_id=%s", get_user_id_from_query(query))
        await query.answer()
        user_id = get_user_id_from_query(query)
        state.pending_inputs.pop(user_id, None)
        state.asset_delete_selection.pop(user_id, None)
        state.alert_edit_sessions.pop(user_id, None)
        await edit_alerts_menu_message(query, state)

    @router.callback_query(F.data == CALLBACK_MENU_DELETE)
    async def menu_delete_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        logger.info("Delete menu opened by user_id=%s", get_user_id_from_query(query))
        await query.answer()
        user_id = get_user_id_from_query(query)
        state.pending_inputs.pop(user_id, None)
        state.asset_delete_selection.pop(user_id, None)
        state.alert_edit_sessions.pop(user_id, None)
        await edit_delete_menu_message(query, state)

    @router.callback_query(F.data == CALLBACK_CANCEL)
    async def cancel_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        logger.info("Cancel input by user_id=%s", user_id)
        await query.answer()
        waiting = state.pending_inputs.pop(user_id, None)
        state.asset_delete_selection.pop(user_id, None)
        state.alert_edit_sessions.pop(user_id, None)
        if waiting is not None and waiting.get("asset"):
            await edit_asset_alert_message(query, state, waiting["asset"])
            return
        await edit_alerts_menu_message(query, state)

    @router.callback_query(F.data.startswith(CALLBACK_BACK_ASSET_PREFIX))
    async def back_asset_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()
        user_id = get_user_id_from_query(query)
        state.pending_inputs.pop(user_id, None)
        state.asset_delete_selection.pop(user_id, None)
        state.alert_edit_sessions.pop(user_id, None)
        data = query.data or ""
        asset = data[len(CALLBACK_BACK_ASSET_PREFIX) :]
        if not asset:
            await edit_alerts_menu_message(query, state)
            return
        await edit_asset_alert_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_ALERT_ASSET_PREFIX))
    async def alert_asset_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()

        data = query.data or ""
        asset = data[len(CALLBACK_ALERT_ASSET_PREFIX) :]
        user_id = get_user_id_from_query(query)
        state.asset_delete_selection.pop(user_id, None)
        state.alert_edit_sessions.pop(user_id, None)
        logger.info("Asset menu opened user_id=%s asset=%s", get_user_id_from_query(query), asset)
        await edit_asset_alert_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_EDIT_ALERT_MENU_PREFIX))
    async def edit_alert_menu_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        data = query.data or ""
        asset = data[len(CALLBACK_EDIT_ALERT_MENU_PREFIX) :]
        if not asset:
            await query.answer("Некорректные данные", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        user_id = get_user_id_from_query(query)
        state.pending_inputs.pop(user_id, None)
        state.asset_delete_selection.pop(user_id, None)
        state.alert_edit_sessions.pop(user_id, None)

        asset_alerts = state.alert_store.list_for_user_asset(user_id, asset)
        if not asset_alerts:
            await query.answer("Нет активных алертов", show_alert=False)
            await edit_asset_alert_message(query, state, asset)
            return

        await query.answer()
        await edit_asset_edit_select_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_EDIT_ALERT_PICK_PREFIX))
    async def edit_alert_pick_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        data = query.data or ""
        selector = data[len(CALLBACK_EDIT_ALERT_PICK_PREFIX) :]
        user_id = get_user_id_from_query(query)
        alert = find_user_alert_by_selector(state, user_id, selector)
        parsed = parse_alert_selector(selector)
        if alert is None or parsed is None:
            await query.answer("Алерт не найден", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        asset, _, _ = parsed
        state.alert_edit_sessions[user_id] = {
            "asset": asset,
            "selector": selector,
            "original_alert": alert,
            "target_type": "",
            "direction": None,
            "target": None,
            "timeframe_code": None,
            "trigger_at_utc": None,
            "message": None,
            "step": "choose_type",
            "field": "",
            "history": [],
        }
        state.pending_inputs.pop(user_id, None)
        await query.answer()
        await continue_alert_edit_flow_query(query, state, user_id)

    @router.callback_query(F.data.startswith(CALLBACK_EDIT_TYPE_PREFIX))
    async def edit_alert_type_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        session = state.alert_edit_sessions.get(user_id)
        if session is None:
            await query.answer("Сессия редактирования не найдена", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        edit_type = (query.data or "")[len(CALLBACK_EDIT_TYPE_PREFIX) :]
        if edit_type not in {
            EDIT_TYPE_PRICE_CROSS,
            EDIT_TYPE_PRICE_HOLD,
            EDIT_TYPE_PRICE_CANDLE,
            EDIT_TYPE_TIME_CANDLE,
            EDIT_TYPE_TIME_CUSTOM,
        }:
            await query.answer("Некорректный тип", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        push_edit_session_history(session)
        session["target_type"] = edit_type
        session["direction"] = None
        session["target"] = None
        session["timeframe_code"] = None
        session["trigger_at_utc"] = None
        session["message"] = None
        advance_edit_session_step(session)
        await query.answer()
        await continue_alert_edit_flow_query(query, state, user_id)

    @router.callback_query(F.data.startswith(CALLBACK_EDIT_KEEP_PREFIX))
    async def edit_alert_keep_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        session = state.alert_edit_sessions.get(user_id)
        if session is None:
            await query.answer("Сессия редактирования не найдена", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        field = (query.data or "")[len(CALLBACK_EDIT_KEEP_PREFIX) :]
        if str(session.get("step") or "") != "ask_keep_change" or str(session.get("field") or "") != field:
            await query.answer("Шаг устарел", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        original_alert = session.get("original_alert")
        edit_type = str(session.get("target_type") or "")
        if not isinstance(original_alert, AlertRule):
            await query.answer("Ошибка данных", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        original_value = get_original_edit_field_value(original_alert, edit_type, field)
        if original_value is None:
            await query.answer("Нечего сохранять", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        push_edit_session_history(session)
        if field == "direction":
            session["direction"] = str(original_value)
        elif field == "target":
            session["target"] = float(original_value)
        elif field == "timeframe":
            session["timeframe_code"] = str(original_value)
        elif field == "trigger_at_utc":
            session["trigger_at_utc"] = str(original_value)
        elif field == "message":
            session["message"] = str(original_value)

        advance_edit_session_step(session)
        await query.answer()
        await continue_alert_edit_flow_query(query, state, user_id)

    @router.callback_query(F.data.startswith(CALLBACK_EDIT_CHANGE_PREFIX))
    async def edit_alert_change_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        session = state.alert_edit_sessions.get(user_id)
        if session is None:
            await query.answer("Сессия редактирования не найдена", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        field = (query.data or "")[len(CALLBACK_EDIT_CHANGE_PREFIX) :]
        if str(session.get("step") or "") != "ask_keep_change" or str(session.get("field") or "") != field:
            await query.answer("Шаг устарел", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        push_edit_session_history(session)
        set_edit_step(session, choose_edit_input_step(field), field)
        await query.answer()
        await continue_alert_edit_flow_query(query, state, user_id)

    @router.callback_query(F.data.startswith(CALLBACK_EDIT_SET_DIR_PREFIX))
    async def edit_alert_set_direction_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        session = state.alert_edit_sessions.get(user_id)
        if session is None:
            await query.answer("Сессия редактирования не найдена", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        direction = (query.data or "")[len(CALLBACK_EDIT_SET_DIR_PREFIX) :]
        if direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}:
            await query.answer("Некорректное направление", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        if str(session.get("step") or "") != "choose_direction":
            await query.answer("Шаг устарел", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        push_edit_session_history(session)
        session["direction"] = direction
        advance_edit_session_step(session)
        await query.answer()
        await continue_alert_edit_flow_query(query, state, user_id)

    @router.callback_query(F.data.startswith(CALLBACK_EDIT_SET_TF_PREFIX))
    async def edit_alert_set_timeframe_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        session = state.alert_edit_sessions.get(user_id)
        if session is None:
            await query.answer("Сессия редактирования не найдена", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        timeframe_code = (query.data or "")[len(CALLBACK_EDIT_SET_TF_PREFIX) :].lower()
        options = set(get_edit_timeframe_options(str(session.get("target_type") or "")))
        if timeframe_code not in options:
            await query.answer("Некорректный TF", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        if str(session.get("step") or "") != "choose_timeframe":
            await query.answer("Шаг устарел", show_alert=False)
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        push_edit_session_history(session)
        session["timeframe_code"] = timeframe_code
        advance_edit_session_step(session)
        await query.answer()
        await continue_alert_edit_flow_query(query, state, user_id)

    @router.callback_query(F.data == CALLBACK_EDIT_BACK)
    async def edit_alert_back_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        session = state.alert_edit_sessions.get(user_id)
        if session is None:
            await query.answer()
            await edit_alerts_menu_message(query, state)
            return

        if pop_edit_session_history(session):
            await query.answer()
            await continue_alert_edit_flow_query(query, state, user_id)
            return

        asset = str(session.get("asset") or "")
        state.alert_edit_sessions.pop(user_id, None)
        state.pending_inputs.pop(user_id, None)
        await query.answer()
        if asset:
            await edit_asset_edit_select_message(query, state, asset)
            return
        await edit_alerts_menu_message(query, state)

    @router.callback_query(F.data.startswith(CALLBACK_DELETE_ASSET_PREFIX))
    async def delete_asset_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        data = query.data or ""
        asset = data[len(CALLBACK_DELETE_ASSET_PREFIX) :]
        if not asset:
            await query.answer("Некорректные данные", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        user_id = get_user_id_from_query(query)
        state.alert_edit_sessions.pop(user_id, None)
        asset_alerts = state.alert_store.list_for_user_asset(user_id, asset)
        if not asset_alerts:
            state.asset_delete_selection.pop(user_id, None)
            await query.answer("Активных алертов нет")
            await edit_asset_alert_message(query, state, asset)
            return

        await query.answer()
        logger.info("Asset delete menu opened user_id=%s asset=%s", user_id, asset)
        await edit_asset_delete_select_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_DELETE_ONE_HOME_PREFIX))
    async def delete_one_from_home_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        data = query.data or ""
        payload = data[len(CALLBACK_DELETE_ONE_HOME_PREFIX) :]
        parsed = parse_alert_selector(payload)
        if parsed is None:
            logger.warning("Invalid delete-one-home callback payload: %s", data)
            await query.answer("Некорректные данные")
            await edit_delete_menu_message(query, state)
            return

        asset, kind, created_at_utc = parsed
        user_id = get_user_id_from_query(query)
        deleted = state.alert_store.remove_one(user_id, asset, kind, created_at_utc)
        await query.answer("Удалено" if deleted else "Алерт не найден")
        await edit_delete_menu_message(query, state)

    @router.callback_query(F.data.startswith(CALLBACK_DELETE_ONE_ASSET_PREFIX))
    async def delete_one_from_asset_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        data = query.data or ""
        payload = data[len(CALLBACK_DELETE_ONE_ASSET_PREFIX) :]
        parsed = parse_alert_selector(payload)
        if parsed is None:
            logger.warning("Invalid delete-one-asset callback payload: %s", data)
            await query.answer("Некорректные данные", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        asset, kind, created_at_utc = parsed
        user_id = get_user_id_from_query(query)
        asset_alerts, selected = get_asset_delete_selection(state, user_id, asset)
        valid_selectors = {build_alert_selector(alert) for alert in asset_alerts}
        selector = f"{asset}|{kind}|{created_at_utc}"
        if selector not in valid_selectors:
            await query.answer("Алерт не найден", show_alert=False)
            await edit_asset_delete_select_message(query, state, asset)
            return

        if selector in selected:
            selected.remove(selector)
        else:
            selected.add(selector)

        state.asset_delete_selection[user_id] = AssetDeleteSelectionState(
            asset=asset,
            selected_selectors=selected,
        )
        await query.answer("Обновлено", show_alert=False)
        await edit_asset_delete_select_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_DELETE_APPLY_ASSET_PREFIX))
    async def apply_asset_delete_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        data = query.data or ""
        asset = data[len(CALLBACK_DELETE_APPLY_ASSET_PREFIX) :]
        if not asset:
            await query.answer("Некорректные данные", show_alert=False)
            await edit_alerts_menu_message(query, state)
            return

        user_id = get_user_id_from_query(query)
        existing = state.asset_delete_selection.get(user_id)
        if existing is None or existing.asset != asset:
            await query.answer("Нечего удалять", show_alert=False)
            await edit_asset_alert_message(query, state, asset)
            return

        selected = set(existing.selected_selectors)
        if not selected:
            await query.answer("Ничего не выбрано", show_alert=False)
            await edit_asset_delete_select_message(query, state, asset)
            return

        removed = 0
        for selector in selected:
            parsed = parse_alert_selector(selector)
            if parsed is None:
                continue
            parsed_asset, kind, created_at_utc = parsed
            if parsed_asset != asset:
                continue
            if state.alert_store.remove_one(user_id, parsed_asset, kind, created_at_utc):
                removed += 1

        state.asset_delete_selection.pop(user_id, None)
        logger.info(
            "Selected asset alerts removed user_id=%s asset=%s removed=%s",
            user_id,
            asset,
            removed,
        )
        await query.answer(f"Удалено: {removed}", show_alert=False)
        await edit_asset_alert_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_PRICE_CROSS_MENU_PREFIX))
    async def price_cross_menu_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()
        data = query.data or ""
        asset = data[len(CALLBACK_PRICE_CROSS_MENU_PREFIX) :]
        if not asset:
            await edit_alerts_menu_message(query, state)
            return

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                "Выберите направление пересечения цены."
            ),
            reply_markup=build_price_cross_direction_keyboard(asset),
        )

    @router.callback_query(F.data.startswith(CALLBACK_PRICE_TIME_MENU_PREFIX))
    async def price_time_menu_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()
        data = query.data or ""
        asset = data[len(CALLBACK_PRICE_TIME_MENU_PREFIX) :]
        if not asset:
            await edit_alerts_menu_message(query, state)
            return

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                "Цена + время: выберите режим."
            ),
            reply_markup=build_price_time_mode_keyboard(asset),
        )

    @router.callback_query(F.data.startswith(CALLBACK_PRICE_TIME_CANDLE_MENU_PREFIX))
    async def price_time_candle_menu_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()
        data = query.data or ""
        asset = data[len(CALLBACK_PRICE_TIME_CANDLE_MENU_PREFIX) :]
        if not asset:
            await edit_alerts_menu_message(query, state)
            return

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                "Выберите направление пересечения."
            ),
            reply_markup=build_price_time_direction_keyboard(
                asset,
                back_callback=f"{CALLBACK_PRICE_TIME_MENU_PREFIX}{asset}",
            ),
        )

    @router.callback_query(F.data.startswith(CALLBACK_PRICE_TIME_DIR_PREFIX))
    async def price_time_dir_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()
        data = query.data or ""
        payload = data[len(CALLBACK_PRICE_TIME_DIR_PREFIX) :]
        parts = payload.split("|", maxsplit=1)
        if len(parts) != 2:
            logger.warning("Invalid price-time-dir payload: %s", data)
            await edit_alerts_menu_message(query, state)
            return

        asset, direction = parts
        if direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}:
            logger.warning("Invalid price-time direction payload: %s", data)
            await edit_asset_alert_message(query, state, asset)
            return

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                "Выберите TF свечи для закрытия."
            ),
            reply_markup=build_price_time_tf_keyboard(
                asset,
                direction,
                back_callback=f"{CALLBACK_PRICE_TIME_CANDLE_MENU_PREFIX}{asset}",
            ),
        )

    @router.callback_query(F.data.startswith(CALLBACK_PRICE_TIME_TF_PREFIX))
    async def price_time_tf_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()
        data = query.data or ""
        payload = data[len(CALLBACK_PRICE_TIME_TF_PREFIX) :]
        parts = payload.split("|", maxsplit=2)
        if len(parts) != 3:
            logger.warning("Invalid price-time-tf payload: %s", data)
            await edit_alerts_menu_message(query, state)
            return

        asset, direction, timeframe_code = parts
        if direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}:
            logger.warning("Invalid price-time direction payload: %s", data)
            await edit_asset_alert_message(query, state, asset)
            return
        if not is_supported_candle_timeframe(timeframe_code):
            logger.warning("Invalid price-time timeframe payload: %s", data)
            await edit_asset_alert_message(query, state, asset)
            return

        user_id = get_user_id_from_query(query)
        state.pending_inputs[user_id] = {
            "type": ALERT_KIND_PRICE_TIME,
            "asset": asset,
            "pt_mode": PRICE_TIME_MODE_CANDLE_CLOSE,
            "timeframe_code": timeframe_code,
            "direction": direction,
            "back_callback": f"{CALLBACK_PRICE_TIME_DIR_PREFIX}{asset}|{direction}",
        }

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                f"Закрытие свечи <b>{timeframe_label(timeframe_code)}</b>, "
                f"пересечение <b>{direction_human(direction)}</b>.\n"
                "Введите уровень цены."
            ),
            reply_markup=build_input_step_keyboard(
                asset,
                f"{CALLBACK_PRICE_TIME_DIR_PREFIX}{asset}|{direction}",
            ),
        )

    @router.callback_query(F.data.startswith(CALLBACK_TIME_CANDLE_MENU_PREFIX))
    async def time_candle_menu_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()
        data = query.data or ""
        asset = data[len(CALLBACK_TIME_CANDLE_MENU_PREFIX) :]
        if not asset:
            await edit_alerts_menu_message(query, state)
            return

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                "Выберите TF свечи для таймера."
            ),
            reply_markup=build_time_candle_tf_keyboard(asset),
        )

    @router.callback_query(F.data.startswith(CALLBACK_PRICE_SET_PREFIX))
    async def price_set_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()

        data = query.data or ""
        payload = data[len(CALLBACK_PRICE_SET_PREFIX) :]
        parts = payload.split("|", maxsplit=1)
        if len(parts) != 2:
            logger.warning("Invalid price callback payload: %s", data)
            await edit_alerts_menu_message(query, state)
            return

        asset, direction = parts
        if direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}:
            logger.warning("Invalid price-cross direction payload: %s", data)
            await edit_asset_alert_message(query, state, asset)
            return

        user_id = get_user_id_from_query(query)
        state.pending_inputs[user_id] = {
            "type": ALERT_KIND_PRICE,
            "asset": asset,
            "direction": direction,
            "back_callback": f"{CALLBACK_PRICE_CROSS_MENU_PREFIX}{asset}",
        }

        logger.info(
            "Price alert input started user_id=%s asset=%s direction=%s",
            user_id,
            asset,
            direction,
        )

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                f"Введите уровень цены.\n"
                f"Условие: пересечение <b>{direction_human(direction)}</b> уровня."
            ),
            reply_markup=build_input_step_keyboard(
                asset,
                f"{CALLBACK_PRICE_CROSS_MENU_PREFIX}{asset}",
            ),
        )

    @router.callback_query(F.data.startswith(CALLBACK_TIME_QUICK_PREFIX))
    async def time_quick_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()

        data = query.data or ""
        payload = data[len(CALLBACK_TIME_QUICK_PREFIX) :]
        parts = payload.split("|", maxsplit=1)
        if len(parts) != 2:
            logger.warning("Invalid quick-time callback payload: %s", data)
            await edit_alerts_menu_message(query, state)
            return

        asset, timeframe_code = parts
        if not asset:
            logger.warning("Empty asset in quick-time callback payload: %s", data)
            await edit_alerts_menu_message(query, state)
            return

        if timeframe_code not in {TIMEFRAME_M15, TIMEFRAME_H1, TIMEFRAME_H4}:
            logger.warning("Invalid quick-time timeframe payload: %s", data)
            await edit_asset_alert_message(query, state, asset)
            return

        user_id = get_user_id_from_query(query)
        try:
            trigger_at_utc, delay, group = compute_timeframe_trigger_utc(
                state, asset, timeframe_code
            )
        except ValueError:
            logger.warning("Cannot compute timeframe trigger payload: %s", data)
            await edit_asset_alert_message(query, state, asset)
            return

        state.pending_inputs[user_id] = {
            "type": "alert_message_input",
            "asset": asset,
            "draft_kind": ALERT_KIND_TIME,
            "trigger_at_utc": trigger_at_utc.isoformat(),
            "delay_minutes": delay,
            "back_callback": f"{CALLBACK_TIME_CANDLE_MENU_PREFIX}{asset}",
        }

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                f"Таймер по закрытию свечи <b>{html.escape(timeframe_label(timeframe_code))}</b> "
                f"({html.escape(group)}).\n"
                "Введите сообщение к алерту или <code>-</code>, если сообщение не нужно."
            ),
            reply_markup=build_input_step_keyboard(
                asset,
                f"{CALLBACK_TIME_CANDLE_MENU_PREFIX}{asset}",
            ),
        )

        logger.info(
            "Quick time alert condition saved user_id=%s asset=%s timeframe=%s group=%s delay_minutes=%s trigger_at_utc=%s",
            user_id,
            asset,
            timeframe_code,
            group,
            delay,
            trigger_at_utc.isoformat(),
        )

    @router.callback_query(F.data.startswith(CALLBACK_TIME_CUSTOM_PREFIX))
    async def time_custom_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()

        data = query.data or ""
        asset = data[len(CALLBACK_TIME_CUSTOM_PREFIX) :]
        if not asset:
            logger.warning("Empty asset in custom-time callback payload: %s", data)
            await edit_alerts_menu_message(query, state)
            return

        user_id = get_user_id_from_query(query)

        state.pending_inputs[user_id] = {
            "type": ALERT_KIND_TIME,
            "asset": asset,
            "mode": "custom",
            "back_callback": f"{CALLBACK_BACK_ASSET_PREFIX}{asset}",
        }

        logger.info("Custom time input started user_id=%s asset=%s", user_id, asset)

        await safe_edit_message(
            query,
            text=(
                f"<b>{html.escape(asset)}</b>\n"
                f"Введите время в зоне <b>{html.escape(USER_TIMEZONE_LABEL)}</b>.\n"
                "Форматы:\n"
                "1) <code>HH:MM</code> (например 14:30)\n"
                "2) <code>dd.mm.yyyy HH:MM</code> (например 20.02.2026 14:30)\n"
                "3) <code>yyyy-mm-dd HH:MM</code> (например 2026-02-20 14:30)"
            ),
            reply_markup=build_input_step_keyboard(
                asset,
                f"{CALLBACK_BACK_ASSET_PREFIX}{asset}",
            ),
        )

    @router.message(F.text)
    async def text_handler(message: Message) -> None:
        if not await ensure_message_allowed(state, message):
            return

        user_id = get_user_id_from_message(message)
        waiting = state.pending_inputs.get(user_id)
        if waiting is None:
            return

        input_type = waiting.get("type", "")
        asset = waiting.get("asset", "")
        asset_text = str(asset)

        if input_type == "edit_target_input":
            session = state.alert_edit_sessions.get(user_id)
            if session is None:
                state.pending_inputs.pop(user_id, None)
                await message.answer("Сессия редактирования завершена.")
                await send_alerts_menu_message(message, state)
                return

            target = parse_price(message.text or "")
            if target is None:
                back_callback = str(waiting.get("back_callback") or CALLBACK_EDIT_BACK)
                await message.answer(
                    "Не распознал уровень цены. Пример: <code>1.2456</code>.",
                    reply_markup=build_input_step_keyboard(asset_text, back_callback),
                )
                return

            push_edit_session_history(session)
            session["target"] = target
            advance_edit_session_step(session)
            state.pending_inputs.pop(user_id, None)
            await continue_alert_edit_flow_message(message, state, user_id)
            return

        if input_type == "edit_time_input":
            session = state.alert_edit_sessions.get(user_id)
            if session is None:
                state.pending_inputs.pop(user_id, None)
                await message.answer("Сессия редактирования завершена.")
                await send_alerts_menu_message(message, state)
                return

            parsed = parse_custom_time_to_utc(message.text or "")
            if parsed is None:
                back_callback = str(waiting.get("back_callback") or CALLBACK_EDIT_BACK)
                await message.answer(
                    "Не распознал время.\n"
                    "Используйте: <code>dd.mm.yyyy HH:MM</code> "
                    "(или <code>HH:MM</code>, <code>yyyy-mm-dd HH:MM</code>).",
                    reply_markup=build_input_step_keyboard(asset_text, back_callback),
                )
                return

            trigger_at_utc, _ = parsed
            push_edit_session_history(session)
            session["trigger_at_utc"] = trigger_at_utc.isoformat()
            advance_edit_session_step(session)
            state.pending_inputs.pop(user_id, None)
            await continue_alert_edit_flow_message(message, state, user_id)
            return

        if input_type == "edit_message_input":
            session = state.alert_edit_sessions.get(user_id)
            if session is None:
                state.pending_inputs.pop(user_id, None)
                await message.answer("Сессия редактирования завершена.")
                await send_alerts_menu_message(message, state)
                return

            parsed_message, error_text = parse_user_alert_message_input(message.text or "")
            if error_text:
                back_callback = str(waiting.get("back_callback") or CALLBACK_EDIT_BACK)
                await message.answer(
                    error_text,
                    reply_markup=build_input_step_keyboard(asset_text, back_callback),
                )
                return

            push_edit_session_history(session)
            session["message"] = parsed_message or ""
            advance_edit_session_step(session)
            state.pending_inputs.pop(user_id, None)
            await continue_alert_edit_flow_message(message, state, user_id)
            return

        if input_type == "backtest_interval_input":
            if not is_backtest_user_allowed(state, user_id):
                state.pending_inputs.pop(user_id, None)
                await message.answer("Доступ к бектесту не разрешен.")
                return

            parsed_interval = parse_backtest_interval_input(message.text or "")
            if parsed_interval is None:
                await message.answer(
                    "Не распознал интервал.\n"
                    "Формат: <code>dd.mm.yyyy hh:mm - dd.mm.yyyy hh:mm</code>",
                    reply_markup=build_backtest_input_keyboard(asset_text),
                )
                return

            start_utc, end_utc = parsed_interval
            ok, response_text = await start_backtest_task(
                message.bot,
                state,
                user_id=user_id,
                asset=asset_text,
                start_utc=start_utc,
                end_utc=end_utc,
            )

            if not ok:
                await message.answer(
                    response_text,
                    reply_markup=build_backtest_input_keyboard(asset_text),
                )
                return

            state.pending_inputs.pop(user_id, None)
            await message.answer(response_text)
            await send_backtest_period_menu_message(message, state, asset_text)
            return
        if input_type == ALERT_KIND_PRICE:
            target = parse_price(message.text or "")
            direction = waiting.get("direction", "")
            if target is None or direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}:
                logger.warning(
                    "Invalid price input from user_id=%s text=%s",
                    user_id,
                    message.text,
                )
                back_callback = str(
                    waiting.get("back_callback") or f"{CALLBACK_PRICE_CROSS_MENU_PREFIX}{asset_text}"
                )
                await message.answer(
                    "Не распознал цену. Пример: <code>1.2456</code>",
                    reply_markup=build_input_step_keyboard(asset_text, back_callback),
                )
                return

            logger.info(
                "Price alert condition saved user_id=%s asset=%s direction=%s target=%s",
                user_id,
                asset_text,
                direction,
                target,
            )

            state.pending_inputs[user_id] = {
                "type": "alert_message_input",
                "asset": asset_text,
                "draft_kind": ALERT_KIND_PRICE,
                "direction": str(direction),
                "target": target,
                "back_callback": f"{CALLBACK_PRICE_SET_PREFIX}{asset_text}|{direction}",
            }

            await message.answer(
                "<b>Почти готово</b>\n"
                f"<code>{html.escape(asset_text)}</code>: "
                f"{direction_label(str(direction))} <b>{format_target(target)}</b>\n"
                "Введите сообщение к алерту или <code>-</code>, если сообщение не нужно.",
                reply_markup=build_input_step_keyboard(
                    asset_text,
                    f"{CALLBACK_PRICE_SET_PREFIX}{asset_text}|{direction}",
                ),
            )
            return

        if input_type == ALERT_KIND_PRICE_TIME:
            target = parse_price(message.text or "")
            direction = waiting.get("direction", "")
            mode = waiting.get("pt_mode", "")
            timeframe_code = str(waiting.get("timeframe_code", "")).lower()

            if target is None:
                logger.warning(
                    "Invalid price-time target from user_id=%s text=%s",
                    user_id,
                    message.text,
                )
                back_callback = str(waiting.get("back_callback") or CALLBACK_EDIT_BACK)
                await message.answer(
                    "Не распознал уровень цены. Пример: <code>1.2456</code>.",
                    reply_markup=build_input_step_keyboard(asset_text, back_callback),
                )
                return

            if direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}:
                logger.warning(
                    "Unknown price-time direction in pending input user_id=%s direction=%s",
                    user_id,
                    direction,
                )
                await message.answer("Ошибка настройки алерта. Повторите через меню.")
                state.pending_inputs.pop(user_id, None)
                await send_asset_alert_message(message, state, asset_text)
                return

            if mode != PRICE_TIME_MODE_CANDLE_CLOSE:
                logger.warning(
                    "Unknown price-time mode in pending input user_id=%s mode=%s",
                    user_id,
                    mode,
                )
                await message.answer("Ошибка настройки алерта. Повторите через меню.")
                state.pending_inputs.pop(user_id, None)
                await send_asset_alert_message(message, state, asset_text)
                return

            if not is_supported_candle_timeframe(timeframe_code):
                logger.warning(
                    "Unsupported close timeframe in pending input user_id=%s timeframe=%s",
                    user_id,
                    timeframe_code,
                )
                await message.answer("Ошибка настройки алерта. Повторите создание через меню.")
                state.pending_inputs.pop(user_id, None)
                await send_asset_alert_message(message, state, asset_text)
                return

            logger.info(
                "Price-time alert condition saved user_id=%s asset=%s direction=%s target=%s timeframe=%s",
                user_id,
                asset_text,
                direction,
                target,
                timeframe_code,
            )

            state.pending_inputs[user_id] = {
                "type": "alert_message_input",
                "asset": asset_text,
                "draft_kind": ALERT_KIND_PRICE_TIME,
                "direction": str(direction),
                "target": target,
                "pt_mode": str(mode),
                "timeframe_code": timeframe_code,
                "back_callback": (
                    f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset_text}|{direction}|{timeframe_code}"
                ),
            }

            await message.answer(
                "<b>Почти готово</b>\n"
                f"<code>{html.escape(asset_text)}</code>: закрытие "
                f"<b>{html.escape(timeframe_label(timeframe_code))}</b>, "
                f"условие {direction_label(str(direction))} <b>{format_target(target)}</b>\n"
                "Введите сообщение к алерту или <code>-</code>, если сообщение не нужно.",
                reply_markup=build_input_step_keyboard(
                    asset_text,
                    f"{CALLBACK_PRICE_TIME_TF_PREFIX}{asset_text}|{direction}|{timeframe_code}",
                ),
            )
            return

        if input_type == ALERT_KIND_TIME and waiting.get("mode") == "custom":
            parsed = parse_custom_time_to_utc(message.text or "")
            if parsed is None:
                logger.warning(
                    "Invalid custom time from user_id=%s text=%s",
                    user_id,
                    message.text,
                )
                back_callback = str(waiting.get("back_callback") or f"{CALLBACK_BACK_ASSET_PREFIX}{asset_text}")
                await message.answer(
                    "Не распознал время.\n"
                    "Используйте: <code>dd.mm.yyyy HH:MM</code> "
                    "(или <code>HH:MM</code>, <code>yyyy-mm-dd HH:MM</code>).",
                    reply_markup=build_input_step_keyboard(asset_text, back_callback),
                )
                return

            trigger_at_utc, delay_minutes = parsed

            logger.info(
                "Custom time alert condition saved user_id=%s asset=%s trigger_at_utc=%s delay_minutes=%s",
                user_id,
                asset_text,
                trigger_at_utc.isoformat(),
                delay_minutes,
            )

            state.pending_inputs[user_id] = {
                "type": "alert_message_input",
                "asset": asset_text,
                "draft_kind": ALERT_KIND_TIME,
                "trigger_at_utc": trigger_at_utc.isoformat(),
                "delay_minutes": delay_minutes,
                "back_callback": f"{CALLBACK_TIME_CUSTOM_PREFIX}{asset_text}",
            }

            await message.answer(
                "<b>Почти готово</b>\n"
                f"<code>{html.escape(asset_text)}</code>: "
                f"<b>{html.escape(format_local_datetime(trigger_at_utc.isoformat()))}</b>\n"
                "Введите сообщение к алерту или <code>-</code>, если сообщение не нужно.",
                reply_markup=build_input_step_keyboard(
                    asset_text,
                    f"{CALLBACK_TIME_CUSTOM_PREFIX}{asset_text}",
                ),
            )
            return

        if input_type == "alert_message_input":
            message_text, error_text = parse_user_alert_message_input(message.text or "")
            if error_text:
                back_callback = str(waiting.get("back_callback") or f"{CALLBACK_BACK_ASSET_PREFIX}{asset_text}")
                await message.answer(
                    error_text,
                    reply_markup=build_input_step_keyboard(asset_text, back_callback),
                )
                return

            draft_kind = str(waiting.get("draft_kind") or "")

            if draft_kind == ALERT_KIND_PRICE:
                direction = str(waiting.get("direction") or "")
                target_raw = waiting.get("target")
                target = (
                    float(target_raw)
                    if isinstance(target_raw, (int, float))
                    else parse_price(str(target_raw or ""))
                )
                if direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP} or target is None:
                    state.pending_inputs.pop(user_id, None)
                    await message.answer("Ошибка настройки алерта. Повторите через меню.")
                    await send_asset_alert_message(message, state, asset_text)
                    return

                state.alert_store.upsert_price(
                    user_id=user_id,
                    asset=asset_text,
                    direction=direction,
                    target=target,
                    message_text=message_text,
                )
                state.pending_inputs.pop(user_id, None)
                await message.answer(
                    "<b>Ценовой алерт сохранен</b>\n"
                    f"<code>{html.escape(asset_text)}</code>: "
                    f"{direction_label(direction)} <b>{format_target(target)}</b>"
                    f"{format_alert_message_block(message_text)}"
                )
                await send_asset_alert_message(message, state, asset_text)
                return

            if draft_kind == ALERT_KIND_PRICE_TIME:
                direction = str(waiting.get("direction") or "")
                mode = str(waiting.get("pt_mode") or "")
                timeframe_code = str(waiting.get("timeframe_code") or "").lower()
                target_raw = waiting.get("target")
                target = (
                    float(target_raw)
                    if isinstance(target_raw, (int, float))
                    else parse_price(str(target_raw or ""))
                )

                if (
                    direction not in {CROSS_TOP_DOWN, CROSS_BOTTOM_UP}
                    or target is None
                    or mode != PRICE_TIME_MODE_CANDLE_CLOSE
                    or not is_supported_candle_timeframe(timeframe_code)
                ):
                    state.pending_inputs.pop(user_id, None)
                    await message.answer("Ошибка настройки алерта. Повторите через меню.")
                    await send_asset_alert_message(message, state, asset_text)
                    return

                trigger_at_utc, _, _ = compute_timeframe_trigger_utc(
                    state,
                    asset_text,
                    timeframe_code,
                )
                state.alert_store.add_price_time(
                    user_id=user_id,
                    asset=asset_text,
                    direction=direction,
                    target=target,
                    mode=PRICE_TIME_MODE_CANDLE_CLOSE,
                    timeframe_code=timeframe_code,
                    trigger_at_utc=trigger_at_utc,
                    message_text=message_text,
                )
                state.pending_inputs.pop(user_id, None)
                await message.answer(
                    "<b>Price+Time алерт сохранен</b>\n"
                    f"<code>{html.escape(asset_text)}</code>: закрытие "
                    f"<b>{html.escape(timeframe_label(timeframe_code))}</b>, "
                    f"условие {direction_label(direction)} <b>{format_target(target)}</b>\n"
                    f"Следующая проверка: "
                    f"<b>{html.escape(format_local_datetime(trigger_at_utc.isoformat()))}</b>"
                    f"{format_alert_message_block(message_text)}"
                )
                await send_asset_alert_message(message, state, asset_text)
                return

            if draft_kind == ALERT_KIND_TIME:
                trigger_raw = str(waiting.get("trigger_at_utc") or "")
                trigger_at_utc = parse_utc_iso(trigger_raw)
                delay_raw = waiting.get("delay_minutes")
                delay_minutes = 0
                if isinstance(delay_raw, (int, float, str)):
                    with contextlib.suppress(ValueError, TypeError):
                        delay_minutes = int(delay_raw)
                if trigger_at_utc is None or delay_minutes <= 0:
                    state.pending_inputs.pop(user_id, None)
                    await message.answer("Ошибка настройки алерта. Повторите через меню.")
                    await send_asset_alert_message(message, state, asset_text)
                    return

                state.alert_store.add_time(
                    user_id=user_id,
                    asset=asset_text,
                    trigger_at_utc=trigger_at_utc,
                    delay_minutes=delay_minutes,
                    message_text=message_text,
                )
                state.pending_inputs.pop(user_id, None)
                await message.answer(
                    "<b>Алерт по времени сохранен</b>\n"
                    f"<code>{html.escape(asset_text)}</code>: "
                    f"<b>{html.escape(format_local_datetime(trigger_at_utc.isoformat()))}</b>"
                    f"{format_alert_message_block(message_text)}"
                )
                await send_asset_alert_message(message, state, asset_text)
                return

            state.pending_inputs.pop(user_id, None)
            await message.answer("Ошибка настройки алерта. Повторите через меню.")
            await send_asset_alert_message(message, state, asset_text)
            return

        logger.warning("Unknown pending input state for user_id=%s: %s", user_id, waiting)

    @router.message()
    async def unauthorized_fallback_handler(message: Message) -> None:
        await ensure_message_allowed(state, message)

    return router


async def periodic_checker(bot: Bot, state: BotState) -> None:
    await asyncio.sleep(5)

    interval = max(10, state.config.telegram.check_interval_seconds)
    logger.info("Periodic checker started, interval=%s seconds", interval)

    while True:
        try:
            await refresh_quotes_and_alerts(bot, state, process_alerts=True)
        except asyncio.CancelledError:
            logger.info("Periodic checker cancelled")
            raise
        except Exception:
            logger.exception("Periodic quote check failed")

        try:
            await send_auto_eye_notifications(bot, state)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Periodic auto-eye notifications check failed")

        await asyncio.sleep(interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Telegram quote alerts bot")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/site_config.yaml"),
        help="Path to YAML config file",
    )
    return parser.parse_args()


def run(config_path: Path) -> None:
    config = load_config(config_path)
    log_path = configure_logging(
        level=config.logging.level,
        file_path=config.logging.file,
        max_bytes=config.logging.max_bytes,
        backup_count=config.logging.backup_count,
    )

    logger.info("Logging initialized: %s", log_path)
    logger.info("Starting bot with config: %s", config_path)

    if not config.telegram.bot_token:
        logger.error("Telegram bot token is empty")
        raise ValueError(
            "Telegram bot token is empty. Set telegram.bot_token or TELEGRAM_BOT_TOKEN."
        )

    if not config.telegram.allowed_user_ids:
        logger.error("telegram.allowed_user_ids is empty")
        raise ValueError(
            "telegram.allowed_user_ids is empty. Add allowed Telegram user IDs to config."
        )

    logger.info(
        "Telegram access list enabled: %s",
        ", ".join(str(user_id) for user_id in config.telegram.allowed_user_ids),
    )

    alerts_path = resolve_output_path(config.telegram.alerts_json)
    quotes_path = resolve_output_path(config.scraper.output_json)
    timeframe_rules = load_timeframe_rules(TIMEFRAME_RULES_PATH)
    auto_eye_state_dir = resolve_auto_eye_state_dir(config)
    auto_eye_seen_path = resolve_output_path(
        config.telegram.auto_eye_notifications.seen_ids_json
    )

    logger.info(
        "Auto-eye notifications config: enabled=%s state_dir=%s timeframes=%s elements=%s seen_store=%s",
        config.telegram.auto_eye_notifications.enabled,
        auto_eye_state_dir,
        ", ".join(config.telegram.auto_eye_notifications.timeframes),
        ", ".join(config.telegram.auto_eye_notifications.elements),
        auto_eye_seen_path,
    )
    logger.info(
        "Auto-eye mode: H1 price proximity alerts with trend and trade draft (SL behind H1 zone, TP to nearest H1 RB/SNR/FVG)"
    )
    logger.info(
        "Backtest config: enabled=%s allowed=%s max_interval_hours=%s warmup_bars=%s max_proposals_to_send=%s decisions_log_file=%s",
        config.telegram.backtest.enabled,
        ", ".join(str(user_id) for user_id in config.telegram.backtest.allowed_user_ids),
        config.telegram.backtest.max_interval_hours,
        config.telegram.backtest.warmup_bars,
        config.telegram.backtest.max_proposals_to_send,
        resolve_backtest_log_path(config),
    )

    state = BotState(
        config=config,
        alert_store=AlertStore(alerts_path),
        timeframe_rules=timeframe_rules,
        scrape_lock=asyncio.Lock(),
        pending_inputs={},
        asset_delete_selection={},
        alert_edit_sessions={},
        last_quotes=load_cached_quotes(quotes_path),
        dashboard_message_ids={},
        auto_eye_state_dir=auto_eye_state_dir,
        auto_eye_seen_store=AutoEyeSeenStore(auto_eye_seen_path),
        backtest_tasks={},
    )

    bot = Bot(
        token=config.telegram.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(build_router(state))

    @dp.startup()
    async def on_startup() -> None:
        logger.info("Bot startup completed")
        state.periodic_task = asyncio.create_task(periodic_checker(bot, state))

    @dp.shutdown()
    async def on_shutdown() -> None:
        logger.info("Bot shutdown started")

        if state.periodic_task is not None:
            state.periodic_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await state.periodic_task

        backtest_tasks = [
            task
            for task in state.backtest_tasks.values()
            if not task.done()
        ]
        for task in backtest_tasks:
            task.cancel()
        for task in backtest_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        logger.info("Bot shutdown completed")

    dp.run_polling(bot)


if __name__ == "__main__":
    args = parse_args()
    run(args.config)


