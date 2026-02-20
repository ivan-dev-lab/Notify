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
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app_logging import configure_logging
from config_loader import AppConfig, load_config
from main import QuotesMap, collect_quotes, resolve_output_path, save_quotes

CALLBACK_REFRESH = "refresh"
CALLBACK_MENU_ALERTS = "menu_alerts"
CALLBACK_MENU_HOME = "menu_home"
CALLBACK_CANCEL = "cancel"
CALLBACK_NOOP = "noop"

CALLBACK_ALERT_ASSET_PREFIX = "alerts_asset|"
CALLBACK_PRICE_SET_PREFIX = "price_set|"
CALLBACK_TIME_QUICK_PREFIX = "time_q|"
CALLBACK_TIME_CUSTOM_PREFIX = "time_c|"
CALLBACK_DELETE_ASSET_PREFIX = "del_asset|"

CALLBACK_RENEW_PRICE_PREFIX = "renew_p|"
CALLBACK_RENEW_TIME_PREFIX = "renew_t|"

ALERT_KIND_PRICE = "price"
ALERT_KIND_TIME = "time"

DIRECTION_ABOVE = "above"
DIRECTION_BELOW = "below"

USER_TIMEZONE = timezone(timedelta(hours=5))
USER_TIMEZONE_LABEL = "GMT+5"

HHMM_PATTERN = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*$")
FULL_DATETIME_PATTERN = re.compile(
    r"^\s*(\d{4})-(\d{2})-(\d{2})\s+(\d{1,2}):(\d{2})\s*$"
)
DMY_DATETIME_PATTERN = re.compile(
    r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})\s*$"
)

PREFERRED_GROUP_ORDER = [
    "INDICES",
    "GBP/N*",
    "USD/N*",
    "EUR/N*",
    "AUD/N*",
    "NZD/N*",
]

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


@dataclass
class TriggeredAlert:
    alert: AlertRule
    current_value_text: str


@dataclass
class BotState:
    config: AppConfig
    alert_store: "AlertStore"
    scrape_lock: asyncio.Lock
    pending_inputs: dict[int, dict[str, str]]
    last_quotes: QuotesMap
    periodic_task: asyncio.Task | None = None


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

        if user_id <= 0:
            return None

        kind = str(item.get("kind", "")).strip().lower()

        if kind == ALERT_KIND_PRICE:
            direction = str(item.get("direction", "")).strip().lower()
            target_raw = item.get("target")
            if direction not in {DIRECTION_ABOVE, DIRECTION_BELOW}:
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
            )

        # Backward compatibility with old price schema (no kind field).
        direction = str(item.get("direction", "")).strip().lower()
        target_raw = item.get("target")
        if direction in {DIRECTION_ABOVE, DIRECTION_BELOW} and target_raw is not None:
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

    def upsert_price(self, user_id: int, asset: str, direction: str, target: float) -> None:
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
        self, user_id: int, asset: str, trigger_at_utc: datetime, delay_minutes: int
    ) -> None:
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

    def consume_triggered(self, quotes: QuotesMap) -> list[TriggeredAlert]:
        now_utc = datetime.now(timezone.utc)
        triggered: list[TriggeredAlert] = []
        active: list[AlertRule] = []

        for alert in self.alerts:
            if alert.kind == ALERT_KIND_PRICE:
                record = quotes.get(alert.asset, {})
                current_text = str(record.get("value") or "").strip()
                current_value = parse_price(current_text)

                if current_value is None or alert.target is None or alert.direction is None:
                    active.append(alert)
                    continue

                if alert.direction == DIRECTION_ABOVE and current_value > alert.target:
                    triggered.append(
                        TriggeredAlert(alert=alert, current_value_text=current_text)
                    )
                    continue

                if alert.direction == DIRECTION_BELOW and current_value < alert.target:
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

            active.append(alert)

        if len(active) != len(self.alerts):
            self.alerts = active
            self.save()

        if triggered:
            logger.info("Triggered %s alerts", len(triggered))
        return triggered

def direction_label(direction: str) -> str:
    if direction == DIRECTION_ABOVE:
        return "выше"
    if direction == DIRECTION_BELOW:
        return "ниже"
    return direction


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
    if alert.kind == ALERT_KIND_PRICE:
        if alert.target is None or alert.direction is None:
            return f"• <code>{html.escape(alert.asset)}</code>: некорректный ценовой алерт"
        return (
            f"• <code>{html.escape(alert.asset)}</code>: "
            f"{direction_label(alert.direction)} <b>{format_target(alert.target)}</b>"
        )

    if alert.kind == ALERT_KIND_TIME:
        when = format_local_datetime(alert.trigger_at_utc)
        return f"• <code>{html.escape(alert.asset)}</code>: по времени <b>{html.escape(when)}</b>"

    return f"• <code>{html.escape(alert.asset)}</code>: неизвестный алерт"


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


def build_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Обновить и проверить", callback_data=CALLBACK_REFRESH)],
            [InlineKeyboardButton(text="Меню алертов", callback_data=CALLBACK_MENU_ALERTS)],
        ]
    )


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


def build_asset_alert_keyboard(asset: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Цена выше",
                    callback_data=f"{CALLBACK_PRICE_SET_PREFIX}{asset}|{DIRECTION_ABOVE}",
                ),
                InlineKeyboardButton(
                    text="Цена ниже",
                    callback_data=f"{CALLBACK_PRICE_SET_PREFIX}{asset}|{DIRECTION_BELOW}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Через 15 минут",
                    callback_data=f"{CALLBACK_TIME_QUICK_PREFIX}{asset}|15",
                ),
                InlineKeyboardButton(
                    text="Через 1 час",
                    callback_data=f"{CALLBACK_TIME_QUICK_PREFIX}{asset}|60",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Через 4 часа",
                    callback_data=f"{CALLBACK_TIME_QUICK_PREFIX}{asset}|240",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Точное время",
                    callback_data=f"{CALLBACK_TIME_CUSTOM_PREFIX}{asset}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Удалить все по паре",
                    callback_data=f"{CALLBACK_DELETE_ASSET_PREFIX}{asset}",
                )
            ],
            [InlineKeyboardButton(text="Назад к меню", callback_data=CALLBACK_MENU_ALERTS)],
        ]
    )


def build_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data=CALLBACK_CANCEL)]]
    )


def build_extend_keyboard(alert: AlertRule) -> InlineKeyboardMarkup | None:
    if alert.kind == ALERT_KIND_PRICE and alert.direction and alert.target is not None:
        callback_data = (
            f"{CALLBACK_RENEW_PRICE_PREFIX}{alert.asset}|{alert.direction}|{format_target(alert.target)}"
        )
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Продлить алерт", callback_data=callback_data)]
            ]
        )

    if alert.kind == ALERT_KIND_TIME:
        delay = max(1, int(alert.delay_minutes or 60))
        callback_data = f"{CALLBACK_RENEW_TIME_PREFIX}{alert.asset}|{delay}"
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Продлить алерт", callback_data=callback_data)]
            ]
        )

    return None


def get_display_assets(config: AppConfig, quotes: QuotesMap) -> list[str]:
    assets = [asset for asset in config.scraper.assets if asset]
    known = set(assets)

    extras = sorted(asset for asset in quotes.keys() if asset and asset not in known)
    assets.extend(extras)
    return assets


def render_dashboard_text(config: AppConfig, quotes: QuotesMap, alerts: list[AlertRule]) -> str:
    lines: list[str] = []
    lines.extend(render_grouped_quotes(config, quotes))
    lines.append("")
    lines.append("<b>Активные алерты</b>")

    if not alerts:
        lines.append("• нет")
    else:
        for alert in sorted(alerts, key=lambda item: (item.asset, item.kind, item.created_at_utc)):
            lines.append(render_alert_line(alert))

    return "\n".join(lines)


def render_alerts_menu_text(chat_alerts: list[AlertRule]) -> str:
    lines = ["<b>Управление алертами</b>", "", "Выберите актив из списка ниже.", ""]
    lines.append("<b>Текущие алерты</b>")

    if not chat_alerts:
        lines.append("• нет")
    else:
        for alert in sorted(chat_alerts, key=lambda item: (item.asset, item.kind, item.created_at_utc)):
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
        for alert in sorted(asset_alerts, key=lambda item: (item.kind, item.created_at_utc)):
            lines.append(render_alert_line(alert))

    lines.append("")
    lines.append("<i>Выберите действие ниже.</i>")
    return "\n".join(lines)


def is_user_allowed(state: BotState, user_id: int) -> bool:
    allowed = state.config.telegram.allowed_user_ids
    if not allowed:
        return True
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


def create_relative_time_alert(
    state: BotState, user_id: int, asset: str, delay_minutes: int
) -> tuple[datetime, int]:
    safe_delay = max(1, int(delay_minutes))
    now_utc = datetime.now(timezone.utc)
    trigger_at_utc = now_utc + timedelta(minutes=safe_delay)
    state.alert_store.add_time(
        user_id=user_id,
        asset=asset,
        trigger_at_utc=trigger_at_utc,
        delay_minutes=safe_delay,
    )
    return trigger_at_utc, safe_delay


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

async def refresh_quotes_and_alerts(
    bot: Bot,
    state: BotState,
    *,
    process_alerts: bool,
) -> QuotesMap:
    logger.info("Refreshing quotes (process_alerts=%s)", process_alerts)

    async with state.scrape_lock:
        quotes = await asyncio.to_thread(collect_quotes, state.config, False)
        await asyncio.to_thread(save_quotes, state.config, quotes)
        state.last_quotes = quotes

        triggered: list[TriggeredAlert] = []
        if process_alerts:
            triggered = state.alert_store.consume_triggered(quotes)

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
        else:
            text = (
                "<b>Сработал алерт</b>\n"
                "<b>Тип:</b> время\n"
                f"<b>Актив:</b> <code>{html.escape(alert.asset)}</code>\n"
                f"<b>Запланировано:</b> "
                f"<b>{html.escape(format_local_datetime(alert.trigger_at_utc))}</b>\n"
                f"<b>Текущая цена:</b> <b>{html.escape(event.current_value_text)}</b>"
            )

        try:
            await bot.send_message(
                chat_id=alert.user_id,
                text=text,
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

    text = render_dashboard_text(
        state.config,
        quotes,
        state.alert_store.list_for_user(get_user_id_from_message(message)),
    )
    await message.answer(text=text, reply_markup=build_home_keyboard())


async def edit_dashboard_message(
    query: CallbackQuery,
    state: BotState,
    *,
    quotes: QuotesMap | None = None,
) -> None:
    if quotes is None:
        quotes = state.last_quotes

    user_id = get_user_id_from_query(query)
    text = render_dashboard_text(
        state.config,
        quotes,
        state.alert_store.list_for_user(user_id),
    )
    await safe_edit_message(query, text=text, reply_markup=build_home_keyboard())


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


async def edit_asset_alert_message(query: CallbackQuery, state: BotState, asset: str) -> None:
    user_id = get_user_id_from_query(query)
    asset_alerts = state.alert_store.list_for_user_asset(user_id, asset)
    await safe_edit_message(
        query,
        text=render_asset_menu_text(asset, asset_alerts),
        reply_markup=build_asset_alert_keyboard(asset),
    )


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

    @router.callback_query(F.data == CALLBACK_NOOP)
    async def noop_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return
        await query.answer()

    @router.callback_query(F.data == CALLBACK_REFRESH)
    async def refresh_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        logger.info("Manual refresh requested by user_id=%s", user_id)
        await query.answer()

        try:
            quotes = await refresh_quotes_and_alerts(query.bot, state, process_alerts=True)
        except Exception:
            logger.exception("Manual refresh failed")
            await safe_edit_message(
                query,
                text="<b>Не удалось обновить котировки.</b>",
                reply_markup=build_home_keyboard(),
            )
            return

        await edit_dashboard_message(query, state, quotes=quotes)

    @router.callback_query(F.data == CALLBACK_MENU_HOME)
    async def menu_home_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        logger.info("Home menu opened by user_id=%s", get_user_id_from_query(query))
        await query.answer()
        await edit_dashboard_message(query, state)

    @router.callback_query(F.data == CALLBACK_MENU_ALERTS)
    async def menu_alerts_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        logger.info("Alerts menu opened by user_id=%s", get_user_id_from_query(query))
        await query.answer()
        await edit_alerts_menu_message(query, state)

    @router.callback_query(F.data == CALLBACK_CANCEL)
    async def cancel_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        user_id = get_user_id_from_query(query)
        logger.info("Cancel input by user_id=%s", user_id)
        await query.answer()
        state.pending_inputs.pop(user_id, None)
        await edit_alerts_menu_message(query, state)

    @router.callback_query(F.data.startswith(CALLBACK_ALERT_ASSET_PREFIX))
    async def alert_asset_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()

        data = query.data or ""
        asset = data[len(CALLBACK_ALERT_ASSET_PREFIX) :]
        logger.info("Asset menu opened user_id=%s asset=%s", get_user_id_from_query(query), asset)
        await edit_asset_alert_message(query, state, asset)

    @router.callback_query(F.data.startswith(CALLBACK_DELETE_ASSET_PREFIX))
    async def delete_asset_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()

        data = query.data or ""
        asset = data[len(CALLBACK_DELETE_ASSET_PREFIX) :]
        user_id = get_user_id_from_query(query)
        removed = state.alert_store.remove_asset_alerts(user_id, asset)
        logger.info("Delete asset alerts user_id=%s asset=%s removed=%s", user_id, asset, removed)
        await edit_asset_alert_message(query, state, asset)

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
        user_id = get_user_id_from_query(query)
        state.pending_inputs[user_id] = {
            "type": ALERT_KIND_PRICE,
            "asset": asset,
            "direction": direction,
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
                f"Введите целевую цену.\n"
                f"Условие: когда цена будет <b>{direction_label(direction)}</b> указанной отметки."
            ),
            reply_markup=build_cancel_keyboard(),
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

        asset, minutes_raw = parts
        try:
            minutes = int(minutes_raw)
        except ValueError:
            logger.warning("Invalid quick-time minutes payload: %s", data)
            await edit_alerts_menu_message(query, state)
            return

        user_id = get_user_id_from_query(query)
        trigger_at_utc, delay = create_relative_time_alert(state, user_id, asset, minutes)

        await safe_edit_message(
            query,
            text=(
                f"<b>Алерт по времени создан</b>\n"
                f"<code>{html.escape(asset)}</code>\n"
                f"Сработает: <b>{html.escape(format_local_datetime(trigger_at_utc.isoformat()))}</b>"
            ),
            reply_markup=build_asset_alert_keyboard(asset),
        )

        logger.info(
            "Quick time alert created user_id=%s asset=%s delay_minutes=%s",
            user_id,
            asset,
            delay,
        )

    @router.callback_query(F.data.startswith(CALLBACK_TIME_CUSTOM_PREFIX))
    async def time_custom_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer()

        data = query.data or ""
        asset = data[len(CALLBACK_TIME_CUSTOM_PREFIX) :]
        user_id = get_user_id_from_query(query)

        state.pending_inputs[user_id] = {
            "type": ALERT_KIND_TIME,
            "asset": asset,
            "mode": "custom",
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
            reply_markup=build_cancel_keyboard(),
        )

    @router.callback_query(F.data.startswith(CALLBACK_RENEW_PRICE_PREFIX))
    async def renew_price_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer("Продлено")

        data = query.data or ""
        payload = data[len(CALLBACK_RENEW_PRICE_PREFIX) :]
        parts = payload.split("|", maxsplit=2)
        if len(parts) != 3:
            logger.warning("Invalid renew-price callback payload: %s", data)
            return

        asset, direction, target_raw = parts
        target = parse_price(target_raw)
        if target is None:
            logger.warning("Invalid renew-price target payload: %s", data)
            return

        user_id = get_user_id_from_query(query)
        state.alert_store.upsert_price(user_id, asset, direction, target)
        logger.info(
            "Renewed price alert user_id=%s asset=%s direction=%s target=%s",
            user_id,
            asset,
            direction,
            target,
        )

        if query.message is not None:
            with contextlib.suppress(TelegramBadRequest):
                await query.message.edit_reply_markup(reply_markup=None)

    @router.callback_query(F.data.startswith(CALLBACK_RENEW_TIME_PREFIX))
    async def renew_time_handler(query: CallbackQuery) -> None:
        if not await ensure_callback_allowed(state, query):
            return

        await query.answer("Продлено")

        data = query.data or ""
        payload = data[len(CALLBACK_RENEW_TIME_PREFIX) :]
        parts = payload.split("|", maxsplit=1)
        if len(parts) != 2:
            logger.warning("Invalid renew-time callback payload: %s", data)
            return

        asset, delay_raw = parts
        try:
            delay_minutes = max(1, int(delay_raw))
        except ValueError:
            logger.warning("Invalid renew-time delay payload: %s", data)
            return

        user_id = get_user_id_from_query(query)
        trigger_at_utc, delay = create_relative_time_alert(state, user_id, asset, delay_minutes)

        logger.info(
            "Renewed time alert user_id=%s asset=%s delay_minutes=%s trigger_at_utc=%s",
            user_id,
            asset,
            delay,
            trigger_at_utc.isoformat(),
        )

        if query.message is not None:
            with contextlib.suppress(TelegramBadRequest):
                await query.message.edit_reply_markup(reply_markup=None)

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

        if input_type == ALERT_KIND_PRICE:
            target = parse_price(message.text or "")
            direction = waiting.get("direction", "")
            if target is None or direction not in {DIRECTION_ABOVE, DIRECTION_BELOW}:
                logger.warning(
                    "Invalid price input from user_id=%s text=%s",
                    user_id,
                    message.text,
                )
                await message.answer(
                    "Не распознал цену. Пример: <code>1.2456</code>",
                    reply_markup=build_cancel_keyboard(),
                )
                return

            state.alert_store.upsert_price(
                user_id=user_id,
                asset=asset,
                direction=direction,
                target=target,
            )
            state.pending_inputs.pop(user_id, None)

            logger.info(
                "Price alert saved from text user_id=%s asset=%s direction=%s target=%s",
                user_id,
                asset,
                direction,
                target,
            )

            await message.answer(
                "<b>Ценовой алерт сохранен</b>\n"
                f"<code>{html.escape(asset)}</code>: "
                f"{direction_label(direction)} <b>{format_target(target)}</b>"
            )
            await send_dashboard_message(message, state)
            return

        if input_type == ALERT_KIND_TIME and waiting.get("mode") == "custom":
            parsed = parse_custom_time_to_utc(message.text or "")
            if parsed is None:
                logger.warning(
                    "Invalid custom time from user_id=%s text=%s",
                    user_id,
                    message.text,
                )
                await message.answer(
                    "Не распознал время.\n"
                    "Используйте: <code>dd.mm.yyyy HH:MM</code> "
                    "(или <code>HH:MM</code>, <code>yyyy-mm-dd HH:MM</code>).",
                    reply_markup=build_cancel_keyboard(),
                )
                return

            trigger_at_utc, delay_minutes = parsed
            state.alert_store.add_time(
                user_id=user_id,
                asset=asset,
                trigger_at_utc=trigger_at_utc,
                delay_minutes=delay_minutes,
            )
            state.pending_inputs.pop(user_id, None)

            logger.info(
                "Custom time alert saved user_id=%s asset=%s trigger_at_utc=%s delay_minutes=%s",
                user_id,
                asset,
                trigger_at_utc.isoformat(),
                delay_minutes,
            )

            await message.answer(
                "<b>Алерт по времени сохранен</b>\n"
                f"<code>{html.escape(asset)}</code>: "
                f"<b>{html.escape(format_local_datetime(trigger_at_utc.isoformat()))}</b>"
            )
            await send_dashboard_message(message, state)
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
            logger.exception("Periodic check failed")

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

    state = BotState(
        config=config,
        alert_store=AlertStore(alerts_path),
        scrape_lock=asyncio.Lock(),
        pending_inputs={},
        last_quotes=load_cached_quotes(quotes_path),
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

        if state.periodic_task is None:
            return

        state.periodic_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await state.periodic_task

        logger.info("Bot shutdown completed")

    dp.run_polling(bot)


if __name__ == "__main__":
    args = parse_args()
    run(args.config)
