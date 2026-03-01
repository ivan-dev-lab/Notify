from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class BrowserConfig:
    name: str
    headless: bool
    implicit_wait: int
    page_load_timeout: int


@dataclass
class ScraperConfig:
    assets: list[str]
    output_json: str
    symbol_map: dict[str, str]


@dataclass
class MetaTraderConfig:
    login: int
    password: str
    server: str
    terminal_path: str
    timeout_ms: int


@dataclass
class AutoEyeNotificationsConfig:
    enabled: bool
    timeframes: list[str]
    elements: list[str]
    state_dir: str
    seen_ids_json: str


@dataclass
class TelegramBacktestConfig:
    enabled: bool
    allowed_user_ids: list[int]
    max_interval_hours: int
    warmup_bars: int
    max_proposals_to_send: int
    decisions_log_file: str = "logs/backtest_decisions.log"


@dataclass
class TelegramConfig:
    bot_token: str
    check_interval_seconds: int
    alerts_json: str
    allowed_user_ids: list[int]
    auto_eye_notifications: AutoEyeNotificationsConfig
    backtest: TelegramBacktestConfig


@dataclass
class LoggingConfig:
    level: str
    file: str
    max_bytes: int
    backup_count: int


@dataclass
class AutoEyeConfig:
    enabled: bool
    symbols: list[str]
    timeframes: list[str]
    elements: list[str]
    history_days: int
    history_buffer_days: int
    incremental_bars: int
    update_interval_seconds: int
    scheduler_poll_seconds: int
    output_json: str
    output_csv: str
    state_json: str
    min_gap_points: float
    require_displacement: bool
    displacement_k: float
    atr_period: int
    median_body_period: int
    fill_rule: str
    snr_departure_start: str = "pivot"
    snr_include_break_candle: bool = False


@dataclass
class AppConfig:
    url: str
    browser: BrowserConfig
    scraper: ScraperConfig
    metatrader: MetaTraderConfig
    telegram: TelegramConfig
    logging: LoggingConfig
    auto_eye: AutoEyeConfig


def _parse_user_ids(raw_value: object) -> list[int]:
    values: list[int] = []
    candidates: list[object] = []

    if isinstance(raw_value, list):
        candidates.extend(raw_value)
    elif isinstance(raw_value, str):
        candidates.extend(part.strip() for part in raw_value.split(","))

    for candidate in candidates:
        text_value = str(candidate).strip()
        if not text_value:
            continue
        try:
            parsed = int(text_value)
        except ValueError:
            continue
        if parsed not in values:
            values.append(parsed)

    return values


def _normalize_notification_timeframe(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized == "M1":
        # Monthly TF in this project is stored as MN1 in State snapshots.
        return "MN1"
    return normalized


def _normalize_notification_element(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized in {"fractal", "fractals"}:
        return "fractal"
    if normalized in {"fvg", "snr", "rb"}:
        return normalized
    return ""


def load_config(config_path: Path) -> AppConfig:
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)

    site = raw.get("site", {})
    browser = raw.get("browser", {})
    scraper = raw.get("scraper", {})
    metatrader = raw.get("metatrader", {})
    telegram = raw.get("telegram", {})
    logging_raw = raw.get("logging", {})
    auto_eye_raw = raw.get("auto_eye", {})

    raw_assets = scraper.get("assets", [])
    assets: list[str] = []
    for asset in raw_assets:
        normalized = str(asset).strip().upper()
        if normalized:
            assets.append(normalized)

    symbol_map: dict[str, str] = {}
    for asset, symbol in scraper.get("symbol_map", {}).items():
        normalized_asset = str(asset).strip().upper()
        normalized_symbol = str(symbol).strip().upper()
        if normalized_asset and normalized_symbol:
            symbol_map[normalized_asset] = normalized_symbol

    login_raw = str(metatrader.get("login", "")).strip()
    if not login_raw:
        login_raw = os.getenv("MT5_LOGIN", "").strip()

    password = str(metatrader.get("password", "")).strip()
    if not password:
        password = os.getenv("MT5_PASSWORD", "").strip()

    server = str(metatrader.get("server", "")).strip()
    if not server:
        server = os.getenv("MT5_SERVER", "").strip()

    terminal_path = str(metatrader.get("terminal_path", "")).strip()
    if not terminal_path:
        terminal_path = os.getenv("MT5_TERMINAL_PATH", "").strip()

    bot_token = str(telegram.get("bot_token", "")).strip()
    if not bot_token:
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

    allowed_user_ids: list[int] = _parse_user_ids(telegram.get("allowed_user_ids", []))
    env_allowed = _parse_user_ids(os.getenv("TELEGRAM_ALLOWED_USER_IDS", "").strip())
    for user_id in env_allowed:
        if user_id not in allowed_user_ids:
            allowed_user_ids.append(user_id)

    backtest_raw = telegram.get("backtest", {})
    if not isinstance(backtest_raw, dict):
        backtest_raw = {}

    backtest_allowed_user_ids: list[int] = _parse_user_ids(
        backtest_raw.get("allowed_user_ids", [])
    )
    env_backtest_allowed = _parse_user_ids(
        os.getenv("TELEGRAM_BACKTEST_USER_IDS", "").strip()
    )
    for user_id in env_backtest_allowed:
        if user_id not in backtest_allowed_user_ids:
            backtest_allowed_user_ids.append(user_id)

    if not backtest_allowed_user_ids:
        backtest_allowed_user_ids = list(allowed_user_ids)

    auto_eye_symbols: list[str] = []
    auto_eye_raw_symbols = auto_eye_raw.get("symbols", [])
    if isinstance(auto_eye_raw_symbols, list):
        for symbol in auto_eye_raw_symbols:
            normalized_symbol = str(symbol).strip().upper()
            if normalized_symbol:
                auto_eye_symbols.append(normalized_symbol)
    if not auto_eye_symbols:
        auto_eye_symbols = list(assets)

    auto_eye_timeframes: list[str] = []
    raw_timeframes = auto_eye_raw.get("timeframes", ["M5"])
    if isinstance(raw_timeframes, list):
        for timeframe in raw_timeframes:
            normalized_timeframe = str(timeframe).strip().upper()
            if normalized_timeframe:
                auto_eye_timeframes.append(normalized_timeframe)
    if not auto_eye_timeframes:
        auto_eye_timeframes = ["M5"]

    auto_eye_elements: list[str] = []
    raw_elements = auto_eye_raw.get("elements", ["fvg"])
    if isinstance(raw_elements, list):
        for element in raw_elements:
            normalized_element = str(element).strip().lower()
            if normalized_element and normalized_element not in auto_eye_elements:
                auto_eye_elements.append(normalized_element)
    if not auto_eye_elements:
        auto_eye_elements = ["fvg"]

    auto_eye_notification_raw = telegram.get("auto_eye_notifications", {})
    if not isinstance(auto_eye_notification_raw, dict):
        auto_eye_notification_raw = {}

    notification_timeframes: list[str] = []
    raw_notification_timeframes = auto_eye_notification_raw.get("timeframes", [])
    if isinstance(raw_notification_timeframes, list):
        for timeframe in raw_notification_timeframes:
            normalized_timeframe = _normalize_notification_timeframe(str(timeframe))
            if normalized_timeframe and normalized_timeframe not in notification_timeframes:
                notification_timeframes.append(normalized_timeframe)

    if not notification_timeframes:
        for timeframe in auto_eye_timeframes:
            normalized_timeframe = _normalize_notification_timeframe(timeframe)
            if normalized_timeframe and normalized_timeframe not in notification_timeframes:
                notification_timeframes.append(normalized_timeframe)

    notification_elements: list[str] = []
    raw_notification_elements = auto_eye_notification_raw.get("elements", [])
    if isinstance(raw_notification_elements, list):
        for element in raw_notification_elements:
            normalized_element = _normalize_notification_element(str(element))
            if normalized_element and normalized_element not in notification_elements:
                notification_elements.append(normalized_element)

    if not notification_elements:
        for element in auto_eye_elements:
            normalized_element = _normalize_notification_element(element)
            if normalized_element and normalized_element not in notification_elements:
                notification_elements.append(normalized_element)

    if not notification_elements:
        notification_elements = ["fvg"]

    fill_rule = str(auto_eye_raw.get("fill_rule", "both")).strip().lower()
    if fill_rule not in {"touch", "full", "both"}:
        fill_rule = "both"

    snr_departure_start = str(
        auto_eye_raw.get("snr_departure_start", "pivot")
    ).strip().lower()
    if snr_departure_start not in {"pivot", "confirm"}:
        snr_departure_start = "pivot"

    return AppConfig(
        url=str(site.get("url", "")),
        browser=BrowserConfig(
            name=str(browser.get("name", "chrome")).lower(),
            headless=bool(browser.get("headless", True)),
            implicit_wait=int(browser.get("implicit_wait", 5)),
            page_load_timeout=int(browser.get("page_load_timeout", 30)),
        ),
        scraper=ScraperConfig(
            assets=assets,
            output_json=str(scraper.get("output_json", "output/forex_quotes.json")),
            symbol_map=symbol_map,
        ),
        metatrader=MetaTraderConfig(
            login=int(login_raw) if login_raw else 0,
            password=password,
            server=server,
            terminal_path=terminal_path,
            timeout_ms=int(metatrader.get("timeout_ms", 10000)),
        ),
        telegram=TelegramConfig(
            bot_token=bot_token,
            check_interval_seconds=int(telegram.get("check_interval_seconds", 300)),
            alerts_json=str(telegram.get("alerts_json", "output/alerts.json")),
            allowed_user_ids=allowed_user_ids,
            auto_eye_notifications=AutoEyeNotificationsConfig(
                enabled=bool(auto_eye_notification_raw.get("enabled", True)),
                timeframes=notification_timeframes,
                elements=notification_elements,
                state_dir=str(auto_eye_notification_raw.get("state_dir", "")).strip(),
                seen_ids_json=str(
                    auto_eye_notification_raw.get(
                        "seen_ids_json",
                        "output/auto_eye_notified_elements.json",
                    )
                ).strip(),
            ),
            backtest=TelegramBacktestConfig(
                enabled=bool(backtest_raw.get("enabled", True)),
                allowed_user_ids=backtest_allowed_user_ids,
                max_interval_hours=max(1, int(backtest_raw.get("max_interval_hours", 168))),
                warmup_bars=max(50, int(backtest_raw.get("warmup_bars", 500))),
                max_proposals_to_send=max(1, int(backtest_raw.get("max_proposals_to_send", 30))),
                decisions_log_file=str(
                    backtest_raw.get(
                        "decisions_log_file",
                        "logs/backtest_decisions.log",
                    )
                ).strip(),
            ),
        ),
        logging=LoggingConfig(
            level=str(logging_raw.get("level", "INFO")),
            file=str(logging_raw.get("file", "logs/notify.log")),
            max_bytes=int(logging_raw.get("max_bytes", 5_000_000)),
            backup_count=int(logging_raw.get("backup_count", 5)),
        ),
        auto_eye=AutoEyeConfig(
            enabled=bool(auto_eye_raw.get("enabled", True)),
            symbols=auto_eye_symbols,
            timeframes=auto_eye_timeframes,
            elements=auto_eye_elements,
            history_days=max(1, int(auto_eye_raw.get("history_days", 30))),
            history_buffer_days=max(0, int(auto_eye_raw.get("history_buffer_days", 5))),
            incremental_bars=max(20, int(auto_eye_raw.get("incremental_bars", 500))),
            update_interval_seconds=max(
                10, int(auto_eye_raw.get("update_interval_seconds", 300))
            ),
            scheduler_poll_seconds=max(
                10, int(auto_eye_raw.get("scheduler_poll_seconds", 60))
            ),
            output_json=str(auto_eye_raw.get("output_json", "output/auto_eye_zones.json")),
            output_csv=str(auto_eye_raw.get("output_csv", "output/auto_eye_zones.csv")),
            state_json=str(auto_eye_raw.get("state_json", "output/auto_eye_state.json")),
            min_gap_points=float(auto_eye_raw.get("min_gap_points", 0)),
            require_displacement=bool(auto_eye_raw.get("require_displacement", False)),
            displacement_k=float(auto_eye_raw.get("displacement_k", 1.5)),
            atr_period=max(1, int(auto_eye_raw.get("atr_period", 14))),
            median_body_period=max(1, int(auto_eye_raw.get("median_body_period", 20))),
            fill_rule=fill_rule,
            snr_departure_start=snr_departure_start,
            snr_include_break_candle=bool(
                auto_eye_raw.get("snr_include_break_candle", False)
            ),
        ),
    )

