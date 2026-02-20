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
class TelegramConfig:
    bot_token: str
    check_interval_seconds: int
    alerts_json: str
    allowed_user_ids: list[int]


@dataclass
class LoggingConfig:
    level: str
    file: str
    max_bytes: int
    backup_count: int


@dataclass
class AppConfig:
    url: str
    browser: BrowserConfig
    scraper: ScraperConfig
    metatrader: MetaTraderConfig
    telegram: TelegramConfig
    logging: LoggingConfig


def load_config(config_path: Path) -> AppConfig:
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)

    site = raw.get("site", {})
    browser = raw.get("browser", {})
    scraper = raw.get("scraper", {})
    metatrader = raw.get("metatrader", {})
    telegram = raw.get("telegram", {})
    logging_raw = raw.get("logging", {})

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

    allowed_user_ids: list[int] = []
    raw_allowed = telegram.get("allowed_user_ids", [])
    allowed_candidates: list[object] = []

    if isinstance(raw_allowed, list):
        allowed_candidates.extend(raw_allowed)
    elif isinstance(raw_allowed, str):
        allowed_candidates.extend(part.strip() for part in raw_allowed.split(","))

    env_allowed = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "").strip()
    if env_allowed:
        allowed_candidates.extend(part.strip() for part in env_allowed.split(","))

    for candidate in allowed_candidates:
        text_value = str(candidate).strip()
        if not text_value:
            continue
        try:
            user_id = int(text_value)
        except ValueError:
            continue
        if user_id not in allowed_user_ids:
            allowed_user_ids.append(user_id)

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
        ),
        logging=LoggingConfig(
            level=str(logging_raw.get("level", "INFO")),
            file=str(logging_raw.get("file", "logs/notify.log")),
            max_bytes=int(logging_raw.get("max_bytes", 5_000_000)),
            backup_count=int(logging_raw.get("backup_count", 5)),
        ),
    )
