from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

try:
    import MetaTrader5 as mt5
    MT5_IMPORT_ERROR: Exception | None = None
except Exception as import_error:  # pragma: no cover - handled at runtime
    mt5 = None
    MT5_IMPORT_ERROR = import_error

from app_logging import configure_logging
from config_loader import AppConfig, load_config

QuoteRecord = dict[str, str | None]
QuotesMap = dict[str, QuoteRecord]

logger = logging.getLogger(__name__)


def resolve_output_path(output_json: str) -> Path:
    output_path = Path(output_json)
    if output_path.is_absolute():
        return output_path
    return Path.cwd() / output_path


def normalize_asset_to_symbol(asset: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", asset.upper())


def resolve_symbol(config: AppConfig, asset: str) -> str:
    custom_symbol = config.scraper.symbol_map.get(asset)
    if custom_symbol:
        logger.debug("Using custom symbol mapping: %s -> %s", asset, custom_symbol)
        return custom_symbol
    return normalize_asset_to_symbol(asset)


def pick_price(bid: float, ask: float, last: float) -> float | None:
    if last and last > 0:
        return last

    if bid and bid > 0 and ask and ask > 0:
        return (bid + ask) / 2

    if bid and bid > 0:
        return bid

    if ask and ask > 0:
        return ask

    return None


def format_price(value: float, digits: int) -> str:
    safe_digits = max(0, min(8, digits))
    return f"{value:.{safe_digits}f}".rstrip("0").rstrip(".")


def initialize_mt5(config: AppConfig) -> None:
    if mt5 is None:
        details = ""
        if MT5_IMPORT_ERROR is not None:
            details = (
                f" Original import error: {type(MT5_IMPORT_ERROR).__name__}: "
                f"{MT5_IMPORT_ERROR}"
            )
        raise RuntimeError(
            "Package MetaTrader5 is not available in this runtime. "
            "For EXE build include MetaTrader5 and numpy dependencies."
            + details
        )

    init_kwargs: dict[str, object] = {
        "timeout": config.metatrader.timeout_ms,
    }

    if config.metatrader.terminal_path:
        init_kwargs["path"] = config.metatrader.terminal_path

    has_credentials = (
        config.metatrader.login > 0
        and bool(config.metatrader.password)
        and bool(config.metatrader.server)
    )
    if has_credentials:
        init_kwargs["login"] = config.metatrader.login
        init_kwargs["password"] = config.metatrader.password
        init_kwargs["server"] = config.metatrader.server

    mode = (
        "by account credentials"
        if has_credentials
        else "by attaching to a running terminal session"
    )
    logger.info(
        "Initializing MetaTrader 5 (%s), timeout_ms=%s, terminal_path=%s",
        mode,
        config.metatrader.timeout_ms,
        config.metatrader.terminal_path or "<auto>",
    )

    if not mt5.initialize(**init_kwargs):
        error_code, error_message = mt5.last_error()
        logger.error(
            "MetaTrader initialize failed (%s): %s %s",
            mode,
            error_code,
            error_message,
        )
        raise RuntimeError(
            f"MetaTrader initialize failed ({mode}): {error_code} {error_message}"
        )

    account = mt5.account_info()
    if account is None:
        mt5.shutdown()
        if has_credentials:
            logger.error("Connected to MT5, but account info is unavailable")
            raise RuntimeError("Connected to MT5, but account info is unavailable")
        logger.error("MT5 attached, but no logged-in account found")
        raise RuntimeError(
            "MT5 terminal is reachable, but no logged-in account found. "
            "Login in terminal manually or provide metatrader.login/password/server."
        )

    logger.info(
        "MetaTrader activated: account=%s server=%s company=%s",
        getattr(account, "login", "unknown"),
        getattr(account, "server", "unknown"),
        getattr(account, "company", "unknown"),
    )


def shutdown_mt5() -> None:
    if mt5 is not None:
        logger.info("Shutting down MetaTrader 5 connection")
        mt5.shutdown()


def read_symbol_quote(symbol: str) -> QuoteRecord:
    assert mt5 is not None

    logger.debug("Reading quote for symbol=%s", symbol)

    if not mt5.symbol_select(symbol, True):
        error_code, error_message = mt5.last_error()
        logger.warning(
            "symbol_select failed for %s: %s %s", symbol, error_code, error_message
        )
        return {
            "pair": symbol,
            "value": None,
            "error": f"symbol_select failed: {error_code} {error_message}",
        }

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        error_code, error_message = mt5.last_error()
        logger.warning(
            "symbol_info_tick failed for %s: %s %s", symbol, error_code, error_message
        )
        return {
            "pair": symbol,
            "value": None,
            "error": f"symbol_info_tick failed: {error_code} {error_message}",
        }

    info = mt5.symbol_info(symbol)
    bid = float(getattr(tick, "bid", 0.0) or 0.0)
    ask = float(getattr(tick, "ask", 0.0) or 0.0)
    last = float(getattr(tick, "last", 0.0) or 0.0)

    price = pick_price(bid, ask, last)
    if price is None:
        logger.warning("Tick price is empty for symbol=%s", symbol)
        return {
            "pair": symbol,
            "value": None,
            "error": "tick price is empty",
        }

    digits = int(getattr(info, "digits", 5) or 5)
    value_text = format_price(price, digits)

    return {
        "pair": symbol,
        "value": value_text,
        "bid": format_price(bid, digits) if bid > 0 else None,
        "ask": format_price(ask, digits) if ask > 0 else None,
    }


def collect_quotes(config: AppConfig, verbose: bool = True) -> QuotesMap:
    if not config.scraper.assets:
        raise ValueError("No assets found in config.scraper.assets")

    quotes: QuotesMap = {}
    logger.info("Collecting quotes for %s assets", len(config.scraper.assets))

    initialize_mt5(config)
    try:
        for asset in config.scraper.assets:
            symbol = resolve_symbol(config, asset)
            logger.info("Fetching asset=%s symbol=%s", asset, symbol)
            quote = read_symbol_quote(symbol)
            quotes[asset] = quote

            if quote.get("value"):
                if verbose:
                    logger.info(
                        "%s -> %s (pair=%s bid=%s ask=%s)",
                        asset,
                        quote.get("value"),
                        quote.get("pair"),
                        quote.get("bid"),
                        quote.get("ask"),
                    )
                else:
                    logger.debug(
                        "%s -> %s (pair=%s)",
                        asset,
                        quote.get("value"),
                        quote.get("pair"),
                    )
            else:
                logger.warning(
                    "%s failed: %s", asset, quote.get("error", "unknown error")
                )
    finally:
        shutdown_mt5()

    logger.info("Quote collection completed")
    return quotes


def build_quotes_payload(config: AppConfig, quotes: QuotesMap) -> dict[str, object]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "metatrader5",
        "source_server": config.metatrader.server,
        "quotes": quotes,
    }


def save_quotes(config: AppConfig, quotes: QuotesMap) -> Path:
    output_path = resolve_output_path(config.scraper.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = build_quotes_payload(config, quotes)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    logger.info("Saved quotes JSON to %s", output_path)
    return output_path


def run(config_path: Path) -> None:
    config = load_config(config_path)
    log_path = configure_logging(
        level=config.logging.level,
        file_path=config.logging.file,
        max_bytes=config.logging.max_bytes,
        backup_count=config.logging.backup_count,
    )

    logger.info("Logging initialized: %s", log_path)
    logger.info("Starting quote collector with config: %s", config_path)

    quotes = collect_quotes(config, verbose=True)
    save_quotes(config, quotes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect forex quotes using MetaTrader 5")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/site_config.yaml"),
        help="Path to YAML config file",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.config)
