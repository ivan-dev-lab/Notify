from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from config_loader import AppConfig
from main import initialize_mt5, mt5, resolve_symbol, shutdown_mt5

from auto_eye.models import OHLCBar
from auto_eye.timeframes import resolve_mt5_timeframe, timeframe_to_seconds

logger = logging.getLogger(__name__)


class MT5BarsSource:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._connected = False

    def connect(self) -> None:
        if self._connected:
            return
        initialize_mt5(self.config)
        self._connected = True

    def close(self) -> None:
        if not self._connected:
            return
        shutdown_mt5()
        self._connected = False

    def resolve_symbol(self, asset_or_symbol: str) -> str:
        # Supports both assets from scraper config and direct MT5 symbols.
        raw_value = str(asset_or_symbol).strip()
        if not raw_value:
            return raw_value

        upper_value = raw_value.upper()
        if upper_value in self.config.scraper.assets:
            return resolve_symbol(self.config, upper_value)

        mapped = self.config.scraper.symbol_map.get(upper_value)
        if mapped:
            return mapped

        return raw_value

    def get_point_size(self, symbol: str) -> float:
        self._ensure_connected()
        assert mt5 is not None

        info = mt5.symbol_info(symbol)
        if info is None:
            return 0.0
        try:
            return float(getattr(info, "point", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def fetch_history(
        self,
        *,
        symbol: str,
        timeframe_code: str,
        history_days: int,
        history_buffer_days: int,
    ) -> list[OHLCBar] | None:
        self._ensure_connected()
        assert mt5 is not None

        timeframe_value = resolve_mt5_timeframe(mt5, timeframe_code)
        total_days = max(1, history_days + history_buffer_days)
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(days=total_days)

        self._ensure_symbol_selected(symbol)
        raw_rates = mt5.copy_rates_range(symbol, timeframe_value, start_utc, now_utc)
        if raw_rates is None:
            error_code, error_message = mt5.last_error()
            logger.warning(
                "copy_rates_range returned None for %s %s: %s %s",
                symbol,
                timeframe_code,
                error_code,
                error_message,
            )
            return None
        return self._parse_rates(raw_rates)

    def fetch_incremental(
        self,
        *,
        symbol: str,
        timeframe_code: str,
        last_bar_time: datetime,
        incremental_bars: int,
        history_days: int,
        history_buffer_days: int,
    ) -> list[OHLCBar] | None:
        self._ensure_connected()
        assert mt5 is not None

        timeframe_value = resolve_mt5_timeframe(mt5, timeframe_code)
        self._ensure_symbol_selected(symbol)

        now_utc = datetime.now(timezone.utc)
        seconds = timeframe_to_seconds(timeframe_code)
        rewind = timedelta(seconds=max(60, seconds * 4))
        from_utc = (last_bar_time.astimezone(timezone.utc) - rewind).replace(
            microsecond=0
        )
        history_limit = now_utc - timedelta(days=max(1, history_days + history_buffer_days))
        if from_utc < history_limit:
            from_utc = history_limit

        raw_rates = mt5.copy_rates_range(symbol, timeframe_value, from_utc, now_utc)
        if raw_rates is None:
            error_code, error_message = mt5.last_error()
            logger.warning(
                "incremental copy_rates_range returned None for %s %s: %s %s",
                symbol,
                timeframe_code,
                error_code,
                error_message,
            )
            return None

        bars = self._parse_rates(raw_rates)
        if len(bars) >= 3:
            return bars

        fallback_count = max(20, incremental_bars)
        fallback_rates = mt5.copy_rates_from_pos(symbol, timeframe_value, 0, fallback_count)
        if fallback_rates is None:
            error_code, error_message = mt5.last_error()
            logger.warning(
                "copy_rates_from_pos returned None for %s %s: %s %s",
                symbol,
                timeframe_code,
                error_code,
                error_message,
            )
            return None
        return self._parse_rates(fallback_rates)

    def _ensure_connected(self) -> None:
        if not self._connected:
            self.connect()

    def _ensure_symbol_selected(self, symbol: str) -> None:
        assert mt5 is not None
        if mt5.symbol_select(symbol, True):
            return
        error_code, error_message = mt5.last_error()
        raise RuntimeError(
            f"symbol_select failed for {symbol}: {error_code} {error_message}"
        )

    @staticmethod
    def _parse_rates(raw_rates: object) -> list[OHLCBar]:

        bars: list[OHLCBar] = []
        for row in raw_rates:
            try:
                bar_time = datetime.fromtimestamp(int(row["time"]), tz=timezone.utc)
                open_price = float(row["open"])
                high_price = float(row["high"])
                low_price = float(row["low"])
                close_price = float(row["close"])
                tick_volume = int(row["tick_volume"])
            except (KeyError, TypeError, ValueError):
                continue

            bars.append(
                OHLCBar(
                    time=bar_time,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    tick_volume=tick_volume,
                )
            )

        bars.sort(key=lambda item: item.time)
        return bars
