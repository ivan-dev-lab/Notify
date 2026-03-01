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

    def get_market_quote(self, symbol: str) -> dict[str, object] | None:
        self._ensure_connected()
        assert mt5 is not None

        self._ensure_symbol_selected(symbol)
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            error_code, error_message = mt5.last_error()
            logger.warning(
                "symbol_info_tick returned None for %s: %s %s",
                symbol,
                error_code,
                error_message,
            )
            return None

        try:
            bid = float(getattr(tick, "bid", 0.0) or 0.0)
            ask = float(getattr(tick, "ask", 0.0) or 0.0)
            last = float(getattr(tick, "last", 0.0) or 0.0)
            tick_time_raw = int(getattr(tick, "time", 0) or 0)
        except (TypeError, ValueError):
            return None

        price = last
        if price <= 0:
            if bid > 0 and ask > 0:
                price = (bid + ask) / 2.0
            elif bid > 0:
                price = bid
            elif ask > 0:
                price = ask
        if price <= 0:
            return None

        if tick_time_raw > 0:
            tick_time = datetime.fromtimestamp(tick_time_raw, tz=timezone.utc)
        else:
            tick_time = datetime.now(timezone.utc)

        return {
            "price": price,
            "bid": bid if bid > 0 else None,
            "ask": ask if ask > 0 else None,
            "source": "MT5",
            "tick_time_utc": tick_time.isoformat(),
        }

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

    def fetch_range(
        self,
        *,
        symbol: str,
        timeframe_code: str,
        start_time_utc: datetime,
        end_time_utc: datetime,
    ) -> list[OHLCBar] | None:
        self._ensure_connected()
        assert mt5 is not None

        timeframe_value = resolve_mt5_timeframe(mt5, timeframe_code)
        self._ensure_symbol_selected(symbol)

        from_utc = start_time_utc.astimezone(timezone.utc).replace(microsecond=0)
        to_utc = end_time_utc.astimezone(timezone.utc).replace(microsecond=0)
        if to_utc <= from_utc:
            return []

        raw_rates = mt5.copy_rates_range(symbol, timeframe_value, from_utc, to_utc)
        if raw_rates is None:
            error_code, error_message = mt5.last_error()
            logger.warning(
                "backtest copy_rates_range returned None for %s %s: %s %s",
                symbol,
                timeframe_code,
                error_code,
                error_message,
            )
            return None

        bars = self._parse_rates(raw_rates)
        if len(bars) > 0:
            return bars

        fallback = self._fetch_range_fallback(
            symbol=symbol,
            timeframe_value=timeframe_value,
            timeframe_code=timeframe_code,
            from_utc=from_utc,
            to_utc=to_utc,
        )
        if len(fallback) > 0:
            return fallback

        logger.warning(
            "backtest copy_rates_range returned 0 bars for %s %s: from=%s to=%s",
            symbol,
            timeframe_code,
            from_utc.isoformat(),
            to_utc.isoformat(),
        )
        return []

    def _fetch_range_fallback(
        self,
        *,
        symbol: str,
        timeframe_value: int,
        timeframe_code: str,
        from_utc: datetime,
        to_utc: datetime,
    ) -> list[OHLCBar]:
        assert mt5 is not None

        count = self._estimate_bar_count(
            timeframe_code=timeframe_code,
            from_utc=from_utc,
            to_utc=to_utc,
        )

        raw_from = mt5.copy_rates_from(symbol, timeframe_value, to_utc, count)
        if raw_from is not None:
            from_bars = [
                bar
                for bar in self._parse_rates(raw_from)
                if from_utc <= bar.time <= to_utc
            ]
            if len(from_bars) > 0:
                logger.info(
                    "backtest fallback copy_rates_from used for %s %s: bars=%s",
                    symbol,
                    timeframe_code,
                    len(from_bars),
                )
                return from_bars

        raw_pos = mt5.copy_rates_from_pos(symbol, timeframe_value, 0, count)
        if raw_pos is None:
            error_code, error_message = mt5.last_error()
            logger.warning(
                "backtest fallback copy_rates_from_pos returned None for %s %s: %s %s",
                symbol,
                timeframe_code,
                error_code,
                error_message,
            )
            return []

        pos_bars = [
            bar
            for bar in self._parse_rates(raw_pos)
            if from_utc <= bar.time <= to_utc
        ]
        if len(pos_bars) > 0:
            logger.info(
                "backtest fallback copy_rates_from_pos used for %s %s: bars=%s",
                symbol,
                timeframe_code,
                len(pos_bars),
            )
        return pos_bars

    @staticmethod
    def _estimate_bar_count(
        *,
        timeframe_code: str,
        from_utc: datetime,
        to_utc: datetime,
    ) -> int:
        seconds = max(60, timeframe_to_seconds(timeframe_code))
        span_seconds = max(0, int((to_utc - from_utc).total_seconds()))
        estimate = (span_seconds // seconds) + 5
        return max(200, min(50000, estimate + 100))

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
