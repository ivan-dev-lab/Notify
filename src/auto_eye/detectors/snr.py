from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime

from config_loader import AutoEyeConfig

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.detectors.fractal import FractalDetector
from auto_eye.models import (
    OHLCBar,
    STATUS_ACTIVE,
    STATUS_INVALIDATED,
    STATUS_RETESTED,
    TrackedElement,
    datetime_from_iso,
    datetime_to_iso,
)

ROLE_SUPPORT = "support"
ROLE_RESISTANCE = "resistance"
BREAK_UP_CLOSE = "break_up_close"
BREAK_DOWN_CLOSE = "break_down_close"


class SNRDetector(MarketElementDetector):
    element_type = "snr"

    def __init__(self) -> None:
        self._fractals = FractalDetector()
        self._fractal_cache_key: tuple[str, str, datetime, datetime, int] | None = None
        self._fractal_cache: dict[str, TrackedElement] = {}

    def detect(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: Sequence[OHLCBar],
        point_size: float,
        config: AutoEyeConfig,
    ) -> list[TrackedElement]:
        del point_size

        if len(bars) < 4:
            return []

        index_by_time = {bar.time: idx for idx, bar in enumerate(bars)}
        fractals = self._fractals.detect(
            symbol=symbol,
            timeframe=timeframe,
            bars=bars,
            point_size=0.0,
            config=config,
        )

        detected: list[TrackedElement] = []
        for fractal in fractals:
            confirm_index = index_by_time.get(fractal.c3_time)
            if confirm_index is None:
                continue

            break_data = self._find_break(
                bars=bars,
                l_price=self._metadata_float(
                    fractal.metadata.get("l_price"),
                    fallback=fractal.zone_low,
                ),
                start_index=confirm_index + 1,
            )
            if break_data is None:
                continue

            role, break_type, break_bar = break_data
            element = self._build_snr_from_fractal(
                symbol=symbol,
                timeframe=timeframe,
                fractal=fractal,
                role=role,
                break_type=break_type,
                break_bar=break_bar,
                bars=bars,
                config=config,
            )
            if element is None:
                continue
            self.update_status(element=element, bars=bars, config=config)
            detected.append(element)

        return detected

    def update_status(
        self,
        *,
        element: TrackedElement,
        bars: Sequence[OHLCBar],
        config: AutoEyeConfig,
    ) -> TrackedElement:
        if len(bars) == 0:
            return element

        self._refresh_zone_from_bars(element=element, bars=bars, config=config)

        role = str(element.metadata.get("role") or element.direction or ROLE_SUPPORT)
        snr_low = self._metadata_float(
            element.metadata.get("snr_low"),
            fallback=element.zone_low,
        )
        snr_high = self._metadata_float(
            element.metadata.get("snr_high"),
            fallback=element.zone_high,
        )
        break_time = self._metadata_time(
            element.metadata.get("break_time"),
            fallback=element.formation_time,
        )

        future_bars = [bar for bar in bars if bar.time > break_time]
        if not future_bars:
            self._sync_status_timestamps(element)
            return element

        if element.status != STATUS_INVALIDATED:
            for bar in future_bars:
                if role == ROLE_SUPPORT and bar.close < snr_low:
                    element.status = STATUS_INVALIDATED
                    if element.mitigated_time is None:
                        element.mitigated_time = bar.time
                    break

                if role == ROLE_RESISTANCE and bar.close > snr_high:
                    element.status = STATUS_INVALIDATED
                    if element.mitigated_time is None:
                        element.mitigated_time = bar.time
                    break

                if (
                    element.status == STATUS_ACTIVE
                    and bar.high >= snr_low
                    and bar.low <= snr_high
                ):
                    element.status = STATUS_RETESTED
                    if element.touched_time is None:
                        element.touched_time = bar.time

        self._sync_status_timestamps(element)
        return element

    def _build_snr_from_fractal(
        self,
        *,
        symbol: str,
        timeframe: str,
        fractal: TrackedElement,
        role: str,
        break_type: str,
        break_bar: OHLCBar,
        bars: Sequence[OHLCBar],
        config: AutoEyeConfig,
    ) -> TrackedElement | None:
        l_price = self._metadata_float(
            fractal.metadata.get("l_price"),
            fallback=fractal.zone_low,
        )
        fractal_extreme_price = self._metadata_float(
            fractal.metadata.get("extreme_price"),
            fallback=fractal.zone_high,
        )

        start_time = self._fractal_start_time(fractal=fractal, config=config)
        include_break = bool(getattr(config, "snr_include_break_candle", False))
        departure_price, departure_time = self._find_departure_extreme(
            bars=bars,
            role=role,
            start_time=start_time,
            break_time=break_bar.time,
            include_break_candle=include_break,
        )
        if departure_price is None or departure_time is None:
            departure_price = fractal_extreme_price
            departure_time = fractal.c2_time

        if role == ROLE_SUPPORT:
            snr_low = float(departure_price)
            snr_high = float(l_price)
        else:
            snr_low = float(l_price)
            snr_high = float(departure_price)

        break_time_iso = break_bar.time.isoformat()
        element = TrackedElement(
            id=self._build_id(
                symbol=symbol,
                timeframe=timeframe,
                origin_fractal_id=fractal.id,
                break_time=break_time_iso,
                role=role,
                break_type=break_type,
            ),
            element_type=self.element_type,
            symbol=symbol,
            timeframe=timeframe,
            direction=role,
            formation_time=break_bar.time,
            zone_low=snr_low,
            zone_high=snr_high,
            zone_size=max(0.0, snr_high - snr_low),
            c1_time=break_bar.time,
            c2_time=break_bar.time,
            c3_time=break_bar.time,
            status=STATUS_ACTIVE,
            fill_price=float(break_bar.close),
            metadata={
                "origin_fractal_id": fractal.id,
                "role": role,
                "break_type": break_type,
                "break_time": break_time_iso,
                "break_close": float(break_bar.close),
                "l_price": float(l_price),
                "extreme_price": float(fractal_extreme_price),
                "departure_extreme_price": float(departure_price),
                "departure_extreme_time": datetime_to_iso(departure_time),
                "departure_range_start_time": datetime_to_iso(start_time),
                "departure_range_end_time": break_time_iso,
                "snr_low": float(snr_low),
                "snr_high": float(snr_high),
                "retest_time": None,
                "invalidated_time": None,
            },
        )
        return element

    def _refresh_zone_from_bars(
        self,
        *,
        element: TrackedElement,
        bars: Sequence[OHLCBar],
        config: AutoEyeConfig,
    ) -> None:
        role = str(element.metadata.get("role") or element.direction or ROLE_SUPPORT)
        break_type = str(element.metadata.get("break_type") or "")
        if break_type not in {BREAK_UP_CLOSE, BREAK_DOWN_CLOSE}:
            break_type = BREAK_UP_CLOSE if role == ROLE_SUPPORT else BREAK_DOWN_CLOSE
        break_time = self._metadata_time(
            element.metadata.get("break_time"),
            fallback=element.formation_time,
        )
        break_bar = self._bar_at_time(bars=bars, bar_time=break_time)

        l_price = self._metadata_float(
            element.metadata.get("l_price"),
            fallback=(element.zone_high if role == ROLE_SUPPORT else element.zone_low),
        )

        origin_fractal_id = str(element.metadata.get("origin_fractal_id") or "")
        origin_fractal = None
        if origin_fractal_id:
            lookup = self._get_fractal_lookup(
                symbol=element.symbol,
                timeframe=element.timeframe,
                bars=bars,
                config=config,
            )
            origin_fractal = lookup.get(origin_fractal_id)

        range_start = self._metadata_time_or_none(
            element.metadata.get("departure_range_start_time")
        )
        if range_start is None and origin_fractal is not None:
            range_start = self._fractal_start_time(fractal=origin_fractal, config=config)
        if range_start is None:
            range_start = break_time

        include_break = bool(getattr(config, "snr_include_break_candle", False))
        departure_price, departure_time = self._find_departure_extreme(
            bars=bars,
            role=role,
            start_time=range_start,
            break_time=break_time,
            include_break_candle=include_break,
        )
        if departure_price is None:
            departure_price = self._metadata_float(
                element.metadata.get("departure_extreme_price"),
                fallback=(element.zone_low if role == ROLE_SUPPORT else element.zone_high),
            )
        if departure_time is None:
            departure_time = self._metadata_time(
                element.metadata.get("departure_extreme_time"),
                fallback=break_time,
            )

        fractal_extreme_price = self._metadata_float(
            element.metadata.get("extreme_price"),
            fallback=(origin_fractal.zone_high if origin_fractal is not None else element.zone_high),
        )
        if origin_fractal is not None:
            fractal_extreme_price = self._metadata_float(
                origin_fractal.metadata.get("extreme_price"),
                fallback=fractal_extreme_price,
            )

        if role == ROLE_SUPPORT:
            snr_low = float(departure_price)
            snr_high = float(l_price)
        else:
            snr_low = float(l_price)
            snr_high = float(departure_price)

        break_close = self._metadata_optional_float(element.metadata.get("break_close"))
        if break_close is None and break_bar is not None:
            break_close = float(break_bar.close)

        element.direction = role
        element.zone_low = snr_low
        element.zone_high = snr_high
        element.zone_size = max(0.0, snr_high - snr_low)
        element.fill_price = break_close

        element.metadata["origin_fractal_id"] = origin_fractal_id
        element.metadata["role"] = role
        element.metadata["break_type"] = break_type
        element.metadata["break_time"] = datetime_to_iso(break_time)
        element.metadata["break_close"] = break_close
        element.metadata["l_price"] = float(l_price)
        element.metadata["extreme_price"] = float(fractal_extreme_price)
        element.metadata["departure_extreme_price"] = float(departure_price)
        element.metadata["departure_extreme_time"] = datetime_to_iso(departure_time)
        element.metadata["departure_range_start_time"] = datetime_to_iso(range_start)
        element.metadata["departure_range_end_time"] = datetime_to_iso(break_time)
        element.metadata["snr_low"] = float(snr_low)
        element.metadata["snr_high"] = float(snr_high)

    def _get_fractal_lookup(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: Sequence[OHLCBar],
        config: AutoEyeConfig,
    ) -> dict[str, TrackedElement]:
        if len(bars) == 0:
            return {}
        cache_key = (
            symbol,
            timeframe.upper(),
            bars[0].time,
            bars[-1].time,
            len(bars),
        )
        if self._fractal_cache_key == cache_key:
            return self._fractal_cache

        fractals = self._fractals.detect(
            symbol=symbol,
            timeframe=timeframe,
            bars=bars,
            point_size=0.0,
            config=config,
        )
        self._fractal_cache_key = cache_key
        self._fractal_cache = {item.id: item for item in fractals}
        return self._fractal_cache

    @staticmethod
    def _find_break(
        *,
        bars: Sequence[OHLCBar],
        l_price: float,
        start_index: int,
    ) -> tuple[str, str, OHLCBar] | None:
        start = max(1, start_index)
        for index in range(start, len(bars)):
            previous_close = bars[index - 1].close
            current_close = bars[index].close
            if current_close > l_price and previous_close <= l_price:
                return ROLE_SUPPORT, BREAK_UP_CLOSE, bars[index]
            if current_close < l_price and previous_close >= l_price:
                return ROLE_RESISTANCE, BREAK_DOWN_CLOSE, bars[index]
        return None

    @staticmethod
    def _find_departure_extreme(
        *,
        bars: Sequence[OHLCBar],
        role: str,
        start_time: datetime,
        break_time: datetime,
        include_break_candle: bool,
    ) -> tuple[float | None, datetime | None]:
        selected_bars: list[OHLCBar] = []
        for bar in bars:
            if bar.time < start_time:
                continue
            if include_break_candle:
                if bar.time > break_time:
                    continue
            else:
                if bar.time >= break_time:
                    continue
            selected_bars.append(bar)

        if not selected_bars:
            return None, None

        chosen_price: float | None = None
        chosen_time: datetime | None = None
        for bar in selected_bars:
            if role == ROLE_SUPPORT:
                candidate = float(bar.low)
                if chosen_price is None or candidate < chosen_price:
                    chosen_price = candidate
                    chosen_time = bar.time
            else:
                candidate = float(bar.high)
                if chosen_price is None or candidate > chosen_price:
                    chosen_price = candidate
                    chosen_time = bar.time

        return chosen_price, chosen_time

    @staticmethod
    def _fractal_start_time(*, fractal: TrackedElement, config: AutoEyeConfig) -> datetime:
        mode = str(getattr(config, "snr_departure_start", "pivot") or "pivot").strip().lower()
        pivot_time = datetime_from_iso(str(fractal.metadata.get("pivot_time") or ""))
        if pivot_time is None:
            pivot_time = fractal.c2_time
        confirm_time = datetime_from_iso(str(fractal.metadata.get("confirm_time") or ""))
        if confirm_time is None:
            confirm_time = fractal.c3_time
        if mode == "confirm":
            return confirm_time
        return pivot_time

    def _sync_status_timestamps(self, element: TrackedElement) -> None:
        if element.touched_time is not None:
            element.metadata["retest_time"] = datetime_to_iso(element.touched_time)
        if element.mitigated_time is not None:
            element.metadata["invalidated_time"] = datetime_to_iso(element.mitigated_time)

    @staticmethod
    def _build_id(
        *,
        symbol: str,
        timeframe: str,
        origin_fractal_id: str,
        break_time: str,
        role: str,
        break_type: str,
    ) -> str:
        seed = (
            f"snr|{symbol}|{timeframe}|{origin_fractal_id}|"
            f"{break_time}|{role}|{break_type}"
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]

    @staticmethod
    def _bar_at_time(*, bars: Sequence[OHLCBar], bar_time: datetime) -> OHLCBar | None:
        for bar in bars:
            if bar.time == bar_time:
                return bar
        return None

    @staticmethod
    def _metadata_float(value: object, *, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    @staticmethod
    def _metadata_optional_float(value: object) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _metadata_time(value: object, *, fallback: datetime) -> datetime:
        if isinstance(value, datetime):
            return value
        parsed = datetime_from_iso(str(value or ""))
        if parsed is None:
            return fallback
        return parsed

    @staticmethod
    def _metadata_time_or_none(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value
        return datetime_from_iso(str(value or ""))