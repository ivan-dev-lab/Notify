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
    TrackedElement,
    datetime_from_iso,
    datetime_to_iso,
)

ROLE_SUPPORT = "support"
ROLE_RESISTANCE = "resistance"
BREAK_UP_CLOSE = "break_up_close"
BREAK_DOWN_CLOSE = "break_down_close"
STATUS_RETESTED = "retested"
STATUS_INVALIDATED = "invalidated"


class SNRDetector(MarketElementDetector):
    element_type = "snr"

    def __init__(self) -> None:
        self._fractals = FractalDetector()

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
            l_price = self._metadata_float(
                fractal.metadata.get("l_price"),
                fallback=fractal.zone_low,
            )
            extreme_price = self._metadata_float(
                fractal.metadata.get("extreme_price"),
                fallback=fractal.zone_high,
            )
            snr_low = min(l_price, extreme_price)
            snr_high = max(l_price, extreme_price)

            element = TrackedElement(
                id=self._build_id(
                    symbol=symbol,
                    timeframe=timeframe,
                    origin_fractal_id=fractal.id,
                    break_time=break_bar.time.isoformat(),
                    role=role,
                    snr_low=snr_low,
                    snr_high=snr_high,
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
                metadata={
                    "origin_fractal_id": fractal.id,
                    "role": role,
                    "break_type": break_type,
                    "break_time": break_bar.time.isoformat(),
                    "break_close": float(break_bar.close),
                    "l_price": l_price,
                    "extreme_price": extreme_price,
                    "snr_low": snr_low,
                    "snr_high": snr_high,
                    "retest_time": None,
                    "invalidated_time": None,
                },
            )
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
        del config

        if element.status == STATUS_INVALIDATED or len(bars) == 0:
            return element

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
            return element

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

        if element.touched_time is not None:
            element.metadata["retest_time"] = datetime_to_iso(element.touched_time)
        if element.mitigated_time is not None:
            element.metadata["invalidated_time"] = datetime_to_iso(element.mitigated_time)

        return element

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
    def _build_id(
        *,
        symbol: str,
        timeframe: str,
        origin_fractal_id: str,
        break_time: str,
        role: str,
        snr_low: float,
        snr_high: float,
    ) -> str:
        seed = (
            f"snr|{symbol}|{timeframe}|{origin_fractal_id}|{break_time}|{role}|"
            f"{snr_low:.10f}|{snr_high:.10f}"
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]

    @staticmethod
    def _metadata_float(value: object, *, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    @staticmethod
    def _metadata_time(value: object, *, fallback: datetime) -> datetime:
        if isinstance(value, datetime):
            return value
        parsed = datetime_from_iso(str(value or ""))
        if parsed is None:
            return fallback
        return parsed
