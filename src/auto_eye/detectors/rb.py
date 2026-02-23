from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime

from config_loader import AutoEyeConfig

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.detectors.fractal import FRACTAL_HIGH, FRACTAL_LOW, FractalDetector
from auto_eye.models import (
    OHLCBar,
    STATUS_ACTIVE,
    TrackedElement,
    datetime_from_iso,
    datetime_to_iso,
)

RB_STATUS_BROKEN = "broken"
RB_STATUS_EXPIRED = "expired"


class RBDetector(MarketElementDetector):
    element_type = "rb"

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

        if len(bars) < 3:
            return []

        fractals = self._fractals.detect(
            symbol=symbol,
            timeframe=timeframe,
            bars=bars,
            point_size=0.0,
            config=config,
        )
        detected: list[TrackedElement] = []
        for fractal in fractals:
            element = self._build_from_fractal(fractal=fractal)
            if element is None:
                continue
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

        if len(bars) == 0:
            return element

        rb_type = str(element.metadata.get("rb_type") or element.direction or "").strip().lower()
        if rb_type not in {FRACTAL_HIGH, FRACTAL_LOW}:
            l_value = self._metadata_float(
                element.metadata.get("l_price"),
                fallback=element.zone_low,
            )
            extreme_value = self._metadata_float(
                element.metadata.get("extreme_price"),
                fallback=element.zone_high,
            )
            rb_type = FRACTAL_HIGH if extreme_value >= l_value else FRACTAL_LOW

        rb_low = self._metadata_float(element.metadata.get("rb_low"), fallback=element.zone_low)
        rb_high = self._metadata_float(element.metadata.get("rb_high"), fallback=element.zone_high)
        pivot_time = self._metadata_time(
            element.metadata.get("pivot_time"),
            fallback=element.c2_time,
        )
        confirm_time = self._metadata_time(
            element.metadata.get("confirm_time"),
            fallback=element.formation_time,
        )
        origin_fractal_id = str(element.metadata.get("origin_fractal_id") or "").strip()
        broken_time = self._metadata_time_or_none(element.metadata.get("broken_time"))
        broken_side = str(element.metadata.get("broken_side") or "").strip().lower()
        status = str(element.status or STATUS_ACTIVE).strip().lower() or STATUS_ACTIVE

        for bar in bars:
            if bar.time <= confirm_time:
                continue
            if status != STATUS_ACTIVE:
                break
            if bar.close > rb_high:
                status = RB_STATUS_BROKEN
                broken_time = bar.time
                broken_side = "up"
                break
            if bar.close < rb_low:
                status = RB_STATUS_BROKEN
                broken_time = bar.time
                broken_side = "down"
                break

        l_price_default = rb_low if rb_type == FRACTAL_HIGH else rb_high
        extreme_price_default = rb_high if rb_type == FRACTAL_HIGH else rb_low
        l_price = self._metadata_float(element.metadata.get("l_price"), fallback=l_price_default)
        l_alt_price = self._metadata_float(element.metadata.get("l_alt_price"), fallback=l_price)
        extreme_price = self._metadata_float(
            element.metadata.get("extreme_price"),
            fallback=extreme_price_default,
        )

        element.direction = rb_type
        element.status = status
        element.zone_low = float(rb_low)
        element.zone_high = float(rb_high)
        element.zone_size = max(0.0, element.zone_high - element.zone_low)
        element.metadata.update(
            {
                "rb_type": rb_type,
                "origin_fractal_id": origin_fractal_id,
                "pivot_time": datetime_to_iso(pivot_time),
                "confirm_time": datetime_to_iso(confirm_time),
                "c1_time": datetime_to_iso(element.c1_time),
                "c2_time": datetime_to_iso(element.c2_time),
                "c3_time": datetime_to_iso(element.c3_time),
                "l_price": float(l_price),
                "l_alt_price": float(l_alt_price),
                "extreme_price": float(extreme_price),
                "rb_low": float(rb_low),
                "rb_high": float(rb_high),
                "broken_time": datetime_to_iso(broken_time),
                "broken_side": broken_side or None,
            }
        )
        element.mitigated_time = broken_time if status == RB_STATUS_BROKEN else None
        return element

    def _build_from_fractal(self, *, fractal: TrackedElement) -> TrackedElement | None:
        rb_type = str(fractal.metadata.get("fractal_type") or fractal.direction or "").strip().lower()
        if rb_type not in {FRACTAL_HIGH, FRACTAL_LOW}:
            return None

        pivot_time = self._metadata_time(fractal.metadata.get("pivot_time"), fallback=fractal.c2_time)
        confirm_time = self._metadata_time(
            fractal.metadata.get("confirm_time"),
            fallback=fractal.formation_time,
        )

        if rb_type == FRACTAL_HIGH:
            l_fallback = fractal.zone_low
            extreme_fallback = fractal.zone_high
        else:
            l_fallback = fractal.zone_high
            extreme_fallback = fractal.zone_low

        l_price = self._metadata_float(fractal.metadata.get("l_price"), fallback=l_fallback)
        l_alt_price = self._metadata_float(fractal.metadata.get("l_alt_price"), fallback=l_price)
        extreme_price = self._metadata_float(
            fractal.metadata.get("extreme_price"),
            fallback=extreme_fallback,
        )
        rb_low = min(l_price, extreme_price)
        rb_high = max(l_price, extreme_price)
        zone_size = max(0.0, rb_high - rb_low)

        element_id = self._build_id(
            symbol=fractal.symbol,
            timeframe=fractal.timeframe,
            rb_type=rb_type,
            pivot_time=pivot_time,
            l_price=l_price,
            extreme_price=extreme_price,
        )

        return TrackedElement(
            id=element_id,
            element_type=self.element_type,
            symbol=fractal.symbol,
            timeframe=fractal.timeframe,
            direction=rb_type,
            formation_time=confirm_time,
            zone_low=rb_low,
            zone_high=rb_high,
            zone_size=zone_size,
            c1_time=fractal.c1_time,
            c2_time=fractal.c2_time,
            c3_time=fractal.c3_time,
            status=STATUS_ACTIVE,
            metadata={
                "rb_type": rb_type,
                "origin_fractal_id": fractal.id,
                "pivot_time": datetime_to_iso(pivot_time),
                "confirm_time": datetime_to_iso(confirm_time),
                "c1_time": datetime_to_iso(fractal.c1_time),
                "c2_time": datetime_to_iso(fractal.c2_time),
                "c3_time": datetime_to_iso(fractal.c3_time),
                "l_price": float(l_price),
                "l_alt_price": float(l_alt_price),
                "extreme_price": float(extreme_price),
                "rb_low": float(rb_low),
                "rb_high": float(rb_high),
                "broken_time": None,
                "broken_side": None,
            },
        )

    @staticmethod
    def _build_id(
        *,
        symbol: str,
        timeframe: str,
        rb_type: str,
        pivot_time: datetime,
        l_price: float,
        extreme_price: float,
    ) -> str:
        seed = (
            f"rb|{symbol}|{timeframe}|{rb_type}|{pivot_time.isoformat()}|"
            f"{l_price:.10f}|{extreme_price:.10f}"
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

    @staticmethod
    def _metadata_time_or_none(value: object) -> datetime | None:
        if isinstance(value, datetime):
            return value
        return datetime_from_iso(str(value or ""))
