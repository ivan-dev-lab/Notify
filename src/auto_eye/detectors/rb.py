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
L_RULE_BEARISH = "bearish_C1close"
L_RULE_BULLISH = "bullish_C2close"


class RBDetector(MarketElementDetector):
    element_type = "rb"

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
        if len(bars) == 0:
            return element

        rb_type = self._resolve_rb_type(element)
        pivot_time = self._metadata_time(
            element.metadata.get("pivot_time"),
            fallback=element.c2_time,
        )
        confirm_time = self._metadata_time(
            element.metadata.get("confirm_time"),
            fallback=element.formation_time,
        )
        origin_fractal_id = str(element.metadata.get("origin_fractal_id") or "").strip()

        origin_fractal = None
        if origin_fractal_id:
            lookup = self._get_fractal_lookup(
                symbol=element.symbol,
                timeframe=element.timeframe,
                bars=bars,
                config=config,
            )
            origin_fractal = lookup.get(origin_fractal_id)

        if origin_fractal is not None:
            (
                l_price_bearish,
                l_alt_bearish,
                l_price_bullish,
                l_alt_bullish,
            ) = self._line_bundle_from_fractal(origin_fractal)
            extreme_price = self._metadata_float(
                origin_fractal.metadata.get("extreme_price"),
                fallback=self._fractal_extreme_fallback(origin_fractal),
            )
        else:
            (
                l_price_bearish,
                l_alt_bearish,
                l_price_bullish,
                l_alt_bullish,
            ) = self._line_bundle_from_rb_metadata(element=element)
            extreme_price = self._metadata_float(
                element.metadata.get("extreme_price"),
                fallback=(element.zone_high if rb_type == FRACTAL_HIGH else element.zone_low),
            )

        (
            line_used,
            line_alt_used,
            line_rule_used,
        ) = self._line_for_rb_type(
            rb_type=rb_type,
            l_price_bearish=l_price_bearish,
            l_alt_bearish=l_alt_bearish,
            l_price_bullish=l_price_bullish,
            l_alt_bullish=l_alt_bullish,
        )
        rb_low = min(line_used, extreme_price)
        rb_high = max(line_used, extreme_price)

        broken_time = self._metadata_time_or_none(element.metadata.get("broken_time"))
        broken_side = str(element.metadata.get("broken_side") or "").strip().lower()
        status = str(element.status or STATUS_ACTIVE).strip().lower() or STATUS_ACTIVE

        for bar in bars:
            if bar.time <= confirm_time:
                continue
            if status != STATUS_ACTIVE:
                break

            if rb_type == FRACTAL_HIGH and (bar.close > rb_high or bar.high > rb_high):
                status = RB_STATUS_BROKEN
                broken_time = bar.time
                broken_side = "up"
                break

            if rb_type == FRACTAL_LOW and (bar.close < rb_low or bar.low < rb_low):
                status = RB_STATUS_BROKEN
                broken_time = bar.time
                broken_side = "down"
                break

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
                "l_price": float(line_used),
                "l_alt_price": float(line_alt_used),
                "l_price_bearish": float(l_price_bearish),
                "l_alt_bearish": float(l_alt_bearish),
                "l_price_bullish": float(l_price_bullish),
                "l_alt_bullish": float(l_alt_bullish),
                "l_price_used": float(line_used),
                "l_rule_used": line_rule_used,
                "line_used": float(line_used),
                "line_rule_used": line_rule_used,
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

        (
            l_price_bearish,
            l_alt_bearish,
            l_price_bullish,
            l_alt_bullish,
        ) = self._line_bundle_from_fractal(fractal)
        (
            line_used,
            line_alt_used,
            line_rule_used,
        ) = self._line_for_rb_type(
            rb_type=rb_type,
            l_price_bearish=l_price_bearish,
            l_alt_bearish=l_alt_bearish,
            l_price_bullish=l_price_bullish,
            l_alt_bullish=l_alt_bullish,
        )

        extreme_price = self._metadata_float(
            fractal.metadata.get("extreme_price"),
            fallback=self._fractal_extreme_fallback(fractal),
        )
        rb_low = min(line_used, extreme_price)
        rb_high = max(line_used, extreme_price)
        zone_size = max(0.0, rb_high - rb_low)

        element_id = self._build_id(
            symbol=fractal.symbol,
            timeframe=fractal.timeframe,
            rb_type=rb_type,
            pivot_time=pivot_time,
            line_used=line_used,
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
                "l_price": float(line_used),
                "l_alt_price": float(line_alt_used),
                "l_price_bearish": float(l_price_bearish),
                "l_alt_bearish": float(l_alt_bearish),
                "l_price_bullish": float(l_price_bullish),
                "l_alt_bullish": float(l_alt_bullish),
                "l_price_used": float(line_used),
                "l_rule_used": line_rule_used,
                "line_used": float(line_used),
                "line_rule_used": line_rule_used,
                "extreme_price": float(extreme_price),
                "rb_low": float(rb_low),
                "rb_high": float(rb_high),
                "broken_time": None,
                "broken_side": None,
            },
        )

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
    def _resolve_rb_type(element: TrackedElement) -> str:
        rb_type = str(element.metadata.get("rb_type") or element.direction or "").strip().lower()
        if rb_type in {FRACTAL_HIGH, FRACTAL_LOW}:
            return rb_type

        l_value = RBDetector._metadata_float(
            element.metadata.get("l_price"),
            fallback=element.zone_low,
        )
        extreme_value = RBDetector._metadata_float(
            element.metadata.get("extreme_price"),
            fallback=element.zone_high,
        )
        return FRACTAL_HIGH if extreme_value >= l_value else FRACTAL_LOW

    @staticmethod
    def _line_for_rb_type(
        *,
        rb_type: str,
        l_price_bearish: float,
        l_alt_bearish: float,
        l_price_bullish: float,
        l_alt_bullish: float,
    ) -> tuple[float, float, str]:
        if rb_type == FRACTAL_LOW:
            return float(l_price_bullish), float(l_alt_bullish), L_RULE_BULLISH
        return float(l_price_bearish), float(l_alt_bearish), L_RULE_BEARISH

    @staticmethod
    def _fractal_extreme_fallback(fractal: TrackedElement) -> float:
        fractal_type = str(fractal.metadata.get("fractal_type") or fractal.direction or "").strip().lower()
        if fractal_type == FRACTAL_LOW:
            return float(fractal.zone_low)
        return float(fractal.zone_high)

    def _line_bundle_from_fractal(
        self,
        fractal: TrackedElement,
    ) -> tuple[float, float, float, float]:
        fractal_type = str(fractal.metadata.get("fractal_type") or fractal.direction or "").strip().lower()
        if fractal_type == FRACTAL_LOW:
            legacy_l_fallback = float(fractal.zone_high)
        else:
            legacy_l_fallback = float(fractal.zone_low)

        l_price_bearish = self._metadata_float(
            fractal.metadata.get("l_price_bearish"),
            fallback=self._metadata_float(fractal.metadata.get("l_price"), fallback=legacy_l_fallback),
        )
        l_alt_bearish = self._metadata_float(
            fractal.metadata.get("l_alt_bearish"),
            fallback=self._metadata_float(fractal.metadata.get("l_alt_price"), fallback=l_price_bearish),
        )
        l_price_bullish = self._metadata_float(
            fractal.metadata.get("l_price_bullish"),
            fallback=l_price_bearish,
        )
        l_alt_bullish = self._metadata_float(
            fractal.metadata.get("l_alt_bullish"),
            fallback=l_price_bullish,
        )
        return (
            float(l_price_bearish),
            float(l_alt_bearish),
            float(l_price_bullish),
            float(l_alt_bullish),
        )

    def _line_bundle_from_rb_metadata(
        self,
        *,
        element: TrackedElement,
    ) -> tuple[float, float, float, float]:
        legacy_l = self._metadata_float(
            element.metadata.get("l_price"),
            fallback=(element.zone_low + element.zone_high) / 2,
        )
        legacy_alt = self._metadata_float(
            element.metadata.get("l_alt_price"),
            fallback=legacy_l,
        )
        l_price_bearish = self._metadata_float(
            element.metadata.get("l_price_bearish"),
            fallback=legacy_l,
        )
        l_alt_bearish = self._metadata_float(
            element.metadata.get("l_alt_bearish"),
            fallback=legacy_alt,
        )

        l_price_used = self._metadata_float(
            element.metadata.get("l_price_used"),
            fallback=legacy_l,
        )
        l_rule_used = str(element.metadata.get("l_rule_used") or "").strip()
        bullish_fallback = l_price_used if l_rule_used == L_RULE_BULLISH else legacy_l

        l_price_bullish = self._metadata_float(
            element.metadata.get("l_price_bullish"),
            fallback=bullish_fallback,
        )
        l_alt_bullish = self._metadata_float(
            element.metadata.get("l_alt_bullish"),
            fallback=legacy_alt,
        )

        return (
            float(l_price_bearish),
            float(l_alt_bearish),
            float(l_price_bullish),
            float(l_alt_bullish),
        )

    @staticmethod
    def _build_id(
        *,
        symbol: str,
        timeframe: str,
        rb_type: str,
        pivot_time: datetime,
        line_used: float,
        extreme_price: float,
    ) -> str:
        seed = (
            f"rb|{symbol}|{timeframe}|{rb_type}|{pivot_time.isoformat()}|"
            f"{line_used:.10f}|{extreme_price:.10f}"
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
