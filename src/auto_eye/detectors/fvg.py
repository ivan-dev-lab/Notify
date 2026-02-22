from __future__ import annotations

import hashlib
from collections.abc import Sequence
from statistics import median

from config_loader import AutoEyeConfig

from auto_eye.models import (
    OHLCBar,
    STATUS_ACTIVE,
    STATUS_MITIGATED_PARTIAL,
    STATUS_MITIGATED_FULL,
    STATUS_TOUCHED,
    TrackedElement,
)
from auto_eye.detectors.base import MarketElementDetector

BULLISH = "bullish"
BEARISH = "bearish"


class FVGDetector(MarketElementDetector):
    element_type = "fvg"

    def detect(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: Sequence[OHLCBar],
        point_size: float,
        config: AutoEyeConfig,
    ) -> list[TrackedElement]:
        if len(bars) < 3:
            return []

        detected: list[TrackedElement] = []
        gap_threshold = self._gap_threshold(config.min_gap_points, point_size)

        for i in range(2, len(bars)):
            c1 = bars[i - 2]
            c2 = bars[i - 1]
            c3 = bars[i]

            if config.require_displacement and not self._passes_displacement(
                bars=bars,
                c2_index=i - 1,
                config=config,
            ):
                continue

            if c1.high < c3.low:
                zone_low = c1.high
                zone_high = c3.low
                zone_size = zone_high - zone_low
                if zone_size >= gap_threshold:
                    detected.append(
                        self._build_element(
                            symbol=symbol,
                            timeframe=timeframe,
                            direction=BULLISH,
                            c1=c1,
                            c2=c2,
                            c3=c3,
                            zone_low=zone_low,
                            zone_high=zone_high,
                            zone_size=zone_size,
                        )
                    )

            if c1.low > c3.high:
                zone_low = c3.high
                zone_high = c1.low
                zone_size = zone_high - zone_low
                if zone_size >= gap_threshold:
                    detected.append(
                        self._build_element(
                            symbol=symbol,
                            timeframe=timeframe,
                            direction=BEARISH,
                            c1=c1,
                            c2=c2,
                            c3=c3,
                            zone_low=zone_low,
                            zone_high=zone_high,
                            zone_size=zone_size,
                        )
                    )

        return detected

    def update_status(
        self,
        *,
        element: TrackedElement,
        bars: Sequence[OHLCBar],
        config: AutoEyeConfig,
    ) -> TrackedElement:
        if element.status == STATUS_MITIGATED_FULL:
            return element

        if len(bars) == 0:
            return element

        fill_rule = (config.fill_rule or "both").strip().lower()
        if fill_rule not in {"touch", "full", "both"}:
            fill_rule = "both"

        max_depth = self._filled_depth_from_percent(
            zone_size=element.zone_size,
            fill_percent=element.fill_percent,
        )
        future_bars = [bar for bar in bars if bar.time > element.c3_time]
        if not future_bars:
            return element

        for bar in future_bars:
            is_touched = self._is_touched(
                zone_low=element.zone_low,
                zone_high=element.zone_high,
                bar=bar,
            )
            if is_touched and element.touched_time is None:
                element.touched_time = bar.time

            depth = self._fill_depth(
                direction=element.direction,
                zone_low=element.zone_low,
                zone_high=element.zone_high,
                bar=bar,
            )
            if depth > max_depth:
                max_depth = depth

            if fill_rule in {"both", "full"} and self._is_fully_mitigated(
                direction=element.direction,
                zone_low=element.zone_low,
                zone_high=element.zone_high,
                bar=bar,
            ):
                element.status = STATUS_MITIGATED_FULL
                if element.mitigated_time is None:
                    element.mitigated_time = bar.time
                element.fill_price = (
                    element.zone_low
                    if element.direction == BULLISH
                    else element.zone_high
                )
                max_depth = max(max_depth, element.zone_size)
                break

        if element.zone_size > 0:
            element.fill_percent = round(
                min(100.0, max_depth / element.zone_size * 100.0),
                2,
            )
        else:
            element.fill_percent = None

        if element.status != STATUS_MITIGATED_FULL:
            if max_depth > 0:
                element.status = STATUS_MITIGATED_PARTIAL
            elif element.touched_time is not None:
                element.status = STATUS_TOUCHED
            else:
                element.status = STATUS_ACTIVE

        return element

    def _build_element(
        self,
        *,
        symbol: str,
        timeframe: str,
        direction: str,
        c1: OHLCBar,
        c2: OHLCBar,
        c3: OHLCBar,
        zone_low: float,
        zone_high: float,
        zone_size: float,
    ) -> TrackedElement:
        return TrackedElement(
            id=self._build_id(
                symbol=symbol,
                timeframe=timeframe,
                direction=direction,
                formation_time=c3.time.isoformat(),
                zone_low=zone_low,
                zone_high=zone_high,
            ),
            element_type=self.element_type,
            symbol=symbol,
            timeframe=timeframe,
            direction=direction,
            formation_time=c3.time,
            zone_low=zone_low,
            zone_high=zone_high,
            zone_size=zone_size,
            c1_time=c1.time,
            c2_time=c2.time,
            c3_time=c3.time,
            status=STATUS_ACTIVE,
        )

    @staticmethod
    def _build_id(
        *,
        symbol: str,
        timeframe: str,
        direction: str,
        formation_time: str,
        zone_low: float,
        zone_high: float,
    ) -> str:
        seed = (
            f"fvg|{symbol}|{timeframe}|{direction}|{formation_time}|"
            f"{zone_low:.10f}|{zone_high:.10f}"
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]

    @staticmethod
    def _gap_threshold(min_gap_points: float, point_size: float) -> float:
        if min_gap_points <= 0:
            return 0.0
        if point_size > 0:
            return min_gap_points * point_size
        return min_gap_points

    @staticmethod
    def _passes_displacement(
        *,
        bars: Sequence[OHLCBar],
        c2_index: int,
        config: AutoEyeConfig,
    ) -> bool:
        if c2_index <= 0 or c2_index >= len(bars):
            return True

        c2 = bars[c2_index]
        body = abs(c2.close - c2.open)
        if body <= 0:
            return False

        atr_value = FVGDetector._atr(
            bars=bars,
            end_index=c2_index,
            period=max(1, config.atr_period),
        )
        baseline = atr_value
        if baseline is None or baseline <= 0:
            baseline = FVGDetector._median_body(
                bars=bars,
                end_index=c2_index,
                period=max(1, config.median_body_period),
            )
        if baseline is None or baseline <= 0:
            return True
        return body >= float(config.displacement_k) * baseline

    @staticmethod
    def _median_body(
        *,
        bars: Sequence[OHLCBar],
        end_index: int,
        period: int,
    ) -> float | None:
        start_index = max(0, end_index - period + 1)
        values = [abs(bar.close - bar.open) for bar in bars[start_index : end_index + 1]]
        if not values:
            return None
        return float(median(values))

    @staticmethod
    def _atr(
        *,
        bars: Sequence[OHLCBar],
        end_index: int,
        period: int,
    ) -> float | None:
        if end_index <= 0:
            return None

        start_index = max(1, end_index - period + 1)
        trs: list[float] = []
        for index in range(start_index, end_index + 1):
            current = bars[index]
            previous = bars[index - 1]
            tr = max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
            trs.append(max(0.0, tr))

        if not trs:
            return None
        return sum(trs) / len(trs)

    @staticmethod
    def _is_touched(*, zone_low: float, zone_high: float, bar: OHLCBar) -> bool:
        return bar.low <= zone_high and bar.high >= zone_low

    @staticmethod
    def _is_fully_mitigated(
        *,
        direction: str,
        zone_low: float,
        zone_high: float,
        bar: OHLCBar,
    ) -> bool:
        if direction == BULLISH:
            return bar.low <= zone_low
        return bar.high >= zone_high

    @staticmethod
    def _fill_depth(
        *,
        direction: str,
        zone_low: float,
        zone_high: float,
        bar: OHLCBar,
    ) -> float:
        if direction == BULLISH:
            if bar.low >= zone_high:
                return 0.0
            return max(0.0, zone_high - min(zone_high, bar.low))
        if bar.high <= zone_low:
            return 0.0
        return max(0.0, min(zone_high, bar.high) - zone_low)

    @staticmethod
    def _filled_depth_from_percent(
        *,
        zone_size: float,
        fill_percent: float | None,
    ) -> float:
        if fill_percent is None or zone_size <= 0:
            return 0.0
        return max(0.0, min(zone_size, zone_size * (fill_percent / 100.0)))
