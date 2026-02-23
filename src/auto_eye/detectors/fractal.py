from __future__ import annotations

import hashlib
from collections.abc import Sequence

from config_loader import AutoEyeConfig

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.models import OHLCBar, TrackedElement

FRACTAL_HIGH = "high"
FRACTAL_LOW = "low"


class FractalDetector(MarketElementDetector):
    element_type = "fractal"

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
        del config

        if len(bars) < 3:
            return []

        detected: list[TrackedElement] = []
        for index in range(2, len(bars)):
            c1 = bars[index - 2]
            c2 = bars[index - 1]
            c3 = bars[index]

            if c2.high > c1.high and c2.high > c3.high:
                detected.append(
                    self._build_element(
                        symbol=symbol,
                        timeframe=timeframe,
                        fractal_type=FRACTAL_HIGH,
                        c1=c1,
                        c2=c2,
                        c3=c3,
                        extreme_price=c2.high,
                    )
                )

            if c2.low < c1.low and c2.low < c3.low:
                detected.append(
                    self._build_element(
                        symbol=symbol,
                        timeframe=timeframe,
                        fractal_type=FRACTAL_LOW,
                        c1=c1,
                        c2=c2,
                        c3=c3,
                        extreme_price=c2.low,
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
        del bars
        del config
        return element

    def _build_element(
        self,
        *,
        symbol: str,
        timeframe: str,
        fractal_type: str,
        c1: OHLCBar,
        c2: OHLCBar,
        c3: OHLCBar,
        extreme_price: float,
    ) -> TrackedElement:
        l_price = float(c1.close)
        l_alt_price = float(c2.open)
        zone_low = min(l_price, extreme_price)
        zone_high = max(l_price, extreme_price)
        zone_size = max(0.0, zone_high - zone_low)
        pivot_time_iso = c2.time.isoformat()
        confirm_time_iso = c3.time.isoformat()

        return TrackedElement(
            id=self._build_id(
                symbol=symbol,
                timeframe=timeframe,
                fractal_type=fractal_type,
                pivot_time=pivot_time_iso,
                extreme_price=extreme_price,
                l_price=l_price,
            ),
            element_type=self.element_type,
            symbol=symbol,
            timeframe=timeframe,
            direction=fractal_type,
            formation_time=c3.time,
            zone_low=zone_low,
            zone_high=zone_high,
            zone_size=zone_size,
            c1_time=c1.time,
            c2_time=c2.time,
            c3_time=c3.time,
            metadata={
                "fractal_type": fractal_type,
                "pivot_time": pivot_time_iso,
                "confirm_time": confirm_time_iso,
                "extreme_price": float(extreme_price),
                "l_price": l_price,
                "l_alt_price": l_alt_price,
            },
        )

    @staticmethod
    def _build_id(
        *,
        symbol: str,
        timeframe: str,
        fractal_type: str,
        pivot_time: str,
        extreme_price: float,
        l_price: float,
    ) -> str:
        seed = (
            f"fractal|{symbol}|{timeframe}|{fractal_type}|{pivot_time}|"
            f"{extreme_price:.10f}|{l_price:.10f}"
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]
