from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from config_loader import AutoEyeConfig

from auto_eye.models import OHLCBar, TrackedElement


class MarketElementDetector(Protocol):
    element_type: str

    def detect(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: Sequence[OHLCBar],
        point_size: float,
        config: AutoEyeConfig,
    ) -> list[TrackedElement]:
        ...

    def update_status(
        self,
        *,
        element: TrackedElement,
        bars: Sequence[OHLCBar],
        config: AutoEyeConfig,
    ) -> TrackedElement:
        ...
