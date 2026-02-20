from __future__ import annotations

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.detectors.fvg import FVGDetector


def build_detectors(names: list[str]) -> dict[str, MarketElementDetector]:
    available: dict[str, MarketElementDetector] = {
        "fvg": FVGDetector(),
    }

    selected: dict[str, MarketElementDetector] = {}
    for raw_name in names:
        normalized = raw_name.strip().lower()
        if not normalized:
            continue
        detector = available.get(normalized)
        if detector is None:
            continue
        selected[normalized] = detector

    return selected
