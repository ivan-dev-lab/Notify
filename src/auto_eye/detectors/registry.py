from __future__ import annotations

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.detectors.fractal import FractalDetector
from auto_eye.detectors.fvg import FVGDetector
from auto_eye.detectors.rb import RBDetector
from auto_eye.detectors.snr import SNRDetector


def build_detectors(names: list[str]) -> dict[str, MarketElementDetector]:
    available: dict[str, MarketElementDetector] = {
        "fvg": FVGDetector(),
        "fractal": FractalDetector(),
        "snr": SNRDetector(),
        "rb": RBDetector(),
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
