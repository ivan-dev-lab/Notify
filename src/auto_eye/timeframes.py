from __future__ import annotations

from collections.abc import Iterable

TIMEFRAME_SECONDS: dict[str, int] = {
    "M1": 60,
    "M2": 120,
    "M3": 180,
    "M4": 240,
    "M5": 300,
    "M6": 360,
    "M10": 600,
    "M12": 720,
    "M15": 900,
    "M20": 1200,
    "M30": 1800,
    "H1": 3600,
    "H2": 7200,
    "H3": 10800,
    "H4": 14400,
    "H6": 21600,
    "H8": 28800,
    "H12": 43200,
    "D1": 86400,
    "W1": 604800,
    "MN1": 2629800,
}


def normalize_timeframe_code(value: str) -> str:
    return value.strip().upper()


def list_supported_timeframes(mt5_module: object) -> list[str]:
    supported: list[str] = []
    for code in TIMEFRAME_SECONDS:
        attr_name = f"TIMEFRAME_{code}"
        if hasattr(mt5_module, attr_name):
            supported.append(code)
    return supported


def resolve_mt5_timeframe(mt5_module: object, timeframe_code: str) -> int:
    normalized = normalize_timeframe_code(timeframe_code)
    attr_name = f"TIMEFRAME_{normalized}"
    if not hasattr(mt5_module, attr_name):
        available = ", ".join(list_supported_timeframes(mt5_module))
        raise ValueError(
            f"Unsupported timeframe: {normalized}. Supported: {available}"
        )
    return int(getattr(mt5_module, attr_name))


def timeframe_to_seconds(timeframe_code: str) -> int:
    normalized = normalize_timeframe_code(timeframe_code)
    return TIMEFRAME_SECONDS.get(normalized, 300)


def normalize_timeframes(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        code = normalize_timeframe_code(str(value))
        if code and code not in normalized:
            normalized.append(code)
    return normalized
