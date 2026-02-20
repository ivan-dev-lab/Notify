from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

STATUS_ACTIVE = "active"
STATUS_TOUCHED = "touched"
STATUS_MITIGATED_FULL = "mitigated_full"


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def datetime_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return ensure_utc(value).isoformat()


def datetime_from_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return ensure_utc(parsed)


@dataclass(frozen=True)
class OHLCBar:
    time: datetime
    open: float
    high: float
    low: float
    close: float
    tick_volume: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "time", ensure_utc(self.time))


@dataclass
class TrackedElement:
    id: str
    element_type: str
    symbol: str
    timeframe: str
    direction: str
    formation_time: datetime
    zone_low: float
    zone_high: float
    zone_size: float
    c1_time: datetime
    c2_time: datetime
    c3_time: datetime
    status: str = STATUS_ACTIVE
    touched_time: datetime | None = None
    mitigated_time: datetime | None = None
    fill_price: float | None = None
    fill_percent: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.formation_time = ensure_utc(self.formation_time)
        self.c1_time = ensure_utc(self.c1_time)
        self.c2_time = ensure_utc(self.c2_time)
        self.c3_time = ensure_utc(self.c3_time)
        if self.touched_time is not None:
            self.touched_time = ensure_utc(self.touched_time)
        if self.mitigated_time is not None:
            self.mitigated_time = ensure_utc(self.mitigated_time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "element_type": self.element_type,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "direction": self.direction,
            "formation_time": datetime_to_iso(self.formation_time),
            "fvg_low": self.zone_low,
            "fvg_high": self.zone_high,
            "gap_size": self.zone_size,
            "c1_time": datetime_to_iso(self.c1_time),
            "c2_time": datetime_to_iso(self.c2_time),
            "c3_time": datetime_to_iso(self.c3_time),
            "status": self.status,
            "touched_time": datetime_to_iso(self.touched_time),
            "mitigated_time": datetime_to_iso(self.mitigated_time),
            "fill_price": self.fill_price,
            "fill_percent": self.fill_percent,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> TrackedElement | None:
        formation_time = datetime_from_iso(str(raw.get("formation_time") or ""))
        c1_time = datetime_from_iso(str(raw.get("c1_time") or ""))
        c2_time = datetime_from_iso(str(raw.get("c2_time") or ""))
        c3_time = datetime_from_iso(str(raw.get("c3_time") or ""))
        touched_time = datetime_from_iso(str(raw.get("touched_time") or ""))
        mitigated_time = datetime_from_iso(str(raw.get("mitigated_time") or ""))

        if formation_time is None or c1_time is None or c2_time is None or c3_time is None:
            return None

        try:
            zone_low = float(raw.get("fvg_low"))
            zone_high = float(raw.get("fvg_high"))
            zone_size = float(raw.get("gap_size"))
        except (TypeError, ValueError):
            return None

        metadata = raw.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}

        fill_price_raw = raw.get("fill_price")
        fill_percent_raw = raw.get("fill_percent")

        fill_price: float | None = None
        fill_percent: float | None = None

        if fill_price_raw is not None:
            try:
                fill_price = float(fill_price_raw)
            except (TypeError, ValueError):
                fill_price = None

        if fill_percent_raw is not None:
            try:
                fill_percent = float(fill_percent_raw)
            except (TypeError, ValueError):
                fill_percent = None

        return cls(
            id=str(raw.get("id", "")),
            element_type=str(raw.get("element_type", "")),
            symbol=str(raw.get("symbol", "")),
            timeframe=str(raw.get("timeframe", "")).upper(),
            direction=str(raw.get("direction", "")),
            formation_time=formation_time,
            zone_low=zone_low,
            zone_high=zone_high,
            zone_size=zone_size,
            c1_time=c1_time,
            c2_time=c2_time,
            c3_time=c3_time,
            status=str(raw.get("status", STATUS_ACTIVE)),
            touched_time=touched_time,
            mitigated_time=mitigated_time,
            fill_price=fill_price,
            fill_percent=fill_percent,
            metadata=metadata,
        )


@dataclass
class AutoEyeState:
    updated_at_utc: datetime
    last_bar_time_by_key: dict[str, datetime]
    elements: list[TrackedElement]

    def to_dict(self) -> dict[str, Any]:
        return {
            "updated_at_utc": datetime_to_iso(self.updated_at_utc),
            "last_bar_time": {
                key: datetime_to_iso(value)
                for key, value in self.last_bar_time_by_key.items()
            },
            "elements": [element.to_dict() for element in self.elements],
        }

    @classmethod
    def empty(cls) -> AutoEyeState:
        return cls(
            updated_at_utc=datetime.now(timezone.utc),
            last_bar_time_by_key={},
            elements=[],
        )

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> AutoEyeState:
        updated_at = datetime_from_iso(str(raw.get("updated_at_utc") or ""))
        if updated_at is None:
            updated_at = datetime.now(timezone.utc)

        parsed_last_bar: dict[str, datetime] = {}
        raw_last_bar = raw.get("last_bar_time")
        if isinstance(raw_last_bar, dict):
            for key, raw_value in raw_last_bar.items():
                parsed = datetime_from_iso(str(raw_value))
                if parsed is None:
                    continue
                parsed_last_bar[str(key)] = parsed

        parsed_elements: list[TrackedElement] = []
        raw_elements = raw.get("elements")
        if isinstance(raw_elements, list):
            for item in raw_elements:
                if not isinstance(item, dict):
                    continue
                parsed = TrackedElement.from_dict(item)
                if parsed is None:
                    continue
                parsed_elements.append(parsed)

        return cls(
            updated_at_utc=updated_at,
            last_bar_time_by_key=parsed_last_bar,
            elements=parsed_elements,
        )
