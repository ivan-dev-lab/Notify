from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

STATUS_ACTIVE = "active"
STATUS_TOUCHED = "touched"
STATUS_MITIGATED_PARTIAL = "mitigated_partial"
STATUS_MITIGATED_FULL = "mitigated_full"
STATUS_RETESTED = "retested"
STATUS_INVALIDATED = "invalidated"
STATUS_BROKEN = "broken"
STATUS_EXPIRED = "expired"


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
        normalized_type = self.element_type.strip().lower()
        if normalized_type == "fractal":
            return self._to_fractal_dict()
        if normalized_type == "snr":
            return self._to_snr_dict()
        if normalized_type == "rb":
            return self._to_rb_dict()
        return self._to_fvg_dict()

    def _to_fvg_dict(self) -> dict[str, Any]:
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

    def _to_fractal_dict(self) -> dict[str, Any]:
        fractal_type = str(self.metadata.get("fractal_type") or self.direction or "")
        pivot_time = datetime_from_iso(str(self.metadata.get("pivot_time") or ""))
        if pivot_time is None:
            pivot_time = self.c2_time
        confirm_time = datetime_from_iso(str(self.metadata.get("confirm_time") or ""))
        if confirm_time is None:
            confirm_time = self.formation_time
        extreme_price = self._safe_float(
            self.metadata.get("extreme_price"),
            fallback=self.zone_high,
        )
        l_price = self._safe_float(
            self.metadata.get("l_price"),
            fallback=self.zone_low,
        )
        l_alt_price = self._safe_float(
            self.metadata.get("l_alt_price"),
            fallback=l_price,
        )
        l_price_bearish = self._safe_float(
            self.metadata.get("l_price_bearish"),
            fallback=l_price,
        )
        l_alt_bearish = self._safe_float(
            self.metadata.get("l_alt_bearish"),
            fallback=l_alt_price,
        )
        l_price_bullish = self._safe_float(
            self.metadata.get("l_price_bullish"),
            fallback=l_price_bearish,
        )
        l_alt_bullish = self._safe_float(
            self.metadata.get("l_alt_bullish"),
            fallback=l_price_bullish,
        )
        return {
            "id": self.id,
            "element_type": "fractal",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "fractal_type": fractal_type,
            "pivot_time": datetime_to_iso(pivot_time),
            "confirm_time": datetime_to_iso(confirm_time),
            "c1_time": datetime_to_iso(self.c1_time),
            "c2_time": datetime_to_iso(self.c2_time),
            "c3_time": datetime_to_iso(self.c3_time),
            "extreme_price": extreme_price,
            "l_price": l_price,
            "l_alt_price": l_alt_price,
            "l_price_bearish": l_price_bearish,
            "l_alt_bearish": l_alt_bearish,
            "l_price_bullish": l_price_bullish,
            "l_alt_bullish": l_alt_bullish,
            "metadata": self.metadata,
        }

    def _to_snr_dict(self) -> dict[str, Any]:
        role = str(self.metadata.get("role") or self.direction or "")
        break_type = str(self.metadata.get("break_type") or "")
        break_time = datetime_from_iso(str(self.metadata.get("break_time") or ""))
        if break_time is None:
            break_time = self.formation_time
        break_close = self._safe_optional_float(self.metadata.get("break_close"))
        if break_close is None:
            break_close = self._safe_optional_float(self.fill_price)
        l_price = self._safe_float(
            self.metadata.get("l_price"),
            fallback=self.zone_low,
        )
        l_alt_price = self._safe_float(
            self.metadata.get("l_alt_price"),
            fallback=l_price,
        )
        l_price_bearish = self._safe_float(
            self.metadata.get("l_price_bearish"),
            fallback=l_price,
        )
        l_alt_bearish = self._safe_float(
            self.metadata.get("l_alt_bearish"),
            fallback=l_alt_price,
        )
        l_price_bullish = self._safe_float(
            self.metadata.get("l_price_bullish"),
            fallback=l_price_bearish,
        )
        l_alt_bullish = self._safe_float(
            self.metadata.get("l_alt_bullish"),
            fallback=l_price_bullish,
        )
        l_price_used = self._safe_float(
            self.metadata.get("l_price_used"),
            fallback=l_price,
        )
        l_rule_used = str(
            self.metadata.get("l_rule_used")
            or ("bullish_C2close" if role == "support" else "bearish_C1close")
        )
        extreme_price = self._safe_float(
            self.metadata.get("extreme_price"),
            fallback=self.zone_high,
        )
        snr_low = self._safe_float(
            self.metadata.get("snr_low"),
            fallback=self.zone_low,
        )
        snr_high = self._safe_float(
            self.metadata.get("snr_high"),
            fallback=self.zone_high,
        )
        departure_extreme_price = self._safe_optional_float(
            self.metadata.get("departure_extreme_price")
        )
        if departure_extreme_price is None:
            if role == "support":
                departure_extreme_price = snr_low
            else:
                departure_extreme_price = snr_high
        departure_extreme_time = datetime_from_iso(
            str(self.metadata.get("departure_extreme_time") or "")
        )
        if departure_extreme_time is None:
            departure_extreme_time = break_time
        departure_range_start_time = datetime_from_iso(
            str(self.metadata.get("departure_range_start_time") or "")
        )
        if departure_range_start_time is None:
            departure_range_start_time = break_time
        departure_range_end_time = datetime_from_iso(
            str(self.metadata.get("departure_range_end_time") or "")
        )
        if departure_range_end_time is None:
            departure_range_end_time = break_time
        return {
            "id": self.id,
            "element_type": "snr",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "origin_fractal_id": str(self.metadata.get("origin_fractal_id") or ""),
            "role": role,
            "break_type": break_type,
            "break_time": datetime_to_iso(break_time),
            "break_close": break_close,
            "l_price": l_price,
            "l_alt_price": l_alt_price,
            "l_price_bearish": l_price_bearish,
            "l_alt_bearish": l_alt_bearish,
            "l_price_bullish": l_price_bullish,
            "l_alt_bullish": l_alt_bullish,
            "l_price_used": l_price_used,
            "l_rule_used": l_rule_used,
            "extreme_price": extreme_price,
            "snr_low": snr_low,
            "snr_high": snr_high,
            "departure_extreme_price": departure_extreme_price,
            "departure_extreme_time": datetime_to_iso(departure_extreme_time),
            "departure_range_start_time": datetime_to_iso(departure_range_start_time),
            "departure_range_end_time": datetime_to_iso(departure_range_end_time),
            "status": self.status,
            "retest_time": datetime_to_iso(self.touched_time),
            "invalidated_time": datetime_to_iso(self.mitigated_time),
            "metadata": self.metadata,
        }

    def _to_rb_dict(self) -> dict[str, Any]:
        rb_type = str(self.metadata.get("rb_type") or self.direction or "")
        pivot_time = datetime_from_iso(str(self.metadata.get("pivot_time") or ""))
        if pivot_time is None:
            pivot_time = self.c2_time
        confirm_time = datetime_from_iso(str(self.metadata.get("confirm_time") or ""))
        if confirm_time is None:
            confirm_time = self.formation_time
        if rb_type == "low":
            l_fallback = self.zone_high
            extreme_fallback = self.zone_low
        else:
            l_fallback = self.zone_low
            extreme_fallback = self.zone_high

        l_price = self._safe_float(
            self.metadata.get("l_price"),
            fallback=l_fallback,
        )
        l_alt_price = self._safe_float(
            self.metadata.get("l_alt_price"),
            fallback=l_price,
        )
        l_price_bearish = self._safe_float(
            self.metadata.get("l_price_bearish"),
            fallback=l_price,
        )
        l_alt_bearish = self._safe_float(
            self.metadata.get("l_alt_bearish"),
            fallback=l_alt_price,
        )
        l_price_bullish = self._safe_float(
            self.metadata.get("l_price_bullish"),
            fallback=l_price_bearish,
        )
        l_alt_bullish = self._safe_float(
            self.metadata.get("l_alt_bullish"),
            fallback=l_price_bullish,
        )
        l_price_used = self._safe_float(
            self.metadata.get("l_price_used"),
            fallback=l_price,
        )
        l_rule_used = str(
            self.metadata.get("l_rule_used")
            or ("bullish_C2close" if rb_type == "low" else "bearish_C1close")
        )
        extreme_price = self._safe_float(
            self.metadata.get("extreme_price"),
            fallback=extreme_fallback,
        )
        rb_low = self._safe_float(
            self.metadata.get("rb_low"),
            fallback=min(l_price, extreme_price),
        )
        rb_high = self._safe_float(
            self.metadata.get("rb_high"),
            fallback=max(l_price, extreme_price),
        )

        broken_time = datetime_from_iso(str(self.metadata.get("broken_time") or ""))
        if broken_time is None:
            broken_time = self.mitigated_time

        broken_side = self.metadata.get("broken_side")
        if broken_side is None:
            broken_side = None
        else:
            broken_side = str(broken_side)

        return {
            "id": self.id,
            "element_type": "rb",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "rb_type": rb_type,
            "origin_fractal_id": str(self.metadata.get("origin_fractal_id") or ""),
            "pivot_time": datetime_to_iso(pivot_time),
            "confirm_time": datetime_to_iso(confirm_time),
            "c1_time": datetime_to_iso(self.c1_time),
            "c2_time": datetime_to_iso(self.c2_time),
            "c3_time": datetime_to_iso(self.c3_time),
            "l_price": l_price,
            "l_alt_price": l_alt_price,
            "l_price_bearish": l_price_bearish,
            "l_alt_bearish": l_alt_bearish,
            "l_price_bullish": l_price_bullish,
            "l_alt_bullish": l_alt_bullish,
            "l_price_used": l_price_used,
            "l_rule_used": l_rule_used,
            "line_used": l_price_used,
            "line_rule_used": l_rule_used,
            "extreme_price": extreme_price,
            "rb_low": rb_low,
            "rb_high": rb_high,
            "status": self.status,
            "broken_time": datetime_to_iso(broken_time),
            "broken_side": broken_side,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> TrackedElement | None:
        normalized_type = str(raw.get("element_type", "")).strip().lower()
        if normalized_type == "fractal":
            return cls._from_fractal_dict(raw)
        if normalized_type == "snr":
            return cls._from_snr_dict(raw)
        if normalized_type == "rb":
            return cls._from_rb_dict(raw)
        return cls._from_fvg_dict(raw)

    @classmethod
    def _from_fvg_dict(cls, raw: dict[str, Any]) -> TrackedElement | None:
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

    @classmethod
    def _from_fractal_dict(cls, raw: dict[str, Any]) -> TrackedElement | None:
        c1_time = datetime_from_iso(str(raw.get("c1_time") or ""))
        c2_time = datetime_from_iso(str(raw.get("c2_time") or ""))
        c3_time = datetime_from_iso(str(raw.get("c3_time") or ""))
        pivot_time = datetime_from_iso(str(raw.get("pivot_time") or ""))
        confirm_time = datetime_from_iso(str(raw.get("confirm_time") or ""))
        if c1_time is None or c2_time is None or c3_time is None:
            return None
        if pivot_time is None:
            pivot_time = c2_time
        if confirm_time is None:
            confirm_time = c3_time

        extreme_price = cls._safe_optional_float(raw.get("extreme_price"))
        l_price = cls._safe_optional_float(raw.get("l_price"))
        l_alt_price = cls._safe_optional_float(raw.get("l_alt_price"))
        l_price_bearish = cls._safe_optional_float(raw.get("l_price_bearish"))
        l_alt_bearish = cls._safe_optional_float(raw.get("l_alt_bearish"))
        l_price_bullish = cls._safe_optional_float(raw.get("l_price_bullish"))
        l_alt_bullish = cls._safe_optional_float(raw.get("l_alt_bullish"))
        if extreme_price is None or l_price is None:
            return None
        if l_alt_price is None:
            l_alt_price = l_price
        if l_price_bearish is None:
            l_price_bearish = l_price
        if l_alt_bearish is None:
            l_alt_bearish = l_alt_price
        if l_price_bullish is None:
            l_price_bullish = l_price_bearish
        if l_alt_bullish is None:
            l_alt_bullish = l_price_bullish

        zone_low = min(l_price, extreme_price)
        zone_high = max(l_price, extreme_price)
        zone_size = max(0.0, zone_high - zone_low)

        metadata = raw.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        fractal_type = str(raw.get("fractal_type") or metadata.get("fractal_type") or "")
        metadata.update(
            {
                "fractal_type": fractal_type,
                "pivot_time": datetime_to_iso(pivot_time),
                "confirm_time": datetime_to_iso(confirm_time),
                "extreme_price": extreme_price,
                "l_price": l_price,
                "l_alt_price": l_alt_price,
                "l_price_bearish": l_price_bearish,
                "l_alt_bearish": l_alt_bearish,
                "l_price_bullish": l_price_bullish,
                "l_alt_bullish": l_alt_bullish,
            }
        )

        return cls(
            id=str(raw.get("id", "")),
            element_type="fractal",
            symbol=str(raw.get("symbol", "")),
            timeframe=str(raw.get("timeframe", "")).upper(),
            direction=fractal_type,
            formation_time=confirm_time,
            zone_low=zone_low,
            zone_high=zone_high,
            zone_size=zone_size,
            c1_time=c1_time,
            c2_time=c2_time,
            c3_time=c3_time,
            status=str(raw.get("status", STATUS_ACTIVE)),
            touched_time=None,
            mitigated_time=None,
            fill_price=None,
            fill_percent=None,
            metadata=metadata,
        )

    @classmethod
    def _from_snr_dict(cls, raw: dict[str, Any]) -> TrackedElement | None:
        break_time = datetime_from_iso(str(raw.get("break_time") or ""))
        if break_time is None:
            return None

        role = str(raw.get("role") or "")
        snr_low = cls._safe_optional_float(raw.get("snr_low"))
        snr_high = cls._safe_optional_float(raw.get("snr_high"))
        l_price = cls._safe_optional_float(raw.get("l_price"))
        l_alt_price = cls._safe_optional_float(raw.get("l_alt_price"))
        l_price_bearish = cls._safe_optional_float(raw.get("l_price_bearish"))
        l_alt_bearish = cls._safe_optional_float(raw.get("l_alt_bearish"))
        l_price_bullish = cls._safe_optional_float(raw.get("l_price_bullish"))
        l_alt_bullish = cls._safe_optional_float(raw.get("l_alt_bullish"))
        l_price_used = cls._safe_optional_float(raw.get("l_price_used"))
        l_rule_used = str(raw.get("l_rule_used") or "")
        extreme_price = cls._safe_optional_float(raw.get("extreme_price"))
        if l_price is None:
            l_price = l_price_used
        if l_price is None and role == "support":
            l_price = snr_high
        if l_price is None and role == "resistance":
            l_price = snr_low
        if (
            snr_low is None
            or snr_high is None
            or l_price is None
            or extreme_price is None
        ):
            return None
        if l_alt_price is None:
            l_alt_price = l_price
        if l_price_bearish is None:
            l_price_bearish = l_price
        if l_alt_bearish is None:
            l_alt_bearish = l_alt_price
        if l_price_bullish is None:
            l_price_bullish = l_price
        if l_alt_bullish is None:
            l_alt_bullish = l_alt_price
        if l_price_used is None:
            l_price_used = l_price
        if not l_rule_used:
            l_rule_used = "bullish_C2close" if role == "support" else "bearish_C1close"

        departure_extreme_price = cls._safe_optional_float(raw.get("departure_extreme_price"))
        if departure_extreme_price is None:
            if role == "support":
                departure_extreme_price = snr_low
            else:
                departure_extreme_price = snr_high
        departure_extreme_time = datetime_from_iso(
            str(raw.get("departure_extreme_time") or "")
        )
        if departure_extreme_time is None:
            departure_extreme_time = break_time
        departure_range_start_time = datetime_from_iso(
            str(raw.get("departure_range_start_time") or "")
        )
        if departure_range_start_time is None:
            departure_range_start_time = break_time
        departure_range_end_time = datetime_from_iso(
            str(raw.get("departure_range_end_time") or "")
        )
        if departure_range_end_time is None:
            departure_range_end_time = break_time

        retest_time = datetime_from_iso(str(raw.get("retest_time") or ""))
        invalidated_time = datetime_from_iso(str(raw.get("invalidated_time") or ""))
        break_close = cls._safe_optional_float(raw.get("break_close"))

        metadata = raw.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(
            {
                "origin_fractal_id": str(raw.get("origin_fractal_id") or ""),
                "role": str(raw.get("role") or ""),
                "break_type": str(raw.get("break_type") or ""),
                "break_time": datetime_to_iso(break_time),
                "break_close": break_close,
                "l_price": l_price,
                "l_alt_price": l_alt_price,
                "l_price_bearish": l_price_bearish,
                "l_alt_bearish": l_alt_bearish,
                "l_price_bullish": l_price_bullish,
                "l_alt_bullish": l_alt_bullish,
                "l_price_used": l_price_used,
                "l_rule_used": l_rule_used,
                "extreme_price": extreme_price,
                "snr_low": snr_low,
                "snr_high": snr_high,
                "departure_extreme_price": departure_extreme_price,
                "departure_extreme_time": datetime_to_iso(departure_extreme_time),
                "departure_range_start_time": datetime_to_iso(departure_range_start_time),
                "departure_range_end_time": datetime_to_iso(departure_range_end_time),
                "retest_time": datetime_to_iso(retest_time),
                "invalidated_time": datetime_to_iso(invalidated_time),
            }
        )

        return cls(
            id=str(raw.get("id", "")),
            element_type="snr",
            symbol=str(raw.get("symbol", "")),
            timeframe=str(raw.get("timeframe", "")).upper(),
            direction=role,
            formation_time=break_time,
            zone_low=snr_low,
            zone_high=snr_high,
            zone_size=max(0.0, snr_high - snr_low),
            c1_time=break_time,
            c2_time=break_time,
            c3_time=break_time,
            status=str(raw.get("status", STATUS_ACTIVE)),
            touched_time=retest_time,
            mitigated_time=invalidated_time,
            fill_price=break_close,
            fill_percent=None,
            metadata=metadata,
        )

    @classmethod
    def _from_rb_dict(cls, raw: dict[str, Any]) -> TrackedElement | None:
        c1_time = datetime_from_iso(
            str(raw.get("c1_time") or raw.get("c1_time_utc") or "")
        )
        c2_time = datetime_from_iso(
            str(raw.get("c2_time") or raw.get("c2_time_utc") or "")
        )
        c3_time = datetime_from_iso(
            str(raw.get("c3_time") or raw.get("c3_time_utc") or "")
        )
        if c1_time is None or c2_time is None or c3_time is None:
            return None

        pivot_time = datetime_from_iso(
            str(raw.get("pivot_time") or raw.get("pivot_time_utc") or "")
        )
        if pivot_time is None:
            pivot_time = c2_time
        confirm_time = datetime_from_iso(
            str(raw.get("confirm_time") or raw.get("confirm_time_utc") or "")
        )
        if confirm_time is None:
            confirm_time = c3_time

        rb_type = str(raw.get("rb_type") or "")
        if not rb_type:
            rb_type = str(raw.get("direction") or "")
        rb_type = rb_type.strip().lower()

        l_price = cls._safe_optional_float(raw.get("l_price"))
        l_alt_price = cls._safe_optional_float(raw.get("l_alt_price"))
        l_price_bearish = cls._safe_optional_float(raw.get("l_price_bearish"))
        l_alt_bearish = cls._safe_optional_float(raw.get("l_alt_bearish"))
        l_price_bullish = cls._safe_optional_float(raw.get("l_price_bullish"))
        l_alt_bullish = cls._safe_optional_float(raw.get("l_alt_bullish"))
        l_price_used = cls._safe_optional_float(
            raw.get("l_price_used") or raw.get("line_used")
        )
        l_rule_used = str(raw.get("l_rule_used") or raw.get("line_rule_used") or "")
        extreme_price = cls._safe_optional_float(raw.get("extreme_price"))
        rb_low = cls._safe_optional_float(raw.get("rb_low"))
        rb_high = cls._safe_optional_float(raw.get("rb_high"))

        if l_price is None:
            l_price = l_price_used
        if l_price is None and rb_type == "low" and rb_high is not None:
            l_price = rb_high
        if l_price is None and rb_low is not None:
            l_price = rb_low
        if extreme_price is None and rb_type == "low" and rb_low is not None:
            extreme_price = rb_low
        if extreme_price is None and rb_high is not None:
            extreme_price = rb_high
        if l_price is None or extreme_price is None:
            return None
        if l_alt_price is None:
            l_alt_price = l_price
        if l_price_bearish is None:
            l_price_bearish = l_price
        if l_alt_bearish is None:
            l_alt_bearish = l_alt_price
        if l_price_bullish is None:
            l_price_bullish = l_price
        if l_alt_bullish is None:
            l_alt_bullish = l_alt_price
        if l_price_used is None:
            l_price_used = l_price
        if not l_rule_used:
            l_rule_used = "bullish_C2close" if rb_type == "low" else "bearish_C1close"

        if rb_low is None:
            rb_low = min(l_price, extreme_price)
        if rb_high is None:
            rb_high = max(l_price, extreme_price)

        broken_time = datetime_from_iso(
            str(raw.get("broken_time") or raw.get("broken_time_utc") or "")
        )
        broken_side_raw = raw.get("broken_side")
        broken_side = None if broken_side_raw is None else str(broken_side_raw)

        metadata = raw.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(
            {
                "rb_type": rb_type,
                "origin_fractal_id": str(raw.get("origin_fractal_id") or ""),
                "pivot_time": datetime_to_iso(pivot_time),
                "confirm_time": datetime_to_iso(confirm_time),
                "c1_time": datetime_to_iso(c1_time),
                "c2_time": datetime_to_iso(c2_time),
                "c3_time": datetime_to_iso(c3_time),
                "l_price": float(l_price),
                "l_alt_price": float(l_alt_price),
                "l_price_bearish": float(l_price_bearish),
                "l_alt_bearish": float(l_alt_bearish),
                "l_price_bullish": float(l_price_bullish),
                "l_alt_bullish": float(l_alt_bullish),
                "l_price_used": float(l_price_used),
                "l_rule_used": l_rule_used,
                "line_used": float(l_price_used),
                "line_rule_used": l_rule_used,
                "extreme_price": float(extreme_price),
                "rb_low": float(rb_low),
                "rb_high": float(rb_high),
                "broken_time": datetime_to_iso(broken_time),
                "broken_side": broken_side,
            }
        )

        element_id = str(raw.get("id", "")).strip()
        if not element_id:
            seed = (
                f"rb|{str(raw.get('symbol', ''))}|{str(raw.get('timeframe', '')).upper()}|"
                f"{rb_type}|{datetime_to_iso(pivot_time) or ''}|{float(l_price):.10f}|"
                f"{float(extreme_price):.10f}"
            )
            element_id = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:20]

        return cls(
            id=element_id,
            element_type="rb",
            symbol=str(raw.get("symbol", "")),
            timeframe=str(raw.get("timeframe", "")).upper(),
            direction=rb_type,
            formation_time=confirm_time,
            zone_low=float(rb_low),
            zone_high=float(rb_high),
            zone_size=max(0.0, float(rb_high) - float(rb_low)),
            c1_time=c1_time,
            c2_time=c2_time,
            c3_time=c3_time,
            status=str(raw.get("status", STATUS_ACTIVE)),
            touched_time=None,
            mitigated_time=broken_time,
            fill_price=None,
            fill_percent=None,
            metadata=metadata,
        )

    @staticmethod
    def _safe_float(value: object, *, fallback: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(fallback)

    @staticmethod
    def _safe_optional_float(value: object) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


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
