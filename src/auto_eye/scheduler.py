from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def normalize_schedule_timeframe(timeframe: str) -> str:
    normalized = timeframe.strip().upper()
    if normalized == "M1":
        return "MN1"
    return normalized


@dataclass
class TimeframeScheduler:
    def is_due(
        self,
        *,
        timeframe: str,
        now_utc: datetime,
        last_check_utc: datetime | None,
    ) -> bool:
        normalized_tf = normalize_schedule_timeframe(timeframe)
        now_utc = ensure_utc(now_utc)
        last = ensure_utc(last_check_utc) if last_check_utc is not None else None
        if last is None:
            return True

        if normalized_tf in {"M15", "H1", "H4"}:
            period_seconds = {
                "M15": 15 * 60,
                "H1": 60 * 60,
                "H4": 4 * 60 * 60,
            }[normalized_tf]
            now_slot = int(now_utc.timestamp()) // period_seconds
            last_slot = int(last.timestamp()) // period_seconds
            return now_slot > last_slot

        if normalized_tf == "D1":
            return now_utc.date() > last.date()

        if normalized_tf == "W1":
            now_year, now_week, _ = now_utc.isocalendar()
            last_year, last_week, _ = last.isocalendar()
            return (now_year, now_week) > (last_year, last_week)

        if normalized_tf == "MN1":
            return (now_utc.year, now_utc.month) > (last.year, last.month)

        return False


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
