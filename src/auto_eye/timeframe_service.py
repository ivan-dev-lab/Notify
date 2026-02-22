from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from config_loader import AppConfig

from auto_eye.detectors.base import MarketElementDetector
from auto_eye.exporters import resolve_output_path
from auto_eye.models import STATUS_ACTIVE, TrackedElement
from auto_eye.mt5_source import MT5BarsSource
from auto_eye.scheduler import TimeframeScheduler
from auto_eye.timeframe_files import TimeframeFileStore, TimeframeSnapshot
from auto_eye.timeframes import normalize_timeframes

logger = logging.getLogger(__name__)


@dataclass
class TimeframeUpdateReport:
    timeframe: str
    file_updated: bool
    skipped_no_data: bool
    primary_load: bool
    new_count: int
    status_updated_count: int
    total_active: int
    total_elements: int
    message: str


class TimeframeUpdateService:
    def __init__(
        self,
        *,
        config: AppConfig,
        detectors: dict[str, MarketElementDetector],
        source: MT5BarsSource | None = None,
        scheduler: TimeframeScheduler | None = None,
        file_store: TimeframeFileStore | None = None,
    ) -> None:
        self.config = config
        self.detectors = detectors
        self.source = source or MT5BarsSource(config)
        self.scheduler = scheduler or TimeframeScheduler()
        self.file_store = file_store or TimeframeFileStore(
            resolve_output_path(config.auto_eye.output_json),
        )
        self.last_check_by_timeframe: dict[str, datetime] = {}
        self.last_bar_by_key: dict[str, datetime] = {}

    def run_all(self, *, force: bool = False) -> list[TimeframeUpdateReport]:
        return self._run(force=force, due_only=False)

    def run_due(self) -> list[TimeframeUpdateReport]:
        return self._run(force=False, due_only=True)

    def _run(self, *, force: bool, due_only: bool) -> list[TimeframeUpdateReport]:
        if not self.detectors:
            raise RuntimeError("No detectors configured in auto_eye.elements")

        reports: list[TimeframeUpdateReport] = []
        now_utc = datetime.now(timezone.utc)
        timeframes = normalize_timeframes(self.config.auto_eye.timeframes)
        symbols = self._resolve_symbols()

        self.source.connect()
        try:
            for timeframe in timeframes:
                snapshot = self.file_store.load(timeframe, symbols)
                last_check = self.last_check_by_timeframe.get(timeframe)
                if last_check is None:
                    last_check = snapshot.updated_at_utc

                if due_only and not force:
                    if not self.scheduler.is_due(
                        timeframe=timeframe,
                        now_utc=now_utc,
                        last_check_utc=last_check,
                    ):
                        continue

                report = self._refresh_timeframe(
                    timeframe=timeframe,
                    symbols=symbols,
                    now_utc=now_utc,
                    previous=snapshot,
                )
                reports.append(report)

                if report.skipped_no_data:
                    continue
                self.last_check_by_timeframe[timeframe] = now_utc
        finally:
            self.source.close()

        return reports

    def _refresh_timeframe(
        self,
        *,
        timeframe: str,
        symbols: list[str],
        now_utc: datetime,
        previous: TimeframeSnapshot,
    ) -> TimeframeUpdateReport:
        history_cutoff = now_utc - timedelta(
            days=self.config.auto_eye.history_days + self.config.auto_eye.history_buffer_days
        )
        existing_elements = [
            element
            for element in previous.elements
            if element.timeframe.upper() == timeframe.upper()
        ]
        old_by_id = {element.id: element for element in existing_elements}

        next_last_bar_by_symbol = dict(previous.last_bar_time_by_symbol)
        next_elements: list[TrackedElement] = []
        skipped_no_data = False

        for symbol in symbols:
            symbol_existing = [
                element for element in existing_elements if element.symbol == symbol
            ]
            last_bar = self._resolve_last_bar(timeframe, symbol, previous)

            if last_bar is None or not previous.initialized:
                bars = self.source.fetch_history(
                    symbol=symbol,
                    timeframe_code=timeframe,
                    history_days=self.config.auto_eye.history_days,
                    history_buffer_days=self.config.auto_eye.history_buffer_days,
                )
            else:
                bars = self.source.fetch_incremental(
                    symbol=symbol,
                    timeframe_code=timeframe,
                    last_bar_time=last_bar,
                    incremental_bars=self.config.auto_eye.incremental_bars,
                    history_days=self.config.auto_eye.history_days,
                    history_buffer_days=self.config.auto_eye.history_buffer_days,
                )

            if bars is None:
                skipped_no_data = True
                logger.warning(
                    "Timeframe %s skipped: MT5 returned no data for symbol=%s",
                    timeframe,
                    symbol,
                )
                break

            if bars:
                next_last_bar_by_symbol[symbol] = bars[-1].time
                self.last_bar_by_key[self._build_symbol_key(timeframe, symbol)] = bars[-1].time

            if len(bars) < 3:
                next_elements.extend(symbol_existing)
                continue

            point_size = self.source.get_point_size(symbol)
            next_elements.extend(
                self._process_symbol(
                    symbol=symbol,
                    timeframe=timeframe,
                    bars=bars,
                    point_size=point_size,
                    existing=symbol_existing,
                )
            )

        if skipped_no_data:
            return TimeframeUpdateReport(
                timeframe=timeframe,
                file_updated=False,
                skipped_no_data=True,
                primary_load=not previous.initialized,
                new_count=0,
                status_updated_count=0,
                total_active=0,
                total_elements=len(existing_elements),
                message="MT5 returned None, timeframe file left untouched",
            )

        filtered_elements = [
            element for element in next_elements if element.formation_time >= history_cutoff
        ]
        deduped_elements = self._deduplicate_elements(filtered_elements)

        new_count = 0
        status_updates = 0
        for element in deduped_elements:
            old = old_by_id.get(element.id)
            if old is None:
                new_count += 1
                continue
            if self._element_state_changed(old, element):
                status_updates += 1

        total_active = sum(
            1 for element in deduped_elements if element.status == STATUS_ACTIVE
        )
        primary_load = not previous.initialized
        should_write = primary_load or new_count > 0 or status_updates > 0

        if should_write:
            snapshot = TimeframeSnapshot(
                timeframe=timeframe,
                initialized=True,
                updated_at_utc=now_utc,
                last_bar_time_by_symbol=next_last_bar_by_symbol,
                elements=deduped_elements,
            )
            saved_paths = self.file_store.save(snapshot)
            logger.info(
                "FVG %s updated: new=%s status_updated=%s active=%s total=%s asset_files=%s",
                timeframe,
                new_count,
                status_updates,
                total_active,
                len(deduped_elements),
                len(saved_paths),
            )
            return TimeframeUpdateReport(
                timeframe=timeframe,
                file_updated=True,
                skipped_no_data=False,
                primary_load=primary_load,
                new_count=new_count,
                status_updated_count=status_updates,
                total_active=total_active,
                total_elements=len(deduped_elements),
                message="updated",
            )

        logger.info(
            "FVG %s no changes: new=%s status_updated=%s active=%s total=%s",
            timeframe,
            new_count,
            status_updates,
            total_active,
            len(deduped_elements),
        )
        return TimeframeUpdateReport(
            timeframe=timeframe,
            file_updated=False,
            skipped_no_data=False,
            primary_load=primary_load,
            new_count=new_count,
            status_updated_count=status_updates,
            total_active=total_active,
            total_elements=len(deduped_elements),
            message="no changes",
        )

    def _resolve_symbols(self) -> list[str]:
        symbols: list[str] = []
        for raw in self.config.auto_eye.symbols:
            symbol = self.source.resolve_symbol(raw)
            if symbol and symbol not in symbols:
                symbols.append(symbol)
        return symbols

    def _resolve_last_bar(
        self,
        timeframe: str,
        symbol: str,
        snapshot: TimeframeSnapshot,
    ) -> datetime | None:
        cache_key = self._build_symbol_key(timeframe, symbol)
        cached = self.last_bar_by_key.get(cache_key)
        if cached is not None:
            return cached
        return snapshot.last_bar_time_by_symbol.get(symbol)

    def _process_symbol(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: list,
        point_size: float,
        existing: list[TrackedElement],
    ) -> list[TrackedElement]:
        result: list[TrackedElement] = []
        enabled_names = set(self.detectors.keys())

        for detector_name, detector in self.detectors.items():
            detector_existing = {
                element.id: element
                for element in existing
                if element.element_type == detector_name
            }
            detected = detector.detect(
                symbol=symbol,
                timeframe=timeframe,
                bars=bars,
                point_size=point_size,
                config=self.config.auto_eye,
            )
            for item in detected:
                detector_existing.setdefault(item.id, item)

            for item in detector_existing.values():
                detector.update_status(
                    element=item,
                    bars=bars,
                    config=self.config.auto_eye,
                )
            result.extend(detector_existing.values())

        for old in existing:
            if old.element_type not in enabled_names:
                result.append(old)

        return result

    @staticmethod
    def _deduplicate_elements(elements: list[TrackedElement]) -> list[TrackedElement]:
        deduped: dict[str, TrackedElement] = {}
        for element in elements:
            deduped[element.id] = element
        values = list(deduped.values())
        values.sort(key=lambda item: (item.symbol, item.c3_time, item.id))
        return values

    @staticmethod
    def _element_state_changed(old: TrackedElement, new: TrackedElement) -> bool:
        return (
            old.status != new.status
            or old.touched_time != new.touched_time
            or old.mitigated_time != new.mitigated_time
            or old.fill_price != new.fill_price
            or old.fill_percent != new.fill_percent
        )

    @staticmethod
    def _build_symbol_key(timeframe: str, symbol: str) -> str:
        return f"{timeframe.upper()}|{symbol}"
