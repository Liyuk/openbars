"""US hourly selection and a side-effect-free daily consistency report."""
from collections import defaultdict
from dataclasses import dataclass, replace
from decimal import Decimal
from datetime import datetime
from typing import Iterable, Mapping

from openbars.core.models import Interval, Market, RawBar, Session
from openbars.core.series import (ALPACA_SIP_SPLIT, MASSIVE_AGGS_SPLIT,
                                  TWELVE_DATA_INTRADAY_SPLIT, BarSeriesCatalog)
from openbars.reconcile.overrides import SelectionOverride
from openbars.reconcile.policy import DEFAULT_POLICY
from openbars.reconcile.projection import SourceObservation, _difference_pct, project_canonical_bars
from openbars.storage.rows import CanonicalBarRow, hourly_adjustment, raw_ingested_at

# Reversible default: retain the daily policy version; interval is part of the storage key.
HOURLY_POLICY = replace(
    DEFAULT_POLICY, primary=MASSIVE_AGGS_SPLIT,
    fill_sources=(DEFAULT_POLICY.fill_sources[0],
                  replace(DEFAULT_POLICY.fill_sources[1], identity=TWELVE_DATA_INTRADAY_SPLIT)),
    volume_priority=(MASSIVE_AGGS_SPLIT, ALPACA_SIP_SPLIT),
)


def reconcile_hourly(
    bars: Iterable[RawBar], *, overrides: Mapping[tuple[str, datetime], SelectionOverride] | None = None
) -> tuple[CanonicalBarRow, ...]:
    groups = defaultdict(list)
    for bar in bars:
        if bar.interval != Interval.H1 or bar.volume is None or bar.session_date is None:
            continue
        feed = bar.series.feed if bar.series else (bar.feed or 'default').split(':')[-1]
        adjustment = hourly_adjustment(bar)
        from openbars.core.series import SourceIdentity
        series = bar.series or BarSeriesCatalog.resolve(SourceIdentity(bar.provider, feed, adjustment), interval=Interval.H1)
        obs = SourceObservation(bar.instrument_id or None, bar.symbol_as_returned, bar.session_date,
                                bar.open, bar.high, bar.low, bar.close, bar.volume,
                                bar.provider, feed, adjustment, revision=bar.revision, series=series,
                                available_at=raw_ingested_at(bar))
        groups[(obs.key, bar.ts_start)].append((obs, bar))
    result = []
    for (key, ts), group in sorted(groups.items()):
        override = (overrides or {}).get((key, ts))
        daily_override = {(key, group[0][0].session_date): override} if override else None
        selections = project_canonical_bars(
            (o for o, _ in group), policy=HOURLY_POLICY, overrides=daily_override
        )
        if not selections:
            continue
        sel = selections[0]
        raw = next(b for o, b in group if o is sel.selected)
        source = f'{raw.provider}:{sel.selected.feed}'
        result.append(CanonicalBarRow(
            instrument_id=sel.instrument_id, market=Market.US, interval=Interval.H1,
            ts_start=ts, session_date=sel.session_date,
            session=Session(raw.extra.get('session', 'regular')), provider=raw.provider,
            open=raw.open, high=raw.high, low=raw.low, close=raw.close, volume=raw.volume,
            price_source=source, volume_source=source, reconcile_rule=sel.selection_reason,
            policy_version=sel.policy_version, feed=sel.selected.feed, adjustment=sel.selected.adjustment,
            revision=raw.revision, selection_reason=sel.selection_reason, quality_status=sel.quality_status,
            capabilities=sel.capabilities, sources_seen=tuple(s.provider for s in sel.sources_seen),
            currency=sel.selected.resolved_series.currency,
            available_at=raw_ingested_at(raw),
            freshness=sel.selected.freshness,
        ))
    return tuple(result)


@dataclass(frozen=True)
class HourlyDailyReport:
    aggregate: CanonicalBarRow | None
    differences: tuple[str, ...]


def compare_hourly_to_daily(hourly: Iterable[CanonicalBarRow], daily: CanonicalBarRow,
                            *, include_extended: bool = False) -> HourlyDailyReport:
    """Compare one instrument/session; missing hourly data is reported explicitly."""
    if daily.interval != Interval.D1:
        raise ValueError('daily row must have daily interval')
    bars = list(hourly)
    if any(b.interval != Interval.H1 or b.instrument_id != daily.instrument_id or
           b.session_date != daily.session_date for b in bars):
        raise ValueError('hourly rows must belong to the daily instrument and session')
    bars = sorted((b for b in bars if include_extended or b.session == Session.REGULAR), key=lambda b: b.ts_start)
    if not bars:
        return HourlyDailyReport(None, ('missing_hourly',))
    if len({b.ts_start for b in bars}) != len(bars):
        raise ValueError('duplicate hourly timestamp')
    volume = None if any(b.volume is None for b in bars) else sum((b.volume for b in bars), Decimal(0))
    aggregate = replace(daily, open=bars[0].open, high=max(b.high for b in bars),
                        low=min(b.low for b in bars), close=bars[-1].close, volume=volume)
    differences = [name for name in ('open', 'high', 'low', 'close')
                   if _difference_pct(getattr(aggregate, name), getattr(daily, name)) > DEFAULT_POLICY.price_tolerance_pct]
    if volume is None or daily.volume is None:
        differences.append('volume_missing')
    elif _difference_pct(volume, daily.volume) > DEFAULT_POLICY.volume_tolerance_pct:
        differences.append('volume')
    return HourlyDailyReport(aggregate, tuple(differences))
