"""Corporate-action vote/merge and stale-history detection (pure functions, no I/O).

Merge (``merge_corporate_actions``)
-----------------------------------
Observations are append-only: every source reports its own ``CorporateAction`` rows. Rows
with the same ``(instrument_id, ex_date, kind)`` describe the same event and *vote* on
``ratio`` and ``amount`` (each field separately; a source that leaves a field ``None``
abstains on it). Values are compared at the stored precision (8 fractional digits).

* Unanimous (all voting sources agree on every field): one canonical row, ``disputed=False``.
* Disagreement on a field: the value with the most votes wins; a tie goes to the best-ranked
  source (position in ``source_priority``; sources not listed rank after it, in
  lexicographic order). The conflict is *recorded* (``ActionConflict`` with every source's
  value) and the merged action is ``disputed=True`` / ``QualityFlag.DIVERGENT``. Values are
  never averaged.
* The canonical row's ``source`` is the best-ranked source among all contributors.
* Several rows from one source for the same key: the later one in the input wins.

Stale history (``find_stale_after_split``)
------------------------------------------
Under ``research-daily-v2`` a stored bar is split-adjusted *as of the day it was fetched*.
A bar whose session precedes a split's ``ex_date`` but that was fetched before that
``ex_date`` was never adjusted for the split, so it sits on a different price basis than
bars fetched later and needs a refetch. ``stale_bar_indexes`` names those bars one by one;
the bar reader ORs ``QualityFlag.STALE`` onto them (meaning "fetched before a later split,
the price basis may be stale"), without refetching anything.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Iterable, Iterator, Mapping, Sequence

from openbars.core.models import Adjustment, CorporateAction, CorporateActionKind, QualityFlag

__all__ = [
    "ActionConflict",
    "MergedAction",
    "StaleRange",
    "find_stale_after_split",
    "stale_bar_indexes",
    "merge_corporate_actions",
    "stale_instruments",
]

_QUANTUM = Decimal(1).scaleb(-8)


def _q(value: Decimal | None) -> Decimal | None:
    return None if value is None else value.quantize(_QUANTUM, rounding=ROUND_HALF_EVEN)


@dataclass(frozen=True)
class ActionConflict:
    """Sources disagreed on one field of one event."""

    field: str  # "ratio" | "amount"
    votes: Mapping[str, Decimal]  # source -> reported value
    chosen: Decimal


@dataclass(frozen=True)
class MergedAction:
    action: CorporateAction  # the canonical row
    sources: tuple[str, ...]  # every contributing source, best-ranked first
    conflicts: tuple[ActionConflict, ...] = ()

    @property
    def disputed(self) -> bool:
        return bool(self.conflicts)

    @property
    def quality(self) -> QualityFlag:
        flags = QualityFlag.NONE
        if self.conflicts:
            flags |= QualityFlag.DIVERGENT
        if len(self.sources) == 1:
            flags |= QualityFlag.SINGLE_SOURCE
        return flags


def merge_corporate_actions(
    actions: Iterable[CorporateAction], *, source_priority: Sequence[str] = ()
) -> list[MergedAction]:
    """Vote observations into one canonical action per ``(instrument_id, ex_date, kind)``.

    Output is sorted by ``(instrument_id, ex_date, kind)``. See the module docstring.
    """
    rank = {s: i for i, s in enumerate(source_priority)}

    def source_rank(source: str) -> tuple[int, str]:
        return (rank.get(source, len(rank)), source)

    latest: dict[tuple, dict[str, CorporateAction]] = defaultdict(dict)
    for a in actions:
        latest[(a.instrument_id, a.ex_date, CorporateActionKind(a.kind).value)][a.source] = a

    merged: list[MergedAction] = []
    for key in sorted(latest):
        by_source = latest[key]
        ordered = sorted(by_source, key=source_rank)
        chosen: dict[str, Decimal | None] = {}
        conflicts: list[ActionConflict] = []
        for name in ("ratio", "amount"):
            votes = {s: getattr(by_source[s], name) for s in ordered if getattr(by_source[s], name) is not None}
            if not votes:
                chosen[name] = None
                continue
            tally: dict[Decimal, list[str]] = defaultdict(list)
            for s, v in votes.items():
                tally[_q(v)].append(s)
            # most votes first; ties -> the group holding the best-ranked source
            winner = min(tally, key=lambda v: (-len(tally[v]), source_rank(tally[v][0])))
            chosen[name] = votes[tally[winner][0]]
            if len(tally) > 1:
                conflicts.append(ActionConflict(name, dict(votes), chosen[name]))
        first = by_source[ordered[0]]
        merged.append(
            MergedAction(
                CorporateAction(
                    first.instrument_id,
                    first.ex_date,
                    first.kind,
                    ordered[0],
                    ratio=chosen["ratio"],
                    amount=chosen["amount"],
                ),
                tuple(ordered),
                tuple(conflicts),
            )
        )
    return merged


# --- stale history ----------------------------------------------------------------------


@dataclass(frozen=True)
class StaleRange:
    """Stored bars of ``instrument_id`` that predate ``ex_date`` and were fetched before it."""

    instrument_id: str
    ex_date: date
    ratio: Decimal
    first_session: date
    last_session: date
    bar_count: int
    unknown_fetch_time: int = 0  # how many of ``bar_count`` carry no fetch timestamp


def _fetched_on(bar: object) -> date | None:
    stamp = getattr(bar, "generated_at", None) or getattr(bar, "ingested_at", None)
    if not isinstance(stamp, datetime):
        return None
    return stamp.astimezone(UTC).date() if stamp.tzinfo else stamp.date()


def _iter_stale(
    bars: Iterable[object], actions: Iterable[CorporateAction], treat_unknown_as_stale: bool
) -> Iterator[tuple[int, str, date, CorporateAction, bool]]:
    """``(bar index, instrument, session, split, fetch time unknown)`` per stale bar/split pair."""
    splits: dict[str, list[CorporateAction]] = defaultdict(list)
    for m in merge_corporate_actions(a for a in actions if a.kind is CorporateActionKind.SPLIT):
        a = m.action
        if a.ratio is not None and a.ratio != 1:
            splits[a.instrument_id].append(a)
    if not splits:
        return
    for index, bar in enumerate(bars):
        iid = bar.instrument_id  # type: ignore[attr-defined]
        if iid not in splits or Adjustment(getattr(bar, "adjustment", Adjustment.SPLIT)) is Adjustment.RAW:
            continue
        session = bar.session_date  # type: ignore[attr-defined]
        fetched = _fetched_on(bar)
        for split in splits[iid]:
            if session >= split.ex_date:
                continue
            if fetched is None:
                if not treat_unknown_as_stale:
                    continue
            elif fetched >= split.ex_date:
                continue
            yield index, iid, session, split, fetched is None


def find_stale_after_split(
    bars: Iterable[object],
    actions: Iterable[CorporateAction],
    *,
    treat_unknown_as_stale: bool = True,
) -> list[StaleRange]:
    """Which stored bars need a refetch because a later split post-dates their fetch.

    ``bars`` are ``Bar`` / ``CanonicalBarRow`` / ``RawBarRow``-like objects (``instrument_id``,
    ``session_date``, ``adjustment`` and a fetch time: ``generated_at`` or ``ingested_at``).
    ``actions`` are splits (ratio != 1, deduplicated by ``(instrument, ex_date)`` through
    ``merge_corporate_actions``). A bar is stale for a split when ``session_date < ex_date``
    and it was fetched before ``ex_date`` (a bar fetched on the ex-date is assumed adjusted).
    Bars stored ``RAW`` never go stale. A bar without a fetch time is reported
    (``unknown_fetch_time``) unless ``treat_unknown_as_stale=False``. Returns one range per
    ``(instrument, split)`` sorted by instrument and ex-date.
    """
    stats: dict[tuple[str, date], list] = {}
    for _, iid, session, split, unknown in _iter_stale(bars, actions, treat_unknown_as_stale):
        entry = stats.setdefault((iid, split.ex_date), [session, session, 0, 0, split.ratio])
        entry[0], entry[1] = min(entry[0], session), max(entry[1], session)
        entry[2] += 1
        entry[3] += unknown
    return [
        StaleRange(iid, ex, e[4], e[0], e[1], e[2], e[3])
        for (iid, ex), e in sorted(stats.items())
    ]


def stale_bar_indexes(
    bars: Iterable[object], actions: Iterable[CorporateAction], *, treat_unknown_as_stale: bool = True
) -> set[int]:
    """Positions (in ``bars``) of the bars ``find_stale_after_split`` counts as stale."""
    return {i for i, *_ in _iter_stale(bars, actions, treat_unknown_as_stale)}


def stale_instruments(
    bars: Iterable[object], actions: Iterable[CorporateAction], *, treat_unknown_as_stale: bool = True
) -> set[str]:
    """Instrument ids with at least one stale range (see ``find_stale_after_split``)."""
    return {
        r.instrument_id
        for r in find_stale_after_split(bars, actions, treat_unknown_as_stale=treat_unknown_as_stale)
    }
