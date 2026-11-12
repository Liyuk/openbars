"""Manual / semi-automatic overlay for unscheduled XHKG closures (typhoon, black rain).

This module does **not** ship a list of weather holidays. ``exchange-calendars`` already
records some historical ad-hoc closures; those stay in the library. When the library
lags, or a new HKEX announcement appears *inside* the bundled coverage window, a user
(or a later importer they run) may register the day here, with a source and a
reference to the announcement. Days outside ``coverage()`` may be stored as a record,
but ``is_session`` still refuses to invent a holiday table beyond the library range.

openbars does not scrape HKEX and does not guess future typhoon days.
Design: https://github.com/Liyuk/openbars/discussions/68
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol

__all__ = [
    "CLOSURE_REASONS",
    "AdhocClosure",
    "AdhocClosureStore",
    "JsonAdhocClosureStore",
    "MemoryAdhocClosureStore",
    "default_closure_store",
    "discard_adhoc_closure",
    "load_adhoc_closures",
    "register_adhoc_closure",
]

CLOSURE_REASONS = frozenset({"typhoon", "black_rain", "other"})


def _require_day(day: date) -> date:
    if type(day) is not date:
        raise TypeError(f"day must be a datetime.date, got {type(day).__name__}")
    return day


@dataclass(frozen=True)
class AdhocClosure:
    """One unscheduled closure registered from an HKEX (or similar) announcement."""

    day: date
    reason: str
    source: str
    reference: str
    note: str | None = None
    recorded_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_day(self.day)
        if self.reason not in CLOSURE_REASONS:
            raise ValueError(
                f"reason must be one of {sorted(CLOSURE_REASONS)}, got {self.reason!r}"
            )
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError(
                "source must be a non-empty string (e.g. 'hkex_announcement')"
            )
        if not isinstance(self.reference, str) or not self.reference.strip():
            raise ValueError(
                "reference must identify the announcement (URL, circular number, or title); "
                "bare dates without a source document are not accepted"
            )
        object.__setattr__(self, "source", self.source.strip())
        object.__setattr__(self, "reference", self.reference.strip())
        if self.note is not None:
            object.__setattr__(self, "note", self.note.strip() or None)
        if self.recorded_at is not None and (
            self.recorded_at.tzinfo is None or self.recorded_at.utcoffset() is None
        ):
            raise ValueError("recorded_at must be timezone-aware")


class AdhocClosureStore(Protocol):
    def register(self, closure: AdhocClosure) -> AdhocClosure: ...
    def discard(self, day: date) -> AdhocClosure | None: ...
    def get(self, day: date) -> AdhocClosure | None: ...
    def closures(
        self, start: date | None = None, end: date | None = None
    ) -> tuple[AdhocClosure, ...]: ...


class MemoryAdhocClosureStore:
    """In-memory store; starts empty. Nothing is bundled with the package."""

    def __init__(self) -> None:
        self._rows: dict[date, AdhocClosure] = {}

    def register(self, closure: AdhocClosure) -> AdhocClosure:
        if not isinstance(closure, AdhocClosure):
            raise TypeError("closure must be an AdhocClosure")
        self._rows[closure.day] = closure
        return closure

    def discard(self, day: date) -> AdhocClosure | None:
        return self._rows.pop(_require_day(day), None)

    def get(self, day: date) -> AdhocClosure | None:
        return self._rows.get(_require_day(day))

    def closures(
        self, start: date | None = None, end: date | None = None
    ) -> tuple[AdhocClosure, ...]:
        rows = list(self._rows.values())
        if start is not None:
            start = _require_day(start)
            rows = [r for r in rows if r.day >= start]
        if end is not None:
            end = _require_day(end)
            rows = [r for r in rows if r.day <= end]
        return tuple(sorted(rows, key=lambda r: r.day))


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _parse_recorded_at(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


class JsonAdhocClosureStore:
    """JSON file the user owns. The package never ships a pre-filled file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._memory = MemoryAdhocClosureStore()
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        records = (
            payload.get("closures", payload) if isinstance(payload, dict) else payload
        )
        if not isinstance(records, list):
            raise TypeError(f"{self.path} must contain a list of closures")
        for item in records:
            if not isinstance(item, Mapping):
                raise TypeError("each closure record must be a mapping")
            self._memory.register(_closure_from_mapping(item))

    def _dump(self) -> None:
        body = {
            "closures": [
                {
                    "day": c.day.isoformat(),
                    "reason": c.reason,
                    "source": c.source,
                    "reference": c.reference,
                    "note": c.note,
                    "recorded_at": _iso(c.recorded_at),
                }
                for c in self._memory.closures()
            ]
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(body, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        tmp.replace(self.path)

    def register(self, closure: AdhocClosure) -> AdhocClosure:
        row = self._memory.register(closure)
        self._dump()
        return row

    def discard(self, day: date) -> AdhocClosure | None:
        row = self._memory.discard(day)
        self._dump()
        return row

    def get(self, day: date) -> AdhocClosure | None:
        return self._memory.get(day)

    def closures(
        self, start: date | None = None, end: date | None = None
    ) -> tuple[AdhocClosure, ...]:
        return self._memory.closures(start, end)


def _closure_from_mapping(
    item: Mapping[str, object], *, recorded_at: datetime | None = None
) -> AdhocClosure:
    raw_day = item.get("day") or item.get("date")
    if raw_day is None:
        raise ValueError("closure record is missing 'day'")
    if isinstance(raw_day, date) and type(raw_day) is date:
        day = raw_day
    else:
        day = date.fromisoformat(str(raw_day).strip()[:10])
    stamp = item.get("recorded_at", recorded_at)
    return AdhocClosure(
        day=day,
        reason=str(item.get("reason", "other")),
        source=str(item.get("source", "")),
        reference=str(item.get("reference") or item.get("url") or ""),
        note=None if item.get("note") is None else str(item.get("note")),
        recorded_at=_parse_recorded_at(stamp),
    )


_DEFAULT_STORE = MemoryAdhocClosureStore()


def default_closure_store() -> MemoryAdhocClosureStore:
    """Process-wide default store used by the module-level HK calendar functions."""
    return _DEFAULT_STORE


def register_adhoc_closure(
    day: date,
    reason: str,
    source: str,
    reference: str,
    *,
    note: str | None = None,
    recorded_at: datetime | None = None,
    store: AdhocClosureStore | None = None,
) -> AdhocClosure:
    """Register one announcement-backed closure. ``reference`` is required."""
    target = store if store is not None else default_closure_store()
    stamp = recorded_at if recorded_at is not None else datetime.now(UTC)
    return target.register(
        AdhocClosure(
            day=day,
            reason=reason,
            source=source,
            reference=reference,
            note=note,
            recorded_at=stamp,
        )
    )


def discard_adhoc_closure(
    day: date, *, store: AdhocClosureStore | None = None
) -> AdhocClosure | None:
    target = store if store is not None else default_closure_store()
    return target.discard(day)


def load_adhoc_closures(
    records: Iterable[Mapping[str, object]],
    *,
    store: AdhocClosureStore | None = None,
    recorded_at: datetime | None = None,
) -> list[AdhocClosure]:
    """Semi-automatic ingest of already-parsed announcement records.

    This does not fetch or scrape HKEX. The caller is responsible for turning an
    announcement into ``{day, reason, source, reference}`` mappings.
    """
    target = store if store is not None else default_closure_store()
    stamp = recorded_at if recorded_at is not None else datetime.now(UTC)
    out: list[AdhocClosure] = []
    for item in records:
        out.append(target.register(_closure_from_mapping(item, recorded_at=stamp)))
    return out
