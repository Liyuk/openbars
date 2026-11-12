"""Compare an openbars canonical series to a backend research-daily-v2 series.

This module is pure: it never talks to the network, never sees credentials, and never
writes files. Classification is limited to price, volume, missing sessions, and adjustment.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Iterable, Mapping

from openbars.core.ohlcv import PRICE_SCALE, VOLUME_SCALE

PRICE_TOLERANCE_PCT = Decimal("0.5")
VOLUME_TOLERANCE_PCT = Decimal("5")

DIFF_PRICE = "price"
DIFF_VOLUME = "volume"
DIFF_MISSING_LOCAL = "missing_local"  # present in the backend, absent in openbars
DIFF_MISSING_BACKEND = "missing_backend"  # present in openbars, absent in the backend
DIFF_ADJUSTMENT = "adjustment"


@dataclass(frozen=True)
class ShadowBar:
    """One session of a reconciled daily series (either side)."""

    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None
    adjustment: str | None = None
    provider: str | None = None
    feed: str | None = None

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> "ShadowBar":
        day = raw.get("session_date") or raw.get("date")
        if isinstance(day, str):
            day = date.fromisoformat(day)
        if not isinstance(day, date):
            raise ValueError("bar is missing a session date")
        def _dec(name: str) -> Decimal:
            value = raw[name]
            return value if isinstance(value, Decimal) else Decimal(str(value))
        vol = raw.get("volume")
        return cls(
            session_date=day,
            open=_dec("open"),
            high=_dec("high"),
            low=_dec("low"),
            close=_dec("close"),
            volume=None if vol is None or vol == "" else (
                vol if isinstance(vol, Decimal) else Decimal(str(vol))
            ),
            adjustment=_opt_str(raw.get("adjustment")),
            provider=_opt_str(raw.get("provider")),
            feed=_opt_str(raw.get("feed")),
        )


def _opt_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass
class CellDiff:
    symbol: str
    session_date: date
    kinds: tuple[str, ...]
    detail: str


@dataclass
class SymbolReport:
    symbol: str
    local_sessions: int
    backend_sessions: int
    comparable: int
    diffs: list[CellDiff] = field(default_factory=list)
    counts: Counter[str] = field(default_factory=Counter)

    @property
    def price_agreements(self) -> int:
        return self.comparable - sum(1 for d in self.diffs if DIFF_PRICE in d.kinds)

    @property
    def volume_agreements(self) -> int:
        return self.comparable - sum(1 for d in self.diffs if DIFF_VOLUME in d.kinds)


@dataclass
class ShadowReport:
    window_start: date
    window_end: date
    policy_version: str
    price_tolerance_pct: Decimal
    volume_tolerance_pct: Decimal
    symbols: list[SymbolReport] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def comparable(self) -> int:
        return sum(s.comparable for s in self.symbols)

    def rate(self, kind: str | None = None) -> Decimal | None:
        """Agreement rate over comparable cells; ``kind`` limits the numerator to that diff."""
        total = self.comparable
        if total == 0:
            return None
        if kind is None:
            disagreed = sum(1 for s in self.symbols for d in s.diffs if d.kinds and DIFF_MISSING_LOCAL not in d.kinds and DIFF_MISSING_BACKEND not in d.kinds)
            # overall price+volume+adjustment disagreements on overlapping sessions
            disagreed = sum(
                1
                for s in self.symbols
                for d in s.diffs
                if any(k in d.kinds for k in (DIFF_PRICE, DIFF_VOLUME, DIFF_ADJUSTMENT))
            )
            return (Decimal(total - disagreed) / Decimal(total) * 100).quantize(Decimal("0.01"))
        disagreed = sum(1 for s in self.symbols for d in s.diffs if kind in d.kinds)
        return (Decimal(total - disagreed) / Decimal(total) * 100).quantize(Decimal("0.01"))

    def missing_counts(self) -> Counter[str]:
        counts: Counter[str] = Counter()
        for s in self.symbols:
            counts.update(s.counts)
        return counts

    def to_dict(self) -> dict[str, object]:
        missing = self.missing_counts()
        return {
            "window": {"start": self.window_start.isoformat(), "end": self.window_end.isoformat()},
            "policy_version": self.policy_version,
            "tolerances": {
                "price_pct": str(self.price_tolerance_pct),
                "volume_pct": str(self.volume_tolerance_pct),
            },
            "symbols": len(self.symbols),
            "comparable_sessions": self.comparable,
            "agreement_pct": {
                "overall": _rate_str(self.rate()),
                "price": _rate_str(self.rate(DIFF_PRICE)),
                "volume": _rate_str(self.rate(DIFF_VOLUME)),
                "adjustment": _rate_str(self.rate(DIFF_ADJUSTMENT)),
            },
            "missing": {
                DIFF_MISSING_LOCAL: missing.get(DIFF_MISSING_LOCAL, 0),
                DIFF_MISSING_BACKEND: missing.get(DIFF_MISSING_BACKEND, 0),
            },
            "diff_counts": {k: v for k, v in sorted(missing.items())},
            "per_symbol": [
                {
                    "symbol": s.symbol,
                    "local_sessions": s.local_sessions,
                    "backend_sessions": s.backend_sessions,
                    "comparable": s.comparable,
                    "counts": dict(s.counts),
                    "diffs": [
                        {
                            "session_date": d.session_date.isoformat(),
                            "kinds": list(d.kinds),
                            "detail": d.detail,
                        }
                        for d in s.diffs
                    ],
                }
                for s in self.symbols
            ],
            "notes": list(self.notes),
        }


def _rate_str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def rel_diff_pct(left: Decimal, right: Decimal) -> Decimal:
    """Percentage relative difference vs the mid of the two values."""
    if left == right:
        return Decimal("0")
    mid = (abs(left) + abs(right)) / 2
    if mid == 0:
        return Decimal("100")
    return (abs(left - right) / mid * 100)


def _price_fields(bar: ShadowBar) -> tuple[tuple[str, Decimal], ...]:
    return (("open", bar.open), ("high", bar.high), ("low", bar.low), ("close", bar.close))


def compare_symbol(
    symbol: str,
    local: Iterable[ShadowBar],
    backend: Iterable[ShadowBar],
    *,
    price_tolerance_pct: Decimal = PRICE_TOLERANCE_PCT,
    volume_tolerance_pct: Decimal = VOLUME_TOLERANCE_PCT,
) -> SymbolReport:
    loc = {b.session_date: b for b in local}
    back = {b.session_date: b for b in backend}
    report = SymbolReport(symbol=symbol, local_sessions=len(loc), backend_sessions=len(back), comparable=0)
    for day in sorted(set(loc) | set(back)):
        a, b = loc.get(day), back.get(day)
        if a is None:
            report.counts[DIFF_MISSING_LOCAL] += 1
            report.diffs.append(CellDiff(symbol, day, (DIFF_MISSING_LOCAL,), "present in backend only"))
            continue
        if b is None:
            report.counts[DIFF_MISSING_BACKEND] += 1
            report.diffs.append(CellDiff(symbol, day, (DIFF_MISSING_BACKEND,), "present in openbars only"))
            continue
        report.comparable += 1
        kinds: list[str] = []
        parts: list[str] = []
        for name, av in _price_fields(a):
            bv = getattr(b, name)
            delta = rel_diff_pct(av, bv)
            if delta > price_tolerance_pct:
                kinds.append(DIFF_PRICE)
                parts.append(f"{name} {delta.quantize(Decimal('0.01'))}%")
                break
        if a.volume is not None and b.volume is not None:
            vdelta = rel_diff_pct(a.volume, b.volume)
            if vdelta > volume_tolerance_pct:
                kinds.append(DIFF_VOLUME)
                parts.append(f"volume {vdelta.quantize(Decimal('0.01'))}%")
        elif (a.volume is None) != (b.volume is None):
            kinds.append(DIFF_VOLUME)
            parts.append("volume missing on one side")
        adj_a, adj_b = (a.adjustment or "").lower(), (b.adjustment or "").lower()
        if adj_a and adj_b and adj_a != adj_b:
            kinds.append(DIFF_ADJUSTMENT)
            parts.append(f"adjustment {adj_a} vs {adj_b}")
        if kinds:
            for k in kinds:
                report.counts[k] += 1
            report.diffs.append(CellDiff(symbol, day, tuple(kinds), "; ".join(parts)))
    return report


def compare_shadow(
    series: Mapping[str, tuple[Iterable[ShadowBar], Iterable[ShadowBar]]],
    *,
    window_start: date,
    window_end: date,
    policy_version: str = "research-daily-v2",
    price_tolerance_pct: Decimal = PRICE_TOLERANCE_PCT,
    volume_tolerance_pct: Decimal = VOLUME_TOLERANCE_PCT,
    notes: Iterable[str] = (),
) -> ShadowReport:
    report = ShadowReport(
        window_start=window_start,
        window_end=window_end,
        policy_version=policy_version,
        price_tolerance_pct=price_tolerance_pct,
        volume_tolerance_pct=volume_tolerance_pct,
        notes=list(notes),
    )
    for symbol in sorted(series):
        local, backend = series[symbol]
        report.symbols.append(
            compare_symbol(
                symbol, local, backend,
                price_tolerance_pct=price_tolerance_pct,
                volume_tolerance_pct=volume_tolerance_pct,
            )
        )
    return report


def format_report(report: ShadowReport) -> list[str]:
    """Human-readable summary. Never includes credentials or raw payloads."""
    lines = [
        f"shadow reconcile  {report.window_start.isoformat()}..{report.window_end.isoformat()}  "
        f"policy={report.policy_version}",
        f"symbols: {len(report.symbols)}  comparable sessions: {report.comparable}",
        "agreement (overlapping sessions): "
        f"overall={_rate_str(report.rate()) or 'n/a'}%  "
        f"price={_rate_str(report.rate(DIFF_PRICE)) or 'n/a'}%  "
        f"volume={_rate_str(report.rate(DIFF_VOLUME)) or 'n/a'}%  "
        f"adjustment={_rate_str(report.rate(DIFF_ADJUSTMENT)) or 'n/a'}%",
    ]
    missing = report.missing_counts()
    lines.append(
        "missing sessions: "
        f"openbars={missing.get(DIFF_MISSING_LOCAL, 0)}  "
        f"backend={missing.get(DIFF_MISSING_BACKEND, 0)}"
    )
    lines.append(
        f"tolerances: price {report.price_tolerance_pct}%  volume {report.volume_tolerance_pct}%"
    )
    if report.notes:
        lines.extend(f"note: {n}" for n in report.notes)
    # Per-symbol one-liners; cap listed diffs so a large run stays readable.
    for symbol in report.symbols:
        counts = ", ".join(f"{k}={v}" for k, v in sorted(symbol.counts.items())) or "ok"
        lines.append(
            f"  {symbol.symbol}: local={symbol.local_sessions} backend={symbol.backend_sessions} "
            f"comparable={symbol.comparable} {counts}"
        )
    return lines


# imported for callers that want the official scale when they quantize fixtures
__all__ = [
    "PRICE_SCALE",
    "VOLUME_SCALE",
    "PRICE_TOLERANCE_PCT",
    "VOLUME_TOLERANCE_PCT",
    "DIFF_PRICE",
    "DIFF_VOLUME",
    "DIFF_MISSING_LOCAL",
    "DIFF_MISSING_BACKEND",
    "DIFF_ADJUSTMENT",
    "ShadowBar",
    "CellDiff",
    "SymbolReport",
    "ShadowReport",
    "rel_diff_pct",
    "compare_symbol",
    "compare_shadow",
    "format_report",
]
