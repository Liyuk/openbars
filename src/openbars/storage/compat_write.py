"""Helpers for ``upstream_compat`` writes (no DDL; map openbars rows onto upstream tables).

Identity: upstream uses integer ``instrument_id`` FKs; openbars uses strings. Numeric
strings are reused as the bigint; anything else is a symbol bootstrap that looks up or
inserts ``instrument`` / ``symbol_alias``. Ticker reuse (more than one instrument covering
the same symbol and session) is refused.

Canonical writes take a transaction-level advisory lock per ``(policy_version,
session_date)`` so a concurrent upstream projector cannot race the unique key. The lock
algorithm is a compatibility contract: the prefix string must stay exactly
``research-daily``.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from dataclasses import dataclass
from fractions import Fraction

from openbars.core.models import AssetClass, CorporateAction, CorporateActionKind
from openbars.storage.errors import StorageError
from openbars.storage.rows import quality_status_for

# Compatibility contract with upstream projectors (architecture.md section 11.3). Do not change.
ADVISORY_LOCK_PREFIX = "research-daily"
_INT_ID = re.compile(r"-?\d+$")
_ARTIFACT_REF = re.compile(r"^(?:source_artifact:)?(\d+)$")

# Predicate used by external research-series readers when joining symbol_alias (a)
# to a bar table (b). The second arm is a single-future-open-alias fallback.
COVERING_ALIAS_PREDICATE = """(
    (a.valid_from <= b.session_date AND (a.valid_to IS NULL OR a.valid_to >= b.session_date))
    OR (
        a.valid_from > b.session_date
        AND a.valid_to IS NULL
        AND (SELECT count(*) FROM symbol_alias h WHERE h.instrument_id = b.instrument_id) = 1
    )
)"""

RESEARCH_SERIES_READ_SQL = """
SELECT a.symbol, b.*
FROM research_daily_bar b
JOIN symbol_alias a ON a.instrument_id = b.instrument_id
WHERE b.instrument_id = ANY(?)
  AND b.session_date BETWEEN ? AND ?
  AND b.policy_version = ?
  AND (
    (a.valid_from <= b.session_date AND (a.valid_to IS NULL OR a.valid_to >= b.session_date))
    OR (
        a.valid_from > b.session_date
        AND a.valid_to IS NULL
        AND (SELECT count(*) FROM symbol_alias h WHERE h.instrument_id = b.instrument_id) = 1
    )
  )
ORDER BY b.instrument_id, b.session_date, a.symbol
"""


def advisory_lock_key(policy_version: str, session_date: date) -> int:
    """Signed 64-bit key for ``pg_advisory_xact_lock`` (sha256, first 8 bytes, big-endian)."""
    digest = hashlib.sha256(
        f"{ADVISORY_LOCK_PREFIX}:{policy_version}:{session_date.isoformat()}".encode()
    ).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def parse_int_id(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or not _INT_ID.fullmatch(text):
        return None
    return int(text)


def parse_optional_int_ref(value: str | int | None) -> int | None:
    """``source_artifact:12`` / ``"12"`` / ``12`` -> 12; anything else -> None."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    matched = _ARTIFACT_REF.fullmatch(text)
    if matched:
        return int(matched.group(1))
    return parse_int_id(text)


def quality_status_of(row: Any) -> str:
    status = getattr(row, "quality_status", None)
    if status:
        return str(status)
    flags = getattr(row, "flags", ())
    if flags:
        return quality_status_for(flags)
    return "complete"


def security_type_for(asset_class: AssetClass | str | None) -> str:
    if asset_class is None:
        return "common_stock"
    value = AssetClass(asset_class) if not isinstance(asset_class, AssetClass) else asset_class
    if value is AssetClass.EQUITY:
        return "common_stock"
    return value.value


def fingerprint_or_raise(row: Any) -> str:
    value = str(getattr(row, "source_fingerprint", "") or "")
    if not value:
        raise StorageError("canonical row is missing source_fingerprint")
    if len(value) > 64:
        raise StorageError("source_fingerprint is longer than 64 characters")
    return value


def capabilities_json(row: Any) -> str:
    caps = getattr(row, "capabilities", ()) or ()
    return json.dumps(list(caps))



@dataclass(frozen=True)
class InstrumentCandidate:
    """One instrument a symbol bootstrap could mean (covering alias on the session date)."""

    instrument_id: int
    symbol: str
    valid_from: date
    valid_to: date | None
    exchange: str | None = None
    asset_class: str | None = None
    security_type: str | None = None

    def describe(self) -> str:
        end = "open" if self.valid_to is None else self.valid_to.isoformat()
        bits = [f"instrument_id={self.instrument_id}", f"valid={self.valid_from.isoformat()}..{end}"]
        if self.exchange:
            bits.append(f"exchange={self.exchange}")
        if self.security_type:
            bits.append(f"security_type={self.security_type}")
        elif self.asset_class:
            bits.append(f"asset_class={self.asset_class}")
        return "  - " + "  ".join(bits)


class AmbiguousInstrumentError(StorageError):
    """Symbol bootstrap hit more than one covering instrument; pass a numeric id."""

    def __init__(self, symbol: str, on: date, candidates: list[InstrumentCandidate]) -> None:
        self.symbol = symbol
        self.on = on
        # one row per instrument_id (first covering alias)
        seen: dict[int, InstrumentCandidate] = {}
        for c in candidates:
            seen.setdefault(c.instrument_id, c)
        self.candidates = tuple(sorted(seen.values(), key=lambda c: c.instrument_id))
        super().__init__(self.format_report())

    def format_report(self) -> str:
        lines = [
            f"symbol {self.symbol!r} on {self.on.isoformat()} matches {len(self.candidates)} instruments; "
            "pass a numeric instrument_id (one of the ids below) instead of the ticker:",
        ]
        lines.extend(c.describe() for c in self.candidates)
        return "\n".join(lines)


def covering_alias_rows(conn: Any, symbol: str, on: date) -> list[dict[str, Any]]:
    return list(
        conn.execute(
            "SELECT a.instrument_id, a.symbol, a.valid_from, a.valid_to, a.exchange, "
            "i.asset_class, i.security_type "
            "FROM symbol_alias a JOIN instrument i ON i.id = a.instrument_id "
            "WHERE a.symbol = ? AND a.valid_from <= ? AND (a.valid_to IS NULL OR a.valid_to >= ?) "
            "ORDER BY a.instrument_id, a.valid_from",
            [symbol, on, on],
        ).fetchall()
    )


def candidates_from_rows(rows: list[Any]) -> list[InstrumentCandidate]:
    out: list[InstrumentCandidate] = []
    for r in rows:
        rec = dict(r)
        out.append(
            InstrumentCandidate(
                instrument_id=int(rec["instrument_id"]),
                symbol=str(rec["symbol"]),
                valid_from=rec["valid_from"],
                valid_to=rec["valid_to"],
                exchange=rec.get("exchange"),
                asset_class=rec.get("asset_class"),
                security_type=rec.get("security_type"),
            )
        )
    return out


def action_type_for(kind: CorporateActionKind | str, *, ratio: Any = None, amount: Any = None) -> str:
    value = CorporateActionKind(kind)
    if value is CorporateActionKind.SPLIT:
        return "split"
    if value is CorporateActionKind.DIVIDEND:
        return "stock_dividend" if ratio is not None and amount is None else "cash_dividend"
    if value is CorporateActionKind.SPINOFF:
        return "spin_off"
    if value is CorporateActionKind.SYMBOL_CHANGE:
        return "symbol_change"
    return value.value


def split_fraction(ratio: Any) -> tuple[int, int] | None:
    if ratio is None:
        return None
    frac = Fraction(ratio).limit_denominator(10_000)
    if frac.numerator <= 0 or frac.denominator <= 0:
        raise StorageError(f"split ratio must be positive, got {ratio}")
    return int(frac.numerator), int(frac.denominator)


def action_details(kind: CorporateActionKind | str, *, ratio: Any = None, amount: Any = None, currency: Any = None) -> dict[str, Any]:
    typ = action_type_for(kind, ratio=ratio, amount=amount)
    details: dict[str, Any] = {}
    if typ == "split":
        parts = split_fraction(ratio)
        if parts:
            details["new_shares_numerator"] = parts[0]
            details["old_shares_denominator"] = parts[1]
    elif typ == "cash_dividend" and amount is not None:
        details["cash_amount"] = str(amount)
    elif typ == "stock_dividend":
        parts = split_fraction(ratio)
        if parts:
            details["new_shares_numerator"] = parts[0]
            details["old_shares_denominator"] = parts[1]
    elif typ == "spin_off" and ratio is not None:
        details["ratio"] = str(ratio)
    elif typ == "rights":
        if ratio is not None:
            details["ratio"] = str(ratio)
        if amount is not None:
            details["amount"] = str(amount)
    if currency:
        details["currency"] = str(currency).strip().upper()
    return details


def action_event_key(kind: str, instrument_id: int, ex_date: date) -> str:
    """Stable event key so repeated upserts land on the same upstream event."""
    return f"openbars:{kind}:{instrument_id}:{ex_date.isoformat()}"


def _as_json(value: Any) -> Any:
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return value


def kind_from_action_type(action_type: str) -> CorporateActionKind | None:
    typ = (action_type or "").strip()
    if typ == "split":
        return CorporateActionKind.SPLIT
    if typ in {"cash_dividend", "stock_dividend", "dividend"}:
        return CorporateActionKind.DIVIDEND
    if typ in {"spin_off", "spinoff"}:
        return CorporateActionKind.SPINOFF
    if typ == "symbol_change":
        return CorporateActionKind.SYMBOL_CHANGE
    if typ in {"rights", "rights_issue"}:
        return CorporateActionKind.RIGHTS
    return None


def ratio_amount_from_details(action_type: str, details: Any) -> tuple[Decimal | None, Decimal | None]:
    rec = _as_json(details) or {}
    if not isinstance(rec, dict):
        rec = {}
    typ = (action_type or "").strip()
    ratio = amount = None
    num, den = rec.get("new_shares_numerator"), rec.get("old_shares_denominator")
    if typ in {"split", "stock_dividend"} and num is not None and den not in (None, 0, "0"):
        ratio = Decimal(str(num)) / Decimal(str(den))
    if typ == "cash_dividend" and rec.get("cash_amount") is not None:
        amount = Decimal(str(rec["cash_amount"]))
    if typ in {"spin_off", "spinoff"} and rec.get("ratio") is not None:
        ratio = Decimal(str(rec["ratio"]))
    if ratio is None and rec.get("ratio") is not None:
        ratio = Decimal(str(rec["ratio"]))
    if amount is None and rec.get("amount") is not None:
        amount = Decimal(str(rec["amount"]))
    return ratio, amount


def actions_from_revision(
    *,
    action_type: str,
    ex_date: date,
    instrument_ids: Any,
    details: Any,
    source: str | None,
) -> list[CorporateAction]:
    """Map one upstream revision (latest) back to openbars corporate actions."""
    kind = kind_from_action_type(action_type)
    if kind is None or ex_date is None:
        return []
    ratio, amount = ratio_amount_from_details(action_type, details)
    raw_ids = _as_json(instrument_ids) or []
    if isinstance(raw_ids, (str, int)):
        raw_ids = [raw_ids]
    ids = [str(i) for i in raw_ids if i is not None and str(i) != ""]
    if not ids:
        return []
    src = (source or "").strip() or "unknown"
    rec = _as_json(details) or {}
    currency = rec.get("currency") if isinstance(rec, dict) else None
    return [
        CorporateAction(
            instrument_id=i,
            ex_date=ex_date,
            kind=kind,
            source=src,
            ratio=ratio,
            amount=amount,
            currency=currency,
        )
        for i in ids
    ]


class CompatIdentity:
    """Resolve or create integer instrument ids and covering ``symbol_alias`` rows."""

    def __init__(self) -> None:
        self._cache: dict[str, int] = {}

    def resolve(
        self,
        conn: Any,
        instrument_id: str | None,
        *,
        symbol: str | None,
        on: date,
        asset_class: str = "equity",
        security_type: str | None = None,
        exchange: str = "UNKNOWN",
    ) -> int:
        token = (instrument_id or "").strip() or (symbol or "").strip()
        if not token:
            raise StorageError("cannot resolve a blank instrument id")
        cached = self._cache.get(token)
        if cached is not None:
            iid = cached
        else:
            numeric = parse_int_id(token)
            if numeric is not None:
                iid = self._ensure_numeric(
                    conn, numeric, asset_class, security_type or "common_stock"
                )
            else:
                iid = self._lookup_or_insert(
                    conn,
                    symbol=symbol or token,
                    on=on,
                    asset_class=asset_class,
                    security_type=security_type or "common_stock",
                    exchange=exchange,
                )
            self._cache[token] = iid
            self._cache[str(iid)] = iid
        alias_symbol = symbol or (None if parse_int_id(token) is not None else token)
        if alias_symbol:
            self._ensure_alias(conn, iid, alias_symbol, on=on, exchange=exchange)
        return iid

    def _ensure_numeric(self, conn: Any, iid: int, asset_class: str, security_type: str) -> int:
        existing = conn.execute("SELECT id FROM instrument WHERE id = ?", [iid]).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO instrument (id, asset_class, security_type, created_at) VALUES (?, ?, ?, ?)",
                [iid, asset_class, security_type, datetime.now(UTC)],
            )
        return iid

    def _lookup_or_insert(
        self,
        conn: Any,
        *,
        symbol: str,
        on: date,
        asset_class: str,
        security_type: str,
        exchange: str,
    ) -> int:
        rows = covering_alias_rows(conn, symbol, on)
        ids = {int(r["instrument_id"]) for r in rows}
        if len(ids) > 1:
            raise AmbiguousInstrumentError(symbol, on, candidates_from_rows(rows))
        if len(ids) == 1:
            return next(iter(ids))
        created = conn.execute(
            "INSERT INTO instrument (asset_class, security_type, created_at) VALUES (?, ?, ?) RETURNING id",
            [asset_class, security_type, datetime.now(UTC)],
        ).fetchone()
        if not created or created.get("id") is None:
            raise StorageError("failed to allocate an instrument id")
        iid = int(created["id"])
        self._insert_alias(conn, iid, symbol, on=on, exchange=exchange)
        return iid

    def _ensure_alias(self, conn: Any, iid: int, symbol: str, *, on: date, exchange: str) -> None:
        covering = conn.execute(
            "SELECT id FROM symbol_alias WHERE instrument_id = ? AND symbol = ? "
            "AND valid_from <= ? AND (valid_to IS NULL OR valid_to >= ?) LIMIT 1",
            [iid, symbol, on, on],
        ).fetchone()
        if covering is not None:
            return
        self._insert_alias(conn, iid, symbol, on=on, exchange=exchange)

    def _insert_alias(self, conn: Any, iid: int, symbol: str, *, on: date, exchange: str) -> None:
        conn.execute(
            "INSERT INTO symbol_alias (instrument_id, symbol, exchange, valid_from, "
            "valid_from_basis, valid_to, verification_status) "
            "VALUES (?, ?, ?, ?, 'unknown', NULL, 'unresolved')",
            [iid, symbol, exchange or "UNKNOWN", on],
        )
