"""Shared machinery for broker-gateway Hong Kong daily adapters (Futu OpenD #73, LongPort #74).

Both sources need the *user's own* account and market-data entitlement; openbars never opens
an account, never stores credentials, and keeps quotes in the user's local data directory only
(see discussion #68). A subclass supplies the source identity, terms and quota, and one method,
``_symbol_rows(code, window, acquire)``, that returns unadjusted daily rows for a 5-digit code
(calling ``acquire()`` before every network request so the quota gate sees each page).

Rows are mappings with ``date`` and ``open`` / ``high`` / ``low`` / ``close`` / ``volume``; the
base class validates the XHKG session, the request window and the decimal scales, and turns
per-symbol failures into a combined ``ProviderError`` with a resumable cursor on quota stops.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from typing import Any, ClassVar

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import Adjustment, AssetClass, CorporateAction, DateRange, Interval, Market, RawBar
from openbars.core.ohlcv import PRICE_SCALE, VOLUME_SCALE
from openbars.core.series import BarSeriesIdentity
from openbars.normalize.hk_instruments import normalize_hk_symbol
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import (
    API_VERSION,
    BatchShape,
    FetchRequest,
    ProviderInstrument,
    ProviderTerms,
    RawBatch,
    SourceCapability,
)
from openbars.providers.hk.akshare import _is_missing, _is_transient, _quantized, _row_date
from openbars.providers.http import digest_payload
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy

ADJUSTMENT = "raw"
_OHLCV = ("open", "high", "low", "close", "volume")


@dataclass(frozen=True)
class HkBrokerResult:
    batch: RawBatch
    symbol_errors: Mapping[str, ProviderError] = field(default_factory=dict)
    skipped: Mapping[str, int] = field(default_factory=dict)


def hk_code(symbol: str) -> str:
    """5-digit XHKG code for any accepted spelling (``00700``, ``700``, ``0700.HK``, ``HK.00700``)."""
    venue, code = normalize_hk_symbol(symbol)
    if venue != "xhkg":  # pragma: no cover - single HK venue today
        raise ValueError(f"not an XHKG symbol: {symbol!r}")
    return code


class HkBrokerDailyProvider:
    """Base class; see the module docstring. Subclasses set the ``ClassVar`` fields."""

    id: ClassVar[str]
    feed: ClassVar[str]
    series: ClassVar[BarSeriesIdentity]
    terms_url: ClassVar[str]
    terms_notes: ClassVar[str]
    not_installed_code: ClassVar[str]
    api_version = API_VERSION

    def __init__(
        self,
        *,
        quota_gate: QuotaGate | None = None,
        before_request: Callable[[], None] | None = None,
        clock: Callable[[], datetime] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._quota_gate: QuotaGate = quota_gate if quota_gate is not None else NullQuotaGate()
        self._before_request = before_request
        self._clock = clock or (lambda: datetime.now(UTC))
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)

    # -- to implement --------------------------------------------------------------------

    def quota_policy(self) -> QuotaPolicy:  # pragma: no cover - abstract
        raise NotImplementedError

    def _symbol_rows(self, code: str, window: DateRange, acquire: Callable[[], None]) -> Iterable[Mapping[str, Any]]:
        raise NotImplementedError  # pragma: no cover - abstract

    # -- Provider protocol ---------------------------------------------------------------

    def capabilities(self) -> Sequence[SourceCapability]:
        return (
            SourceCapability(
                provider=self.id,
                market=Market.HK,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY, AssetClass.ETF}),
                history_start=None,
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=1,
                adjusted_modes=frozenset({Adjustment.RAW}),
                feed=self.feed,
                terms=ProviderTerms(
                    personal_only=True, redistribution_allowed=False, url=self.terms_url, notes="待验证：港股盘后当日可得性未由适配器保证；尽力获取、不承诺 T+0。 " + self.terms_notes
                ),
            ),
        )

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        raise NotImplementedError(f"instrument listing is not part of the {self.id} HK adapter")

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        raise NotImplementedError(f"corporate actions are not part of the {self.id} HK adapter")

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        return self.fetch_bars_result(request).batch

    def fetch_bars_result(self, request: FetchRequest) -> HkBrokerResult:
        self._check_request(request)
        try:
            symbols = _unique(request.symbols)
        except ValueError as exc:
            raise ProviderError("invalid_request", str(exc), retryable=False) from exc
        if not symbols:
            raise ProviderError("invalid_request", "no symbols requested", retryable=False)
        first = _decode_cursor(request.cursor, len(symbols))
        fetch_id = self._fetch_id()
        window = request.window
        bars: list[RawBar] = []
        errors: dict[str, ProviderError] = {}
        skipped: dict[str, int] = {}
        next_cursor: str | None = None
        for index in range(first, len(symbols)):
            symbol = symbols[index]
            try:
                rows = list(self._symbol_rows(hk_code(symbol), window, self._acquire))
            except QuotaExhausted as exc:
                errors[symbol] = ProviderError("quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at)
                next_cursor = str(index)
                break
            except ProviderError as exc:
                if exc.code == self.not_installed_code:
                    raise
                errors[symbol] = exc
                continue
            except Exception as exc:  # noqa: BLE001 - classified below
                errors[symbol] = self._symbol_error(exc)
                continue
            parsed = self._parse_rows(symbol, rows, window, fetch_id, skipped)
            if parsed:
                bars.extend(parsed)
            else:
                errors[symbol] = ProviderError("empty_result", f"no bars returned for {symbol}", retryable=False)
        error = _combine(errors)
        if not bars and error is not None:
            raise error
        bars.sort(key=lambda b: (b.symbol_as_returned, b.session_date))
        batch = RawBatch(self.id, fetch_id, request, tuple(bars), next_cursor=next_cursor, error=error)
        return HkBrokerResult(batch, dict(errors), dict(skipped))

    # -- helpers -------------------------------------------------------------------------

    def _acquire(self) -> None:
        self._quota_gate.acquire(self.id)
        if self._before_request is not None:
            self._before_request()

    def _check_request(self, request: FetchRequest) -> None:
        if request.market is not Market.HK or request.interval is not Interval.D1:
            raise ProviderError("unsupported_request", f"{self.id} HK adapter serves HK 1d bars only", retryable=False)
        feed = (request.feed or self.feed).strip().lower()
        if feed != self.feed:
            raise ProviderError("invalid_request", f"unsupported feed {feed!r}", retryable=False)
        adjustment = request.adjustment or ADJUSTMENT
        adjustment = (adjustment.value if isinstance(adjustment, Adjustment) else adjustment).strip().lower()
        if adjustment != ADJUSTMENT:
            raise ProviderError(
                "invalid_request",
                f"unsupported adjustment {adjustment!r}; only unadjusted (raw) is fetched",
                retryable=False,
            )
        from openbars.calendars.hk import coverage  # lazy: heavy import

        lo, hi = coverage()
        if request.window.start < lo or request.window.end > hi:
            raise ProviderError(
                "invalid_request",
                f"XHKG holiday coverage is {lo.isoformat()}..{hi.isoformat()}; window "
                f"{request.window.start.isoformat()}..{request.window.end.isoformat()} is outside that range",
                retryable=False,
            )

    def _symbol_error(self, exc: Exception) -> ProviderError:
        if not _is_transient(exc):
            return ProviderError("provider_unavailable", f"request failed ({type(exc).__name__})", retryable=False)
        return ProviderError("rate_limited", f"temporary block or timeout ({type(exc).__name__})", retryable=True)

    def _parse_rows(
        self, symbol: str, rows: list[Mapping[str, Any]], window: DateRange, fetch_id: str, skipped: dict[str, int]
    ) -> list[RawBar]:
        from openbars.calendars.hk import is_session  # lazy: heavy import

        def skip(reason: str) -> None:
            skipped[reason] = skipped.get(reason, 0) + 1

        out: dict[date, RawBar] = {}
        for row in rows:
            if all(_is_missing(row.get(name)) for name in _OHLCV):
                skip("nan_row")
                continue
            try:
                day = _row_date(row)
                if not window.start <= day <= window.end:
                    skip("out_of_window")
                    continue
                try:
                    session = bool(is_session(day))
                except ValueError:
                    session = False
                if not session:
                    skip("non_session")
                    continue
                values = {
                    name: _quantized(row.get(name), name, VOLUME_SCALE if name == "volume" else PRICE_SCALE)
                    for name in _OHLCV
                }
                payload = {str(k): ("" if _is_missing(v) else str(v)) for k, v in sorted(row.items(), key=lambda kv: str(kv[0]))}
                out[day] = RawBar(
                    provider=self.id,
                    symbol_as_returned=symbol,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=datetime.combine(day, time.min, tzinfo=UTC),
                    open=values["open"],
                    high=values["high"],
                    low=values["low"],
                    close=values["close"],
                    volume=values["volume"],
                    feed=self.feed,
                    payload_hash=digest_payload(payload),
                    session_date=day,
                    series=self.series,
                )
            except (ValueError, TypeError, ProviderError):
                skip("invalid_row")
        return list(out.values())


def _unique(symbols: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in symbols:
        if not raw or not str(raw).strip():
            continue
        text = str(raw).strip()
        code = hk_code(text)
        if code not in seen:
            seen.add(code)
            out.append(text)
    return out


def _decode_cursor(cursor: str | None, count: int) -> int:
    if cursor is None:
        return 0
    if not cursor.isdigit() or int(cursor) >= count:
        raise ProviderError("invalid_request", "malformed fetch cursor", retryable=False)
    return int(cursor)


def _combine(errors: Mapping[str, ProviderError]) -> ProviderError | None:
    if not errors:
        return None
    lead = sorted(errors.values(), key=lambda e: not e.retryable)[0]
    detail = ", ".join(f"{sym}={err.code}" for sym, err in errors.items())
    return ProviderError(lead.code, f"{len(errors)} symbol(s) failed: {detail}", retryable=lead.retryable, retry_at=lead.retry_at)
