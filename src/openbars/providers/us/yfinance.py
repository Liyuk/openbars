"""Optional yfinance fallback for US daily bars (source identity ``yfinance/yahoo/split``).

Yahoo publishes no official API or quota; this provider is a conservative, best-effort
fallback for history before 2016-01-01 and for gaps the priority sources do not cover.
Terms: https://policies.yahoo.com/us/en/yahoo/terms/index.htm (personal use only).

The ``yfinance`` package is an optional extra (``pip install openbars[yfinance]``). It is
imported lazily inside the default download function; tests and callers may inject any
``download(ticker, **kwargs)`` callable instead, so importing this module never needs it.
"""

from __future__ import annotations

import numbers
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from typing import Any

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    DateRange,
    Interval,
    Market,
    RawBar,
)
from openbars.core.ohlcv import PRICE_SCALE, VOLUME_SCALE
from openbars.core.series import BarSeriesCatalog, SourceIdentity
from openbars.providers.base import (
    API_VERSION,
    BatchShape,
    FetchRequest,
    ProviderInstrument,
    ProviderTerms,
    RawBatch,
    SourceCapability,
)
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.http import decimal_value, digest_payload
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy, RateLimit

__all__ = [
    "YFinanceProvider",
    "YFinanceResult",
    "chunk_symbols",
    "should_request_yfinance",
    "to_yahoo_ticker",
]

PROVIDER_ID = "yfinance"
FEED = "yahoo"
ADJUSTMENT = "split"
TERMS_URL = "https://policies.yahoo.com/us/en/yahoo/terms/index.htm"
MAX_SYMBOLS_PER_REQUEST = 50
REQUESTS_PER_MINUTE = 30
DOWNLOAD_TIMEOUT_SECONDS = 10
ALPACA_HISTORY_START = date(2016, 1, 1)

NOT_INSTALLED_CODE = "yfinance_not_installed"
_TRANSIENT_MARKERS = ("rate", "timeout", "timed out", "temporarily", "connection", "too many requests")
_OHLCV = ("open", "high", "low", "close", "volume")
_DATE_KEYS = ("date", "datetime")

Download = Callable[..., Any]


def chunk_symbols(symbols: Iterable[str], size: int = MAX_SYMBOLS_PER_REQUEST) -> list[tuple[str, ...]]:
    """Normalise (strip, upper, unique) and split into chunks of at most ``size``."""
    if size < 1:
        raise ValueError("size must be >= 1")
    unique = tuple(dict.fromkeys(s.strip().upper() for s in symbols if s and s.strip()))
    return [unique[i : i + size] for i in range(0, len(unique), size)]


def to_yahoo_ticker(symbol: str) -> str:
    """Yahoo spells share classes with a dash (``ZZZ.A`` -> ``ZZZ-A``)."""
    return symbol.replace(".", "-")


def _yfinance_download(ticker: str, **kwargs: Any) -> Any:
    """Default download: lazy import of the optional ``yfinance`` package."""
    try:
        import yfinance  # noqa: PLC0415 - optional extra, imported only when fetching
    except ImportError:
        raise ProviderError(
            NOT_INSTALLED_CODE,
            "yfinance is not installed; install the extra (openbars[yfinance]) "
            "or pass a download callable",
            retryable=False,
        ) from None
    return yfinance.download(ticker, **kwargs)


@dataclass(frozen=True)
class YFinanceResult:
    """A ``RawBatch`` plus per-symbol detail that ``RawBatch`` itself has no room for."""

    batch: RawBatch
    symbol_errors: Mapping[str, ProviderError] = field(default_factory=dict)
    skipped: Mapping[str, int] = field(default_factory=dict)  # reason -> row count


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("", "nan", "none")
    try:
        return bool(value != value)  # NaN
    except Exception:  # pragma: no cover - exotic scalar types
        return False


def _quantized(value: Any, name: str, scale: int) -> Decimal:
    """Exact ``Decimal`` for one OHLCV cell.

    Yahoo/pandas deliver IEEE floats whose shortest ``repr`` can carry binary noise
    (for example ``10.123456789012``). Floats (incl. numpy floating scalars) are therefore taken via
    ``repr`` -> ``Decimal`` and rounded ROUND_HALF_EVEN to ``scale`` places, the same rule as
    ``storage.rows.to_decimal``; no float arithmetic is involved. Exact inputs
    (``Decimal``/``int``/``str``) are passed through untouched, so ``core.ohlcv`` still
    rejects them if they carry more than ``scale`` fractional digits.
    """
    if isinstance(value, (bool, int, Decimal, str)) or value is None:
        return decimal_value(value, name)
    if not isinstance(value, numbers.Real):
        return decimal_value(value, name)  # raises invalid
    number = decimal_value(float(value), name)  # finite check; Decimal(repr(float))
    quantum = Decimal(1).scaleb(-scale)
    try:
        with localcontext() as ctx:
            ctx.prec = 80
            return number.quantize(quantum, rounding=ROUND_HALF_EVEN)
    except InvalidOperation:
        raise ValueError(f"{name} is out of range") from None


def _field_name(key: Any) -> str:
    if isinstance(key, tuple):  # (ticker, field) / (field, ticker) column pairs
        for part in key:
            name = str(part).strip().lower()
            if name in _OHLCV or name in _DATE_KEYS:
                return name
        key = key[0] if key else ""
    return str(key).strip().lower()


def _rows(data: Any) -> list[dict[str, Any]]:
    """Normalise a download result to ``[{field: value}]`` with lower-case field names."""
    if data is None:
        return []
    if hasattr(data, "reset_index") and hasattr(data, "to_dict"):  # pandas DataFrame
        data = data.reset_index().to_dict("records")
    elif isinstance(data, Mapping):  # column-oriented: {"Date": [...], "Close": [...]}
        columns = {_field_name(k): list(v) for k, v in data.items()}
        length = max((len(v) for v in columns.values()), default=0)
        data = [{k: v[i] for k, v in columns.items() if i < len(v)} for i in range(length)]
    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
        raise ProviderError("invalid_payload", "download result is not tabular", retryable=False)
    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, Mapping):
            raise ProviderError("invalid_payload", "download row is not a mapping", retryable=False)
        rows.append({_field_name(k): v for k, v in item.items()})
    return rows


def _row_date(row: Mapping[str, Any]) -> date:
    value = next((row[k] for k in _DATE_KEYS if k in row), None)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value.strip()[:10])
    raise ValueError("row has no date")


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    text = f"{type(exc).__name__} {exc}".lower()
    return any(marker in text for marker in _TRANSIENT_MARKERS)


class YFinanceProvider:
    id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        download: Download | None = None,
        *,
        batch_size: int = MAX_SYMBOLS_PER_REQUEST,
        quota_gate: QuotaGate | None = None,
        before_request: Callable[[], None] | None = None,
        clock: Callable[[], datetime] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
    ) -> None:
        if not 1 <= batch_size <= MAX_SYMBOLS_PER_REQUEST:
            raise ValueError(f"batch_size must be between 1 and {MAX_SYMBOLS_PER_REQUEST}")
        self._download = download
        self._batch_size = batch_size
        self._quota_gate: QuotaGate = quota_gate if quota_gate is not None else NullQuotaGate()
        self._before_request = before_request
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)

    # -- Provider contract -------------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        terms = ProviderTerms(
            personal_only=True,
            redistribution_allowed=False,
            url=TERMS_URL,
            notes="Unofficial Yahoo data; default fallback for pre-2016 history and primary gaps.",
        )
        return (
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.US,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY, AssetClass.ETF}),
                history_start=None,  # since listing
                batch=BatchShape.MULTI_SYMBOL,
                max_symbols_per_request=MAX_SYMBOLS_PER_REQUEST,
                adjusted_modes=frozenset({Adjustment.SPLIT}),
                feed=FEED,
                terms=terms,
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        # No official quota exists; this is a conservative default.
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(RateLimit(REQUESTS_PER_MINUTE, timedelta(minutes=1)),),
            key_scope="per_ip",
        )

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        raise NotImplementedError("instrument listing is not part of the yfinance adapter")

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        # Out of scope for the bars-only fallback; the primary source owns corporate actions.
        raise NotImplementedError("corporate actions are not part of the yfinance adapter")

    def chunk_symbols(self, symbols: Iterable[str]) -> list[tuple[str, ...]]:
        return chunk_symbols(symbols, self._batch_size)

    # -- bars ------------------------------------------------------------------------------
    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        return self.fetch_bars_result(request).batch

    def fetch_bars_result(self, request: FetchRequest) -> YFinanceResult:
        if request.market is not Market.US or request.interval is not Interval.D1:
            raise ProviderError(
                "unsupported_request", "yfinance adapter serves US 1d bars only", retryable=False
            )
        feed = (request.feed or FEED).strip().lower()
        if feed != FEED:
            raise ProviderError("invalid_request", f"unsupported feed {feed!r}", retryable=False)
        adjustment = request.adjustment or ADJUSTMENT
        adjustment = (adjustment.value if isinstance(adjustment, Adjustment) else adjustment).strip().lower()
        if adjustment != ADJUSTMENT:
            raise ProviderError(
                "invalid_request", f"unsupported adjustment {adjustment!r}", retryable=False
            )
        chunks = self.chunk_symbols(request.symbols)
        if not chunks:
            raise ProviderError("invalid_request", "no symbols requested", retryable=False)
        symbols = [s for chunk in chunks for s in chunk]
        first = self._decode_cursor(request.cursor, len(symbols))

        download = self._download or _yfinance_download
        series = BarSeriesCatalog.resolve(SourceIdentity(PROVIDER_ID, FEED, ADJUSTMENT))
        fetch_id = self._fetch_id()
        window = request.window
        kwargs: dict[str, Any] = {
            "start": window.start.isoformat(),
            "end": (window.end + timedelta(days=1)).isoformat(),  # Yahoo's end is exclusive
            "auto_adjust": False,
            "actions": False,
            "progress": False,
            "threads": False,
            "timeout": DOWNLOAD_TIMEOUT_SECONDS,
            "group_by": "ticker",
        }

        bars: list[RawBar] = []
        errors: dict[str, ProviderError] = {}
        skipped: dict[str, int] = {}
        next_cursor: str | None = None
        for index in range(first, len(symbols)):
            symbol = symbols[index]
            try:
                self._quota_gate.acquire(PROVIDER_ID)
                if self._before_request is not None:
                    self._before_request()
            except QuotaExhausted as exc:
                errors[symbol] = ProviderError(
                    "quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at
                )
                next_cursor = str(index)  # resume here; later symbols were not attempted
                break
            try:
                data = download(to_yahoo_ticker(symbol), **kwargs)
                rows = _rows(data)
            except ProviderError as exc:
                if exc.code == NOT_INSTALLED_CODE:
                    raise
                errors[symbol] = exc
                continue
            except Exception as exc:  # noqa: BLE001 - classified below, never leaked raw
                errors[symbol] = self._symbol_error(exc)
                continue
            parsed = self._parse_rows(symbol, rows, window, fetch_id, series, skipped)
            if parsed:
                bars.extend(parsed)
            else:
                errors[symbol] = ProviderError(
                    "empty_result", f"no bars returned for {symbol}", retryable=False
                )

        error = self._combine(errors)
        if not bars and error is not None:
            raise error
        bars.sort(key=lambda b: (b.symbol_as_returned, b.session_date))
        batch = RawBatch(
            provider=PROVIDER_ID,
            fetch_id=fetch_id,
            request=request,
            bars=tuple(bars),
            next_cursor=next_cursor,
            error=error,
        )
        return YFinanceResult(batch, dict(errors), dict(skipped))

    @staticmethod
    def _decode_cursor(cursor: str | None, count: int) -> int:
        if cursor is None:
            return 0
        if not cursor.isdigit() or int(cursor) >= count:
            raise ProviderError("invalid_request", "malformed fetch cursor", retryable=False)
        return int(cursor)

    def _symbol_error(self, exc: Exception) -> ProviderError:
        if not _is_transient(exc):
            return ProviderError(
                "provider_unavailable",
                f"download failed ({type(exc).__name__})",
                retryable=False,
            )
        retry_at = getattr(exc, "retry_at", None)
        if not isinstance(retry_at, datetime):
            after = getattr(exc, "retry_after", None)
            retry_at = (
                self._clock() + timedelta(seconds=float(after))
                if isinstance(after, (int, float)) and not isinstance(after, bool)
                else None
            )
        if retry_at is not None:
            self._quota_gate.penalize(PROVIDER_ID, retry_at)
        return ProviderError(
            "rate_limited",
            f"temporary block or timeout ({type(exc).__name__})",
            retryable=True,
            retry_at=retry_at,
        )

    @staticmethod
    def _combine(errors: Mapping[str, ProviderError]) -> ProviderError | None:
        """One summary error for a batch; retryable if any failed symbol is retryable."""
        if not errors:
            return None
        ordered = sorted(errors.items(), key=lambda kv: not kv[1].retryable)
        lead = ordered[0][1]
        detail = ", ".join(f"{sym}={err.code}" for sym, err in errors.items())
        return ProviderError(
            lead.code,
            f"{len(errors)} symbol(s) failed: {detail}",
            retryable=lead.retryable,
            retry_at=lead.retry_at,
        )

    @staticmethod
    def _parse_rows(
        symbol: str,
        rows: list[dict[str, Any]],
        window: DateRange,
        fetch_id: str,
        series: Any,
        skipped: dict[str, int],
    ) -> list[RawBar]:
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
                values = {
                    name: _quantized(row.get(name), name, VOLUME_SCALE if name == "volume" else PRICE_SCALE)
                    for name in _OHLCV
                }
                payload = {k: ("" if _is_missing(v) else str(v)) for k, v in sorted(row.items())}
                out[day] = RawBar(
                    provider=PROVIDER_ID,
                    symbol_as_returned=symbol,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=datetime.combine(day, time.min, tzinfo=timezone.utc),
                    open=values["open"],
                    high=values["high"],
                    low=values["low"],
                    close=values["close"],
                    volume=values["volume"],
                    feed=FEED,
                    payload_hash=digest_payload(payload),
                    session_date=day,
                    series=series,
                )
            except (ValueError, TypeError, ProviderError):
                skip("invalid_row")
        return list(out.values())


def should_request_yfinance(
    window: DateRange,
    *,
    primary_covered: DateRange | Iterable[date] | None,
    alpaca_history_start: date = ALPACA_HISTORY_START,
    sessions: Iterable[date] | None = None,
) -> list[DateRange]:
    """Parts of ``window`` yfinance should fetch: pre-``alpaca_history_start`` sessions plus
    sessions the primary source does not cover.

    ``sessions`` are the trading days of ``window`` (default: the XNYS calendar).
    Consecutive needed sessions are merged into one ``DateRange``.
    """
    if sessions is None:
        from openbars.calendars.xnys import sessions as xnys_sessions  # lazy: heavy import

        days = list(xnys_sessions(window.start, window.end))
    else:
        days = sorted({d for d in sessions if window.start <= d <= window.end})
    if primary_covered is None:
        covered: Callable[[date], bool] = lambda _d: False  # noqa: E731
    elif isinstance(primary_covered, DateRange):
        covered = lambda d: primary_covered.start <= d <= primary_covered.end  # noqa: E731
    else:
        covered_set = set(primary_covered)
        covered = covered_set.__contains__

    ranges: list[DateRange] = []
    previous_needed = False
    for day in days:
        needed = day < alpaca_history_start or not covered(day)
        if needed:
            if previous_needed and ranges:
                ranges[-1] = DateRange(ranges[-1].start, day)
            else:
                ranges.append(DateRange(day, day))
        previous_needed = needed
    return ranges
