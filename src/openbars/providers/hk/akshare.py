"""AKShare Hong Kong daily bars (source identity ``akshare/eastmoney/raw``).

Uses ``stock_hk_hist`` (Eastmoney). The **library** is MIT; that does **not** grant
rights to the underlying quotes. AKShare's official position is academic research;
commercial use is the caller's risk. Eastmoney's legal terms forbid copying or
redistributing exchange quotes without prior written consent of the exchanges
(https://about.eastmoney.com/home/disclaimer). openbars only fetches into the
user's local process / data dir and does not ship quotes or expose a public API.
Callers must follow those terms. Default pacing is deliberately low
(4 requests/minute and 80/day) and the adapter is per-symbol: it is a fallback,
not a bulk crawl. No VPS is used here; network access is the caller's.

``adjust=""`` (unadjusted) is the only mode this adapter requests. Vendor ``qfq`` /
``hfq`` series are not treated as the canonical basis (see discussion #68 / issue #75).
Design: https://github.com/Liyuk/openbars/discussions/68
"""

from __future__ import annotations

import numbers
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
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
from openbars.core.series import (
    AdjustmentScope,
    BarSeriesIdentity,
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
)
from openbars.normalize.hk_instruments import from_yahoo_hk_ticker
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
from openbars.providers.http import decimal_value, digest_payload
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy, RateLimit

__all__ = [
    "AKSHARE_EASTMONEY_RAW",
    "AKShareHKProvider",
    "AKShareHKResult",
    "to_akshare_hk_symbol",
]

PROVIDER_ID = "akshare"
FEED = "eastmoney"
ADJUSTMENT = "raw"
TERMS_URL = "https://about.eastmoney.com/home/disclaimer"
AKSHARE_PROJECT_URL = "https://github.com/akfamily/akshare"
MAX_SYMBOLS_PER_REQUEST = 1
REQUESTS_PER_MINUTE = 4
REQUESTS_PER_DAY = 80
NOT_INSTALLED_CODE = "akshare_not_installed"
_OHLCV = ("open", "high", "low", "close", "volume")
_TRANSIENT_MARKERS = (
    "rate",
    "timeout",
    "timed out",
    "temporarily",
    "connection",
    "too many requests",
)
_COLUMNS = {
    "date": "date",
    "datetime": "date",
    "日期": "date",
    "open": "open",
    "开盘": "open",
    "high": "high",
    "最高": "high",
    "low": "low",
    "最低": "low",
    "close": "close",
    "收盘": "close",
    "volume": "volume",
    "成交量": "volume",
}

Hist = Callable[..., Any]

AKSHARE_EASTMONEY_RAW = BarSeriesIdentity(
    provider=PROVIDER_ID,
    feed=FEED,
    legacy_adjustment=ADJUSTMENT,
    interval=Interval.D1,
    price_basis=PriceBasis.RAW,
    adjustment_scope=AdjustmentScope.NONE,
    session_semantics=SessionSemantics.REGULAR,
    currency="HKD",
    volume_basis=VolumeBasis.AS_TRADED,
)


def to_akshare_hk_symbol(symbol: str) -> str:
    """Map a 5-digit id or common spelling to AKShare's ``00700`` form."""
    return from_yahoo_hk_ticker(symbol)


def _unique_symbols(symbols: Iterable[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for raw in symbols:
        if not raw or not str(raw).strip():
            continue
        text = str(raw).strip()
        code = to_akshare_hk_symbol(text)
        if code in seen:
            continue
        seen.add(code)
        unique.append(text)
    return unique


def _akshare_hist(symbol: str, **kwargs: Any) -> Any:
    try:
        import akshare
    except ImportError:
        raise ProviderError(
            NOT_INSTALLED_CODE,
            "akshare is not installed; install the extra (openbars[akshare]) "
            "or pass a hist callable",
            retryable=False,
        ) from None
    return akshare.stock_hk_hist(symbol, **kwargs)


@dataclass(frozen=True)
class AKShareHKResult:
    batch: RawBatch
    symbol_errors: Mapping[str, ProviderError] = field(default_factory=dict)
    skipped: Mapping[str, int] = field(default_factory=dict)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("", "nan", "none")
    try:
        return bool(value != value)  # noqa: PLR0124 - NaN
    except Exception:  # noqa: BLE001 - exotic scalar types
        return False


def _quantized(value: Any, name: str, scale: int) -> Decimal:
    if isinstance(value, (bool, int, Decimal, str)) or value is None:
        return decimal_value(value, name)
    if not isinstance(value, numbers.Real):
        return decimal_value(value, name)
    number = decimal_value(float(value), name)
    quantum = Decimal(1).scaleb(-scale)
    try:
        with localcontext() as ctx:
            ctx.prec = 80
            return number.quantize(quantum, rounding=ROUND_HALF_EVEN)
    except InvalidOperation:
        raise ValueError(f"{name} is out of range") from None


def _field_name(key: Any) -> str:
    raw = str(key).strip()
    return _COLUMNS.get(raw, _COLUMNS.get(raw.lower(), raw.lower()))


def _rows(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if hasattr(data, "empty") and bool(data.empty):
        return []
    if hasattr(data, "reset_index") and hasattr(data, "to_dict"):
        data = data.reset_index().to_dict("records")
    elif isinstance(data, Mapping):
        columns = {_field_name(k): list(v) for k, v in data.items()}
        length = max((len(v) for v in columns.values()), default=0)
        data = [
            {k: v[i] for k, v in columns.items() if i < len(v)} for i in range(length)
        ]
    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
        raise ProviderError(
            "invalid_payload", "hist result is not tabular", retryable=False
        )
    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, Mapping):
            raise ProviderError(
                "invalid_payload", "hist row is not a mapping", retryable=False
            )
        rows.append({_field_name(k): v for k, v in item.items()})
    return rows


def _row_date(row: Mapping[str, Any]) -> date:
    value = row.get("date")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip().replace("/", "-")
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        return date.fromisoformat(text[:10])
    raise ValueError("row has no date")


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return True
    text = f"{type(exc).__name__} {exc}".lower()
    return any(marker in text for marker in _TRANSIENT_MARKERS)


class AKShareHKProvider:
    id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        hist: Hist | None = None,
        *,
        quota_gate: QuotaGate | None = None,
        before_request: Callable[[], None] | None = None,
        clock: Callable[[], datetime] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._hist = hist
        self._quota_gate: QuotaGate = (
            quota_gate if quota_gate is not None else NullQuotaGate()
        )
        self._before_request = before_request
        self._clock = clock or (lambda: datetime.now(UTC))
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)

    def capabilities(self) -> Sequence[SourceCapability]:
        return (
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.HK,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY, AssetClass.ETF}),
                history_start=None,
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=MAX_SYMBOLS_PER_REQUEST,
                adjusted_modes=frozenset({Adjustment.RAW}),
                feed=FEED,
                terms=ProviderTerms(
                    personal_only=True,
                    redistribution_allowed=False,
                    url=TERMS_URL,
                    notes=(
                        "AKShare is MIT; quotes come from Eastmoney and similar sites and "
                        "are not relicensed. Official use is academic research. Do not "
                        "redistribute exchange quotes. Local private storage only; low-rate "
                        "per-symbol fallback, not a bulk crawl. See also "
                        f"{AKSHARE_PROJECT_URL}."
                    ),
                ),
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(
                RateLimit(
                    REQUESTS_PER_MINUTE, timedelta(minutes=1), label="per_minute"
                ),
                RateLimit(REQUESTS_PER_DAY, timedelta(days=1), label="per_24h"),
            ),
            max_batch_symbols=1,
            official_docs_url=AKSHARE_PROJECT_URL,
        )

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        raise NotImplementedError(
            "instrument listing is not part of the AKShare HK adapter"
        )

    def fetch_corporate_actions(
        self, request: FetchRequest
    ) -> Iterable[CorporateAction]:
        raise NotImplementedError(
            "corporate actions are not part of the AKShare HK adapter"
        )

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        return self.fetch_bars_result(request).batch

    def fetch_bars_result(self, request: FetchRequest) -> AKShareHKResult:
        if request.market is not Market.HK or request.interval is not Interval.D1:
            raise ProviderError(
                "unsupported_request",
                "AKShare HK adapter serves HK 1d bars only",
                retryable=False,
            )
        feed = (request.feed or FEED).strip().lower()
        if feed != FEED:
            raise ProviderError(
                "invalid_request", f"unsupported feed {feed!r}", retryable=False
            )
        adjustment = request.adjustment or ADJUSTMENT
        adjustment = (
            (adjustment.value if isinstance(adjustment, Adjustment) else adjustment)
            .strip()
            .lower()
        )
        if adjustment != ADJUSTMENT:
            raise ProviderError(
                "invalid_request",
                f"unsupported adjustment {adjustment!r}; only unadjusted (raw) is fetched",
                retryable=False,
            )
        self._require_hk_coverage(request.window)
        try:
            symbols = _unique_symbols(request.symbols)
        except ValueError as exc:
            raise ProviderError("invalid_request", str(exc), retryable=False) from exc
        if not symbols:
            raise ProviderError(
                "invalid_request", "no symbols requested", retryable=False
            )
        first = self._decode_cursor(request.cursor, len(symbols))

        hist = self._hist or _akshare_hist
        series = AKSHARE_EASTMONEY_RAW
        fetch_id = self._fetch_id()
        window = request.window
        kwargs = {
            "period": "daily",
            "start_date": window.start.strftime("%Y%m%d"),
            "end_date": window.end.strftime("%Y%m%d"),
            "adjust": "",
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
                next_cursor = str(index)
                break
            try:
                data = hist(to_akshare_hk_symbol(symbol), **kwargs)
                rows = _rows(data)
            except ProviderError as exc:
                if exc.code == NOT_INSTALLED_CODE:
                    raise
                errors[symbol] = exc
                continue
            except Exception as exc:  # noqa: BLE001 - classified below
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
        return AKShareHKResult(batch, dict(errors), dict(skipped))

    @staticmethod
    def _decode_cursor(cursor: str | None, count: int) -> int:
        if cursor is None:
            return 0
        if not cursor.isdigit() or int(cursor) >= count:
            raise ProviderError(
                "invalid_request", "malformed fetch cursor", retryable=False
            )
        return int(cursor)

    def _symbol_error(self, exc: Exception) -> ProviderError:
        if not _is_transient(exc):
            return ProviderError(
                "provider_unavailable",
                f"hist failed ({type(exc).__name__})",
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
    def _require_hk_coverage(window: DateRange) -> None:
        from openbars.calendars.hk import coverage  # lazy: heavy import

        first, last = coverage()
        if window.start < first or window.end > last:
            raise ProviderError(
                "invalid_request",
                f"XHKG holiday coverage is {first.isoformat()}..{last.isoformat()}; "
                f"window {window.start.isoformat()}..{window.end.isoformat()} is outside that range. "
                "Upgrade exchange-calendars rather than inventing sessions.",
                retryable=False,
            )

    @staticmethod
    def _hk_session_ok(day: date) -> bool:
        from openbars.calendars.hk import is_session  # lazy: heavy import

        try:
            return bool(is_session(day))
        except ValueError:
            return False

    def _parse_rows(
        self,
        symbol: str,
        rows: list[dict[str, Any]],
        window: DateRange,
        fetch_id: str,
        series: BarSeriesIdentity,
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
                if not self._hk_session_ok(day):
                    skip("non_session")
                    continue
                values = {
                    name: _quantized(
                        row.get(name),
                        name,
                        VOLUME_SCALE if name == "volume" else PRICE_SCALE,
                    )
                    for name in _OHLCV
                }
                payload = {
                    k: ("" if _is_missing(v) else str(v))
                    for k, v in sorted(row.items())
                }
                out[day] = RawBar(
                    provider=PROVIDER_ID,
                    symbol_as_returned=symbol,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=datetime.combine(day, time.min, tzinfo=UTC),
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
