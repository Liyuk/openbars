"""AKShare A-share daily backup bars (``akshare_ashare/{eastmoney,sina}/raw``).

``stock_zh_a_hist`` (Eastmoney, default feed) and ``stock_zh_a_daily`` (Sina) are used as a
low-frequency *backup* for the BaoStock A-share source. The **library** is MIT; that does not
grant rights to the underlying quotes. Eastmoney's legal terms forbid copying or
redistributing exchange quotes without the prior written consent of the exchanges and forbid
putting a disproportionate load on their site
(https://about.eastmoney.com/home/disclaimer). Sina's user terms restrict unlicensed scraping
and commercial use (https://finance.sina.com.cn/). openbars only fetches into the user's local
process / data dir and does not ship quotes or expose a public API. Callers must follow those
terms. Default pacing is deliberately low (4 requests/minute and 80/24h) and the adapter is
per-symbol: it is a fallback, not a bulk crawl. No VPS is used here; network access is the
caller's.

Following BaoStock's storage convention, only ``adjust=""`` (unadjusted) is requested. Vendor
``qfq`` / ``hfq`` series are never treated as the canonical basis; the raw bars plus BaoStock
adjust factors (when available) are the stored/derived basis. Eastmoney reports volume in
lots (``手``, 100 shares); the adapter converts it to shares so the stored volume is comparable
with BaoStock. Sina already reports shares.

Design: https://github.com/Liyuk/openbars/discussions/54
"""

from __future__ import annotations

import numbers
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from typing import Any
from zoneinfo import ZoneInfo

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
    AKSHARE_CN_EASTMONEY_RAW as AKSHARE_CN_EASTMONEY_SOURCE,
    AKSHARE_CN_SINA_RAW as AKSHARE_CN_SINA_SOURCE,
    BarSeriesCatalog,
    BarSeriesIdentity,
)
from openbars.normalize.cn_instruments import normalize_cn_symbol
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
    "AKSHARE_CN_EASTMONEY_RAW",
    "AKSHARE_CN_SINA_RAW",
    "AKShareCNProvider",
    "AKShareCNResult",
    "to_eastmoney_a_symbol",
    "to_native_cn_symbol",
    "to_sina_a_symbol",
]

PROVIDER_ID = "akshare_ashare"
DEFAULT_FEED = "eastmoney"
EASTMONEY_FEED = "eastmoney"
SINA_FEED = "sina"
FEEDS = (EASTMONEY_FEED, SINA_FEED)
LEGACY_ADJUSTMENT = "raw"
CN_TZ = ZoneInfo("Asia/Shanghai")

AKSHARE_PROJECT_URL = "https://github.com/akfamily/akshare"
EASTMONEY_TERMS_URL = "https://about.eastmoney.com/home/disclaimer"
SINA_TERMS_URL = "https://finance.sina.com.cn/"
TERMS_URL = EASTMONEY_TERMS_URL
NOT_INSTALLED_CODE = "akshare_not_installed"

# Full series semantics for the bars this adapter returns (the catalog constants above are
# ``SourceIdentity`` triples; ``RawBar.series`` needs the resolved ``BarSeriesIdentity``).
AKSHARE_CN_EASTMONEY_RAW = BarSeriesCatalog.resolve(AKSHARE_CN_EASTMONEY_SOURCE)
AKSHARE_CN_SINA_RAW = BarSeriesCatalog.resolve(AKSHARE_CN_SINA_SOURCE)

MAX_SYMBOLS_PER_REQUEST = 1
REQUESTS_PER_MINUTE = 4
REQUESTS_PER_DAY = 80

_OHLCV = ("open", "high", "low", "close", "volume")
_EXTRA_DECIMALS = (
    "amount",
    "amplitude",
    "pct_change",
    "change",
    "turnover_rate",
    "turnover_ratio",
    "outstanding_share",
)
_TRANSIENT_MARKERS = (
    "rate",
    "timeout",
    "timed out",
    "temporarily",
    "connection",
    "too many requests",
)
_FEED_SERIES: dict[str, BarSeriesIdentity] = {
    EASTMONEY_FEED: AKSHARE_CN_EASTMONEY_RAW,
    SINA_FEED: AKSHARE_CN_SINA_RAW,
}
_VOLUME_MULTIPLIER = {EASTMONEY_FEED: Decimal(100), SINA_FEED: Decimal(1)}
_VENUE_PREFIX = {"xshg": "sh", "xshe": "sz"}
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
    "amount": "amount",
    "成交额": "amount",
    "amplitude": "amplitude",
    "振幅": "amplitude",
    "pct_change": "pct_change",
    "涨跌幅": "pct_change",
    "change": "change",
    "涨跌额": "change",
    "turnover_rate": "turnover_rate",
    "换手率": "turnover_rate",
    "outstanding_share": "outstanding_share",
    "turnover": "turnover_ratio",
    "code": "code",
    "股票代码": "code",
}

Hist = Callable[..., Any]


def to_native_cn_symbol(symbol: str) -> str:
    """``600000.SH`` / ``sh.600000`` -> ``sh.600000``; bare codes are rejected."""
    venue, code = normalize_cn_symbol(symbol)
    return f"{_VENUE_PREFIX[venue]}.{code}"


def to_eastmoney_a_symbol(symbol: str) -> str:
    """The bare 6-digit code Eastmoney's ``stock_zh_a_hist`` expects."""
    _, code = normalize_cn_symbol(symbol)
    return code


def to_sina_a_symbol(symbol: str) -> str:
    """The ``sh600000`` / ``sz000001`` spelling Sina's ``stock_zh_a_daily`` expects."""
    venue, code = normalize_cn_symbol(symbol)
    return f"{_VENUE_PREFIX[venue]}{code}"


def _feed_symbol(feed: str, venue: str, code: str) -> str:
    if feed == EASTMONEY_FEED:
        return code
    return f"{_VENUE_PREFIX[venue]}{code}"


def _native_symbol(venue: str, code: str) -> str:
    return f"{_VENUE_PREFIX[venue]}.{code}"


def _unique_codes(symbols: Iterable[str]) -> list[tuple[str, str]]:
    """``(venue, code)`` pairs, deduplicated; raises ``ValueError`` on an unknown spelling."""
    unique: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in symbols:
        if not raw or not str(raw).strip():
            continue
        venue, code = normalize_cn_symbol(str(raw).strip())
        if (venue, code) in seen:
            continue
        seen.add((venue, code))
        unique.append((venue, code))
    return unique


def _akshare_hist(feed: str) -> Hist:
    try:
        import akshare
    except ImportError:
        raise ProviderError(
            NOT_INSTALLED_CODE,
            "akshare is not installed; install the extra (openbars[akshare]) "
            "or pass a hist callable",
            retryable=False,
        ) from None
    if feed == EASTMONEY_FEED:
        return akshare.stock_zh_a_hist
    return akshare.stock_zh_a_daily


@dataclass(frozen=True)
class AKShareCNResult:
    batch: RawBatch
    symbol_errors: Mapping[str, ProviderError] = field(default_factory=dict)
    skipped: Mapping[str, int] = field(default_factory=dict)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("", "nan", "none")
    try:
        return bool(value != value)  # noqa: PLR0124 - NaN / NaT
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


def _decimal_text(value: Decimal) -> str:
    """A stable human-readable decimal without float-style exponent or trailing zeros."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


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


class AKShareCNProvider:
    id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        hist: Hist | Mapping[str, Hist] | None = None,
        *,
        quota_gate: QuotaGate | None = None,
        before_request: Callable[[], None] | None = None,
        clock: Callable[[], datetime] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
    ) -> None:
        if hist is None:
            self._hist: dict[str, Hist] | None = None
        elif callable(hist):
            self._hist = {DEFAULT_FEED: hist}
        elif isinstance(hist, Mapping):
            self._hist = {str(k).strip().lower(): v for k, v in hist.items()}
        else:
            raise TypeError("hist must be a callable, a feed->callable mapping, or None")
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
                market=Market.CN,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY}),
                history_start=None,
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=MAX_SYMBOLS_PER_REQUEST,
                adjusted_modes=frozenset({Adjustment.RAW}),
                feed=DEFAULT_FEED,
                terms=ProviderTerms(
                    personal_only=True,
                    redistribution_allowed=False,
                    url=TERMS_URL,
                    notes=(
                        "待验证：盘后当日可得性未由适配器保证；尽力获取、不承诺 T+0。 "
                        "AKShare is MIT; quotes come from Eastmoney (default) and Sina and are "
                        "not relicensed. Eastmoney's terms forbid redistributing exchange "
                        "quotes without prior written consent of the exchanges and forbid "
                        "disproportionate load; Sina's terms restrict unlicensed scraping and "
                        "commercial use. Unadjusted bars only (adjust=\"\"); Eastmoney lot "
                        "volume is converted to shares. Low-rate per-symbol backup, local "
                        f"private storage only, no redistribution. See also "
                        f"{AKSHARE_PROJECT_URL} and {SINA_TERMS_URL}."
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
            key_scope="per_process",
            safety_margin=0.9,
            max_batch_symbols=1,
            official_docs_url=AKSHARE_PROJECT_URL,
        )

    def secret_values(self) -> tuple[str, ...]:
        return ()  # no API key; the upstream sites are anonymous

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        raise NotImplementedError(
            "instrument listing is not part of the AKShare A-share adapter"
        )

    def fetch_corporate_actions(
        self, request: FetchRequest
    ) -> Iterable[CorporateAction]:
        raise NotImplementedError(
            "corporate actions are not part of the AKShare A-share adapter"
        )

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        return self.fetch_bars_result(request).batch

    def fetch_bars_result(self, request: FetchRequest) -> AKShareCNResult:
        if request.market is not Market.CN or request.interval is not Interval.D1:
            raise ProviderError(
                "unsupported_request",
                "AKShare A-share adapter serves CN 1d bars only",
                retryable=False,
            )
        feed = (request.feed or DEFAULT_FEED).strip().lower()
        if feed not in FEEDS:
            raise ProviderError(
                "invalid_request", f"unsupported feed {feed!r}", retryable=False
            )
        adjustment = request.adjustment or LEGACY_ADJUSTMENT
        adjustment = (
            (adjustment.value if isinstance(adjustment, Adjustment) else adjustment)
            .strip()
            .lower()
        )
        if adjustment != LEGACY_ADJUSTMENT:
            raise ProviderError(
                "invalid_request",
                f"unsupported adjustment {adjustment!r}; only unadjusted (raw) is fetched",
                retryable=False,
            )
        sessions = self._sessions(request.window)
        try:
            codes = _unique_codes(request.symbols)
        except ValueError as exc:
            raise ProviderError("invalid_request", str(exc), retryable=False) from exc
        if not codes:
            raise ProviderError(
                "invalid_request", "no symbols requested", retryable=False
            )
        first = self._decode_cursor(request.cursor, len(codes))
        hist = self._hist_for(feed)
        series = _FEED_SERIES[feed]
        fetch_id = self._fetch_id()
        window = request.window
        kwargs: dict[str, Any] = {
            "start_date": window.start.strftime("%Y%m%d"),
            "end_date": window.end.strftime("%Y%m%d"),
            "adjust": "",
        }
        if feed == EASTMONEY_FEED:
            kwargs["period"] = "daily"

        bars: list[RawBar] = []
        errors: dict[str, ProviderError] = {}
        skipped: dict[str, int] = {}
        next_cursor: str | None = None
        for index in range(first, len(codes)):
            venue, code = codes[index]
            symbol = _native_symbol(venue, code)
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
                data = hist(_feed_symbol(feed, venue, code), **kwargs)
                rows = _rows(data)
            except ProviderError as exc:
                if exc.code == NOT_INSTALLED_CODE:
                    raise
                errors[symbol] = exc
                continue
            except Exception as exc:  # noqa: BLE001 - classified below
                errors[symbol] = self._symbol_error(exc)
                continue
            parsed = self._parse_rows(
                symbol, code, feed, rows, window, sessions, fetch_id, series, skipped
            )
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
        return AKShareCNResult(batch, dict(errors), dict(skipped))

    def _hist_for(self, feed: str) -> Hist:
        if self._hist is not None:
            hist = self._hist.get(feed)
            if hist is None:
                raise ProviderError(
                    "invalid_request",
                    f"no injected hist for feed {feed!r}",
                    retryable=False,
                )
            return hist
        return _akshare_hist(feed)

    @staticmethod
    def _decode_cursor(cursor: str | None, count: int) -> int:
        if cursor is None:
            return 0
        if not cursor.isdigit() or int(cursor) >= count:
            raise ProviderError(
                "invalid_request", "malformed fetch cursor", retryable=False
            )
        return int(cursor)

    @staticmethod
    def _sessions(window: DateRange) -> set[date]:
        from openbars.calendars import cn as cn_calendar  # lazy: heavy import

        try:
            return set(cn_calendar.sessions(window.start, window.end))
        except ValueError as exc:
            raise ProviderError(
                "invalid_request",
                f"XSHG holiday coverage does not reach window "
                f"{window.start.isoformat()}..{window.end.isoformat()}; "
                f"upgrade exchange-calendars rather than inventing sessions ({exc})",
                retryable=False,
            ) from None

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

    def _parse_rows(
        self,
        symbol: str,
        code: str,
        feed: str,
        rows: list[dict[str, Any]],
        window: DateRange,
        sessions: set[date],
        fetch_id: str,
        series: BarSeriesIdentity,
        skipped: dict[str, int],
    ) -> list[RawBar]:
        def skip(reason: str) -> None:
            skipped[reason] = skipped.get(reason, 0) + 1

        out: dict[date, RawBar] = {}
        multiplier = _VOLUME_MULTIPLIER[feed]
        for row in rows:
            if all(_is_missing(row.get(name)) for name in _OHLCV):
                skip("nan_row")
                continue
            try:
                day = _row_date(row)
                if not window.start <= day <= window.end:
                    skip("out_of_window")
                    continue
                if day not in sessions:
                    skip("non_session")
                    continue
                vendor_code = row.get("code")
                if (
                    feed == EASTMONEY_FEED
                    and not _is_missing(vendor_code)
                    and str(vendor_code).strip() != code
                ):
                    skip("other_code")
                    continue
                values = {
                    name: _quantized(
                        row.get(name),
                        name,
                        VOLUME_SCALE if name == "volume" else PRICE_SCALE,
                    )
                    for name in _OHLCV
                }
                volume = _quantized(
                    values["volume"] * multiplier, "volume", VOLUME_SCALE
                )
                extra: dict[str, object] = {"code": symbol}
                if feed == EASTMONEY_FEED:
                    extra["volume_lots"] = _decimal_text(values["volume"])
                for name in _EXTRA_DECIMALS:
                    value = row.get(name)
                    if _is_missing(value):
                        continue
                    extra[name] = _decimal_text(
                        _quantized(value, name, PRICE_SCALE)
                    )
                payload = {
                    k: ("" if _is_missing(v) else str(v))
                    for k, v in sorted(row.items())
                }
                out[day] = RawBar(
                    provider=PROVIDER_ID,
                    symbol_as_returned=symbol,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=datetime.combine(day, time.min, tzinfo=CN_TZ),
                    open=values["open"],
                    high=values["high"],
                    low=values["low"],
                    close=values["close"],
                    volume=volume,
                    feed=feed,
                    extra=extra,
                    payload_hash=digest_payload(payload),
                    session_date=day,
                    series=series,
                )
            except (ValueError, TypeError, ProviderError):
                skip("invalid_row")
        return list(out.values())
