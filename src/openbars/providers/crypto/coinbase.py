"""Coinbase Exchange public candles adapter: UTC-day OHLCV bars, no API key.

Official docs (market data / candles): https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductcandles
Public rate limits: https://docs.cdp.coinbase.com/exchange/docs/rate-limits

The default base URL is ``https://api.exchange.coinbase.com`` (Coinbase Exchange, formerly
Coinbase Pro). It is NOT the Advanced Trade API (``api.coinbase.com``).

``GET /products/{product_id}/candles?granularity=86400`` returns a JSON *array of arrays*
``[time, low, high, open, close, volume]`` (note the order: low/high come before open/close),
newest first. ``time`` is the bucket start in Unix seconds. Daily buckets are cut at
UTC 00:00, which is the crypto calendar's session definition, so ``session_date`` is the
UTC date of ``time``. A request spans at most 300 candles; longer windows are paged.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import httpx

from openbars import __version__
from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    Interval,
    Market,
    RawBar,
)
from openbars.core.series import COINBASE_EXCHANGE_RAW, BarSeriesCatalog, BarSeriesIdentity
from openbars.providers.adapter import HttpProviderAdapter
from openbars.providers.base import (
    API_VERSION,
    BatchShape,
    FetchRequest,
    ProviderInstrument,
    ProviderTerms,
    RawBatch,
    SourceCapability,
)
from openbars.providers.http import decimal_value, digest_payload, integer_value
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy, RateLimit

__all__ = ["CoinbaseProvider", "parse_candles", "parse_products"]

PROVIDER_ID = "coinbase"
FEED = "exchange"
EXCHANGE = "COINBASE"

DEFAULT_BASE_URL = "https://api.exchange.coinbase.com"
PRODUCTS_PATH = "/products"
CANDLES_PATH_TEMPLATE = "/products/{product_id}/candles"

DOCS_URL = "https://docs.cdp.coinbase.com/exchange/docs/rate-limits"
TERMS_URL = "https://www.coinbase.com/legal/user_agreement"

# Documented public REST limit: 10 requests per second per IP; every call weighs 1.
REQUESTS_PER_SECOND = 10
REQUEST_WEIGHT = 1
GRANULARITY_SECONDS = 86_400
MAX_BARS_PER_REQUEST = 300  # documented cap on candles per request

_ONE_DAY = timedelta(days=1)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_PRODUCT_RE = re.compile(r"^[A-Z0-9]{1,20}-[A-Z0-9]{1,20}$")
_MIN_CANDLE_FIELDS = 6  # time, low, high, open, close, volume


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


def _bad_request(message: str) -> ProviderError:
    return ProviderError("invalid_request", message, retryable=False)


def _midnight(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def _to_seconds(moment: datetime) -> int:
    return int((moment - _EPOCH).total_seconds())


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_candles(
    payload: Any,
    *,
    symbol: str,
    window_start: date,
    window_end: date,
    fetch_id: str,
    series: BarSeriesIdentity,
) -> list[RawBar]:
    """Parse a ``/products/{id}/candles`` array into ascending ``RawBar`` rows.

    Rows are ``[time, low, high, open, close, volume]`` (numbers never touch float). The API
    usually answers newest-first, so rows are sorted ascending by ``time`` first; duplicate
    times are ``invalid_payload``. A ``time`` that is not exactly UTC midnight rejects the
    whole payload: a mis-cut daily bar must never be stored as a UTC-day bar. Rows outside
    ``[window_start, window_end]`` are dropped (the API may treat ``end`` as inclusive).
    """
    if not isinstance(payload, list):
        raise _invalid("candles payload is not an array")
    parsed: list[tuple[int, list[Any]]] = []
    for row in payload:
        if not isinstance(row, list) or len(row) < _MIN_CANDLE_FIELDS:
            raise _invalid("candle row is not an array of at least 6 fields")
        open_s = integer_value(row[0], "time")
        if open_s % GRANULARITY_SECONDS != 0:
            raise _invalid("candle time is not UTC midnight")
        parsed.append((open_s, row))
    parsed.sort(key=lambda item: item[0])
    out: list[RawBar] = []
    previous: int | None = None
    for open_s, row in parsed:
        if previous is not None and open_s == previous:
            raise _invalid("candle rows contain a duplicate time")
        previous = open_s
        ts_start = _EPOCH + timedelta(seconds=open_s)
        session_date = ts_start.date()
        if not window_start <= session_date <= window_end:
            continue
        try:
            out.append(
                RawBar(
                    provider=PROVIDER_ID,
                    symbol_as_returned=symbol,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=ts_start,
                    low=decimal_value(row[1], "low"),
                    high=decimal_value(row[2], "high"),
                    open=decimal_value(row[3], "open"),
                    close=decimal_value(row[4], "close"),
                    volume=decimal_value(row[5], "volume"),
                    feed=FEED,
                    payload_hash=digest_payload(row),
                    session_date=session_date,
                    series=series,
                )
            )
        except (ValueError, TypeError):
            raise _invalid("candle failed OHLCV validation") from None
    return out


def _asset(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def parse_products(payload: Any) -> list[ProviderInstrument]:
    """Parse ``/products`` into ``ProviderInstrument`` rows.

    ``active`` is ``status == "online"`` and not ``trading_disabled`` (when that field is
    present). ``base_asset`` / ``quote_asset`` come from ``base_currency`` / ``quote_currency``
    (stripped; missing or blank stays ``None``). No instrument id is derived here.
    """
    if not isinstance(payload, list):
        raise _invalid("products payload is not an array")
    out: list[ProviderInstrument] = []
    for item in payload:
        if not isinstance(item, dict):
            raise _invalid("product entry is not an object")
        product_id = item.get("id")
        if not isinstance(product_id, str) or not product_id.strip():
            raise _invalid("product entry has no id")
        product_id = product_id.strip()
        out.append(
            ProviderInstrument(
                symbol=product_id,
                market=Market.CRYPTO,
                asset_class=AssetClass.CRYPTO_SPOT,
                exchange=EXCHANGE,
                active=item.get("status") == "online" and not item.get("trading_disabled", False),
                native_id=product_id,
                base_asset=_asset(item.get("base_currency")),
                quote_asset=_asset(item.get("quote_currency")),
            )
        )
    return out


class CoinbaseProvider(HttpProviderAdapter):
    id = PROVIDER_ID
    provider_id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        quota_gate: QuotaGate | None = None,
        client: httpx.Client | None = None,
        before_request: Callable[[], None] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
        user_agent: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(
            client=client, quota_gate=quota_gate, before_request=before_request, timeout=timeout
        )
        self._base_url = base_url.rstrip("/")
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)
        self._user_agent = user_agent or f"openbars/{__version__}"

    # -- HTTP ------------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        return {"Accept": "application/json", "User-Agent": self._user_agent}

    def _get(self, path: str, params: Mapping[str, Any] | None) -> httpx.Response:
        return self.get(
            f"{self._base_url}{path}",
            params=params,
            headers=self._headers(),
            weight=REQUEST_WEIGHT,
        )

    # -- Provider contract -------------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        terms = ProviderTerms(
            personal_only=False,
            redistribution_allowed=False,
            url=TERMS_URL,
            notes="Public market data, no API key. Redistribution is subject to Coinbase "
            "terms (not reviewed here, so not assumed allowed); docs: "
            "https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductcandles",
        )
        return (
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.CRYPTO,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.CRYPTO_SPOT}),
                history_start=None,  # since listing
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=1,
                max_bars_per_request=MAX_BARS_PER_REQUEST,
                adjusted_modes=frozenset({Adjustment.RAW}),
                feed=FEED,
                terms=terms,
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        # Public REST: 10 requests / second per IP; every call has weight 1.
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(RateLimit(REQUESTS_PER_SECOND, timedelta(seconds=1)),),
            key_scope="per_ip",
            safety_margin=0.9,
            max_batch_symbols=1,
            max_bars_per_request=MAX_BARS_PER_REQUEST,
            history_start=None,
            official_docs_url=DOCS_URL,
        )

    def secret_values(self) -> tuple[str, ...]:
        return ()  # public endpoints, no credentials

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        if market is not Market.CRYPTO:
            raise _bad_request("Coinbase adapter serves the crypto market only")
        return parse_products(self._load_json(self._get(PRODUCTS_PATH, None)))

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        return iter(())  # spot candles have no equity-style corporate actions

    # -- bars --------------------------------------------------------------------------
    @staticmethod
    def _load_json(response: httpx.Response) -> Any:
        try:
            # Floats become Decimal at parse time so numbers never round-trip through float.
            return json.loads(response.content, parse_float=Decimal)
        except ValueError:
            raise _invalid("response body is not valid JSON") from None

    @staticmethod
    def _product(request: FetchRequest) -> str:
        if len(request.symbols) != 1:
            raise _bad_request("Coinbase candles are fetched one product per request")
        symbol = request.symbols[0].strip().upper() if isinstance(request.symbols[0], str) else ""
        if not _PRODUCT_RE.match(symbol):
            raise _bad_request("invalid Coinbase product id")
        return symbol

    def _pages(
        self, product: str, start: datetime, end: datetime
    ) -> Iterator[tuple[datetime, Any]]:
        """Yield ``(page_start, payload)`` for ``[start, end]``, at most 300 days per request.

        The next page starts after the last row of this one (never before the page's own
        span ends, since a full page covers it), and must move strictly forward.
        """
        path = CANDLES_PATH_TEMPLATE.format(product_id=product)
        previous_start: datetime | None = None
        while start < end:
            if previous_start is not None and start <= previous_start:
                raise ProviderError(
                    "pagination_loop", "candles cursor did not advance", retryable=False
                )
            previous_start = start
            page_end = min(end, start + MAX_BARS_PER_REQUEST * _ONE_DAY)
            response = self._get(
                path,
                {
                    "granularity": str(GRANULARITY_SECONDS),
                    "start": _iso(start),
                    "end": _iso(page_end),
                },
            )
            payload = self._load_json(response)
            yield start, payload
            start = page_end
            last_open = _last_open(payload)
            if last_open is not None and last_open + _ONE_DAY > start:
                start = last_open + _ONE_DAY  # server returned rows past the page span

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        if request.market is not Market.CRYPTO or request.interval is not Interval.D1:
            raise _bad_request("Coinbase adapter serves crypto 1d bars only")
        product = self._product(request)
        window = request.window
        first = _midnight(window.start)
        end = _midnight(window.end) + _ONE_DAY  # next midnight; the row for it is dropped
        start = first
        if request.cursor is not None:
            cursor = request.cursor
            if not cursor.isdigit() or int(cursor) % GRANULARITY_SECONDS != 0:
                raise _bad_request("malformed fetch cursor")
            start = _EPOCH + timedelta(seconds=int(cursor))
            if not first <= start < end:
                raise _bad_request("malformed fetch cursor")
        series = BarSeriesCatalog.resolve(COINBASE_EXCHANGE_RAW, currency=None)
        fetch_id = self._fetch_id()

        bars: list[RawBar] = []
        resume: datetime | None = None
        try:
            for page_start, payload in self._pages(product, start, end):
                resume = page_start
                page = parse_candles(
                    payload,
                    symbol=product,
                    window_start=window.start,
                    window_end=window.end,
                    fetch_id=fetch_id,
                    series=series,
                )
                if bars and page and page[0].ts_start <= bars[-1].ts_start:
                    raise _invalid("candle pages overlap or are not ascending")
                bars.extend(page)
                resume = None
        except (ProviderError, QuotaExhausted) as exc:
            if not bars:
                raise
            error = (
                ProviderError("quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at)
                if isinstance(exc, QuotaExhausted)
                else exc
            )
            cursor_at = resume if resume is not None else bars[-1].ts_start + _ONE_DAY
            return RawBatch(
                provider=PROVIDER_ID,
                fetch_id=fetch_id,
                request=request,
                bars=tuple(bars),
                next_cursor=str(_to_seconds(cursor_at)),
                error=error,
            )
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))


def _last_open(payload: Any) -> datetime | None:
    """Latest bucket start in a page, or ``None`` when it cannot be read (parse rejects it)."""
    if not isinstance(payload, list):
        return None
    latest: int | None = None
    for row in payload:
        if not isinstance(row, list) or not row:
            return None
        try:
            value = integer_value(row[0], "time")
        except ProviderError:
            return None
        latest = value if latest is None else max(latest, value)
    return None if latest is None else _EPOCH + timedelta(seconds=latest)
