"""Binance public spot-klines adapter: UTC-day OHLCV bars, no API key.

Official docs (market data endpoints): https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
Request weights / limits: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits

The default base URL is ``https://data-api.binance.vision``, the public market-data-only
mirror of the spot REST API. The main host (``https://api.binance.com``) answers HTTP 451
for some regions (including US IPs), so it is not the default; pass ``base_url`` to
override. Future source (not implemented here): bulk archives at
``https://data.binance.vision`` (``ARCHIVE_URL``).

Klines are cut at UTC 00:00 (``interval=1d``), which is exactly the crypto calendar's
session definition, so ``session_date`` is the UTC date of ``openTime``.
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
from openbars.core.errors import ProviderError, QuotaExhausted, parse_retry_after
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    Interval,
    Market,
    RawBar,
)
from openbars.core.series import BINANCE_SPOT_RAW, BarSeriesCatalog, BarSeriesIdentity
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

__all__ = ["BinanceProvider", "parse_exchange_info", "parse_klines"]

PROVIDER_ID = "binance"
FEED = "spot"
EXCHANGE = "BINANCE"
LEGACY_ADJUSTMENT = "raw"

DEFAULT_BASE_URL = "https://data-api.binance.vision"
# Future bulk source (daily/monthly zip archives); intentionally not implemented.
ARCHIVE_URL = "https://data.binance.vision"
KLINES_PATH = "/api/v3/klines"
EXCHANGE_INFO_PATH = "/api/v3/exchangeInfo"

DOCS_URL = "https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits"
TERMS_URL = "https://www.binance.com/en/terms"

# Documented spot limit: REQUEST_WEIGHT 6000 per rolling minute, per IP.
REQUEST_WEIGHT_LIMIT = 6000
# Documented weights: klines = 2 (flat, independent of ``limit``), exchangeInfo = 20.
KLINES_WEIGHT = 2
EXCHANGE_INFO_WEIGHT = 20
MAX_BARS_PER_REQUEST = 1000

_ONE_DAY_MS = 86_400_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MS = timedelta(milliseconds=1)
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,20}$")
_MIN_KLINE_FIELDS = 9  # up to and including trade count


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


def _bad_request(message: str) -> ProviderError:
    return ProviderError("invalid_request", message, retryable=False)


def _to_ms(moment: datetime) -> int:
    return (moment - _EPOCH) // _MS


def _window_ms(start: date, end: date) -> tuple[int, int]:
    """Inclusive UTC window: ``start 00:00:00.000`` .. ``end 23:59:59.999``."""
    first = datetime.combine(start, time.min, tzinfo=timezone.utc)
    last = datetime.combine(end, time(23, 59, 59, 999000), tzinfo=timezone.utc)
    return _to_ms(first), _to_ms(last)


def parse_klines(
    payload: Any,
    *,
    symbol: str,
    window_start: date,
    window_end: date,
    fetch_id: str,
    series: BarSeriesIdentity,
) -> list[RawBar]:
    """Parse a ``/api/v3/klines`` array into ``RawBar`` rows (numbers never touch float).

    A row whose ``openTime`` is not exactly UTC midnight rejects the whole payload
    (``invalid_payload``): a mis-cut daily bar must never be stored as a UTC-day bar.
    Rows outside ``[window_start, window_end]`` are dropped; rows must be strictly ascending.
    """
    if not isinstance(payload, list):
        raise _invalid("klines payload is not an array")
    out: list[RawBar] = []
    previous_open: int | None = None
    for row in payload:
        if not isinstance(row, list) or len(row) < _MIN_KLINE_FIELDS:
            raise _invalid("kline row is not an array of at least 9 fields")
        open_ms = integer_value(row[0], "openTime")
        if open_ms % _ONE_DAY_MS != 0:
            raise _invalid("kline openTime is not UTC midnight")
        if previous_open is not None and open_ms <= previous_open:
            raise _invalid("kline rows are not strictly ascending")
        previous_open = open_ms
        ts_start = _EPOCH + timedelta(milliseconds=open_ms)
        session_date = ts_start.date()
        if not window_start <= session_date <= window_end:
            continue
        extra: dict[str, object] = {
            "quote_volume": str(decimal_value(row[7], "quoteVolume")),
            "trade_count": integer_value(row[8], "trades"),
        }
        try:
            out.append(
                RawBar(
                    provider=PROVIDER_ID,
                    symbol_as_returned=symbol,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=ts_start,
                    open=decimal_value(row[1], "open"),
                    high=decimal_value(row[2], "high"),
                    low=decimal_value(row[3], "low"),
                    close=decimal_value(row[4], "close"),
                    volume=decimal_value(row[5], "volume"),
                    feed=FEED,
                    extra=extra,
                    payload_hash=digest_payload(row),
                    session_date=session_date,
                    series=series,
                )
            )
        except (ValueError, TypeError):
            raise _invalid("kline failed OHLCV validation") from None
    return out


def _is_spot(item: Mapping[str, Any]) -> bool:
    if item.get("isSpotTradingAllowed") is True:
        return True
    permissions = item.get("permissions")
    if isinstance(permissions, list) and "SPOT" in permissions:
        return True
    sets = item.get("permissionSets")
    return isinstance(sets, list) and any(isinstance(s, list) and "SPOT" in s for s in sets)


def parse_exchange_info(payload: Any) -> list[ProviderInstrument]:
    """Parse ``/api/v3/exchangeInfo`` into spot ``ProviderInstrument`` rows.

    Base/quote assets are not carried (``ProviderInstrument`` has no such fields yet) and
    no instrument id is derived here.
    """
    if not isinstance(payload, dict):
        raise _invalid("exchangeInfo payload is not an object")
    symbols = payload.get("symbols")
    if not isinstance(symbols, list):
        raise _invalid("exchangeInfo has no 'symbols' array")
    out: list[ProviderInstrument] = []
    for item in symbols:
        if not isinstance(item, dict):
            raise _invalid("exchangeInfo symbol entry is not an object")
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            raise _invalid("exchangeInfo symbol entry has no symbol")
        if not _is_spot(item):
            continue
        out.append(
            ProviderInstrument(
                symbol=symbol.strip(),
                market=Market.CRYPTO,
                asset_class=AssetClass.CRYPTO_SPOT,
                exchange=EXCHANGE,
                active=item.get("status") == "TRADING",
                native_id=symbol.strip(),
            )
        )
    return out


class BinanceProvider(HttpProviderAdapter):
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

    def http_error(self, response: httpx.Response) -> ProviderError:
        error = super().http_error(response)
        status = response.status_code
        if status == 451:
            return ProviderError(
                "provider_rejected_request",
                "Binance endpoint is unavailable in this region (HTTP 451); "
                f"the default base URL is {DEFAULT_BASE_URL}, "
                "api.binance.com is blocked for some regions",
                retryable=False,
                status_code=status,
                request_id=error.request_id,
                raw_payload=error.raw_payload,
            )
        if status == 418:  # IP auto-banned after repeated 429s; Retry-After says how long
            return ProviderError(
                "rate_limited",
                "Binance temporarily banned this IP (HTTP 418)",
                retryable=True,
                status_code=status,
                request_id=error.request_id,
                raw_payload=error.raw_payload,
                retry_at=parse_retry_after(response.headers.get("retry-after")),
            )
        return error

    def _get(self, path: str, params: Mapping[str, Any] | None, weight: int) -> httpx.Response:
        return self.get(
            f"{self._base_url}{path}",
            params=params,
            headers=self._headers(),
            weight=weight,
        )

    # -- Provider contract -------------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        terms = ProviderTerms(
            personal_only=False,
            redistribution_allowed=False,
            url=TERMS_URL,
            notes="Public market data, no API key. Redistribution is subject to Binance "
            "terms (not reviewed here, so not assumed allowed); docs: "
            "https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints",
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
        # REQUEST_WEIGHT 6000 / rolling minute per IP; each call is charged its weight.
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(RateLimit(REQUEST_WEIGHT_LIMIT, timedelta(minutes=1)),),
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
            raise _bad_request("Binance adapter serves the crypto market only")
        response = self._get(EXCHANGE_INFO_PATH, None, EXCHANGE_INFO_WEIGHT)
        return parse_exchange_info(self._load_json(response))

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        return iter(())  # spot klines have no equity-style corporate actions

    # -- bars --------------------------------------------------------------------------
    @staticmethod
    def _load_json(response: httpx.Response) -> Any:
        try:
            # Floats become Decimal at parse time so numbers never round-trip through float.
            return json.loads(response.content, parse_float=Decimal)
        except ValueError:
            raise _invalid("response body is not valid JSON") from None

    @staticmethod
    def _symbol(request: FetchRequest) -> str:
        if len(request.symbols) != 1:
            raise _bad_request("Binance klines are fetched one symbol per request")
        symbol = request.symbols[0].strip().upper() if isinstance(request.symbols[0], str) else ""
        if not _SYMBOL_RE.match(symbol):
            raise _bad_request("invalid Binance symbol")
        return symbol

    def _pages(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[tuple[int, Any]]:
        """Yield ``(page_start_ms, payload)``; advance past the last row until a short page."""
        previous_start: int | None = None
        while start_ms <= end_ms:
            if previous_start is not None and start_ms <= previous_start:
                raise ProviderError("pagination_loop", "klines cursor did not advance", retryable=False)
            previous_start = start_ms
            response = self._get(
                KLINES_PATH,
                {
                    "symbol": symbol,
                    "interval": "1d",
                    "startTime": str(start_ms),
                    "endTime": str(end_ms),
                    "limit": str(MAX_BARS_PER_REQUEST),
                },
                KLINES_WEIGHT,
            )
            payload = self._load_json(response)
            yield previous_start, payload
            if not isinstance(payload, list) or len(payload) < MAX_BARS_PER_REQUEST:
                return
            last = payload[-1]
            if not isinstance(last, list) or not last:
                return  # parse step rejects the payload
            start_ms = integer_value(last[0], "openTime") + 1

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        if request.market is not Market.CRYPTO or request.interval is not Interval.D1:
            raise _bad_request("Binance adapter serves crypto 1d bars only")
        symbol = self._symbol(request)
        start_ms, end_ms = _window_ms(request.window.start, request.window.end)
        if request.cursor is not None:
            if not request.cursor.isdigit() or not start_ms <= int(request.cursor) <= end_ms:
                raise _bad_request("malformed fetch cursor")
            start_ms = int(request.cursor)
        series = BarSeriesCatalog.resolve(BINANCE_SPOT_RAW, currency=None)
        fetch_id = self._fetch_id()

        bars: list[RawBar] = []
        resume: int | None = None
        try:
            for page_start, payload in self._pages(symbol, start_ms, end_ms):
                resume = page_start
                bars.extend(
                    parse_klines(
                        payload,
                        symbol=symbol,
                        window_start=request.window.start,
                        window_end=request.window.end,
                        fetch_id=fetch_id,
                        series=series,
                    )
                )
                resume = None
        except (ProviderError, QuotaExhausted) as exc:
            if not bars:
                raise
            error = (
                ProviderError("quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at)
                if isinstance(exc, QuotaExhausted)
                else exc
            )
            cursor = resume if resume is not None else _to_ms_after(bars[-1])
            return RawBatch(
                provider=PROVIDER_ID,
                fetch_id=fetch_id,
                request=request,
                bars=tuple(bars),
                next_cursor=str(cursor),
                error=error,
            )
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))


def _to_ms_after(bar: RawBar) -> int:
    """Resume point right after ``bar`` (used when a failure hits between pages)."""
    return _to_ms(bar.ts_start) + 1
