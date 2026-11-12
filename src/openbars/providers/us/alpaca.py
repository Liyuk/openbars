"""Alpaca market-data adapter: daily bars (SIP / IEX feeds) and corporate actions.

Official docs: https://docs.alpaca.markets/docs/about-market-data-api
Public Basic-plan rate limit (200 requests / minute): https://docs.alpaca.markets/docs/about-market-data-api#subscription-plans
Credentials come from constructor arguments or the ``ALPACA_API_KEY_ID`` /
``ALPACA_API_SECRET_KEY`` environment variables and are only needed when fetching.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import httpx

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    CorporateActionKind,
    Interval,
    Market,
    RawBar,
)
from openbars.core.series import BarSeriesCatalog, BarSeriesIdentity, SourceIdentity
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
from openbars.providers.http import (
    TokenLoopGuard,
    decimal_value,
    digest_payload,
    integer_value,
    require_object,
)
from openbars.providers.us.alpaca_time import (
    SIP_RECENT_DATA_DELAY,
    historical_end_parameter,
    normalize_feed,
)
from openbars.providers.us.hourly import (
    ALPACA_HOURLY_HISTORY_START,
    HourlyGrid,
    aggregate_to_grid,
    clip_hourly_window,
    hourly_options,
    hourly_series,
    sub_bar_from_json,
    to_raw_bar,
)
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy, RateLimit

__all__ = ["AlpacaProvider", "CorporateActionsResult", "historical_end_parameter", "parse_assets"]

PROVIDER_ID = "alpaca"
BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
CORPORATE_ACTIONS_URL = "https://data.alpaca.markets/v1/corporate-actions"
# Trading API assets list: https://docs.alpaca.markets/reference/get-v2-assets
ASSETS_URL = "https://api.alpaca.markets/v2/assets"
_EXCHANGE_MIC = {
    "NASDAQ": "XNAS", "NYSE": "XNYS", "ARCA": "ARCX", "AMEX": "XASE", "BATS": "BATS",
}
ENV_KEY_ID = "ALPACA_API_KEY_ID"
ENV_SECRET_KEY = "ALPACA_API_SECRET_KEY"
TERMS_URL = "https://alpaca.markets/disclosures"

MAX_SYMBOLS_PER_REQUEST = 200
MAX_BARS_PER_REQUEST = 10000
CORPORATE_ACTIONS_LIMIT = 1000
HISTORY_START = date(2016, 1, 1)
# 1h is built from 30-minute bars so session_open (09:30) slots can be cut exactly.
HOURLY_TIMEFRAME = "30Min"
_SUB_BAR = timedelta(minutes=30)
DEFAULT_FEED = "sip"
SUPPORTED_FEEDS = ("sip", "iex")
SUPPORTED_ADJUSTMENTS = ("split", "raw")

_SPLIT_COLLECTIONS = ("forward_splits", "reverse_splits", "unit_splits")
SUPPORTED_COLLECTIONS = (
    "forward_splits",
    "reverse_splits",
    "unit_splits",
    "stock_dividends",
    "name_changes",
    "cash_dividends",
    "spin_offs",
    "cash_mergers",
    "stock_mergers",
)
# Collections with no CorporateActionKind equivalent yet; counted as skipped.
_UNMAPPED_COLLECTIONS = ("cash_mergers", "stock_mergers")


@dataclass(frozen=True)
class CorporateActionsResult:
    actions: tuple[CorporateAction, ...]
    skipped: Mapping[str, int] = field(default_factory=dict)  # "<collection>[:reason]" -> count
    next_cursor: str | None = None
    error: ProviderError | None = None


class _Progress:
    """Cursor of the request in flight, so a failure can report where to resume."""

    def __init__(self) -> None:
        self.cursor: str | None = None


def _encode_cursor(chunk: int, token: str | None) -> str:
    return f"{chunk}:{token or ''}"


def _decode_cursor(cursor: str | None, chunk_count: int) -> tuple[int, str | None]:
    if cursor is None:
        return 0, None
    head, sep, token = cursor.partition(":")
    if not sep or not head.isdigit() or int(head) >= chunk_count:
        raise ProviderError("invalid_request", "malformed fetch cursor", retryable=False)
    return int(head), token or None


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


def _parse_date(value: Any, field_name: str) -> date:
    if not isinstance(value, str):
        raise _invalid(f"{field_name} is not a date string")
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        raise _invalid(f"{field_name} is not a valid date") from None


def parse_assets(payload: Any) -> list[ProviderInstrument]:
    """Parse an Alpaca ``/v2/assets`` array into ``ProviderInstrument`` rows."""
    if not isinstance(payload, list):
        raise _invalid("assets payload is not an array")
    out: list[ProviderInstrument] = []
    for item in payload:
        if not isinstance(item, dict):
            raise _invalid("asset entry is not an object")
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            raise _invalid("asset entry has no symbol")
        native = item.get("id")
        exchange = item.get("exchange")
        out.append(
            ProviderInstrument(
                symbol=symbol.strip().upper(),
                market=Market.US,
                asset_class=None,
                name=item.get("name") if isinstance(item.get("name"), str) else None,
                exchange=_EXCHANGE_MIC.get(exchange) if isinstance(exchange, str) else None,
                active=item.get("status") == "active",
                native_id=native if isinstance(native, str) and native else None,
            )
        )
    return out


class AlpacaProvider(HttpProviderAdapter):
    id = PROVIDER_ID
    provider_id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        *,
        quota_gate: QuotaGate | None = None,
        client: httpx.Client | None = None,
        before_request: Callable[[], None] | None = None,
        env: Mapping[str, str] | None = None,
        clock: Callable[[], datetime] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(
            client=client, quota_gate=quota_gate, before_request=before_request, timeout=timeout
        )
        self._api_key = api_key
        self._api_secret = api_secret
        self._env = env if env is not None else os.environ
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)

    # -- credentials ---------------------------------------------------------------
    def _credentials(self) -> tuple[str, str] | None:
        key = (self._api_key or self._env.get(ENV_KEY_ID) or "").strip()
        secret = (self._api_secret or self._env.get(ENV_SECRET_KEY) or "").strip()
        return (key, secret) if key and secret else None

    def secret_values(self) -> tuple[str, ...]:
        key = (self._api_key or self._env.get(ENV_KEY_ID) or "").strip()
        secret = (self._api_secret or self._env.get(ENV_SECRET_KEY) or "").strip()
        return tuple(v for v in (key, secret) if v)

    def _headers(self) -> dict[str, str]:
        creds = self._credentials()
        if creds is None:
            raise ProviderError(
                "not_configured",
                f"Alpaca credentials missing: set {ENV_KEY_ID} and {ENV_SECRET_KEY} or pass them explicitly",
                retryable=False,
            )
        return {"APCA-API-KEY-ID": creds[0], "APCA-API-SECRET-KEY": creds[1]}

    # -- Provider contract -----------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        terms = ProviderTerms(
            personal_only=True,
            redistribution_allowed=False,
            url=TERMS_URL,
            notes="Market data is licensed to the account holder; see "
            "https://docs.alpaca.markets/docs/about-market-data-api",
        )
        return tuple(
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.US,
                interval=interval,
                asset_classes=frozenset({AssetClass.EQUITY, AssetClass.ETF}),
                history_start=HISTORY_START,
                batch=BatchShape.MULTI_SYMBOL,
                max_symbols_per_request=MAX_SYMBOLS_PER_REQUEST,
                max_bars_per_request=MAX_BARS_PER_REQUEST,
                adjusted_modes=frozenset({Adjustment.SPLIT, Adjustment.RAW}),
                feed=f"alpaca:{feed}",
                terms=terms,
            )
            for interval in (Interval.D1, Interval.H1)
            for feed in SUPPORTED_FEEDS
        )

    def quota_policy(self) -> QuotaPolicy:
        # Public Basic documented limit: 200 requests per minute.
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(RateLimit(200, timedelta(minutes=1)),),
            key_scope="per_key",
        )

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        """Active US equity assets (``asset_class`` is ``None``: the assets API does not
        distinguish ETFs from common stock, so rows are symbol + native-id evidence only)."""
        if market is not Market.US:
            return []
        response = self.get(
            ASSETS_URL,
            params={"status": "active", "asset_class": "us_equity"},
            headers=self._headers(),
        )
        try:
            payload = response.json()
        except ValueError:
            raise _invalid("assets response is not JSON") from None
        return parse_assets(payload)

    # -- shared request helpers --------------------------------------------------------
    @staticmethod
    def _chunks(symbols: Sequence[str]) -> list[tuple[str, ...]]:
        unique = tuple(dict.fromkeys(s.strip() for s in symbols if s and s.strip()))
        if not unique:
            raise ProviderError("invalid_request", "no symbols requested", retryable=False)
        return [
            unique[i : i + MAX_SYMBOLS_PER_REQUEST]
            for i in range(0, len(unique), MAX_SYMBOLS_PER_REQUEST)
        ]

    def _pages(
        self,
        url: str,
        chunks: list[tuple[str, ...]],
        build_params: Callable[[tuple[str, ...], str | None], dict[str, str]],
        cursor: str | None,
        progress: _Progress,
    ) -> Iterator[tuple[int, Mapping[str, Any]]]:
        """Yield ``(chunk_index, payload)`` for every page, following ``next_page_token``."""
        headers = self._headers()
        first_chunk, token = _decode_cursor(cursor, len(chunks))
        for index in range(first_chunk, len(chunks)):
            guard = TokenLoopGuard(token)
            while True:
                progress.cursor = _encode_cursor(index, token)
                response = self.get(url, params=build_params(chunks[index], token), headers=headers)
                payload = self.json_object(response)
                yield index, payload
                following = payload.get("next_page_token")
                if following is None:
                    break
                token = guard.accept(following)
            token = None
        progress.cursor = None

    @staticmethod
    def _as_provider_error(exc: QuotaExhausted) -> ProviderError:
        return ProviderError(
            "quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at
        )

    # -- bars ----------------------------------------------------------------------------
    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        if request.market is Market.US and request.interval is Interval.H1:
            return self._fetch_hourly(request)
        if request.market is not Market.US or request.interval is not Interval.D1:
            raise ProviderError(
                "unsupported_request", "Alpaca adapter serves US 1d bars only", retryable=False
            )
        feed = normalize_feed(request.feed or DEFAULT_FEED)
        if feed not in SUPPORTED_FEEDS:
            raise ProviderError("invalid_request", f"unsupported feed {feed!r}", retryable=False)
        adjustment = self._adjustment(request.adjustment)
        chunks = self._chunks(request.symbols)
        end_param = historical_end_parameter(request.window.end, feed=feed, now=self._clock())
        start_param = request.window.start.isoformat()
        series = BarSeriesCatalog.resolve(SourceIdentity(PROVIDER_ID, feed, adjustment))
        fetch_id = self._fetch_id()

        def build_params(symbols: tuple[str, ...], token: str | None) -> dict[str, str]:
            params = {
                "symbols": ",".join(symbols),
                "timeframe": "1Day",
                "start": start_param,
                "end": end_param,
                "feed": feed,
                "adjustment": adjustment,
                "limit": str(MAX_BARS_PER_REQUEST),
            }
            if token:
                params["page_token"] = token
            return params

        bars: list[RawBar] = []
        progress = _Progress()
        try:
            for index, payload in self._pages(BARS_URL, chunks, build_params, request.cursor, progress):
                bars.extend(
                    self._parse_bars(payload, chunks[index], request, feed, fetch_id, series)
                )
        except (ProviderError, QuotaExhausted) as exc:
            if not bars:
                raise
            error = self._as_provider_error(exc) if isinstance(exc, QuotaExhausted) else exc
            return RawBatch(
                provider=PROVIDER_ID,
                fetch_id=fetch_id,
                request=request,
                bars=tuple(bars),
                next_cursor=progress.cursor,
                error=error,
            )
        if not bars:
            raise ProviderError("empty_result", "Alpaca returned no bars", retryable=False)
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))

    def _fetch_hourly(self, request: FetchRequest) -> RawBatch:
        """30-minute bars summed onto the XNYS hourly grid (regular session by default)."""
        feed = normalize_feed(request.feed or DEFAULT_FEED)
        if feed not in SUPPORTED_FEEDS:
            raise ProviderError("invalid_request", f"unsupported feed {feed!r}", retryable=False)
        adjustment = self._adjustment(request.adjustment)
        chunks = self._chunks(request.symbols)
        options = hourly_options(request)
        now = self._clock()
        window = clip_hourly_window(
            request.window, provider=PROVIDER_ID, now=now, history_start=ALPACA_HOURLY_HISTORY_START
        )
        grid = HourlyGrid(window, options)
        if not grid:
            raise ProviderError("empty_result", "no XNYS sessions in the window", retryable=False)
        # SIP: the most recent ~15 minutes are not served on the free plan.
        latest = now - SIP_RECENT_DATA_DELAY if feed == "sip" else now
        end = min(grid.end, latest)
        if end <= grid.start:
            raise ProviderError("data_not_ready", "no completed hourly slot yet", retryable=True)
        stamp = "%Y-%m-%dT%H:%M:%SZ"
        start_param = grid.start.astimezone(timezone.utc).strftime(stamp)
        end_param = end.astimezone(timezone.utc).replace(microsecond=0).strftime(stamp)
        series = hourly_series(
            BarSeriesCatalog.resolve(SourceIdentity(PROVIDER_ID, feed, adjustment), interval=Interval.H1),
            options,
        )
        fetch_id = self._fetch_id()
        first, token = _decode_cursor(request.cursor, len(chunks))
        if token is not None:  # hourly resumes at a chunk boundary only
            raise ProviderError("invalid_request", "malformed fetch cursor", retryable=False)
        headers = self._headers()
        bars: list[RawBar] = []
        skipped: dict[str, int] = {}
        for index in range(first, len(chunks)):
            chunk = chunks[index]
            allowed = set(chunk)
            subs: dict[str, list[Any]] = {}
            page: str | None = None
            guard = TokenLoopGuard()
            try:
                while True:
                    params = {
                        "symbols": ",".join(chunk),
                        "timeframe": HOURLY_TIMEFRAME,
                        "start": start_param,
                        "end": end_param,
                        "feed": feed,
                        "adjustment": adjustment,
                        "limit": str(MAX_BARS_PER_REQUEST),
                    }
                    if page:
                        params["page_token"] = page
                    payload = self.json_object(self.get(BARS_URL, params=params, headers=headers))
                    if "bars" not in payload:
                        raise _invalid("response has no 'bars' member")
                    container = payload["bars"]
                    by_symbol = {} if container is None else require_object(container, "bars")
                    for symbol, items in by_symbol.items():
                        if symbol not in allowed:
                            raise _invalid("response contains a symbol that was not requested")
                        if not isinstance(items, list):
                            raise _invalid("bars for a symbol are not an array")
                        for item in items:
                            subs.setdefault(symbol, []).append(self._sub_bar(item))
                    following = payload.get("next_page_token")
                    if following is None:
                        break
                    page = guard.accept(following)
            except (ProviderError, QuotaExhausted) as exc:
                # A half-read chunk is dropped whole: never emit a partial hour.
                if not bars:
                    raise
                error = self._as_provider_error(exc) if isinstance(exc, QuotaExhausted) else exc
                return RawBatch(
                    provider=PROVIDER_ID,
                    fetch_id=fetch_id,
                    request=request,
                    bars=tuple(bars),
                    next_cursor=_encode_cursor(index, None),
                    error=error,
                )
            for symbol in chunk:
                for grid_bar in aggregate_to_grid(
                    subs.get(symbol, ()), grid, sub_length=_SUB_BAR, now=end, skipped=skipped
                ):
                    bars.append(
                        to_raw_bar(
                            grid_bar,
                            provider=PROVIDER_ID,
                            symbol=symbol,
                            fetch_id=fetch_id,
                            feed=feed,
                            series=series,
                        )
                    )
        if not bars:
            raise ProviderError("empty_result", "Alpaca returned no hourly bars", retryable=False)
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))

    @staticmethod
    def _sub_bar(item: Any) -> Any:
        obj = require_object(item, "bar")
        stamp = obj.get("t")
        if not isinstance(stamp, str):
            raise _invalid("bar timestamp is missing or not a string")
        try:
            ts = datetime.fromisoformat(stamp)
        except ValueError:
            raise _invalid("bar timestamp is not ISO 8601") from None
        if ts.tzinfo is None or ts.utcoffset() is None:
            raise _invalid("bar timestamp has no UTC offset")
        return sub_bar_from_json(obj, ts.astimezone(timezone.utc))

    @staticmethod
    def _adjustment(value: str | Adjustment | None) -> str:
        if value is None:
            return "split"
        text = value.value if isinstance(value, Adjustment) else str(value).strip().lower()
        if text not in SUPPORTED_ADJUSTMENTS:
            raise ProviderError(
                "invalid_request", f"unsupported adjustment {text!r}", retryable=False
            )
        return text

    def _parse_bars(
        self,
        payload: Mapping[str, Any],
        requested: tuple[str, ...],
        request: FetchRequest,
        feed: str,
        fetch_id: str,
        series: BarSeriesIdentity,
    ) -> list[RawBar]:
        if "bars" not in payload:
            raise _invalid("response has no 'bars' member")
        container = payload["bars"]
        by_symbol = {} if container is None else require_object(container, "bars")
        allowed = set(requested)
        out: list[RawBar] = []
        for symbol, items in by_symbol.items():
            if symbol not in allowed:
                raise _invalid("response contains a symbol that was not requested")
            if not isinstance(items, list):
                raise _invalid("bars for a symbol are not an array")
            for item in items:
                out.append(self._parse_bar(symbol, item, request, feed, fetch_id, series))
        return out

    @staticmethod
    def _parse_bar(
        symbol: str, item: Any, request: FetchRequest, feed: str, fetch_id: str, series: BarSeriesIdentity
    ) -> RawBar:
        obj = require_object(item, "bar")
        stamp = obj.get("t")
        if not isinstance(stamp, str):
            raise _invalid("bar timestamp is missing or not a string")
        try:
            ts = datetime.fromisoformat(stamp)
        except ValueError:
            raise _invalid("bar timestamp is not ISO 8601") from None
        if ts.tzinfo is None or ts.utcoffset() is None:
            raise _invalid("bar timestamp has no UTC offset")
        session_date = ts.date()  # date as delivered (daily bars are stamped at session midnight)
        if not request.window.start <= session_date <= request.window.end:
            raise _invalid("bar date is outside the requested window")
        vwap: Decimal | None = None
        if obj.get("vw") is not None:
            raw_vwap = decimal_value(obj["vw"], "vw")
            if raw_vwap > 0:
                vwap = raw_vwap.quantize(Decimal(1).scaleb(-8))
        extra: dict[str, object] = {}
        if obj.get("n") is not None:
            extra["trade_count"] = integer_value(obj["n"], "n")
        try:
            return RawBar(
                provider=PROVIDER_ID,
                symbol_as_returned=symbol,
                fetch_id=fetch_id,
                interval=Interval.D1,
                ts_start=ts.astimezone(timezone.utc),
                open=decimal_value(obj.get("o"), "o"),
                high=decimal_value(obj.get("h"), "h"),
                low=decimal_value(obj.get("l"), "l"),
                close=decimal_value(obj.get("c"), "c"),
                volume=decimal_value(obj.get("v"), "v"),
                feed=feed,
                extra=extra,
                vwap=vwap,
                payload_hash=digest_payload(obj),
                session_date=session_date,
                series=series,
            )
        except (ValueError, TypeError):
            raise _invalid("bar failed OHLCV validation") from None

    # -- corporate actions ---------------------------------------------------------------
    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        """As returned (ticker in ``instrument_id``); map with ``ingest_corporate_actions``."""
        result = self._fetch_corporate_actions(request, allow_partial=False)
        return result.actions

    def fetch_corporate_actions_result(self, request: FetchRequest) -> CorporateActionsResult:
        """Like ``fetch_corporate_actions`` but keeps earlier pages when a later page fails."""
        return self._fetch_corporate_actions(request, allow_partial=True)

    def _fetch_corporate_actions(
        self, request: FetchRequest, *, allow_partial: bool
    ) -> CorporateActionsResult:
        chunks = self._chunks(request.symbols)
        start_param = request.window.start.isoformat()
        end_param = request.window.end.isoformat()

        def build_params(symbols: tuple[str, ...], token: str | None) -> dict[str, str]:
            params = {
                "symbols": ",".join(symbols),
                "start": start_param,
                "end": end_param,
                "data_quality": "all",
                "limit": str(CORPORATE_ACTIONS_LIMIT),
            }
            if token:
                params["page_token"] = token
            return params

        actions: list[CorporateAction] = []
        skipped: dict[str, int] = {}
        progress = _Progress()
        try:
            for _index, payload in self._pages(
                CORPORATE_ACTIONS_URL, chunks, build_params, request.cursor, progress
            ):
                page_skipped: dict[str, int] = {}
                page_actions = self._parse_corporate_actions(payload, page_skipped)
                actions.extend(page_actions)
                for name, count in page_skipped.items():
                    skipped[name] = skipped.get(name, 0) + count
        except (ProviderError, QuotaExhausted) as exc:
            if not allow_partial or not (actions or skipped):
                raise
            error = self._as_provider_error(exc) if isinstance(exc, QuotaExhausted) else exc
            return CorporateActionsResult(
                tuple(actions), dict(skipped), next_cursor=progress.cursor, error=error
            )
        return CorporateActionsResult(tuple(actions), dict(skipped))

    @staticmethod
    def _parse_corporate_actions(
        payload: Mapping[str, Any], skipped: dict[str, int]
    ) -> list[CorporateAction]:
        if "corporate_actions" not in payload:
            raise _invalid("response has no 'corporate_actions' member")
        container = payload["corporate_actions"]
        collections = {} if container is None else require_object(container, "corporate_actions")
        out: list[CorporateAction] = []

        def skip(label: str) -> None:
            skipped[label] = skipped.get(label, 0) + 1

        for name, items in collections.items():
            supported = name in SUPPORTED_COLLECTIONS
            if not supported or name in _UNMAPPED_COLLECTIONS:
                count = len(items) if isinstance(items, list) else (1 if items else 0)
                if count:
                    label = name if supported else f"{name}:unsupported_collection"
                    skipped[label] = skipped.get(label, 0) + count
                continue
            if items is None:
                continue
            if not isinstance(items, list):
                raise _invalid(f"corporate action collection {name} is not an array")
            for item in items:
                action = _corporate_action(name, require_object(item, name))
                if action is None:
                    skip(f"{name}:incomplete_item")
                else:
                    out.append(action)
        return out


def _ratio(new: Any, old: Any) -> Decimal | None:
    """new/old as an exact ``Decimal`` (4-for-1 -> 4, 1-for-4 -> 0.25); ``None`` if undefined."""
    if new is None or old is None:
        return None
    old_value = decimal_value(old, "old_rate")
    if old_value <= 0:
        return None
    ratio = decimal_value(new, "new_rate") / old_value
    return ratio if ratio > 0 else None


def _first(item: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if item.get(name) is not None:
            return item[name]
    return None


def _corporate_action(collection: str, item: Mapping[str, Any]) -> CorporateAction | None:
    """Map one Alpaca item onto ``CorporateAction``; ``None`` if essential fields are absent."""
    ratio: Decimal | None = None
    amount: Decimal | None = None
    if collection in _SPLIT_COLLECTIONS:
        kind = CorporateActionKind.SPLIT
        symbol = _first(item, "symbol", "old_symbol")
        ratio = _ratio(item.get("new_rate"), item.get("old_rate"))
        if ratio is None:
            return None
    elif collection == "stock_dividends":
        kind = CorporateActionKind.DIVIDEND
        symbol = _first(item, "symbol")
        rate = item.get("rate")
        ratio = decimal_value(rate, "rate") if rate is not None else None
    elif collection == "cash_dividends":
        kind = CorporateActionKind.DIVIDEND
        symbol = _first(item, "symbol")
        rate = item.get("rate")
        if rate is None:
            return None
        amount = decimal_value(rate, "rate")
    elif collection == "spin_offs":
        kind = CorporateActionKind.SPINOFF
        symbol = _first(item, "source_symbol", "symbol")
        ratio = _ratio(item.get("new_rate"), item.get("source_rate"))
    elif collection == "name_changes":
        kind = CorporateActionKind.SYMBOL_CHANGE
        symbol = _first(item, "old_symbol", "symbol")
    else:  # pragma: no cover - guarded by the caller
        return None
    when = _first(item, "ex_date", "process_date")
    if not isinstance(symbol, str) or not symbol or when is None:
        return None
    return CorporateAction(
        instrument_id=symbol,
        ex_date=_parse_date(when, "ex_date"),
        kind=kind,
        source=PROVIDER_ID,
        ratio=ratio,
        amount=amount,
    )
