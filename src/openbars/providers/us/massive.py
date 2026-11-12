"""Massive daily adapter (public brand of the former Polygon.io API): grouped daily
(one request per session for the whole US market), single-ticker range daily, plus
corporate-action and reference-list helpers.

Official docs: https://massive.com/docs
Public pricing / plans: https://massive.com/pricing
The key comes from the constructor or the ``MASSIVE_API_KEY`` environment variable, is
sent as the ``apiKey`` query parameter, and is only needed when fetching.
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

import httpx

from openbars.calendars import xnys
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
from openbars.core.series import MASSIVE_EOD_SPLIT, BarSeriesCatalog, BarSeriesIdentity
from openbars.normalize.instruments import InstrumentMaster
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
from openbars.providers.us.massive_window import (
    NEW_YORK,
    ROLLING_WINDOW_DAYS,
    massive_grouped_sessions,
    session_is_ready,
)
from openbars.quota.defaults import MASSIVE as MASSIVE_QUOTA_POLICY
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy

__all__ = [
    "CorporateActionsResult",
    "MassiveProvider",
    "ROLLING_WINDOW_DAYS",
    "massive_grouped_sessions",
    "session_is_ready",
]

PROVIDER_ID = "massive"
API_HOST = "api.massive.com"
BASE_URL = f"https://{API_HOST}"
GROUPED_URL = BASE_URL + "/v2/aggs/grouped/locale/us/market/stocks/{session}"
TICKER_RANGE_PATH = "/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
SPLITS_PATH = "/stocks/v1/splits"
TICKER_EVENTS_PATH = "/vX/reference/tickers/{symbol}/events"
TICKERS_PATH = "/v3/reference/tickers"
ENV_API_KEY = "MASSIVE_API_KEY"
TERMS_URL = "https://massive.com/pricing"

DEFAULT_FEED = "eod"
SUPPORTED_FEEDS = ("eod",)
SUPPORTED_ADJUSTMENTS = ("split",)
TICKER_EVENTS_FEED = "ticker_events"
SPLITS_FEED = "splits"

TICKER_RANGE_LIMIT = 50000
SPLITS_LIMIT = 1000
TICKERS_LIMIT = 1000
# ``SourceCapability.max_symbols_per_request`` must be an int. A grouped-daily request
# ignores the symbol list and returns every ticker, so use a large sentinel there.
WHOLE_MARKET_SYMBOLS = 100_000
# Typical delay after the regular close (16:00 ET) until 00:00 ET of the next day.
TYPICAL_LATENCY = timedelta(hours=8)
_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_STRIPPED_QUERY_KEYS = frozenset({"apikey", "api_key"})
_NOT_READY_MARKER = "today's data before end of day"

_SPLIT_TYPES = {
    "forward_split": CorporateActionKind.SPLIT,
    "reverse_split": CorporateActionKind.SPLIT,
    "stock_dividend": CorporateActionKind.DIVIDEND,
}


@dataclass(frozen=True)
class CorporateActionsResult:
    actions: tuple[CorporateAction, ...]
    skipped: Mapping[str, int] = field(default_factory=dict)  # "<kind>[:reason]" -> count
    next_cursor: str | None = None
    error: ProviderError | None = None


class _Progress:
    """Cursor of the request in flight, so a failure can report where to resume."""

    def __init__(self) -> None:
        self.cursor: str | None = None


def _encode_cursor(index: int, url: str | None) -> str:
    raw = json.dumps({"i": index, "u": url}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str | None, count: int) -> tuple[int, str | None]:
    if cursor is None:
        return 0, None
    try:
        data = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        index, url = data["i"], data["u"]
        if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < count:
            raise ValueError
        if url is not None and not isinstance(url, str):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        raise ProviderError("invalid_request", "malformed fetch cursor", retryable=False) from None
    return index, url


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


def _parse_date(value: Any, field_name: str) -> date:
    if not isinstance(value, str):
        raise _invalid(f"{field_name} is not a date string")
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        raise _invalid(f"{field_name} is not a valid date") from None


def _millis_to_utc(value: Any, field_name: str) -> datetime:
    millis = integer_value(value, field_name)
    try:
        return _EPOCH + timedelta(milliseconds=millis)
    except OverflowError:
        raise _invalid(f"{field_name} is out of range") from None


def _clean_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(s.strip() for s in symbols if s and s.strip()))


class MassiveProvider(HttpProviderAdapter):
    id = PROVIDER_ID
    provider_id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        api_key: str | None = None,
        *,
        quota_gate: QuotaGate | None = None,
        client: httpx.Client | None = None,
        before_request: Callable[[], None] | None = None,
        env: Mapping[str, str] | None = None,
        clock: Callable[[], datetime] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
        instrument_master: InstrumentMaster | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(
            client=client, quota_gate=quota_gate, before_request=before_request, timeout=timeout
        )
        self._api_key = api_key
        self._env = env if env is not None else os.environ
        self._clock = clock or (lambda: datetime.now(_UTC))
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)
        self._master = instrument_master

    # -- credentials / errors --------------------------------------------------------
    def _key(self) -> str | None:
        return (self._api_key or self._env.get(ENV_API_KEY) or "").strip() or None

    def secret_values(self) -> tuple[str, ...]:
        key = self._key()
        return (key,) if key else ()

    def _require_key(self) -> str:
        key = self._key()
        if key is None:
            raise ProviderError(
                "not_configured",
                f"Massive API key missing: set {ENV_API_KEY} or pass api_key explicitly",
                retryable=False,
            )
        return key

    def http_error(self, response: httpx.Response) -> ProviderError:
        error = super().http_error(response)
        body = (error.raw_payload or "").lower().replace("\u2019", "'")
        if response.status_code == 403 and _NOT_READY_MARKER in body:
            return ProviderError(
                "data_not_ready",
                "provider has not published this session yet (HTTP 403)",
                retryable=True,
                status_code=403,
                request_id=error.request_id,
                raw_payload=error.raw_payload,
            )
        return error

    @staticmethod
    def _as_provider_error(exc: QuotaExhausted) -> ProviderError:
        return ProviderError("quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at)

    # -- Provider contract -------------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        terms = ProviderTerms(
            personal_only=True,
            redistribution_allowed=False,
            url=TERMS_URL,
            notes="Public Basic free tier: 5 calls/min, ~2 year rolling window; grouped "
            "daily is available after 00:00 America/New_York on the calendar day after "
            "the session. Data is licensed for personal use; see https://massive.com/pricing",
        )
        return (
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.US,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY, AssetClass.ETF}),
                history_start=None,  # since listing
                rolling_window=timedelta(days=ROLLING_WINDOW_DAYS),
                latency=TYPICAL_LATENCY,
                batch=BatchShape.WHOLE_MARKET_PER_DAY,
                max_symbols_per_request=WHOLE_MARKET_SYMBOLS,
                max_bars_per_request=TICKER_RANGE_LIMIT,
                adjusted_modes=frozenset({Adjustment.SPLIT}),
                feed=f"{PROVIDER_ID}:{DEFAULT_FEED}",
                terms=terms,
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        return MASSIVE_QUOTA_POLICY

    # -- request validation --------------------------------------------------------------
    @staticmethod
    def _check_market(request: FetchRequest) -> None:
        if request.market is not Market.US or request.interval is not Interval.D1:
            raise ProviderError(
                "unsupported_request", "Massive adapter serves US 1d bars only", retryable=False
            )

    @staticmethod
    def _feed(value: str | None) -> str:
        text = (value or DEFAULT_FEED).strip().lower()
        if text.startswith(f"{PROVIDER_ID}:"):
            text = text[len(PROVIDER_ID) + 1 :]
        if text not in SUPPORTED_FEEDS:
            raise ProviderError("invalid_request", f"unsupported feed {text!r}", retryable=False)
        return text

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

    # -- URL handling ---------------------------------------------------------------------
    def _clean_next_url(self, url: str, path_ok: Callable[[str], bool]) -> str:
        """Validate a pagination URL and drop any key it carries; the caller adds its own."""

        def reject() -> ProviderError:
            return ProviderError(
                "invalid_pagination_url", "untrusted pagination URL", retryable=False
            )

        try:
            parts = urlsplit(url)
            port = parts.port
        except ValueError:
            raise reject() from None
        if (
            parts.scheme != "https"
            or parts.hostname != API_HOST
            or "@" in parts.netloc
            or port not in (None, 443)
            or parts.fragment
            or not path_ok(parts.path)
        ):
            raise reject()
        query = [
            (k, v)
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k.lower() not in _STRIPPED_QUERY_KEYS
        ]
        return urlunsplit(("https", API_HOST, parts.path, urlencode(query), ""))

    @staticmethod
    def _path_matcher(*template: str) -> Callable[[str], bool]:
        """Match ``/a/b/{symbol}/c`` style paths; ``None`` entries are wildcards."""

        def matches(path: str) -> bool:
            segments = path.split("/")
            if len(segments) != len(template):
                return False
            return all(
                want is None or unquote(got) == want for want, got in zip(template, segments)
            )

        return matches

    @staticmethod
    def _request_args(url: str, send: Mapping[str, str]) -> tuple[str, Mapping[str, str] | None]:
        """httpx replaces the URL query when ``params`` is given, so merge it by hand for
        pagination URLs (which already carry a cursor)."""
        parts = urlsplit(url)
        if not parts.query:
            return url, send
        query = parse_qsl(parts.query, keep_blank_values=True) + list(send.items())
        return urlunsplit(parts._replace(query=urlencode(query))), None

    def _pages(
        self,
        url: str,
        params: Mapping[str, str],
        path_ok: Callable[[str], bool],
        *,
        index: int,
        resume_url: str | None,
        progress: _Progress,
    ) -> Iterator[Mapping[str, Any]]:
        """Yield each page's payload, following validated ``next_url`` links."""
        key = self._require_key()
        guard = TokenLoopGuard()
        if resume_url is not None:
            url = self._clean_next_url(resume_url, path_ok)
            guard = TokenLoopGuard(url)
            send: dict[str, str] = {"apiKey": key}
        else:
            send = {**params, "apiKey": key}
        while True:
            progress.cursor = _encode_cursor(index, url if resume_url is not None else None)
            target, query = self._request_args(url, send)
            payload = self.json_object(self.get(target, params=query))
            yield payload
            following = payload.get("next_url")
            if following is None:
                return
            if isinstance(following, str) and following:
                following = self._clean_next_url(following, path_ok)
            url = guard.accept(following)
            resume_url = url
            send = {"apiKey": key}

    # -- bars ------------------------------------------------------------------------------
    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        self._check_market(request)
        feed = self._feed(request.feed)
        adjustment = self._adjustment(request.adjustment)
        if (feed, adjustment) != (MASSIVE_EOD_SPLIT.feed, MASSIVE_EOD_SPLIT.adjustment):
            raise ProviderError("invalid_request", "unsupported feed/adjustment", retryable=False)
        series = BarSeriesCatalog.resolve(MASSIVE_EOD_SPLIT)
        self._require_key()
        symbols = _clean_symbols(request.symbols)
        if symbols:
            return self._fetch_ticker_daily(request, symbols, feed, series)
        return self._fetch_grouped_daily(request, feed, series)

    def _partial_or_raise(
        self,
        exc: ProviderError | QuotaExhausted,
        bars: list[RawBar],
        request: FetchRequest,
        fetch_id: str,
        next_cursor: str | None,
    ) -> RawBatch:
        if not bars:
            raise exc
        error = self._as_provider_error(exc) if isinstance(exc, QuotaExhausted) else exc
        return RawBatch(
            provider=PROVIDER_ID,
            fetch_id=fetch_id,
            request=request,
            bars=tuple(bars),
            next_cursor=next_cursor,
            error=error,
        )

    # A. grouped daily: whole market, one request per session
    def _fetch_grouped_daily(
        self, request: FetchRequest, feed: str, series: BarSeriesIdentity
    ) -> RawBatch:
        window = request.window
        in_window = xnys.sessions(window.start, window.end)
        if not in_window:
            raise ProviderError(
                "invalid_request", "window contains no XNYS session", retryable=False
            )
        now = self._clock()
        as_of = now.astimezone(NEW_YORK).date()
        planned = massive_grouped_sessions(
            window, as_of=as_of, now=now, sessions=in_window
        )
        if request.cursor is not None:
            try:
                resume = date.fromisoformat(request.cursor)
            except ValueError:
                raise ProviderError(
                    "invalid_request", "malformed fetch cursor", retryable=False
                ) from None
            planned = [s for s in planned if s >= resume]
        if not planned:
            rolling_start = as_of - timedelta(days=ROLLING_WINDOW_DAYS)
            in_rolling = [s for s in in_window if s >= rolling_start]
            if in_rolling and not any(session_is_ready(s, now) for s in in_rolling):
                raise ProviderError(
                    "data_not_ready",
                    "no session in the window is available yet (ready after 00:00 "
                    "America/New_York on the next calendar day)",
                    retryable=True,
                )
            if not in_rolling:
                raise ProviderError(
                    "invalid_request",
                    "window is outside the rolling 2 year history window",
                    retryable=False,
                )
            # Cursor is past the last requestable session; nothing left to fetch.
            return RawBatch(
                provider=PROVIDER_ID, fetch_id=self._fetch_id(), request=request, bars=()
            )
        fetch_id = self._fetch_id()
        bars: list[RawBar] = []
        for session in planned:
            try:
                bars.extend(self._grouped_session(session, feed, fetch_id, series))
            except (ProviderError, QuotaExhausted) as exc:
                return self._partial_or_raise(exc, bars, request, fetch_id, session.isoformat())
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))

    def _grouped_session(
        self, session: date, feed: str, fetch_id: str, series: BarSeriesIdentity
    ) -> list[RawBar]:
        key = self._require_key()
        response = self.get(
            GROUPED_URL.format(session=session.isoformat()),
            params={"adjusted": "true", "include_otc": "false", "apiKey": key},
        )
        payload = self.json_object(response)
        self._check_status(payload, allowed=("OK",))
        if payload.get("adjusted") is not True:
            raise ProviderError(
                "adjustment_mismatch", "response is not split-adjusted", retryable=False
            )
        results = payload.get("results")
        if results is None or results == []:
            raise ProviderError(
                "empty_result",
                f"no bars returned for XNYS session {session.isoformat()}",
                retryable=True,
            )
        if not isinstance(results, list):
            raise _invalid("results is not an array")
        self._check_count(payload, len(results))
        out: list[RawBar] = []
        for item in results:
            obj = require_object(item, "bar")
            symbol = obj.get("T")
            if not isinstance(symbol, str) or not symbol:
                raise _invalid("bar ticker is missing or not a string")
            ts = _millis_to_utc(obj.get("t"), "t")
            if ts.date() != session:
                raise _invalid("bar date does not match the requested session")
            out.append(self._raw_bar(symbol, obj, ts, feed, fetch_id, series))
        return out

    def _check_status(self, payload: Mapping[str, Any], *, allowed: tuple[str, ...]) -> None:
        status = payload.get("status")
        if status not in allowed:
            shown = self.redact(str(status))[:40] if isinstance(status, str) else "missing"
            raise ProviderError(
                "provider_error", f"provider status {shown!r}", retryable=True
            )

    @staticmethod
    def _check_count(payload: Mapping[str, Any], actual: int) -> None:
        count = payload.get("resultsCount")
        if isinstance(count, int) and not isinstance(count, bool) and count != actual:
            raise ProviderError(
                "result_count_mismatch",
                f"resultsCount {count} does not match {actual} results",
                retryable=True,
            )

    def _raw_bar(
        self,
        symbol: str,
        obj: Mapping[str, Any],
        ts: datetime,
        feed: str,
        fetch_id: str,
        series: BarSeriesIdentity,
    ) -> RawBar:
        session_date = ts.date()
        vwap: Decimal | None = None
        if obj.get("vw") is not None:
            raw_vwap = decimal_value(obj["vw"], "vw")
            if raw_vwap > 0:
                vwap = raw_vwap.quantize(Decimal(1).scaleb(-8))
        extra: dict[str, object] = {}
        if obj.get("n") is not None:
            extra["trade_count"] = integer_value(obj["n"], "n")
        instrument_id = None
        if self._master is not None:
            instrument_id = self._master.resolve(
                symbol, on=session_date, provider=PROVIDER_ID, market=Market.US
            )
        try:
            return RawBar(
                provider=PROVIDER_ID,
                symbol_as_returned=symbol,
                fetch_id=fetch_id,
                interval=Interval.D1,
                ts_start=ts,
                open=decimal_value(obj.get("o"), "o"),
                high=decimal_value(obj.get("h"), "h"),
                low=decimal_value(obj.get("l"), "l"),
                close=decimal_value(obj.get("c"), "c"),
                volume=decimal_value(obj.get("v"), "v"),
                feed=feed,
                extra=extra,
                vwap=vwap,
                payload_hash=digest_payload({"T": symbol, **obj}),
                instrument_id=instrument_id,
                session_date=session_date,
                series=series,
            )
        except (ValueError, TypeError):
            raise _invalid("bar failed OHLCV validation") from None

    # B. ticker daily: explicit symbols, one request chain per symbol
    def _fetch_ticker_daily(
        self,
        request: FetchRequest,
        symbols: tuple[str, ...],
        feed: str,
        series: BarSeriesIdentity,
    ) -> RawBatch:
        fetch_id = self._fetch_id()
        first, resume_url = _decode_cursor(request.cursor, len(symbols))
        start, end = request.window.start, request.window.end
        bars: list[RawBar] = []
        progress = _Progress()
        try:
            for index in range(first, len(symbols)):
                symbol = symbols[index]
                url = BASE_URL + TICKER_RANGE_PATH.format(
                    symbol=quote(symbol, safe=""), start=start.isoformat(), end=end.isoformat()
                )
                params = {
                    "adjusted": "true",
                    "sort": "asc",
                    "limit": str(TICKER_RANGE_LIMIT),
                }
                path_ok = self._path_matcher(
                    "", "v2", "aggs", "ticker", symbol, "range", "1", "day", None, None
                )
                for payload in self._pages(
                    url,
                    params,
                    lambda p, ok=path_ok: ok(p) and _range_tail_ok(p),
                    index=index,
                    resume_url=resume_url,
                    progress=progress,
                ):
                    bars.extend(
                        self._parse_ticker_page(payload, symbol, request, feed, fetch_id, series)
                    )
                resume_url = None
        except (ProviderError, QuotaExhausted) as exc:
            return self._partial_or_raise(exc, bars, request, fetch_id, progress.cursor)
        if not bars:
            raise ProviderError("empty_result", "Massive returned no bars", retryable=False)
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))

    def _parse_ticker_page(
        self,
        payload: Mapping[str, Any],
        symbol: str,
        request: FetchRequest,
        feed: str,
        fetch_id: str,
        series: BarSeriesIdentity,
    ) -> list[RawBar]:
        self._check_status(payload, allowed=("OK", "DELAYED"))
        if payload.get("adjusted") is not True:
            raise ProviderError(
                "adjustment_mismatch", "response is not split-adjusted", retryable=False
            )
        returned = payload.get("ticker")
        if returned is not None and returned != symbol:
            raise _invalid("response ticker does not match the requested symbol")
        results = payload.get("results")
        if results is None:
            results = []
        if not isinstance(results, list):
            raise _invalid("results is not an array")
        self._check_count(payload, len(results))
        out: list[RawBar] = []
        for item in results:
            obj = require_object(item, "bar")
            ts = _millis_to_utc(obj.get("t"), "t")
            if not request.window.start <= ts.date() <= request.window.end:
                raise _invalid("bar date is outside the requested window")
            out.append(self._raw_bar(symbol, obj, ts, feed, fetch_id, series))
        return out

    # -- corporate actions -------------------------------------------------------------------
    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        """Splits by default; ``request.feed == "ticker_events"`` selects ticker events.

        As returned (ticker in ``instrument_id``); map with ``ingest_corporate_actions``.
        """
        return self._corporate_actions(request, allow_partial=False).actions

    def fetch_corporate_actions_result(self, request: FetchRequest) -> CorporateActionsResult:
        """Like ``fetch_corporate_actions`` but keeps earlier pages when a later page fails."""
        return self._corporate_actions(request, allow_partial=True)

    def fetch_ticker_events(self, request: FetchRequest) -> CorporateActionsResult:
        return self._ticker_events(request, allow_partial=True)

    def _corporate_actions(
        self, request: FetchRequest, *, allow_partial: bool
    ) -> CorporateActionsResult:
        dataset = (request.feed or SPLITS_FEED).strip().lower()
        if dataset == SPLITS_FEED:
            return self._splits(request, allow_partial=allow_partial)
        if dataset == TICKER_EVENTS_FEED:
            return self._ticker_events(request, allow_partial=allow_partial)
        raise ProviderError(
            "invalid_request", f"unsupported corporate action dataset {dataset!r}", retryable=False
        )

    def _collect(
        self,
        targets: Sequence[str | None],
        request: FetchRequest,
        build: Callable[[str | None], tuple[str, dict[str, str], Callable[[str], bool]]],
        parse: Callable[[Mapping[str, Any], str | None, dict[str, int]], list[CorporateAction]],
        *,
        allow_partial: bool,
    ) -> CorporateActionsResult:
        self._require_key()
        first, resume_url = _decode_cursor(request.cursor, len(targets))
        actions: list[CorporateAction] = []
        skipped: dict[str, int] = {}
        progress = _Progress()
        try:
            for index in range(first, len(targets)):
                url, params, path_ok = build(targets[index])
                for payload in self._pages(
                    url, params, path_ok, index=index, resume_url=resume_url, progress=progress
                ):
                    actions.extend(parse(payload, targets[index], skipped))
                resume_url = None
        except (ProviderError, QuotaExhausted) as exc:
            if not allow_partial or not (actions or skipped):
                raise
            error = self._as_provider_error(exc) if isinstance(exc, QuotaExhausted) else exc
            return CorporateActionsResult(
                tuple(actions), dict(skipped), next_cursor=progress.cursor, error=error
            )
        return CorporateActionsResult(tuple(actions), dict(skipped))

    def _splits(self, request: FetchRequest, *, allow_partial: bool) -> CorporateActionsResult:
        symbols = _clean_symbols(request.symbols)
        targets: list[str | None] = list(symbols) if symbols else [None]

        def build(symbol: str | None) -> tuple[str, dict[str, str], Callable[[str], bool]]:
            params = {
                "execution_date.gte": request.window.start.isoformat(),
                "execution_date.lte": request.window.end.isoformat(),
                "limit": str(SPLITS_LIMIT),
                "sort": "execution_date.asc",
            }
            if symbol is not None:
                params["ticker"] = symbol
            return BASE_URL + SPLITS_PATH, params, lambda p: p == SPLITS_PATH

        def parse(
            payload: Mapping[str, Any], _symbol: str | None, skipped: dict[str, int]
        ) -> list[CorporateAction]:
            self._check_status(payload, allowed=("OK",))
            return _parse_splits(_results_list(payload), skipped)

        return self._collect(targets, request, build, parse, allow_partial=allow_partial)

    def _ticker_events(
        self, request: FetchRequest, *, allow_partial: bool
    ) -> CorporateActionsResult:
        symbols = _clean_symbols(request.symbols)
        if not symbols:
            raise ProviderError(
                "invalid_request", "ticker events need at least one symbol", retryable=False
            )
        targets: list[str | None] = list(symbols)

        def build(symbol: str | None) -> tuple[str, dict[str, str], Callable[[str], bool]]:
            assert symbol is not None
            url = BASE_URL + TICKER_EVENTS_PATH.format(symbol=quote(symbol, safe=""))
            matcher = self._path_matcher("", "vX", "reference", "tickers", symbol, "events")
            return url, {"types": "ticker_change"}, matcher

        def parse(
            payload: Mapping[str, Any], symbol: str | None, skipped: dict[str, int]
        ) -> list[CorporateAction]:
            self._check_status(payload, allowed=("OK",))
            assert symbol is not None
            return _parse_ticker_events(payload, symbol, request, skipped)

        return self._collect(targets, request, build, parse, allow_partial=allow_partial)

    # -- reference lists ----------------------------------------------------------------------
    def list_instruments(
        self, market: Market, *, on: date | None = None
    ) -> Iterable[ProviderInstrument]:
        """Active US common stocks. Thin reference listing; the full master is separate."""
        return self._list_tickers(market, active=True, on=on)

    def list_delisted(
        self, market: Market = Market.US, *, on: date | None = None
    ) -> Iterable[ProviderInstrument]:
        return self._list_tickers(market, active=False, on=on)

    def _list_tickers(
        self, market: Market, *, active: bool, on: date | None
    ) -> list[ProviderInstrument]:
        if market is not Market.US:
            raise ProviderError(
                "unsupported_request", "Massive adapter lists US instruments only", retryable=False
            )
        params = {
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "active": "true" if active else "false",
            "order": "asc",
            "sort": "ticker",
            "limit": str(TICKERS_LIMIT),
        }
        if on is not None:
            params["date"] = on.isoformat()
        out: list[ProviderInstrument] = []
        for payload in self._pages(
            BASE_URL + TICKERS_PATH,
            params,
            lambda p: p == TICKERS_PATH,
            index=0,
            resume_url=None,
            progress=_Progress(),
        ):
            self._check_status(payload, allowed=("OK",))
            for item in _results_list(payload):
                obj = require_object(item, "ticker")
                symbol = obj.get("ticker")
                if not isinstance(symbol, str) or not symbol:
                    raise _invalid("ticker is missing or not a string")
                name, exchange = obj.get("name"), obj.get("primary_exchange")
                out.append(
                    ProviderInstrument(
                        symbol=symbol,
                        market=Market.US,
                        asset_class=AssetClass.EQUITY,
                        name=name if isinstance(name, str) else None,
                        exchange=exchange if isinstance(exchange, str) else None,
                        active=obj.get("active") is not False if active else False,
                    )
                )
        if not out and active:
            raise ProviderError("empty_result", "Massive returned no tickers", retryable=True)
        return out


def _range_tail_ok(path: str) -> bool:
    """Last two path segments of a range URL: ISO dates or millisecond timestamps."""
    start, end = path.split("/")[-2:]
    return all(_is_date_or_millis(part) for part in (start, end))


def _is_date_or_millis(text: str) -> bool:
    if text.isascii() and text.isdigit():
        return True
    try:
        date.fromisoformat(text)
    except ValueError:
        return False
    return len(text) == 10


def _results_list(payload: Mapping[str, Any]) -> list[Any]:
    results = payload.get("results")
    if results is None:
        return []
    if not isinstance(results, list):
        raise _invalid("results is not an array")
    return results


def _ratio(split_to: Any, split_from: Any) -> Decimal | None:
    """to/from as an exact ``Decimal`` (4-for-1 -> 4, 1-for-4 -> 0.25); ``None`` if undefined."""
    if split_to is None or split_from is None:
        return None
    denominator = decimal_value(split_from, "split_from")
    if denominator <= 0:
        return None
    ratio = decimal_value(split_to, "split_to") / denominator
    return ratio if ratio > 0 else None


def _parse_splits(items: list[Any], skipped: dict[str, int]) -> list[CorporateAction]:
    out: list[CorporateAction] = []

    def skip(label: str) -> None:
        skipped[label] = skipped.get(label, 0) + 1

    for item in items:
        obj = require_object(item, "split")
        kind_name = obj.get("adjustment_type")
        kind = _SPLIT_TYPES.get(kind_name) if isinstance(kind_name, str) else None
        if kind is None:
            skip(f"{kind_name if isinstance(kind_name, str) else 'unknown'}:unsupported_type")
            continue
        ticker, when = obj.get("ticker"), obj.get("execution_date")
        if not isinstance(ticker, str) or not ticker or when is None:
            skip(f"{kind_name}:incomplete_item")
            continue
        out.append(
            CorporateAction(
                instrument_id=ticker,
                ex_date=_parse_date(when, "execution_date"),
                kind=kind,
                source=PROVIDER_ID,
                ratio=_ratio(obj.get("split_to"), obj.get("split_from")),
            )
        )
    return out


def _parse_ticker_events(
    payload: Mapping[str, Any], symbol: str, request: FetchRequest, skipped: dict[str, int]
) -> list[CorporateAction]:
    container = payload.get("results")
    if container is None:
        return []
    events = require_object(container, "results").get("events")
    if events is None:
        return []
    if not isinstance(events, list):
        raise _invalid("events is not an array")
    out: list[CorporateAction] = []
    for item in events:
        obj = require_object(item, "event")
        kind_name = obj.get("type")
        if kind_name != "ticker_change":
            label = kind_name if isinstance(kind_name, str) else "unknown"
            skipped[f"{label}:unsupported_type"] = skipped.get(f"{label}:unsupported_type", 0) + 1
            continue
        when = _parse_date(obj.get("date"), "date")
        if not request.window.start <= when <= request.window.end:
            continue
        out.append(
            CorporateAction(
                instrument_id=symbol,
                ex_date=when,
                kind=CorporateActionKind.SYMBOL_CHANGE,
                source=PROVIDER_ID,
            )
        )
    return out
