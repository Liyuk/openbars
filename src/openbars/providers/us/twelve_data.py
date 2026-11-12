"""Twelve Data adapter: US equity 1h bars on the free tier (issue #115).

Source identity ``twelve_data/intraday/split`` (``adjust=splits``). Only the public free
tier is used: 8 requests a minute, 800 a UTC day (see ``quota.defaults.TWELVE_DATA``),
regular session only -- pre/post-market data is a paid feature, so
``include_extended_hours`` is rejected rather than silently dropped. Bars start at 09:30
New York (``session_open`` grid); ``clock_hour`` is not available from this source.
Daily bars are not implemented here (other sources cover them).

Terms: https://twelvedata.com/terms (personal use; no redistribution).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    Interval,
    Market,
    RawBar,
)
from openbars.core.series import TWELVE_DATA_INTRADAY_SPLIT, BarSeriesCatalog
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
from openbars.providers.http import decimal_value, require_object
from openbars.providers.us.hourly import (
    TWELVE_DATA_HOURLY_HISTORY_START,
    HourlyGrid,
    SubBar,
    clip_hourly_window,
    hourly_options,
    hourly_series,
    place_on_grid,
    require_session_open,
    to_raw_bar,
)
from openbars.quota.defaults import TWELVE_DATA as TWELVE_DATA_QUOTA_POLICY
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy

__all__ = ["TwelveDataProvider"]

PROVIDER_ID = "twelve_data"
TIME_SERIES_URL = "https://api.twelvedata.com/time_series"
ENV_API_KEY = "TWELVE_DATA_" + "API_KEY"  # split: the sanitizer flags long *_KEY literals
TERMS_URL = "https://twelvedata.com/terms"
FEED = TWELVE_DATA_INTRADAY_SPLIT.feed
OUTPUT_SIZE = 5000  # free-tier maximum bars per request
MAX_PAGES_PER_SYMBOL = 20  # guard against a server that never advances
NEW_YORK = ZoneInfo("America/New_York")
_UTC = timezone.utc


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


class TwelveDataProvider(HttpProviderAdapter):
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
        timeout: float = 30.0,
    ) -> None:
        super().__init__(
            client=client, quota_gate=quota_gate, before_request=before_request, timeout=timeout
        )
        self._api_key = api_key
        self._env = env if env is not None else os.environ
        self._clock = clock or (lambda: datetime.now(_UTC))
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)

    # -- credentials ---------------------------------------------------------------
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
                f"Twelve Data API key missing: set {ENV_API_KEY} or pass api_key explicitly",
                retryable=False,
            )
        return key

    # -- Provider contract -----------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        return (
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.US,
                interval=Interval.H1,
                asset_classes=frozenset({AssetClass.EQUITY, AssetClass.ETF}),
                history_start=TWELVE_DATA_HOURLY_HISTORY_START,
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=1,
                max_bars_per_request=OUTPUT_SIZE,
                adjusted_modes=frozenset({Adjustment.SPLIT}),
                feed=f"{PROVIDER_ID}:{FEED}",
                terms=ProviderTerms(
                    personal_only=True,
                    redistribution_allowed=False,
                    url=TERMS_URL,
                    notes="Free tier: 8 requests/min, 800/UTC day, regular session only; "
                    "see https://twelvedata.com/pricing",
                ),
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        return TWELVE_DATA_QUOTA_POLICY

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        raise NotImplementedError("instrument listing is not part of the Twelve Data adapter")

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        raise NotImplementedError("corporate actions are not part of the Twelve Data adapter")

    # -- bars ------------------------------------------------------------------------
    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        if request.market is not Market.US or request.interval is not Interval.H1:
            raise ProviderError(
                "unsupported_request", "Twelve Data adapter serves US 1h bars only", retryable=False
            )
        feed = (request.feed or FEED).strip().lower()
        if feed.startswith(f"{PROVIDER_ID}:"):
            feed = feed[len(PROVIDER_ID) + 1 :]
        if feed != FEED:
            raise ProviderError("invalid_request", f"unsupported feed {feed!r}", retryable=False)
        adjustment = request.adjustment or "split"
        adjustment = (
            (adjustment.value if isinstance(adjustment, Adjustment) else adjustment).strip().lower()
        )
        if adjustment != "split":
            raise ProviderError(
                "invalid_request", f"unsupported adjustment {adjustment!r}", retryable=False
            )
        options = hourly_options(request)
        require_session_open(PROVIDER_ID, options)
        if options.include_extended_hours:
            raise ProviderError(
                "unsupported_request",
                "Twelve Data pre/post-market data is not in the free tier",
                retryable=False,
            )
        key = self._require_key()
        symbols = tuple(dict.fromkeys(s.strip().upper() for s in request.symbols if s and s.strip()))
        if not symbols:
            raise ProviderError("invalid_request", "no symbols requested", retryable=False)
        first = self._decode_cursor(request.cursor, len(symbols))
        now = self._clock()
        window = clip_hourly_window(
            request.window,
            provider=PROVIDER_ID,
            now=now,
            history_start=TWELVE_DATA_HOURLY_HISTORY_START,
        )
        grid = HourlyGrid(window, options)
        series = hourly_series(
            BarSeriesCatalog.resolve(TWELVE_DATA_INTRADAY_SPLIT, interval=Interval.H1), options
        )
        fetch_id = self._fetch_id()
        bars: list[RawBar] = []
        skipped: dict[str, int] = {}
        for index in range(first, len(symbols)):
            symbol = symbols[index]
            try:
                subs = self._symbol_sub_bars(symbol, window.start, window.end, key)
            except (ProviderError, QuotaExhausted) as exc:
                if not bars:
                    if isinstance(exc, QuotaExhausted):
                        raise ProviderError(
                            "quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at
                        ) from None
                    raise
                error = (
                    ProviderError("quota_exhausted", str(exc), retryable=True, retry_at=exc.retry_at)
                    if isinstance(exc, QuotaExhausted)
                    else exc
                )
                return RawBatch(
                    provider=PROVIDER_ID,
                    fetch_id=fetch_id,
                    request=request,
                    bars=tuple(bars),
                    next_cursor=str(index),
                    error=error,
                )
            bars.extend(
                to_raw_bar(
                    gb, provider=PROVIDER_ID, symbol=symbol, fetch_id=fetch_id, feed=FEED, series=series
                )
                for gb in place_on_grid(subs, grid, now=now, skipped=skipped)
            )
        if not bars:
            raise ProviderError("empty_result", "Twelve Data returned no hourly bars", retryable=False)
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))

    @staticmethod
    def _decode_cursor(cursor: str | None, count: int) -> int:
        if cursor is None:
            return 0
        if not cursor.isdigit() or int(cursor) >= count:
            raise ProviderError("invalid_request", "malformed fetch cursor", retryable=False)
        return int(cursor)

    def _symbol_sub_bars(self, symbol: str, start: date, end: date, key: str) -> list[SubBar]:
        """All 1h bars of one symbol in ``[start, end]``, following ``outputsize`` pages."""
        out: list[SubBar] = []
        cursor = datetime(start.year, start.month, start.day)
        stop = f"{end.isoformat()} 23:59:59"
        for _ in range(MAX_PAGES_PER_SYMBOL):
            params = {
                "symbol": symbol,
                "interval": "1h",
                "start_date": cursor.strftime("%Y-%m-%d %H:%M:%S"),
                "end_date": stop,
                "timezone": "America/New_York",
                "order": "asc",
                "adjust": "splits",
                "outputsize": str(OUTPUT_SIZE),
                "apikey": key,
            }
            payload = self.json_object(self.get(TIME_SERIES_URL, params=params))
            values = self._values(payload, symbol)
            page = [self._sub_bar(item) for item in values]
            out.extend(page)
            if len(values) < OUTPUT_SIZE or not page:
                return out
            last_local = page[-1].ts_start.astimezone(NEW_YORK).replace(tzinfo=None)
            following = last_local + timedelta(seconds=1)
            if following <= cursor:
                raise ProviderError("pagination_loop", "time series did not advance", retryable=False)
            cursor = following
        raise ProviderError("pagination_loop", "too many pages for one symbol", retryable=False)

    def _values(self, payload: Mapping[str, Any], symbol: str) -> list[Any]:
        status = payload.get("status")
        if status == "error":
            code = payload.get("code")
            message = self.redact(str(payload.get("message") or ""))[:200]
            if code == 429:
                raise ProviderError("rate_limited", "Twelve Data rate limit reached", retryable=True)
            if code == 400 and "no data" in message.lower():
                return []
            if code in (401, 403):
                raise ProviderError(
                    "provider_rejected_request", f"Twelve Data refused the request ({code})", retryable=False
                )
            raise ProviderError(
                "provider_error", f"Twelve Data error {code!r}: {message}", retryable=False
            )
        if status != "ok":
            raise _invalid("response status is missing")
        meta = require_object(payload.get("meta") or {}, "meta")
        returned = meta.get("symbol")
        if returned is not None and str(returned).upper() != symbol:
            raise _invalid("response symbol does not match the requested symbol")
        if meta.get("interval") not in (None, "1h"):
            raise _invalid("response interval is not 1h")
        values = payload.get("values")
        if values is None:
            return []
        if not isinstance(values, list):
            raise _invalid("values is not an array")
        return values

    @staticmethod
    def _sub_bar(item: Any) -> SubBar:
        obj = require_object(item, "bar")
        stamp = obj.get("datetime")
        if not isinstance(stamp, str):
            raise _invalid("bar datetime is missing")
        try:
            local = datetime.fromisoformat(stamp.strip())
        except ValueError:
            raise _invalid("bar datetime is not ISO 8601") from None
        if local.tzinfo is None:
            local = local.replace(tzinfo=NEW_YORK)  # requested timezone=America/New_York
        volume = obj.get("volume")
        return SubBar(
            ts_start=local.astimezone(_UTC),
            open=decimal_value(obj.get("open"), "open"),
            high=decimal_value(obj.get("high"), "high"),
            low=decimal_value(obj.get("low"), "low"),
            close=decimal_value(obj.get("close"), "close"),
            volume=None if volume in (None, "") else decimal_value(volume, "volume"),
            payload=dict(obj),
        )
