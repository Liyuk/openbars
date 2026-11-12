"""LongPort OpenAPI Hong Kong daily bars (source identity ``longport/openapi/raw``), issue #74.

Needs the user's **own LongPort account and developer verification**; a pip-installable SDK is
not a licence to collect. The App Key, App Secret and access token come from the user's
environment at runtime (``providers.longport.app_key`` / ``app_secret`` / ``access_token``, normally
``${LONGPORT_APP_KEY}``-style references) and are never logged or stored by openbars.

The API licence is typically personal, revocable, non-commercial and non-transferable, and the
market-data addendum forbids copying, redistribution or commercial use without written consent.
Free core-API / HK LV1 tiers versus paid LV2 follow LongPort's current terms
(https://open.longportapp.com/). openbars keeps quotes in the user's local data directory only.
The SDK (``longport``) is not an openbars dependency; install it yourself. Only unadjusted
candlesticks (``AdjustType.NoAdjust``) are requested (#75).

The planner does not route HK jobs here (yfinance is the HK primary). Design:
https://github.com/Liyuk/openbars/discussions/68
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any

from openbars.core.errors import ProviderError
from openbars.core.models import DateRange, Interval
from openbars.core.series import AdjustmentScope, BarSeriesIdentity, PriceBasis, SessionSemantics, VolumeBasis
from openbars.providers.hk.broker import HkBrokerDailyProvider
from openbars.quota.policy import QuotaPolicy, RateLimit

__all__ = ["LONGPORT_OPENAPI_RAW", "LongPortProvider", "to_longport_hk_symbol"]

PROVIDER_ID = "longport"
FEED = "openapi"
TERMS_URL = "https://open.longportapp.com/"
NOT_INSTALLED_CODE = "longport_not_installed"
# LongPort documents at most 10 quote calls per second; stay at half of that.
REQUESTS_PER_SECOND = 5
HK_TZ = ZoneInfo("Asia/Hong_Kong")
CHUNK_DAYS = 1000  # calendar days per call (< 1000 HK sessions, under the per-call cap)

LONGPORT_OPENAPI_RAW = BarSeriesIdentity(
    provider=PROVIDER_ID,
    feed=FEED,
    legacy_adjustment="raw",
    interval=Interval.D1,
    price_basis=PriceBasis.RAW,
    adjustment_scope=AdjustmentScope.NONE,
    session_semantics=SessionSemantics.REGULAR,
    currency="HKD",
    volume_basis=VolumeBasis.AS_TRADED,
)

_RATE_MARKERS = ("rate limit", "too many", "frequency", "频率", "频繁")
_AUTH_MARKERS = ("unauthorized", "token", "signature", "app key", "app_key", "认证", "鉴权")
_RIGHT_MARKERS = ("permission", "no quote", "quote access", "not subscribed", "权限")
_CONN_MARKERS = ("timeout", "timed out", "connect", "network", "unavailable", "连接")

# (symbol, start, end) -> candlesticks (objects with attributes, or mappings).
History = Callable[[str, date, date], Sequence[Any]]


def to_longport_hk_symbol(code: str) -> str:
    """5-digit XHKG code -> LongPort's ``700.HK`` (no leading zeros)."""
    return f"{int(code)}.HK"


def _classify(exc: BaseException) -> ProviderError:
    text = f"{type(exc).__name__} {exc}".lower()
    if isinstance(exc, (TimeoutError, ConnectionError)) or any(m in text for m in _CONN_MARKERS):
        return ProviderError("provider_unavailable", "LongPort not reachable", retryable=True)
    if any(m in text for m in _RATE_MARKERS):
        return ProviderError("rate_limited", "LongPort request rate limit", retryable=True)
    if any(m in text for m in _RIGHT_MARKERS):
        return ProviderError("no_entitlement", "LongPort account lacks HK quote rights", retryable=False)
    if any(m in text for m in _AUTH_MARKERS):
        return ProviderError("auth_failed", "LongPort rejected the credentials", retryable=False)
    return ProviderError("provider_error", f"LongPort request failed ({type(exc).__name__})", retryable=False)


def _candle_row(candle: Any) -> dict[str, Any]:
    get = candle.get if isinstance(candle, Mapping) else (lambda k: getattr(candle, k, None))
    return {
        "date": _session_day(get("timestamp")),
        "open": get("open"),
        "high": get("high"),
        "low": get("low"),
        "close": get("close"),
        "volume": get("volume"),
        "turnover": get("turnover"),
    }


def _session_day(stamp: Any) -> Any:
    """Daily candle stamp -> HK session date. Aware stamps (UTC from the SDK) are converted to
    ``Asia/Hong_Kong`` first, so a midnight-HKT bar stamped 16:00 UTC the day before maps to the
    right session; naive stamps are taken as HK local time. Dates pass through."""
    if isinstance(stamp, datetime):
        if stamp.tzinfo is not None:
            return stamp.astimezone(HK_TZ).date()
        return stamp.date()
    return stamp


def _chunks(window: DateRange) -> list[tuple[date, date]]:
    out = []
    start = window.start
    while start <= window.end:
        end = min(window.end, start + timedelta(days=CHUNK_DAYS - 1))
        out.append((start, end))
        start = end + timedelta(days=1)
    return out


class LongPortProvider(HkBrokerDailyProvider):
    id = PROVIDER_ID
    feed = FEED
    series = LONGPORT_OPENAPI_RAW
    terms_url = TERMS_URL
    terms_notes = (
        "Needs the user's own LongPort account and developer verification. The API licence is "
        "personal, revocable, non-commercial and non-transferable; the market-data addendum forbids "
        "copying, redistribution or commercial use without consent. Local private storage only."
    )
    not_installed_code = NOT_INSTALLED_CODE

    def __init__(
        self,
        history: History | None = None,
        *,
        app_key: str | None = None,
        app_secret: str | None = None,
        access_token: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._history = history
        self._credentials = (app_key, app_secret, access_token)
        self._ctx: Any = None

    def __repr__(self) -> str:  # never show credentials
        return f"{type(self).__name__}()"

    def quota_policy(self) -> QuotaPolicy:
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(RateLimit(REQUESTS_PER_SECOND, timedelta(seconds=1), label="per_second"),),
            max_batch_symbols=1,
            official_docs_url=TERMS_URL,
        )

    def _sdk_history(self) -> History:
        if any(not (v or "").strip() for v in self._credentials):
            raise ProviderError(
                "missing_credentials",
                "LongPort needs app_key, app_secret and access_token from your environment",
                retryable=False,
            )
        try:
            from longport.openapi import AdjustType, Config, Period, QuoteContext  # type: ignore[import-not-found]
        except ImportError:
            raise ProviderError(
                NOT_INSTALLED_CODE,
                "longport is not installed; install the LongPort SDK yourself (read its terms first) "
                "or pass a history callable",
                retryable=False,
            ) from None
        if self._ctx is None:
            key, secret, token = (v.strip() for v in self._credentials)  # type: ignore[union-attr]
            self._ctx = QuoteContext(Config(app_key=key, app_secret=secret, access_token=token))
        ctx = self._ctx

        def history(symbol: str, start: date, end: date) -> Sequence[Any]:
            return ctx.history_candlesticks_by_date(symbol, Period.Day, AdjustType.NoAdjust, start, end)

        return history

    def _symbol_rows(self, code: str, window: DateRange, acquire: Callable[[], None]) -> Iterable[Mapping[str, Any]]:
        history = self._history or self._sdk_history()
        rows: list[dict[str, Any]] = []
        for start, end in _chunks(window):
            acquire()
            try:
                candles = history(to_longport_hk_symbol(code), start, end)
            except ProviderError:
                raise
            except Exception as exc:  # noqa: BLE001 - classified; message never echoed
                raise _classify(exc) from None
            rows.extend(_candle_row(c) for c in candles or ())
        return rows
