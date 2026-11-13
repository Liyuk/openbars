"""Tushare free-tier A-share daily adapter (source identity ``tushare/ashare/raw``).

Design: https://github.com/Liyuk/openbars/discussions/54
Tushare docs: https://tushare.pro/document/2?doc_id=27 (``daily`` endpoint)
Tushare terms: https://tushare.pro/document/1?doc_id=405

* **Free tier only**: about 120 points, 50 requests/minute and 8,000 requests/day. It exposes
  **unadjusted daily bars only**; ``qfq`` / ``hfq`` (``pro_bar``) and ``adj_factor`` normally
  need a higher tier, so this adapter never calls them. The stored series is ``RAW``
  (不复权); adjusted views, when available, are derived elsewhere from raw bars plus adjust
  factors.
* **Token required**: only from the injected configuration value (which the config loads from
  ``${TUSHARE_TOKEN}`` / the environment) or from the ``TUSHARE_TOKEN`` environment variable
  when used directly. It is never written to disk, logged or included in an error message;
  ``secret_values()`` exposes it only so the CLI can redact it.
* **Terms**: Tushare grants a personal, non-transferable, non-commercial, revocable licence.
  If the data is used publicly it must be attributed as 「数据来源：Tushare数据」. openbars only
  fetches into the user's local data directory and does not redistribute quotes.
* **Not installed by default**: the default client lazily imports the optional ``tushare``
  package (``pip install openbars[tushare]``). Importing this module never needs it.
* **Tushare semantics**: ``daily`` returns ``vol`` in lots (手) and ``amount`` in thousand CNY;
  the adapter stores volume in **shares** (×100, comparable with BaoStock / AKShare) and keeps
  both the source ``volume_lots`` and ``amount`` plus the converted ``amount_cny`` on
  ``RawBar.extra``. Tushare omits suspended sessions instead of returning placeholder bars, so
  the adapter cannot infer ``tradestatus`` / ``isST``.
* Symbols are normalized with ``normalize.cn_instruments`` (``600000.SH`` / ``sh.600000`` /
  ``SH600000``); bare 6-digit codes are rejected. The API call uses Tushare's ``600000.SH``
  spelling while the stored ``symbol_as_returned`` is the A-share-native ``sh.600000`` (the
  same spelling BaoStock and the planner coverage use).
* Quota: 50/minute and 8,000/day, both taken through ``QuotaGate``. One symbol per request; no
  retry loop here, failures are returned as ``ProviderError`` and retries are the caller's
  decision.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from openbars.core.errors import ProviderError
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    DateRange,
    Interval,
    Market,
    RawBar,
)
from openbars.core.series import TUSHARE_ASHARE_RAW, BarSeriesCatalog, BarSeriesIdentity
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
    "DefaultTushareClient",
    "TushareClient",
    "TushareProvider",
    "parse_daily",
    "to_native_cn_symbol",
    "to_tushare_code",
]

PROVIDER_ID = "tushare"
FEED = TUSHARE_ASHARE_RAW.feed
LEGACY_ADJUSTMENT = TUSHARE_ASHARE_RAW.adjustment
CURRENCY = "CNY"
CN_TZ = ZoneInfo("Asia/Shanghai")

TOKEN_ENV = "TUSHARE_TOKEN"
NOT_INSTALLED_CODE = "tushare_not_installed"
TOKEN_MISSING_CODE = "tushare_token_missing"
DOCS_URL = "https://tushare.pro/document/2?doc_id=27"
TERMS_URL = "https://tushare.pro/document/1?doc_id=405"

MAX_SYMBOLS_PER_REQUEST = 1
REQUESTS_PER_MINUTE = 50
REQUESTS_PER_DAY = 8000
MAX_BARS_PER_REQUEST = 6000  # the daily endpoint returns at most 6000 rows per call

# Tushare's ``daily`` column names. ``vol`` is lots and ``amount`` is thousand CNY.
_REQUIRED = ("ts_code", "trade_date", "open", "high", "low", "close", "vol")
_VENUE_SUFFIX = {"xshg": "SH", "xshe": "SZ"}
_VENUE_PREFIX = {"xshg": "sh", "xshe": "sz"}
_LOT = Decimal(100)  # Tushare ``vol`` is in 手 (100 shares)
_THOUSAND = Decimal(1000)  # Tushare ``amount`` is in 千元


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


def _bad_request(message: str) -> ProviderError:
    return ProviderError("invalid_request", message, retryable=False)


def to_tushare_code(symbol: str) -> str:
    """``sh.600000`` / ``600000.SH`` / ``SZ000001`` -> Tushare ``600000.SH`` / ``000001.SZ``.

    Raises ``ValueError`` for bare 6-digit codes and anything unrecognised.
    """
    venue, code = normalize_cn_symbol(symbol)
    return f"{code}.{_VENUE_SUFFIX[venue]}"


def to_native_cn_symbol(symbol: str) -> str:
    """Normalize to the A-share-native stored spelling ``sh.600000`` / ``sz.000001``."""
    venue, code = normalize_cn_symbol(symbol)
    return f"{_VENUE_PREFIX[venue]}.{code}"


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


# -- client ----------------------------------------------------------------------------------
class TushareClient(Protocol):
    """What the adapter needs from Tushare: one ``daily`` call returning rows.

    Rows are mappings (or a pandas-like frame that converts to records). Implementations raise
    ``ProviderError`` (or any exception, which the adapter maps to ``provider_unavailable``).
    """

    def daily(self, *, ts_code: str, start_date: str, end_date: str) -> Any: ...


def _tushare_client_error(exc: Exception) -> ProviderError:
    """Classify a Tushare SDK failure without copying its message (it may echo user input)."""
    text = f"{type(exc).__name__} {exc}".lower()
    if any(marker in text for marker in ("timeout", "timed out", "connection", "network")):
        return ProviderError(
            "provider_unavailable", f"tushare daily failed ({type(exc).__name__})", retryable=True
        )
    if any(marker in text for marker in ("每分钟", "每天", "频率", "rate limit", "too many", "429")):
        return ProviderError(
            "rate_limited", f"tushare daily rate limit ({type(exc).__name__})", retryable=True
        )
    if any(
        marker in text
        for marker in (
            "权限",
            "积分",
            "permission",
            "not allowed",
            "401",
            "403",
            "invalid token",
            "token无效",
            "token 无效",
            "token错误",
            "token 错误",
            "token不对",
            "token 不对",
        )
    ):
        return ProviderError(
            "provider_rejected_request",
            f"tushare daily rejected the token or permission ({type(exc).__name__})",
            retryable=False,
        )
    return ProviderError("provider_unavailable", f"tushare daily failed ({type(exc).__name__})", retryable=True)


class DefaultTushareClient:
    """Real client: lazy ``import tushare`` and a single ``pro_api(token)`` per call.

    The token stays in this object only; it is never put into a log line or error message.
    """

    def __init__(self, token: str) -> None:
        self._token = token
        self._pro: Any = None

    @staticmethod
    def _import() -> Any:
        try:
            import tushare  # noqa: PLC0415 - optional extra, imported only when fetching
        except ImportError:
            raise ProviderError(
                NOT_INSTALLED_CODE,
                "tushare is not installed; install the extra (openbars[tushare]) "
                "or inject a client",
                retryable=False,
            ) from None
        return tushare

    def _api(self) -> Any:
        if self._pro is None:
            module = self._import()
            try:
                self._pro = module.pro_api(self._token)
            except ProviderError:
                raise
            except Exception as exc:  # noqa: BLE001 - no raw message: it can echo the token
                raise _tushare_client_error(exc) from None
        return self._pro

    def daily(self, *, ts_code: str, start_date: str, end_date: str) -> Any:
        api = self._api()
        try:
            return api.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except ProviderError:
            raise
        except Exception as exc:  # noqa: BLE001 - classified above; message is not copied
            raise _tushare_client_error(exc) from None


# -- parsing ---------------------------------------------------------------------------------
def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("", "nan", "none", "nat")
    try:
        return bool(value != value)  # noqa: PLR0124 - NaN / NaT
    except Exception:  # noqa: BLE001 - exotic scalar types
        return False


def _rows(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if hasattr(data, "empty") and bool(data.empty):
        return []
    if hasattr(data, "reset_index") and hasattr(data, "to_dict"):
        data = data.reset_index().to_dict("records")
    elif isinstance(data, Mapping):
        try:
            columns = {str(k): list(v) for k, v in data.items()}
        except TypeError:
            raise _invalid("daily result is not tabular") from None
        length = max((len(v) for v in columns.values()), default=0)
        data = [
            {k: v[i] for k, v in columns.items() if i < len(v)} for i in range(length)
        ]
    if not isinstance(data, Sequence) or isinstance(data, (str, bytes)):
        raise _invalid("daily result is not tabular")
    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, Mapping):
            raise _invalid("daily row is not a mapping")
        rows.append({str(k).strip(): v for k, v in item.items()})
    return rows


def _parse_trade_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip().replace("/", "-")
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            raise _invalid("trade_date is not a date") from None
    if isinstance(value, int) and not isinstance(value, bool):
        text = str(value)
        if len(text) == 8:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    raise _invalid("trade_date is not a date")


def parse_daily(
    rows: Any,
    *,
    code: str,
    window_start: date,
    window_end: date,
    fetch_id: str,
    series: BarSeriesIdentity,
    sessions: set[date] | None = None,
) -> list[RawBar]:
    """Parse ``daily`` rows into ascending ``RawBar`` rows.

    Prices/volume become ``Decimal`` through their string form (never via arithmetic on
    floats). ``vol`` lots are converted to shares. Rows for another ``ts_code``, duplicate
    dates or malformed cells reject the whole payload (``invalid_payload``); rows outside the
    window (or outside ``sessions`` when given) are dropped. Tushare gives no suspension or ST
    flag, so none is invented.
    """
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise _invalid("daily payload is not a list of rows")
    out: dict[date, RawBar] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise _invalid("daily row is not a mapping")
        for name in _REQUIRED:
            if name not in row:
                raise _invalid(f"daily row has no {name!r}")
        if str(row["ts_code"]).strip().upper() != code.upper():
            raise _invalid("daily row belongs to another code")
        day = _parse_trade_date(row["trade_date"])
        if day in out:
            raise _invalid("duplicate trade_date")
        if not window_start <= day <= window_end:
            continue
        if sessions is not None and day not in sessions:
            continue
        volume_lots = decimal_value(row["vol"], "vol")
        if volume_lots < 0:
            raise _invalid("vol must be >= 0")
        extra: dict[str, object] = {
            "code": code,
            "ts_code": code,
            "volume_lots": _decimal_text(volume_lots),
        }
        if not _is_missing(row.get("amount")):
            amount = decimal_value(row["amount"], "amount")
            extra["amount"] = _decimal_text(amount)
            extra["amount_unit"] = "thousand_cny"
            extra["amount_cny"] = _decimal_text(amount * _THOUSAND)
        for name in ("pre_close", "change", "pct_chg"):
            if name in row and not _is_missing(row.get(name)):
                extra[name] = _decimal_text(decimal_value(row[name], name))
        payload = {
            k: ("" if _is_missing(v) else str(v))
            for k, v in sorted(row.items())
        }
        try:
            out[day] = RawBar(
                provider=PROVIDER_ID,
                symbol_as_returned=to_native_cn_symbol(code),
                fetch_id=fetch_id,
                interval=Interval.D1,
                ts_start=datetime.combine(day, time.min, tzinfo=CN_TZ),
                open=decimal_value(row["open"], "open"),
                high=decimal_value(row["high"], "high"),
                low=decimal_value(row["low"], "low"),
                close=decimal_value(row["close"], "close"),
                volume=volume_lots * _LOT,
                feed=FEED,
                extra=extra,
                payload_hash=digest_payload(payload),
                session_date=day,
                series=series,
            )
        except (ValueError, TypeError):
            raise _invalid("daily row failed OHLCV validation") from None
    return [out[day] for day in sorted(out)]


# -- provider --------------------------------------------------------------------------------
class TushareProvider:
    id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        token: str | None = None,
        *,
        client: TushareClient | None = None,
        quota_gate: QuotaGate | None = None,
        before_request: Callable[[], None] | None = None,
        clock: Callable[[], datetime] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
    ) -> None:
        resolved = token if token is not None else os.environ.get(TOKEN_ENV)
        self._token = (resolved or "").strip()
        self._client = client
        self._quota_gate: QuotaGate = quota_gate if quota_gate is not None else NullQuotaGate()
        self._before_request = before_request
        self._clock = clock or (lambda: datetime.now(CN_TZ))
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)

    # -- Provider contract -------------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        notes = (
            "待验证：收盘后当日可得性未由适配器保证；尽力获取、不承诺 T+0。 "
            "Free tier (~120 points): unadjusted daily bars only, about 50 requests/minute and "
            "8,000/day; qfq/hfq and adj_factor are not requested. Token is required and read "
            "from the environment (TUSHARE_TOKEN). Personal, non-transferable, non-commercial, "
            "revocable licence: if the data is used publicly it must be attributed as "
            "「数据来源：Tushare数据」. openbars stores data locally and never redistributes "
            "quotes."
        )
        return (
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.CN,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY}),
                history_start=None,  # since listing
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=MAX_SYMBOLS_PER_REQUEST,
                max_bars_per_request=MAX_BARS_PER_REQUEST,
                adjusted_modes=frozenset({Adjustment.RAW}),
                feed=FEED,
                terms=ProviderTerms(
                    personal_only=True,
                    redistribution_allowed=False,
                    url=TERMS_URL,
                    notes=notes,
                ),
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(
                RateLimit(REQUESTS_PER_MINUTE, timedelta(minutes=1), label="per_minute"),
                RateLimit(REQUESTS_PER_DAY, timedelta(days=1), label="per_24h"),
            ),
            key_scope="per_key",
            safety_margin=0.9,
            max_batch_symbols=MAX_SYMBOLS_PER_REQUEST,
            max_bars_per_request=MAX_BARS_PER_REQUEST,
            history_start=None,
            official_docs_url=DOCS_URL,
        )

    def secret_values(self) -> tuple[str, ...]:
        return (self._token,) if self._token else ()

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        raise NotImplementedError("instrument listing is not part of the Tushare daily adapter")

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        return iter(())  # the free tier exposes no adj_factor; do not invent events

    # -- request checks ----------------------------------------------------------------
    @staticmethod
    def _code(request: FetchRequest) -> tuple[str, str]:
        if request.market is not Market.CN or request.interval is not Interval.D1:
            raise _bad_request("Tushare adapter serves CN 1d bars only")
        if len(request.symbols) != 1:
            raise _bad_request("Tushare is fetched one symbol per request")
        if request.cursor is not None:
            raise _bad_request("Tushare adapter does not use fetch cursors")
        try:
            return to_tushare_code(request.symbols[0]), to_native_cn_symbol(request.symbols[0])
        except ValueError as exc:
            raise _bad_request(f"invalid Tushare symbol: {exc}") from None

    @staticmethod
    def _check_raw_only(request: FetchRequest) -> None:
        feed = (request.feed or FEED).strip().lower()
        if feed not in (FEED, "daily"):
            raise _bad_request(f"unsupported feed {feed!r}")
        adjustment = request.adjustment or LEGACY_ADJUSTMENT
        adjustment = (
            adjustment.value if isinstance(adjustment, Adjustment) else str(adjustment)
        ).strip().lower()
        if adjustment != LEGACY_ADJUSTMENT:
            raise _bad_request(f"unsupported adjustment {adjustment!r}: only unadjusted daily bars are fetched")

    def _client_for_fetch(self) -> TushareClient:
        if self._client is not None:
            return self._client
        if not self._token:
            raise ProviderError(
                TOKEN_MISSING_CODE,
                f"a Tushare token is required; set {TOKEN_ENV} (it is never read from the repository)",
                retryable=False,
            )
        return DefaultTushareClient(self._token)

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

    # -- bars --------------------------------------------------------------------------
    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        """Unadjusted daily bars for one symbol. One quota token and one ``daily`` call."""
        code, _ = self._code(request)
        self._check_raw_only(request)
        client = self._client_for_fetch()
        window = request.window
        sessions = self._sessions(window)
        series = BarSeriesCatalog.resolve(TUSHARE_ASHARE_RAW, currency=CURRENCY)
        fetch_id = self._fetch_id()
        self._quota_gate.acquire(PROVIDER_ID)
        if self._before_request is not None:
            self._before_request()
        try:
            data = client.daily(
                ts_code=code,
                start_date=window.start.strftime("%Y%m%d"),
                end_date=window.end.strftime("%Y%m%d"),
            )
        except ProviderError:
            raise
        except Exception as exc:  # noqa: BLE001 - never leak raw transport text
            raise _tushare_client_error(exc) from None
        rows = _rows(data)
        bars = parse_daily(
            rows,
            code=code,
            window_start=window.start,
            window_end=window.end,
            fetch_id=fetch_id,
            series=series,
            sessions=sessions,
        )
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))
