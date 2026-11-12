"""BaoStock A-share daily adapter (source identity ``baostock/ashare/raw``).

Design: https://github.com/Liyuk/openbars/discussions/54
BaoStock docs: https://www.baostock.com/helpDocsHome

* **Stored series is unadjusted** (``adjustflag="3"``, 不复权). The adapter never asks the API
  for forward/backward adjusted prices (``adjustflag`` 1/2); adjusted views are derived later
  from the raw bars plus the adjust factors (``fetch_adjust_factors``).
* **Suspension fields are kept**: ``tradestatus``, ``isST``, ``preclose``, ``pctChg``,
  ``amount`` and ``turn`` land on ``RawBar.extra``. Suspended rows (``tradestatus == 0``) are
  *not* dropped: volume/amount may be 0 and OHLC usually equals the previous close.
* **Not HTTP**: BaoStock is a login + socket API. The adapter talks to a small injected
  client (``BaoStockClient``); the default client lazily imports the optional ``baostock``
  package (``pip install openbars[baostock]``), logs in anonymously, runs one call and logs
  out. Importing this module never needs the package. There is **no retry loop** here;
  failures are returned as ``ProviderError`` and any retry is the caller's decision.
* Symbols are BaoStock native (``sh.600000`` / ``sz.000001``); ``600000.SH`` style input is
  accepted via ``normalize.cn_instruments`` and bare 6-digit codes are rejected.
* Quota: BaoStock publishes **no** rate limit. The policy below is a conservative guess.
* Terms: the PyPI package is BSD licensed, but the website forbids copying, distributing or
  commercial use without written permission. openbars only stores the data locally.
"""

from __future__ import annotations

import contextlib
import io
import socket
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from openbars.core.errors import ProviderError
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    Interval,
    Market,
    RawBar,
)
from openbars.core.series import BAOSTOCK_ASHARE_RAW, BarSeriesCatalog, BarSeriesIdentity
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
    "BaoStockAdjustFactor",
    "BaoStockClient",
    "BaoStockProvider",
    "DefaultBaoStockClient",
    "parse_adjust_factors",
    "parse_klines",
    "to_baostock_code",
]

PROVIDER_ID = "baostock"
FEED = BAOSTOCK_ASHARE_RAW.feed
LEGACY_ADJUSTMENT = BAOSTOCK_ASHARE_RAW.adjustment
CURRENCY = "CNY"
CN_TZ = ZoneInfo("Asia/Shanghai")

NOT_INSTALLED_CODE = "baostock_not_installed"
DOCS_URL = "https://www.baostock.com/helpDocsHome"
TERMS_URL = "https://www.baostock.com/"
LOGIN_TIMEOUT_SECONDS = 10.0

ADJUSTFLAG_RAW = "3"  # 不复权: the only price series this adapter requests
ADJUST_METHOD = "pct_return"  # 涨跌幅复权法
ADJUST_SOURCE = PROVIDER_ID

# Conservative: BaoStock publishes no limit. Two requests per second, one after the margin.
REQUESTS_PER_SECOND = 2

KLINE_FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
)
_REQUIRED = ("date", "code", "open", "high", "low", "close")
_OPTIONAL_DECIMALS = ("preclose", "amount", "turn", "pctChg")
_FACTOR_FIELDS = ("code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor")

# A-share equity prefixes in BaoStock ``query_all_stock`` (skips indices, funds, B shares).
_EQUITY_PREFIXES = ("sh.60", "sh.68", "sz.00", "sz.30")
_VENUE_PREFIX = {"xshg": "sh", "xshe": "sz"}
_VENUE_EXCHANGE = {"sh": "XSHG", "sz": "XSHE"}

Row = Mapping[str, str]


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


def _bad_request(message: str) -> ProviderError:
    return ProviderError("invalid_request", message, retryable=False)


def to_baostock_code(symbol: str) -> str:
    """``sh.600000`` / ``600000.SH`` / ``SZ000001`` -> BaoStock ``sh.600000`` / ``sz.000001``.

    Raises ``ValueError`` for bare 6-digit codes and anything unrecognised.
    """
    venue, code = normalize_cn_symbol(symbol)
    return f"{_VENUE_PREFIX[venue]}.{code}"


# -- client ----------------------------------------------------------------------------------
class BaoStockClient(Protocol):
    """What the adapter needs from BaoStock. Rows are ``{field: str}`` mappings.

    Implementations raise ``ProviderError`` (or any exception, which the adapter maps to
    ``provider_unavailable``) on failure. ``query_all_stock`` is optional (listing only).
    """

    def login(self) -> None: ...

    def logout(self) -> None: ...

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjustflag: str = "3",
    ) -> Sequence[Row]: ...

    def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> Sequence[Row]: ...


def _error_for_code(error_code: str, error_msg: str, *, what: str) -> ProviderError:
    message = f"baostock {what} failed ({error_code}: {error_msg})"
    if error_code.startswith("10002"):  # socket / network
        return ProviderError("provider_unavailable", message, retryable=True)
    if error_code.startswith("10004"):  # bad parameters / unsupported code
        return ProviderError("provider_rejected_request", message, retryable=False)
    return ProviderError("provider_unavailable", message, retryable=False)


class DefaultBaoStockClient:
    """Real client: lazy ``import baostock``; anonymous login, short socket timeout."""

    def __init__(self, *, timeout: float = LOGIN_TIMEOUT_SECONDS) -> None:
        self._timeout = timeout
        self._bs: Any = None

    @staticmethod
    def _import() -> Any:
        try:
            import baostock  # noqa: PLC0415 - optional extra, imported only when fetching
        except ImportError:
            raise ProviderError(
                NOT_INSTALLED_CODE,
                "baostock is not installed; install the extra (openbars[baostock]) "
                "or inject a client",
                retryable=False,
            ) from None
        return baostock

    def login(self) -> None:
        bs = self._import()
        self._bs = bs
        # baostock opens its socket inside ``login``; a socket keeps the default timeout that
        # was active when it was created, so set it only around that call.
        previous = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self._timeout)
        try:
            with contextlib.redirect_stdout(io.StringIO()):  # the package prints progress text
                result = bs.login()
        except ProviderError:
            raise
        except Exception as exc:  # noqa: BLE001 - e.g. refused/timed-out connection
            raise ProviderError(
                "provider_unavailable", f"baostock login failed ({type(exc).__name__})", retryable=True
            ) from None
        finally:
            socket.setdefaulttimeout(previous)
        if getattr(result, "error_code", None) != "0":
            raise _error_for_code(
                str(getattr(result, "error_code", "")), str(getattr(result, "error_msg", "")), what="login"
            )

    def logout(self) -> None:
        if self._bs is None:
            return
        with contextlib.redirect_stdout(io.StringIO()):
            self._bs.logout()

    def _rows(self, result: Any, *, what: str) -> list[Row]:
        if getattr(result, "error_code", None) != "0":
            raise _error_for_code(
                str(getattr(result, "error_code", "")), str(getattr(result, "error_msg", "")), what=what
            )
        fields = list(result.fields)
        rows: list[Row] = []
        while result.next():
            rows.append(dict(zip(fields, result.get_row_data(), strict=False)))
        return rows

    def _call(self, name: str, *args: Any, **kwargs: Any) -> list[Row]:
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                result = getattr(self._bs, name)(*args, **kwargs)
                return self._rows(result, what=name)
        except ProviderError:
            raise
        except Exception as exc:  # noqa: BLE001 - socket errors surface as arbitrary exceptions
            raise ProviderError(
                "provider_unavailable", f"baostock {name} failed ({type(exc).__name__})", retryable=True
            ) from None

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjustflag: str = ADJUSTFLAG_RAW,
    ) -> list[Row]:
        return self._call(
            "query_history_k_data_plus",
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag,
        )

    def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> list[Row]:
        return self._call("query_adjust_factor", code, start_date=start_date, end_date=end_date)

    def query_all_stock(self, day: str) -> list[Row]:
        return self._call("query_all_stock", day=day)


# -- parsing ---------------------------------------------------------------------------------
def _parse_date(value: Any, name: str) -> date:
    if not isinstance(value, str):
        raise _invalid(f"{name} is not a date string")
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        raise _invalid(f"{name} is not an ISO date") from None


def _flag(value: Any, name: str) -> int:
    text = str(value).strip() if value is not None else ""
    if text not in ("0", "1"):
        raise _invalid(f"{name} is not 0/1")
    return int(text)


def _optional_decimal(value: Any, name: str) -> Decimal | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return decimal_value(value, name)


def parse_klines(
    rows: Any,
    *,
    code: str,
    window_start: date,
    window_end: date,
    fetch_id: str,
    series: BarSeriesIdentity,
) -> list[RawBar]:
    """Parse ``query_history_k_data_plus`` rows into ascending ``RawBar`` rows.

    Prices/volume are parsed from strings with ``Decimal``. Suspended rows are kept. A row
    for another code, a duplicate date, a row whose ``adjustflag`` is not ``3`` or a malformed
    cell rejects the whole payload (``invalid_payload``). Rows outside the window are dropped.
    """
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise _invalid("k-line payload is not a list of rows")
    out: dict[date, RawBar] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise _invalid("k-line row is not a mapping")
        for name in _REQUIRED:
            if name not in row:
                raise _invalid(f"k-line row has no {name!r}")
        if str(row["code"]).strip().lower() != code:
            raise _invalid("k-line row belongs to another code")
        flag = row.get("adjustflag")
        if flag is not None and str(flag).strip() != ADJUSTFLAG_RAW:
            raise _invalid("k-line row is adjusted (adjustflag is not 3)")
        day = _parse_date(row["date"], "date")
        if day in out:
            raise _invalid("duplicate k-line date")
        if not window_start <= day <= window_end:
            continue
        tradestatus = _flag(row.get("tradestatus"), "tradestatus") if "tradestatus" in row else None
        extra: dict[str, object] = {"code": code, "adjustflag": ADJUSTFLAG_RAW}
        if tradestatus is not None:
            extra["tradestatus"] = tradestatus
        if "isST" in row:
            extra["isST"] = _flag(row["isST"], "isST")
        for name in _OPTIONAL_DECIMALS:
            value = _optional_decimal(row.get(name), name)
            if value is not None:
                extra[name] = str(value)
        raw_volume = row.get("volume")
        if isinstance(raw_volume, str) and not raw_volume.strip() and tradestatus == 0:
            volume = Decimal(0)  # suspended: nothing traded
        else:
            volume = decimal_value(raw_volume, "volume")
        try:
            out[day] = RawBar(
                provider=PROVIDER_ID,
                symbol_as_returned=code,
                fetch_id=fetch_id,
                interval=Interval.D1,
                ts_start=datetime(day.year, day.month, day.day, tzinfo=CN_TZ),
                open=decimal_value(row["open"], "open"),
                high=decimal_value(row["high"], "high"),
                low=decimal_value(row["low"], "low"),
                close=decimal_value(row["close"], "close"),
                volume=volume,
                feed=FEED,
                extra=extra,
                payload_hash=digest_payload(dict(row)),
                session_date=day,
                series=series,
            )
        except (ValueError, TypeError):
            raise _invalid("k-line row failed OHLCV validation") from None
    return [out[day] for day in sorted(out)]


@dataclass(frozen=True)
class BaoStockAdjustFactor:
    """One ``query_adjust_factor`` row (涨跌幅复权法 factors effective from ``divid_operate_date``)."""

    code: str
    divid_operate_date: date
    fore_adjust_factor: Decimal
    back_adjust_factor: Decimal
    adjust_factor: Decimal
    source: str = ADJUST_SOURCE
    method: str = ADJUST_METHOD


def parse_adjust_factors(rows: Any, *, code: str) -> list[BaoStockAdjustFactor]:
    """Parse ``query_adjust_factor`` rows (ascending by ``dividOperateDate``). Empty -> ``[]``."""
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise _invalid("adjust-factor payload is not a list of rows")
    out: dict[date, BaoStockAdjustFactor] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise _invalid("adjust-factor row is not a mapping")
        for name in _FACTOR_FIELDS:
            if name not in row:
                raise _invalid(f"adjust-factor row has no {name!r}")
        if str(row["code"]).strip().lower() != code:
            raise _invalid("adjust-factor row belongs to another code")
        day = _parse_date(row["dividOperateDate"], "dividOperateDate")
        if day in out:
            raise _invalid("duplicate dividOperateDate")
        factors = []
        for name in ("foreAdjustFactor", "backAdjustFactor", "adjustFactor"):
            value = decimal_value(row[name], name)
            if value <= 0:
                raise _invalid(f"{name} must be positive")
            factors.append(value)
        out[day] = BaoStockAdjustFactor(code, day, *factors)
    return [out[day] for day in sorted(out)]


# -- provider --------------------------------------------------------------------------------
class BaoStockProvider:
    id = PROVIDER_ID
    api_version = API_VERSION

    def __init__(
        self,
        *,
        client: BaoStockClient | None = None,
        quota_gate: QuotaGate | None = None,
        before_request: Callable[[], None] | None = None,
        fetch_id_factory: Callable[[], str] | None = None,
        timeout: float = LOGIN_TIMEOUT_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._client: BaoStockClient = client if client is not None else DefaultBaoStockClient(timeout=timeout)
        self._quota_gate: QuotaGate = quota_gate if quota_gate is not None else NullQuotaGate()
        self._before_request = before_request
        self._fetch_id = fetch_id_factory or (lambda: uuid.uuid4().hex)
        self._clock = clock or (lambda: datetime.now(CN_TZ))

    # -- Provider contract -------------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        terms = ProviderTerms(
            personal_only=True,
            redistribution_allowed=False,
            url=TERMS_URL,
            notes="No API key. The baostock PyPI package is BSD licensed, but the website forbids "
            "copying, distribution or commercial use without written permission; openbars only "
            "stores the data locally. Prices are unadjusted (adjustflag 3).",
        )
        return (
            SourceCapability(
                provider=PROVIDER_ID,
                market=Market.CN,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY}),
                history_start=None,  # since listing
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=1,
                adjusted_modes=frozenset({Adjustment.RAW}),
                feed=FEED,
                terms=terms,
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        # No official published rate: a conservative guess, one request per second after margin.
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(RateLimit(REQUESTS_PER_SECOND, timedelta(seconds=1)),),
            key_scope="per_process",
            safety_margin=0.9,
            max_batch_symbols=1,
            history_start=None,
            official_docs_url=DOCS_URL,
        )

    def secret_values(self) -> tuple[str, ...]:
        return ()  # anonymous login, no credentials

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        # Adjust factors are not US-style splits; use ``fetch_adjust_factors``.
        return iter(())

    # -- session -----------------------------------------------------------------------
    @contextlib.contextmanager
    def _session(self) -> Iterator[BaoStockClient]:
        """Quota token, login, yield the client for ONE call, logout. No retries."""
        self._quota_gate.acquire(PROVIDER_ID)
        if self._before_request is not None:
            self._before_request()
        client = self._client
        try:
            client.login()
        except ProviderError:
            raise
        except Exception as exc:  # noqa: BLE001 - never leak raw transport text
            raise ProviderError(
                "provider_unavailable", f"baostock login failed ({type(exc).__name__})", retryable=True
            ) from None
        try:
            yield client
        finally:
            with contextlib.suppress(Exception):  # a failed logout must not mask the result
                client.logout()

    @staticmethod
    def _call(fn: Callable[[], Any], what: str) -> Any:
        try:
            return fn()
        except ProviderError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ProviderError(
                "provider_unavailable", f"baostock {what} failed ({type(exc).__name__})", retryable=True
            ) from None

    # -- request checks ----------------------------------------------------------------
    @staticmethod
    def _code(request: FetchRequest) -> str:
        if request.market is not Market.CN or request.interval is not Interval.D1:
            raise _bad_request("BaoStock adapter serves CN 1d bars only")
        if len(request.symbols) != 1:
            raise _bad_request("BaoStock is fetched one symbol per request")
        if request.cursor is not None:
            raise _bad_request("BaoStock adapter does not use fetch cursors")
        try:
            return to_baostock_code(request.symbols[0])
        except ValueError as exc:
            raise _bad_request(f"invalid BaoStock symbol: {exc}") from None

    @staticmethod
    def _check_raw_only(request: FetchRequest) -> None:
        feed = (request.feed or FEED).strip().lower()
        if feed != FEED:
            raise _bad_request(f"unsupported feed {feed!r}")
        adjustment = request.adjustment or LEGACY_ADJUSTMENT
        adjustment = (adjustment.value if isinstance(adjustment, Adjustment) else adjustment).strip().lower()
        if adjustment != LEGACY_ADJUSTMENT:
            raise _bad_request(f"unsupported adjustment {adjustment!r}: only raw (adjustflag 3) is stored")

    # -- bars --------------------------------------------------------------------------
    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        """Unadjusted daily bars for one symbol. Always ``adjustflag="3"``; one API call."""
        code = self._code(request)
        self._check_raw_only(request)
        series = BarSeriesCatalog.resolve(BAOSTOCK_ASHARE_RAW, currency=CURRENCY)
        fetch_id = self._fetch_id()
        window = request.window
        with self._session() as client:
            rows = self._call(
                lambda: client.query_history_k_data_plus(
                    code,
                    KLINE_FIELDS,
                    window.start.isoformat(),
                    window.end.isoformat(),
                    frequency="d",
                    adjustflag=ADJUSTFLAG_RAW,
                ),
                "query_history_k_data_plus",
            )
        bars = parse_klines(
            rows,
            code=code,
            window_start=window.start,
            window_end=window.end,
            fetch_id=fetch_id,
            series=series,
        )
        return RawBatch(provider=PROVIDER_ID, fetch_id=fetch_id, request=request, bars=tuple(bars))

    def fetch_adjust_factors(self, request: FetchRequest) -> list[BaoStockAdjustFactor]:
        """Adjust factors (``query_adjust_factor``) for one symbol; no events -> ``[]``.

        The window is the 除权除息日 range and defaults to the request window. Pitfall: the
        official default start is 2015-01-01, so a window that begins too late silently misses
        earlier events and the factors become incomplete. Callers that need the full history
        should pass a wide window (listing date, or ``1990-01-01``); this method does not widen
        it for them.
        """
        code = self._code(request)
        window = request.window
        with self._session() as client:
            rows = self._call(
                lambda: client.query_adjust_factor(code, window.start.isoformat(), window.end.isoformat()),
                "query_adjust_factor",
            )
        return parse_adjust_factors(rows, code=code)

    # -- listing -----------------------------------------------------------------------
    def list_instruments(
        self, market: Market, *, day: date | None = None
    ) -> Iterable[ProviderInstrument]:
        """A-share equities known to BaoStock on ``day`` (``query_all_stock``, one call).

        ``day`` defaults to the latest weekday (Asia/Shanghai); on an exchange holiday the API
        answers empty, so pass a trading day when that matters. Indices, funds and other
        non-equity codes are skipped. No ``instrument_id`` is generated here (see
        ``normalize.cn_instruments``). Needs a client with ``query_all_stock``.
        """
        if market is not Market.CN:
            raise _bad_request("BaoStock adapter serves the CN market only")
        if day is None:
            day = self._clock().date()
            while day.weekday() >= 5:
                day -= timedelta(days=1)
        with self._session() as client:
            query = getattr(client, "query_all_stock", None)
            if query is None:
                raise NotImplementedError("this BaoStock client has no query_all_stock")
            rows = self._call(lambda: query(day.isoformat()), "query_all_stock")
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise _invalid("stock list payload is not a list of rows")
        out: list[ProviderInstrument] = []
        for row in rows:
            if not isinstance(row, Mapping) or not isinstance(row.get("code"), str):
                raise _invalid("stock list row has no code")
            code = row["code"].strip().lower()
            if not code.startswith(_EQUITY_PREFIXES):
                continue
            name = row.get("code_name")
            out.append(
                ProviderInstrument(
                    symbol=code,
                    market=Market.CN,
                    asset_class=AssetClass.EQUITY,
                    name=name.strip() if isinstance(name, str) and name.strip() else None,
                    exchange=_VENUE_EXCHANGE[code[:2]],
                    active=str(row.get("tradeStatus", "1")).strip() == "1",
                    native_id=code,
                )
            )
        return out
