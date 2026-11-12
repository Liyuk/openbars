"""Futu OpenD Hong Kong daily bars (source identity ``futu/opend/raw``), issue #73.

Talks to a **local OpenD gateway** the user runs and logs into themselves (Futu ID or a
registered phone / e-mail). openbars never handles the Futu login: it only needs the
gateway address (``providers.futu.host`` / ``port``, default ``127.0.0.1:11111``), read at
runtime. Logging in does not imply a market-data entitlement: HK quote level, history-K
quota (distinct symbols per 30 days) and mainland / international status (often decided by
the OpenD login IP) depend on the account and Futu's current terms -- check them first.

Quotes are governed by the Futu user agreement, the OpenAPI agreement and exchange
market-data rights: no unauthorised forwarding, resale or commercial redistribution.
openbars stores them in the user's local data directory only. The SDK (``futu-api``) is not
an openbars dependency; install it yourself (``pip install futu-api``) after reading
https://openapi.futunn.com/ . Only unadjusted K-lines (``AuType.NONE``) are requested;
vendor forward / backward adjusted series are never the canonical basis (#75).

The planner does not route HK jobs here (yfinance is the HK primary); call the adapter
directly or wire it explicitly. Design: https://github.com/Liyuk/openbars/discussions/68
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import timedelta
from typing import Any

from openbars.core.errors import ProviderError
from openbars.core.models import DateRange, Interval
from openbars.core.series import AdjustmentScope, BarSeriesIdentity, PriceBasis, SessionSemantics, VolumeBasis
from openbars.providers.hk.akshare import _rows
from openbars.providers.hk.broker import HkBrokerDailyProvider
from openbars.quota.policy import QuotaPolicy, RateLimit

__all__ = ["FUTU_OPEND_RAW", "FutuOpenDProvider", "to_futu_hk_code"]

PROVIDER_ID = "futu"
FEED = "opend"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 11111
TERMS_URL = "https://openapi.futunn.com/"
NOT_INSTALLED_CODE = "futu_not_installed"
# Futu documents 60 history-K requests per 30 s; stay at half of that.
REQUESTS_PER_30S = 30
PAGE_SIZE = 1000
MAX_PAGES = 100  # guard against a gateway that keeps returning a page key
RET_OK = 0  # futu.RET_OK
KTYPE_DAY = "K_DAY"  # futu.KLType.K_DAY
AUTYPE_NONE = "None"  # futu.AuType.NONE

FUTU_OPEND_RAW = BarSeriesIdentity(
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

_COLUMNS = {"time_key": "date", "open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"}
_RATE_MARKERS = ("频率", "频繁", "too frequent", "frequency", "rate limit")
_QUOTA_MARKERS = ("额度", "quota")
_RIGHT_MARKERS = ("权限", "no right", "permission", "not authorized", "no authority")
_CONN_MARKERS = ("连接", "connect", "timeout", "timed out", "disconnect")

# () -> an OpenQuoteContext-like object with ``request_history_kline`` and ``close``.
ContextFactory = Callable[[], Any]


def to_futu_hk_code(code: str) -> str:
    """5-digit XHKG code -> Futu's ``HK.00700``."""
    return f"HK.{code}"


def _sdk_context(host: str, port: int) -> Any:
    try:
        from futu import OpenQuoteContext  # type: ignore[import-not-found]
    except ImportError:
        raise ProviderError(
            NOT_INSTALLED_CODE,
            "futu-api is not installed; `pip install futu-api` (read the Futu OpenAPI terms first) "
            "or pass a context factory",
            retryable=False,
        ) from None
    return OpenQuoteContext(host=host, port=port)


def _classify(message: str) -> ProviderError:
    text = str(message).lower()
    if any(m in text for m in _RATE_MARKERS):
        return ProviderError("rate_limited", "OpenD: request frequency limit", retryable=True)
    if any(m in text for m in _QUOTA_MARKERS):
        return ProviderError("history_quota_exhausted", "OpenD: history K-line quota exhausted", retryable=False)
    if any(m in text for m in _RIGHT_MARKERS):
        return ProviderError("no_entitlement", "OpenD: account lacks HK quote rights for this request", retryable=False)
    if any(m in text for m in _CONN_MARKERS):
        return ProviderError("provider_unavailable", "OpenD gateway not reachable", retryable=True)
    return ProviderError("provider_error", "OpenD returned an error", retryable=False)


def _normalise_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {_COLUMNS.get(str(k), str(k)): v for k, v in row.items()}


class FutuOpenDProvider(HkBrokerDailyProvider):
    id = PROVIDER_ID
    feed = FEED
    series = FUTU_OPEND_RAW
    terms_url = TERMS_URL
    terms_notes = (
        "Needs the user's own Futu login in a local OpenD; HK quote level and history-K quota depend "
        "on the account. Futu user / OpenAPI agreements and exchange rights forbid unauthorised "
        "forwarding or commercial redistribution. Local private storage only."
    )
    not_installed_code = NOT_INSTALLED_CODE

    def __init__(
        self,
        context_factory: ContextFactory | None = None,
        *,
        host: str | None = None,
        port: int | str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._host = (host or DEFAULT_HOST).strip()
        try:
            self._port = int(port) if port not in (None, "") else DEFAULT_PORT
        except (TypeError, ValueError):
            raise ProviderError("invalid_config", "providers.futu.port must be an integer", retryable=False) from None
        self._context_factory = context_factory or (lambda: _sdk_context(self._host, self._port))

    def quota_policy(self) -> QuotaPolicy:
        return QuotaPolicy(
            provider=PROVIDER_ID,
            limits=(RateLimit(REQUESTS_PER_30S, timedelta(seconds=30), label="per_30s"),),
            max_batch_symbols=1,
            official_docs_url=TERMS_URL,
        )

    def _symbol_rows(self, code: str, window: DateRange, acquire: Callable[[], None]) -> Iterable[Mapping[str, Any]]:
        ctx = self._context_factory()
        try:
            rows: list[dict[str, Any]] = []
            page_key = None
            for _ in range(MAX_PAGES):
                acquire()
                ret, data, page_key = ctx.request_history_kline(
                    to_futu_hk_code(code),
                    start=window.start.isoformat(),
                    end=window.end.isoformat(),
                    ktype=KTYPE_DAY,
                    autype=AUTYPE_NONE,
                    max_count=PAGE_SIZE,
                    page_req_key=page_key,
                )
                if ret != RET_OK:
                    raise _classify(str(data))
                rows.extend(_normalise_row(r) for r in _rows(data))
                if page_key is None:
                    return rows
            raise ProviderError("invalid_payload", "OpenD kept paging past the window", retryable=False)
        finally:
            close = getattr(ctx, "close", None)
            if callable(close):
                close()
