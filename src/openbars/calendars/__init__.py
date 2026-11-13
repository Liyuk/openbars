"""Concrete trading calendars (registered under the ``openbars.calendars`` entry point).

Further markets get their own module next to ``xnys``, ``crypto``, ``cn``, and ``hk``.
"""

from typing import Any

from openbars.calendars.cn import CNCalendar, XSHECalendar, XSHGCalendar
from openbars.calendars.cn import is_session as cn_is_session
from openbars.calendars.cn import (
    latest_completed_session as cn_latest_completed_session,
)
from openbars.calendars.cn import session_bounds as cn_session_bounds
from openbars.calendars.cn import sessions as cn_sessions
from openbars.calendars.cn import trading_session as cn_trading_session
from openbars.calendars.cn import trading_sessions as cn_trading_sessions
from openbars.calendars.crypto import CryptoCalendar
from openbars.calendars.crypto import is_non_session_date as crypto_is_non_session_date
from openbars.calendars.crypto import is_session as crypto_is_session
from openbars.calendars.crypto import (
    latest_completed_session as crypto_latest_completed_session,
)
from openbars.calendars.crypto import (
    reject_non_session_bars as crypto_reject_non_session_bars,
)
from openbars.calendars.crypto import session_bounds as crypto_session_bounds
from openbars.calendars.crypto import session_index as crypto_session_index
from openbars.calendars.crypto import sessions as crypto_sessions
from openbars.calendars.hk import HKCalendar, XHKGCalendar
from openbars.calendars.hk import is_session as hk_is_session
from openbars.calendars.hk import (
    latest_completed_session as hk_latest_completed_session,
)
from openbars.calendars.hk import session_bounds as hk_session_bounds
from openbars.calendars.hk import sessions as hk_sessions
from openbars.calendars.hk import trading_session as hk_trading_session
from openbars.calendars.hk import trading_sessions as hk_trading_sessions
from openbars.calendars.hk_closures import (
    AdhocClosure,
    JsonAdhocClosureStore,
    MemoryAdhocClosureStore,
    default_closure_store,
    discard_adhoc_closure,
    load_adhoc_closures,
    register_adhoc_closure,
)
from openbars.calendars.xnys import (
    BarFilterResult,
    BarRejection,
    HourlyBarSlot,
    XNYSCalendar,
    filter_hourly_slots,
    hourly_slots,
    is_non_session_date,
    latest_completed_session,
    reject_non_session_bars,
    session_for_hourly_ts,
    session_index,
)
from openbars.core.calendar import TradingSession
from openbars.core.models import Market

# Exchange calendar name per market (CN: XSHE is an alias of XSHG; Beijing is out of scope).
CALENDAR_NAMES = {
    Market.US: "XNYS",
    Market.CRYPTO: "UTC 24/7",
    Market.CN: "XSHG",
    Market.HK: "XHKG",
}


def calendar_for_market(market: Market | str) -> Any:
    """The trading calendar of ``market``; ``ValueError`` for a market without one."""
    market = Market(market)
    if market is Market.US:
        return XNYSCalendar()
    if market is Market.CRYPTO:
        return CryptoCalendar()
    if market is Market.CN:
        return CNCalendar()
    if market is Market.HK:
        return HKCalendar()
    raise ValueError(f"unsupported market {market.value}")


__all__ = [
    "CALENDAR_NAMES",
    "AdhocClosure",
    "BarFilterResult",
    "BarRejection",
    "CNCalendar",
    "CryptoCalendar",
    "HKCalendar",
    "HourlyBarSlot",
    "JsonAdhocClosureStore",
    "MemoryAdhocClosureStore",
    "TradingSession",
    "XHKGCalendar",
    "XNYSCalendar",
    "XSHECalendar",
    "XSHGCalendar",
    "calendar_for_market",
    "cn_is_session",
    "cn_latest_completed_session",
    "cn_session_bounds",
    "cn_sessions",
    "cn_trading_session",
    "cn_trading_sessions",
    "crypto_is_non_session_date",
    "crypto_is_session",
    "crypto_latest_completed_session",
    "crypto_reject_non_session_bars",
    "crypto_session_bounds",
    "crypto_session_index",
    "crypto_sessions",
    "default_closure_store",
    "discard_adhoc_closure",
    "filter_hourly_slots",
    "hk_is_session",
    "hk_latest_completed_session",
    "hk_session_bounds",
    "hk_sessions",
    "hk_trading_session",
    "hk_trading_sessions",
    "hourly_slots",
    "is_non_session_date",
    "latest_completed_session",
    "load_adhoc_closures",
    "register_adhoc_closure",
    "reject_non_session_bars",
    "session_for_hourly_ts",
    "session_index",
]
