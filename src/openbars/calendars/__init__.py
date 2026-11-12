"""Concrete trading calendars (registered under the ``openbars.calendars`` entry point).

Further markets (HK) get their own module next to ``xnys`` and ``crypto``.
"""

from openbars.calendars.cn import CNCalendar, XSHECalendar, XSHGCalendar
from openbars.calendars.cn import is_session as cn_is_session
from openbars.calendars.cn import latest_completed_session as cn_latest_completed_session
from openbars.calendars.cn import session_bounds as cn_session_bounds
from openbars.calendars.cn import sessions as cn_sessions
from openbars.calendars.cn import trading_session as cn_trading_session
from openbars.calendars.cn import trading_sessions as cn_trading_sessions
from openbars.calendars.crypto import CryptoCalendar
from openbars.calendars.crypto import is_non_session_date as crypto_is_non_session_date
from openbars.calendars.crypto import is_session as crypto_is_session
from openbars.calendars.crypto import latest_completed_session as crypto_latest_completed_session
from openbars.calendars.crypto import reject_non_session_bars as crypto_reject_non_session_bars
from openbars.calendars.crypto import session_bounds as crypto_session_bounds
from openbars.calendars.crypto import session_index as crypto_session_index
from openbars.calendars.crypto import sessions as crypto_sessions
from openbars.calendars.xnys import (
    BarFilterResult,
    BarRejection,
    XNYSCalendar,
    is_non_session_date,
    latest_completed_session,
    reject_non_session_bars,
    session_index,
)
from openbars.core.calendar import TradingSession

__all__ = [
    "CNCalendar",
    "BarFilterResult",
    "BarRejection",
    "CryptoCalendar",
    "TradingSession",
    "XNYSCalendar",
    "XSHECalendar",
    "XSHGCalendar",
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
    "is_non_session_date",
    "latest_completed_session",
    "reject_non_session_bars",
    "session_index",
]
