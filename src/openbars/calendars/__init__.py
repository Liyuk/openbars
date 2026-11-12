"""Concrete trading calendars (registered under the ``openbars.calendars`` entry point).

Further markets (crypto, CN, HK) get their own module next to ``xnys``.
"""

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
    "BarFilterResult",
    "BarRejection",
    "TradingSession",
    "XNYSCalendar",
    "is_non_session_date",
    "latest_completed_session",
    "reject_non_session_bars",
    "session_index",
]
