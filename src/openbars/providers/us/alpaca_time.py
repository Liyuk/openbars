"""Time helpers for Alpaca historical requests."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")
# The SIP feed is not served for the most recent ~15 minutes on the free tier;
# stay one minute past that so a same-day end never asks for newer data.
SIP_RECENT_DATA_DELAY = timedelta(minutes=16)


def normalize_feed(feed: str) -> str:
    """``"alpaca:SIP"`` / ``" sip "`` -> ``"sip"``."""
    value = feed.strip().lower()
    if value.startswith("alpaca:"):
        value = value[len("alpaca:"):]
    return value


def historical_end_parameter(end_date: date, *, feed: str, now: datetime) -> str:
    """Value for the ``end`` query parameter.

    For the SIP feed and an ``end_date`` equal to today in New York, return
    ``now - 16 minutes`` as an RFC 3339 UTC ("Z") timestamp; otherwise the plain date.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if normalize_feed(feed) == "sip" and end_date == now.astimezone(NEW_YORK).date():
        cutoff = (now - SIP_RECENT_DATA_DELAY).astimezone(timezone.utc)
        return cutoff.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    return end_date.isoformat()
