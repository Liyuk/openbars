"""Announcement-backed XHKG ad-hoc closures: interface only, no shipped holiday dates."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from openbars.calendars.hk import (
    HKCalendar,
    coverage,
    is_session,
    latest_completed_session,
)
from openbars.calendars.hk_closures import (
    AdhocClosure,
    JsonAdhocClosureStore,
    MemoryAdhocClosureStore,
    discard_adhoc_closure,
    load_adhoc_closures,
    register_adhoc_closure,
)

# A regular library session used only as a fixture day. This is not a weather holiday
# and is never checked in as a bundled typhoon / black-rain date.
_FIXTURE_SESSION = date(2026, 3, 3)
_FIXTURE_SOURCE = "hkex_announcement"
_FIXTURE_REF = "https://example.test/hkex-circular-fixture-not-a-real-closure"


def _isolated() -> tuple[MemoryAdhocClosureStore, HKCalendar]:
    store = MemoryAdhocClosureStore()
    return store, HKCalendar(closures=store)


def test_fixture_day_is_a_library_session_without_overlay() -> None:
    assert is_session(_FIXTURE_SESSION)


def test_register_requires_source_and_reference() -> None:
    with pytest.raises(ValueError, match="source"):
        AdhocClosure(_FIXTURE_SESSION, "typhoon", "", _FIXTURE_REF)
    with pytest.raises(ValueError, match="reference"):
        AdhocClosure(_FIXTURE_SESSION, "typhoon", _FIXTURE_SOURCE, "  ")
    with pytest.raises(ValueError, match="reason"):
        AdhocClosure(_FIXTURE_SESSION, "typhoon_signal", _FIXTURE_SOURCE, _FIXTURE_REF)


def test_calendar_overlay_closes_a_library_session() -> None:
    store, cal = _isolated()
    assert cal.is_session(_FIXTURE_SESSION)
    register_adhoc_closure(
        _FIXTURE_SESSION,
        "typhoon",
        _FIXTURE_SOURCE,
        _FIXTURE_REF,
        note="fixture only",
        store=store,
    )
    assert not cal.is_session(_FIXTURE_SESSION)
    assert cal.session_bounds(_FIXTURE_SESSION) == ()
    assert cal.trading_session(_FIXTURE_SESSION) is None
    week = cal.sessions(date(2026, 3, 2), date(2026, 3, 6))
    assert _FIXTURE_SESSION not in week
    assert date(2026, 3, 2) in week
    assert date(2026, 3, 4) in week
    assert all(
        s.session_date != _FIXTURE_SESSION
        for s in cal.trading_sessions(date(2026, 3, 2), date(2026, 3, 6))
    )
    assert store.get(_FIXTURE_SESSION) is not None
    # Isolated store must not leak into the module-level calendar.
    assert is_session(_FIXTURE_SESSION)


def test_discard_restores_library_session() -> None:
    store, cal = _isolated()
    register_adhoc_closure(
        _FIXTURE_SESSION, "black_rain", _FIXTURE_SOURCE, _FIXTURE_REF, store=store
    )
    assert not cal.is_session(_FIXTURE_SESSION)
    discarded = discard_adhoc_closure(_FIXTURE_SESSION, store=store)
    assert discarded is not None
    assert discarded.reason == "black_rain"
    assert cal.is_session(_FIXTURE_SESSION)


def test_module_level_register_uses_default_store_and_is_cleaned_up() -> None:
    assert is_session(_FIXTURE_SESSION)
    try:
        register_adhoc_closure(_FIXTURE_SESSION, "other", _FIXTURE_SOURCE, _FIXTURE_REF)
        assert not is_session(_FIXTURE_SESSION)
        # latest_completed_session should also skip the overlaid day
        just_after_close = datetime(2026, 3, 3, 8, 30, tzinfo=UTC)  # 16:30 HKT
        latest = latest_completed_session(just_after_close, wait_minutes=0)
        assert latest == date(2026, 3, 2)
    finally:
        discard_adhoc_closure(_FIXTURE_SESSION)
    assert is_session(_FIXTURE_SESSION)


def test_registration_outside_coverage_is_stored_but_is_session_still_raises() -> None:
    store, cal = _isolated()
    first, _last = coverage()
    outside = date(first.year - 2, 6, 1)
    assert outside < first
    register_adhoc_closure(outside, "other", _FIXTURE_SOURCE, _FIXTURE_REF, store=store)
    assert store.get(outside) is not None
    with pytest.raises(ValueError, match="outside that range"):
        cal.is_session(outside)


def test_load_adhoc_closures_from_parsed_records() -> None:
    store, cal = _isolated()
    loaded = load_adhoc_closures(
        [
            {
                "day": "2026-03-03",
                "reason": "typhoon",
                "source": _FIXTURE_SOURCE,
                "reference": _FIXTURE_REF,
            }
        ],
        store=store,
    )
    assert len(loaded) == 1
    assert loaded[0].day == _FIXTURE_SESSION
    assert not cal.is_session(_FIXTURE_SESSION)


def test_json_store_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "closures.json"
    store = JsonAdhocClosureStore(path)
    register_adhoc_closure(
        _FIXTURE_SESSION,
        "typhoon",
        _FIXTURE_SOURCE,
        _FIXTURE_REF,
        store=store,
        recorded_at=datetime(2026, 3, 2, 8, 0, tzinfo=UTC),
    )
    assert path.is_file()
    text = path.read_text(encoding="utf-8")
    assert _FIXTURE_REF in text
    assert "2026-03-03" in text
    # Reload from disk.
    again = JsonAdhocClosureStore(path)
    row = again.get(_FIXTURE_SESSION)
    assert row is not None
    assert row.reason == "typhoon"
    assert row.source == _FIXTURE_SOURCE
    assert row.reference == _FIXTURE_REF
    cal = HKCalendar(closures=again)
    assert not cal.is_session(_FIXTURE_SESSION)
