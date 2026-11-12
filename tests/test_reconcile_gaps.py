from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone

from reconcile_helpers import MASSIVE, SIP, YAHOO, obs

from openbars.reconcile import InMemoryCanonicalStore, MemoryReconciler, ReconcilePolicy, detect_gaps
from openbars.reconcile.gaps import BackfillJob, KnownGap

GEN = datetime(2024, 7, 10, 23, tzinfo=timezone.utc)
# 2024-07-03 Wed (session), 07-04 Thu (holiday), 07-05 Fri, 07-06/07 weekend, 07-08 Mon
START, END = date(2024, 7, 3), date(2024, 7, 8)
SESSIONS = [date(2024, 7, 3), date(2024, 7, 5), date(2024, 7, 8)]


def bar(day, source=MASSIVE, **kw):
    return obs(source, "100", day=day, instrument_id="ZZZA", **kw)


def test_detect_gaps_only_expects_sessions():
    report = detect_gaps("ZZZA", START, END, SESSIONS)
    assert report.backfill_jobs == () and report.known_gaps == ()


def test_missing_session_yields_backfill_job_not_weekend_or_holiday():
    report = detect_gaps("ZZZA", START, END, [date(2024, 7, 3), date(2024, 7, 8)])
    assert report.backfill_jobs == (BackfillJob("ZZZA", date(2024, 7, 5), "gap"),)
    assert report.known_gaps == ()


def test_unfillable_missing_session_becomes_known_gap():
    report = detect_gaps(
        "ZZZA", START, END, [date(2024, 7, 3)], unfillable={date(2024, 7, 5)}
    )
    assert report.known_gaps == (KnownGap("ZZZA", date(2024, 7, 5), "no_source_coverage"),)
    assert report.backfill_jobs == (BackfillJob("ZZZA", date(2024, 7, 8), "gap"),)


def test_refill_disabled_records_known_gaps():
    report = detect_gaps("ZZZA", START, END, [], refill=False)
    assert [g.session_date for g in report.known_gaps] == SESSIONS
    assert report.backfill_jobs == ()


def test_reconciler_end_to_end_with_gap_and_non_session_bars():
    rec = MemoryReconciler()
    observations = [
        bar(date(2024, 7, 3)),
        bar(date(2024, 7, 4)),  # holiday
        bar(date(2024, 7, 6)),  # Saturday
        bar(date(2024, 7, 8)),
        obs(MASSIVE, "100", day=date(2024, 7, 5), instrument_id="OTHER"),  # other instrument
    ]
    result = rec.reconcile_range("ZZZA", START, END, observations, generated_at=GEN)
    assert [r.session_date for r in result.canonical] == [date(2024, 7, 3), date(2024, 7, 8)]
    assert {(r.observation.session_date, r.reason) for r in result.rejected} == {
        (date(2024, 7, 4), "non_session_date"),
        (date(2024, 7, 6), "non_session_date"),
    }
    assert result.backfill_jobs == (BackfillJob("ZZZA", date(2024, 7, 5), "gap"),)
    assert result.known_gaps == ()
    assert len(result.audit) == 2


def test_gap_becomes_known_gap_when_unfillable_and_job_cleared_on_fill():
    store = InMemoryCanonicalStore()
    rec = MemoryReconciler(store)
    first = rec.reconcile_range(
        "ZZZA", START, END, [bar(date(2024, 7, 3)), bar(date(2024, 7, 8))], generated_at=GEN
    )
    assert store.backfill_jobs == first.backfill_jobs
    second = rec.reconcile_range(
        "ZZZA", START, END, [bar(date(2024, 7, 3)), bar(date(2024, 7, 8))],
        generated_at=GEN, unfillable={date(2024, 7, 5)},
    )  # fmt: skip
    assert second.known_gaps == (KnownGap("ZZZA", date(2024, 7, 5), "no_source_coverage"),)
    assert store.backfill_jobs == () and len(store.known_gaps) == 1
    rec.reconcile_range("ZZZA", START, END, [bar(date(2024, 7, 5), SIP)], generated_at=GEN)
    assert store.known_gaps == ()


def test_policy_refill_off_goes_to_known_gaps():
    rec = MemoryReconciler(policy=ReconcilePolicy(refill_gaps=False))
    result = rec.reconcile_range("ZZZA", START, END, [bar(date(2024, 7, 3))], generated_at=GEN)
    assert result.backfill_jobs == ()
    assert [g.session_date for g in result.known_gaps] == SESSIONS[1:]


def test_revision_bumps_when_selected_source_changes():
    store = InMemoryCanonicalStore()
    rec = MemoryReconciler(store)
    day = date(2024, 7, 3)
    r1 = rec.reconcile_range("ZZZA", day, day, [bar(day, YAHOO)], generated_at=GEN)
    assert r1.canonical[0].revision == 1 and r1.audit[0].revised is False
    # same data again: nothing changes
    r2 = rec.reconcile_range("ZZZA", day, day, [bar(day, YAHOO)], generated_at=GEN)
    assert r2.canonical[0].revision == 1 and r2.audit[0].revised is False
    # a better source arrives: selected source and fingerprint change
    r3 = rec.reconcile_range(
        "ZZZA", day, day, [bar(day, YAHOO), bar(day, MASSIVE, source_bar_id="9")], generated_at=GEN
    )
    assert r3.canonical[0].revision == 2
    assert r3.audit[0].revised is True
    assert r3.canonical[0].provider == "massive"
    assert len(store.history(r3.canonical[0].unique_key)) == 1
    # source revision increases
    r4 = rec.reconcile_range(
        "ZZZA", day, day, [replace(bar(day, MASSIVE, source_bar_id="9"), revision=2)],
        generated_at=GEN,
    )  # fmt: skip
    assert r4.canonical[0].revision == 3 and r4.audit[0].revised is True
    assert [a.revision for a in store.audit] == [1, 2, 3]
