"""CorporateAction model, vote/merge and stale-history detection (synthetic only)."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal as D

import pytest

from openbars.core.models import Adjustment, CorporateAction, CorporateActionKind, QualityFlag
from openbars.normalize.actions import find_stale_after_split, merge_corporate_actions, stale_instruments

SPLIT = CorporateActionKind.SPLIT
DIV = CorporateActionKind.DIVIDEND
EX = date(2020, 8, 31)


def act(source, ratio=None, amount=None, *, kind=SPLIT, iid="us:eq:T1", ex=EX):
    return CorporateAction(iid, ex, kind, source, ratio=ratio, amount=amount)


# --- model -----------------------------------------------------------------------------


def test_ratio_and_amount_are_decimal_never_float():
    a = CorporateAction("us:eq:T1", EX, SPLIT, "s", ratio=4, amount="0.125")
    assert a.ratio == D("4") and isinstance(a.ratio, D) and a.amount == D("0.125")
    with pytest.raises(TypeError):
        CorporateAction("us:eq:T1", EX, SPLIT, "s", ratio=4.0)
    with pytest.raises(TypeError):
        CorporateAction("us:eq:T1", EX, DIV, "s", amount=0.125)
    with pytest.raises(ValueError):
        CorporateAction("us:eq:T1", EX, SPLIT, "s", ratio=D("NaN"))
    with pytest.raises(ValueError):
        CorporateAction("us:eq:T1", EX, SPLIT, "s", ratio=D("Infinity"))
    for bad in (D("0"), D("-2")):
        with pytest.raises(ValueError, match="positive"):
            CorporateAction("us:eq:T1", EX, SPLIT, "s", ratio=bad)
    assert CorporateAction("us:eq:T1", EX, SPLIT, "s").ratio is None  # source gave none


# --- merge -----------------------------------------------------------------------------


def test_sources_that_agree_give_one_canonical_row():
    merged = merge_corporate_actions([act("b", D("4")), act("a", D("4.00000000"))])
    assert len(merged) == 1
    m = merged[0]
    assert m.action.ratio == D("4") and m.action.source == "a"  # lexicographic without priority
    assert m.sources == ("a", "b") and not m.disputed and m.conflicts == ()
    assert m.quality == QualityFlag.NONE


def test_disagreement_is_marked_never_averaged():
    merged = merge_corporate_actions([act("a", D("4")), act("b", D("2"))], source_priority=["b", "a"])
    (m,) = merged
    assert m.action.ratio == D("2") and m.action.source == "b"  # tie -> first in priority
    assert m.disputed and QualityFlag.DIVERGENT in m.quality
    (c,) = m.conflicts
    assert c.field == "ratio" and dict(c.votes) == {"a": D("4"), "b": D("2")} and c.chosen == D("2")
    # no priority: lexicographic source wins the tie
    assert merge_corporate_actions([act("b", D("2")), act("a", D("4"))])[0].action.ratio == D("4")


def test_majority_wins_and_conflict_is_still_recorded():
    m = merge_corporate_actions(
        [act("a", D("4")), act("b", D("2")), act("c", D("2"))], source_priority=["a"]
    )[0]
    assert m.action.ratio == D("2") and m.disputed


def test_none_fields_abstain_and_keys_stay_separate():
    rows = [
        act("a", D("4")),
        act("b", None),  # carries no ratio: does not dispute
        act("a", None, D("0.5"), kind=DIV, ex=date(2020, 9, 1)),
        act("a", D("2"), ex=date(2021, 1, 4), iid="us:eq:T2"),
    ]
    merged = merge_corporate_actions(rows)
    assert [(m.action.instrument_id, m.action.ex_date, m.action.kind) for m in merged] == [
        ("us:eq:T1", EX, SPLIT), ("us:eq:T1", date(2020, 9, 1), DIV), ("us:eq:T2", date(2021, 1, 4), SPLIT),
    ]
    assert not any(m.disputed for m in merged)
    assert merged[0].action.ratio == D("4") and merged[1].action.amount == D("0.5")
    assert QualityFlag.SINGLE_SOURCE in merged[1].quality and QualityFlag.SINGLE_SOURCE not in merged[0].quality


def test_append_only_same_source_later_row_wins():
    (m,) = merge_corporate_actions([act("a", D("4")), act("a", D("2"))])
    assert m.action.ratio == D("2") and not m.disputed and m.sources == ("a",)


def test_amount_disagreement_is_per_field():
    rows = [act("a", D("1.05"), D("1"), kind=DIV), act("b", D("1.05"), D("2"), kind=DIV)]
    (m,) = merge_corporate_actions(rows)
    assert [c.field for c in m.conflicts] == ["amount"] and m.action.ratio == D("1.05")


# --- stale history ---------------------------------------------------------------------


@dataclass(frozen=True)
class StoredBar:
    instrument_id: str
    session_date: date
    generated_at: datetime | None
    adjustment: str = "split"


def at(day):
    return datetime(day.year, day.month, day.day, 22, tzinfo=UTC)


def test_bars_fetched_before_the_split_are_stale():
    bars = [
        StoredBar("us:eq:T1", date(2020, 8, 27), at(date(2020, 8, 28))),  # predates split, fetched before
        StoredBar("us:eq:T1", date(2020, 8, 28), at(date(2020, 8, 28))),
        StoredBar("us:eq:T1", date(2020, 8, 26), at(date(2020, 9, 2))),  # refetched after: fine
        StoredBar("us:eq:T1", date(2020, 9, 1), at(date(2020, 9, 2))),  # after ex-date: fine
        StoredBar("us:eq:T2", date(2020, 8, 27), at(date(2020, 8, 28))),  # no split for T2
        StoredBar("us:eq:T3", date(2020, 8, 27), at(date(2020, 8, 28)), adjustment="raw"),  # raw never stale
    ]
    actions = [act("a", D("4")), act("b", D("4")), act("a", D("1.0"), ex=date(2020, 8, 1), iid="us:eq:T2"),
               act("a", None, D("1"), kind=DIV)]
    found = find_stale_after_split(bars, actions + [act("a", D("4"), iid="us:eq:T3")])
    assert len(found) == 1
    r = found[0]
    assert (r.instrument_id, r.ex_date, r.ratio) == ("us:eq:T1", EX, D("4"))
    assert (r.first_session, r.last_session, r.bar_count, r.unknown_fetch_time) == (
        date(2020, 8, 27), date(2020, 8, 28), 2, 0,
    )
    assert stale_instruments(bars, actions) == {"us:eq:T1"}


def test_fetch_on_the_ex_date_counts_as_adjusted_and_unknown_time_is_explicit():
    fetched_on_ex = StoredBar("us:eq:T1", date(2020, 8, 28), at(EX))
    unknown = StoredBar("us:eq:T1", date(2020, 8, 27), None)
    actions = [act("a", D("4"))]
    assert find_stale_after_split([fetched_on_ex], actions) == []
    (r,) = find_stale_after_split([fetched_on_ex, unknown], actions)
    assert r.bar_count == 1 and r.unknown_fetch_time == 1
    assert find_stale_after_split([unknown], actions, treat_unknown_as_stale=False) == []


def test_stale_with_canonical_bar_models():
    from synth import utc

    from openbars.core.models import Bar, Interval, Session

    bar = Bar("us:eq:T1", Interval.D1, utc(date(2020, 8, 28)), date(2020, 8, 28), Session.REGULAR,
              D("1"), D("2"), D("1"), D("2"), D("1"), "prov_a", adjustment=Adjustment.SPLIT,
              ingested_at=at(date(2020, 8, 29)))
    assert stale_instruments([bar], [act("a", D("4"))]) == {"us:eq:T1"}
