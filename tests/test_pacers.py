from datetime import UTC, datetime, timedelta

from openbars.quota.pacer import PersistentRequestPacer, RequestPacer, store_slot_reserver
from openbars.quota.store import SqliteQuotaStore


def test_request_pacer_spaces_requests_to_provider_allowance():
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    pacer = RequestPacer(5, monotonic=lambda: now[0], sleep=sleep)
    pacer.wait()
    pacer.wait()
    pacer.wait()

    assert sleeps == [12.0, 12.0]


def test_request_spacing_survives_pacer_recreation(clock, tmp_path):
    store = SqliteQuotaStore(tmp_path / "state.db")

    def pacer():
        return PersistentRequestPacer(
            "massive",
            5,
            reserve_slot=store_slot_reserver(store, clock=clock),
            utcnow=clock,
            sleep=clock.sleep,
        )

    start = clock.now
    pacer().wait()
    pacer().wait()  # a worker restart between provider requests

    assert clock.sleeps == [12.0]
    assert clock.now == start + timedelta(seconds=12)
    with store.transaction("massive", "pacer") as tx:
        state, _ = tx.load(start)
    assert state.next_request_at == clock.now + timedelta(seconds=12)
    store.close()


def test_pacer_state_is_shared_through_the_file_and_honors_penalties(clock, tmp_path):
    path = tmp_path / "state.db"
    first, second = SqliteQuotaStore(path), SqliteQuotaStore(path)
    interval = timedelta(seconds=12)
    a = store_slot_reserver(first, clock=clock)
    b = store_slot_reserver(second, clock=clock)

    assert a("massive", interval) == clock.now
    assert b("massive", interval) == clock.now + interval
    second.penalize("massive", clock.now + timedelta(minutes=5))
    assert a("massive", interval) == clock.now + timedelta(minutes=5)
    first.close()
    second.close()


def test_persistent_pacer_treats_naive_slot_as_utc():
    now = datetime(2026, 10, 15, tzinfo=UTC)
    sleeps = []
    pacer = PersistentRequestPacer(
        "p",
        60,
        reserve_slot=lambda provider, interval: datetime(2026, 10, 15, 0, 0, 3),
        utcnow=lambda: now,
        sleep=sleeps.append,
    )
    pacer.wait()
    assert sleeps == [3.0]
