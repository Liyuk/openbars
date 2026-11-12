"""Several processes sharing one SQLite file must never exceed the effective limits.

Real clocks and short windows keep this fast; the assertions are on the granted instants
the processes report back.
"""

import multiprocessing as mp
from datetime import timedelta

from openbars.quota.persistent import SqliteQuotaGate
from openbars.quota.policy import QuotaPolicy, RateLimit

# 12 per second window, 6 per half second: bursts are possible, windows must still hold.
LIMITS = (
    RateLimit(6, timedelta(seconds=0.5), label="half"),
    RateLimit(12, timedelta(seconds=1), label="second"),
)
POLICY = QuotaPolicy("p", LIMITS, safety_margin=1.0)
PROCESSES = 4
GRANTS_EACH = 6


def worker(path: str, seed: int) -> list[tuple[str, int]]:
    gate = SqliteQuotaGate(path, {"p": POLICY})
    out = []
    for i in range(GRANTS_EACH):
        weight = 1 + (seed + i) % 2
        lease = gate.acquire("p", weight=weight)
        out.append((lease.granted_at.isoformat(), weight))
    gate.close()
    return out


def test_granted_weight_never_exceeds_the_window_cap_across_processes(tmp_path):
    from datetime import datetime

    path = str(tmp_path / "state.db")
    ctx = mp.get_context("spawn")
    with ctx.Pool(PROCESSES) as pool:
        results = pool.starmap(worker, [(path, n) for n in range(PROCESSES)])

    grants = sorted(
        (datetime.fromisoformat(at), weight) for result in results for at, weight in result
    )
    assert len(grants) == PROCESSES * GRANTS_EACH
    for limit in LIMITS:
        for at, _ in grants:
            total = sum(w for t, w in grants if at - limit.per < t <= at)
            assert total <= limit.count, (limit.name, total)
    # The cap was actually exercised: the busiest half-second used most of its budget.
    busiest = max(sum(w for t, w in grants if at - LIMITS[0].per < t <= at) for at, _ in grants)
    assert busiest >= 4
