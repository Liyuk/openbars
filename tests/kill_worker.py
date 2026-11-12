"""Subprocess worker for the kill -9 resume test: run the queue until idle.

usage: kill_worker.py STATE_DB SIDE_DB LEASE_TTL_SECONDS FETCH_DELAY_SECONDS
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

from fakes import FakeProvider, SqliteSideStore, exec_key
from openbars.orchestrator.nodes import LocalNode
from openbars.orchestrator.runner import LocalOrchestrator
from openbars.storage.state import SqliteStateStore


def main() -> None:
    state_db, side_db, ttl, delay = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
    store = SqliteStateStore(state_db)
    side = SqliteSideStore(side_db)
    provider = FakeProvider("fake", delay=delay, on_fetch=lambda req: side.log_exec(exec_key(req)))
    node = LocalNode({"fake": provider}, store=store, raw=side)
    orch = LocalOrchestrator(
        store,
        node,
        policies={"fake": provider.quota_policy()},
        lease_ttl=timedelta(seconds=ttl),
        lock_path=Path(state_db).with_name("scheduler.lock"),
    )
    report = orch.run()
    print(f"done={report.done} failed={report.failed} dead={report.dead}", flush=True)


if __name__ == "__main__":
    main()
