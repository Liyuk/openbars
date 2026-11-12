from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pathlib import Path

import pytest
from fakes import FakeProvider, always_fail

from openbars.cli.main import main
from openbars.core.errors import ProviderError
from openbars.core.models import Interval
from openbars.orchestrator.jobs import JobState
from openbars.orchestrator.lock import SchedulerLock
from openbars.providers.base import BatchShape
from openbars.storage.state import SqliteStateStore

pytestmark = pytest.mark.usefixtures("no_network")


NOW = datetime(2026, 3, 10, 15, 0, tzinfo=UTC)  # latest ready session: Monday 2026-03-09
SECRET = "tkn-0123" + "456789abcdef"


def config(tmp_path, *, intervals="[1d]", extra_universe="[TEST1, ZZZA]", start="2026-03-02") -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: {intervals}
history: {{start: {start}, end: null}}
universe:
  us:
    symbols: {extra_universe}
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  massive:
    enabled: true
    api_key: ${{TEST_API_KEY}}
"""
    )
    return str(path)


def factory(**massive_kw):
    def build(settings, gate):
        return {
            "massive": FakeProvider(
                "massive", batch=BatchShape.WHOLE_MARKET_PER_DAY, rolling_window=timedelta(days=730),
                max_symbols=10**6, feed="eod", **massive_kw,
            )  # fmt: skip
        }

    return build


def cli(argv, tmp_path, provider_factory=None, **kw):
    return main(
        ["--config", config(tmp_path), *argv],
        provider_factory=provider_factory or factory(),
        clock=lambda: NOW,
        env={"TEST_API_KEY": SECRET},
        **kw,
    )


def state(tmp_path) -> SqliteStateStore:
    return SqliteStateStore(tmp_path / "data" / "state.db")


def test_no_command_prints_help_and_returns_zero(capsys):
    assert main([]) == 0
    assert "usage" in capsys.readouterr().out.lower()


def test_plan_is_a_dry_run(tmp_path, capsys):
    assert cli(["plan"], tmp_path) == 0
    out = capsys.readouterr().out
    assert "plan: 6 jobs (6 new" in out and "massive" in out and "daily" in out and "backfill" in out
    assert "budget" in out and "6 requests" in out and "dry run" in out
    assert state(tmp_path).list_jobs() == []  # nothing queued


def test_plan_submit_queues_jobs_idempotently(tmp_path, capsys):
    assert cli(["plan", "--submit"], tmp_path) == 0
    assert "submitted 6 new jobs" in capsys.readouterr().out
    assert state(tmp_path).count_jobs()[JobState.PENDING] == 6
    assert cli(["plan", "--submit"], tmp_path) == 0
    out = capsys.readouterr().out
    assert "submitted 0 new jobs" in out and "(0 new, 6 already queued)" in out
    assert len(state(tmp_path).list_jobs()) == 6


def test_run_executes_until_idle_and_is_idempotent(tmp_path, capsys):
    fake_calls = []
    f = factory(on_fetch=lambda req: fake_calls.append(req))
    assert cli(["run"], tmp_path, f) == 0
    out = capsys.readouterr().out
    assert "done 6" in out and "dead 0" in out
    counts = state(tmp_path).count_jobs()
    assert counts[JobState.DONE] == 6 and sum(counts.values()) == 6
    assert len(fake_calls) == 6
    # Second run: raw coverage now covers every session -> nothing to plan or execute.
    assert cli(["run"], tmp_path, f) == 0
    out = capsys.readouterr().out
    assert "plan: 0 jobs" in out and "claimed 0" in out
    assert len(fake_calls) == 6


def test_run_max_jobs(tmp_path, capsys):
    assert cli(["run", "--max-jobs", "2"], tmp_path) == 0
    assert "claimed 2" in capsys.readouterr().out
    counts = state(tmp_path).count_jobs()
    assert counts[JobState.DONE] == 2 and counts[JobState.PENDING] == 4


def test_status_shows_dead_job(tmp_path, capsys):
    err = ProviderError("invalid_request", "window rejected by provider", retryable=False, status_code=400)
    assert cli(["run"], tmp_path, factory(behavior=always_fail(err))) == 0
    capsys.readouterr()
    assert cli(["status"], tmp_path) == 0
    out = capsys.readouterr().out
    assert "dead=6" in out and "dead jobs (6)" in out
    dead = state(tmp_path).list_jobs(state=JobState.DEAD)
    assert dead[0].job_id in out and "massive" in out and "window rejected by provider" in out


def test_status_shows_counts_failed_and_checkpoints(tmp_path, capsys):
    err = ProviderError("timeout", "slow upstream", retryable=True)
    assert cli(["run", "--max-jobs", "1"], tmp_path, factory(behavior=always_fail(err))) == 0
    capsys.readouterr()
    cli(["status"], tmp_path)
    out = capsys.readouterr().out
    assert "failed=1" in out and "pending=5" in out and "waiting for retry" in out and "retry_at=" in out
    assert "checkpoints: 0 in progress" in out


def test_status_without_state_db(tmp_path, capsys):
    assert cli(["status"], tmp_path) == 0
    assert "no state database" in capsys.readouterr().out


def test_missing_config_is_a_clean_error(tmp_path, capsys):
    assert main(["--config", str(tmp_path / "absent.yaml"), "plan"]) == 2
    err = capsys.readouterr().err
    assert "config file not found" in err and "Traceback" not in err


def test_invalid_market_is_a_clean_error(tmp_path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text("markets: [mars]\n")
    assert main(["--config", str(bad), "plan"]) == 2
    assert "invalid market" in capsys.readouterr().err


def test_data_flag_overrides_storage_path(tmp_path, capsys):
    other = tmp_path / "elsewhere"
    assert main(["--config", config(tmp_path), "--data", str(other), "plan", "--submit"],
                provider_factory=factory(), clock=lambda: NOW, env={"TEST_API_KEY": SECRET}) == 0  # fmt: skip
    assert (other / "state.db").exists() and not (tmp_path / "data" / "state.db").exists()


def test_secrets_never_reach_output(tmp_path, capsys):
    leak = ProviderError("auth", f"rejected key {SECRET} for request", retryable=False)
    assert cli(["plan", "--submit"], tmp_path, factory(behavior=always_fail(leak))) == 0
    assert cli(["run"], tmp_path, factory(behavior=always_fail(leak))) == 0
    assert cli(["status"], tmp_path) == 0
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert "rejected key" in captured.out  # the message itself is still shown, masked
    assert SECRET not in "".join(j.last_error or "" for j in state(tmp_path).list_jobs())


def test_second_run_overlapping_the_lock_is_refused(tmp_path, capsys):
    (tmp_path / "data").mkdir()
    with SchedulerLock(tmp_path / "data" / "scheduler.lock"):
        assert cli(["run"], tmp_path) == 1
        assert cli(["plan", "--submit"], tmp_path) == 1
    err = capsys.readouterr().err
    assert "already running" in err
    assert not (tmp_path / "data" / "state.db").exists() or state(tmp_path).count_jobs()[JobState.DONE] == 0


def test_large_non_daily_budget_needs_accept_flag(tmp_path, capsys):
    class Hourly(FakeProvider):
        def capabilities(self):
            (cap,) = super().capabilities()
            from dataclasses import replace

            return (replace(cap, max_bars_per_request=100),)

    def build(settings, gate):
        return {"hourly": Hourly("hourly", interval=Interval.H1, limit_per_minute=1)}

    argv = ["--config", config(tmp_path, intervals="[1h]", extra_universe="[" + ",".join(f"S{i:03d}" for i in range(100)) + "]", start="2026-01-05"),
            "plan", "--submit"]  # fmt: skip
    kw = dict(provider_factory=build, clock=lambda: NOW, env={"TEST_API_KEY": SECRET})
    assert main(argv, **kw) == 3
    assert "--accept-budget" in capsys.readouterr().err
    assert not (tmp_path / "data" / "state.db").exists() or not state(tmp_path).list_jobs()
    assert main([*argv, "--accept-budget"], **kw) == 0
    assert state(tmp_path).list_jobs()


def test_default_factory_without_any_credentials_is_a_clear_error(tmp_path, capsys):
    # The real factory is used; no key -> no provider at all for `us` (nothing touches the network).
    for command in ("plan", "run"):
        assert main(["--config", config(tmp_path), command], clock=lambda: NOW, env={}) == 2
        err = capsys.readouterr().err
        assert "missing credentials" in err and "TEST_API_KEY" in err and "massive" in err
    assert not (tmp_path / "data" / "state.db").exists()


TWO_PROVIDERS = """
version: 1
markets: [us]
intervals: [1d]
history: {{start: 2026-03-02, end: null}}
universe: {{us: {{symbols: [TEST1]}}}}
storage: {{backend: parquet_duckdb, path: {data}}}
providers:
  massive:
    enabled: true
    api_key: ${{MASSIVE_API_KEY}}
  alpaca:
    enabled: true
    role: {role}
    key_id: ${{ALPACA_API_KEY_ID}}
    secret_key: ${{ALPACA_API_SECRET_KEY}}
"""


def two_provider_config(tmp_path, role="primary") -> str:
    path = tmp_path / "config.yaml"
    path.write_text(TWO_PROVIDERS.format(data=tmp_path / "data", role=role))
    return str(path)


def test_run_fails_when_a_primary_provider_lacks_credentials(tmp_path, capsys):
    key = "massive-" + "key-12345678"
    code = main(["--config", two_provider_config(tmp_path), "run"], clock=lambda: NOW, env={"MASSIVE_API_KEY": key})
    captured = capsys.readouterr()
    assert code == 2
    assert "ALPACA_API_KEY_ID" in captured.err and "ALPACA_API_SECRET_KEY" in captured.err
    assert "key_id" in captured.err and "MASSIVE_API_KEY" not in captured.err  # massive is fine
    assert key not in captured.out + captured.err
    assert not (tmp_path / "data" / "state.db").exists()  # failed before writing anything


def test_plan_still_dry_runs_with_a_missing_secondary_credential(tmp_path, capsys):
    key = "massive-" + "key-12345678"
    code = main(["--config", two_provider_config(tmp_path), "plan"], clock=lambda: NOW, env={"MASSIVE_API_KEY": key})
    out = capsys.readouterr().out
    assert code == 0 and "plan:" in out
    assert "provider alpaca enabled but skipped: missing credentials" in out and "ALPACA_API_KEY_ID" in out
    assert key not in out


def test_run_ignores_missing_credentials_of_non_primary_providers(tmp_path, capsys):
    cfg = two_provider_config(tmp_path, role="audit")
    key = "massive-" + "key-12345678"
    assert main(["--config", cfg, "run", "--max-jobs", "0"], clock=lambda: NOW, env={"MASSIVE_API_KEY": key},
                provider_factory=factory()) == 0  # fmt: skip
    assert "missing credentials" in capsys.readouterr().out


def test_run_with_real_factory_and_default_dotenv(tmp_path, capsys):
    key = "massive-" + "key-12345678"
    cfg = two_provider_config(tmp_path, role="audit")
    (tmp_path / ".env").write_text(f"MASSIVE_API_KEY={key}\n")
    code = main(["--config", cfg, "plan"], clock=lambda: NOW, env={})
    out = capsys.readouterr().out
    assert code == 0 and "massive" in out and key not in out


def test_unimplemented_interval_is_a_clear_error(tmp_path, capsys):
    for command in ("plan", "run"):
        argv = ["--config", config(tmp_path, intervals="[1d, 1h]"), command]
        code = main(argv, provider_factory=factory(), clock=lambda: NOW, env={"TEST_API_KEY": SECRET})
        err = capsys.readouterr().err
        assert code == 2 and "interval 1h is not implemented" in err and "available: 1d" in err
    assert not (tmp_path / "data" / "state.db").exists()


def test_interval_check_ignores_providers_that_are_not_built(tmp_path, capsys):
    # A disabled provider that *would* offer 1h does not count; an enabled+built one does.
    class Hourly(FakeProvider):
        pass

    def build(settings, gate):
        return {"hourly": Hourly("hourly", interval=Interval.H1)}

    argv = ["--config", config(tmp_path, intervals="[1h]"), "plan"]
    assert main(argv, provider_factory=build, clock=lambda: NOW, env={"TEST_API_KEY": SECRET}) == 0
    assert main(argv, provider_factory=factory(), clock=lambda: NOW, env={"TEST_API_KEY": SECRET}) == 2
    assert "interval 1h" in capsys.readouterr().err


def test_daily_us_still_works(tmp_path, capsys):
    assert cli(["plan"], tmp_path) == 0
    assert "plan: 6 jobs" in capsys.readouterr().out


def test_widening_quota_needs_override_true(tmp_path, capsys):
    def with_quota(quota: str) -> str:
        path = tmp_path / "config.yaml"
        path.write_text(Path(config(tmp_path)).read_text() + f"    quota: {quota}\n")
        return str(path)

    kw = dict(provider_factory=factory(), clock=lambda: NOW, env={"TEST_API_KEY": SECRET})
    assert main(["--config", with_quota("{per_minute: 5000}"), "plan"], **kw) == 2
    err = capsys.readouterr().err
    assert "providers.massive.quota" in err and "per_minute" in err and "override: true" in err
    assert main(["--config", with_quota("{per_minute: 5000, override: true}"), "plan"], **kw) == 0
    assert main(["--config", with_quota("{per_minute: 1}"), "plan"], **kw) == 0


def test_config_command_uses_default_dotenv(tmp_path, capsys):
    key = "dot-cli-" + "aaaabbbb"
    (tmp_path / ".env").write_text(f"TEST_API_KEY={key}\n")
    assert main(["--config", config(tmp_path), "config"], env={}) == 0
    out = capsys.readouterr().out
    assert key not in out and "api_key: set" in out and "(./.env)" in out and str(tmp_path / ".env") in out
    # process env still wins over the file; the output stays redacted
    other = "proc-cli-" + "ccccdddd"
    assert main(["--config", config(tmp_path), "config"], env={"TEST_API_KEY": other}) == 0
    assert other not in capsys.readouterr().out


def test_env_file_flag_and_var_beat_default_dotenv_in_cli(tmp_path, capsys):
    (tmp_path / ".env").write_text("TEST_API_KEY=\n")  # default file would leave the key missing
    named = tmp_path / "named.env"
    named.write_text("TEST_API_KEY=" + "named-key-" + "12345678\n")
    out_file = main(["--config", config(tmp_path), "--env-file", str(named), "config"], env={})
    assert out_file == 0
    out = capsys.readouterr().out
    assert "api_key: set" in out and "(--env-file)" in out
    assert main(["--config", config(tmp_path), "config"], env={"OPENBARS_ENV_FILE": str(named)}) == 0
    out = capsys.readouterr().out
    assert "api_key: set" in out and "(OPENBARS_ENV_FILE)" in out
    assert main(["--config", config(tmp_path), "config"], env={}) == 0
    assert "api_key: missing" in capsys.readouterr().out


def test_missing_default_dotenv_is_not_an_error_in_cli(tmp_path, capsys):
    assert main(["--config", config(tmp_path), "config"], env={}) == 0
    assert "env_file: (none)" in capsys.readouterr().out


def test_config_command_redacts_secrets(tmp_path, capsys):
    key_id = "kid-cli-" + "aaaabbbb"
    secret = "sec-cli-" + "ccccdddd"
    env_path = tmp_path / "keys.env"
    env_path.write_text(f"T_KEY_ID={key_id}\nT_SECRET={secret}\n")
    path = tmp_path / "config.yaml"
    path.write_text(
        """
markets: [us]
providers:
  alpaca:
    feed: sip
    key_id: ${T_KEY_ID}
    secret_key: ${T_SECRET}
"""
    )
    assert main(["--config", str(path), "--env-file", str(env_path), "config"], env={}) == 0
    out = capsys.readouterr().out
    assert key_id not in out and secret not in out
    assert "key_id: set" in out and "secret_key: set" in out
    assert "--env-file" in out
    assert "feed: sip" in out


def test_plan_postgres_without_dsn_is_a_clean_error(tmp_path, capsys):
    path = tmp_path / "config.yaml"
    data = tmp_path / "data"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: [1d]
history: {{start: 2026-03-02, end: null}}
universe:
  us:
    symbols: [TEST1]
storage:
  backend: postgres
  path: {data}
  postgres:
    dsn: ${{OPENBARS_PG_DSN}}
    mode: upstream_compat
    write: true
providers:
  massive:
    enabled: true
    api_key: ${{TEST_API_KEY}}
"""
    )
    code = main(
        ["--config", str(path), "plan"],
        provider_factory=factory(),
        clock=lambda: NOW,
        env={"TEST_API_KEY": SECRET},
    )
    captured = capsys.readouterr()
    assert code == 2
    assert "storage.postgres.dsn is required" in captured.err
    assert "postgresql://" not in captured.out + captured.err


def test_sparse_hourly_session_is_not_planned_after_run(tmp_path, capsys):
    from dataclasses import replace
    from openbars.calendars import xnys

    class SparseHourly(FakeProvider):
        def bar(self, symbol, day, fetch_id):
            return replace(super().bar(symbol, day, fetch_id), interval=Interval.H1,
                           ts_start=xnys.hourly_slots(day)[0].ts_start)

    provider = SparseHourly("massive", interval=Interval.H1)
    settings = config(tmp_path, intervals="[1h]", extra_universe="[TEST1]", start="2026-03-09")
    kw = dict(provider_factory=lambda settings, gate: {"massive": provider},
              clock=lambda: NOW, env={"TEST_API_KEY": SECRET})
    assert main(["--config", settings, "plan"], **kw) == 0
    assert "plan: 1 jobs" in capsys.readouterr().out
    assert main(["--config", settings, "run"], **kw) == 0
    assert "done 1" in capsys.readouterr().out
    assert len(provider.requests) == 1
    assert main(["--config", settings, "plan"], **kw) == 0
    assert "plan: 0 jobs" in capsys.readouterr().out


def test_hourly_cli_reports_raw_coverage_and_resume(tmp_path, capsys):
    from dataclasses import replace
    from openbars.calendars import xnys

    class Hourly(FakeProvider):
        def bar(self, symbol, day, fetch_id):
            return replace(super().bar(symbol, day, fetch_id), interval=Interval.H1,
                           ts_start=xnys.hourly_slots(day)[0].ts_start)

    provider = Hourly("massive", interval=Interval.H1)
    cfg = config(tmp_path, intervals="[1h]", extra_universe="[TEST1]", start="2026-03-09")
    kw = dict(provider_factory=lambda settings, gate: {"massive": provider},
              clock=lambda: NOW, env={"TEST_API_KEY": SECRET})
    assert main(["--config", cfg, "plan"], **kw) == 0
    assert "1h" in capsys.readouterr().out
    assert main(["--config", cfg, "run", "--max-jobs", "0"], **kw) == 0
    capsys.readouterr()
    assert main(["--config", cfg, "backfill"], **kw) == 0
    assert "canonical" in capsys.readouterr().out
    assert main(["--config", cfg, "status"], **kw) == 0
    out = capsys.readouterr().out
    assert "raw coverage" in out and "us/1h" in out and "2026-03-09" in out
    assert len(provider.requests) == 1
    assert any("interval=1h" in str(path) for path in (tmp_path / "data").rglob("*.parquet"))
    assert not list((tmp_path / "data" / "canonical").rglob("*.parquet"))


def test_mixed_intervals_plan_and_status(tmp_path, capsys):
    from dataclasses import replace
    from openbars.calendars import xnys

    class Hourly(FakeProvider):
        def bar(self, symbol, day, fetch_id):
            return replace(super().bar(symbol, day, fetch_id), interval=Interval.H1,
                           ts_start=xnys.hourly_slots(day)[0].ts_start)

    daily = FakeProvider("massive")
    hourly = Hourly("twelve_data", interval=Interval.H1)
    cfg = config(tmp_path, intervals="[1d, 1h]", extra_universe="[TEST1]", start="2026-03-09")
    with open(cfg, "a") as file:
        file.write("  twelve_data:\n    enabled: true\n    api_key: ${TWELVE_DATA_API_KEY}\n")
    kw = dict(provider_factory=lambda settings, gate: {"massive": daily, "twelve_data": hourly},
              clock=lambda: NOW, env={"TEST_API_KEY": SECRET, "TWELVE_DATA_API_KEY": SECRET})
    assert main(["--config", cfg, "plan"], **kw) == 0
    out = capsys.readouterr().out
    assert "interval 1d" in out and "interval 1h" in out and "twelve_data" in out
    assert main(["--config", cfg, "run"], **kw) == 0
    capsys.readouterr()
    assert main(["--config", cfg, "status"], **kw) == 0
    out = capsys.readouterr().out
    assert "massive us/1d: 2026-03-09" in out
    assert "twelve_data us/1h: 2026-03-09" in out
    assert main(["--config", cfg, "config"], **kw) == 0
    out = capsys.readouterr().out
    assert "1d: 2026-03-09" in out and "1h: 2026-03-09" in out
    assert "twelve_data:" in out and SECRET not in out


def test_status_summarizes_large_raw_store_without_state_or_scan(tmp_path, capsys, monkeypatch):
    import duckdb
    from openbars.storage.parquet_duckdb import ParquetRawStore

    assert cli(["run", "--max-jobs", "1"], tmp_path) == 0
    capsys.readouterr()
    part = next((tmp_path / "data" / "raw").rglob("*.parquet"))
    synthetic = part.with_name("part-9999999999999999999-synthetic.parquet")
    con = duckdb.connect()
    con.execute("COPY (SELECT * REPLACE (symbol_as_returned || i::VARCHAR AS symbol_as_returned) FROM read_parquet(?) CROSS JOIN range(20000) t(i)) TO ? (FORMAT PARQUET)", [str(part), str(synthetic)])
    con.close()
    (tmp_path / "data" / "state.db").unlink()
    monkeypatch.setattr(ParquetRawStore, "scan", lambda *a, **k: pytest.fail("full scan called"))
    assert cli(["status"], tmp_path) == 0
    out = capsys.readouterr().out
    assert "no state database" in out
    assert "massive us/1d: 2026-03-09" in out
    assert "40002 bars" in out
    assert "1 sessions" in out


def test_hourly_pagination_resumes_saved_cursor(tmp_path, clock):
    from dataclasses import replace
    from datetime import date
    from test_orchestrator import Env
    from openbars.core.models import DateRange, Market
    from openbars.orchestrator.jobs import Job, JobKind, make_job_id

    def interrupt(req, number):
        if number == 2:
            raise ProviderError("timeout", "interrupted", retryable=True)

    class Hourly(FakeProvider):
        def bar(self, symbol, day, fetch_id):
            return replace(super().bar(symbol, day, fetch_id), interval=Interval.H1)

    provider = Hourly(page_size=1, behavior=interrupt, interval=Interval.H1)
    env = Env(tmp_path, clock, [provider])
    window = DateRange(date(2026, 3, 3), date(2026, 3, 5))
    symbols = ("TEST1",)
    job = Job(make_job_id(JobKind.BACKFILL, "fake", Market.US, Interval.H1, symbols, window),
              JobKind.BACKFILL, "fake", Market.US, Interval.H1, symbols, window)
    env.orch.submit([job])
    env.orch.run()
    assert env.state(job).state is JobState.FAILED
    assert env.store.load_checkpoint(job.job_id).cursor == "1"
    clock.advance(seconds=5)
    env.orch.run()
    assert env.state(job).state is JobState.DONE
    assert [request.cursor for request in provider.requests] == [None, "1", "1", "2"]
    assert len(env.raw.rows) == 3


def test_plan_hourly_uses_state_for_empty_fetch_and_multi_session_windows(tmp_path, capsys):
    from datetime import date

    from openbars.core.models import DateRange, Market
    from openbars.orchestrator.jobs import Job, JobKind

    config_path = Path(config(tmp_path, intervals="[1h]", extra_universe="[TEST1]"))
    config_path.write_text(config_path.read_text().replace("massive:", "alpaca:").replace("end: null", "end: 2026-03-06"))
    store = state(tmp_path)
    middle = date(2026, 3, 4)
    job = Job("empty", JobKind.BACKFILL, "alpaca", Market.US, Interval.H1, ("TEST1",),
              DateRange(middle, middle), state=JobState.DONE)
    store.save_job(job)
    store.start_fetch(provider="alpaca", fetch_id="empty-fetch", job_id=job.job_id,
                      market=Market.US, interval=Interval.H1, window=job.window,
                      started_at=datetime(2026, 3, 5, 15, tzinfo=UTC))
    store.finish_fetch("alpaca", "empty-fetch", "empty", row_count=0)
    provider = FakeProvider("alpaca", interval=Interval.H1)
    assert main(["--config", str(config_path), "plan", "--submit", "--accept-budget"],
                provider_factory=lambda settings, gate: {"alpaca": provider},
                clock=lambda: NOW, env={"TEST_API_KEY": SECRET}) == 0
    assert "submitted 2 new jobs" in capsys.readouterr().out
    windows = {j.window for j in store.list_jobs() if j.job_id != "empty"}
    assert windows == {DateRange(date(2026, 3, 2), date(2026, 3, 3)),
                       DateRange(date(2026, 3, 5), date(2026, 3, 6))}
