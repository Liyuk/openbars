from __future__ import annotations

from datetime import UTC, datetime, timedelta

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


def test_init_is_not_implemented(capsys):
    assert main(["init"]) == 2


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


def test_default_factory_skips_providers_without_credentials(tmp_path, capsys):
    # The real factory is used; no key -> provider skipped (and nothing touches the network).
    assert main(["--config", config(tmp_path), "plan"], clock=lambda: NOW, env={}) == 0
    out = capsys.readouterr().out
    assert "plan: 0 jobs" in out and "missing credentials" in out


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
