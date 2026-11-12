"""CLI and bind-address tests for `openbars serve`."""

from __future__ import annotations

from pathlib import Path

import pytest

from openbars.cli.main import main
from openbars.cli.serve import HTTP_EXTRA_HINT, check_bind_host, is_loopback_host

pytestmark = pytest.mark.usefixtures("no_network")

SECRET = "serve-key-" + "0123456789abcdef"


def _config(tmp_path: Path) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: [1d]
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers: {{}}
"""
    )
    return path


@pytest.mark.parametrize(
    "host",
    ["127.0.0.1", "127.0.0.2", "127.255.255.255", "::1", "[::1]", "localhost", "LOCALHOST"],
)
def test_loopback_hosts_are_accepted(host):
    assert is_loopback_host(host)
    assert check_bind_host(host) is None


@pytest.mark.parametrize(
    "host",
    ["0.0.0.0", "::", "::0", "203.0.113.10", "198.51.100.1", "8.8.8.8", "example.com", "", "  ", "*"],
)
def test_non_loopback_hosts_are_rejected(host):
    assert not is_loopback_host(host)
    assert check_bind_host(host)


def test_serve_is_listed_in_help(capsys):
    assert main([]) == 0
    assert "serve" in capsys.readouterr().out
    # --help must work without starting a server or needing a config.
    with pytest.raises(SystemExit) as exc:
        main(["serve", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "127.0.0.1" in out and "8787" in out and "loopback" in out.lower()


def test_non_loopback_host_exits_before_binding(tmp_path, capsys):
    recorded: list[tuple] = []

    def server(*_a, **_k):
        recorded.append((_a, _k))
        raise AssertionError("server must not start for a non-loopback host")

    code = main(
        ["--config", str(_config(tmp_path)), "serve", "--host", "0.0.0.0"],
        env={},
    )
    assert code == 2
    err = capsys.readouterr().err
    assert "loopback" in err.lower() and "0.0.0.0" in err
    assert recorded == []


def test_lan_host_is_rejected(tmp_path, capsys):
    assert main(["--config", str(_config(tmp_path)), "serve", "--host", "203.0.113.10"], env={}) == 2
    assert "203.0.113.10" in capsys.readouterr().err


def test_serve_starts_on_loopback_and_prints_read_only_banner(tmp_path, capsys, monkeypatch):
    seen: dict[str, object] = {}

    def fake_run(app, *, host, port, log_level="info"):
        seen.update(app=app, host=host, port=port, log_level=log_level)

    import sys
    import types

    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.run = fake_run
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)

    assert main(["--config", str(_config(tmp_path)), "serve"], env={}) == 0
    out = capsys.readouterr().out
    assert "read-only" in out.lower()
    assert "127.0.0.1" in out and "8787" in out
    assert "no fetch" in out.lower() or "no writes" in out.lower()
    assert seen["host"] == "127.0.0.1" and seen["port"] == 8787
    assert SECRET not in out


def test_custom_loopback_port(tmp_path, capsys, monkeypatch):
    seen: dict[str, object] = {}

    def fake_run(app, *, host, port, log_level="info"):
        seen.update(host=host, port=port)

    import sys, types
    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.run = fake_run
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)

    assert main(["--config", str(_config(tmp_path)), "serve", "--host", "localhost", "--port", "8790"], env={}) == 0
    assert seen["host"] == "localhost" and seen["port"] == 8790
    assert SECRET not in capsys.readouterr().out


def test_missing_http_extra_has_install_hint(tmp_path, capsys, monkeypatch):
    import builtins
    import sys

    # Ensure a fresh failure path even if fastapi is installed in this environment.
    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "fastapi" or name.startswith("fastapi.") or name == "openbars.api.http":
            raise ImportError("No module named 'fastapi'", name="fastapi")
        if name == "uvicorn" or name.startswith("uvicorn."):
            raise ImportError("No module named 'uvicorn'", name="uvicorn")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    sys.modules.pop("openbars.api.http", None)
    sys.modules.pop("fastapi", None)
    sys.modules.pop("uvicorn", None)

    code = main(["--config", str(_config(tmp_path)), "serve"], env={})
    assert code == 2
    err = capsys.readouterr().err
    assert "optional dependency" in err and HTTP_EXTRA_HINT in err
    assert "parquet" not in err


def test_serve_output_redacts_secrets(tmp_path, capsys, monkeypatch):
    import sys, types

    cfg = _config(tmp_path)
    env_file = tmp_path / ".env"
    env_file.write_text("MASSIVE_API_KEY=" + SECRET + "\n")
    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.run = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)
    assert main(["--config", str(cfg), "serve"], env={"MASSIVE_API_KEY": SECRET}) == 0
    captured = "".join(capsys.readouterr())
    assert SECRET not in captured


def test_serve_does_not_create_quota_db(tmp_path, monkeypatch):
    """``openbars serve`` must not create quota.db when the file is absent."""
    import sys
    import types

    seen: dict[str, object] = {}

    def fake_run(app, *, host, port, log_level="info"):
        seen["app"] = app

    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.run = fake_run
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)

    data = tmp_path / "data"
    assert main(["--config", str(_config(tmp_path)), "serve"], env={}) == 0
    assert not (data / "quota.db").exists()
    assert not (tmp_path / "quota.db").exists()
    app = seen["app"]
    assert app.state.quota_gate is None
