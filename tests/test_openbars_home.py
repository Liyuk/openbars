from __future__ import annotations

from pathlib import Path

from openbars.cli.main import main


def _show_config(cwd: Path, args: list[str], env: dict[str, str], monkeypatch, capsys) -> str:
    cwd.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(cwd)
    assert main([*args, "config"], env=env) == 0
    return capsys.readouterr().out


def test_missing_cwd_config_uses_openbars_home_for_config_and_data(tmp_path, monkeypatch, capsys):
    cwd = tmp_path / "work"
    home = tmp_path / "runtime"
    config = home / "config" / "config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("markets: [us]\n", encoding="utf-8")

    output = _show_config(cwd, [], {"OPENBARS_HOME": str(home)}, monkeypatch, capsys)

    assert str(config) in output
    assert f"{home / 'data'}" in output


def test_default_openbars_home_is_dot_openbars_under_home(tmp_path, monkeypatch, capsys):
    cwd = tmp_path / "work"
    home = tmp_path / "user-home"
    config = home / ".openbars" / "config" / "config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("markets: [us]\n", encoding="utf-8")

    output = _show_config(cwd, [], {"HOME": str(home)}, monkeypatch, capsys)

    assert str(config) in output
    assert str(home / ".openbars" / "data") in output


def test_existing_cwd_config_keeps_legacy_relative_data_path(tmp_path, monkeypatch, capsys):
    cwd = tmp_path / "work"
    cwd.mkdir()
    (cwd / "config.yaml").write_text("markets: [us]\n", encoding="utf-8")

    output = _show_config(cwd, [], {"OPENBARS_HOME": str(tmp_path / "runtime")}, monkeypatch, capsys)

    assert "config: ./config.yaml" in output
    assert "./data" in output


def test_explicit_config_and_storage_path_override_openbars_home(tmp_path, monkeypatch, capsys):
    cwd = tmp_path / "work"
    cwd.mkdir()
    home = tmp_path / "runtime"
    config = tmp_path / "custom.yaml"
    config.write_text("markets: [us]\nstorage: {path: ./custom-data}\n", encoding="utf-8")

    output = _show_config(cwd, ["--config", str(config)], {"OPENBARS_HOME": str(home)}, monkeypatch, capsys)

    assert f"config: {config}" in output
    assert "./custom-data" in output
    assert str(home) not in output


def test_storage_path_in_home_config_overrides_runtime_data_default(tmp_path, monkeypatch, capsys):
    cwd = tmp_path / "work"
    home = tmp_path / "runtime"
    config = home / "config" / "config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("markets: [us]\nstorage: {path: ./pinned-data}\n", encoding="utf-8")

    output = _show_config(cwd, [], {"OPENBARS_HOME": str(home)}, monkeypatch, capsys)

    assert "./pinned-data" in output
    assert str(home / "data") not in output
