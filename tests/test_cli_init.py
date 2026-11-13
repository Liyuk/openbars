from __future__ import annotations

from pathlib import Path

import pytest

from openbars.cli import init as init_mod
from openbars.cli.main import main
from openbars.config.loader import load_settings

pytestmark = pytest.mark.usefixtures("no_network")

REPO = Path(__file__).resolve().parent.parent
SECRET = "init-key-" + "0123456789"


def test_init_in_an_empty_directory(tmp_path, capsys):
    assert main(["init", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert (tmp_path / "config.yaml").read_bytes() == (REPO / "config.example.yaml").read_bytes()
    assert (tmp_path / ".env").read_bytes() == (REPO / ".env.example").read_bytes()
    assert "created" in out and "openbars config" in out and "openbars plan" in out and "openbars run" in out
    assert ".env" in out and "--env-file" in out
    assert (tmp_path / ".env").stat().st_mode & 0o077 == 0  # private


def test_init_defaults_to_the_current_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0
    assert (tmp_path / "config.yaml").is_file() and (tmp_path / ".env").is_file()


def test_init_creates_a_missing_directory(tmp_path):
    target = tmp_path / "new" / "project"
    assert main(["init", str(target)]) == 0
    assert (target / "config.yaml").is_file()


@pytest.mark.parametrize("existing", ["config.yaml", ".env"])
def test_init_never_overwrites_and_writes_nothing(tmp_path, capsys, existing):
    original = b"# mine\nKEEP=" + SECRET.encode() + b"\n"
    (tmp_path / existing).write_bytes(original)
    assert main(["init", str(tmp_path)]) == 2
    captured = capsys.readouterr()
    assert existing in captured.err and "never overwrites" in captured.err
    assert (tmp_path / existing).read_bytes() == original  # byte-identical
    assert sorted(p.name for p in tmp_path.iterdir()) == [existing]  # no half-init
    assert captured.out == ""


def test_init_twice_is_refused(tmp_path, capsys):
    assert main(["init", str(tmp_path)]) == 0
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    capsys.readouterr()
    assert main(["init", str(tmp_path)]) == 2
    assert "config.yaml" in capsys.readouterr().err
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_init_target_that_is_a_file(tmp_path, capsys):
    (tmp_path / "file").write_text("x")
    assert main(["init", str(tmp_path / "file")]) == 2
    assert "not a directory" in capsys.readouterr().err


def test_init_rejects_config_flag(tmp_path, capsys):
    assert main(["--config", str(tmp_path / "other.yaml"), "init", str(tmp_path)]) == 2
    assert "directory" in capsys.readouterr().err and not (tmp_path / "config.yaml").exists()


def test_init_output_contains_no_secret(tmp_path, capsys, monkeypatch):
    for name in ("MASSIVE_API_KEY", "ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"):
        monkeypatch.setenv(name, SECRET)
    assert main(["init", str(tmp_path)]) == 0
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert SECRET not in (tmp_path / "config.yaml").read_text() + (tmp_path / ".env").read_text()
    assert main(["init", str(tmp_path)]) == 2
    assert SECRET not in "".join(capsys.readouterr())


def test_generated_files_are_usable_and_hold_no_values(tmp_path, capsys):
    assert main(["init", str(tmp_path)]) == 0
    # .env ships empty values; the config loads and picks up the sibling .env automatically.
    (tmp_path / ".env").write_text("MASSIVE_API_KEY=" + SECRET + "\n")
    settings = load_settings(tmp_path / "config.yaml", env={})
    assert settings.env_file_source == "./.env"
    assert settings.providers["massive"].options["api_key"] == SECRET
    capsys.readouterr()
    assert main(["--config", str(tmp_path / "config.yaml"), "config"], env={}) == 0
    assert SECRET not in capsys.readouterr().out


def test_templates_come_from_the_package_dir_first_then_the_repo_root(tmp_path, monkeypatch):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "config.example.yaml").write_text("markets: [us]\n# from package\n")
    (package / ".env.example").write_text("# from package\n")
    monkeypatch.setattr(init_mod, "template_dirs", lambda: [package, REPO])
    target = tmp_path / "out"
    assert main(["init", str(target)]) == 0
    assert "from package" in (target / "config.yaml").read_text()
    # Only the repo root has them: fall back.
    monkeypatch.setattr(init_mod, "template_dirs", lambda: [tmp_path / "nowhere", REPO])
    other = tmp_path / "out2"
    assert main(["init", str(other)]) == 0
    assert (other / ".env").read_bytes() == (REPO / ".env.example").read_bytes()


def test_default_template_locations():
    dirs = init_mod.template_dirs()
    assert dirs[0].name == "_templates" and dirs[0].parent.name == "openbars"
    assert dirs[1] == REPO
    assert init_mod.find_template(init_mod.CONFIG_TEMPLATE) is not None


def test_missing_templates_are_reported(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(init_mod, "template_dirs", lambda: [tmp_path / "nowhere"])
    assert main(["init", str(tmp_path / "t")]) == 1
    assert "cannot find template" in capsys.readouterr().err
    assert not (tmp_path / "t").exists()
