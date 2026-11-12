from __future__ import annotations

import io
from pathlib import Path

import pytest
import yaml

from openbars.cli.main import main

pytestmark = pytest.mark.usefixtures("no_network")


class Terminal(io.StringIO):
    def isatty(self):
        return True


def test_wizard_writes_selected_settings_and_hides_keys(tmp_path, monkeypatch, capsys):
    secret = "private-" + "sample-value"
    monkeypatch.setattr("sys.stdin", Terminal("us,crypto\n1d,1h\nalpaca,binance\n"))
    monkeypatch.setattr("getpass.getpass", lambda prompt: secret)
    assert main(["init", str(tmp_path)]) == 0
    config = yaml.safe_load((tmp_path / "config.yaml").read_text())
    assert config["markets"] == ["us", "crypto"]
    assert config["intervals"] == ["1d", "1h"]
    assert config["providers"]["alpaca"]["enabled"]
    assert config["providers"]["binance"]["enabled"]
    assert not config["providers"]["massive"]["enabled"]
    assert secret in (tmp_path / ".env").read_text()
    assert (tmp_path / ".env").stat().st_mode & 0o077 == 0
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert main(["--config", str(tmp_path / "config.yaml"), "config"], env={}) == 0
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert "set" in output.out


def test_yes_bypasses_terminal_wizard(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.stdin", Terminal())
    assert main(["init", "--yes", str(tmp_path)]) == 0
    repo = Path(__file__).resolve().parent.parent
    assert (tmp_path / "config.yaml").read_bytes() == (repo / "config.example.yaml").read_bytes()
    assert (tmp_path / ".env").read_bytes() == (repo / ".env.example").read_bytes()


@pytest.mark.parametrize("existing", ["config.yaml", ".env"])
def test_terminal_conflict_does_not_prompt_or_write(tmp_path, monkeypatch, existing):
    (tmp_path / existing).write_text("keep")
    monkeypatch.setattr("sys.stdin", Terminal())
    assert main(["init", str(tmp_path)]) == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == [existing]
    assert (tmp_path / existing).read_text() == "keep"


def test_cancelled_wizard_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.stdin", Terminal("us\n1d\nalpaca\n"))
    def cancel(prompt):
        raise EOFError
    monkeypatch.setattr("getpass.getpass", cancel)
    assert main(["init", str(tmp_path)]) == 2
    assert list(tmp_path.iterdir()) == []
