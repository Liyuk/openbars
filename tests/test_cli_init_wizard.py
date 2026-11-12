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
    monkeypatch.setattr("sys.stdin", Terminal("us,crypto\n1d,1h\nalpaca,binance\n\n\n"))
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
    monkeypatch.setattr("sys.stdin", Terminal("us\n1d\nalpaca\n\n"))
    def cancel(prompt):
        raise EOFError
    monkeypatch.setattr("getpass.getpass", cancel)
    assert main(["init", str(tmp_path)]) == 2
    assert list(tmp_path.iterdir()) == []


def test_hourly_wizard_enables_twelve_data_and_hides_its_key(tmp_path, monkeypatch, capsys):
    secret = "private-" + "hourly-sample"
    prompts = []
    monkeypatch.setattr("sys.stdin", Terminal("us\n1h\ntwelve_data\n\n"))
    monkeypatch.setattr("getpass.getpass", lambda prompt: prompts.append(prompt) or secret)
    assert main(["init", str(tmp_path)]) == 0
    config = yaml.safe_load((tmp_path / "config.yaml").read_text())
    assert config["providers"]["twelve_data"]["enabled"]
    assert "TWELVE_DATA_API_KEY" in prompts[0]
    assert secret in (tmp_path / ".env").read_text()
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert "#117" in output.out and "#118" in output.out
    assert main(["--config", str(tmp_path / "config.yaml"), "config"], env={}) == 0
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert "set" in output.out

@pytest.mark.parametrize(('answers', 'missing'), [
    ('us,crypto\n1d\nbinance\nalpaca,binance\n\n\n', 'us/1d'),
    ('us\n1d,1h\ntwelve_data\nalpaca,twelve_data\n\n', 'us/1d'),
])
def test_wizard_reprompts_for_uncovered_selection(tmp_path, monkeypatch, capsys, answers, missing):
    monkeypatch.setattr('sys.stdin', Terminal(answers))
    monkeypatch.setattr('getpass.getpass', lambda prompt: '')
    assert main(['init', str(tmp_path)]) == 0
    assert missing in capsys.readouterr().out


def test_wizard_universe_loads_and_plans(tmp_path, monkeypatch, capsys):
    from openbars.config.loader import load_settings
    from openbars.core.models import Market
    from openbars.orchestrator.planner import DefaultPlanner
    from test_planner import EmptyCoverage, NOW, yfinance

    monkeypatch.setattr('sys.stdin', Terminal('us\n1d\nyfinance\nTEST1, ZZZA\n'))
    assert main(['init', str(tmp_path)]) == 0
    settings = load_settings(tmp_path / 'config.yaml', env={})
    assert settings.universe[Market.US] == ('TEST1', 'ZZZA')
    plan = DefaultPlanner().plan(settings.selection, EmptyCoverage(), now=NOW,
                                 universe=settings.universe, providers={"yfinance": yfinance()})
    assert any('TEST1' in job.instruments for job in plan.jobs)
    assert '# symbols: [TEST1, ZZZA]' in (tmp_path / 'config.yaml').read_text()
    assert 'https://github.com/Liyuk/openbars/blob/main/docs/provider-keys.md' in capsys.readouterr().out


def test_wizard_credentials_quote_and_reject_newline(tmp_path, monkeypatch, capsys):
    from openbars.config.loader import parse_env_file
    values = iter(['bad\nvalue', 'a"b', 'second'])
    monkeypatch.setattr('sys.stdin', Terminal('us\n1d\nalpaca\n\n'))
    monkeypatch.setattr('getpass.getpass', lambda prompt: next(values))
    assert main(['init', str(tmp_path)]) == 0
    assert parse_env_file(tmp_path / '.env')['ALPACA_API_KEY_ID'] == 'a"b'
    assert 'bad\nvalue' not in (tmp_path / '.env').read_text()
    assert 'a"b' not in capsys.readouterr().out


def test_wizard_multiple_market_symbols_load(tmp_path, monkeypatch):
    from openbars.config.loader import load_settings
    from openbars.core.models import Market
    monkeypatch.setattr("sys.stdin", Terminal("cn,hk,crypto\n1d\nbaostock,yfinance,binance\nsh.600000\n00700\nTESTUSDT\n"))
    assert main(["init", str(tmp_path)]) == 0
    settings = load_settings(tmp_path / "config.yaml", env={})
    assert settings.universe[Market.CN] == ("sh.600000",)
    assert settings.universe[Market.HK] == ("00700",)
    assert settings.universe[Market.CRYPTO] == ("TESTUSDT",)


def test_wizard_reprompts_when_non_us_market_has_no_interval(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", Terminal("us,crypto\n1h\n1d,1h\nyfinance,binance\n\n\n"))
    assert main(["init", str(tmp_path)]) == 0
    assert "Non-US markets require 1d" in capsys.readouterr().out
