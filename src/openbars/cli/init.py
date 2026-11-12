"""``openbars init [directory]``: write ``config.yaml`` and ``.env`` from the shipped examples.

Templates are located, in order:

1. ``openbars/_templates/`` inside the installed package (the wheel force-includes
   ``config.example.yaml`` and ``.env.example`` there, see ``pyproject.toml``);
2. the repository root, three levels above this module (``src/openbars/cli/init.py``),
   which is what a git checkout / editable install uses.

``init`` never overwrites: if *either* target already exists nothing is written.
"""

from __future__ import annotations

import os
import getpass
import sys
import warnings
from pathlib import Path
from typing import Protocol

import yaml

CONFIG_TEMPLATE = "config.example.yaml"
ENV_TEMPLATE = ".env.example"
CONFIG_TARGET = "config.yaml"
ENV_TARGET = ".env"


class _Writer(Protocol):
    def line(self, text: str = "") -> None: ...

    def err(self, text: str) -> None: ...


def template_dirs() -> list[Path]:
    package = Path(__file__).resolve().parent.parent
    return [package / "_templates", package.parent.parent]


def find_template(name: str) -> Path | None:
    for directory in template_dirs():
        candidate = directory / name
        if candidate.is_file():
            return candidate
    return None


def _choose(prompt: str, options: set[str], default: str) -> list[str]:
    while True:
        answer = input(f"{prompt} [{default}]: ").strip().lower() or default
        selected = list(dict.fromkeys(part.strip() for part in answer.split(",")))
        if selected and all(item in options for item in selected):
            return selected
        print("选择无效，请重试 / Invalid selection, try again.")


def _wizard(config_src: Path, out: _Writer) -> tuple[bytes, bytes]:
    template = config_src.read_text()
    root = yaml.compose(template)
    sections = {key.value: value for key, value in root.value}
    markets = _choose("市场 / Markets (us, crypto, cn, hk; comma-separated / 逗号分隔)",
                      {"us", "crypto", "cn", "hk"}, "us")
    while True:
        intervals = _choose("粒度 / Intervals (1d, 1h; 1h US only / 小时线仅美股)",
                            {"1d", "1h"} if "us" in markets else {"1d"}, "1d")
        if "1d" in intervals or all(market == "us" for market in markets):
            break
        out.line("非美股市场需要日线 / Non-US markets require 1d; 请重选 / choose again.")
    if "1h" in intervals:
        out.line("提示 / Note: 1h runs need a version with #117/#118 / 小时线运行需要包含 #117/#118 的版本。")
    available = set()
    for market, names in {"us": {"alpaca", "massive", "yfinance"}, "hk": {"yfinance"},
                          "crypto": {"binance", "coinbase"}, "cn": {"baostock"}}.items():
        if market in markets:
            available.update(names)
    if "us" in markets and "1h" in intervals:
        available.add("twelve_data")
    choices = ",".join(sorted(available))
    coverage = {
        ("us", "1d"): {"alpaca", "massive", "yfinance"},
        ("us", "1h"): {"alpaca", "massive", "yfinance", "twelve_data"},
        ("hk", "1d"): {"yfinance"}, ("cn", "1d"): {"baostock"},
        ("crypto", "1d"): {"binance", "coinbase"},
    }
    while True:
        providers = _choose(f"数据源 / Providers ({choices}; comma-separated / 逗号分隔)", available, choices)
        missing = [f"{market}/{interval}" for market in markets for interval in intervals
                   if (market, interval) in coverage and not coverage[market, interval].intersection(providers)]
        if not missing:
            break
        out.line(f"数据源未覆盖 / Providers do not cover: {', '.join(missing)}; 请重选 / choose again.")

    edits = []
    for section, value in (("markets", markets), ("intervals", intervals)):
        node = sections[section]
        edits.append((node.start_mark.index, node.end_mark.index, yaml.safe_dump(value, default_flow_style=True).strip()))
    for key, body in sections["providers"].value:
        enabled = next(value for field, value in body.value if field.value == "enabled")
        edits.append((enabled.start_mark.index, enabled.end_mark.index, str(key.value in providers).lower()))
    universe = {key.value: value for key, value in sections["universe"].value}
    additions = []
    for market in markets:
        symbols = [part.strip() for part in input(f"{market} 代码 / symbols (逗号分隔，可留空 / comma-separated, optional): ").split(",") if part.strip()]
        if not symbols:
            out.line(f"请在 config.yaml 的 universe.{market}.symbols 填写代码 / Add symbols to universe.{market}.symbols in config.yaml.")
            continue
        rendered = yaml.safe_dump(symbols, default_flow_style=True, allow_unicode=True, width=1000000).strip()
        if market in universe:
            node = universe[market]
            index = node.value[0][0].start_mark.index
            edits.append((index, index, f"symbols: {rendered}\n    "))
        else:
            additions.append(f"  {market}:\n    symbols: {rendered}\n")
    if additions:
        index = next(key.start_mark.index for key, _ in root.value if key.value == "storage")
        edits.append((index, index, "".join(additions) + "\n"))
    for start, end, replacement in sorted(edits, reverse=True):
        template = template[:start] + replacement + template[end:]
    keys = {"alpaca": ("ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"), "massive": ("MASSIVE_API_KEY",),
            "twelve_data": ("TWELVE_DATA_API_KEY",)}
    lines = ["# Private credentials / 私有凭据；不要提交"]
    for provider in providers:
        for name in keys.get(provider, ()):
            while True:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", getpass.GetPassWarning)
                    value = getpass.getpass(f"{name} (可留空 / optional): ")
                if not any(c in value for c in "\r\n\x00") and "'" not in value:
                    break
                out.line("密钥含不支持的字符，请重试 / Unsupported character in credential; try again.")
            lines.append(f"{name}='{value}'")
    return template.encode(), ("\n".join(lines) + "\n").encode()


def init_project(directory: str | os.PathLike[str], out: _Writer, *, exit_usage: int = 2, yes: bool = False) -> int:
    """Create ``<directory>/config.yaml`` and ``<directory>/.env``. Returns an exit code."""
    target = Path(directory).expanduser()
    config_src, env_src = find_template(CONFIG_TEMPLATE), find_template(ENV_TEMPLATE)
    if config_src is None or env_src is None:
        missing = [n for n, p in ((CONFIG_TEMPLATE, config_src), (ENV_TEMPLATE, env_src)) if p is None]
        out.err(f"error: cannot find template file(s): {', '.join(missing)}")
        return 1
    if target.exists() and not target.is_dir():
        out.err(f"error: {target} exists and is not a directory")
        return exit_usage
    config_dst, env_dst = target / CONFIG_TARGET, target / ENV_TARGET
    existing = [p for p in (config_dst, env_dst) if os.path.lexists(p)]
    if existing:
        names = ", ".join(str(p) for p in existing)
        out.err(f"error: {names} already exists; init never overwrites. Nothing was written.")
        return exit_usage

    try:
        contents = _wizard(config_src, out) if sys.stdin.isatty() and not yes else (config_src.read_bytes(), env_src.read_bytes())
    except (EOFError, KeyboardInterrupt, getpass.GetPassWarning):
        out.err("error: 初始化已取消或无法安全读取密钥 / cancelled or secure input unavailable; nothing written.")
        return exit_usage
    target.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    try:
        for content, dst, mode in ((contents[0], config_dst, 0o644), (contents[1], env_dst, 0o600)):
            with os.fdopen(os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode), "wb") as handle:
                written.append(dst)
                handle.write(content)
            os.chmod(dst, mode)
    except FileExistsError as exc:
        for path in written:  # roll back a half-init
            if path != Path(exc.filename):
                path.unlink(missing_ok=True)
        out.err(f"error: {exc.filename} already exists; init never overwrites.")
        return exit_usage

    out.line(f"created {config_dst}")
    out.line(f"created {env_dst}  (git-ignored; keep it private)")
    out.line("下一步 / Next steps:")
    prefix = "" if target == Path(".") else f"cd {target} && "
    out.line("  1. 编辑 .env 填写密钥 / edit .env for keys (or use OPENBARS_ENV_FILE / --env-file)")
    out.line("     .env next to config.yaml is loaded automatically / 自动加载同目录的 .env")
    out.line(f"  2. {prefix}openbars config   # 生效配置 / effective settings; secrets: set/missing")
    out.line("  3. openbars plan              # 预览任务和预算 / dry run: jobs and budget")
    out.line("  4. openbars run               # 获取数据，可重复运行 / fetch; safe to re-run")
    out.line("  密钥申请指南 / Key guide: https://github.com/Liyuk/openbars/blob/main/docs/provider-keys.md")
    return 0
