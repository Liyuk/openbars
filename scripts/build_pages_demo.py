#!/usr/bin/env python3
"""Build the static GitHub Pages demo of the ``openbars serve`` UI.

The demo publishes the real read-only frontend with a tiny, checked-in synthetic
sample (``TEST*`` symbols, fictional ``example`` provider). It never contacts a
backend or an external data source: the browser reads a single JavaScript bundle
generated from ``demo/sample/*.json``.

Two modes:

``--write-sample``
    Regenerate ``demo/sample`` by seeding a throwaway Parquet/DuckDB store with the
    synthetic sample and exporting the exact ``/api/v1`` responses with the FastAPI
    test client, so the JSON shapes are the same ones ``openbars serve`` returns.
    Needs the ``http`` (or ``dev``) extra: ``uv run --extra http python ...``.

default
    Assemble a deployable static site from ``src/openbars/web/static`` plus the
    checked-in ``demo/sample`` JSON. Needs only the standard library.

Examples::

    uv run --extra http python scripts/build_pages_demo.py --write-sample
    python scripts/build_pages_demo.py --out _site
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import re
import shutil
import sys
import tempfile
from datetime import UTC, date, datetime
from decimal import Decimal as D
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode

ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = ROOT / "src" / "openbars" / "web" / "static"
SAMPLE_DIR = ROOT / "demo" / "sample"
SHIM_FILE = ROOT / "demo" / "static" / "static-api.js"
SELF_HOSTED_DOCS = "https://github.com/Liyuk/openbars#local-service"

# Synthetic sessions shared by every demo instrument (UTC).
DAYS = [date(2026, 11, 6), date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 11), date(2026, 11, 12)]
GAP_DAY = date(2026, 11, 10)  # TEST2 intentionally has no row here


class Demo:
    """One synthetic instrument and the closes/flags of its sample sessions."""

    def __init__(
        self,
        symbol: str,
        instrument_id: str,
        market: str,
        asset_class: str,
        currency: str,
        name: str,
        exchange: str | None,
        closes: list[str],
        *,
        skip: set[date] | None = None,
        flags: dict[date, tuple[str, ...]] | None = None,
        volumes: dict[date, str] | None = None,
        default_volume: str,
    ) -> None:
        self.symbol = symbol
        self.instrument_id = instrument_id
        self.market = market
        self.asset_class = asset_class
        self.currency = currency
        self.name = name
        self.exchange = exchange
        self.closes = [D(value) for value in closes]
        self.skip = skip or set()
        self.flags = flags or {}
        self.volumes = volumes or {}
        self.default_volume = D(default_volume)

    def rows(self) -> list[tuple[date, D, str, tuple[str, ...], D]]:
        """``(session, close, freshness, flags, volume)`` for stored sessions."""
        out = []
        for index, day in enumerate(DAYS):
            if day in self.skip:
                continue
            freshness = "provisional" if day == DAYS[-1] else "final"
            volume = self.volumes.get(day, self.default_volume)
            out.append((day, self.closes[index], freshness, self.flags.get(day, ()), volume))
        return out


DEMOS = [
    Demo(
        "TEST1",
        "us:eq:xnys:TEST1",
        "us",
        "equity",
        "USD",
        "Test One",
        "XNYS",
        ["101.20", "102.00", "101.50", "103.85", "103.10"],
        flags={
            date(2026, 11, 9): ("DIVERGENT",),
            date(2026, 11, 11): ("SPIKE_SUSPECT",),
            date(2026, 11, 12): ("VOLUME_MISSING",),
        },
        volumes={date(2026, 11, 12): D("0")},
        default_volume="1204331",
    ),
    Demo(
        "TEST2",
        "us:eq:xnys:TEST2",
        "us",
        "equity",
        "USD",
        "Test Two",
        "XNYS",
        ["50", "51", "52", "53", "54"],
        skip={GAP_DAY},
        default_volume="640210",
    ),
    Demo(
        "TESTA",
        "cn:eq:xshg:TESTA",
        "cn",
        "equity",
        "CNY",
        "Test A",
        "XSHG",
        ["10", "11", "12", "13", "14"],
        default_volume="812400",
    ),
    Demo(
        "TESTH",
        "hk:eq:xhk:TESTH",
        "hk",
        "equity",
        "HKD",
        "Test H",
        "XHKG",
        ["80", "81", "82", "83", "84"],
        default_volume="501220",
    ),
    Demo(
        "TESTBTC",
        "crypto:spot:TESTBTC",
        "crypto",
        "crypto_spot",
        "USD",
        "Test BTC",
        None,
        ["10000", "10020", "10040", "10060", "10080"],
        default_volume="210",
    ),
]

# Provider names are fictional; no real account, key or ticker appears here.
PROVIDER = "example"


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    path.write_text(text + "\n", encoding="utf-8")


def _canonical_row(demo: Demo, day: date, close: D, freshness: str, flags: tuple[str, ...], volume: D) -> Any:
    from openbars.core.models import Interval, Market
    from openbars.reconcile import policy_version_for_market
    from openbars.storage.rows import CanonicalBarRow

    return CanonicalBarRow(
        instrument_id=demo.instrument_id,
        market=Market(demo.market),
        interval=Interval.D1,
        ts_start=datetime(day.year, day.month, day.day, 21, tzinfo=UTC),
        session_date=day,
        session="regular",
        provider=PROVIDER,
        open=close - D("0.50"),
        high=close + D("0.90"),
        low=close - D("1.10"),
        close=close,
        volume=volume,
        price_source=PROVIDER,
        reconcile_rule="primary_wins",
        policy_version=policy_version_for_market(demo.market),
        # CN / HK canonical series are stored unadjusted (cn-daily-v1 / hk-daily-v1).
        adjustment="raw" if demo.market in {"cn", "hk"} else "split",
        flags=flags,
        generated_at=datetime(day.year, day.month, day.day, 21, tzinfo=UTC),
        available_at=datetime(day.year, day.month, day.day, 22, tzinfo=UTC),
        freshness=freshness,
    )


class _DemoQuota:
    """Fictional quota snapshot; no keys and no account identifiers."""

    def snapshot(self) -> dict[str, Any]:
        return {
            PROVIDER: {
                "used": {"day": 12, "minute": 3},
                "remaining": {"day": 88, "minute": 57},
                "penalized_until": None,
            }
        }


def _seed_store(root: Path) -> Any:
    from openbars.core.models import (
        AssetClass,
        CorporateAction,
        CorporateActionKind,
        Instrument,
        Market,
        SymbolMapping,
    )
    from openbars.storage.parquet_duckdb import open_backend

    backend = open_backend(root)
    instruments = []
    mappings = []
    rows = []
    for demo in DEMOS:
        instruments.append(
            Instrument(
                demo.instrument_id,
                Market(demo.market),
                AssetClass(demo.asset_class),
                demo.currency,
                name=demo.name,
                exchange=demo.exchange,
            )
        )
        mappings.append(SymbolMapping(demo.instrument_id, demo.symbol, date(2020, 1, 1)))
        rows.extend(_canonical_row(demo, *row) for row in demo.rows())
    backend.instruments.upsert(instruments, mappings)
    # hk-daily-v1 stores raw bars and needs at least one recorded action to derive the
    # default split view; a no-op split before the sample window keeps prices untouched.
    backend.instruments.upsert_corporate_actions(
        [
            CorporateAction(
                "hk:eq:xhk:TESTH",
                date(2019, 1, 2),
                CorporateActionKind.SPLIT,
                PROVIDER,
                ratio=D("1"),
            )
        ]
    )
    backend.canonical.upsert_rows(rows)
    return backend


def _normalize_request(url: str) -> str:
    """Drop volatile window parameters and sort the query for a stable key."""
    path, _, query = url.partition("?")
    params = sorted(parse_qsl(query, keep_blank_values=True))
    params = [(name, value) for name, value in params if name not in {"start", "end"}]
    return path + ("?" + urlencode(params) if params else "")


def _file_name(key: str) -> str:
    path, _, query = key.partition("?")
    parts = [path.rsplit("/", 1)[-1] or "root"]
    parts += [f"{name}-{value}" for name, value in sorted(parse_qsl(query, keep_blank_values=True))]
    safe = []
    for char in "-".join(parts).lower():
        if char.isalnum():
            safe.append(char)
        elif not safe or safe[-1] != "-":
            safe.append("-")
    name = "".join(safe).strip("-")
    return (name or "response") + ".json"


def build_sample(sample_dir: Path) -> dict[str, Any]:
    """Seed a throwaway store, export real API responses, write ``sample_dir``."""
    from fastapi.testclient import TestClient

    from openbars.api.http import create_app

    with tempfile.TemporaryDirectory(prefix="openbars-pages-") as tmp:
        backend = _seed_store(Path(tmp) / "data")
        client = TestClient(create_app(backend=backend, data_dir=backend.root, quota_gate=_DemoQuota()))

        requests: list[str] = []
        requests += [f"/api/v1/markets?interval={interval}" for interval in ("1d", "1h")]
        requests += [f"/api/v1/flags?limit=1&interval={interval}" for interval in ("1d", "1h")]
        requests += [f"/api/v1/flags?interval={interval}&limit=200&offset=0" for interval in ("1d", "1h")]
        requests.append("/api/v1/quota")
        for demo in DEMOS:
            for interval in ("1d", "1h"):
                window = "" if interval == "1d" else f"&start={DAYS[0]}&end={DAYS[-1]}"
                requests.append(f"/api/v1/bars?interval={interval}&source=canonical&symbol={demo.symbol}{window}")
                requests.append(f"/api/v1/bars?id={demo.instrument_id}&interval={interval}&source=canonical{window}")

        responses: dict[str, str] = {}
        written: dict[str, Any] = {}
        for request in requests:
            response = client.get(request)
            assert response.status_code == 200, (request, response.status_code, response.text)
            payload = response.json()
            key = _normalize_request(request)
            name = _file_name(key)
            responses[key] = name
            written[name] = payload

        # One flags request per flag so the register's chips work with the sample.
        for interval in ("1d", "1h"):
            counts = written[responses[f"/api/v1/flags?interval={interval}&limit=1"]]["counts"]
            for flag, count in sorted(counts.items()):
                if not count:
                    continue
                request = f"/api/v1/flags?flag={flag}&interval={interval}&limit=200&offset=0"
                response = client.get(request)
                assert response.status_code == 200, (request, response.status_code, response.text)
                key = _normalize_request(request)
                responses[key] = _file_name(key)
                written[responses[key]] = response.json()

        unknown = client.get("/api/v1/bars?symbol=NOSUCH&interval=1d&source=canonical")
        assert unknown.status_code == 400, unknown.text

    manifest = {
        "note": (
            "Synthetic TEST* sample for the static GitHub Pages demo. "
            "No real market data, no real provider, no account identifiers."
        ),
        "generated_by": "scripts/build_pages_demo.py --write-sample",
        "self_hosted_docs": SELF_HOSTED_DOCS,
        "responses": dict(sorted(responses.items())),
        "errors": {
            # Same {"detail": ...} error shape the read API returns for these requests.
            "unknown_symbol": "unknown symbol '{symbol}' is not part of this synthetic sample",
            "unknown_source": "source '{source}' is not part of this synthetic sample; only canonical bars are published",
        },
    }

    if sample_dir.exists():
        shutil.rmtree(sample_dir)
    sample_dir.mkdir(parents=True)
    for name, payload in written.items():
        _json_dump(sample_dir / name, payload)
    _json_dump(sample_dir / "manifest.json", manifest)
    return manifest


def load_sample(sample_dir: Path) -> dict[str, Any]:
    manifest = json.loads((sample_dir / "manifest.json").read_text(encoding="utf-8"))
    files = {}
    for name in sorted(set(manifest["responses"].values())):
        files[name] = json.loads((sample_dir / name).read_text(encoding="utf-8"))
    return {"manifest": manifest, "files": files}


REPO_URL = "https://github.com/Liyuk/openbars"
SITE_URL = "https://liyuk.github.io/openbars/"
DOC_URLS = {
    "data_sources_en": f"{REPO_URL}/blob/main/DATA_SOURCES.md",
    "data_sources_zh": f"{REPO_URL}/blob/main/DATA_SOURCES.zh-CN.md",
    "architecture_en": f"{REPO_URL}/blob/main/docs/ARCHITECTURE.md",
    "architecture_zh": f"{REPO_URL}/blob/main/docs/ARCHITECTURE.zh-CN.md",
    "api": f"{REPO_URL}/blob/main/docs/api.md",
    "provider_keys": f"{REPO_URL}/blob/main/docs/provider-keys.md",
}

# The logo is docs/images/logo/logo.svg; the demo uses it for the
# header, the footer and the favicon. Changing it means replacing that file and rerunning
# scripts/render_brand_assets.py.
LOGO_FILE = ROOT / "docs" / "images" / "logo" / "logo.svg"
SOCIAL_FILE = ROOT / "docs" / "images" / "social-preview.png"
TOUCH_ICON_FILE = ROOT / "demo" / "static" / "apple-touch-icon.png"
CSS_FILE = ROOT / "demo" / "static" / "demo.css"
CONTENT_FILE = ROOT / "demo" / "static" / "content.json"

# Octicons "mark-github" (MIT licensed), drawn on a 16x16 grid.
GITHUB_MARK = (
    '<svg viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 '
    "7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15"
    ".08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 "
    "1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83"
    "-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 "
    '1 0 8c0-4.42 3.58-8 8-8Z"/></svg>'
)

# page key -> (output file, is a subdirectory page)
PAGES = {
    "dash": ("index.html", False),
    "chart": ("chart/index.html", True),
    "flags": ("flags/index.html", True),
    "data-sources": ("data-sources/index.html", True),
}
TITLES = {
    "dash": "OpenBars demo · Dashboard (synthetic data)",
    "chart": "OpenBars demo · Chart (synthetic data)",
    "flags": "OpenBars demo · Quality flags (synthetic data)",
    "data-sources": "OpenBars demo · Data sources",
}
DESCRIPTION = (
    "A static demo of the OpenBars read-only UI with synthetic TEST* data. "
    "OpenBars is a self-hosted OHLCV bar collection, cleaning and reconciliation framework; "
    "this site is not a hosted service and fetches no live data."
)


def load_content() -> dict[str, Any]:
    content = json.loads(CONTENT_FILE.read_text(encoding="utf-8"))
    en, zh = content["strings"]["en"], content["strings"]["zh"]
    assert set(en) == set(zh), f"content.json en/zh keys differ: {sorted(set(en) ^ set(zh))}"
    return content


def _t(content: dict[str, Any], key: str) -> str:
    """Escaped English default text for ``key`` (the page script swaps in Chinese)."""
    return html_lib.escape(content["strings"]["en"][key], quote=False)


def _i(content: dict[str, Any], key: str, tag: str = "span", **attrs: str) -> str:
    """``<tag data-i18n=key>English text</tag>``; ``cls`` sets the class attribute."""
    extra = "".join(f' {"class" if name == "cls" else name}="{value}"' for name, value in attrs.items())
    return f'<{tag}{extra} data-i18n="{key}">{_t(content, key)}</{tag}>'


def _banner(content: dict[str, Any], prefix: str) -> str:
    return (
        '<div class="demo-banner" role="note">\n'
        '  <p><b data-i18n="demo.banner">Demo with synthetic data</b>\n'
        '  <span data-i18n="demo.note">Synthetic TEST* symbols only. This is not a hosted OpenBars service and it reads no live data.</span></p>\n'
        '  <p class="dm-links">'
        f'<a href="{SELF_HOSTED_DOCS}" rel="external noopener"><span data-i18n="demo.cta">Run your own self-hosted OpenBars</span> &rsaquo;</a>'
        f'<a href="{prefix}data-sources/" data-page="data-sources"><span data-i18n="demo.sources_cta">Data sources</span> &rsaquo;</a></p>\n'
        "</div>\n"
    )


def _header(content: dict[str, Any], prefix: str) -> str:
    return f"""<header class="mast dm-head">
  <div class="mast-in">
    <a class="word" href="{prefix or './'}"><img src="{prefix}assets/logo.svg" width="34" height="34" alt=""><span data-i18n="brand">OpenBars</span></a>
    <nav class="tabs" aria-label="Pages">
      <a href="{prefix or './'}" data-p="dash" data-i18n="nav.dashboard">Dashboard</a>
      <a href="{prefix}chart/" data-p="chart" data-i18n="nav.chart">Chart</a>
      <a href="{prefix}flags/" data-p="flags" data-i18n="nav.flags">Flags</a>
      <a href="{prefix}data-sources/" data-p="data-sources" data-i18n="nav.sources">Data sources</a>
    </nav>
    <span class="dm-pill"><i aria-hidden="true"></i>{_i(content, "demo.pill")}</span>
    <button class="lang" type="button" aria-label="Switch language" id="lang-toggle">中文</button>
    <a class="dm-gh" href="{REPO_URL}" rel="noopener">{GITHUB_MARK}<span class="sr" data-i18n="demo.github">{_t(content, "demo.github")}</span></a>
  </div>
</header>"""


def _tour(content: dict[str, Any]) -> str:
    def step(key: str, link: tuple[str, str] | None = None) -> str:
        anchor = f'\n        <a href="#" data-page="{link[0]}" data-i18n="{link[1]}">{_t(content, link[1])}</a>' if link else ""
        return f'      <li><div>\n        <p data-i18n="{key}">{_t(content, key)}</p>{anchor}\n      </div></li>\n'

    return (
        '    <section class="dm-tour" aria-labelledby="tour-h">\n'
        f'      {_i(content, "tour.title", "h2", id="tour-h")}\n'
        f'      {_i(content, "tour.lede", "p")}\n'
        "      <ol>\n"
        + step("tour.step1")
        + step("tour.step2", ("chart", "tour.open_chart"))
        + step("tour.step3", ("flags", "tour.open_flags"))
        + step("tour.step4", ("data-sources", "tour.open_sources"))
        + "      </ol>\n    </section>\n"
    )


def _tier_table(content: dict[str, Any]) -> str:
    rows = "".join(
        f"      <tr><th scope=\"row\" data-i18n=\"sources.tier_{m}\">{_t(content, f'sources.tier_{m}')}</th>"
        f"<td data-i18n=\"sources.tier_{m}_prov\">{_t(content, f'sources.tier_{m}_prov')}</td>"
        f"<td data-i18n=\"sources.tier_{m}_final\">{_t(content, f'sources.tier_{m}_final')}</td></tr>\n"
        for m in ("us", "crypto", "cn", "hk")
    )
    return (
        '    <table class="reg dm-tiers">\n'
        f'      <caption class="sr" data-i18n="sources.sameday_title">{_t(content, "sources.sameday_title")}</caption>\n'
        "      <thead><tr>"
        f'<th scope="col" data-i18n="sources.tier_market">{_t(content, "sources.tier_market")}</th>'
        f'<th scope="col" data-i18n="sources.tier_provisional">{_t(content, "sources.tier_provisional")}</th>'
        f'<th scope="col" data-i18n="sources.tier_final">{_t(content, "sources.tier_final")}</th></tr></thead>\n'
        f"      <tbody>\n{rows}      </tbody>\n    </table>\n"
    )


def _source_row(content: dict[str, Any], src: dict[str, str]) -> str:
    key = src["key"]
    status = src["status"]

    def cell(column: str, field: str) -> str:
        return (
            f'<td><span class="dm-lab" data-i18n="sources.col_{column}">{_t(content, f"sources.col_{column}")}</span>'
            f'<span data-i18n="src.{key}.{field}">{_t(content, f"src.{key}.{field}")}</span></td>'
        )

    name = html_lib.escape(src["name"], quote=False)
    return (
        "        <tr>\n"
        f'          <td class="dm-main mk"><span class="dm-name">{name}</span>'
        f'<span class="dm-status {status}" data-i18n="sources.status_{status}">{_t(content, f"sources.status_{status}")}</span> '
        f'<a class="dm-terms" href="{html_lib.escape(src["terms"])}" rel="noopener external" data-i18n="sources.terms_link">{_t(content, "sources.terms_link")}</a></td>\n'
        f'          <td class="dm-int"><span class="dm-lab" data-i18n="sources.col_intervals">{_t(content, "sources.col_intervals")}</span>'
        f'<span>{html_lib.escape(src["intervals"])}</span></td>\n'
        f"          {cell('access', 'access')}\n"
        f"          {cell('sameday', 'sameday')}\n"
        f"          {cell('notes', 'notes')}\n"
        "        </tr>\n"
    )


def _sources_page(content: dict[str, Any]) -> str:
    sections = []
    for group in content["groups"]:
        rows = "".join(_source_row(content, s) for s in content["sources"] if s["group"] == group["id"])
        heading = f"src-{group['id']}"
        sections.append(
            f'    <section class="dm-sec" aria-labelledby="{heading}">\n'
            f"      {_i(content, group['title'], 'h2', id=heading)}\n"
            '      <table class="reg dm-src">\n'
            f'        <caption class="sr" data-i18n="{group["title"]}">{_t(content, group["title"])}</caption>\n'
            "        <thead><tr>"
            + "".join(
                f'<th scope="col" data-i18n="sources.col_{c}">{_t(content, f"sources.col_{c}")}</th>'
                for c in ("source", "intervals", "access", "sameday", "notes")
            )
            + "</tr></thead>\n"
            f"        <tbody>\n{rows}        </tbody>\n      </table>\n    </section>\n"
        )
    return (
        '  <div id="p-data-sources" hidden>\n'
        '    <div class="head"><div>'
        f'{_i(content, "sources.title", "h1")}{_i(content, "sources.lede", "p", cls="lede")}'
        "</div></div>\n"
        '    <aside class="dm-note" aria-labelledby="dsn-h">\n'
        f'      {_i(content, "sources.notice_title", "h2", id="dsn-h")}\n'
        f'      {_i(content, "sources.notice", "p")}\n'
        "    </aside>\n"
        '    <section class="dm-sec" aria-labelledby="ds-same">\n'
        f'      {_i(content, "sources.sameday_title", "h2", id="ds-same")}\n'
        f'      {_i(content, "sources.sameday", "p")}\n'
        f"{_tier_table(content)}"
        "    </section>\n"
        + "".join(sections)
        + '    <section class="dm-sec" aria-labelledby="ds-not">\n'
        f'      {_i(content, "sources.not_title", "h2", id="ds-not")}\n'
        f'      {_i(content, "sources.not", "p")}\n'
        "    </section>\n"
        '    <section class="dm-sec" aria-labelledby="ds-terms">\n'
        f'      {_i(content, "sources.terms_title", "h2", id="ds-terms")}\n'
        f'      {_i(content, "sources.terms", "p")}\n'
        '      <div class="dm-links-row">\n'
        f'        {_i(content, "sources.full_doc_label")}\n'
        f'        <a href="{DOC_URLS["data_sources_en"]}" lang="en" rel="noopener">DATA_SOURCES.md</a>\n'
        f'        <a href="{DOC_URLS["data_sources_zh"]}" lang="zh-Hans" rel="noopener">DATA_SOURCES.zh-CN.md</a>\n'
        f'        <a href="{DOC_URLS["provider_keys"]}" rel="noopener" data-i18n="sources.keys_doc">{_t(content, "sources.keys_doc")}</a>\n'
        "      </div>\n"
        "    </section>\n"
        "  </div>\n"
    )


def _footer(content: dict[str, Any], prefix: str) -> str:
    return f"""<footer>
  <div class="dm-foot">
    <div>
      <div class="dm-brand"><img src="{prefix}assets/logo.svg" width="28" height="28" alt=""><span>OpenBars</span></div>
      {_i(content, "foot.demo", "p")}
      {_i(content, "foot.license", "p")}
    </div>
    <nav aria-label="Project links">
      <a href="{REPO_URL}" rel="noopener" data-i18n="foot.github">{_t(content, "foot.github")}</a>
      <a href="{SELF_HOSTED_DOCS}" rel="noopener" data-i18n="foot.selfhost">{_t(content, "foot.selfhost")}</a>
      <a href="{DOC_URLS["data_sources_en"]}" lang="en" rel="noopener">Data sources (English)</a>
      <a href="{DOC_URLS["data_sources_zh"]}" lang="zh-Hans" rel="noopener">数据源（中文）</a>
      <a href="{DOC_URLS["architecture_en"]}" lang="en" rel="noopener">Architecture (English)</a>
      <a href="{DOC_URLS["architecture_zh"]}" lang="zh-Hans" rel="noopener">架构总览（中文）</a>
      <a href="{DOC_URLS["api"]}" rel="noopener" data-i18n="foot.api">{_t(content, "foot.api")}</a>
    </nav>
  </div>
</footer>"""


def _head_extras(page: str, prefix: str) -> str:
    title = html_lib.escape(TITLES[page])
    description = html_lib.escape(DESCRIPTION, quote=True)
    image = SITE_URL + "assets/social-preview.png"
    return (
        f'  <meta name="description" content="{description}">\n'
        '  <meta name="theme-color" content="#0f766e">\n'
        f'  <link rel="icon" type="image/svg+xml" href="{prefix}assets/logo.svg">\n'
        f'  <link rel="apple-touch-icon" href="{prefix}assets/apple-touch-icon.png">\n'
        '  <meta property="og:type" content="website">\n'
        '  <meta property="og:site_name" content="OpenBars">\n'
        f'  <meta property="og:title" content="{title}">\n'
        f'  <meta property="og:description" content="{description}">\n'
        f'  <meta property="og:image" content="{image}">\n'
        '  <meta name="twitter:card" content="summary_large_image">\n'
    )


def _render_html(source: str, content: dict[str, Any], *, page: str, subpage: bool) -> str:
    prefix = "../" if subpage else ""
    html = re.sub(r"<!--.*?-->\s*", "", source, flags=re.S)  # drop the shell's design notes
    html = html.replace('href="/assets/', f'href="{prefix}assets/').replace('src="/assets/', f'src="{prefix}assets/')
    title = html_lib.escape(TITLES[page])
    html = re.sub(r"<title>.*?</title>", f"<title>{title}</title>", html, count=1)
    html = html.replace(
        f'<link rel="stylesheet" href="{prefix}assets/style.css">',
        _head_extras(page, prefix)
        + f'  <link rel="stylesheet" href="{prefix}assets/style.css">\n'
        + f'  <link rel="stylesheet" href="{prefix}assets/demo.css">',
        1,
    )
    html = re.sub(r'<header class="mast">.*?</header>', lambda _m: _header(content, prefix), html, count=1, flags=re.S)
    html = re.sub(r"<footer>.*?</footer>", lambda _m: _footer(content, prefix), html, count=1, flags=re.S)
    html = html.replace("<main>", '<main id="main" tabindex="-1">', 1)
    # The how-to-read panel closes the dashboard (the page's own h1 stays first).
    html, count = re.subn(r'(\n  </div>\s*)(<div id="p-chart")', lambda m: "\n" + _tour(content) + m.group(1) + m.group(2), html, count=1)
    assert count == 1, "dashboard end marker missing from the shell"
    html = html.replace("</main>", _sources_page(content) + "</main>", 1)
    html = html.replace(
        "<body>\n",
        '<body>\n<a class="dm-skip" href="#main" data-i18n="demo.skip">Skip to content</a>\n' + _banner(content, prefix),
        1,
    )
    scripts = (
        "<script>window.OPENBARS_STATIC = true;</script>\n"
        f'<script src="{prefix}sample/demo-data.js"></script>\n'
        f'<script src="{prefix}assets/static-api.js"></script>\n'
        f'<script src="{prefix}assets/demo-i18n.js"></script>\n'
    )
    html = html.replace(
        f'<script src="{prefix}assets/app.js"></script>', scripts + f'<script src="{prefix}assets/app.js"></script>', 1
    )
    assert "OPENBARS_STATIC = true" in html, "static marker missing from the built shell"
    assert "Demo with synthetic data" in html, "banner missing from the built shell"
    assert 'id="p-data-sources"' in html and 'id="main"' in html, "demo sections missing from the built shell"
    return html


def _demo_data_js(sample: dict[str, Any]) -> str:
    payload = json.dumps(sample, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return "window.OPENBARS_DEMO_DATA = " + payload + ";\n"


def _demo_i18n_js(content: dict[str, Any]) -> str:
    payload = json.dumps(content["strings"], sort_keys=True, ensure_ascii=False, indent=1)
    return "window.OPENBARS_EXTRA_I18N = " + payload + ";\n"


def build_site(out: Path, sample_dir: Path) -> None:
    sample = load_sample(sample_dir)
    content = load_content()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    # The vendored chart library and the shared script ship unchanged; the demo layers its own
    # stylesheet, strings, in-memory API and brand assets on top.
    shutil.copytree(STATIC_DIR / "assets", out / "assets")
    shutil.copyfile(SHIM_FILE, out / "assets" / "static-api.js")
    shutil.copyfile(CSS_FILE, out / "assets" / "demo.css")
    (out / "assets" / "demo-i18n.js").write_text(_demo_i18n_js(content), encoding="utf-8")
    shutil.copyfile(LOGO_FILE, out / "assets" / "logo.svg")
    shutil.copyfile(TOUCH_ICON_FILE, out / "assets" / "apple-touch-icon.png")
    shutil.copyfile(SOCIAL_FILE, out / "assets" / "social-preview.png")

    shell = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    for page, (relative, subpage) in PAGES.items():
        target = out / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_render_html(shell, content, page=page, subpage=subpage), encoding="utf-8")

    (out / "sample").mkdir(parents=True, exist_ok=True)
    (out / "sample" / "demo-data.js").write_text(_demo_data_js(sample), encoding="utf-8")
    for name in sample["files"]:
        shutil.copyfile(sample_dir / name, out / "sample" / name)
    shutil.copyfile(sample_dir / "manifest.json", out / "sample" / "manifest.json")
    (out / ".nojekyll").write_text("", encoding="utf-8")


def _check_sample(sample_dir: Path) -> None:
    """Fail when a checked-in sample file contains anything but synthetic TEST* data."""
    text = "".join(path.read_text(encoding="utf-8") for path in sorted(sample_dir.glob("*.json")))
    for token in ("TEST1", "TEST2", "TESTA", "TESTH", "TESTBTC"):
        assert token in text, f"sample missing {token}"
    assert PROVIDER in text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, help="write the static site to this directory")
    parser.add_argument(
        "--sample-dir",
        type=Path,
        default=SAMPLE_DIR,
        help=f"checked-in sample directory (default: {SAMPLE_DIR.relative_to(ROOT)})",
    )
    parser.add_argument(
        "--write-sample",
        action="store_true",
        help="regenerate the sample JSON from a synthetic store (needs the http extra)",
    )
    args = parser.parse_args(argv)

    sample_dir = args.sample_dir
    if args.write_sample:
        build_sample(sample_dir)
        print(f"wrote synthetic sample: {sample_dir}")
    if not sample_dir.is_dir():
        parser.error(f"sample directory not found: {sample_dir} (run with --write-sample)")
    _check_sample(sample_dir)
    if args.out is not None:
        build_site(args.out, sample_dir)
        print(f"wrote static demo site: {args.out}")
    elif not args.write_sample:
        parser.error("nothing to do: pass --out and/or --write-sample")
    return 0


if __name__ == "__main__":
    sys.exit(main())
