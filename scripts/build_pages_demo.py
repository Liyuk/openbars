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
import json
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


BANNER = """\
<div class="demo-banner" role="note">
  <p><b data-i18n="demo.banner">Demo with synthetic data</b>
  <span data-i18n="demo.note">Synthetic TEST* symbols only. This is not a hosted OpenBars service and it reads no live data.</span></p>
  <p><a href="{docs}" rel="external noopener"><span data-i18n="demo.cta">Run your own self-hosted OpenBars</span> &rsaquo;</a></p>
</div>
"""

DEMO_SCRIPTS = """\
<script>window.OPENBARS_STATIC = true;</script>
<script src="sample/demo-data.js"></script>
<script src="assets/static-api.js"></script>
"""


def _render_html(source: str, *, subpage: bool) -> str:
    html = source.replace('href="/assets/', 'href="assets/').replace('src="/assets/', 'src="assets/')
    # Static copies keep the shell but point at the checked-in sample.
    html = html.replace('href="/chart"', 'href="chart/"').replace('href="/flags"', 'href="flags/"')
    html = html.replace('href="/"', 'href="./"')
    if subpage:
        html = html.replace("<head>", '<head>\n  <base href="../">', 1)
    html = html.replace("<body>\n", "<body>\n" + BANNER.format(docs=SELF_HOSTED_DOCS), 1)
    html = html.replace(
        '<script src="assets/app.js"></script>',
        DEMO_SCRIPTS + '<script src="assets/app.js"></script>',
        1,
    )
    assert "OPENBARS_STATIC = true" in html, "static marker missing from the built shell"
    assert "Demo with synthetic data" in html, "banner missing from the built shell"
    return html


def _demo_data_js(sample: dict[str, Any]) -> str:
    payload = json.dumps(sample, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return "window.OPENBARS_DEMO_DATA = " + payload + ";\n"


def build_site(out: Path, sample_dir: Path) -> None:
    sample = load_sample(sample_dir)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    # The vendored chart library and the shared stylesheet/script ship unchanged.
    shutil.copytree(STATIC_DIR / "assets", out / "assets")
    shim = SHIM_FILE.read_text(encoding="utf-8")
    (out / "assets" / "static-api.js").write_text(shim, encoding="utf-8")

    shell = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    pages = {"index.html": False, "chart/index.html": True, "flags/index.html": True}
    for relative, subpage in pages.items():
        target = out / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_render_html(shell, subpage=subpage), encoding="utf-8")

    (out / "sample").mkdir(parents=True, exist_ok=True)
    (out / "sample" / "demo-data.js").write_text(_demo_data_js(sample), encoding="utf-8")
    for name in sample["files"]:
        shutil.copyfile(sample_dir / name, out / "sample" / name)
    shutil.copyfile(sample_dir / "manifest.json", out / "sample" / "manifest.json")


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
