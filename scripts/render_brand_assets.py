#!/usr/bin/env python3
"""Render the PNG brand assets from the checked-in SVG logos.

Inputs (checked in): ``docs/images/logo/logo.svg``.

Outputs (checked in; rerun after changing the logo):

* ``docs/images/logo/previews/logo-{light,dark}.png``: 512x512 previews
* ``docs/images/social-preview.png``: 1280x640 GitHub social preview
* ``demo/static/apple-touch-icon.png``: 180x180 touch icon for the static demo

Needs a Chromium-family browser for rasterisation (``CHROME`` env var, or Google Chrome /
Chromium on the PATH or in the usual macOS location). Nothing here touches the network.

    python scripts/render_brand_assets.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGO_DIR = ROOT / "docs" / "images" / "logo"
PREVIEW_DIR = LOGO_DIR / "previews"
SOCIAL = ROOT / "docs" / "images" / "social-preview.png"
TOUCH_ICON = ROOT / "demo" / "static" / "apple-touch-icon.png"

LIGHT_BG = "#ffffff"
DARK_BG = "#0b1220"
FONT = '-apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif'


def find_browser() -> str:
    candidates = [
        os.environ.get("CHROME", ""),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        shutil.which("google-chrome") or "",
        shutil.which("chromium") or "",
        shutil.which("chromium-browser") or "",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    sys.exit("no Chromium-family browser found; set CHROME to its executable")


def screenshot(browser: str, html: str, out: Path, width: int, height: int) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="openbars-brand-") as tmp:
        source = Path(tmp) / "page.html"
        source.write_text(html, encoding="utf-8")
        subprocess.run(
            [
                browser,
                "--headless=new",
                "--disable-gpu",
                "--hide-scrollbars",
                "--force-device-scale-factor=1",
                f"--user-data-dir={tmp}/profile",
                f"--window-size={width},{height}",
                f"--screenshot={out}",
                source.as_uri(),
            ],
            check=True,
            capture_output=True,
        )


def svg_inline(path: Path, size: int) -> str:
    """The SVG markup with an explicit pixel size."""
    text = path.read_text(encoding="utf-8")
    return text.replace('width="128" height="128"', f'width="{size}" height="{size}"', 1)


def page(body: str, background: str) -> str:
    return (
        "<!doctype html><meta charset=utf-8><style>"
        f"html,body{{margin:0;background:{background};font-family:{FONT}}}"
        f"svg{{display:block}}</style><body>{body}</body>"
    )


def render_logo_previews(browser: str) -> None:
    svg = LOGO_DIR / "logo.svg"
    for name, background in (("light", LIGHT_BG), ("dark", DARK_BG)):
        body = f'<div style="width:512px;height:512px;display:grid;place-items:center">{svg_inline(svg, 384)}</div>'
        screenshot(browser, page(body, background), PREVIEW_DIR / f"logo-{name}.png", 512, 512)


def decorative_candles() -> str:
    """A fixed, synthetic candle series for the social card (no market data)."""
    series = [
        (60, 78, 52, 84),
        (78, 70, 66, 90),
        (70, 88, 64, 92),
        (88, 82, 76, 100),
        (82, 98, 78, 104),
        (98, 92, 86, 112),
        (92, 110, 88, 116),
        (110, 104, 98, 126),
        (104, 124, 100, 130),
        (124, 118, 112, 140),
        (118, 136, 114, 146),
        (136, 150, 130, 158),
    ]  # (open, close, low, high) in arbitrary units
    step, base, scale = 38, 440, 2.6

    def y(value: float) -> float:
        return base - value * scale

    parts = []
    for index, (o, c, low, high) in enumerate(series):
        x = index * step + 14
        color = "#2dd4bf" if c >= o else "#fb7185"
        top, bottom = y(max(o, c)), y(min(o, c))
        parts.append(
            f'<line x1="{x}" x2="{x}" y1="{y(high):.1f}" y2="{y(low):.1f}" '
            f'stroke="{color}" stroke-width="3" stroke-linecap="round"/>'
        )
        parts.append(
            f'<rect x="{x - 10}" y="{top:.1f}" width="20" height="{max(bottom - top, 4):.1f}" rx="4" fill="{color}"/>'
        )
    width = len(series) * step + 20
    return f'<svg width="{width}" height="470" viewBox="0 0 {width} 470">{"".join(parts)}</svg>'


def render_social_preview(browser: str) -> None:
    logo = svg_inline(LOGO_DIR / "logo.svg", 150)
    body = f"""
<div style="position:relative;width:1280px;height:640px;overflow:hidden;
  background:radial-gradient(1100px 600px at 100% 0%, #123a42 0%, #0b1220 62%)">
  <div style="position:absolute;left:96px;top:104px;display:flex;align-items:center;gap:32px">
    {logo}
    <div style="font-size:92px;font-weight:700;letter-spacing:-0.03em;color:#f1f5f9">OpenBars</div>
  </div>
  <div style="position:absolute;left:96px;top:296px;width:620px;font-size:38px;line-height:1.3;color:#b6c4d8">
    Free, multi-source OHLCV bars, reconciled into one canonical dataset.
  </div>
  <div style="position:absolute;left:96px;top:470px;font-size:27px;color:#5eead4;letter-spacing:0.01em">
    Self-hosted &middot; Parquet + DuckDB &middot; Apache-2.0
  </div>
  <div style="position:absolute;left:96px;top:520px;font-size:22px;color:#8396b0">
    github.com/Liyuk/openbars
  </div>
  <div style="position:absolute;left:760px;top:110px;opacity:.92">{decorative_candles()}</div>
</div>"""
    screenshot(browser, page(body, DARK_BG), SOCIAL, 1280, 640)


def render_touch_icon(browser: str) -> None:
    # Opaque square in the disc colour so the platform's own corner mask shows no seam.
    tile = "#0b1f2a"
    body = (
        f'<div style="width:180px;height:180px;background:{tile};display:grid;place-items:center">'
        f'{svg_inline(LOGO_DIR / "logo.svg", 190)}</div>'
    )
    screenshot(browser, page(body, tile), TOUCH_ICON, 180, 180)


def main() -> int:
    browser = find_browser()
    render_logo_previews(browser)
    render_social_preview(browser)
    render_touch_icon(browser)
    outputs = (*sorted(PREVIEW_DIR.glob("*.png")), SOCIAL, TOUCH_ICON)
    for path in outputs:
        print("wrote", path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
