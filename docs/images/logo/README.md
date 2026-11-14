# OpenBars logo candidates

Three original SVG marks, all on a 128×128 grid, readable at 16 px and on both light and dark
backgrounds. They are hand-drawn from rectangles and lines; no existing trademark was used.

| File | Idea | Notes |
| --- | --- | --- |
| [`candidate-a.svg`](candidate-a.svg) | Three rising candlesticks on a teal rounded tile | Self-contained tile, so it works on any background. **Current default.** |
| [`candidate-b.svg`](candidate-b.svg) | Three open-high-low-close bars (one down, two up), no tile | Transparent background; teal and coral are mid-tones that hold on light and dark. |
| [`candidate-c.svg`](candidate-c.svg) | A hollow candle that doubles as the letter "O", on a navy disc | Most compact and abstract; the disc carries a thin teal rim so it stays visible on dark pages. |

## Default

[`logo.svg`](logo.svg) is the default logo (currently a copy of `candidate-a.svg`). It is what the
README files, the static Pages demo (header, footer, favicon) and the social preview use. To
change the default, replace `logo.svg` with the chosen candidate and rerun
`python scripts/render_brand_assets.py`; the demo and READMEs need no other edit. Keep all
candidates in the tree.

## Previews and generated images

- [`candidates-preview.png`](candidates-preview.png): the three candidates side by side on a
  light and a dark background, at 208, 48, 24 and 16 px.
- [`previews/`](previews/): one 512×512 PNG per candidate and background (`candidate-a-light.png`,
  `candidate-a-dark.png`, ...).
- [`../social-preview.png`](../social-preview.png): the 1280×640 social preview image. GitHub does
  not read it from the repository: a maintainer uploads it under *Settings → General → Social
  preview*. Its decorative candles are a fixed synthetic series, not market data.
- `demo/static/apple-touch-icon.png` (180×180): the touch icon of the static demo.

All PNGs come from `scripts/render_brand_assets.py` (needs a Chromium-family browser; no network).
