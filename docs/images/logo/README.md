# OpenBars logo

An original 128×128 SVG: a hollow candlestick that also reads as the letter "O", on a navy
disc with a thin teal rim so the mark stays visible on both light and dark backgrounds. It is
hand-drawn from a circle, a rounded rectangle and two lines; no existing trademark was used.

[`logo.svg`](logo.svg) is the mark used in the README files, the static Pages demo (header,
footer and favicon) and the social preview. After editing it, rerun
`python scripts/render_brand_assets.py`.

## Generated images

- [`previews/logo-light.png`](previews/logo-light.png) and
  [`previews/logo-dark.png`](previews/logo-dark.png): 512×512 previews on light and dark
  backgrounds.
- [`../social-preview.png`](../social-preview.png): the 1280×640 social preview image. GitHub
  does not read it from the repository: a maintainer uploads it under
  *Settings → General → Social preview*. Its decorative candles are a fixed synthetic series,
  not market data.
- `demo/static/apple-touch-icon.png` (180×180): the touch icon of the static demo, on the same
  navy as the disc.

All PNGs come from `scripts/render_brand_assets.py` (needs a Chromium-family browser; no network).
