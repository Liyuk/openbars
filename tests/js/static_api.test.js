// The GitHub Pages demo must answer the UI's /api/v1 reads from the checked-in
// synthetic sample, with no network access. Run with:
//   node --test tests/js/static_api.test.js
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const ROOT = path.join(__dirname, '../..');
const SAMPLE = path.join(ROOT, 'demo', 'sample');
const manifest = JSON.parse(fs.readFileSync(path.join(SAMPLE, 'manifest.json'), 'utf8'));
const files = {};
for (const name of new Set(Object.values(manifest.responses))) {
  files[name] = JSON.parse(fs.readFileSync(path.join(SAMPLE, name), 'utf8'));
}

const window = { OPENBARS_DEMO_DATA: { manifest, files } };
vm.runInNewContext(fs.readFileSync(path.join(ROOT, 'demo', 'static', 'static-api.js'), 'utf8'), {
  window,
  Promise,
  URL,
});
const api = window.OPENBARS_STATIC_API;

async function apiGet(path) {
  const res = await api(path);
  const body = await res.json();
  if (!res.ok) {
    const err = new Error(body && body.detail ? body.detail : res.statusText);
    err.body = body;
    err.status = res.status;
    throw err;
  }
  return body;
}

test('canonical daily bars for TEST1 render without gaps', async () => {
  const body = await apiGet('/api/v1/bars?symbol=TEST1&source=canonical&interval=1d');
  assert.equal(body.symbol, 'TEST1');
  assert.equal(body.instrument_id, 'us:eq:xnys:TEST1');
  assert.equal(body.adjustment, 'split');
  assert.deepEqual([...body.available_sources], ['canonical']);
  assert.equal(body.bars.length, 5);
  assert.equal(body.bars[0].session_date, '2026-11-06');
  assert.equal(body.bars[0].close, '101.20000000');
  assert.equal(body.bars.at(-1).freshness, 'provisional');
  assert.equal(body.missing_sessions.length, 0);
});

test('instrument ids and date windows are answered from the sample', async () => {
  const byId = await apiGet('/api/v1/bars?id=us%3Aeq%3Axnys%3ATEST1&source=canonical&interval=1d');
  assert.equal(byId.symbol, 'TEST1');
  const windowed = await apiGet(
    '/api/v1/bars?symbol=TEST2&source=canonical&interval=1d&start=2026-11-09&end=2026-11-11',
  );
  assert.deepEqual([...windowed.bars].map((bar) => bar.session_date), ['2026-11-09', '2026-11-11']);
  assert.deepEqual([...windowed.missing_sessions], ['2026-11-10']);
  assert.equal(windowed.start, '2026-11-09');
  assert.equal(windowed.end, '2026-11-11');
});

test('hourly bars are an empty register, not an invented series', async () => {
  const body = await apiGet('/api/v1/bars?symbol=TEST1&source=canonical&interval=1h');
  assert.equal(body.interval, '1h');
  assert.equal(body.bars.length, 0);
  assert.equal(body.note, 'No canonical hourly bars are available.');
});

test('unknown symbols and raw sources fail like the read API', async () => {
  const unknown = await api('/api/v1/bars?symbol=NOPE&source=canonical&interval=1d');
  assert.equal(unknown.ok, false);
  assert.equal(unknown.status, 400);
  const unknownBody = await unknown.json();
  assert.deepEqual(Object.keys(unknownBody).sort(), ['detail']);
  assert.match(unknownBody.detail, /unknown symbol 'NOPE'/);

  const raw = await api('/api/v1/bars?symbol=TEST1&source=example&interval=1d');
  assert.equal(raw.ok, false);
  assert.equal(raw.status, 400);
  const rawBody = await raw.json();
  assert.deepEqual(Object.keys(rawBody).sort(), ['detail']);
  assert.match(rawBody.detail, /only canonical bars are published/);
});

test('dashboard, flags and quota snapshots come from the sample', async () => {
  const markets = await apiGet('/api/v1/markets?interval=1d');
  assert.deepEqual([...markets.markets].map((row) => row.market), ['us', 'crypto', 'cn', 'hk']);
  const flags = await apiGet('/api/v1/flags?interval=1d&limit=1');
  assert.equal(flags.counts.divergent, 1);
  assert.equal(flags.counts.spike_suspect, 1);
  assert.equal(flags.counts.volume_missing, 1);
  const register = await apiGet('/api/v1/flags?interval=1d&limit=200&offset=0');
  assert.equal(register.total, 3);
  const quota = await apiGet('/api/v1/quota');
  assert.equal(quota.available, true);
  assert.equal(quota.providers[0].used.day, 12);
  assert.equal(quota.providers[0].used.minute, 3);
  assert.equal(quota.providers[0].provider, 'example');
});
