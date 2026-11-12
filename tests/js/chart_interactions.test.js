const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

class Element {
  constructor(value = '') { this.value = value; this.innerHTML = ''; this.textContent = ''; this.listeners = {}; }
  addEventListener(type, callback) { this.listeners[type] = callback; }
  appendChild() {}
  dispatch(type) { this.listeners[type].call(this, { preventDefault() {} }); }
}

async function settle() { await new Promise(resolve => setImmediate(resolve)); }

test('historical hourly sources and source/interval changes use fresh default windows', async () => {
  const elements = Object.fromEntries(['app', 'lang-toggle', 'chart-form', 'chart-source', 'chart-start',
    'chart-end', 'chart-q', 'interval', 'chart-note', 'chart-meta', 'chart'].map(id => [id, new Element()]));
  elements['chart-q'].value = 'TEST1';
  elements['chart-source'].value = 'canonical';
  elements.interval.value = '1h';
  const requests = [];
  const document = {
    documentElement: {}, getElementById: id => elements[id], querySelectorAll: () => [],
    querySelector: () => null, createElement: () => new Element(),
  };
  const context = {
    document, window: { addEventListener() {} }, navigator: { language: 'en' },
    localStorage: { getItem() { return null; }, setItem() {} },
    location: { pathname: '/chart', search: '?symbol=TEST1&interval=1h' },
    history: { replaceState() {} }, URLSearchParams, Intl, Date,
    fetch: async url => {
      const params = new URL(url, 'http://localhost').searchParams;
      requests.push(params);
      const source = params.get('source');
      const interval = params.get('interval');
      const day = interval === '1d' ? '2019-03-04' : source === 'prov_old' ? '2020-01-02' : '2026-01-01';
      return { ok: true, json: async () => ({ source, start: day, end: day,
        available_sources: ['prov_old'], bars: [], missing_hours: [], missing_sessions: [], note: source === 'canonical' }) };
    },
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../../src/openbars/web/static/assets/app.js'), 'utf8'), context);
  await settle();
  assert.match(elements['chart-source'].innerHTML, /prov_old/);
  assert.equal(requests[0].get('source'), 'canonical');
  assert.equal(requests[0].get('interval'), '1h');
  elements['chart-source'].value = 'prov_old';
  elements['chart-source'].dispatch('change');
  await settle();
  assert.equal(requests[1].get('source'), 'prov_old');
  assert.equal(requests[1].has('start'), false);
  assert.equal(requests[1].has('end'), false);
  assert.equal(elements['chart-start'].value, '2020-01-02');
  assert.equal(elements['chart-end'].value, '2020-01-02');
  elements.interval.value = '1d';
  elements.interval.dispatch('change');
  await settle();
  assert.equal(requests[2].get('interval'), '1d');
  assert.equal(requests[2].get('source'), 'prov_old');
  assert.equal(requests[2].has('start'), false);
  assert.equal(requests[2].has('end'), false);
  assert.equal(elements['chart-start'].value, '2019-03-04');
  assert.equal(elements['chart-end'].value, '2019-03-04');
});

test('a late response cannot overwrite the latest source and window', async () => {
  const elements = Object.fromEntries(['app', 'lang-toggle', 'chart-form', 'chart-source', 'chart-start',
    'chart-end', 'chart-q', 'interval', 'chart-note', 'chart-meta', 'chart'].map(id => [id, new Element()]));
  elements['chart-q'].value = 'TEST1';
  elements['chart-source'].value = 'canonical';
  elements.interval.value = '1h';
  const pending = [];
  const rendered = [];
  const charts = { createChart() { return { remove() {}, timeScale() { return { fitContent() {} }; },
    addCandlestickSeries() { return { setData(data) { rendered.push(data); }, setMarkers() {} }; } }; } };
  const document = { documentElement: {}, getElementById: id => elements[id], querySelectorAll: () => [],
    querySelector: () => null, createElement: () => new Element() };
  const context = { document, window: { addEventListener() {}, LightweightCharts: charts }, LightweightCharts: charts, navigator: { language: 'en' },
    localStorage: { getItem() { return null; }, setItem() {} },
    location: { pathname: '/chart', search: '?symbol=TEST1&interval=1h' },
    history: { replaceState() {} }, URLSearchParams, Intl, Date,
    fetch: () => new Promise(resolve => pending.push(resolve)) };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../../src/openbars/web/static/assets/app.js'), 'utf8'), context);
  elements['chart-source'].value = 'prov_b';
  elements['chart-source'].dispatch('change');
  const response = (source, day) => ({ ok: true, json: async () => ({ source, start: day, end: day,
    available_sources: [source], bars: [{ ts_start: day + 'T14:30:00Z', open: 1, high: 2, low: 1, close: source === 'prov_b' ? 2 : 1 }], missing_hours: [], note: false }) });
  pending[1](response('prov_b', '2020-01-02'));
  await settle();
  pending[0](response('canonical', '2026-01-01'));
  await settle();
  assert.equal(elements['chart-source'].value, 'prov_b');
  assert.equal(elements['chart-start'].value, '2020-01-02');
  assert.equal(elements['chart-end'].value, '2020-01-02');
  assert.match(elements['chart-note'].textContent, /raw/i);
  assert.equal(rendered.length, 1);
  assert.equal(rendered[0][0].close, 2);
});
