const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

const APP_JS = path.join(__dirname, '../../src/openbars/web/static/assets/app.js');

class Element {
  constructor(attrs = {}) {
    this.value = '';
    this.innerHTML = '';
    this.textContent = '';
    this.title = '';
    this.checked = false;
    this.hidden = false;
    this.clientWidth = 0;
    this.attrs = Object.assign({}, attrs);
    this.listeners = {};
  }

  addEventListener(type, callback) {
    (this.listeners[type] = this.listeners[type] || []).push(callback);
  }

  dispatch(type, event) {
    const handlers = this.listeners[type];
    if (!handlers || !handlers.length) {
      throw new Error('no "' + type + '" listener bound on ' + (this.attrs.id || 'element'));
    }
    handlers.slice().forEach((handler) => handler.call(this, event || { preventDefault() {} }));
  }

  setAttribute(name, value) { this.attrs[name] = String(value); }
  getAttribute(name) { return name in this.attrs ? this.attrs[name] : null; }
  toggleAttribute(name, on) { if (on) this.attrs[name] = ''; else delete this.attrs[name]; }
  appendChild() {}
}

// A radio input: dispatching "change" moves the checked state inside its group,
// the way a browser does, before running the listeners.
class Radio extends Element {
  constructor(value, group, name) {
    super({ name });
    this.value = value;
    this.group = group;
    group.push(this);
  }

  dispatch(type, event) {
    this.group.forEach((radio) => { radio.checked = radio === this; });
    super.dispatch(type, event);
  }
}

const ELEMENT_IDS = [
  'app', 'lang-toggle', 'host-addr',
  'chart-form', 'chart-source', 'chart-start', 'chart-end', 'chart-q',
  'chart-record', 'chart-remarks', 'chart',
  'bar-table-daily', 'bar-table-hourly', 'daily-bars-rows', 'hourly-bars-rows',
  's-date', 's-o', 's-h', 's-l', 's-c', 's-v', 's-fr', 's-av', 's-fl',
];

function buildPage(search) {
  const elements = {};
  ELEMENT_IDS.forEach((id) => { elements[id] = new Element({ id }); });
  elements['chart-source'].value = 'canonical';

  const groups = { iv1: [], iv2: [], iv3: [] };
  Object.keys(groups).forEach((name) => {
    new Radio('1d', groups[name], name);
    new Radio('1h', groups[name], name);
  });

  const selectAll = (selector) => {
    if (selector === "input[name='iv1']") return groups.iv1;
    if (selector === "input[name='iv2']") return groups.iv2;
    if (selector === "input[name='iv3']") return groups.iv3;
    if (selector === "input[name='iv1']:checked") return groups.iv1.filter((r) => r.checked);
    if (selector === "input[name='iv2']:checked") return groups.iv2.filter((r) => r.checked);
    if (selector === "input[name='iv3']:checked") return groups.iv3.filter((r) => r.checked);
    return [];
  };

  const document = {
    documentElement: {},
    getElementById: (id) => elements[id],
    querySelectorAll: selectAll,
    querySelector: (selector) => selectAll(selector)[0] || null,
    createElement: () => new Element(),
  };

  const window = {
    addEventListener(type, callback) {
      (window.listeners[type] = window.listeners[type] || []).push(callback);
    },
    listeners: {},
    dispatch(type) {
      (window.listeners[type] || []).forEach((callback) => callback({}));
    },
  };

  const location = { pathname: '/chart', search, host: '127.0.0.1:9000' };
  const localStorage = { getItem() { return null; }, setItem() {} };
  return { elements, groups, document, window, location, localStorage };
}

function fakeCharts(state) {
  return {
    createChart() {
      return {
        remove() {},
        timeScale() { return { fitContent() {} }; },
        subscribeCrosshairMove() {},
        addCandlestickSeries() {
          return {
            setData(data) { state.rendered.push(data); },
            setMarkers(value) { state.markers = value; },
          };
        },
      };
    },
  };
}

function run(page, fetchImpl) {
  const charts = fakeCharts(page.chartsState);
  page.window.LightweightCharts = charts;
  const context = {
    document: page.document,
    window: page.window,
    navigator: { language: 'en' },
    localStorage: page.localStorage,
    location: page.location,
    history: { replaceState() {} },
    getComputedStyle: () => ({ getPropertyValue: () => '#b42318' }),
    URLSearchParams,
    Intl,
    Date,
    fetch: fetchImpl,
    LightweightCharts: charts,
  };
  vm.runInNewContext(fs.readFileSync(APP_JS, 'utf8'), context);
  // The app boots from DOMContentLoaded, as it does in the browser.
  page.window.dispatch('DOMContentLoaded');
}

async function settle() { await new Promise((resolve) => setImmediate(resolve)); }

test('historical hourly sources and source/interval changes use fresh default windows', async () => {
  const page = buildPage('?symbol=TEST1&interval=1h');
  page.chartsState = { rendered: [], markers: [] };
  const requests = [];
  const fetchImpl = async (url) => {
    const params = new URL(url, 'http://localhost').searchParams;
    requests.push(params);
    const source = params.get('source');
    const interval = params.get('interval');
    const day = interval === '1d' ? '2019-03-04' : source === 'prov_old' ? '2020-01-02' : '2026-01-01';
    return {
      ok: true,
      json: async () => ({
        source, start: day, end: day,
        available_sources: ['prov_old'],
        bars: [], missing_hours: [], missing_sessions: [], note: source === 'canonical',
      }),
    };
  };

  run(page, fetchImpl);
  await settle();

  const { elements, groups } = page;
  assert.match(elements['chart-source'].innerHTML, /prov_old/);
  assert.equal(requests[0].get('symbol'), 'TEST1');
  assert.equal(requests[0].get('source'), 'canonical');
  assert.equal(requests[0].get('interval'), '1h');
  assert.equal(requests[0].has('start'), false);
  assert.equal(requests[0].has('end'), false);
  // The interval radios carry real values, so the URL interval wins over the default.
  assert.equal(groups.iv2[1].checked, true);
  assert.equal(groups.iv2[1].value, '1h');
  // The masthead shows the address the page was served from.
  assert.equal(elements['host-addr'].textContent, '127.0.0.1:9000');

  elements['chart-source'].value = 'prov_old';
  elements['chart-source'].dispatch('change');
  await settle();
  assert.equal(requests[1].get('source'), 'prov_old');
  assert.equal(requests[1].get('interval'), '1h');
  assert.equal(requests[1].has('start'), false);
  assert.equal(requests[1].has('end'), false);
  assert.equal(elements['chart-start'].value, '2020-01-02');
  assert.equal(elements['chart-end'].value, '2020-01-02');

  groups.iv2[0].dispatch('change');
  await settle();
  assert.equal(requests[2].get('interval'), '1d');
  assert.equal(requests[2].get('source'), 'prov_old');
  assert.equal(requests[2].has('start'), false);
  assert.equal(requests[2].has('end'), false);
  assert.equal(elements['chart-start'].value, '2019-03-04');
  assert.equal(elements['chart-end'].value, '2019-03-04');
});

test('a late response cannot overwrite the latest source and window', async () => {
  const page = buildPage('?symbol=TEST1&interval=1h');
  page.chartsState = { rendered: [], markers: [] };
  const pending = [];
  const fetchImpl = () => new Promise((resolve) => pending.push(resolve));

  run(page, fetchImpl);
  const { elements } = page;
  elements['chart-source'].value = 'prov_b';
  elements['chart-source'].dispatch('change');

  const response = (source, day) => ({
    ok: true,
    json: async () => ({
      source, start: day, end: day,
      available_sources: [source],
      bars: [{ ts_start: day + 'T14:30:00Z', open: 1, high: 2, low: 1, close: source === 'prov_b' ? 2 : 1 }],
      missing_hours: [], note: false,
    }),
  });

  pending[1](response('prov_b', '2020-01-02'));
  await settle();
  pending[0](response('canonical', '2026-01-01'));
  await settle();

  assert.equal(elements['chart-source'].value, 'prov_b');
  assert.equal(elements['chart-start'].value, '2020-01-02');
  assert.equal(elements['chart-end'].value, '2020-01-02');
  assert.match(elements['chart-record'].innerHTML, /raw/i);
  assert.equal(page.chartsState.rendered.length, 1);
  assert.equal(page.chartsState.rendered[0][0].close, 2);
});

test('canonical quality markers only show flags while freshness remains separate', async () => {
  const page = buildPage('?symbol=TEST1&interval=1d');
  page.chartsState = { rendered: [], markers: [] };
  const bar = (session_date, freshness, flags, volume) => ({
    session_date, freshness, flags, volume,
    available_at: '2026-01-03T00:00:00Z', open: 1, high: 2, low: 1, close: 2,
  });
  const fetchImpl = async () => ({
    ok: true,
    json: async () => ({
      source: 'canonical',
      available_sources: [],
      bars: [bar('2026-01-01', 'final', [], 10), bar('2026-01-02', 'provisional', ['divergent'], 0)],
      missing_sessions: [], note: false,
    }),
  });

  run(page, fetchImpl);
  await settle();

  const { elements } = page;
  const markers = page.chartsState.markers;
  assert.equal(markers.length, 1);
  assert.equal(markers[0].time, '2026-01-02');
  assert.match(markers[0].text, /divergent/i);
  assert.doesNotMatch(markers[0].text, /provisional|final/i);
  // Freshness and availability live in the reading slip, never in the flag markers.
  assert.match(elements['s-fr'].innerHTML, /provisional/);
  assert.equal(elements['s-av'].textContent, '2026-01-03T00:00');
  // A stored volume of 0 is drawn as 0, not as a blank.
  assert.match(elements['daily-bars-rows'].innerHTML, />0</);
  // Keyboard screen-reader summary of what the canvas holds.
  assert.match(elements['chart'].getAttribute('aria-label'), /2 bars/);
  assert.match(elements['chart'].getAttribute('aria-label'), /0 gaps/);
});
