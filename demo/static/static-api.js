/* Static-mode API for the GitHub Pages demo.
 *
 * The demo ships a checked-in synthetic sample (demo/sample/*.json) bundled as
 * demo-data.js, so this shim answers the UI's /api/v1 reads from memory. It never
 * uses the network: no backend and no external data source is contacted.
 *
 * Loaded only in the static build, before app.js; app.js calls it through the
 * window.OPENBARS_STATIC_API hook.
 */
(function () {
  "use strict";

  var DATA = window.OPENBARS_DEMO_DATA;
  if (!DATA || !DATA.manifest || !DATA.files) return;

  // Same shape the live /api/v1 uses: HTTP 400 with {"detail": "..."}.
  function respond(ok, body, status) {
    return Promise.resolve({
      ok: ok,
      status: status || (ok ? 200 : 400),
      statusText: ok ? "OK" : "Bad Request",
      json: function () {
        return Promise.resolve(body);
      },
    });
  }

  function fail(detail) {
    return respond(false, { detail: detail }, 400);
  }

  function format(template, values) {
    return String(template == null ? "" : template).replace(/\{(\w+)\}/g, function (match, name) {
      return values[name] == null ? match : values[name];
    });
  }

  function parse(path) {
    var url = new URL(String(path || "/"), "http://openbars-demo.invalid");
    var params = [];
    url.searchParams.forEach(function (value, name) {
      params.push([name, value]);
    });
    params.sort(function (a, b) {
      if (a[0] !== b[0]) return a[0] < b[0] ? -1 : 1;
      if (a[1] !== b[1]) return a[1] < b[1] ? -1 : 1;
      return 0;
    });
    return { pathname: url.pathname, params: params };
  }

  function requestKey(pathname, params) {
    var query = params
      .map(function (pair) {
        return encodeURIComponent(pair[0]) + "=" + encodeURIComponent(pair[1]);
      })
      .join("&");
    return query ? pathname + "?" + query : pathname;
  }

  function lookup(key) {
    var name = DATA.manifest.responses[key];
    return name ? DATA.files[name] : null;
  }

  function within(day, start, end) {
    if (!day) return false;
    if (start && day < start) return false;
    if (end && day > end) return false;
    return true;
  }

  function bars(parsed) {
    var start = null;
    var end = null;
    var wanted = {};
    var params = parsed.params.filter(function (pair) {
      if (pair[0] === "start") {
        start = pair[1];
        return false;
      }
      if (pair[0] === "end") {
        end = pair[1];
        return false;
      }
      wanted[pair[0]] = pair[1];
      return true;
    });

    var body = lookup(requestKey(parsed.pathname, params));
    if (!body) {
      var source = wanted.source || "canonical";
      if (source !== "canonical") {
        return fail(format(DATA.manifest.errors.unknown_source, { source: source }));
      }
      return fail(format(DATA.manifest.errors.unknown_symbol, { symbol: wanted.symbol || wanted.id || "" }));
    }

    // The sample stores each series once; a date window is filtered locally.
    var copy = JSON.parse(JSON.stringify(body));
    if (start || end) {
      copy.bars = (copy.bars || []).filter(function (bar) {
        return within(bar.session_date, start, end);
      });
      copy.missing_sessions = (copy.missing_sessions || []).filter(function (day) {
        return within(day, start, end);
      });
      copy.missing_hours = (copy.missing_hours || []).filter(function (gap) {
        return within(String(gap.ts_start || "").slice(0, 10), start, end);
      });
      if (start) copy.start = start;
      if (end) copy.end = end;
    }
    return respond(true, copy);
  }

  window.OPENBARS_STATIC_API = function (path) {
    var parsed = parse(path);
    if (parsed.pathname === "/api/v1/bars") return bars(parsed);
    var body = lookup(requestKey(parsed.pathname, parsed.params));
    if (body) return respond(true, body);
    return fail("not part of the static demo dataset: " + parsed.pathname);
  };
})();
