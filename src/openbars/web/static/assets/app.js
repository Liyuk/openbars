/* openbars local read-only UI. Pages are bilingual; /api/v1 is for this UI only. */
(function () {
  "use strict";

  var I18N = {
    en: {
      "nav.dashboard": "Dashboard",
      "nav.chart": "Chart",
      "nav.flags": "Flags",
      "header.readonly": "Local read-only UI. Not a public API.",
      "footer.api": "Routes under /api/v1 are for this local UI only and are not a stable public API.",
      "dash.title": "Coverage dashboard",
      "dash.lede": "Read-only view of stored bars, quality flags, and quota usage. This page never fetches or writes data.",
      "dash.markets": "Markets",
      "dash.no_markets": "No stored markets yet.",
      "dash.instruments": "Instruments",
      "dash.bars": "Bars",
      "dash.first": "First session",
      "dash.last": "Last session",
      "dash.flags": "Quality flags",
      "dash.quota": "Quota usage",
      "dash.quota_empty": "No quota database found. The server does not create one in read-only mode.",
      "dash.provider": "Provider",
      "dash.used": "Used",
      "dash.remaining": "Remaining",
      "dash.penalized": "Penalized until",
      "flags.title": "Quality flags",
      "flags.lede": "Canonical bars marked divergent, spike_suspect, volume_missing, or stale.",
      "flags.empty": "No flagged bars in the current window.",
      "flags.date": "Session",
      "flags.symbol": "Symbol",
      "flags.id": "Instrument",
      "flags.names": "Flags",
      "flags.market": "Market",
      "chart.title": "K-line",
      "chart.lede": "Default view is the aggregated canonical series. Switch source to inspect a raw provider; missing sessions appear as gaps.",
      "chart.symbol": "Symbol or id",
      "chart.source": "Data source",
      "chart.start": "Start",
      "chart.end": "End",
      "chart.load": "Load",
      "chart.canonical_note": "Showing aggregated canonical bars (filled and aligned by policy).",
      "chart.raw_note": "Showing raw bars from this provider. Gaps and missing fields come from the source and are not filled.",
      "chart.gaps": "Missing sessions",
      "chart.missing_fields": "Bars with missing fields",
      "chart.empty": "No bars for this selection.",
      "chart.pick": "Enter a symbol or instrument id, then load.",
      "chart.flags": "Flags on this bar",
      "err.generic": "Unable to load data.",
      "dash.flags_empty": "No flagged bars.",
      "flag.divergent": "divergent",
      "flag.spike_suspect": "spike suspect",
      "flag.volume_missing": "volume missing",
      "flag.single_source": "single source",
      "flag.filled_from_fallback": "filled from fallback",
      "flag.stale": "stale",
      "flag.gap": "gap",
      "flag.revised": "revised",
      "flag.adjustment_unverified": "adjustment unverified",
      "flag.limit_up": "limit up",
      "flag.limit_down": "limit down",
      "flag.suspended": "suspended",
      "gap.label": "gap"
    },
    zh: {
      "nav.dashboard": "看板",
      "nav.chart": "K 线",
      "nav.flags": "质量标记",
      "header.readonly": "本机只读界面，不是公开 API。",
      "footer.api": "/api/v1 仅供本机页面内部使用，本版不承诺稳定。",
      "dash.title": "覆盖看板",
      "dash.lede": "只读查看已存 K 线覆盖、质量标记和额度用量。本页不会抓取或写入数据。",
      "dash.markets": "市场",
      "dash.no_markets": "还没有已存储的市场数据。",
      "dash.instruments": "品种数",
      "dash.bars": "K 线条数",
      "dash.first": "首个交易日",
      "dash.last": "最近交易日",
      "dash.flags": "质量标记",
      "dash.quota": "额度用量",
      "dash.quota_empty": "未找到额度数据库。只读模式下服务不会创建该文件。",
      "dash.provider": "渠道",
      "dash.used": "已用",
      "dash.remaining": "剩余",
      "dash.penalized": "惩罚至",
      "flags.title": "质量标记",
      "flags.lede": "canonical 层中带 divergent、spike_suspect、volume_missing 或 stale 标记的 K 线。",
      "flags.empty": "当前窗口没有带标记的 K 线。",
      "flags.date": "交易日",
      "flags.symbol": "代码",
      "flags.id": "品种",
      "flags.names": "标记",
      "flags.market": "市场",
      "chart.title": "K 线",
      "chart.lede": "默认查看按规则对齐填充后的聚合数据（canonical）。可切换到各渠道原始数据；缺失的交易日会显示为缺口。",
      "chart.symbol": "代码或品种 id",
      "chart.source": "数据源",
      "chart.start": "开始",
      "chart.end": "结束",
      "chart.load": "加载",
      "chart.canonical_note": "正在显示聚合后的 canonical 数据（按策略对齐填充）。",
      "chart.raw_note": "正在显示该渠道的原始数据。缺口和缺字段来自源数据，不会被填充。",
      "chart.gaps": "缺失交易日",
      "chart.missing_fields": "缺字段的 K 线",
      "chart.empty": "没有符合条件的 K 线。",
      "chart.pick": "输入代码或品种 id，然后加载。",
      "chart.flags": "本根标记",
      "err.generic": "无法加载数据。",
      "dash.flags_empty": "没有带标记的 K 线。",
      "flag.divergent": "价格分歧",
      "flag.spike_suspect": "疑似尖刺",
      "flag.volume_missing": "成交量缺失",
      "flag.single_source": "单一来源",
      "flag.filled_from_fallback": "由备源填充",
      "flag.stale": "陈旧",
      "flag.gap": "缺口",
      "flag.revised": "已修订",
      "flag.adjustment_unverified": "复权未核实",
      "flag.limit_up": "涨停",
      "flag.limit_down": "跌停",
      "flag.suspended": "停牌",
      "gap.label": "缺失"
    }
  };

  var lang = "en";
  var chartApi = null;
  var candleSeries = null;

  function detectLang() {
    try {
      var saved = localStorage.getItem("openbars.lang");
      if (saved === "zh" || saved === "en") return saved;
    } catch (err) { /* ignore */ }
    var nav = (navigator.language || "en").toLowerCase();
    return nav.indexOf("zh") === 0 ? "zh" : "en";
  }

  function t(key) {
    var table = I18N[lang] || I18N.en;
    return table[key] || I18N.en[key] || key;
  }

  function setLang(next) {
    lang = next === "zh" ? "zh" : "en";
    try { localStorage.setItem("openbars.lang", lang); } catch (err) { /* ignore */ }
    document.documentElement.lang = lang === "zh" ? "zh-Hans" : "en";
    document.querySelectorAll("[data-i18n]").forEach(function (el) {
      el.textContent = t(el.getAttribute("data-i18n"));
    });
    var btn = document.getElementById("lang-toggle");
    if (btn) btn.textContent = lang === "zh" ? "English" : "中文";
  }

  function pathOf() {
    var path = location.pathname.replace(/\/+$/, "") || "/";
    return path;
  }

  function markNav() {
    var path = pathOf();
    document.querySelectorAll("nav a[data-nav]").forEach(function (el) {
      var target = el.getAttribute("data-nav");
      el.classList.toggle("active", target === path);
    });
  }

  function apiGet(path) {
    return fetch(path, { headers: { Accept: "application/json" } }).then(function (res) {
      return res.json().then(function (body) {
        if (!res.ok) {
          var detail = body && body.detail ? body.detail : res.statusText;
          throw new Error(detail);
        }
        return body;
      });
    });
  }

  function el(html) {
    var wrap = document.createElement("div");
    wrap.innerHTML = html.trim();
    return wrap.firstElementChild;
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function flagLabel(name) {
    return t("flag." + name) || name;
  }

  function flagBadges(flags) {
    return (flags || []).map(function (name) {
      var danger = name === "divergent" || name === "spike_suspect";
      return '<span class="badge ' + (danger ? "danger" : "warn") + '">' + escapeHtml(flagLabel(name)) + "</span>";
    }).join(" ");
  }

  function formatUsed(obj) {
    if (!obj || typeof obj !== "object") return "—";
    return Object.keys(obj).map(function (k) { return k + ": " + obj[k]; }).join(", ") || "—";
  }

  function renderError(where, err) {
    where.innerHTML = '<p class="err">' + escapeHtml(err && err.message ? err.message : t("err.generic")) + "</p>";
  }

  function renderDashboard(root) {
    root.innerHTML =
      "<h1>" + t("dash.title") + "</h1>" +
      '<p class="lede">' + t("dash.lede") + "</p>" +
      "<h2>" + t("dash.markets") + "</h2>" +
      '<div id="markets" class="grid"></div>' +
      "<h2>" + t("dash.flags") + "</h2>" +
      '<div id="flag-summary" class="card"></div>' +
      "<h2>" + t("dash.quota") + "</h2>" +
      '<div id="quota" class="card"></div>';

    Promise.all([apiGet("/api/v1/markets"), apiGet("/api/v1/flags?limit=1"), apiGet("/api/v1/quota")])
      .then(function (results) {
        var markets = results[0].markets || [];
        var flags = results[1];
        var quota = results[2];
        var box = document.getElementById("markets");
        if (!markets.length) {
          box.innerHTML = '<p class="empty">' + t("dash.no_markets") + "</p>";
        } else {
          box.innerHTML = markets.map(function (m) {
            var flagBits = Object.keys(m.flags || {}).filter(function (k) { return m.flags[k]; }).map(function (k) {
              return flagLabel(k) + " " + m.flags[k];
            }).join(" · ");
            return (
              '<article class="card">' +
                "<h2>" + escapeHtml((m.market || "").toUpperCase()) + "</h2>" +
                '<div class="metric"><span>' + t("dash.instruments") + "</span><span>" + m.instruments + "</span></div>" +
                '<div class="metric"><span>' + t("dash.bars") + "</span><span>" + m.bars + "</span></div>" +
                '<div class="metric"><span>' + t("dash.first") + "</span><span>" + escapeHtml(m.first_session || "—") + "</span></div>" +
                '<div class="metric"><span>' + t("dash.last") + "</span><span>" + escapeHtml(m.last_session || "—") + "</span></div>" +
                (flagBits ? '<p class="note">' + escapeHtml(flagBits) + "</p>" : "") +
              "</article>"
            );
          }).join("");
        }
        var counts = flags.counts || {};
        var parts = Object.keys(counts).filter(function (k) { return counts[k] > 0; }).map(function (k) {
          return '<div class="metric"><span>' + escapeHtml(flagLabel(k)) + "</span><span>" + counts[k] + "</span></div>";
        }).join("");
        document.getElementById("flag-summary").innerHTML =
          (parts || '<p class="empty">' + t("dash.flags_empty") + "</p>") +
          '<p class="note"><a href="/flags">' + t("nav.flags") + "</a></p>";

        var quotaBox = document.getElementById("quota");
        if (!quota.available || !(quota.providers || []).length) {
          quotaBox.innerHTML = '<p class="empty">' + t("dash.quota_empty") + "</p>";
        } else {
          var rows = quota.providers.map(function (p) {
            return "<tr><td class=\"mono\">" + escapeHtml(p.provider || p.id || "") + "</td><td>" +
              escapeHtml(formatUsed(p.used)) + "</td><td>" + escapeHtml(formatUsed(p.remaining)) +
              "</td><td>" + escapeHtml(p.penalized_until || "—") + "</td></tr>";
          }).join("");
          quotaBox.innerHTML =
            "<table><thead><tr><th>" + t("dash.provider") + "</th><th>" + t("dash.used") +
            "</th><th>" + t("dash.remaining") + "</th><th>" + t("dash.penalized") +
            "</th></tr></thead><tbody>" + rows + "</tbody></table>";
        }
      })
      .catch(function (err) { renderError(root, err); });
  }

  function renderFlags(root) {
    root.innerHTML =
      "<h1>" + t("flags.title") + "</h1>" +
      '<p class="lede">' + t("flags.lede") + "</p>" +
      '<div id="flags-table" class="card"></div>';
    apiGet("/api/v1/flags?limit=200").then(function (body) {
      var box = document.getElementById("flags-table");
      var items = body.items || [];
      if (!items.length) {
        box.innerHTML = '<p class="empty">' + t("flags.empty") + "</p>";
        return;
      }
      var rows = items.map(function (item) {
        return "<tr><td>" + escapeHtml(item.session_date || "") + "</td><td>" +
          escapeHtml((item.market || "").toUpperCase()) + "</td><td class=\"mono\">" +
          escapeHtml(item.symbol || "") + "</td><td class=\"mono\">" +
          escapeHtml(item.instrument_id || "") + "</td><td>" + flagBadges(item.flags) + "</td></tr>";
      }).join("");
      box.innerHTML =
        "<table><thead><tr><th>" + t("flags.date") + "</th><th>" + t("flags.market") +
        "</th><th>" + t("flags.symbol") + "</th><th>" + t("flags.id") + "</th><th>" +
        t("flags.names") + "</th></tr></thead><tbody>" + rows + "</tbody></table>";
    }).catch(function (err) { renderError(root, err); });
  }

  function queryParams() {
    return new URLSearchParams(location.search);
  }

  function renderChart(root) {
    var q = queryParams();
    var initial = q.get("id") || q.get("symbol") || "";
    var initialSource = q.get("source") || "canonical";
    root.innerHTML =
      "<h1>" + t("chart.title") + "</h1>" +
      '<p class="lede">' + t("chart.lede") + "</p>" +
      '<form class="toolbar" id="chart-form">' +
        "<label>" + t("chart.symbol") + '<input name="q" id="chart-q" value="' + escapeHtml(initial) + '" placeholder="TEST1" autocomplete="off"></label>' +
        "<label>" + t("chart.source") + '<select name="source" id="chart-source"><option value="canonical">canonical</option></select></label>' +
        "<label>" + t("chart.start") + '<input name="start" id="chart-start" type="date" value="' + escapeHtml(q.get("start") || "") + '"></label>' +
        "<label>" + t("chart.end") + '<input name="end" id="chart-end" type="date" value="' + escapeHtml(q.get("end") || "") + '"></label>' +
        '<button class="action" type="submit">' + t("chart.load") + "</button>" +
      "</form>" +
      '<div id="chart-note" class="note">' + t("chart.pick") + "</div>" +
      '<div class="card"><div id="chart"></div></div>' +
      '<div id="chart-meta"></div>';

    var sourceSel = document.getElementById("chart-source");
    if (initialSource && initialSource !== "canonical") {
      var opt = document.createElement("option");
      opt.value = initialSource;
      opt.textContent = initialSource;
      sourceSel.appendChild(opt);
      sourceSel.value = initialSource;
    }

    document.getElementById("chart-form").addEventListener("submit", function (ev) {
      ev.preventDefault();
      loadChart();
    });
    sourceSel.addEventListener("change", function () {
      if (document.getElementById("chart-q").value.trim()) loadChart();
    });

    if (initial) loadChart();
  }

  function fillSources(available, selected) {
    var sel = document.getElementById("chart-source");
    var current = selected || sel.value || "canonical";
    var names = available && available.length ? available.slice() : ["canonical"];
    if (names.indexOf("canonical") === -1) names.unshift("canonical");
    if (current && names.indexOf(current) === -1) names.push(current);
    sel.innerHTML = names.map(function (name) {
      return '<option value="' + escapeHtml(name) + '">' + escapeHtml(name) + "</option>";
    }).join("");
    sel.value = current;
  }

  function num(value) {
    if (value == null || value === "") return undefined;
    return Number(value);
  }

  function loadChart() {
    var q = document.getElementById("chart-q").value.trim();
    var source = document.getElementById("chart-source").value || "canonical";
    var start = document.getElementById("chart-start").value;
    var end = document.getElementById("chart-end").value;
    var note = document.getElementById("chart-note");
    var meta = document.getElementById("chart-meta");
    if (!q) {
      note.textContent = t("chart.pick");
      return;
    }
    var params = new URLSearchParams();
    if (q.indexOf(":") !== -1) params.set("id", q);
    else params.set("symbol", q);
    params.set("source", source);
    if (start) params.set("start", start);
    if (end) params.set("end", end);
    history.replaceState(null, "", "/chart?" + params.toString());
    apiGet("/api/v1/bars?" + params.toString()).then(function (body) {
      if (body.start) document.getElementById("chart-start").value = body.start;
      if (body.end) document.getElementById("chart-end").value = body.end;
      fillSources(body.available_sources, body.source);
      var isCanonical = body.source === "canonical";
      note.textContent = isCanonical ? t("chart.canonical_note") : t("chart.raw_note");
      var missing = body.missing_sessions || [];
      var bars = body.bars || [];
      drawChart(bars, missing, isCanonical);
      var bits = [];
      if (missing.length) {
        bits.push("<div class=\"notice\"><strong>" + t("chart.gaps") + " (" + missing.length + "):</strong> " +
          escapeHtml(missing.join(", ")) + "</div>");
      }
      var fieldGaps = bars.filter(function (b) { return (b.missing_fields || []).length; });
      if (fieldGaps.length) {
        bits.push("<div class=\"notice\"><strong>" + t("chart.missing_fields") + ":</strong> " +
          fieldGaps.map(function (b) {
            return escapeHtml(b.session_date) + " (" + escapeHtml((b.missing_fields || []).join(", ")) + ")";
          }).join("; ") + "</div>");
      }
      if (!bars.length) bits.unshift('<p class="empty">' + t("chart.empty") + "</p>");
      meta.innerHTML = bits.join("");
    }).catch(function (err) {
      note.innerHTML = '<span class="err">' + escapeHtml(err.message || t("err.generic")) + "</span>";
    });
  }

  function drawChart(bars, missing, isCanonical) {
    var host = document.getElementById("chart");
    if (!window.LightweightCharts) {
      host.textContent = t("err.generic");
      return;
    }
    if (chartApi) {
      chartApi.remove();
      chartApi = null;
      candleSeries = null;
    }
    chartApi = LightweightCharts.createChart(host, {
      width: host.clientWidth || 800,
      height: 440,
      layout: { background: { color: "#ffffff" }, textColor: "#1a2332" },
      grid: { vertLines: { color: "#eef1f4" }, horzLines: { color: "#eef1f4" } },
      rightPriceScale: { borderColor: "#d8dee6" },
      timeScale: { borderColor: "#d8dee6", timeVisible: false }
    });
    candleSeries = chartApi.addCandlestickSeries({
      upColor: "#087f5b",
      downColor: "#b42318",
      borderVisible: false,
      wickUpColor: "#087f5b",
      wickDownColor: "#b42318"
    });
    var byDay = {};
    bars.forEach(function (b) { byDay[b.session_date] = b; });
    var days = bars.map(function (b) { return b.session_date; }).concat(missing || []);
    days.sort();
    var data = [];
    var seen = {};
    days.forEach(function (day) {
      if (!day || seen[day]) return;
      seen[day] = true;
      var b = byDay[day];
      if (!b) {
        data.push({ time: day });
        return;
      }
      var o = num(b.open), h = num(b.high), l = num(b.low), c = num(b.close);
      if (o == null || h == null || l == null || c == null) {
        data.push({ time: day });
        return;
      }
      data.push({ time: day, open: o, high: h, low: l, close: c });
    });
    candleSeries.setData(data);
    var markers = [];
    (missing || []).forEach(function (day) {
      markers.push({
        time: day,
        position: "belowBar",
        color: "#b54708",
        shape: "circle",
        text: t("gap.label")
      });
    });
    if (isCanonical) {
      bars.forEach(function (b) {
        if (!(b.flags || []).length) return;
        markers.push({
          time: b.session_date,
          position: "aboveBar",
          color: "#b42318",
          shape: "arrowDown",
          text: (b.flags || []).map(flagLabel).join(", ")
        });
      });
    } else {
      bars.forEach(function (b) {
        if (!(b.missing_fields || []).length) return;
        markers.push({
          time: b.session_date,
          position: "aboveBar",
          color: "#b54708",
          shape: "arrowDown",
          text: (b.missing_fields || []).join(", ")
        });
      });
    }
    markers.sort(function (a, b) { return a.time < b.time ? -1 : a.time > b.time ? 1 : 0; });
    if (markers.length) candleSeries.setMarkers(markers);
    chartApi.timeScale().fitContent();
  }

  window.addEventListener("resize", function () {
    if (chartApi) {
      var host = document.getElementById("chart");
      if (host) chartApi.applyOptions({ width: host.clientWidth });
    }
  });

  function boot() {
    setLang(detectLang());
    markNav();
    var root = document.getElementById("app");
    var path = pathOf();
    if (path === "/chart") renderChart(root);
    else if (path === "/flags") renderFlags(root);
    else renderDashboard(root);
  }

  document.getElementById("lang-toggle").addEventListener("click", function () {
    setLang(lang === "zh" ? "en" : "zh");
    boot();
  });

  boot();
})();
