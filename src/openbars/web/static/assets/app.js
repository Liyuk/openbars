(function () {
  "use strict";

  var I18N = {
    en: {
      "brand": "OpenBars",
      "nav.dashboard": "Dashboard",
      "nav.chart": "Chart",
      "nav.flags": "Flags",
      "header.readonly": "read-only",
      "header.station_title": "Shows data already on disk. Never fetches or writes.",
      "footer.api": "/api/v1 is a self-hosted API: you deploy it and you own it; there is no hosted service. Bound to this machine by default.",
      "interval.label": "Interval",
      "dash.title": "Observation register",
      "dash.lede": "What is stored on disk, per market. This page never fetches or writes.",
      "dash.market": "Market",
      "dash.session": "Latest session",
      "dash.freshness": "Freshness",
      "dash.available_at": "Available at",
      "dash.observed": "Observed",
      "dash.bars_count": "Bars",
      "dash.instruments": "Instr.",
      "dash.range": "Range",
      "dash.flags_count": "Flags",
      "dash.note_freshness": "Freshness: provisional rows may later be replaced by final rows. Freshness is not a quality flag.",
      "dash.note_coverage": "Observed: instruments with a row for the latest session. Not a completeness guarantee.",
      "dash.flags_section": "Quality flags",
      "dash.quota_section": "Quota",
      "dash.provider": "Provider",
      "dash.used": "Used",
      "dash.remaining": "Remaining",
      "dash.penalized": "Penalized until",
      "dash.open_flags": "Open flags register ›",
      "dash.quota_note": "Counts from the local quota store. Keys are never shown.",
      "dash.no_markets": "No stored markets yet. Run `openbars run` in a terminal, then reload. This page never fetches.",
      "dash.no_quota": "No quota store found. The read-only server never creates one.",
      "fresh.final": "final",
      "fresh.provisional": "provisional",
      "state.loading": "Reading stored data…",
      "state.error": "Couldn't read data.",
      "chart.title": "Chart",
      "chart.lede": "Canonical bars by default. Switch the source to inspect one provider's raw bars; missing sessions stay as gaps.",
      "chart.symbol": "Symbol or id",
      "chart.source": "Source",
      "chart.start": "Start",
      "chart.end": "End",
      "chart.load": "Load",
      "chart.reading": "Reading",
      "chart.available_at": "Available at",
      "chart.flags": "Flags",
      "chart.remarks": "Remarks",
      "chart.tier_note": "Provisional is not final. Freshness is separate from quality flags and coverage.",
      "chart.raw_suffix": "raw, not filled",
      "chart.pick": "Enter a symbol or instrument id, then Load.",
      "chart.empty": "No bars for this selection.",
      "chart.no_hourly": "No canonical hourly bars are available.",
      "chart.unknown_alignment": "Hourly alignment is unknown; missing hours are unavailable.",
      "chart.gaps": "Missing sessions",
      "chart.hours": "Missing hours",
      "chart.missing_fields": "Bars with missing fields",
      "chart.bars_daily_table": "Daily bars",
      "chart.bars_daily_caption": "Loaded daily bars data",
      "chart.bars_hourly_table": "Hourly bars (New York time)",
      "chart.bars_hourly_caption": "Loaded hourly bars data in New York time",
      "chart.bars_date": "Date",
      "chart.bars_time_ny": "Time NY",
      "chart.bars_session": "Session",
      "chart.bars_open": "O",
      "chart.bars_high": "H",
      "chart.bars_low": "L",
      "chart.bars_close": "C",
      "chart.bars_volume": "V",
      "chart.bars_freshness": "Freshness",
      "chart.bars_flags": "Flags",
      "flags.title": "Quality flags",
      "flags.lede": "Canonical bars carrying a quality flag. Read-only.",
      "flags.session": "Session",
      "flags.market": "Market",
      "flags.symbol": "Symbol",
      "flags.instrument": "Instrument",
      "flags.names": "Flags",
      "flags.all": "all",
      "flags.empty": "No flagged bars in this window.",
      "flags.showing": "Showing {from}–{to} of {total}.",
      "page.prev": "Previous 200",
      "page.next": "Next 200",
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
      "flag.suspended": "suspended"
    },
    zh: {
      "brand": "OpenBars",
      "nav.dashboard": "看板",
      "nav.chart": "K 线",
      "nav.flags": "质量标记",
      "header.readonly": "只读",
      "header.station_title": "只展示已落盘数据，不抓取、不写入。",
      "footer.api": "/api/v1 是自托管的公开 API：谁下载谁部署，可完全私有，没有托管服务。默认只绑定本机。",
      "interval.label": "周期",
      "dash.title": "观测记录",
      "dash.lede": "按市场查看已落盘的数据。本页不会抓取或写入。",
      "dash.market": "市场",
      "dash.session": "最新交易日",
      "dash.freshness": "时效性",
      "dash.available_at": "可用时间",
      "dash.observed": "观察覆盖",
      "dash.bars_count": "K 线条数",
      "dash.instruments": "品种数",
      "dash.range": "范围",
      "dash.flags_count": "标记",
      "dash.note_freshness": "时效性：临时数据之后可能被正式数据替换。时效性不是质量标记。",
      "dash.note_coverage": "观察覆盖：最新交易日有数据的品种数，不保证完整。",
      "dash.flags_section": "质量标记",
      "dash.quota_section": "额度",
      "dash.provider": "渠道",
      "dash.used": "已用",
      "dash.remaining": "剩余",
      "dash.penalized": "惩罚至",
      "dash.open_flags": "查看质量标记 ›",
      "dash.quota_note": "来自本机额度库的计数，不显示任何密钥。",
      "dash.no_markets": "还没有已存储的市场。请在终端运行 `openbars run` 后刷新。本页不会抓取。",
      "dash.no_quota": "未找到额度库。只读服务不会创建它。",
      "fresh.final": "正式",
      "fresh.provisional": "临时",
      "state.loading": "正在读取已存数据…",
      "state.error": "无法读取数据。",
      "chart.title": "K 线",
      "chart.lede": "默认查看聚合的 canonical 数据。切换源检查单个渠道的原始数据；缺失交易日显示为缺口。",
      "chart.symbol": "代码或品种 id",
      "chart.source": "数据源",
      "chart.start": "开始",
      "chart.end": "结束",
      "chart.load": "加载",
      "chart.reading": "读数",
      "chart.available_at": "可用时间",
      "chart.flags": "标记",
      "chart.remarks": "备注",
      "chart.tier_note": "临时不等于正式。时效性与质量标记、覆盖率分开显示。",
      "chart.raw_suffix": "原始数据，未填充",
      "chart.pick": "输入代码或品种 id，然后加载。",
      "chart.empty": "没有符合条件的 K 线。",
      "chart.no_hourly": "暂无规范层小时线。",
      "chart.unknown_alignment": "小时线对齐方式未知，无法显示缺失时段。",
      "chart.gaps": "缺失交易日",
      "chart.hours": "缺失时段",
      "chart.missing_fields": "缺字段的 K 线",
      "chart.bars_daily_table": "日线数据",
      "chart.bars_daily_caption": "已加载的日线数据",
      "chart.bars_hourly_table": "小时线（纽约时间）",
      "chart.bars_hourly_caption": "已加载的小时线数据（纽约时间）",
      "chart.bars_date": "日期",
      "chart.bars_time_ny": "纽约时间",
      "chart.bars_session": "交易时段",
      "chart.bars_open": "开",
      "chart.bars_high": "高",
      "chart.bars_low": "低",
      "chart.bars_close": "收",
      "chart.bars_volume": "量",
      "chart.bars_freshness": "时效性",
      "chart.bars_flags": "标记",
      "flags.title": "质量标记",
      "flags.lede": "带质量标记的 canonical K 线。只读。",
      "flags.session": "交易日",
      "flags.market": "市场",
      "flags.symbol": "代码",
      "flags.instrument": "品种",
      "flags.names": "标记",
      "flags.all": "全部",
      "flags.empty": "当前窗口没有带标记的 K 线。",
      "flags.showing": "第 {from}–{to} 条，共 {total} 条。",
      "page.prev": "前 200 条",
      "page.next": "后 200 条",
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
      "flag.suspended": "停牌"
    }
  };

  var lang = "en";
  var chartApi = null;
  var candleSeries = null;

  function detectLang() {
    try {
      var saved = localStorage.getItem("openbars.lang");
      if (saved === "zh" || saved === "en") return saved;
    } catch (err) {}
    var nav = (navigator.language || "en").toLowerCase();
    return nav.indexOf("zh") === 0 ? "zh" : "en";
  }

  function t(key, vars) {
    var table = I18N[lang] || I18N.en;
    var str = table[key] || I18N.en[key] || key;
    if (vars) {
      Object.keys(vars).forEach(function (k) {
        str = str.replace("{" + k + "}", vars[k]);
      });
    }
    return str;
  }

  function setLang(next) {
    lang = next === "zh" ? "zh" : "en";
    try {
      localStorage.setItem("openbars.lang", lang);
    } catch (err) {}
    document.documentElement.lang = lang === "zh" ? "zh-Hans" : "en";
    document.querySelectorAll("[data-i18n]").forEach(function (el) {
      el.textContent = t(el.getAttribute("data-i18n"));
    });
    document.querySelectorAll("[data-i18n-title]").forEach(function (el) {
      el.title = t(el.getAttribute("data-i18n-title"));
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
    var tabs = document.querySelectorAll(".tabs a[data-p]");
    tabs.forEach(function (el) {
      var target = el.getAttribute("data-p");
      var match = target === "dash" ? path === "/" : path === "/" + target;
      el.toggleAttribute("aria-current", match ? "page" : "");
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

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function flagTier(name) {
    var alert = ["divergent", "spike_suspect"];
    var caution = ["volume_missing", "stale", "gap", "filled_from_fallback", "adjustment_unverified", "revised"];
    var note = ["single_source", "limit_up", "limit_down", "suspended"];
    if (alert.indexOf(name) !== -1) return "alert";
    if (caution.indexOf(name) !== -1) return "caution";
    if (note.indexOf(name) !== -1) return "note";
    return "note";
  }

  function remarkMark(name) {
    var tier = flagTier(name);
    return '<span class="rm ' + tier + '">[' + escapeHtml(t("flag." + name)) + "]</span>";
  }

  function formatCompactNum(n) {
    if (n == null || n === "") return "—";
    n = Number(n);
    if (n >= 1e6) return (n / 1e6).toFixed(2).replace(/\.?0+$/, "") + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(2).replace(/\.?0+$/, "") + "K";
    return String(n);
  }

  function formatTime(date, session) {
    if (!date) return "";
    var d = new Date(date);
    var opts = {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false
    };
    return new Intl.DateTimeFormat(lang === "zh" ? "zh-CN" : "en-US", opts).format(d) +
      (session ? " " + escapeHtml(session) : "");
  }

  function formatFreshness(freshness) {
    if (!freshness || typeof freshness !== "object") return "";
    var parts = [];
    ["final", "provisional"].forEach(function (tier) {
      if (freshness[tier]) {
        var cssClass = tier === "final" ? "final" : "prov";
        var label = t("fresh." + tier);
        parts.push('<span class="fr ' + cssClass + '"><i></i><b>' + freshness[tier] + '</b> ' + label + '</span>');
      }
    });
    return parts.join("");
  }

  function showPage(page) {
    document.querySelectorAll("main > div[id^='p-']").forEach(function (el) {
      el.hidden = true;
    });
    var pageEl = document.getElementById("p-" + page);
    if (pageEl) pageEl.hidden = false;
    markNav();
  }

  function queryParams() {
    return new URLSearchParams(location.search);
  }

  function renderDashboard() {
    var interval = queryParams().get("interval") === "1h" ? "1h" : "1d";
    var marketRows = document.getElementById("market-rows");
    var flagsTally = document.getElementById("flags-tally");
    var quotaRows = document.getElementById("quota-rows");

    marketRows.innerHTML = '<tr><td colspan="9" style="text-align: center; padding: 20px; color: var(--pencil);">' + t("state.loading") + "</td></tr>";

    Promise.all([
      apiGet("/api/v1/markets?interval=" + interval),
      apiGet("/api/v1/flags?limit=1&interval=" + interval),
      apiGet("/api/v1/quota")
    ])
      .then(function (results) {
        var markets = results[0].markets || [];
        var flags = results[1];
        var quota = results[2];

        if (!markets.length) {
          marketRows.innerHTML = '<tr><td colspan="9" style="padding: 20px; color: var(--pencil);">' + t("dash.no_markets") + "</td></tr>";
        } else {
          marketRows.innerHTML = markets
            .map(function (m) {
              var flagsTotal = Object.values(m.flags || {}).reduce(function (a, b) { return a + b; }, 0);
              var flagsLink = flagsTotal > 0 ? '<a class="flagcount" href="/flags?interval=' + interval + '">' + flagsTotal + ' ›</a>' : '<span class="zero">0</span>';
              var firstSession = m.first_session || "—";
              var lastSession = m.last_session || "—";
              var range = firstSession === "—" || lastSession === "—" ? "—" : firstSession + " → " + lastSession;

              return '<tr>' +
                '<td class="mk" data-l="' + t("dash.market") + '">' + escapeHtml((m.market || "").toUpperCase()) + '</td>' +
                '<td data-l="' + t("dash.session") + '">' + escapeHtml(m.latest_session || "—") + '</td>' +
                '<td data-l="' + t("dash.freshness") + '">' + formatFreshness(m.latest_freshness) + '</td>' +
                '<td data-l="' + t("dash.available_at") + '" title="' + escapeHtml(m.latest_available_at || "") + '">' + escapeHtml(m.latest_available_at ? m.latest_available_at.substring(0, 16) : "—") + '</td>' +
                '<td class="num" data-l="' + t("dash.observed") + '">' + ((m.observed_session_coverage && m.observed_session_coverage.present) || 0) + ' / ' + ((m.observed_session_coverage && m.observed_session_coverage.instruments) || 0) + '</td>' +
                '<td class="num" data-l="' + t("dash.bars_count") + '" title="' + escapeHtml(m.bars || "") + '">' + formatCompactNum(m.bars) + '</td>' +
                '<td class="num" data-l="' + t("dash.instruments") + '">' + (m.instruments || 0) + '</td>' +
                '<td data-l="' + t("dash.range") + '">' + escapeHtml(range) + '</td>' +
                '<td class="num" data-l="' + t("dash.flags_count") + '">' + flagsLink + '</td>' +
                '</tr>';
            })
            .join("");
        }

        var counts = flags.counts || {};
        var nonZero = Object.keys(counts).filter(function (k) { return counts[k] > 0; });
        if (!nonZero.length) {
          flagsTally.innerHTML = '<tr><td colspan="2" style="text-align: center; padding: 12px; color: var(--pencil);">—</td></tr>';
        } else {
          var tiers = {
            alert: ["divergent", "spike_suspect"],
            caution: ["volume_missing", "stale", "gap", "filled_from_fallback", "adjustment_unverified", "revised"],
            note: ["single_source", "limit_up", "limit_down", "suspended"]
          };
          var sorted = [];
          Object.keys(tiers).forEach(function (tierName) {
            tiers[tierName].forEach(function (flag) {
              if (counts[flag]) {
                sorted.push({ flag: flag, count: counts[flag], tier: tierName });
              }
            });
          });
          sorted.sort(function (a, b) { return b.count - a.count; });

          flagsTally.innerHTML = sorted
            .map(function (item) {
              return '<tr><td>' + remarkMark(item.flag) + '</td><td class="flagcount">' + item.count + '</td></tr>';
            })
            .join("");
        }

        if (!quota.available || !quota.providers || !quota.providers.length) {
          quotaRows.innerHTML = '<tr><td colspan="4" style="padding: 12px; color: var(--pencil);">' + t("dash.no_quota") + "</td></tr>";
        } else {
          quotaRows.innerHTML = quota.providers
            .map(function (p) {
              var usedStrs = p.used ? Object.keys(p.used).map(function (k) { return k + ": " + p.used[k]; }) : [];
              var remStrs = p.remaining ? Object.keys(p.remaining).map(function (k) { return k + ": " + p.remaining[k]; }) : [];
              return '<tr>' +
                '<td class="mono">' + escapeHtml(p.provider || p.id || "") + '</td>' +
                '<td>' + usedStrs.map(function (s) { return '<span class="stack">' + escapeHtml(s) + '</span>'; }).join("") + '</td>' +
                '<td>' + remStrs.map(function (s) { return '<span class="stack">' + escapeHtml(s) + '</span>'; }).join("") + '</td>' +
                '<td>' + escapeHtml(p.penalized_until || "—") + '</td>' +
                '</tr>';
            })
            .join("");
        }
      })
      .catch(function (err) {
        marketRows.innerHTML = '<tr><td colspan="9" style="padding: 12px; color: var(--remark);">Error: ' + escapeHtml(err.message) + '</td></tr>';
      });
  }

  function renderFlags() {
    var interval = queryParams().get("interval") === "1h" ? "1h" : "1d";
    var flag = queryParams().get("flag") || "";
    var offset = Number(queryParams().get("offset")) || 0;

    var chipsDiv = document.getElementById("flag-chips");
    var flagRows = document.getElementById("flag-rows");
    var pagerDiv = document.getElementById("flag-pager");

    flagRows.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 20px; color: var(--pencil);">' + t("state.loading") + "</td></tr>";

    apiGet("/api/v1/flags?interval=" + interval + "&limit=1")
      .then(function (body) {
        var counts = body.counts || {};
        var allCount = Object.values(counts).reduce(function (a, b) { return a + b; }, 0);
        var chips = '<span class="lbl" id="fl">' + t("flags.lede").split(" ")[0] + '</span><div class="seg" role="radiogroup" aria-labelledby="fl">';
        chips += '<label><input type="radio" name="f" value=""' + (!flag ? " checked" : "") + '><span>' + t("flags.all") + ' ' + allCount + '</span></label>';

        var tiers = {
          alert: ["divergent", "spike_suspect"],
          caution: ["volume_missing", "stale", "gap", "filled_from_fallback", "adjustment_unverified", "revised"],
          note: ["single_source", "limit_up", "limit_down", "suspended"]
        };
        Object.keys(tiers).forEach(function (tierName) {
          tiers[tierName].forEach(function (f) {
            if (counts[f]) {
              chips += '<label><input type="radio" name="f" value="' + f + '"' + (flag === f ? " checked" : "") + '><span>' + t("flag." + f) + ' ' + counts[f] + '</span></label>';
            }
          });
        });
        chips += '</div>';
        chipsDiv.innerHTML = chips;

        document.querySelectorAll(".chips input[name='f']").forEach(function (inp) {
          inp.addEventListener("change", function () {
            var params = new URLSearchParams();
            params.set("interval", interval);
            if (this.value) params.set("flag", this.value);
            history.replaceState(null, "", "/flags?" + params.toString());
            renderFlags();
          });
        });

        var apiPath = "/api/v1/flags?interval=" + interval + "&offset=" + offset + "&limit=200";
        if (flag) apiPath += "&flag=" + encodeURIComponent(flag);
        return apiGet(apiPath);
      })
      .then(function (body) {
        var items = body.items || [];
        var total = body.total || 0;

        if (!items.length) {
          flagRows.innerHTML = '<tr><td colspan="5" style="padding: 20px; color: var(--pencil);">' + t("flags.empty") + "</td></tr>";
          pagerDiv.innerHTML = "";
        } else {
          flagRows.innerHTML = items
            .map(function (item) {
              var sessionStr = interval === "1h" ? formatTime(item.ts_start, item.session) : item.session_date;
              return '<tr>' +
                '<td data-l="' + t("flags.session") + '">' + escapeHtml(sessionStr) + '</td>' +
                '<td data-l="' + t("flags.market") + '">' + escapeHtml((item.market || "").toUpperCase()) + '</td>' +
                '<td data-l="' + t("flags.symbol") + '" class="mono">' + escapeHtml(item.symbol || "") + '</td>' +
                '<td data-l="' + t("flags.instrument") + '" class="mono">' + escapeHtml(item.instrument_id || "") + '</td>' +
                '<td data-l="' + t("flags.names") + '">' + (item.flags || []).map(remarkMark).join("") + '</td>' +
                '</tr>';
            })
            .join("");

          var from = offset + 1;
          var to = Math.min(offset + 200, total);
          var showing = t("flags.showing", { from: from, to: to, total: total });
          var pagerHtml = '<span>' + showing + '</span>';
          if (offset > 0) {
            pagerHtml += '<button class="ghost" type="button" data-offset="' + Math.max(0, offset - 200) + '">' + t("page.prev") + '</button>';
          }
          if (to < total) {
            pagerHtml += '<button class="ghost" type="button" data-offset="' + (offset + 200) + '">' + t("page.next") + '</button>';
          }
          pagerDiv.innerHTML = pagerHtml;

          document.querySelectorAll("#flag-pager .ghost").forEach(function (btn) {
          btn.addEventListener("click", function () {
            var newOffset = Number(this.getAttribute("data-offset"));
            var params = new URLSearchParams();
            params.set("interval", interval);
            if (flag) params.set("flag", flag);
            if (newOffset) params.set("offset", newOffset);
            history.replaceState(null, "", "/flags?" + params.toString());
            renderFlags();
          });
        });
        }
      })
      .catch(function (err) {
        flagRows.innerHTML = '<tr><td colspan="5" style="padding: 12px; color: var(--remark);">Error: ' + escapeHtml(err.message) + '</td></tr>';
      });
  }

  var chartRequestSequence = 0;

  function renderChart() {
    var q = queryParams();
    var initial = q.get("id") || q.get("symbol") || "";
    var initialSource = q.get("source") || "canonical";
    var initialStart = q.get("start") || "";
    var initialEnd = q.get("end") || "";
    var interval = q.get("interval") === "1h" ? "1h" : "1d";

    var chartRecord = document.getElementById("chart-record");
    var chartSource = document.getElementById("chart-source");
    var chartQ = document.getElementById("chart-q");
    var chartStart = document.getElementById("chart-start");
    var chartEnd = document.getElementById("chart-end");
    var chartRemarks = document.getElementById("chart-remarks");

    chartQ.value = initial;
    chartStart.value = initialStart;
    chartEnd.value = initialEnd;

    var radios = document.querySelectorAll("input[name='iv2']");
    radios.forEach(function (r) { r.checked = r.value === interval; });

    document.getElementById("chart-form").addEventListener("submit", function (ev) {
      ev.preventDefault();
      loadChartData();
    });

    chartSource.addEventListener("change", resetChartWindow);

    radios.forEach(function (r) {
      r.addEventListener("change", resetChartWindow);
    });

    if (initial) loadChartData();
  }

  function resetChartWindow() {
    document.getElementById("chart-start").value = "";
    document.getElementById("chart-end").value = "";
    loadChartData();
  }

  function loadChartData() {
    var requestSeq = ++chartRequestSequence;
    var q = document.getElementById("chart-q").value.trim();
    var source = document.getElementById("chart-source").value || "canonical";
    var start = document.getElementById("chart-start").value;
    var end = document.getElementById("chart-end").value;
    var interval = document.querySelector("input[name='iv2']:checked").value;
    var chartRecord = document.getElementById("chart-record");
    var chartStart = document.getElementById("chart-start");
    var chartEnd = document.getElementById("chart-end");

    if (!q) {
      chartRecord.textContent = t("chart.pick");
      return;
    }

    var params = new URLSearchParams();
    if (q.indexOf(":") !== -1) params.set("id", q);
    else params.set("symbol", q);
    params.set("source", source);
    params.set("interval", interval);
    if (start) params.set("start", start);
    if (end) params.set("end", end);
    history.replaceState(null, "", "/chart?" + params.toString());

    apiGet("/api/v1/bars?" + params.toString())
      .then(function (body) {
        if (requestSeq !== chartRequestSequence) return;

        if (body.start) chartStart.value = body.start;
        if (body.end) chartEnd.value = body.end;

        fillSources(body.available_sources, body.source);

        var sym = body.symbol || q;
        var id = body.instrument_id || q;
        var market = body.market || "";
        var adjustment = body.adjustment || "";
        var isCanonical = body.source === "canonical";

        var recordLine = '<span class="sym">' + escapeHtml(sym) + '</span>' +
          '<span class="mono pencil">' + escapeHtml(id) + '</span>' +
          '<span class="sep">·</span>' +
          '<span>' + escapeHtml(market) + '</span>' +
          '<span class="sep">·</span>' +
          '<span>' + escapeHtml(source) + '</span>';

        if (adjustment) recordLine += '<span class="sep">·</span><span>' + escapeHtml(adjustment) + '</span>';

        if (body.start && body.end) {
          recordLine += '<span class="sep">·</span><span>' + escapeHtml(body.start) + ' → ' + escapeHtml(body.end) + '</span>';
        }

        if (!isCanonical) {
          recordLine += '<span class="sep">·</span><span class="pencil">' + t("chart.raw_suffix") + '</span>';
        }

        chartRecord.innerHTML = recordLine;

        var bars = body.bars || [];
        var missing = interval === "1h" ? (body.missing_hours || []) : (body.missing_sessions || []);

        // Clear the reading slip first; drawChart() refills it from the latest bar.
        updateReadingSlip(null);
        drawChart(bars, missing, interval);
        populateBarTables(bars, interval);

        // Set aria-label for chart accessibility
        var chartEl = document.getElementById("chart");
        var rangeStr = body.start && body.end ? (body.start + " to " + body.end) : "unknown range";
        var barCountStr = bars.length.toString();
        var gapCountStr = missing.length.toString();
        var ariaLabel = sym + ", " + rangeStr + ", " + barCountStr + " bars, " + gapCountStr + " gaps";
        if (chartEl) {
          chartEl.setAttribute("aria-label", ariaLabel);
        }

        var remarksHtml = "";
        if (missing.length) {
          remarksHtml += '<p><span class="k">' + t(interval === "1h" ? "chart.hours" : "chart.gaps") + ' (' + missing.length + '):</span> ' +
            escapeHtml(missing.map(function (m) { return interval === "1h" ? formatTime(m.ts_start, m.session) : m; }).join(", ")) + '</p>';
        }
        var fieldGaps = bars.filter(function (b) { return (b.missing_fields || []).length; });
        if (fieldGaps.length) {
          remarksHtml += '<p><span class="k">' + t("chart.missing_fields") + ':</span> ' +
            escapeHtml(fieldGaps.map(function (b) {
              var dateStr = interval === "1h" ? formatTime(b.ts_start, b.session) : b.session_date;
              return dateStr + ' (' + (b.missing_fields || []).join(", ") + ')';
            }).join("; ")) + '</p>';
        }

        var remarksSection = document.getElementById("chart-remarks");
        if (remarksHtml) {
          remarksSection.hidden = false;
          remarksSection.innerHTML = '<h2 data-i18n="chart.remarks">' + t("chart.remarks") + '</h2>' + remarksHtml;
        } else {
          remarksSection.hidden = true;
        }
      })
      .catch(function (err) {
        if (requestSeq !== chartRequestSequence) return;
        chartRecord.innerHTML = '<span style="color: var(--remark);">Error: ' + escapeHtml(err.message) + '</span>';
      });
  }

  function fillSources(available, selected) {
    var sel = document.getElementById("chart-source");
    var current = selected || sel.value || "canonical";
    var names = available && available.length ? available.slice() : ["canonical"];
    if (names.indexOf("canonical") === -1) names.unshift("canonical");
    if (current && names.indexOf(current) === -1) names.push(current);
    sel.innerHTML = names.map(function (name) {
      return '<option value="' + escapeHtml(name) + '">' + escapeHtml(name) + '</option>';
    }).join("");
    sel.value = current;
  }

  function formatNumber(value) {
    if (value === 0 || value === "0") return "0";
    if (value == null) return "—";
    return value.toLocaleString(lang === "zh" ? "zh-CN" : "en-US");
  }

  function populateBarTables(bars, interval) {
    var dailyContainer = document.getElementById("bar-table-daily");
    var hourlyContainer = document.getElementById("bar-table-hourly");

    if (!bars || !bars.length) {
      document.getElementById("daily-bars-rows").innerHTML = "";
      document.getElementById("hourly-bars-rows").innerHTML = "";
      if (dailyContainer) dailyContainer.hidden = true;
      if (hourlyContainer) hourlyContainer.hidden = true;
      return;
    }

    if (interval === "1h") {
      // Populate hourly bars table
      var hourlyRows = document.getElementById("hourly-bars-rows");
      hourlyRows.innerHTML = bars
        .map(function (bar) {
          var timeStr = formatTime(bar.ts_start, bar.session);
          var session = escapeHtml(bar.session || "—");
          var open = formatNumber(bar.open);
          var high = formatNumber(bar.high);
          var low = formatNumber(bar.low);
          var close = formatNumber(bar.close);
          var volume = formatNumber(bar.volume);
          var flags = bar.flags && bar.flags.length ? bar.flags.map(remarkMark).join("") : "—";

          return '<tr>' +
            '<td data-l="' + t("chart.bars_time_ny") + '">' + escapeHtml(timeStr) + '</td>' +
            '<td data-l="' + t("chart.bars_session") + '">' + session + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_open") + '">' + open + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_high") + '">' + high + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_low") + '">' + low + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_close") + '">' + close + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_volume") + '">' + volume + '</td>' +
            '<td data-l="' + t("chart.bars_flags") + '">' + flags + '</td>' +
            '</tr>';
        })
        .join("");
      document.getElementById("daily-bars-rows").innerHTML = "";
      if (dailyContainer) dailyContainer.hidden = true;
      if (hourlyContainer) hourlyContainer.hidden = false;
    } else {
      // Populate daily bars table
      var dailyRows = document.getElementById("daily-bars-rows");
      dailyRows.innerHTML = bars
        .map(function (bar) {
          var dateStr = escapeHtml(bar.session_date || "—");
          var open = formatNumber(bar.open);
          var high = formatNumber(bar.high);
          var low = formatNumber(bar.low);
          var close = formatNumber(bar.close);
          var volume = formatNumber(bar.volume);
          var freshness = bar.freshness ? '<span class="fr ' + (bar.freshness === "final" ? "final" : "prov") + '"><i></i>' + t("fresh." + (bar.freshness === "final" ? "final" : "provisional")) + '</span>' : "—";
          var flags = bar.flags && bar.flags.length ? bar.flags.map(remarkMark).join("") : "—";

          return '<tr>' +
            '<td data-l="' + t("chart.bars_date") + '">' + dateStr + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_open") + '">' + open + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_high") + '">' + high + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_low") + '">' + low + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_close") + '">' + close + '</td>' +
            '<td class="num" data-l="' + t("chart.bars_volume") + '">' + volume + '</td>' +
            '<td data-l="' + t("chart.bars_freshness") + '">' + freshness + '</td>' +
            '<td data-l="' + t("chart.bars_flags") + '">' + flags + '</td>' +
            '</tr>';
        })
        .join("");
      document.getElementById("hourly-bars-rows").innerHTML = "";
      if (dailyContainer) dailyContainer.hidden = false;
      if (hourlyContainer) hourlyContainer.hidden = true;
    }
  }

  function drawChart(bars, missing, interval) {
    var hourly = interval === "1h";

    function timeOf(item) {
      if (hourly) return Math.floor(new Date(item.ts_start).getTime() / 1000);
      return item.session_date;
    }

    var host = document.getElementById("chart");
    if (!window.LightweightCharts) {
      host.textContent = "LightweightCharts not available";
      return;
    }

    if (chartApi) {
      chartApi.remove();
      chartApi = null;
      candleSeries = null;
    }

    var root = document.documentElement;
    var styles = getComputedStyle(root);
    var paperColor = styles.getPropertyValue("--paper").trim();
    var pencilColor = styles.getPropertyValue("--pencil").trim();
    var ruleColor = styles.getPropertyValue("--rule").trim();
    var formColor = styles.getPropertyValue("--form").trim();
    var entryColor = styles.getPropertyValue("--entry").trim();

    chartApi = LightweightCharts.createChart(host, {
      width: host.clientWidth || 800,
      height: hourly ? 320 : 460,
      layout: {
        background: { color: paperColor },
        textColor: pencilColor
      },
      grid: {
        vertLines: { color: ruleColor },
        horzLines: { color: ruleColor }
      },
      rightPriceScale: { borderColor: formColor },
      timeScale: { borderColor: formColor, timeVisible: hourly }
    });

    candleSeries = chartApi.addCandlestickSeries({
      upColor: paperColor,
      downColor: entryColor,
      borderUpColor: entryColor,
      borderDownColor: entryColor,
      wickUpColor: entryColor,
      wickDownColor: entryColor,
      borderVisible: true
    });

    var chartData = bars.map(function (b) {
      return {
        time: timeOf(b),
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close
      };
    });

    if (chartData.length) candleSeries.setData(chartData);

    // Lightweight Charts v4 markers: build the list, then call setMarkers() once.
    var gapColor = styles.getPropertyValue("--remark").trim();
    var markers = [];
    missing.forEach(function (m) {
      markers.push({
        time: timeOf(m),
        position: "belowBar",
        color: gapColor,
        shape: "circle",
        text: t("flag.gap")
      });
    });

    bars.forEach(function (b) {
      if (b.flags && b.flags.length) {
        markers.push({
          time: timeOf(b),
          position: "aboveBar",
          color: gapColor,
          shape: "arrowDown",
          text: b.flags.map(function (name) { return t("flag." + name); }).join(", ")
        });
      }
    });

    markers.sort(function (a, b) { return a.time < b.time ? -1 : a.time > b.time ? 1 : 0; });
    if (markers.length) candleSeries.setMarkers(markers);

    var latestBar = null;
    chartApi.subscribeCrosshairMove(function (param) {
      if (param.point === undefined || !param.time || param.seriesPrices.size === 0) {
        if (bars.length > 0) {
          latestBar = bars[bars.length - 1];
          updateReadingSlip(latestBar);
        }
        return;
      }
      var barIdx = bars.findIndex(function (b) { return timeOf(b) === param.time; });
      if (barIdx >= 0) {
        latestBar = bars[barIdx];
        updateReadingSlip(latestBar);
      }
    });

    if (bars.length > 0) {
      latestBar = bars[bars.length - 1];
      updateReadingSlip(latestBar);
    }

    chartApi.timeScale().fitContent();
  }

  function updateReadingSlip(bar) {
    if (!bar) {
      document.getElementById("s-date").textContent = "—";
      document.getElementById("s-o").textContent = "—";
      document.getElementById("s-h").textContent = "—";
      document.getElementById("s-l").textContent = "—";
      document.getElementById("s-c").textContent = "—";
      document.getElementById("s-v").textContent = "—";
      document.getElementById("s-fr").innerHTML = "";
      document.getElementById("s-av").textContent = "—";
      document.getElementById("s-fl").textContent = "—";
      return;
    }

    var dateStr = document.querySelector("input[name='iv2']:checked").value === "1h" ?
      formatTime(bar.ts_start, bar.session) :
      bar.session_date;
    document.getElementById("s-date").textContent = dateStr;

    document.getElementById("s-o").textContent = bar.open != null ? bar.open.toLocaleString(lang === "zh" ? "zh-CN" : "en-US") : "—";
    document.getElementById("s-h").textContent = bar.high != null ? bar.high.toLocaleString(lang === "zh" ? "zh-CN" : "en-US") : "—";
    document.getElementById("s-l").textContent = bar.low != null ? bar.low.toLocaleString(lang === "zh" ? "zh-CN" : "en-US") : "—";
    document.getElementById("s-c").textContent = bar.close != null ? bar.close.toLocaleString(lang === "zh" ? "zh-CN" : "en-US") : "—";
    document.getElementById("s-v").textContent = bar.volume != null ? bar.volume.toLocaleString(lang === "zh" ? "zh-CN" : "en-US") : "—";

    if (bar.freshness) {
      var freshnessClass = bar.freshness === "provisional" ? "prov" : "final";
      var freshnessLabel = t("fresh." + (bar.freshness === "provisional" ? "provisional" : "final"));
      document.getElementById("s-fr").innerHTML =
        '<span class="fr ' + freshnessClass + '"><i></i>' + freshnessLabel + '</span>';
    } else {
      document.getElementById("s-fr").innerHTML = '';
    }

    document.getElementById("s-av").textContent = bar.available_at ? bar.available_at.substring(0, 16) : "—";

    if (bar.flags && bar.flags.length) {
      document.getElementById("s-fl").innerHTML = bar.flags.map(remarkMark).join(" ");
    } else {
      document.getElementById("s-fl").textContent = "—";
    }
  }

  function init() {
    lang = detectLang();
    setLang(lang);

    // Set host address to current location.host
    var hostAddr = document.getElementById("host-addr");
    if (hostAddr) {
      hostAddr.textContent = location.host || "127.0.0.1:8787";
    }

    document.getElementById("lang-toggle").addEventListener("click", function () {
      lang = lang === "zh" ? "en" : "zh";
      setLang(lang);
    });

    var path = pathOf();
    var page = path === "/" ? "dash" : path.substring(1);

    showPage(page);

    var radios1 = document.querySelectorAll("input[name='iv1']");
    radios1.forEach(function (r) {
      r.addEventListener("change", function () {
        location.search = "?interval=" + this.value;
      });
    });

    var radios3 = document.querySelectorAll("input[name='iv3']");
    radios3.forEach(function (r) {
      r.addEventListener("change", function () {
        var q = new URLSearchParams(location.search);
        q.set("interval", this.value);
        history.replaceState(null, "", "/flags?" + q.toString());
        renderFlags();
      });
    });

    var interval = queryParams().get("interval") === "1h" ? "1h" : "1d";
    radios1.forEach(function (r) { r.checked = r.value === interval; });
    radios3.forEach(function (r) { r.checked = r.value === interval; });

    if (page === "dash") {
      renderDashboard();
    } else if (page === "flags") {
      renderFlags();
    } else if (page === "chart") {
      renderChart();
    }
  }

  window.addEventListener("DOMContentLoaded", init);
})();
