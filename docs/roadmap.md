# Roadmap

状态：pre-alpha。默认粒度日线；小时/分钟线为可选能力，开启时触发对应限额规则与回补预算确认。

| 版本 | 范围 | 关键交付 |
| --- | --- | --- |
| **v0.1** 美股（含 ETF）日线 | Alpaca SIP、Massive grouped daily、yfinance（2016 年前兜底） | 核心模型与接口、配置/CLI、QuotaGate、Orchestrator + Checkpoint、Parquet/DuckDB、InstrumentMaster（稳定 ID）、美股日历、公司行动 + 查询时复权、Reconciler + 质量标记、`verify` 对账 |
| **v0.2** 加密日线 | Binance data-api / data.binance.vision；OKX、Coinbase 备源 | UTC 切日、24/7 日历、权重制额度 |
| **v0.3** A 股日线 | 沪深交易日历、`cn:eq:` 证券 id 规则，随后是数据源、复权口径、涨跌停等 issue（Tushare 120 积分、baostock、AKShare） | 午休日历（XSHG，XSHE 为别名）、id 代际规则、远程采集节点、qfq/hfq 查询派生；设计见 [discussion #54](https://github.com/Liyuk/openbars/discussions/54) |
| **v0.4** 港股日线 | XHKG 日历与 `hk:eq:xhkg:` 五位代码，随后是免费数据源、复权口径、美股 ADR 关联 | 半日市与库内假期覆盖上限；设计见 [discussion #68](https://github.com/Liyuk/openbars/discussions/68)；分钟线作为可选 |
| **v0.5** 本机只读界面 | `openbars serve`、覆盖看板、K 线（默认可看 canonical，也可切换各渠道原始数据）、质量标记与额度快照 | 只允许本机回环访问；`/api/v1` 只供页面内部使用，本版不承诺稳定；设计见 [discussion #82](https://github.com/Liyuk/openbars/discussions/82) |
| **v0.6** 美股小时线（可选）+ 实测手册 | Massive / Alpaca 30m 合成 1h、yfinance / Twelve Data 1h；常规时段默认、可选对齐 | raw 采集、预算确认与断点续抓；[中英本地实测手册](measuring-hourly.md)；小时规范层及与日线一致性校验待 #116，看板 1h 待 #119；公开基准报告（自 v0.3 顺延） |
| 之后 | Postgres 后端完善、更多 provider 插件 | — |

非目标：数据分发/托管、实时行情、逐笔、交易与回测。
