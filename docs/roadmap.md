# Roadmap

状态：pre-alpha。默认粒度日线；小时/分钟线为可选能力，开启时触发对应限额规则与回补预算确认。

| 版本 | 范围 | 关键交付 |
| --- | --- | --- |
| **v0.1** 美股（含 ETF）日线 | Alpaca SIP、Massive grouped daily、yfinance（2016 年前兜底） | 核心模型与接口、配置/CLI、QuotaGate、Orchestrator + Checkpoint、Parquet/DuckDB、InstrumentMaster（稳定 ID）、美股日历、公司行动 + 查询时复权、Reconciler + 质量标记、`verify` 对账 |
| **v0.2** 加密日线 | Binance data-api / data.binance.vision；OKX、Coinbase 备源 | UTC 切日、24/7 日历、权重制额度 |
| **v0.3** 美股小时线（可选）+ 实测手册 | Alpaca 30m → 1h 合成（9:30 对齐）、yfinance 1h | Resampler、预算确认、公开基准报告 |
| **v0.4** A 股日线 | Tushare（120 积分）、baostock、AKShare | 远程采集节点、午休日历、qfq/hfq 查询派生 |
| **v0.5** 港股日线 | AKShare（东财）、Yahoo .HK | 收市竞价口径；分钟线作为可选 |
| 之后 | HTTP 读取 API、Postgres 后端完善、更多 provider 插件 | — |

非目标：数据分发/托管、实时行情、逐笔、交易与回测。
