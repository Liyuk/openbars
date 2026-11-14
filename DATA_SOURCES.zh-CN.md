# 数据源

[English](DATA_SOURCES.md) | **中文**

本页列出 OpenBars **目前在代码里实际支持**的全部数据源：每个源在本项目里提供什么市场与周期、是否需要密钥、当日会有什么表现。内容已对照各 provider 适配器（`src/openbars/providers/`）、CLI 接线（`src/openbars/cli/wiring.py`）、[`config.example.yaml`](config.example.yaml) 和 [docs/provider-keys.md](docs/provider-keys.md)（英文）核对。本页没有列出的渠道，OpenBars 不支持。

> **OpenBars 不分发行情数据，也没有托管的抓取服务。** 所有数据由你自己的机器、用你自己的账号和密钥抓取，存放在你自己的数据目录，并受各数据源条款约束。[在线演示](https://liyuk.github.io/openbars/)只展示合成的 `TEST*` 数据，不会连接任何数据源。

## 一览

| 数据源 | 配置键 | 市场与周期 | 密钥 / 接入 | CLI（`plan` / `run`）已接入 |
| --- | --- | --- | --- | --- |
| [Alpaca](#alpaca) | `alpaca` | 美股与 ETF：`1d`、`1h` | API Key ID 与 Secret | 是 |
| [Massive](#massive) | `massive` | 美股与 ETF：`1d`、`1h` | API key | 是 |
| [yfinance (Yahoo Finance)](#yfinance-yahoo-finance) | `yfinance` | 美股：`1d`、`1h`；港股：`1d` | 无 | 是 |
| [Twelve Data](#twelve-data) | `twelve_data` | 美股：仅 `1h` | API key | 是 |
| [Binance](#binance) | `binance` | 加密现货：`1d` | 无（公开行情） | 是 |
| [Coinbase Exchange](#coinbase-exchange) | `coinbase` | 加密现货：`1d` | 无（公开行情） | 是 |
| [BaoStock](#baostock) | `baostock` | A 股：`1d` | 无 | 是 |
| [AKShare A 股](#akshare-a-股) | `akshare_ashare` | A 股：`1d`（东方财富或新浪 feed） | 无 | 是 |
| [Tushare](#tushare) | `tushare` | A 股：`1d` | token | 是 |
| [AKShare 港股](#akshare-港股) | `akshare` | 港股：`1d` | 无 | **否**：仅适配器 |
| [Futu OpenD](#futu-opend) | `futu` | 港股：`1d` | 本地 OpenD 网关 | **否**：仅适配器 |
| [LongPort](#longport) | `longport` | 港股：`1d` | LongPort 账户凭据 | **否**：仅适配器 |

- 「CLI 已接入」指：在 `config.yaml` 里 `enabled` 之后，`cli/wiring.py` 会构造该 provider。标记为**否**的适配器存在、也注册为插件，但规划器不会给它们派任务，`openbars run` 也不会构造它们；可以在 Python 里调用，或自行显式接入。
- 周期只有 `1d`（默认路径）和 `1h`（仅美股，可选，需要 `--accept-budget`）。其他周期（`30m`、`15m`、`5m`、`1m`）没有任何 provider 实现。
- **未实现：** Alpha Vantage 与 Tiingo。它们只作为占位出现在 `.env.example`，或 `config.example.yaml` 里默认关闭的 `alpha_vantage` 条目中，**不能使用**。

可选依赖用来安装 SDK：`openbars[yfinance]`、`openbars[baostock]`、`openbars[akshare]`、`openbars[tushare]`（例如 `uv sync --extra baostock`）。Futu 与 LongPort 的 SDK 不是 OpenBars 的依赖，如果要调用这两个适配器，请自行安装。

## 当日数据：先 provisional，再 final

日线只有一条路径：取各源当前能提供的最新数据，没有模式开关。每根规范层（canonical）行单独带有 `freshness` 标签和可选的 `available_at`（见 [README](README.zh-CN.md) 的「当日模式」一节）：

- `freshness=provisional`：当日（或刚收盘）的临时 K 线，之后的运行可能将其覆盖；
- `freshness=final`：正式数据。之后的 `openbars run` 对同一交易日取到正式数据时，会替换临时行；读取时优先 final；
- 旧行缺列或为空时按 `final` 处理。

| 日线市场 | `provisional` 档 | `final` 档（代码阈值） |
| --- | --- | --- |
| 美股 | 盘中（T+0） | 次日美东 00:00 起 |
| 加密 | UTC 当日（T+0） | 次日 UTC 00:30 起 |
| A 股 | 盘中（T+0） | 收盘后 30 分钟 |
| 港股 | 盘中（T+0） | 实际收盘后 30 分钟 |

这些是 OpenBars 内部的**分档规则，不是数据源的交付 SLA，也不保证完整性。** 某个数据源在某个时刻是否真的已有当日 K 线是另一回事；各源的证据状态见 [docs/source-readiness.md](docs/source-readiness.md)（英文）。代码里只核实了两种行为：Massive 的全市场日线自次日美东 00:00 起就绪（更早的请求会抛出可重试的 `data_not_ready`），以及 Alpaca SIP 对当天的请求截止到 `now − 16 分钟`。其余源均为尽力获取，不承诺 T+0。配置里的 `schedule.daily_at` 只是展示字段，`openbars run` 何时开始由外部调度器决定（见 [docs/ARCHITECTURE.zh-CN.md](docs/ARCHITECTURE.zh-CN.md#调度)）。

## 各数据源详情

### Alpaca

- **提供：** 美股与 ETF 的日线与小时线，来自行情数据 API。feed 为 `sip`（示例配置的默认值）或 `iex`；不同 feed 的成交量永不混用。历史自 2016-01-01 起（适配器里免费档 SIP 的下限）。
- **接入：** 需要账号与密钥：`ALPACA_API_KEY_ID` 与 `ALPACA_API_SECRET_KEY`（也接受别名 `ALPACA_API_KEY` / `ALPACA_API_SECRET`）。默认节奏 200 次/分钟（示例配置收紧为 190）。
- **当日：** 包含当天的 SIP 请求截止到 `now − 16 分钟`（代码已核实），因此盘中或盘后可能出现 provisional 日线。盘后可得时间与完整性**未经验证**；final 档从次日美东 00:00 起。
- **条款：** 行情授权给账户持有人。见 [Alpaca disclosures](https://alpaca.markets/disclosures) 与[行情数据文档](https://docs.alpaca.markets/docs/about-market-data-api)。

### Massive

- **提供：** 美股与 ETF。日线来自全市场 grouped daily 接口（一次请求覆盖所有代码）；小时线由单只代码的 30 分钟聚合累加到小时网格。
- **接入：** 需要密钥：`MASSIVE_API_KEY`。免费 Basic 档约为两年（730 天）滚动窗口、5 次/分钟（默认节奏 4）；当前限额请看[定价页](https://massive.com/pricing)。
- **当日：** grouped daily 自次日美东 00:00 起就绪（代码已核实）；更早的请求以可重试错误失败，稍后再取，所以 Massive 日线只会在进入 final 档之后才被取到。小时线盘后可得时间未经验证。
- **条款：** 按定价/条款页，授权用于个人使用。

### yfinance (Yahoo Finance)

- **提供：** 美股与 ETF（`1d`，以及约最近 730 天的 `1h`）和港股日线。在美股它用来补足 2016 年以前的历史和主源缺口；在港股它是**主源**，也是规划器唯一使用的港股源。
- **接入：** 无需 key。安装可选依赖 `openbars[yfinance]`。没有公开的固定额度；默认 30 次/分钟只是保守设置，不是官方限制。
- **当日：** 适配器不强制可得延迟：尽力获取，不承诺 T+0。
- **说明：** 非官方的 Yahoo 数据，仅限个人使用，不是持牌的交易所行情。港股以港币存为不复权数据（`hk-daily-v1`），拆股复权在读取时派生。
- **条款：** [Yahoo 条款](https://policies.yahoo.com/us/en/yahoo/terms/index.htm)；另见 [yfinance 文档](https://ranaroussi.github.io/yfinance/)。

### Twelve Data

- **提供：** 只有美股小时线（`1h`，仅常规时段；无盘前盘后，无日线）。默认关闭。
- **接入：** 需要密钥：`TWELVE_DATA_API_KEY`。免费档：8 次/分钟，每个 UTC 日 800 次，每次最多 5000 根；历史取决于套餐和品种。
- **当日：** 不强制可得延迟：尽力获取。
- **条款：** [Twelve Data 条款](https://twelvedata.com/terms)与[定价](https://twelvedata.com/pricing)。

### Binance

- **提供：** 加密现货日 K。日线以 UTC 00:00 切分，周末也是交易日（`crypto-daily-v1`）。默认关闭。
- **接入：** 无需 key；使用 `https://data-api.binance.vision` 的公开行情（可用 `providers.binance.base_url` 修改）。请求权重按 IP 限制；历史取决于交易对的上市日期。
- **当日：** 当前 UTC 日在次日 UTC 00:30 之前为 `provisional`（分档规则）；数据源的实际时效未经验证。
- **说明：** 每个交易所是独立品种：Binance 的 K 线绝不与 Coinbase 合并。
- **条款：** [Binance 条款](https://www.binance.com/en/terms)。**不**假定允许再分发。

### Coinbase Exchange

- **提供：** `BASE-QUOTE` 产品的加密现货日 K（Coinbase Exchange 公开 REST）。每次请求最多 300 根，每 IP 10 次/秒。默认关闭。
- **接入：** 无需 key；公开行情。
- **当日 / 说明：** 与 Binance 相同（UTC 日，次日 UTC 00:30 之前为 `provisional`）；不与 Binance 合并。
- **条款：** [Coinbase 用户协议](https://www.coinbase.com/legal/user_agreement)。**不**假定允许再分发。

### BaoStock

- **提供：** A 股日线，**不复权**（`cn-daily-v1`）。同时获取复权因子和停牌字段，因此前/后复权（QFQ / HFQ）在读取时派生，从不落盘。默认关闭。
- **接入：** 无需 key（匿名登录）。安装 `openbars[baostock]`。代码写作 `sh.600000` 或 `600000.SH`；纯 6 位数字代码会被跳过。部分海外网络可能无法访问。
- **当日：** 收盘后 30 分钟进入 final 档（分档规则）；实际交付时间与完整性未经验证。
- **条款：** 仅限个人本地使用；网站禁止未经书面许可的复制、分发或商业使用。见 [BaoStock](https://www.baostock.com/)。

### AKShare A 股

- **提供：** A 股不复权日线，作为**低频备源**。feed 为 `eastmoney`（默认）或 `sina`。配置键 `akshare_ashare`。默认关闭。
- **接入：** 无需 key。安装 `openbars[akshare]`。A 股规范层是单源投影，所以**一次只启用一个** A 股源（BaoStock、AKShare 或 Tushare）。
- **当日：** 同 BaoStock（分档规则，交付时间未验证）。
- **条款：** AKShare 库本身是 MIT，但**不会为上游行情重新授权**。东方财富条款禁止未经书面同意再分发交易所行情，也禁止对站点造成不成比例的负载；新浪限制未经许可的抓取。见[东方财富免责声明](https://about.eastmoney.com/home/disclaimer)与 [AKShare 项目](https://akshare.akfamily.xyz/)。

### Tushare

- **提供：** A 股不复权日线，仅限免费档（约 120 积分：约 50 次/分钟、每天 8000 次）。适配器从不调用 `qfq`/`hfq` 或 `adj_factor`。默认关闭。
- **接入：** 需要 token：`TUSHARE_TOKEN`（在配置中以 `${TUSHARE_TOKEN}` 引用；绝不提交）。安装 `openbars[tushare]`。
- **当日：** 同 BaoStock（分档规则，交付时间未验证）。
- **条款：** 许可为个人、不可转让、非商业、可撤销；公开使用须按 Tushare 要求署名。见 [Tushare 条款](https://tushare.pro/document/1?doc_id=405)。

### AKShare 港股

- **状态：** 适配器（`providers/hk/akshare.py`，插件名 `akshare`）存在，港股合并（`hk-daily-v1`）把 `akshare/eastmoney/raw` 当作同市场的**佐证**来源。它**未接入 CLI**：规划器不会调度它，`openbars run` 也不会构造它。
- **提供：** 港股不复权日线（东方财富 `stock_hk_hist`），港币计价。
- **接入：** 无需 key；`openbars[akshare]`。
- **条款：** 同上文 AKShare / 东方财富的提醒；只在本地私有存储。

### Futu OpenD

- **状态：** 仅适配器，**未接入 CLI**（规划器不会把港股任务派给它；港股主源是 yfinance）。
- **提供：** 港股日 K，只请求不复权数据（`AuType.NONE`）。
- **接入：** 需要你自己运行并登录的本地 OpenD 网关（默认 `127.0.0.1:11111`；OpenBars 从不处理富途登录）。登录不等于拥有港股行情权限，行情级别、历史 K 额度以及内地/国际身份取决于你的账户和富途条款。`futu-api` SDK 不是 OpenBars 的依赖。
- **条款：** 富途用户协议、OpenAPI 协议以及交易所行情权利；不得擅自转发、转售或商业再分发。见 [Futu OpenAPI](https://openapi.futunn.com/)。

### LongPort

- **状态：** 仅适配器，**未接入 CLI**（同 Futu）。
- **提供：** 港股日 K，只请求不复权数据（`AdjustType.NoAdjust`）。
- **接入：** 需要你自己的 LongPort 账户并完成开发者验证；`app_key`、`app_secret` 与 `access_token` 通过 `${ENV}` 引用从环境变量读取（例如 `${LONGPORT_APP_KEY}`），绝不写明文。免费核心 API / 港股 LV1 与付费 LV2 以 LongPort 当前条款为准。`longport` SDK 不是 OpenBars 的依赖。
- **条款：** 通常为个人、可撤销、非商业、不可转让的许可；未经书面同意不得复制或再分发行情。见 [LongPort OpenAPI](https://open.longportapp.com/)。

## 条款与授权

- **OpenBars 不分发也不托管任何行情数据。** 仓库只包含代码、规则和文档；测试与演示数据都是合成的。
- 多数免费数据源只允许个人或内部使用，并禁止展示或再分发。**你有责任阅读并遵守各数据源的条款。** 库的开源许可（MIT、BSD、Apache）不会为其背后的行情重新授权。
- OpenBars 只把数据存放在你自己的数据目录。除非数据源条款明确允许，否则不要提交、公开发布或对外提供这些数据。
- OpenBars 与任何数据源均无关联，也不对准确性、完整性或可用性作任何保证。免费档、历史窗口与限额会变化，请以上面链接的官方页面为准。

## 另见

- [docs/provider-keys.md](docs/provider-keys.md)（英文）：申请方式、免费额度与密钥处理。
- [docs/source-readiness.md](docs/source-readiness.md)（英文）：各源的就绪情况与证据状态。
- [docs/ARCHITECTURE.zh-CN.md](docs/ARCHITECTURE.zh-CN.md#新增数据源)：如何新增数据源。
- [README](README.zh-CN.md)：快速开始、当日模式与各市场说明。
