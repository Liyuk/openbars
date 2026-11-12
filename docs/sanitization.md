# 脱敏规则与发布前检查清单

openbars 的数据层代码来自一套**上游内部实现**（下称 upstream），按“以 upstream 为主搬运 + 脱敏”的方式进入本仓库。仓库目前私有，计划转为公开。本文件规定哪些内容不能进入仓库、怎么处理，以及转公开前必须跑完的检查。

## 1. 护栏一览

| 护栏 | 位置 | 何时运行 | 检查什么 |
| --- | --- | --- | --- |
| gitleaks | `.gitleaks.toml` | pre-commit、CI（全历史 + 工作区） | 默认规则 + 行情源凭据（Alpaca、Massive/Polygon、Twelve Data、Alpha Vantage、Tushare、Binance/OKX 等）、URL 中的 `apikey=` |
| 禁词检查 | `scripts/check_sanitized.py` | pre-commit、CI（工作区 + 全历史）、手动 `--stdin` | 本机 home 路径、私有目录、内网/overlay 网络地址与域名、个人邮箱、疑似凭据赋值、私有词（哈希） |
| 数据文件拦截 | `.pre-commit-config.yaml` | pre-commit | 拒绝提交 parquet / duckdb / sqlite 等数据文件 |
| `.gitignore` | 仓库根 | 始终 | `.env*`、`config.yaml`、`data/` 不入库 |

私有词（内部包名、产品名、主机名、用户名等）**只以加盐 SHA-256 哈希**存放在 `scripts/sanitize_hashes.txt`，禁词表本身不泄露这些词。维护者可在仓库外放一份明文清单，并用环境变量 `OPENBARS_PRIVATE_DENYLIST` 指向它作为补充；新增私有词：

```bash
echo "新词" | python3 scripts/check_sanitized.py --hash-term >> scripts/sanitize_hashes.txt
```

检查只输出“文件:行号: 规则名”，不打印命中的值。

## 2. 处理规则

| 类别 | 例子（描述，不写实值） | 规则 |
| --- | --- | --- |
| 密钥 / token | provider API key、secret、数据库口令 | 一律走环境变量或 `.env`（不入库）；代码中只出现变量名；`config.example.yaml` / `.env.example` 只放空值或 `${VAR}` 占位。日志与错误信息中的凭据按 upstream 做法在落盘前替换为 `[REDACTED]` |
| SEC 等要求的 User-Agent | 带联系邮箱的 UA | 不设默认值；由用户在配置中填写，缺失时报错。文档示例用 `example.com` 域名 |
| 个人邮箱 | 提交作者邮箱、注释中的邮箱 | 文件中禁止；提交作者邮箱使用 GitHub noreply 地址（转公开前用 `--strict-authors` 检查，见 §4） |
| 本机路径 | 用户 home 目录、`~/Documents` 等 | 一律改为相对路径或配置项（如 `data_dir`）；示例用 `./data` |
| 内部主机 / IP | overlay 网络地址与域名、内网 IP、内部机器别名 | 删除；远程节点地址只能来自用户配置 |
| 付费套餐信息 | 维护者自己订阅了哪个档位 | 不写。provider 的**公开免费档**名称与限额可以写，但须附官方文档链接，并作为可覆盖的默认值 |
| 计费 / 权限模块 | upstream 的用户计划、额度计费、权限生成代码 | 不搬运，也不保留引用 |
| 内部编号与私人注释 | upstream 的 issue / PR / Discussion 编号、内部数据统计、人名、聊天内容 | 改成中性的技术描述（写“为什么”，不写“哪个内部单子”）。openbars 自己的 issue 编号可以引用 |
| 策略 / 产品名 | upstream 的产品名、策略名、扫描/回测/提醒等业务概念 | 不搬运；残留的命名改为中性名（如 `research` → `canonical`，只在兼容层保留与 upstream 一致的口径字符串） |
| 硬编码股票池 | 指数成分、基准 ETF 列表、内部关注列表 | 代码中不写死；默认股票池来自 provider 的全市场清单 + 用户配置。测试只用合成代码（如 `TEST1`、`ZZZA`） |
| 真实行情数据 | 抓包得到的 provider 响应、导出的 bar | 不入库（含测试夹具）。测试夹具一律合成，字段形状参考官方文档 |
| upstream 内部对照表 | 模块路径、私有仓库名与 openbars 的对照 | 放在仓库外的私有笔记中，不进入本仓库、issue、PR 或 Discussion |

## 3. 日常流程

1. 安装钩子：`pipx install pre-commit && pre-commit install`（本机需有 Go 以构建 gitleaks 钩子，或自行安装 gitleaks 后改用 `language: system`）。
2. 搬运 upstream 代码时，先在私有笔记中登记来源，再按 §2 逐条处理，PR 描述只写功能来源（“沿用上游内部实现的 X 逻辑”）。
3. 发 issue / PR / Discussion 正文前：`pbpaste | python3 scripts/check_sanitized.py --stdin`。
4. CI 中 `sanitization` 与 `gitleaks` 两个 job 必须通过才能合并。

## 4. 转公开前检查清单

- [ ] `python3 scripts/check_sanitized.py` 通过（工作区）。
- [ ] `python3 scripts/check_sanitized.py --history --release` 通过（全部历史、提交信息、作者邮箱；不使用基线）。`scripts/sanitize_history_baseline.txt` 记录了护栏建立前已推送历史中的已知问题，日常 `--history` 只把它们报为警告；转公开前必须清零：重写历史，或以一个全新的 squash 提交作为公开仓库的起点。
- [ ] `gitleaks git --config .gitleaks.toml --redact -v .` 与 `gitleaks dir --config .gitleaks.toml --redact -v .` 均无发现。
- [ ] 所有 issue、PR、Discussion 正文与评论用 `--stdin` 检查过；无 upstream 路径、包名、仓库名、内部编号。
- [ ] **编辑历史**：GitHub 会保留 issue / PR / Discussion 正文与评论的历史版本，有读权限的人都能看到。脱敏前写过内部信息的条目，需在网页“edited”下拉中逐条删除旧版本（或关闭后新建干净的条目）。
- [ ] 仓库中无数据文件、无真实 provider 响应；测试夹具均为合成数据。
- [ ] README 中的 provider 条款说明完整：openbars 不分发数据，用户自带 key，遵守各源条款。
- [ ] GitHub 仓库设置：开启 secret scanning 与 push protection；Actions 中不存在需要的 secret。
- [ ] 维护者复核 `scripts/sanitize_hashes.txt` 覆盖了当前全部私有词。
