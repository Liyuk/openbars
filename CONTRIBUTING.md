# 参与贡献

## 从想法到交付

功能、设计和一般性改进先在 [Discussions](https://github.com/Liyuk/openbars/discussions) 讨论目标、方案与验收；形成共识后创建 Issue。Issue 关联原 Discussion，写清范围、验收和依赖。领取 Issue 后在功能分支上实现，并开 PR；PR 用 `Closes #编号` 关联对应 Issue，只完成部分范围时用 `Refs #编号`。

Bug 可直接开 Bug Issue，写清复现步骤、预期与实际、环境和证据。不要在 Issue、PR 或 Discussion 里写入密钥、本机路径、个人邮箱或内部专有名称；正文发出前可用 `pbpaste | python3 scripts/check_sanitized.py --stdin` 检查。脱敏规则见 [docs/sanitization.md](docs/sanitization.md)。

## 实现约定

默认用本地 pi 实现代码，模型使用 Claude Sonnet 档（本机若配置了私有路由，在本地使用即可，不要把私有路由名写进仓库）：

```bash
pi -p --model claude-sonnet-5-5
```

在功能分支上工作，不要直接提交到 `main`。提交信息简洁说明改动即可（中文或英文）。不要 force push，不要改写已推送的历史。

## 本地检查

合并前必须在仓库根目录跑通：

```bash
./scripts/check.sh
```

它会依次运行 pytest、禁词检查（工作区 + 全历史）和 gitleaks。本机有 `gitleaks` 就直接用，否则使用 Docker 镜像 `zricethezav/gitleaks:v8.30.1`。

日常不使用 GitHub Actions。仓库里虽然保留 `.github/workflows/ci.yml`，但触发器只有手动 `workflow_dispatch`，以避免消耗付费分钟数。不要为了合并去等待或触发付费检查。

可选安装 git 钩子（不会改全局 git 配置）：

```bash
pipx install pre-commit && pre-commit install && pre-commit install --hook-types pre-push
```

pre-push 钩子会调用 `scripts/check.sh`。

## 合并

openbars 的 PR 由阿福在本地 `scripts/check.sh` 通过后 squash 合并。

## 本地 Docker 测试（可选）

需要 Postgres 等依赖时，可以在本机 Docker 里起一次性测试环境，这只是开发验证，不是生产部署。

- 容器、卷、网络名称一律使用 `openbars-test` 前缀。
- 端口不要和本机已有服务冲突（例如不要占用 `127.0.0.1:8000`）。
- 不要触碰本机已有的其他 compose 项目、容器或卷。
- 测完立即停掉并删除自己起的 `openbars-test*` 容器、卷和网络。
- 仓库不提交任何行情数据；`data/` 已在 `.gitignore` 中。
