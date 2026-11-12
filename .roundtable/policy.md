# 运行政策

> 由 setup 生成后由部署者维护。这里只能收窄宿主权限，不能扩大。只写凭据引用，不写密钥。

## 授权

- 可以接受谁派来的工作：仓库所有者本人指派的任务
- 无需确认即可执行：读取工作区；在功能分支上修改代码；运行本地 `scripts/check.sh`；用带 `openbars-test` 前缀的一次性 Docker 容器做开发验证（测完即停，不占用已有服务端口，不触碰本机其他 compose 项目）；在 openbars 仓库内开 Issue / PR / 评论；在本地 `scripts/check.sh` 通过后，由阿福 squash 合并 openbars 的 PR（可删除已合并的功能分支）
- 必须先问用户：发布或部署、force push 或改写已推送历史、向 openbars 仓库以外发送消息、删除数据、安装全局依赖或第三方 skill、修改 GitHub 计费或启用会消耗付费分钟数的自动 Actions
- 禁止：把本地 Docker 测试当成生产部署，或把测试容器、卷、网络做成长期服务；force push；运行或启用会消耗付费分钟数的 GitHub Actions（仓库里的 CI workflow 仅保留手动 `workflow_dispatch`，日常不使用、也不等待它）；把密钥、本机路径、个人邮箱或内部专有名称写入仓库、Issue、PR 或 Discussion；修改上游内部实现所在仓库或 Roundtable Work 源仓库（对它们只读）

## 合并前检查

- openbars 的 PR 由阿福执行合并（squash），仓库所有者不再需要亲自点合并。
- 合并前必须在本地跑通 `./scripts/check.sh`（pytest、禁词检查、gitleaks）。未通过不得合并。
- 不使用付费 GitHub Actions 做检测；不要为了合并去等或触发付费检查。

## 预算与停止条件

- 每个 assignment 最多尝试：2
- 审查打回最多：2 轮，之后交用户裁决
- 连续无进展回合上限：2（无新事实、无新产物即算无进展）→ `stop`
- 同一文件同时写入者：1
- 单个目标的时间或费用上限：未设置；尽量节省用量，不设定时巡检

## 执行路由

| 工作类型 | 执行方式 | 模型档 | 已验证 |
| --- | --- | --- | --- |
| 实现 / 修复 | 本地 `pi -p --model claude-sonnet-5-5`（默认） | sonnet | 约定已写明，以实际跑通为准 |
| 独立审查 / 调度验收 | 当前会话复核 diff 与 `scripts/check.sh` | 继承 | 以每次合并前的本地检查为准 |

“已验证”只在实际跑通一次后改为日期与证据位置。

## 触发与唤醒

- 用户消息：启用
- 子任务完成通知：未验证（本部署主要在当前会话内调度）
- 外部事件（GitHub、CI、webhook）：未启用
- 定时巡检：未启用

## 通知与记录

- 需要用户决定或交付时：在当前对话中报告
- 工单与续行记录位置：`.roundtable/tickets/`、`.roundtable/state/`（这些目录不入库）
- 记忆位置：`.roundtable/memory/`；敏感内容不写入可提交的文件
