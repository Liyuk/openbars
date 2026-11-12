## 关联

Closes #
Discussion:

## 做了什么

## 怎么验证的

<!-- 只写实际运行过的检查及结果；没跑过的不要标为通过 -->
- [ ] `uv run --extra dev pytest`

## 未完成 / 风险

## 检查清单

- [ ] 没有提交任何密钥、`.env`、数据文件（`data/`、`*.parquet`、`*.duckdb`）
- [ ] 接口或口径变更已同步 `docs/architecture.md`
- [ ] 新 provider 已声明 `SourceCapability`、`QuotaPolicy` 与条款
