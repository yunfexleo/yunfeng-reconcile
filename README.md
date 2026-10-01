# 稳定性服务（笔试题作答仓库）

针对通知平台的独立稳定性服务：对账（业务系统 vs 服务商）+ 巡检告警。

- 技术栈：Go + PostgreSQL
- 配置：环境变量 `PORT` / `DATABASE_URL` / `BIZ_URL` / `PROVIDER_URL` / `ALERT_WEBHOOK_URL`
- 事故复盘：见 [docs/事故复盘-2026-03-12.md](docs/事故复盘-2026-03-12.md)

## 运行（当前为最简骨架）

```bash
go run ./cmd/server        # 默认端口 8080，可用 PORT 覆盖
curl localhost:8080/health
```
