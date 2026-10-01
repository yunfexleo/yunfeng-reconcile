# 稳定性服务（笔试题作答仓库）

针对通知平台的独立稳定性服务：对账（业务系统 vs 服务商）+ 巡检告警。

- 技术栈：Go + PostgreSQL（pgx 驱动）
- 配置：环境变量 `PORT` / `DATABASE_URL` / `BIZ_URL` / `PROVIDER_URL` / `ALERT_WEBHOOK_URL`
- 事故复盘：见 [docs/事故复盘-2026-03-12.md](docs/事故复盘-2026-03-12.md)

## 结构

```
cmd/server/           # 入口：装配客户端、同步主循环、HTTP 接口
internal/reconcile/   # 对账：匹配、状态判定、增量同步（滞后游标）、/api/reconciliation
internal/alert/       # 告警 webhook 客户端：429 退避重试、同 Key 去抖、静默
```

## 运行

```bash
docker compose up -d db    # 启动 PostgreSQL（可省略，退化为内存存储）

$env:DATABASE_URL = "postgres://stability:stability@localhost:5432/stability"
$env:BIZ_URL = "http://localhost:9001"
$env:PROVIDER_URL = "http://localhost:9002"
$env:ALERT_WEBHOOK_URL = "http://localhost:9003"
go run ./cmd/server

curl "localhost:8080/api/reconciliation?senderId=s1&from=2026-10-01T00:00:00Z&to=2026-10-01T12:00:00Z"
```

## 测试

```bash
go test ./...
```
