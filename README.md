# 稳定性服务（笔试题作答仓库）

针对通知平台的独立稳定性服务：对账（业务系统 vs 服务商）+ 巡检告警。

- 技术栈：Go + PostgreSQL（pgx 驱动）
- 配置：环境变量 `PORT` / `DATABASE_URL` / `BIZ_URL` / `PROVIDER_URL` / `ALERT_WEBHOOK_URL`
- 事故复盘：见 [docs/事故复盘-2026-03-12.md](docs/事故复盘-2026-03-12.md)

## 结构

```
cmd/server/           # 入口：装配客户端、同步主循环、巡检引擎、HTTP 接口
cmd/mockbiz/          # mock 业务系统（2.1 节全接口 + 故障注入）
cmd/mockprov/         # mock 服务商（2.2 节：截断/429/503/410/时钟偏差）
cmd/mockalert/        # mock 告警 webhook（输出到 stdout 与 alerts.log）
cmd/watchdog/         # 看门狗：探测你的服务 /health，失联时代它告警（dead man's switch）
internal/reconcile/   # 对账：匹配、状态判定、增量同步（滞后游标）、/api/reconciliation
internal/alert/       # 告警 webhook 客户端：429 退避重试、同 Key 去抖、静默
internal/watch/       # 巡检引擎：探测 + flap 防抖 + 发送方/节点异常聚合
internal/mockbiz/     # mock 业务系统核心（可被测试 import）
internal/mockprov/    # mock 服务商核心（可被测试 import）
```

## 运行

```powershell
# 一键起全套：mockbiz(9001) + mockprov(9002) + mockalert(9003) + 你的服务(8080)
powershell -ExecutionPolicy Bypass -File scripts/dev.ps1

# 或手动：先 docker compose up -d db（可选，缺省用内存存储），再分别 go run ./cmd/xxx
```

### 故障注入开关（环境变量）

| 变量 | 作用 |
|---|---|
| `MOCK_BIZ_503_EVERY=N` | 业务系统每 N 次请求返回 503 |
| `MOCK_BIZ_SLOW=true` | 业务系统请求 hang 30s |
| `MOCK_BIZ_LATE_COMMIT=true` | `updatedAt` 取事务开始值、30s 后才可见（考验滞后游标） |
| `MOCK_BIZ_AUTO_SUBMIT_MS=N` | 每 N 毫秒自动提交一条新消息 |
| `MOCK_PROV_429_EVERY=N` | 服务商每 N 次请求返回 429 |
| `MOCK_PROV_503_EVERY=N` | 服务商每 N 次请求返回 503 |
| `MOCK_PROV_REVOKED=s1,s2` | 指定 sender 返回 410（对账应为 incomplete） |
| `MOCK_PROV_DROP_RATE=0~1` | 提交被服务商放弃的概率（默认 0.1） |
| `MOCK_ALERT_429_EVERY=N` | 告警 webhook 每 N 次请求返回 429 |

## 测试

```bash
go test ./...   # 单元测试（匹配/判定/告警）+ 端到端测试（mock 全链路 + 故障注入）+ 巡检测试
```

## 运维接口

```bash
# 临时静默告警 30 分钟
curl -X POST localhost:8080/api/silence -d '{"minutes": 30}'

# 对账查询
curl "localhost:8080/api/reconciliation?senderId=sender-0&from=...&to=..."

# 看门狗（另起进程，你的服务失联时替你告警）
go run ./cmd/watchdog
```
