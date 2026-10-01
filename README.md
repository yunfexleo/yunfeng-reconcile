# 稳定性服务（笔试题作答仓库）

针对通知平台的独立稳定性服务：**对账**（业务系统发送记录 vs 服务商实际发出）+ **巡检告警**（探测、分级、聚合、静默、看门狗）。

- 技术栈：Go 1.23+ + PostgreSQL（pgx 驱动），全部依赖可经 goproxy.cn 获取
- 配置：环境变量 `PORT` / `DATABASE_URL` / `BIZ_URL` / `PROVIDER_URL` / `ALERT_WEBHOOK_URL`
- 事故复盘：见 [docs/事故复盘-2026-03-12.md](docs/事故复盘-2026-03-12.md)

## 在 Windows 上一键启动

前置：安装 Go（`winget install GoLang.Go`，国内可从 https://mirrors.aliyun.com/golang/ 下载 MSI）。

```powershell
# 一键：检查环境 → 配国内代理 → 编译 → 启动全套（5 个进程，各自一个窗口）
powershell -ExecutionPolicy Bypass -File scripts/dev.ps1

# 一键验证：编译 → 启动 → 9 项自动检查（对账三种结论/提交/静默），输出 PASS/FAIL
powershell -ExecutionPolicy Bypass -File scripts/verify.ps1

# 停止全部
powershell -ExecutionPolicy Bypass -File scripts/stop.ps1
```

启动后：

| 进程 | 地址 | 说明 |
|---|---|---|
| 你的服务 | http://localhost:8080 | `/health` `/api/reconciliation` `/api/silence` |
| mock 业务系统 | http://localhost:9001 | 2.1 节全接口 + 故障注入开关 |
| mock 服务商 | http://localhost:9002 | 2.2 节：截断/429/503/410/时钟偏差 |
| mock 告警 webhook | http://localhost:9003 | 告警落 `alerts.log`，肉眼可查 |
| 看门狗 | — | 探测 8080，你的服务失联时替你告警 |

存储：默认自动探测本机 5432，有 PostgreSQL 就用（`docker compose up -d db` 一键起），没有用内存存储（日志会提示）。

```powershell
# 试试整条链路
curl http://localhost:8080/health
curl -X POST http://localhost:9001/internal/simulate/submit -d '{"senderId":"sender-0","recipient":"a@b.com"}'
curl "http://localhost:8080/api/reconciliation?senderId=sender-0&from=2026-10-01T00:00:00Z&to=2026-10-01T18:00:00Z"
curl -X POST http://localhost:8080/api/silence -d '{"minutes": 30}'
```

## 故障注入开关（启动 dev.ps1 之前设置环境变量）

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

示例：验证「业务系统挂死 → 看门狗和巡检告警」：

```powershell
$env:MOCK_BIZ_SLOW='true'; powershell -ExecutionPolicy Bypass -File scripts/dev.ps1
# 约 40s 后（3 次探测 × 10s 周期）alerts.log 出现「业务系统不可用」
```

## 结构

```
cmd/server/           # 入口：装配客户端、同步主循环、巡检引擎、HTTP 接口
cmd/mockbiz/          # mock 业务系统入口（读环境变量）
cmd/mockprov/         # mock 服务商入口
cmd/mockalert/        # mock 告警 webhook 入口
cmd/watchdog/         # 看门狗（dead man's switch）
internal/reconcile/   # 对账：匹配、状态判定、增量同步（滞后游标）、/api/reconciliation
internal/alert/       # 告警 webhook 客户端：429 退避重试、同 Key 去抖、静默
internal/watch/       # 巡检引擎：探测 + flap 防抖 + 发送方/节点异常聚合
internal/mockbiz/     # mock 业务系统核心（可被测试 import）
internal/mockprov/    # mock 服务商核心（可被测试 import）
scripts/dev.ps1       # 一键配置 + 启动
scripts/stop.ps1      # 停止全部
```

## 测试

```powershell
go test ./...
# 单元测试（匹配/判定/告警/巡检）+ 端到端测试（mock 全链路：提交→同步→闭合→对账）
# + 故障注入测试（503→incomplete、100 条截断击穿、429 退避重试、flap 防抖、节点聚合）
```

## 设计要点（对应题目陷阱）

- **滞后游标**：`updatedAt` 在事务开始时取值、最晚 30s 后提交 → 游标只推进到 `now-60s` 内最后一条；
- **判定时延 95s**：30s 服务商决定 + 60s 查询可见 + 5s 时钟偏差；`accepted`/`unknown` 越过即调 `/resolve` 闭合，409 容忍；
- **重提交语义**：多重集合匹配，重复消息归 `RESUBMISSION_DUPLICATE`（不算不一致），判定永远以最新 `submittedAt` 为准；
- **incomplete 三类成因**：窗口未过可见性时延、429/503 采集缺口、发送方被注销（410）；
- **flap 防抖**：连续 3 败才告警、连续 2 成才恢复——「重启间隙端口短暂可用」不翻转状态。
