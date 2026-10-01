# 一键配置并启动全套环境（Windows，PowerShell 5.1+ / 7+）。
# 用法：powershell -ExecutionPolicy Bypass -File scripts/dev.ps1
# 停止：powershell -ExecutionPolicy Bypass -File scripts/stop.ps1
#
# 可选项（在本脚本运行前设置）：
#   $env:MOCK_BIZ_SLOW='true'          业务系统请求 hang 30s
#   $env:MOCK_PROV_503_EVERY='3'       服务商每 3 次请求 503 一次
#   $env:DATABASE_URL='...'            指定 PostgreSQL，否则 5432 可通时自动使用，否则内存存储
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

# 1. 找到 Go（未进 PATH 时回退到默认安装路径）
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    $goExe = "C:\Program Files\Go\bin\go.exe"
    if (Test-Path $goExe) { $env:PATH = "C:\Program Files\Go\bin;$env:PATH" }
    else { throw "未找到 Go。请先安装：winget install GoLang.Go，或从 https://mirrors.aliyun.com/golang/ 下载 MSI。" }
}
go version
if ($LASTEXITCODE -ne 0) { throw "go 不可用" }

# 2. 配置国内模块代理（幂等，已配置则无效果）
go env -w GOPROXY=https://goproxy.cn,direct

# 3. 拉依赖并编译全部二进制到 bin\
go mod download
New-Item -ItemType Directory -Force bin | Out-Null
foreach ($app in 'server', 'mockbiz', 'mockprov', 'mockalert', 'watchdog') {
    Write-Host "编译 $app ..." -ForegroundColor DarkGray
    go build -o "bin\$app.exe" "./cmd/$app"
    if ($LASTEXITCODE -ne 0) { throw "编译 $app 失败" }
}

# 4. 存储：DATABASE_URL 已设则直接用；否则 5432 可通视为本地 PostgreSQL 已起（compose up -d db）；都没有则用内存存储
if (-not $env:DATABASE_URL) {
    $pgUp = $false
    try {
        $c = New-Object Net.Sockets.TcpClient
        $c.Connect('localhost', 5432); $pgUp = $true; $c.Close()
    } catch {}
    if ($pgUp) { $env:DATABASE_URL = 'postgres://stability:stability@localhost:5432/stability' }
}

# 5. 启动三个 mock（独立窗口，便于观察日志）
$env:MOCK_BIZ_PROV_URL = 'http://localhost:9002'
Start-Process "bin\mockprov.exe"  -WindowStyle Normal | Out-Null
Start-Sleep -Milliseconds 400
Start-Process "bin\mockbiz.exe"   -WindowStyle Normal | Out-Null
Start-Sleep -Milliseconds 400
Start-Process "bin\mockalert.exe" -WindowStyle Normal | Out-Null
Start-Sleep -Milliseconds 400

# 6. 等端口就绪（每个最多 10s）
function Wait-Port([int]$Port, [string]$Name) {
    for ($i = 0; $i -lt 50; $i++) {
        try {
            $c = New-Object Net.Sockets.TcpClient('localhost', $Port)
            $c.Close(); Write-Host ("  {0,-12} :{1} 就绪" -f $Name, $Port) -ForegroundColor Green
            return
        } catch { Start-Sleep -Milliseconds 200 }
    }
    throw "$Name (:$Port) 启动失败，请查看对应窗口的日志"
}
Wait-Port 9002 'mock 服务商'
Wait-Port 9001 'mock 业务系统'
Wait-Port 9003 'mock 告警'

# 7. 启动你的服务；等 8080 就绪后带起看门狗（它探测你的服务 /health）
$env:BIZ_URL = 'http://localhost:9001'
$env:PROVIDER_URL = 'http://localhost:9002'
$env:ALERT_WEBHOOK_URL = 'http://localhost:9003'
Start-Process "bin\server.exe" -WindowStyle Normal | Out-Null
Wait-Port 8080 '你的服务'
Start-Process "bin\watchdog.exe" -WindowStyle Normal | Out-Null
Write-Host ("  {0,-12} :{1} 就绪" -f '看门狗', '外部') -ForegroundColor Green

Write-Host ""
Write-Host "全套环境已启动（停止全部: scripts\stop.ps1）：" -ForegroundColor Cyan
Write-Host "  你的服务(对账/巡检/静默)  http://localhost:8080"
Write-Host "  mock 业务系统             http://localhost:9001"
Write-Host "  mock 服务商               http://localhost:9002"
Write-Host "  mock 告警(webhook 日志)   http://localhost:9003 -> alerts.log"
Write-Host ""
Write-Host "试试：" -ForegroundColor Cyan
Write-Host "  curl http://localhost:8080/health"
Write-Host "  curl -X POST http://localhost:9001/internal/simulate/submit -d '{`"senderId`":`"sender-0`",`"recipient`":`"a@b.com`"}'"
Write-Host "  curl `"http://localhost:8080/api/reconciliation?senderId=sender-0&from=2026-10-01T00:00:00Z&to=2026-10-01T18:00:00Z`""
