# 一键启动全套本地环境：mock 业务系统(9001) + mock 服务商(9002) + mock 告警(9003) + 你的服务(8080)。
# 用法：powershell -ExecutionPolicy Bypass -File scripts/dev.ps1
# 可用环境变量覆盖开关，例如：
#   $env:MOCK_PROV_503_EVERY=3; $env:MOCK_BIZ_SLOW='true'; .\scripts\dev.ps1

$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

$env:BIZ_URL         = "http://localhost:9001"
$env:PROVIDER_URL    = "http://localhost:9002"
$env:ALERT_WEBHOOK_URL = "http://localhost:9003"
if (-not $env:DATABASE_URL) {
    # 没有起 PostgreSQL 就用内存存储；要持久化先执行 docker compose up -d db
    $env:DATABASE_URL = ""
}

Write-Host "starting mock provider :9002"  -ForegroundColor Cyan
Start-Process go -ArgumentList "run","./cmd/mockprov"  -WindowStyle Normal
Start-Sleep -Milliseconds 500
Write-Host "starting mock biz      :9001"  -ForegroundColor Cyan
Start-Process go -ArgumentList "run","./cmd/mockbiz"   -WindowStyle Normal
Start-Sleep -Milliseconds 500
Write-Host "starting mock alert    :9003"  -ForegroundColor Cyan
Start-Process go -ArgumentList "run","./cmd/mockalert" -WindowStyle Normal
Start-Sleep -Milliseconds 500
Write-Host "starting your service  :8080"  -ForegroundColor Green
go run ./cmd/server
