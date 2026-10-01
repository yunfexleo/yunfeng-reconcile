# 自动化验证脚本：编译 → 启动全套进程 → 逐项检查 → 输出 PASS/FAIL。
# 用法：powershell -ExecutionPolicy Bypass -File scripts/verify.ps1
# 结束后进程保留在各自窗口中继续做故障注入实验；停止全部用 scripts\stop.ps1。
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

# ---------- 工具函数 ----------
function Test-Port([int]$Port) {
    try {
        $c = New-Object Net.Sockets.TcpClient('localhost', $Port)
        $c.Close(); return $true
    } catch { return $false }
}

function Wait-Port([int]$Port, [string]$Name) {
    for ($i = 0; $i -lt 50; $i++) {
        if (Test-Port $Port) { return $true }
        Start-Sleep -Milliseconds 200
    }
    Write-Host "  [FAIL] $Name (:$Port) 未就绪" -ForegroundColor Red
    return $false
}

$script:pass = 0; $script:fail = 0
function Check([string]$Name, [string]$Actual, [string]$MustContain) {
    if ($Actual -and $Actual.Contains($MustContain)) {
        Write-Host "  [PASS] $Name" -ForegroundColor Green
        $script:pass++
    } else {
        Write-Host "  [FAIL] $Name" -ForegroundColor Red
        Write-Host "         期望包含: $MustContain" -ForegroundColor DarkGray
        Write-Host "         实际返回: $(if ($Actual) { $Actual.Substring(0, [Math]::Min(160, $Actual.Length)) } else { '(空)' })" -ForegroundColor DarkGray
        $script:fail++
    }
}

function Try-Start([string]$Exe, [int]$Port, [string]$Name) {
    if (Test-Port $Port) { Write-Host "  $Name (:$Port) 已在运行，跳过启动" -ForegroundColor DarkYellow; return }
    $p = Start-Process -FilePath "$root\bin\$Exe" -WindowStyle Normal -PassThru
    Start-Sleep -Milliseconds 500
    if ($p.HasExited) {
        throw "$Name 启动后立即退出（ExitCode=$($p.ExitCode)），请在 bin\ 下直接运行 $Exe 查看报错"
    }
    if (Wait-Port $Port $Name) { Write-Host "  $Name (:$Port) 已启动" -ForegroundColor Green }
    else { throw "$Name 启动后端口未就绪" }
}

function Get-Curl([string]$Url) {
    try { return (& curl.exe -s --max-time 10 $Url) } catch { return "" }
}

# POST JSON：用 Invoke-RestMethod 而非 curl.exe —— PowerShell 5.1 给原生程序传
# 含双引号的参数会乱码（curl 收到的 JSON 被破坏），内建 cmdlet 没有此问题。
function Post-Json([string]$Url, [string]$Body) {
    try {
        $r = Invoke-RestMethod -Method Post -Uri $Url -ContentType 'application/json' -Body $Body -TimeoutSec 10
        return ($r | ConvertTo-Json -Depth 5 -Compress)
    } catch {
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) { return $_.ErrorDetails.Message }
        return ""
    }
}

# ---------- 1. 编译 ----------
Write-Host "`n==> 1. 编译" -ForegroundColor Cyan
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    if (Test-Path "C:\Program Files\Go\bin\go.exe") { $env:PATH = "C:\Program Files\Go\bin;$env:PATH" }
    else { throw "未找到 Go，请先安装" }
}
New-Item -ItemType Directory -Force bin | Out-Null
foreach ($app in 'server', 'mockbiz', 'mockprov', 'mockalert', 'watchdog') {
    & go build -o "bin\$app.exe" "./cmd/$app"
    if ($LASTEXITCODE -ne 0) { throw "编译 $app 失败" }
}
Write-Host "  5 个二进制编译完成" -ForegroundColor Green

# ---------- 2. 启动（注意依赖顺序：provider ← biz；server 依赖前两者） ----------
Write-Host "`n==> 2. 启动进程" -ForegroundColor Cyan
$env:MOCK_BIZ_PROV_URL = 'http://localhost:9002'
Try-Start 'mockprov.exe' 9002 'mock 服务商'
$env:BIZ_URL = 'http://localhost:9001'
$env:PROVIDER_URL = 'http://localhost:9002'
$env:ALERT_WEBHOOK_URL = 'http://localhost:9003'
Try-Start 'mockbiz.exe'   9001 'mock 业务系统'
Try-Start 'mockalert.exe' 9003 'mock 告警'
Try-Start 'server.exe'    8080 '你的服务'
$env:WATCHDOG_TARGET = 'http://localhost:8080'
$env:WATCHDOG_WEBHOOK = 'http://localhost:9003'
$wd = Start-Process -FilePath "$root\bin\watchdog.exe" -WindowStyle Normal -PassThru
Start-Sleep -Milliseconds 500
if ($wd.HasExited) { throw "看门狗启动后立即退出（ExitCode=$($wd.ExitCode)）" }
Write-Host "  看门狗已启动（探测 8080）" -ForegroundColor Green

Write-Host "`n==> 等待同步循环拉取种子数据（6s）..." -ForegroundColor Cyan
Start-Sleep -Seconds 6

# ---------- 3. 逐项验证 ----------
Write-Host "`n==> 3. 验证" -ForegroundColor Cyan

Check "你的服务 /health" (Get-Curl 'http://localhost:8080/health') '"status":"ok"'
Check "mock 业务系统 /internal/health" (Get-Curl 'http://localhost:9001/internal/health') '"db":"ok"'
Check "mock 业务系统 种子记录已生成" (Get-Curl 'http://localhost:9001/internal/send-records?limit=2') 'seed-0'
Check "mock 服务商 查询接口可用" (Get-Curl 'http://localhost:9002/senders/sender-0/messages?from=0&to=9999999999999&limit=5') '"messages"'

$req = Post-Json 'http://localhost:9001/internal/simulate/submit' '{"senderId":"sender-3","recipient":"verify@example.com"}'
Check "提交一条消息" $req '"requestId"'

Check "对账① 窗口未过可见性时延 → incomplete" `
    (Get-Curl 'http://localhost:8080/api/reconciliation?senderId=sender-2&from=2026-10-01T00:00:00Z&to=2026-10-01T23:59:59Z') '"state":"incomplete"'

$to = (Get-Date).ToUniversalTime().AddSeconds(-100).ToString('yyyy-MM-ddTHH:mm:ssZ')
Check "对账② 种子 sent 无对应消息 → inconsistent" `
    (Get-Curl "http://localhost:8080/api/reconciliation?senderId=sender-2&from=2026-10-01T00:00:00Z&to=$to") 'SENT_BUT_NO_PROVIDER_MSG'
Check "对账③ 干净 sender → consistent" `
    (Get-Curl "http://localhost:8080/api/reconciliation?senderId=sender-0&from=2026-10-01T00:00:00Z&to=$to") '"state":"consistent"'

Check "静默接口" (Post-Json 'http://localhost:8080/api/silence' '{"minutes":5}') '300'
# 立即解除静默（minutes=0），避免影响后续故障注入实验
Post-Json 'http://localhost:8080/api/silence' '{"minutes":0}' | Out-Null

# ---------- 4. 汇总 ----------
Write-Host "`n==> 结果：$script:pass 通过 / $script:fail 失败" -ForegroundColor $(if ($script:fail -eq 0) { 'Green' } else { 'Red' })
Write-Host ""
Write-Host "进程仍在运行（各一个窗口），可继续做故障注入实验：" -ForegroundColor Cyan
Write-Host '  例：在 mockbiz 窗口 Ctrl+C 后执行  $env:MOCK_BIZ_SLOW="true"; .\bin\mockbiz.exe'
Write-Host "  约 40s 后查看 alerts.log，应出现「业务系统不可用」critical 告警"
Write-Host "  告警日志: $root\alerts.log"
Write-Host "  停止全部: powershell -ExecutionPolicy Bypass -File scripts\stop.ps1"
if ($script:fail -gt 0) { exit 1 }
