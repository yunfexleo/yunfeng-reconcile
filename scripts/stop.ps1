# 停止 dev.ps1 启动的全部进程（按 bin\*.exe 路径识别）。
# 用法：powershell -ExecutionPolicy Bypass -File scripts/stop.ps1
$ErrorActionPreference = 'SilentlyContinue'
$root = Split-Path $PSScriptRoot -Parent
Get-Process | Where-Object { $_.Path -and $_.Path.StartsWith($root) -and $_.Path -like '*.exe' } | ForEach-Object {
    Write-Host "停止 $($_.ProcessName) (pid $($_.Id))"
    Stop-Process -Id $_.Id -Force
}
