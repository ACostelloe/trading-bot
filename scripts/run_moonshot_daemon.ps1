# Restart moonshot runner if it exits (crash, OOM). Run from repo root.
#   .\scripts\run_moonshot_daemon.ps1
# Prefer NSSM / Windows Task Scheduler / systemd for production.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

while ($true) {
    Write-Host "$(Get-Date -Format o) starting live/run_moonshot.py"
    python "live/run_moonshot.py"
    $code = $LASTEXITCODE
    Write-Host "$(Get-Date -Format o) exited code=$code restarting in 15s"
    Start-Sleep -Seconds 15
}
