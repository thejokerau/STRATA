param(
    [string]$TaskName = "STRATA_Nightly_Research",
    [string]$Time = "02:00",
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$runner = Join-Path $repoRoot "scripts\auto_research_cycle.py"

if (-not (Test-Path $runner)) {
    throw "Runner not found: $runner"
}

$action = "$PythonExe `"$runner`""
$startBoundary = (Get-Date -Hour ([int]$Time.Split(":")[0]) -Minute ([int]$Time.Split(":")[1]) -Second 0).ToString("HH:mm")

schtasks /Create `
  /TN $TaskName `
  /TR $action `
  /SC DAILY `
  /ST $startBoundary `
  /F | Out-Null

Write-Host "Registered task '$TaskName' at $startBoundary."
Write-Host "Command: $action"
