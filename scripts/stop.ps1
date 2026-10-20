param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$processFile = Join-Path $repoRoot '.run/processes.json'
if (-not (Test-Path -LiteralPath $processFile)) { Write-Host 'No managed server processes.'; return }
$records = Get-Content -LiteralPath $processFile -Raw | ConvertFrom-Json
foreach ($record in $records) {
    $process = Get-Process -Id $record.id -ErrorAction SilentlyContinue
    # PID reuse must not stop an unrelated process.
    if ($process -and $process.StartTime.ToUniversalTime().Ticks.ToString() -eq [string]$record.startTicks) {
        Stop-Process -Id $process.Id
        Wait-Process -Id $process.Id -ErrorAction SilentlyContinue
    }
}
Remove-Item -LiteralPath $processFile
Write-Host 'Managed server processes stopped.'
