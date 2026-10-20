param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot
$goExe = Join-Path $repoRoot '.tools/go/bin/go.exe'
$dotnetExe = Join-Path $repoRoot '.tools/dotnet/dotnet.exe'
if (-not (Test-Path -LiteralPath $goExe) -or -not (Test-Path -LiteralPath $dotnetExe)) { throw 'Run .\scripts\bootstrap.ps1 first.' }
if (Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue) { throw 'Port 8080 is already in use. Stop the existing server first.' }
if (Get-NetUDPEndpoint -LocalPort 8081,8082 -ErrorAction SilentlyContinue) { throw 'UDP port 8081 or 8082 is already in use.' }
$runDir = Join-Path $repoRoot '.run'
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$relayExe = Join-Path $runDir 'kokosync.exe'
& $goExe build -o $relayExe ./cmd/kokosync
if ($LASTEXITCODE -ne 0) { throw 'Relay build failed.' }
& $dotnetExe build server/litenetlib -o (Join-Path $runDir 'sidecar')
if ($LASTEXITCODE -ne 0) { throw 'Sidecar build failed.' }
$relay = Start-Process -FilePath $relayExe -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runDir 'relay.out.log') -RedirectStandardError (Join-Path $runDir 'relay.err.log')
$sidecarDll = Join-Path $runDir 'sidecar/KokoSync.Sidecar.dll'
$sidecar = Start-Process -FilePath $dotnetExe -ArgumentList @(('"' + $sidecarDll + '"'), '--insecure-development') -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runDir 'sidecar.out.log') -RedirectStandardError (Join-Path $runDir 'sidecar.err.log')
@(
    @{ id = $relay.Id; startTicks = $relay.StartTime.ToUniversalTime().Ticks.ToString() }
    @{ id = $sidecar.Id; startTicks = $sidecar.StartTime.ToUniversalTime().Ticks.ToString() }
) | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDir 'processes.json')
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Milliseconds 200
    if ($relay.HasExited -or $sidecar.HasExited) { throw 'A server exited during startup; inspect .run/*.log and run scripts/stop.ps1.' }
    try { $null = Invoke-RestMethod 'http://127.0.0.1:8080/health'; $ready = $true; break } catch { }
}
if (-not $ready) { throw 'Relay startup timed out; inspect .run/*.log.' }
Write-Host 'Running: browser http://127.0.0.1:8080/ | QUIC 8081 | LiteNetLib 8082'
Write-Host 'Logs: .run/*.log. Stop: .\scripts\stop.ps1'
