param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot
$goExe = Join-Path $repoRoot '.tools/go/bin/go.exe'
$dotnetExe = Join-Path $repoRoot '.tools/dotnet/dotnet.exe'
& $goExe test ./...
if ($LASTEXITCODE -ne 0) { throw 'Go checks failed.' }
& npm run test:fragmentation
if ($LASTEXITCODE -ne 0) { throw 'JavaScript fragmentation tests failed.' }
& npm run test:routing
if ($LASTEXITCODE -ne 0) { throw 'JavaScript routing tests failed.' }
& npm run test:peer-delivery
if ($LASTEXITCODE -ne 0) { throw 'JavaScript peer delivery tests failed.' }
$null = Invoke-RestMethod 'http://127.0.0.1:8080/health'
& npm run smoke:js -- http://127.0.0.1:8080
if ($LASTEXITCODE -ne 0) { throw 'JavaScript checks failed.' }
& npm run smoke:peer-delivery -- http://127.0.0.1:8080
if ($LASTEXITCODE -ne 0) { throw 'JavaScript peer delivery integration failed.' }
& $dotnetExe run --project sdk/csharp/KokoSync.Checks -- --integration
if ($LASTEXITCODE -ne 0) { throw 'C# integration checks failed.' }
& $dotnetExe run --project sdk/csharp/KokoSync.Checks -- --peer-delivery --routing --fragmentation --ws=ws://127.0.0.1:8080/ws --udp-port=8082
if ($LASTEXITCODE -ne 0) { throw 'C# fragmentation checks failed.' }
Write-Host 'Light smoke checks passed. See docs/implementation-status.md for scope and untested production behavior.'
