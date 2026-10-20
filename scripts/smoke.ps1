param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot
$goExe = Join-Path $repoRoot '.tools/go/bin/go.exe'
$dotnetExe = Join-Path $repoRoot '.tools/dotnet/dotnet.exe'
& $goExe test ./...
if ($LASTEXITCODE -ne 0) { throw 'Go checks failed.' }
$null = Invoke-RestMethod 'http://127.0.0.1:8080/health'
& npm run smoke:js -- http://127.0.0.1:8080
if ($LASTEXITCODE -ne 0) { throw 'JavaScript checks failed.' }
& $dotnetExe run --project sdk/csharp/KokoSync.Checks -- --integration
if ($LASTEXITCODE -ne 0) { throw 'C# integration checks failed.' }
Write-Host 'Light smoke checks passed. See docs/implementation-status.md for scope and untested production behavior.'
