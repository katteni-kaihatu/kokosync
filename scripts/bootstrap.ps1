param()
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot
$toolsDir = Join-Path $repoRoot '.tools'
New-Item -ItemType Directory -Force -Path $toolsDir | Out-Null
if (-not (Get-Command node -ErrorAction SilentlyContinue)) { throw 'Install Node.js 22 or later before running bootstrap.' }
$nodeMajor = [int]((& node -p 'process.versions.node.split(".")[0]') | Select-Object -Last 1)
if ($nodeMajor -lt 22) { throw 'Node.js 22 or later is required.' }
$goExe = Join-Path $toolsDir 'go/bin/go.exe'
if (-not (Test-Path -LiteralPath $goExe)) {
    $archive = Join-Path $toolsDir 'go1.27.3.windows-amd64.zip'
    Write-Host 'Downloading Go 1.27.3 into .tools...'
    Invoke-WebRequest 'https://go.dev/dl/go1.27.3.windows-amd64.zip' -OutFile $archive
    $expected = 'f26081f438319757f8a1b46d7d0b2c5ec7583d4038e485e0aa725cfde7885fff'
    if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expected) { throw 'Go archive SHA256 mismatch.' }
    Expand-Archive -LiteralPath $archive -DestinationPath $toolsDir
}
$dotnetExe = Join-Path $toolsDir 'dotnet/dotnet.exe'
$hasSdk = (Test-Path -LiteralPath $dotnetExe) -and ((& $dotnetExe --list-sdks) -match '^10\.0\.402 ')
if (-not $hasSdk) {
    Write-Host 'Installing .NET SDK 10.0.402 into .tools...'
    $installer = Join-Path $toolsDir 'dotnet-install.ps1'
    Invoke-WebRequest 'https://dot.net/v1/dotnet-install.ps1' -OutFile $installer
    & $installer -Version '10.0.402' -InstallDir (Join-Path $toolsDir 'dotnet') -NoPath
    if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) { throw '.NET SDK installation failed.' }
}
& $goExe mod download
if ($LASTEXITCODE -ne 0) { throw 'Go dependency download failed.' }
& $dotnetExe restore server/litenetlib
if ($LASTEXITCODE -ne 0) { throw '.NET dependency restore failed.' }
& npm ci
if ($LASTEXITCODE -ne 0) { throw 'npm ci failed.' }
Write-Host 'Ready. Run .\scripts\start.ps1, then open http://127.0.0.1:8080/'
