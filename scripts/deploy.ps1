param(
    [string]$IdentityFile = (Join-Path $env:USERPROFILE '.ssh/id_ed25519'),
    [string]$ReleaseId = ('release-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
)
$ErrorActionPreference = 'Stop'
$TargetHost = '45.77.130.146' # Matches the checked-in nginx/systemd template.
if ($ReleaseId -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$' -or $ReleaseId -in @('.', '..')) { throw 'Invalid release ID.' }
if (-not (Test-Path -LiteralPath $IdentityFile -PathType Leaf)) { throw 'SSH identity file not found.' }
$repoRoot = Split-Path -Parent $PSScriptRoot
$goExe = Join-Path $repoRoot '.tools/go/bin/go.exe'
$dotnetExe = Join-Path $repoRoot '.tools/dotnet/dotnet.exe'
if (-not (Test-Path -LiteralPath $goExe) -or -not (Test-Path -LiteralPath $dotnetExe)) { throw 'Run scripts/bootstrap.ps1 first.' }
$buildRoot = Join-Path $repoRoot ('.run/deploy/' + $ReleaseId)
if (Test-Path -LiteralPath $buildRoot) { throw 'Build directory already exists; choose a new release ID.' }
$stage = Join-Path $buildRoot 'stage'
$archive = Join-Path $buildRoot 'kokosync-linux.tar.gz'
$savedGoEnv = @{}
foreach ($name in @('GOOS', 'GOARCH', 'CGO_ENABLED')) { $savedGoEnv[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
Push-Location -LiteralPath $repoRoot
try {
    New-Item -ItemType Directory -Force -Path (Join-Path $stage 'bin'), (Join-Path $stage 'examples'), (Join-Path $stage 'sdk'), (Join-Path $stage 'node_modules'), (Join-Path $stage 'config') | Out-Null
    $env:GOOS = 'linux'; $env:GOARCH = 'amd64'; $env:CGO_ENABLED = '0'
    & $goExe build -trimpath -ldflags '-s -w' -o (Join-Path $stage 'bin/kokosync') ./cmd/kokosync
    if ($LASTEXITCODE -ne 0) { throw 'Linux relay build failed.' }
    & $dotnetExe publish server/litenetlib/KokoSync.Sidecar.csproj -c Release -r linux-x64 --self-contained true -o (Join-Path $stage 'bin/sidecar') -p:PublishSingleFile=false
    if ($LASTEXITCODE -ne 0) { throw 'Linux sidecar publish failed.' }
    Copy-Item -LiteralPath examples/browser -Destination (Join-Path $stage 'examples') -Recurse
    Copy-Item -LiteralPath sdk/js -Destination (Join-Path $stage 'sdk') -Recurse
    Copy-Item -LiteralPath node_modules/@msgpack, node_modules/@noble -Destination (Join-Path $stage 'node_modules') -Recurse
    Copy-Item -LiteralPath config/limits.example.json -Destination (Join-Path $stage 'config/limits.example.json')
    $revision = (& git rev-parse HEAD).Trim()
    $hasChanges = -not [string]::IsNullOrWhiteSpace((& git status --porcelain | Out-String))
    @{ release = $ReleaseId; sourceCommit = $revision; workingTreeChanges = $hasChanges; builtAt = [DateTime]::UtcNow.ToString('o') } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $stage 'build-info.json') -Encoding utf8
    & tar -czf $archive -C $stage .
    if ($LASTEXITCODE -ne 0) { throw 'Release archive creation failed.' }
    $remote = 'root@' + $TargetHost
    & scp -i $IdentityFile -o BatchMode=yes $archive ($remote + ':/root/kokosync-linux.tar.gz')
    if ($LASTEXITCODE -ne 0) { throw 'Release upload failed.' }
    & scp -i $IdentityFile -o BatchMode=yes deploy/linux/activate-host.sh ($remote + ':/root/kokosync-activate-host.sh')
    if ($LASTEXITCODE -ne 0) { throw 'Activation script upload failed.' }
    & ssh -i $IdentityFile -o BatchMode=yes $remote ('bash /root/kokosync-activate-host.sh ' + $ReleaseId)
    if ($LASTEXITCODE -ne 0) { throw 'Activation failed; review server output and rollback status.' }
    Write-Host ('Deployed ' + $ReleaseId + ' to https://' + $TargetHost + '/')
    Write-Host ('Build manifest: ' + (Join-Path $stage 'build-info.json'))
}
finally {
    foreach ($name in $savedGoEnv.Keys) { [Environment]::SetEnvironmentVariable($name, $savedGoEnv[$name], 'Process') }
    Pop-Location
}
