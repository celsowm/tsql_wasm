param(
    [Parameter(Mandatory = $true)]
    [string]$ReleaseTag,
    [string]$TargetTriple = "x86_64-pc-windows-msvc",
    [string]$WorkspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$OutputDir = (Join-Path (Resolve-Path (Join-Path $PSScriptRoot "..")).Path "dist\windows")
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function New-CleanDirectory {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (Test-Path $Path) {
        Remove-Item -Path $Path -Recurse -Force
    }

    New-Item -ItemType Directory -Path $Path -Force | Out-Null
}

function Copy-RequiredFile {
    param(
        [Parameter(Mandatory = $true)][string]$Source,
        [Parameter(Mandatory = $true)][string]$Destination
    )

    if (-not (Test-Path $Source)) {
        throw "Required file not found: $Source"
    }

    Copy-Item -Path $Source -Destination $Destination -Force
}

$match = [regex]::Match($ReleaseTag, '^v?(?<base>\d+\.\d+\.\d+)(?:-windows\.(?<build>\d+))?$')
if (-not $match.Success) {
    throw "ReleaseTag '$ReleaseTag' must look like v0.1.0-windows.1"
}

$baseVersion = $match.Groups['base'].Value
$productVersion = $baseVersion

$artifactStem = "IridiumSQL-$ReleaseTag-x64"
$portableRoot = Join-Path $OutputDir "portable"
$portableStage = Join-Path $portableRoot $artifactStem
$msiStage = Join-Path $OutputDir "msi-stage"
$zipPath = Join-Path $OutputDir "$artifactStem.zip"
$msiPath = Join-Path $OutputDir "$artifactStem.msi"
$checksumPath = Join-Path $OutputDir "$artifactStem.sha256.txt"

$buildOutput = Join-Path $WorkspaceRoot "target\$TargetTriple\release"
$serverExe = Join-Path $buildOutput "iridium-server.exe"
$compatQueryExe = Join-Path $buildOutput "compat-query.exe"

$readme = Join-Path $WorkspaceRoot "README.md"
$license = Join-Path $WorkspaceRoot "LICENSE"
$installNotes = Join-Path $WorkspaceRoot "packaging\windows\INSTALL.txt"
$portableLauncher = Join-Path $WorkspaceRoot "packaging\windows\portable-run.cmd"
$wixSource = Join-Path $WorkspaceRoot "packaging\windows\iridium-sql.wxs"

foreach ($required in @($serverExe, $compatQueryExe, $readme, $license, $installNotes, $portableLauncher, $wixSource)) {
    if (-not (Test-Path $required)) {
        throw "Missing required input: $required"
    }
}

New-CleanDirectory -Path $OutputDir
New-CleanDirectory -Path $portableStage
New-CleanDirectory -Path $msiStage

Copy-RequiredFile -Source $serverExe -Destination (Join-Path $portableStage "iridium-server.exe")
Copy-RequiredFile -Source $compatQueryExe -Destination (Join-Path $portableStage "compat-query.exe")
Copy-RequiredFile -Source $readme -Destination (Join-Path $portableStage "README.md")
Copy-RequiredFile -Source $license -Destination (Join-Path $portableStage "LICENSE")
Copy-RequiredFile -Source $installNotes -Destination (Join-Path $portableStage "INSTALL.txt")
Copy-RequiredFile -Source $portableLauncher -Destination (Join-Path $portableStage "start-iridium-server-portable.cmd")

Copy-RequiredFile -Source $serverExe -Destination (Join-Path $msiStage "iridium-server.exe")
Copy-RequiredFile -Source $compatQueryExe -Destination (Join-Path $msiStage "compat-query.exe")
Copy-RequiredFile -Source $readme -Destination (Join-Path $msiStage "README.md")
Copy-RequiredFile -Source $license -Destination (Join-Path $msiStage "LICENSE")
Copy-RequiredFile -Source $installNotes -Destination (Join-Path $msiStage "INSTALL.txt")

Compress-Archive -Path (Join-Path $portableStage "*") -DestinationPath $zipPath -Force

$candle = (Get-Command candle.exe -ErrorAction Stop).Path
$light = (Get-Command light.exe -ErrorAction Stop).Path
$wixObj = Join-Path $OutputDir "iridium-sql.wixobj"

& $candle -nologo -arch x64 -dSourceDir="$msiStage" -dProductVersion="$productVersion" -out "$wixObj" "$wixSource"
if ($LASTEXITCODE -ne 0) {
    throw "WiX candle failed with exit code $LASTEXITCODE"
}

& $light -nologo -ext WixUIExtension -cultures:en-us -out "$msiPath" "$wixObj"
if ($LASTEXITCODE -ne 0) {
    throw "WiX light failed with exit code $LASTEXITCODE"
}

$hashLines = @()
foreach ($artifact in @($zipPath, $msiPath)) {
    $hash = (Get-FileHash -Path $artifact -Algorithm SHA256).Hash.ToLowerInvariant()
    $hashLines += "$hash  $(Split-Path $artifact -Leaf)"
}

$hashLines | Set-Content -Path $checksumPath -Encoding ASCII

Remove-Item -Path $portableRoot -Recurse -Force
Remove-Item -Path $msiStage -Recurse -Force
Remove-Item -Path $wixObj -Force

Write-Host "Created artifacts:"
Write-Host "  $zipPath"
Write-Host "  $msiPath"
Write-Host "  $checksumPath"
