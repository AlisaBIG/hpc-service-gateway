$ErrorActionPreference = 'Stop'
[string[]]$CargoArguments = @($args)
if ($CargoArguments.Count -eq 0) {
    $CargoArguments = @('check', '--all-targets')
}
$projectRoot = Split-Path -Parent $PSScriptRoot
$localTools = Join-Path $projectRoot '.tools'
$rustBin = Join-Path $localTools 'rust\bin'
if (-not (Test-Path -LiteralPath (Join-Path $rustBin 'cargo.exe'))) {
    throw "Workspace Rust toolchain missing: $rustBin. Install Rust normally or provide the local toolchain."
}
$msvcRoot = 'C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC'
$sdkRoot = 'C:\Program Files (x86)\Windows Kits\10'
$msvcVersion = Get-ChildItem -LiteralPath $msvcRoot -Directory | Sort-Object Name -Descending | Select-Object -First 1
$sdkVersion = Get-ChildItem -LiteralPath (Join-Path $sdkRoot 'Lib') -Directory | Sort-Object Name -Descending | Select-Object -First 1
if (-not $msvcVersion -or -not $sdkVersion) {
    throw 'Microsoft C++ Build Tools and Windows SDK are required for this local MSVC toolchain.'
}
$msvcPath = $msvcVersion.FullName
$sdkNumber = $sdkVersion.Name
$env:CARGO_HOME = Join-Path $localTools 'cargo-home'
$env:CARGO_TARGET_DIR = Join-Path $localTools 'target'
$env:PATH = "$rustBin;$msvcPath\bin\Hostx64\x64;$sdkRoot\bin\$sdkNumber\x64;$env:PATH"
$env:LIB = "$msvcPath\lib\x64;$sdkRoot\Lib\$sdkNumber\ucrt\x64;$sdkRoot\Lib\$sdkNumber\um\x64"
$env:INCLUDE = "$msvcPath\include;$sdkRoot\Include\$sdkNumber\ucrt;$sdkRoot\Include\$sdkNumber\shared;$sdkRoot\Include\$sdkNumber\um"
Write-Host "Rust: $(& (Join-Path $rustBin 'rustc.exe') --version)"
Write-Host "MSVC: $($msvcVersion.Name), Windows SDK: $sdkNumber"
Push-Location -LiteralPath $projectRoot
try {
    & (Join-Path $rustBin 'cargo.exe') @CargoArguments
    $cargoExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $cargoExitCode
