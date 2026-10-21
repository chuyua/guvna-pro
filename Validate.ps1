param([switch]$Race, [switch]$ShortToolchainPaths)
$ErrorActionPreference = 'Stop'
$savedEnvironment = @{}
foreach ($name in @('GCC_EXEC_PREFIX', 'COMPILER_PATH', 'LIBRARY_PATH')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
Push-Location $PSScriptRoot
try {
    if ($ShortToolchainPaths) {
        if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
            throw 'ShortToolchainPaths is a Windows-only option.'
        }
        $gccCommand = Get-Command gcc -ErrorAction Stop
        $toolchainRoot = Split-Path (Split-Path $gccCommand.Source -Parent) -Parent
        $fileSystemObject = New-Object -ComObject Scripting.FileSystemObject
        $shortToolchain = $fileSystemObject.GetFolder($toolchainRoot).ShortPath.Replace('\', '/')
        $env:GCC_EXEC_PREFIX = $shortToolchain + '/lib/gcc/'
        $env:COMPILER_PATH = $shortToolchain + '/bin'
        $env:LIBRARY_PATH = $shortToolchain + '/x86_64-w64-mingw32/lib'
    }
    & go test ./... -count=1
    if ($LASTEXITCODE -ne 0) { throw 'go test failed' }
    & go vet ./...
    if ($LASTEXITCODE -ne 0) { throw 'go vet failed' }
    & go build ./...
    if ($LASTEXITCODE -ne 0) { throw 'go build failed' }
    if ($Race) {
        & go test -race ./... -count=1
        if ($LASTEXITCODE -ne 0) { throw 'race tests failed or the native toolchain is unavailable' }
    }
    Write-Host 'Validation passed.'
} finally {
    foreach ($name in $savedEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process')
    }
    Pop-Location
}
