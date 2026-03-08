# PunkGo installer for Windows PowerShell
# Usage: irm https://raw.githubusercontent.com/PunkGo/punkgo-jack/main/install.ps1 | iex
$ErrorActionPreference = "Stop"

$JackRepo = "PunkGo/punkgo-jack"
$KernelRepo = "PunkGo/punkgo-kernel"
$Target = "x86_64-pc-windows-msvc"

# Install directory: ~/.punkgo/bin (added to PATH)
$InstallDir = Join-Path $env:USERPROFILE ".punkgo\bin"

function Write-Info { param($msg) Write-Host "▸ $msg" -ForegroundColor Green }
function Write-Warn { param($msg) Write-Host "▸ $msg" -ForegroundColor Red }
function Write-Dim  { param($msg) Write-Host "  $msg" -ForegroundColor DarkGray }

function Get-LatestTag {
    param($repo)
    $release = Invoke-RestMethod "https://api.github.com/repos/$repo/releases/latest"
    return $release.tag_name
}

function Install-Binary {
    param($repo, $name, $tag, $assetPrefix)

    $url = "https://github.com/$repo/releases/download/$tag/$assetPrefix-$Target.zip"
    $tmpDir = Join-Path $env:TEMP "punkgo-install-$(Get-Random)"
    $zipPath = Join-Path $tmpDir "archive.zip"

    New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null

    Write-Info "Downloading $name $tag..."
    Write-Dim $url

    try {
        Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing
    } catch {
        Write-Warn "Download failed. Check if release exists for $Target."
        Write-Warn "Fallback: cargo install $name"
        Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue
        return $false
    }

    Expand-Archive -Path $zipPath -DestinationPath $tmpDir -Force

    $binName = "$name.exe"
    $binPath = Join-Path $tmpDir $binName

    if (-not (Test-Path $binPath)) {
        Write-Warn "Binary $binName not found in archive"
        Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue
        return $false
    }

    # Ensure install directory exists
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }

    Copy-Item $binPath (Join-Path $InstallDir $binName) -Force
    Remove-Item -Recurse -Force $tmpDir -ErrorAction SilentlyContinue

    Write-Info "Installed $binName → $InstallDir\$binName"
    return $true
}

function Add-ToPath {
    $currentPath = [Environment]::GetEnvironmentVariable("PATH", "User")
    if ($currentPath -notlike "*$InstallDir*") {
        [Environment]::SetEnvironmentVariable("PATH", "$InstallDir;$currentPath", "User")
        $env:PATH = "$InstallDir;$env:PATH"
        Write-Info "Added $InstallDir to PATH"
    }
}

function Main {
    Write-Host ""
    Write-Info "PunkGo Installer (Windows)"
    Write-Host ""
    Write-Dim "Target: $Target"
    Write-Dim "Install dir: $InstallDir"
    Write-Host ""

    $jackTag = Get-LatestTag $JackRepo
    $kernelTag = Get-LatestTag $KernelRepo

    $ok = $true
    if (-not (Install-Binary $JackRepo "punkgo-jack" $jackTag "punkgo-jack")) { $ok = $false }
    if (-not (Install-Binary $KernelRepo "punkgo-kerneld" $kernelTag "punkgo-kernel-$kernelTag")) { $ok = $false }

    if ($ok) {
        Add-ToPath
        Write-Host ""
        Write-Info "Installation complete!"
        Write-Host ""
        Write-Dim "Next step:"
        Write-Host "  punkgo-jack setup claude-code"
        Write-Host ""
    } else {
        Write-Host ""
        Write-Warn "Some downloads failed. Install manually with:"
        Write-Host "  cargo install punkgo-jack"
        Write-Host "  cargo install punkgo-kernel"
        Write-Host ""
    }
}

Main
