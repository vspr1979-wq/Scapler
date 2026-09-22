# SCAPLER — Windows 11 packaging script (run in the repo root, PowerShell 7+)
#
# Prereqs:
#   * Windows 11 (WebView2 runtime ships with the OS; if a target machine
#     lacks it, install the Evergreen Bootstrapper:
#     https://developer.microsoft.com/microsoft-edge/webview2/)
#   * Python 3.11+ (python.org installer, "Add to PATH")
#
# Output: dist\Scapler\Scapler.exe  (+ the whole dist\Scapler folder)

$ErrorActionPreference = "Stop"

Write-Host "== SCAPLER Windows build ==" -ForegroundColor Cyan

# 1) clean venv
if (Test-Path .venv-build) { Remove-Item -Recurse -Force .venv-build }
python -m venv .venv-build
& .venv-build\Scripts\Activate.ps1

# 2) dependencies (ui + broker extras; pywebview pulls WebView2 support)
python -m pip install --upgrade pip
pip install ".[ui,broker]" pyinstaller

# 3) sanity: unit suite must be green before packaging
python -m pytest tests/unit -q
if ($LASTEXITCODE -ne 0) { throw "tests failed — not packaging" }

# 4) build
pyinstaller packaging/scapler.spec --noconfirm

# 5) smoke: launch the packaged exe with the dev flag off (desktop window).
Write-Host "Built: dist\Scapler\Scapler.exe" -ForegroundColor Green
Write-Host "Secrets at rest use Windows DPAPI (current user). Data dir: %USERPROFILE%\.scapler"
Write-Host "First run: Settings tab → save broker credentials → CONNECT → trade."
