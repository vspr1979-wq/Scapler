# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for SCAPLER (Windows 11 desktop, WebView2).
# Build:  pyinstaller packaging/scapler.spec
# Output: dist/Scapler/Scapler.exe  (onedir — faster cold start than onefile)

from PyInstaller.utils.hooks import collect_submodules

hiddenimports = (
    collect_submodules("msgspec")
    + collect_submodules("aiohttp")
    + collect_submodules("webview")
    + ["orjson", "scapler.brokers.groww.adapter",
       "scapler.brokers.upstox.adapter", "scapler.ui.transport",
       "scapler.ui.runtime", "scapler.ui.demo_feed"]
)

a = Analysis(
    ["../scapler/__main__.py"],
    pathex=[".."],
    binaries=[],
    datas=[("../scapler/ui/web", "scapler/ui/web"),
           ("../scapler/brokers/upstox/proto", "scapler/brokers/upstox/proto")],
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=["tkinter", "matplotlib", "pandas", "numpy", "PyQt5", "PySide6"],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz, a.scripts, [],
    exclude_binaries=True,
    name="Scapler",
    debug=False,
    strip=False,
    upx=False,
    console=False,                    # windowed — no console on Windows
    icon=None,
)
coll = COLLECT(
    exe, a.binaries, a.datas,
    strip=False, upx=False, name="Scapler",
)
