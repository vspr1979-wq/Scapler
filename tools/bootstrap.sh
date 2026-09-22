#!/usr/bin/env bash
# Dev/sandbox bootstrap. Windows target later uses packaging/scapler.spec + Inno Setup.
# NOTE: pip installs live outside the workspace and may need re-running in fresh sandboxes.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m pip install --break-system-packages -e ".[dev,broker]" 2>/dev/null \
  || python3 -m pip install -e ".[dev,broker]" 2>/dev/null \
  || python3 -m pip install --break-system-packages msgspec orjson pytest pytest-asyncio aiohttp websockets
python3 -m pytest tests/unit -q
