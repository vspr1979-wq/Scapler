#!/usr/bin/env bash
# Read-only live validation of the Groww adapter. Credentials via env ONLY.
# Usage: GROWW_API_KEY=... GROWW_API_SECRET=... bash tools/run_groww_validation.sh
set -euo pipefail
cd "$(dirname "$0")/.."
: "${GROWW_API_KEY:?set GROWW_API_KEY}"
: "${GROWW_API_SECRET:?set GROWW_API_SECRET}"
python3 tools/validate_groww_live.py
