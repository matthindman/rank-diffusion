#!/bin/bash
set -euo pipefail
cd /Users/hindman/Documents/GitHub/rank-diffusion-codex
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V2_planted_mechanisms
HAND=llm_fitting/handoff/V2_planted_mechanisms
# Refuse an incomplete study before any final aggregation.
"$PY" -u llm_fitting/validation/planted_audit.py --out "$OUT"
caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_summary.py --include-extensions --out "$OUT"
"$PY" -u llm_fitting/validation/planted_report.py --out "$OUT" --handoff "$HAND"
"$PY" -u llm_fitting/validation/planted_plot.py --out "$OUT"
