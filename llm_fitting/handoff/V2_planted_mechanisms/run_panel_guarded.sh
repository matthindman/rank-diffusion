#!/bin/bash
set -euo pipefail
cd /Users/hindman/Documents/GitHub/rank-diffusion-codex
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V2_planted_mechanisms
panel="$1"
"$PY" -u llm_fitting/validation/planted_warning_guard.py study recovery --panel "$panel" --out "$OUT"
"$PY" -u llm_fitting/validation/planted_warning_guard.py extension --panel "$panel" --out "$OUT"
"$PY" -u llm_fitting/validation/planted_warning_guard.py study diagnostics --panel "$panel" --out "$OUT"
