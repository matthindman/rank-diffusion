#!/bin/bash
set -euo pipefail
cd /Users/hindman/Documents/GitHub/rank-diffusion-codex
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V2_planted_mechanisms
pids=()
for panel in facebook_a reddit_comments reddit_submissions_long; do
  caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_extension.py --panel "$panel" --out "$OUT" >> "$OUT/extension_${panel}.log" 2>&1 &
  pids+=($!)
done
failed=0
for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
exit "$failed"
