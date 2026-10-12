#!/bin/bash
set -euo pipefail
cd /Users/hindman/Documents/GitHub/rank-diffusion-codex
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V2_planted_mechanisms
# Refuse to proceed unless the fixed cross-check passed.
"$PY" -c 'import json; d=json.load(open("llm_fitting/runs/2026-10-12_V2_planted_mechanisms/crosscheck_summary.json")); assert d["status"]=="PASS", "V2 stopping rule"'
# Native panels are sequential to bound memory on the shared 16GB host.
for stage in recovery diagnostics; do
  for panel in facebook_a reddit_comments reddit_submissions_long; do
    caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_study.py "$stage" --panel "$panel" --out "$OUT" >> "$OUT/study.log" 2>&1
  done
done
caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_summary.py --out "$OUT" >> "$OUT/summary.log" 2>&1
