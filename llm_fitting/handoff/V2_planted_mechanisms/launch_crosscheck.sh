#!/bin/bash
set -euo pipefail
cd /Users/hindman/Documents/GitHub/rank-diffusion-codex
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V2_planted_mechanisms
mkdir -p "$OUT"
# Each panel is sequential to bound memory alongside Claude's jobs.
for panel in facebook_a reddit_comments reddit_submissions_long; do
  caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_crosscheck.py run --panel "$panel" --out "$OUT" >> "$OUT/crosscheck.log" 2>&1
done
caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_crosscheck.py summarize --out "$OUT" >> "$OUT/crosscheck.log" 2>&1
