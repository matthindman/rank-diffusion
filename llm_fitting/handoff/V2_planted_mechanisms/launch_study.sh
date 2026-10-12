#!/bin/bash
set -euo pipefail
cd /Users/hindman/Documents/GitHub/rank-diffusion-codex
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V2_planted_mechanisms
"$PY" -c 'import json; d=json.load(open("llm_fitting/runs/2026-10-12_V2_planted_mechanisms/crosscheck_summary.json")); assert d["status"]=="PASS", "V2 stopping rule"'
# Independent native panels use the shared three-slot lock, including Claude.
# Each worker is restartable from atomic per-seed checkpoints.
for stage in recovery diagnostics; do
  pids=()
  for panel in facebook_a reddit_comments reddit_submissions_long; do
    caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_study.py "$stage" --panel "$panel" --out "$OUT" >> "$OUT/${stage}_${panel}.log" 2>&1 &
    pids+=($!)
  done
  failed=0
  for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
  if [[ "$failed" != 0 ]]; then exit 1; fi
done
caffeinate -dims "$PY" -u scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/planted_summary.py --out "$OUT" >> "$OUT/summary.log" 2>&1
