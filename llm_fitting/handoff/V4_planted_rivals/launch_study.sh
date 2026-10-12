#!/usr/bin/env bash
# Start from a user-owned Terminal; atomic cells make an interrupted run resumable.
set -euo pipefail
cd "$(dirname "$0")/../../.."
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-13_V4_planted_rivals
export OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1
mkdir -p "$OUT"
stamp=$(date +%Y%m%dT%H%M%S)
caffeinate -dims "$PY" -u scripts/heavy_lock.py -- \
  "$PY" -u llm_fitting/validation/balance_rivals_study.py prepare --with-absence --out "$OUT" \
  2>&1 | tee "$OUT/prepare_$stamp.log"
caffeinate -dims "$PY" -u scripts/heavy_lock.py -- \
  "$PY" -u llm_fitting/validation/balance_rivals_study.py run --out "$OUT" \
  2>&1 | tee "$OUT/launch_$stamp.log"
caffeinate -dims "$PY" -u scripts/heavy_lock.py -- \
  "$PY" -u llm_fitting/validation/balance_rivals_report.py --out "$OUT" \
  2>&1 | tee "$OUT/report_$stamp.log"
