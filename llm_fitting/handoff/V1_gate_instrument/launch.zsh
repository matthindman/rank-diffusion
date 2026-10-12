#!/bin/zsh
# Restartable V1 launcher. Every bank waits for all recorded settings to pass.
set -eu
cd "$(dirname "$0")/../../.."
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V1_gate_instrument
mkdir -p "$OUT/launch_logs"
for phase in reference bank; do
  "$PY" scripts/heavy_lock.py -- caffeinate -dims "$PY" -u llm_fitting/validation/gate_instrument.py run --panel facebook_a --phase "$phase" > "$OUT/launch_logs/facebook_a_${phase}.log" 2>&1 &
  a=$!
  "$PY" scripts/heavy_lock.py -- caffeinate -dims "$PY" -u llm_fitting/validation/gate_instrument.py run --panel reddit --phase "$phase" > "$OUT/launch_logs/reddit_${phase}.log" 2>&1 &
  b=$!
  "$PY" scripts/heavy_lock.py -- caffeinate -dims "$PY" -u llm_fitting/validation/gate_instrument.py run --panel reddit_comments --phase "$phase" > "$OUT/launch_logs/reddit_comments_${phase}.log" 2>&1 &
  c=$!
  wait "$a"
  wait "$b"
  "$PY" scripts/heavy_lock.py -- caffeinate -dims "$PY" -u llm_fitting/validation/gate_instrument.py run --panel reddit_submissions_long --phase "$phase" > "$OUT/launch_logs/reddit_submissions_long_${phase}.log" 2>&1 &
  d=$!
  wait "$c"
  wait "$d"
done
