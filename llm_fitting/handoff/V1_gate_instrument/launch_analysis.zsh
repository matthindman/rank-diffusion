#!/bin/zsh
set -eu
cd "$(dirname "$0")/../../.."
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_V1_gate_instrument
for panel in facebook_a reddit reddit_comments reddit_submissions_long; do
  while [[ ! -f "$OUT/$panel/bank/COMPLETE.json" ]]; do
    # Stop waiting if the scientific reference check explicitly failed.
    "$PY" - "$OUT" <<'PY'
import json,sys
from pathlib import Path
for f in Path(sys.argv[1]).glob('*/reference/RESULT.json'):
    if json.loads(f.read_text()).get('reference_pass') is False:
        raise SystemExit('Reference mismatch: analysis is stopped.')
PY
    sleep 30
  done
  "$PY" scripts/heavy_lock.py -- caffeinate -dims "$PY" -u llm_fitting/validation/gate_instrument_analysis.py --panel "$panel" > "$OUT/launch_logs/${panel}_analysis.log" 2>&1
  print "Analysis complete: $panel"
done
"$PY" llm_fitting/validation/gate_instrument_report.py
