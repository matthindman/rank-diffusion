#!/bin/zsh
# A5: one stream per panel, arms in order of importance. Output: gate_<arm>__<panel>.json and gates_<panel>.txt
cd "$(dirname "$0")/../../.."
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
panel=$1; k=$2
for arm in movinghome home movinghome_nodrift homedrift; do
  caffeinate -dims $PY scripts/heavy_lock.py -- $PY -u llm_fitting/runs/2026-10-18_pnas_push/gate_arm.py $panel $k $arm >> llm_fitting/runs/2026-10-18_pnas_push/gates_$panel.txt 2>&1
  echo "ARM DONE $arm exit $?" >> llm_fitting/runs/2026-10-18_pnas_push/gates_$panel.txt
done
echo "STREAM DONE" >> llm_fitting/runs/2026-10-18_pnas_push/gates_$panel.txt
