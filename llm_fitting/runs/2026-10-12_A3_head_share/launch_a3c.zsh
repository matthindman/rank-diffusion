#!/bin/zsh
# A3 addendum 2: the structure card under each seeding, five panels in sequence.
# Run from the repository root through scripts/heavy_lock.py.
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
L=llm_fitting/runs/2026-10-12_A3_head_share/logs
for p in facebook_a reddit reddit_submissions_long instagram reddit_comments; do
  "$PY" -u llm_fitting/phase1_a3.py run_c "$p" > "$L/a3c_$p.log" 2>&1
done
echo "A3C CELLS FINISHED"
grep -h "card" "$L"/a3c_*.log
