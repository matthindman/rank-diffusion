#!/bin/zsh
# After run_r12_r13.sh: amendment A4 (the matched control, control_num: worlds and out-of-period head share, 20 seeds
# each) and the post hoc extension of the fixed grid (gridx; not registered).  Waits for the main run to finish.
#
#     zsh llm_fitting/runs/2026-10-18_pnas_push/run_r12_after.sh            # from the repository root
set -u
cd "$(dirname "$0")/../../.."
P=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
L=scripts/heavy_lock.py
S=llm_fitting/runs/2026-10-18_pnas_push
export RANKDIFF_HEAVY_SLOTS=${RANKDIFF_HEAVY_SLOTS:-2}
run() { caffeinate -dims $P $L -- $P -u "$@"; }
until grep -qE "R12 and R13 runs finished|missing .*: stop|T9 not mounted" $S/out_run_r12_r13.txt 2>/dev/null; do sleep 60; done
grep -q "R12 and R13 runs finished" $S/out_run_r12_r13.txt || { echo "main run did not finish: stop"; exit 1; }

for pn in reddit_submissions_long reddit_comments_ext; do run $S/r12_hetgrowth.py worlds $pn control_num 0 19 > $S/out_r12_worlds_control_num_$pn.txt 2>&1 & done; wait
for pn in reddit_submissions_long reddit_comments_ext; do $P $S/r12_hetgrowth.py summarize $pn control_num > $S/out_r12_summary_control_num_$pn.txt 2>&1; done
for pn in reddit_submissions_long reddit_comments_ext; do run $S/r12_hetgrowth.py oop $pn control_num > $S/out_r12_oop_control_num_$pn.txt 2>&1 & done; wait
for pn in reddit_submissions_long reddit_comments_ext; do run $S/r12_hetgrowth.py gridx $pn fixed > $S/out_r12_gridx_fixed_$pn.txt 2>&1 & done; wait
echo "R12 after-runs finished"
