#!/bin/zsh
# The rest of R12 after the owner's instruction of 2026-10-20 (no sustained swapping): one job at a time, every job
# reading the lean universe cache (r12_cache.py, checked exact by r12_cache_check.py) instead of a full panel, every
# job under the swap watchdog (r12_safe_run.sh), stopping at the first watchdog kill.  Replaces the unfinished
# parts of run_r12_r13.sh (step 5 and the comments factor out-of-period run) and run_r12_after.sh.
#
#     R12_CACHE=<dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r12_lean.sh       # from the repository root
set -u
cd "$(dirname "$0")/../../.."
[ -n "${R12_CACHE:-}" ] || { echo "R12_CACHE not set: stop"; exit 1; }
P=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
S=llm_fitting/runs/2026-10-18_pnas_push
export RANKDIFF_HEAVY_SLOTS=1
job() { local log=$1; shift; caffeinate -dims zsh $S/r12_safe_run.sh $log $P scripts/heavy_lock.py -- $P -u "$@" || { echo "stopped at $log"; exit 1; }; }

# 0. the out-of-period cache is exact: the saved closed-form control must be reproduced
cp $S/r12_oop__reddit_submissions_long__control.json $R12_CACHE/saved_oop_control_subs.json
job $S/out_r12_oop_control_reddit_submissions_long__cachecheck.txt $S/r12_hetgrowth.py oop reddit_submissions_long control
cmp -s $S/r12_oop__reddit_submissions_long__control.json $R12_CACHE/saved_oop_control_subs.json \
  && echo "oop cache check: identical" || { echo "oop cache check: DIFFERS: stop"; cp $R12_CACHE/saved_oop_control_subs.json $S/r12_oop__reddit_submissions_long__control.json; exit 1; }

# 1. recovery (registered step 5; A4: nothing planted uses the numeric drift)
for pn in reddit_submissions_long reddit_comments_ext; do
  for s in 0.0 0.15 0.3 0.45; do job $S/out_r12_recovery_${s}_$pn.txt $S/r12_hetgrowth.py recovery $pn $s 0 19; done
done
# 2. the matched control (A4): worlds and out of period; then the comments factor out-of-period rerun
for pn in reddit_submissions_long reddit_comments_ext; do
  job $S/out_r12_worlds_control_num_$pn.txt $S/r12_hetgrowth.py worlds $pn control_num 0 19
  $P $S/r12_hetgrowth.py summarize $pn control_num > $S/out_r12_summary_control_num_$pn.txt 2>&1
  job $S/out_r12_oop_control_num_$pn.txt $S/r12_hetgrowth.py oop $pn control_num
done
job $S/out_r12_oop_factor_reddit_comments_ext.txt $S/r12_hetgrowth.py oop reddit_comments_ext factor
# 3. post hoc grid extension (not registered)
for pn in reddit_submissions_long reddit_comments_ext; do job $S/out_r12_gridx_fixed_$pn.txt $S/r12_hetgrowth.py gridx $pn fixed; done
echo "R12 lean runs finished"
