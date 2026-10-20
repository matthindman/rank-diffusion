#!/bin/zsh
# The rest of run_r12_lean3.sh after its watchdog stopped comments recovery s 0.45 seeds 15-19 at seed 19 (2026-10-20 ~17:58;
# swap +543 MB while the job itself sampled 2.5 GB).  Same safeguards, tighter swap limit (300 MB); finished chunks are
# not rerun (recovery 0.30 all seeds; 0.45 seeds 0-14).
#
#     R12_CACHE=<dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r12_lean4.sh       # from the repository root
set -u
cd "$(dirname "$0")/../../.."
[ -n "${R12_CACHE:-}" ] || { echo "R12_CACHE not set: stop"; exit 1; }
P=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
S=llm_fitting/runs/2026-10-18_pnas_push
export RANKDIFF_HEAVY_SLOTS=1 SWAP_MAX_MB=300
job() { local need=$1 log=$2; shift 2; NEED_GB=$need caffeinate -dims zsh $S/r12_safe_run.sh $log $P scripts/heavy_lock.py -- $P -u "$@" || { echo "stopped at $log"; exit 1; }; }
pn=reddit_comments_ext
job 3 $S/out_r12_recovery_0.45_${pn}__seeds15b.txt $S/r12_hetgrowth.py recovery $pn 0.45 15 19
for c in 0 5 10 15; do job 3 $S/out_r12_worlds_control_num_${pn}__seeds$c.txt $S/r12_hetgrowth.py worlds $pn control_num $c $((c + 4)); done
$P $S/r12_hetgrowth.py summarize $pn control_num > $S/out_r12_summary_control_num_$pn.txt 2>&1
job 3 $S/out_r12_oop_control_num_$pn.txt $S/r12_hetgrowth.py oop $pn control_num
job 4 $S/out_r12_oop_factor_$pn.txt $S/r12_hetgrowth.py oop $pn factor
job 2 $S/out_r12_gridx_fixed_reddit_submissions_long.txt $S/r12_hetgrowth.py gridx reddit_submissions_long fixed
job 3 $S/out_r12_gridx_fixed_reddit_comments_ext.txt $S/r12_hetgrowth.py gridx reddit_comments_ext fixed
echo "R12 lean runs finished"
