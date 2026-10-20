#!/bin/zsh
# The rest of R12 after run_r12_lean2.sh's watchdog stopped the comments recovery at s 0.3 (2026-10-20, 16:0x; swap
# +505 MB at seed 11 although the job started with 3 GB reclaimable).  Seeds now run in chunks of five, each in a
# fresh process (a seed's result depends only on the seed and the deterministic fit, so chunking changes nothing;
# recovery merges chunks by seed, worlds write one file per seed).  Every job waits for the memory it needs
# (NEED_GB) and runs under the swap watchdog, which now also logs the job's own sampled peak memory.
#
#     R12_CACHE=<dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r12_lean3.sh       # from the repository root
set -u
cd "$(dirname "$0")/../../.."
[ -n "${R12_CACHE:-}" ] || { echo "R12_CACHE not set: stop"; exit 1; }
P=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
S=llm_fitting/runs/2026-10-18_pnas_push
export RANKDIFF_HEAVY_SLOTS=1
job() { local need=$1 log=$2; shift 2; NEED_GB=$need caffeinate -dims zsh $S/r12_safe_run.sh $log $P scripts/heavy_lock.py -- $P -u "$@" || { echo "stopped at $log"; exit 1; }; }
pn=reddit_comments_ext
for s in 0.3 0.45; do
  for c in 0 5 10 15; do job 3 $S/out_r12_recovery_${s}_${pn}__seeds$c.txt $S/r12_hetgrowth.py recovery $pn $s $c $((c + 4)); done
done
for c in 0 5 10 15; do job 3 $S/out_r12_worlds_control_num_${pn}__seeds$c.txt $S/r12_hetgrowth.py worlds $pn control_num $c $((c + 4)); done
$P $S/r12_hetgrowth.py summarize $pn control_num > $S/out_r12_summary_control_num_$pn.txt 2>&1
job 3 $S/out_r12_oop_control_num_$pn.txt $S/r12_hetgrowth.py oop $pn control_num
job 4 $S/out_r12_oop_factor_$pn.txt $S/r12_hetgrowth.py oop $pn factor
# post hoc grid extension (not registered)
job 2 $S/out_r12_gridx_fixed_reddit_submissions_long.txt $S/r12_hetgrowth.py gridx reddit_submissions_long fixed
job 3 $S/out_r12_gridx_fixed_reddit_comments_ext.txt $S/r12_hetgrowth.py gridx reddit_comments_ext fixed
echo "R12 lean runs finished"
