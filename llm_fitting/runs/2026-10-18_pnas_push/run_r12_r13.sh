#!/bin/zsh
# R12 (PLAN.md addendum R12 with amendments A1, A2) and the blocked parts of R13 (R13c, R13d), in order.
# Needs /Volumes/T9 mounted (the registered Reddit panels). Heavy jobs go through scripts/heavy_lock.py (at most
# RANKDIFF_HEAVY_SLOTS at a time; the machine thrashes with more than about three model estimations).
#
#     zsh llm_fitting/runs/2026-10-18_pnas_push/run_r12_r13.sh            # from the repository root
#     FROM=2 zsh llm_fitting/runs/2026-10-18_pnas_push/run_r12_r13.sh     # resume at step 2 (steps 0-1 done)
# 2026-10-20: the first launch ran steps 0 and 1 and stopped in step 2 on a key error in r12_hetgrowth.aux_vector
# (JSON keys); fixed, and the later steps now refuse to run without the calibration files they read.
set -u
cd "$(dirname "$0")/../../.."
[ -d /Volumes/T9/rank-diffusion-data ] || { echo "T9 not mounted: stop"; exit 1; }
P=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
L=scripts/heavy_lock.py
S=llm_fitting/runs/2026-10-18_pnas_push
export RANKDIFF_HEAVY_SLOTS=${RANKDIFF_HEAVY_SLOTS:-2}
run() { caffeinate -dims $P $L -- $P -u "$@"; }
FROM=${FROM:-0}
need() { for f in "$@"; do [ -s "$f" ] || { echo "missing $f: stop"; exit 1; }; done; }

# 0. R13c-d on the data (retention over one and two years; the funnel quantiles)
[ $FROM -le 0 ] && for pn in reddit_submissions_long reddit_comments_ext; do run $S/lockin_funnel.py $pn > $S/out_r13_lockin_$pn.txt 2>&1 & done; wait

# 1. data auxiliary statistics for R12 (and the data's asymmetry and lag profile)
[ $FROM -le 1 ] && for pn in reddit_submissions_long reddit_comments_ext; do run $S/r12_hetgrowth.py moments $pn > $S/out_r12_moments_$pn.txt 2>&1 & done; wait

# 2. calibration grids: fixed (s x m), then decay at the m chosen by fixed
need $S/r12_moments__reddit_submissions_long.json $S/r12_moments__reddit_comments_ext.json
[ $FROM -le 2 ] && for pn in reddit_submissions_long reddit_comments_ext; do run $S/r12_hetgrowth.py grid $pn fixed > $S/out_r12_grid_fixed_$pn.txt 2>&1 & done; wait
need $S/r12_grid__reddit_submissions_long__fixed.json $S/r12_grid__reddit_comments_ext__fixed.json
[ $FROM -le 2 ] && for pn in reddit_submissions_long reddit_comments_ext; do
  m=$($P -c "import json; print(json.load(open('$S/r12_grid__${pn}__fixed.json'))['best']['m'])")
  run $S/r12_hetgrowth.py grid $pn decay $m > $S/out_r12_grid_decay_$pn.txt 2>&1 &
done; wait

# 3. assessment worlds, 20 seeds each, at the calibrated parameters (control and factor have none)
need $S/r12_grid__reddit_submissions_long__decay.json $S/r12_grid__reddit_comments_ext__decay.json
[ $FROM -le 3 ] && for pn in reddit_submissions_long reddit_comments_ext; do
  bf=$($P -c "import json; b=json.load(open('$S/r12_grid__${pn}__fixed.json'))['best']; print(b['s'], '-', b['m'])")
  bd=$($P -c "import json; b=json.load(open('$S/r12_grid__${pn}__decay.json'))['best']; print(b['s'], b['half_life'], b['m'])")
  run $S/r12_hetgrowth.py worlds $pn control 0 19 > $S/out_r12_worlds_control_$pn.txt 2>&1 &
  run $S/r12_hetgrowth.py worlds $pn factor 0 19 > $S/out_r12_worlds_factor_$pn.txt 2>&1 &
  wait
  run $S/r12_hetgrowth.py worlds $pn fixed 0 19 ${=bf} > $S/out_r12_worlds_fixed_$pn.txt 2>&1 &
  run $S/r12_hetgrowth.py worlds $pn decay 0 19 ${=bd} > $S/out_r12_worlds_decay_$pn.txt 2>&1 &
  wait
  for v in control factor fixed decay; do $P $S/r12_hetgrowth.py summarize $pn $v > $S/out_r12_summary_${v}_$pn.txt 2>&1; done
done

# 4. out of period (fit before 2021; the 2022 head share), 20 seeds each
[ $FROM -le 4 ] && for pn in reddit_submissions_long reddit_comments_ext; do
  bf=$($P -c "import json; b=json.load(open('$S/r12_grid__${pn}__fixed.json'))['best']; print(b['s'], '-', b['m'])")
  bd=$($P -c "import json; b=json.load(open('$S/r12_grid__${pn}__decay.json'))['best']; print(b['s'], b['half_life'], b['m'])")
  run $S/r12_hetgrowth.py oop $pn control > $S/out_r12_oop_control_$pn.txt 2>&1 &
  run $S/r12_hetgrowth.py oop $pn factor > $S/out_r12_oop_factor_$pn.txt 2>&1 &
  wait
  run $S/r12_hetgrowth.py oop $pn fixed ${=bf} > $S/out_r12_oop_fixed_$pn.txt 2>&1 &
  run $S/r12_hetgrowth.py oop $pn decay ${=bd} > $S/out_r12_oop_decay_$pn.txt 2>&1 &
  wait
done

# 5. recovery of sigma_0 and beta under planted fixed heterogeneity (amendment A1: s in 0, 0.15, 0.30, 0.45)
for pn in reddit_submissions_long reddit_comments_ext; do
  for s in 0.0 0.15 0.3 0.45; do run $S/r12_hetgrowth.py recovery $pn $s 0 19 > $S/out_r12_recovery_${s}_$pn.txt 2>&1 & done; wait
done
echo "R12 and R13 runs finished"
