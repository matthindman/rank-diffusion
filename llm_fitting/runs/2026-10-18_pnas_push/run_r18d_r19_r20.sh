#!/bin/zsh
# Queue for PLAN.md addenda R18-D, R19 and R20 (registered at 2d4a33e). Every job alone under the swap watchdog
# (r12_safe_run.sh); Facebook jobs WITHOUT R12_CACHE and with R18_DIR (the R18 fit), Reddit jobs with R12_CACHE (the
# lean universe cache). Resumable via markers in $Q.
#
#     Q=<marker dir> R18_DIR=<R18 fit dir> R12_CACHE_DIR=<lean cache> [DEFER_COMMENTS=1] zsh llm_fitting/runs/2026-10-18_pnas_push/run_r18d_r19_r20.sh
# DEFER_COMMENTS=1 skips the comments jobs (6 GB each) without marking them, so a second pass runs them.
set -u
[ -n "${Q:-}" ] && [ -n "${R18_DIR:-}" ] && [ -n "${R12_CACHE_DIR:-}" ] || { echo "Q, R18_DIR and R12_CACHE_DIR must be set: stop"; exit 1; }
[ -d /Volumes/T9/rank-diffusion-data ] || { echo "T9 not mounted: stop"; exit 1; }
mkdir -p $Q
R=llm_fitting/runs/2026-10-18_pnas_push
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
export RANKDIFF_HEAVY_SLOTS=1
run() {                                    # run <marker> <fb|reddit> <GB> <log> <script args...>
  local mk=$Q/done__$1 kind=$2 gb=$3 log=$4; shift 4
  if [ -e "$mk" ]; then echo "skip $log"; return 0; fi
  if [ -n "${DEFER_COMMENTS:-}" ] && [[ $log == *comments* ]]; then echo "deferred $log"; return 0; fi   # memory: comments need 6 GB
  echo "== $(date +%T) $log"
  if [ $kind = fb ]; then
    env -u R12_CACHE R18_DIR=$R18_DIR NEED_GB=$gb caffeinate -dims zsh $R/r12_safe_run.sh $R/$log $PY -u "$@"
  else
    R12_CACHE=$R12_CACHE_DIR NEED_GB=$gb caffeinate -dims zsh $R/r12_safe_run.sh $R/$log $PY -u "$@"
  fi
  local rc=$?
  if [ $rc -ne 0 ] || ! grep -q "killed 0" $R/$log; then echo "STOP: $log failed (exit $rc) or was killed"; exit 1; fi
  touch "$mk"
}
gb_of() { [ $1 = reddit_comments_ext ] && echo 6 || echo 3; }

# R18-D: Facebook from a matched starting ladder
run r18d_matchcal fb 2 out_r18d_matchcal.txt $R/r18_facebook.py matchcal
C=$($PY -c "import json; print(json.load(open('$R/r18d_matchcal.json'))['c'])")
for v in controlb0.019m$C calibb0.019m$C; do
  for s0 in 0 5 10 15; do run r18d_${v}_${s0} fb 2 out_r18d_worlds_${v}__seeds${s0}.txt $R/r18_facebook.py worlds $v $s0 $((s0 + 4)); done
done
run r18d_summ fb 2 out_r18d_summ.txt $R/r18_facebook.py summ

# R19: Reddit at Facebook's settings and dates
for pn in reddit_submissions_long reddit_comments_ext; do
  run r19_data_$pn reddit $(gb_of $pn) out_r19_data_$pn.txt $R/r19_fbsettings.py data $pn
  run r19_boot_$pn reddit $(gb_of $pn) out_r19_boot_$pn.txt $R/r19_fbsettings.py boot $pn
  for v in control_num control; do
    for s0 in 0 10; do run r19_${pn}_${v}_${s0} reddit $(gb_of $pn) out_r19_worlds_${pn}_${v}__seeds${s0}.txt $R/r19_fbsettings.py worlds $pn $v $s0 $((s0 + 9)); done
  done
  run r19_summ_$pn reddit 1 out_r19_summ_$pn.txt $R/r19_fbsettings.py summ $pn
done

# R20: the fading-surge world
run r20_moments_facebook_a fb 2 out_r20_moments_facebook_a.txt $R/r20_surge.py moments facebook_a
for pn in reddit_submissions_long reddit_comments_ext; do
  run r20_moments_$pn reddit $(gb_of $pn) out_r20_moments_$pn.txt $R/r20_surge.py moments $pn
done
run r20_calib_fb fb 2 out_r20_calib_facebook_a__fbcontrol.txt $R/r20_surge.py calib facebook_a fbcontrol
for s0 in 0 5 10 15; do run r20_eval_fb_$s0 fb 2 out_r20_eval_facebook_a__fbcontrol__seeds$s0.txt $R/r20_surge.py eval facebook_a fbcontrol $s0 $((s0 + 4)); done
run r20_summ_fb fb 1 out_r20_summ_facebook_a__fbcontrol.txt $R/r20_surge.py summ facebook_a fbcontrol
for pn in reddit_submissions_long reddit_comments_ext; do
  for b in control_num joint; do
    run r20_calib_${pn}_$b reddit $(gb_of $pn) out_r20_calib_${pn}__$b.txt $R/r20_surge.py calib $pn $b
    for s0 in 0 5 10 15; do run r20_eval_${pn}_${b}_$s0 reddit $(gb_of $pn) out_r20_eval_${pn}__${b}__seeds$s0.txt $R/r20_surge.py eval $pn $b $s0 $((s0 + 4)); done
    run r20_summ_${pn}_$b reddit 1 out_r20_summ_${pn}__$b.txt $R/r20_surge.py summ $pn $b
  done
done
echo "== $(date +%T) R18-D, R19 and R20 complete"
