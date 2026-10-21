#!/bin/zsh
# R18 queue (PLAN.md addendum R18): the common model on Facebook. Every job alone under the swap watchdog
# (r12_safe_run.sh), WITHOUT R12_CACHE (Facebook has no lean cache); resumable via markers in $R18_DIR.
#
#     R18_DIR=<scratch dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r18.sh       # from the repository root
set -u
unset R12_CACHE
[ -n "${R18_DIR:-}" ] || { echo "R18_DIR not set: stop"; exit 1; }
[ -d /Volumes/T9/rank-diffusion-data ] || { echo "T9 not mounted: stop"; exit 1; }
R=llm_fitting/runs/2026-10-18_pnas_push
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
export RANKDIFF_HEAVY_SLOTS=1
run() {
  local mk=$R18_DIR/r18_done__$1 gb=$2 log=$3; shift 3
  if [ -e "$mk" ]; then echo "skip $log"; return 0; fi
  echo "== $(date +%T) $log"
  NEED_GB=$gb caffeinate -dims zsh $R/r12_safe_run.sh $R/$log $PY -u "$@"
  local rc=$?
  if [ $rc -ne 0 ] || ! grep -q "killed 0" $R/$log; then echo "STOP: $log failed (exit $rc) or was killed"; exit 1; fi
  touch "$mk"
}
run fit 3 out_r18_fit.txt $R/r18_facebook.py fit
run data 3 out_r18_data.txt $R/r18_facebook.py data
for v in calib control flat; do
  for s0 in 0 5 10 15; do run worlds_${v}_${s0} 3 out_r18_worlds_${v}__seeds${s0}.txt $R/r18_facebook.py worlds $v $s0 $((s0 + 4)); done
done
run summ 2 out_r18_summ.txt $R/r18_facebook.py summ
echo "== $(date +%T) R18 complete"
