#!/bin/zsh
# R20-A1 queue (PLAN.md amendment R20-A1, committed fc1bd35 before its run): the surge calibration with half-lives
# 78, 104 and 156 added, on both Reddit ladders and bases; evaluation only where the optimum moved. Alone under the
# swap watchdog, with the lean cache; resumable via markers in $Q.
#
#     Q=<marker dir> R12_CACHE_DIR=<lean cache> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r20a1.sh
set -u
[ -n "${Q:-}" ] && [ -n "${R12_CACHE_DIR:-}" ] || { echo "Q and R12_CACHE_DIR must be set: stop"; exit 1; }
mkdir -p $Q
R=llm_fitting/runs/2026-10-18_pnas_push
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
export RANKDIFF_HEAVY_SLOTS=1
run() {
  local mk=$Q/done__$1 gb=$2 log=$3; shift 3
  if [ -e "$mk" ]; then echo "skip $log"; return 0; fi
  echo "== $(date +%T) $log"
  R12_CACHE=$R12_CACHE_DIR NEED_GB=$gb caffeinate -dims zsh $R/r12_safe_run.sh $R/$log $PY -u "$@"
  local rc=$?
  if [ $rc -ne 0 ] || ! grep -q "killed 0" $R/$log; then echo "STOP: $log failed (exit $rc) or was killed"; exit 1; fi
  touch "$mk"
}
for pn in reddit_submissions_long reddit_comments_ext; do
  gb=3; [ $pn = reddit_comments_ext ] && gb=6
  for b in control_num joint; do
    run a1_calibx_${pn}_$b $gb out_r20a1_calibx_${pn}__$b.txt $R/r20_surge.py calibx $pn $b
    moved=$($PY -c "import json; a=json.load(open('$R/r20_calib__${pn}__$b.json'))['best']; x=json.load(open('$R/r20_calibx__${pn}__$b.json'))['best']; print(int(any(a[k] != x[k] for k in ('lam','mu','hl'))))")
    if [ $moved = 1 ]; then
      for s0 in 0 5 10 15; do run a1_evalx_${pn}_${b}_$s0 $gb out_r20a1_evalx_${pn}__${b}__seeds$s0.txt $R/r20_surge.py evalx $pn $b $s0 $((s0 + 4)); done
      run a1_summx_${pn}_$b 1 out_r20a1_summx_${pn}__$b.txt $R/r20_surge.py summx $pn $b
    else
      echo "optimum unchanged for $pn $b: no evaluation (R20-A1)"
    fi
  done
done
echo "== $(date +%T) R20-A1 complete"
