#!/bin/zsh
# R17, after run_r17.sh: the item-count asymmetry (PLAN.md amendment R17-B3, committed before this runs) and the post hoc
# frame check of the pooled crossing ratio.  Same rules as run_r17.sh: one job at a time under the swap watchdog, from
# the lean cache; resumable via markers; stops at the first failed or killed job.  asym_items streams the T9 daily panels.
#
#     R12_CACHE=<dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r17b.sh       # from the repository root
set -u
[ -n "${R12_CACHE:-}" ] || { echo "R12_CACHE not set: stop"; exit 1; }
[ -d /Volumes/T9/rank-diffusion-data ] || { echo "T9 not mounted: stop"; exit 1; }
R=llm_fitting/runs/2026-10-18_pnas_push
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
SUB=reddit_submissions_long
COM=reddit_comments_ext
export RANKDIFF_HEAVY_SLOTS=1
need() { [ "$1" = "$COM" ] && echo 6 || echo 3; }
run() {
  local mk=$R12_CACHE/r17_done__$1 gb=$2 log=$3; shift 3
  if [ -e "$mk" ]; then echo "skip $log"; return 0; fi
  echo "== $(date +%T) $log"
  NEED_GB=$gb caffeinate -dims zsh $R/r12_safe_run.sh $R/$log $PY -u "$@"
  local rc=$?
  if [ $rc -ne 0 ] || ! grep -q "killed 0" $R/$log; then echo "STOP: $log failed (exit $rc) or was killed"; exit 1; fi
  touch "$mk"
}
for pn in $SUB $COM; do run asym_items_$pn $(need $pn) out_r17_asym_items_$pn.txt $R/r17_round8.py asym_items $pn; done
for pn in $SUB $COM; do run bframe_$pn $(need $pn) out_r17_boundary_frame_$pn.txt $R/r17_boundary_frame.py $pn; done
echo "== $(date +%T) R17b complete"
