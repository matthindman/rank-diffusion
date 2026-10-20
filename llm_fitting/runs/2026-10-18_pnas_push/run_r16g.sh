#!/bin/zsh
# R16-G queue (PLAN.md R16-G, post hoc, registered before its run): every job alone under the swap watchdog (r12_safe_run.sh), from the lean cache,
# in the registered order.  Resumable: a finished step leaves a marker in $R12_CACHE; the queue stops at the first
# failed or killed job (owner, 2026-10-20: no sustained swapping; a killed run is not interpreted).
#
#     R12_CACHE=<dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r16g.sh       # from the repository root
set -u
[ -n "${R12_CACHE:-}" ] || { echo "R12_CACHE not set: stop"; exit 1; }
R=llm_fitting/runs/2026-10-18_pnas_push
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
SUB=reddit_submissions_long
COM=reddit_comments_ext
export RANKDIFF_HEAVY_SLOTS=1

need() { [ "$1" = "$COM" ] && echo 6 || echo 3; }

run() {                                   # run <marker> <need_gb> <log> <script> <args...>
  local mk=$R12_CACHE/r16_done__$1 gb=$2 log=$3; shift 3
  if [ -e "$mk" ]; then echo "skip $log"; return 0; fi
  echo "== $(date +%T) $log"
  NEED_GB=$gb caffeinate -dims zsh $R/r12_safe_run.sh $R/$log $PY -u "$@"
  local rc=$?
  if [ $rc -ne 0 ] || ! grep -q "killed 0" $R/$log; then
    echo "STOP: $log failed (exit $rc) or was killed; see $R/$log and $R/$log.time"; exit 1
  fi
  touch "$mk"
}

prm() { $PY -c "import json,sys; d=json.load(open('$R/'+sys.argv[1])); print(json.dumps(eval(sys.argv[2], {'d': d})))" "$@"; }
tag() { $PY -c "import json,sys; p=json.loads(sys.argv[2]); print(sys.argv[1] + '_' + '_'.join(f'{k}{v}' for k, v in p.items()))" "$@"; }

# G: a decline state on the joint optimum
for pn in $SUB $COM; do run jd_grid_$pn $(need $pn) out_r16_jd_grid_$pn.txt $R/r16_tier3.py jd_grid $pn; done
for pn in $SUB $COM; do
  P=$(prm r16_jd_grid__$pn.json "d['best']['prm']")
  T=$(tag joint_decline "$P")
  for s0 in 0 5; do
    run worlds_${T}_${s0}_$pn $(need $pn) out_r16_worlds_${T}_${pn}__seeds$s0.txt $R/r16_tier3.py worlds $pn joint_decline $s0 $((s0 + 4)) "$P"
  done
  run summ_${T}_$pn 2 out_r16_summ_${T}_$pn.txt $R/r16_tier3.py summarize $pn $T
  run oop_jdh_$pn $(need $pn) out_r16_oop_jdh_$pn.txt $R/r16_tier3.py oop $pn joint_decline_hist "$P"
done
echo "== $(date +%T) R16-G complete"
