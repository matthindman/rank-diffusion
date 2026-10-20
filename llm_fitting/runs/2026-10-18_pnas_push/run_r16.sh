#!/bin/zsh
# R16 queue (PLAN.md addendum R16): every job alone under the swap watchdog (r12_safe_run.sh), from the lean cache,
# in the registered order.  Resumable: a finished step leaves a marker in $R12_CACHE; the queue stops at the first
# failed or killed job (owner, 2026-10-20: no sustained swapping; a killed run is not interpreted).
#
#     R12_CACHE=<dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r16.sh       # from the repository root
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

# E: the capture audit (data)
for pn in $SUB $COM; do
  run capture_daily_$pn 3 out_r16_capture_daily_$pn.txt $R/r16_capture.py daily $pn
  run capture_rosters_$pn $(need $pn) out_r16_capture_rosters_$pn.txt $R/r16_capture.py rosters $pn
done
# moments for B
for pn in $SUB $COM; do run moments_$pn $(need $pn) out_r16_moments_$pn.txt $R/r16_tier3.py moments $pn; done
# C: the A1 runs and the objective under correct specification
for pn in $SUB $COM; do run a1_$pn $(need $pn) out_r16_a1_$pn.txt $R/r16_tier3.py a1 $pn; done
# A: the decline state
for pn in $SUB $COM; do run decline_grid_$pn $(need $pn) out_r16_decline_grid_$pn.txt $R/r16_tier3.py decline_grid $pn; done
for pn in $SUB $COM; do
  for pi in 0.1 0.25 0.5; do
    P=$(prm r16_decline_grid__$pn.json "d['best_by_pi']['$pi']['prm']")
    T=$(tag decline "$P")
    run worlds_${T}_$pn $(need $pn) out_r16_worlds_${T}_$pn.txt $R/r16_tier3.py worlds $pn decline 0 9 "$P"
    run summ_${T}_$pn 2 out_r16_summ_${T}_$pn.txt $R/r16_tier3.py summarize $pn $T
  done
  P=$(prm r16_decline_grid__$pn.json "d['best']['prm']")
  run oop_decline_$pn $(need $pn) out_r16_oop_decline_$pn.txt $R/r16_tier3.py oop $pn decline "$P"
  run oop_decline_hist_$pn $(need $pn) out_r16_oop_decline_hist_$pn.txt $R/r16_tier3.py oop $pn decline_hist "$P"
done
# B: the joint re-estimation
for pn in $SUB $COM; do run joint_grid_$pn $(need $pn) out_r16_joint_grid_$pn.txt $R/r16_tier3.py joint_grid $pn; done
for pn in $SUB $COM; do
  P=$(prm r16_joint_grid__$pn.json "d['best']['prm']")
  T=$(tag joint "$P")
  for s0 in 0 5 10 15; do
    run worlds_${T}_${s0}_$pn $(need $pn) out_r16_worlds_${T}_${pn}__seeds$s0.txt $R/r16_tier3.py worlds $pn joint $s0 $((s0 + 4)) "$P"
  done
  run summ_${T}_$pn 2 out_r16_summ_${T}_$pn.txt $R/r16_tier3.py summarize $pn $T
  run oop_joint_$pn $(need $pn) out_r16_oop_joint_$pn.txt $R/r16_tier3.py oop $pn joint "$P"
done
# D: the size exponent with the world's own passing parts (comments first)
for pn in $COM $SUB; do
  for s0 in 0 5; do run beta_${s0}_$pn $(need $pn) out_r16_beta_${pn}__seeds$s0.txt $R/r16_tier3.py beta $pn $s0 $((s0 + 4)); done
done
# F: durability in the calibrated model
for pn in $SUB $COM; do run durability_$pn $(need $pn) out_r16_durability_$pn.txt $R/r16_tier3.py durability $pn; done
echo "== $(date +%T) R16 queue complete"
