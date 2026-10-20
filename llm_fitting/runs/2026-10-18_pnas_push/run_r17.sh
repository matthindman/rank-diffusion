#!/bin/zsh
# R17 queue (PLAN.md addendum R17): every job alone under the swap watchdog (r12_safe_run.sh), from the lean cache, in
# the registered order.  Resumable via markers in $R12_CACHE; stops at the first failed or killed job.
#
#     R12_CACHE=<dir> zsh llm_fitting/runs/2026-10-18_pnas_push/run_r17.sh       # from the repository root
set -u
[ -n "${R12_CACHE:-}" ] || { echo "R12_CACHE not set: stop"; exit 1; }
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
tag() { $PY -c "import json,sys; p=json.loads(sys.argv[2]); print(sys.argv[1] + '_' + '_'.join(f'{k}{v}' for k, v in p.items()))" "$@"; }
prm() { $PY -c "import json,sys; d=json.load(open('$R/'+sys.argv[1])); print(json.dumps(eval(sys.argv[2], {'d': d})))" "$@"; }
J_SUB='{"s": 0.8, "head": 0.25, "km": 0.5, "m": 0.8}'; J_COM='{"s": 0.6, "head": 0.25, "km": 0.5, "m": 0.7}'
D_SUB='{"pi": 0.25, "s": 0.7, "hl": 208, "m": 1.0}'; D_COM='{"pi": 0.25, "s": 0.7, "hl": 208, "m": 0.7}'
# A on the data; B
for pn in $SUB $COM; do run asym_data_$pn $(need $pn) out_r17_asym_data_$pn.txt $R/r17_round8.py asym_data $pn; done
for pn in $SUB $COM; do run table_data_$pn $(need $pn) out_r17_table_data_$pn.txt $R/r17_round8.py table_data $pn; done
run events_$SUB 3 out_r17_events_$SUB.txt $R/r17_round8.py events $SUB
# A's worlds
for pn in $SUB $COM; do
  if [ $pn = $SUB ]; then J=$J_SUB; DD=$D_SUB; else J=$J_COM; DD=$D_COM; fi
  for spec in "control_num|{}|20" "joint|$J|20" "decline|$DD|10"; do
    v=${spec%%|*}; rest=${spec#*|}; P=${rest%|*}; n=${rest##*|}
    T=$(tag $v "$P")
    for s0 in $(seq 0 5 $((n - 1))); do
      run asymw_${T}_${s0}_$pn $(need $pn) out_r17_asymw_${T}_${pn}__seeds$s0.txt $R/r17_round8.py asym_worlds $pn $v $s0 $((s0 + 4)) "$P"
    done
    run asyms_${T}_$pn 2 out_r17_asyms_${T}_$pn.txt $R/r17_round8.py asym_summ $pn $T
  done
done
# C
for pn in $SUB $COM; do for w in control_num joint; do run dur2_${w}_$pn $(need $pn) out_r17_dur2_${w}_$pn.txt $R/r17_round8.py durability2 $pn $w; done; done
# D
for pn in $SUB $COM; do run jgrid2_$pn $(need $pn) out_r17_jgrid2_$pn.txt $R/r17_round8.py jgrid2 $pn; done
for pn in $SUB $COM; do run jcalib_$pn $(need $pn) out_r17_jcalib_$pn.txt $R/r17_round8.py jcalib $pn; done
for pn in $SUB $COM; do run jdgrid2_$pn $(need $pn) out_r17_jdgrid2_$pn.txt $R/r17_round8.py jdgrid2 $pn; done
for pn in $SUB $COM; do
  P=$(prm r17_jdgrid2__$pn.json "d['best']['prm']"); T=$(tag joint_decline "$P")
  for s0 in 0 5; do run asymw_${T}_${s0}_$pn $(need $pn) out_r17_asymw_${T}_${pn}__seeds$s0.txt $R/r17_round8.py asym_worlds $pn joint_decline $s0 $((s0 + 4)) "$P"; done
  run asyms_${T}_$pn 2 out_r17_asyms_${T}_$pn.txt $R/r17_round8.py asym_summ $pn $T
  P=$(prm r17_jgrid2__$pn.json "d['best_all']['prm']"); T=$(tag joint "$P")
  if [ ! -e $R12_CACHE/r17_done__asyms_${T}_$pn ]; then
    for s0 in 0 5 10 15; do run asymw_${T}_${s0}_$pn $(need $pn) out_r17_asymw_${T}_${pn}__seeds$s0.txt $R/r17_round8.py asym_worlds $pn joint $s0 $((s0 + 4)) "$P"; done
    run asyms_${T}_$pn 2 out_r17_asyms_${T}_$pn.txt $R/r17_round8.py asym_summ $pn $T
  fi
done
echo "== $(date +%T) R17 queue complete"
