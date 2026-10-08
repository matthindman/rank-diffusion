#!/bin/zsh
# Submissions backtest P6->P9, registered one-pass evaluations (K=5000 from
# P3). Run in a user-owned Terminal to avoid the background-task kills seen in
# the agent session. Each is fail-closed: nonzero exit = a real failure to
# report (not a kill). caffeinate prevents idle sleep on the long runs.
set -u
cd /Users/hindman/Documents/GitHub/rank-diffusion
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
RUN=llm_fitting/runs/2026-07-16_submissions_ig

run() {   # name  logfile  args...
  local name="$1" log="$2"; shift 2
  echo "==== $name  ($(date '+%H:%M:%S')) ===="
  caffeinate -i "$PY" -u "$@" > "$RUN/$log" 2>&1
  local rc=$?
  echo "---- $name exit $rc; verdict lines: ----"
  tail -6 "$RUN/$log"
  echo
  return $rc
}

run "P6 movement gate" phase3_p6.log \
    llm_fitting/gate_verdicts.py p6 --top-k 5000

run "P7 exit audit" phase3_p7.log \
    llm_fitting/exit_audit.py --p7 --platform reddit_submissions_long --top-k 5000

run "P8 head law" phase3_p8.log \
    llm_fitting/e5_headlaw.py reddit_submissions_long --top-k 5000

run "P9 VR decomposition" phase3_p9.log \
    llm_fitting/subs_backtest.py p9 --top-k 5000

echo "==== all four submitted; logs in $RUN/ ===="
