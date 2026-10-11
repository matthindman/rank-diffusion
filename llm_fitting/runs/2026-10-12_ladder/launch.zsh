#!/bin/zsh
# Launches every ladder cell (see DESIGN.md).  Safe to re-run: a cell whose
# JSON exists is skipped, so a killed process costs only its unfinished cell.
# Staggered because the long submissions panel needs about 10 GB while its
# universes are built.
cd /Users/hindman/Documents/GitHub/rank-diffusion || exit 2
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
OUT=llm_fitting/runs/2026-10-12_ladder
mkdir -p $OUT/logs $OUT/cells

run() {  # run <logname> <args...>
  local name=$1; shift
  caffeinate -dims $PY -u llm_fitting/ladder_explainer.py run --out $OUT "$@" \
      > $OUT/logs/proc_$name.log 2>&1 &
  echo "$(date +%H:%M:%S) started $name (pid $!)"
}

wait_for() {  # wait_for <logname> <pattern>: until the pattern or the process's last line appears
  until grep -qE "$2|ALL REQUESTED CELLS DONE|cells to run: \[\]|Traceback" $OUT/logs/proc_$1.log 2>/dev/null; do
    sleep 20
  done
  echo "$(date +%H:%M:%S) $1 reached: $2"
}

# First launched 00:32 from the desktop app's Terminal panel.  The two
# processes that read /Volumes/T9 blocked in open() there (macOS removable-
# volume permission for that host; nothing was read, nothing was scored) and
# were killed.  Relaunched 00:37 from the agent shell, which can read T9.  The
# short-submissions process uses local data and kept running from the first
# launch, so it is not started twice.
run subs_long --panel reddit_submissions_long
run fb --panel facebook_a
pgrep -f "ladder_explainer.py run .*--panel reddit$" > /dev/null || run subs_short --panel reddit
wait_for subs_long "panel slimmed"
run comments_gate_a --panel reddit_comments --kinds gate --rungs R0 R2 R4 G1
wait_for comments_gate_a "panel slimmed"
run comments_gate_b --panel reddit_comments --kinds gate --rungs R1 R3 R5 G2
wait_for comments_gate_b "panel slimmed"
run comments_struct --panel reddit_comments --kinds struct
wait_for fb "ALL REQUESTED CELLS DONE"
run instagram --panel instagram
wait
while pgrep -f "ladder_explainer.py run" > /dev/null; do sleep 30; done
echo "$(date +%H:%M:%S) LADDER LAUNCHER FINISHED: $(ls $OUT/cells | grep -c json) cells on disk"
grep -l Traceback $OUT/logs/proc_*.log 2>/dev/null | sed 's/^/TRACEBACK IN /'
