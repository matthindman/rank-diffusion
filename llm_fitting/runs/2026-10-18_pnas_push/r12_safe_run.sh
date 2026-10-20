#!/bin/zsh
# Run one command with a swap watchdog: the job is killed if the machine's swap in use grows by more than
# SWAP_MAX_MB (default 400) over its value at the start, and its peak resident memory is reported.  Owner's
# instruction, 2026-10-20: sustained heavy swapping is not acceptable on this machine.
# Descendants are recorded at every check, so a kill reaches the grandchild behind heavy_lock.py too.
#
#     zsh llm_fitting/runs/2026-10-18_pnas_push/r12_safe_run.sh <log> <command> [args...]
set -u
log=$1; shift
max=${SWAP_MAX_MB:-400}
swap_mb() { sysctl -n vm.swapusage | sed -E 's/.*used = ([0-9.]+)M.*/\1/' | cut -d. -f1; }
tree() { local c; for c in $(pgrep -P $1); do echo $c; tree $c; done; }
# memory the system can hand over without swapping: free + purgeable + file-backed (cache) pages, in MB
reclaim_mb() { vm_stat | awk '/page size of/ {ps=$8} /Pages free/ {f=$3} /Pages purgeable/ {p=$3} /File-backed pages/ {fb=$3} END {printf "%d", (f+p+fb)*ps/1048576}'; }
need=$(( ${NEED_GB:-0} * 1024 ))
if [ $need -gt 0 ]; then                     # preflight: start only when the job fits without swapping
  told=0
  while [ $(reclaim_mb) -lt $need ]; do
    [ $told -eq 0 ] && echo "waiting: $(reclaim_mb) MB reclaimable without swap, job needs ${need} MB ($(date +%T))" | tee -a "$log.wait"
    told=1; sleep 30
  done
  [ $told -eq 1 ] && echo "starting: $(reclaim_mb) MB reclaimable ($(date +%T))" | tee -a "$log.wait"
fi
base=$(swap_mb)
/usr/bin/time -l "$@" > "$log" 2> "$log.time" &
job=$!
killed=0
sleep 3                        # let the job start its children before the first check
seen=""
maxtree=0
while kill -0 $job 2>/dev/null; do
  seen="$seen $(tree $job | tr '\n' ' ')"
  cur=$(for q in $(tree $job); do ps -o rss= -p $q 2>/dev/null; done | awk '{s+=$1} END {printf "%d", s/1024}')
  [ ${cur:-0} -gt $maxtree ] && maxtree=$cur
  now=$(swap_mb)
  if [ $((now - base)) -gt $max ]; then
    echo "WATCHDOG: swap grew from ${base}M to ${now}M; killing the job" | tee -a "$log"
    for p in $(tree $job) ${=seen}; do kill $p 2>/dev/null; done
    kill $job 2>/dev/null; killed=1; break
  fi
  sleep 5
done
wait $job 2>/dev/null
sleep 1
for p in ${=seen}; do kill -0 $p 2>/dev/null && [ $killed -eq 1 ] && kill -9 $p 2>/dev/null; done
rss=$(grep "maximum resident set size" "$log.time" | awk '{printf "%.2f", $1/1073741824}')
echo "peak RSS ${rss} GB (job tree, sampled: ${maxtree} MB); swap ${base}M -> $(swap_mb)M; killed ${killed}" | tee -a "$log"
[ $killed -eq 0 ]
