#!/bin/bash
set -eu
cd "$(dirname "$0")"
export PYTHONDONTWRITEBYTECODE=1
export TMPDIR="$PWD/.runtime/tmp" XDG_CACHE_HOME="$PWD/.runtime/cache" MPLCONFIGDIR="$PWD/.runtime/cache"
export OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1
export RANKDIFF_HEAVY_LOCK="$PWD/.runtime/rankdiff-heavy" RANKDIFF_HEAVY_SLOTS=1
mkdir -p "$TMPDIR" "$XDG_CACHE_HOME"
py=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
cache=/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache
for panel in facebook_a instagram_hm reddit_comments_ext reddit_submissions_long
do
  test -f "$cache/$panel.npz"
  if [[ "$panel" != facebook_a && -e "$cache/${panel}_spec_b.json" ]]; then
    echo "Unexpected instrument for $panel. Stopped before loading panels."
    exit 1
  fi
done
test -f "$cache/facebook_a_spec_b.json"
mode=both
if [[ -s candidate_frozen_gate_results.json ]]; then
  if [[ -s candidate_gold_benchmark_results.json ]]; then exit 0; fi
  mode=gold
fi
exec /usr/bin/caffeinate -dims "$py" -u ../../../../scripts/heavy_lock.py -- \
  "$py" -u ../benchmark_candidate.py --module "$PWD/common_rankdiff_astra_r7.py" --out-dir "$PWD" --mode "$mode"
