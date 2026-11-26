#!/bin/zsh
# Round 8 benchmark launcher: exact Gate v1 + Gold via ../benchmark_candidate.py on a candidate module copy.
#   zsh run_benchmark.sh <tag> [mode]     -> outputs in runs/<tag>/ (module snapshot saved beside them)
set -eu
cd "$(dirname "$0")"
tag=$1; mode=${2:-both}
out="$PWD/runs/$tag"; mkdir -p "$out" .runtime/tmp .runtime/cache
cp common_rankdiff_opus_r8.py "$out/module_snapshot.py"
export PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/.runtime/tmp" XDG_CACHE_HOME="$PWD/.runtime/cache" MPLCONFIGDIR="$PWD/.runtime/cache"
export OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1
export RANKDIFF_HEAVY_LOCK="$PWD/.runtime/rankdiff-heavy" RANKDIFF_HEAVY_SLOTS=1
py=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
exec /usr/bin/caffeinate -dims zsh ../../2026-10-18_pnas_push/r12_safe_run.sh "$out/benchmark.log" \
  "$py" -u ../../../../scripts/heavy_lock.py -- \
  "$py" -u ../benchmark_candidate.py --module "$out/module_snapshot.py" --out-dir "$out" --mode "$mode"
