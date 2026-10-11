#!/usr/bin/env bash
# Repository health check: research suite, package suite, legacy guard.
#
# Run from anywhere:  scripts/health_check.sh [--no-guard]
# Exit status is nonzero if any check fails. Run it before changing anything
# and again before every commit (see .claude/skills/repo-maintenance).
#
# The interpreter defaults to the registered one when it exists; override
# with RANKDIFF_PYTHON=/path/to/python.

set -u
cd "$(dirname "$0")/.." || exit 2

REGISTERED=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
PY="${RANKDIFF_PYTHON:-}"
if [ -z "$PY" ]; then
  if [ -x "$REGISTERED" ]; then PY="$REGISTERED"; else PY=python3; fi
fi

RUN_GUARD=1
[ "${1:-}" = "--no-guard" ] && RUN_GUARD=0

fail=0
echo "interpreter: $PY"
"$PY" - <<'EOF' || fail=1
import sys
import numpy, pandas
print(f"python {sys.version.split()[0]}  pandas {pandas.__version__}  numpy {numpy.__version__}")
if int(pandas.__version__.split(".")[0]) >= 3:
    print("WARNING: pandas >= 3 — the research suite is validated on pandas 2.x "
          "(MODEL_STATUS 2z-ai); expect intake-gate test failures.")
EOF

echo "== research suite (tests/)"
"$PY" -m pytest tests/ -q -p no:cacheprovider 2>&1 | tail -1
[ "${PIPESTATUS[0]}" -eq 0 ] || fail=1

echo "== sealed-data guard"
"$PY" scripts/sealed_data_guard.py --self-check || fail=1

echo "== package suite (Python/rankdiff/tests)"
"$PY" -m pytest Python/rankdiff/tests -q -p no:cacheprovider 2>&1 | tail -1
[ "${PIPESTATUS[0]}" -eq 0 ] || fail=1

GUARD_DATA=data/raw/fb_ranked_weekly_cutdown.parquet
if [ "$RUN_GUARD" -eq 1 ]; then
  echo "== legacy guard (facebook legacy panel, defaults; expect 14/15, churn 0.013)"
  if [ -f "$GUARD_DATA" ]; then
    out=$("$PY" -u llm_fitting/minimal_rankdiff.py facebook 2>&1 | grep 'v4.3-style score')
    echo "${out:-  (no score line found)}"
    case "$out" in
      *"score: 14/15"*"churn error: 0.013"*) ;;
      *) echo "LEGACY GUARD FAILED"; fail=1 ;;
    esac
  else
    echo "  skipped: $GUARD_DATA not present (local data file; not in git)"
  fi
fi

if [ "$fail" -eq 0 ]; then echo "HEALTH CHECK: PASS"; else echo "HEALTH CHECK: FAIL"; fi
exit "$fail"
