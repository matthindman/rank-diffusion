#!/bin/bash
set -euo pipefail
cd /Users/hindman/Documents/GitHub/rank-diffusion-codex
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
exec caffeinate -dims "$PY" scripts/heavy_lock.py -- "$PY" -u llm_fitting/validation/endpoint_run.py --panel "$1" --out output/V3_endpoint_scores/runs --exports output/V3_endpoint_scores/exports
