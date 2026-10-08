#!/bin/zsh
# Phase-0 intake gate (A1.1), BOUNDED-MEMORY rewrite (2026-07-16 disclosed
# execution correction). Registered command; check_long_panels.py defaults
# already equal the registered endpoints, so no flags are passed.
# Fail-closed: direct redirect (NO tee/pipe) so the checker's real exit
# status propagates -- 0 == PASS, nonzero == INTAKE FAIL / crash.
cd /Users/hindman/Documents/GitHub/rank-diffusion
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
LOG=llm_fitting/runs/2026-07-16_submissions_ig/phase0_intake_gate.log
exec caffeinate -i "$PY" -u llm_fitting/check_long_panels.py \
  data/ssd/derived/reddit_daily_long.parquet \
  data/ssd/derived/reddit_weekly_long.parquet \
  data/ssd/logs/reddit_monthly_processing_log.csv \
  > "$LOG" 2>&1
