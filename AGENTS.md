# rank-diffusion — Agent Instructions (ChatGPT/Codex, Gemini, and other agents)

This repository is a mature, externally reviewed research program modeling
rank dynamics of digital attention (PNAS-track). It has strict, hard-won
methodological rules. **Before doing ANY modeling, fitting, evaluation, data
intake, or paper work, read these in order:**

1. `.claude/skills/stochastic-modeling/SKILL.md` — **the core skill. Read it
   in full, every session.** It encodes the working method, the frozen
   per-platform specs, the pitfall catalogue, and the current agenda.
2. `llm_fitting/MODEL_STATUS.md` — the canonical record of results. Newest
   dated §2 subsections supersede older ones. All numbers live here, not in
   your memory.
3. `llm_fitting/CONFIRMATION_PROTOCOL.md` — registered frozen evaluations for
   extension data. If your task touches new data, this binds you absolutely.

## Non-negotiables (digest — the skill has the full contract)

- The rolling-origin OOS movement gate (≥5 splits, vs persistence, with CI
  coverage) is the ONLY acceptance criterion. In-sample scorecards never
  justify adopting a change. Never report single-split OOS numbers.
- No spec-fishing: every parameter comes from a declared moment or an
  independent instrument. Pre-register predictions before running; report
  failures as findings.
- Defaults stay byte-identical: new behavior behind opt-in flags, gated rng
  streams; the legacy guard (`facebook` legacy panel default run: 14/15,
  churn 0.013) and `python -m pytest tests/ -q` (repo root) must pass at
  every commit.
- Estimate by permanent (time-averaged) rank, never current rank (Eulerian
  selection bias). Universe membership is absence-penalized.
- FB data: only `data/raw/fb_ranked_weekly_cutdown.parquet` (legacy) or the
  SSD Era-A slice; never `fb_ranked_weekly.parquet`. Instagram is a negative
  control — never calibrate to it. Reddit is a census; FB is a censored
  sample — coverage language differs (see skill §4.2).
- Head-collision metrics have seed SD ~±0.15: never interpret 5-rep head
  churn diffs without bands (use `scorecard_bands.py`, reps ≥ 20).
- Document every substantive session as a new dated subsection appended to
  `llm_fitting/MODEL_STATUS.md` (diagnosis → measurements → change →
  pre-registered predictions scored → validation table → verdict →
  reproduction commands). Never rewrite history; supersede explicitly.
- Frozen specs, thresholds, defaults, and the confirmation protocol are
  owner-gated. If a task seems to require changing them, stop and ask.

## Environment notes

- Registered interpreter: `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`.
- Tests import `archive.src.rankdiff...`; run pytest from repo root, no
  PYTHONPATH needed.
- SSD panels need `/Volumes/T9` mounted (`data/ssd`); the comments extension
  raw data needs the WD drive. If unmounted, report and stop that thread.
- Use `python -u` for long runs with redirected output; OOS gates can take
  minutes-to-hours — do not kill them early.
