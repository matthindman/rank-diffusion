# Rank Diffusion

Modeling the macro distribution of digital attention: platform rankings show
**Eulerian stability with Lagrangian churn** — the rank-size curve and per-rank
share are stationary while the identities occupying fixed ranks turn over. The
project's three goals:

1. **G1 — individual movement**: predict how far a page / subreddit / channel
   moves across ranks over time, out-of-sample;
2. **G2 — aggregate structure**: reproduce the stationary rank-size curve and
   fixed-rank occupant churn;
3. **G3 — parsimony**: both with one theoretically coherent mechanism, as
   simple as possible but no simpler.

**Canonical status document: [`llm_fitting/MODEL_STATUS.md`](llm_fitting/MODEL_STATUS.md)**
(model spec, locked-in results, corrected pitfalls, reproduction commands).

## Current model (2026-07)

Weekly log-activity of endpoint *i*: an OU "home" level (reversion κ(z)
estimated from the change-autocovariance tail) + measurement noise
(independently identified from the daily-within-week noise floor on Reddit) +
persistent entity-level volatility heterogeneity ("temperament", one
moment-identified scalar s ≈ 0.9 on both platforms) + Gabaix rebirth at the
bottom of a pre-registered top-coverage universe (Reddit K=5,000 ≈ 90% of
weekly karma, buffer 4K; FB K=3,500). With σ_obs pinned to the daily floor,
the fitted transitory AR(1) component collapses to zero — the model
simplified under identification.

Headline (Reddit, K=5,000): in-sample 14/15 with rank displacement exact at
1/4/13-week horizons; rolling-origin OOS movement gate at par with the
persistence baseline with 100% bootstrap-CI coverage. Facebook: OOS at par
(0.158 ± 0.027 vs 0.146 ± 0.022). Spec-A (weekly covariances) and Spec-B
(daily replication) agree on σ_obs within ~25% — identified, not calibrated.

## Quick start (Python prototypes — primary)

```sh
PY=/path/to/python3.11   # numpy 2, pandas 2, scipy 1.14, pytest

PYTHONPATH=src $PY -m pytest tests/ -q            # 28 tests

# in-sample scorecard, full stack (Reddit)
$PY llm_fitting/minimal_rankdiff.py reddit --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --spec-b

# OOS movement gate (the acceptance criterion)
$PY llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --spec-b
$PY llm_fitting/rankdiff_kalman.py facebook --oos --temperament \
    --min-knot-entities 8 --md-lags 6

# sigma_obs identification report (Spec-A vs Spec-B)
$PY llm_fitting/spec_b_sigma_obs.py 5000
```

## Layout

- `llm_fitting/minimal_rankdiff.py` — minimal prototype: universe construction,
  Lagrangian estimation (MD covariance fit), simulator, 15-metric scorecard.
- `llm_fitting/rankdiff_kalman.py` — Kalman UC estimator + the rolling-origin
  distributional OOS movement gate.
- `llm_fitting/spec_b_sigma_obs.py` — daily-noise-floor identification of σ_obs.
- `llm_fitting/temperament_vs_finebands.py` — rejection battery for the
  fine-σ(rank) alternative hypothesis.
- `llm_fitting/MODEL_STATUS.md` — canonical record; `research_notes.md` —
  literature and data analyses; `iteration_log.md` — session history.
- `src/rankdiff/` + `pyproject.toml` — the v4.3 modular package (predecessor
  of the current prototypes; kept as reference implementation).
- `tests/` — regression suite (band alignment, universe construction,
  temperament recovery, MD partition recovery).
- `R/`, `report/`, `analysis_archive/`, `rankdiffR/` — legacy R pipeline and
  archived analyses (the original K_cut/buffer construction lives here).

## Data (not committed)

- Facebook: `data/raw/fb_ranked_weekly_cutdown.parquet` — **always the cutdown
  file** (the full file has corrupt CrowdTangle data after ~88 weeks).
- Reddit: `data/reddit/reddit_weekly.parquet` (T=30) and
  `reddit_daily.parquet` (Spec-B).
- Instagram: `llm_fitting/ig_weekly_ranked_top50k.parquet` — negative control
  ONLY (query-censored collection); never calibrate to it.
