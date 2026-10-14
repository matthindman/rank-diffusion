# Evidence packet: movement and structure in online attention

_DRAFT for the owner and co-authors. Claude Opus 5.5 (Anthropic), 2026-10-15.
An index: every claim the paper might make, with its numbers, its record
section in `MODEL_STATUS.md` (canonical; quote from there), its figure, its
code and tests, and its standing, failures included. Claim language is the
owner's; nothing here changes the binding claim set (§6 addendum,
2026-07-12). Companion documents: `PAPER_FRAME_DRAFT_2026-10-15.md`,
`REVIEWER_QUESTIONS_2026-10-15.md`, `BALANCE_LAW_THEORY_NOTE_2026-10-14.md`._

## The argument in five sentences

1. Attention ladders keep their form while the names on them churn. A fifth
   of each top set is replaced each year.
2. A balance at every rank holds the form: crossings at a top set's boundary
   return what its members lose against the field, and the crossings are
   predicted by lasting volatility over twice the local slope.
3. The prediction holds rank by rank on Reddit comments, Facebook pages,
   Instagram (censoring modelled) and the body of Reddit submissions. It is
   not automatic: rank-only dynamics fail it.
4. Crossings return a steady amount each half-year, so the ladder changes
   only when members' drift by rank changes, as after Reddit's June 2021
   feed change.
5. The same quantities forecast, ex ante, who stays in the top. Incumbents
   turn out stickier than average movement implies.

## Claims and evidence

| # | claim | key numbers | record | figure | code, tests | standing |
|---|---|---|---|---|---|---|
| C1 | **The puzzle**: ladders keep their form while names churn | 79–84% of a Reddit top set remains after a year, 58–67% after three; median annual change of log rank 0.19–0.24; 7–10% halve or double their rank a year | §2z-cu | `fig_puzzle.png` | `runs/2026-10-15_puzzle/puzzle_data.py` | descriptive |
| C2 | **The exact balance**: shape change = members' movement + reordering; reordering is steady | comments: reordering +0.08 to +0.11 log units in every half-year at every rank, while members' term swings −0.29 to +0.07 | §2z-cr | `fig_shape_decomposition.png` | `runs/2026-10-15_comments_2021_head/broad_or_few.py` | exact accounting; post hoc |
| C3 | **The boundary theory**: crossings = σ²/(2S), predicted, rank by rank | comments 0.98–1.22 (variance route) and 0.85–1.01 (variogram) at every rank, K/64 … K; by start r = 0.94; Facebook 0.77–1.17; Instagram 0.94–1.12; submissions body 0.7–1.0 | §2z-cm, §2z-co, §2z-ct, §2z-cx | `fig_four_ladders.png`, `fig_boundary_theory.png` | `mts_boundary_law.py`, `mts_variogram.py`; `tests/test_mts_boundary_law.py`, `test_mts_variogram.py`, `test_ig_censoring_boundary.py` | registered: holds on comments, Facebook and Instagram totals; partial on submissions (head unmeasurable; shortfall at ranks of about 300–600) |
| C4 | **Not automatic** | rank-only rivals 2.4–3.6 (random displacement) and 0.3–1.7 (neighbour swaps); heavy tails, jumps and drift types 0.85–1.25; volatility types still sorting 1.2–1.3 (body), 2–4 (head) | §2z-cq, §2z-cv | — | `runs/2026-10-15_boundary_robustness/` | planted worlds |
| C5 | **Method**: once one-time constants are removed, ranking by current position and by permanent rank give the same lasting slide | lasting slides agree at 7 of 7 cut-offs on comments, submissions and Facebook, and 6 of 7 on Instagram; the one-time constants have the planted signs and size, e.g. momentum ≈ σ²L/(6S·D) | §2z-cm, §2z-ct, §2z-cx | — | `mts_boundary_law.py` | registered (B2, F2) |
| C6 | **The rate law accounts for change** | same-year form: 60 cells, slope 0.88, r 0.90 (half-year starts); 0.99 and rms 0.81 per 1,000 weeks with 26-week blocks over three starts | §2z-cd, §2z-cg, §2z-cm | `fig_rate_law_and_head_mobility.png` (A) | `mts_balance_law.py` | accounting with a boundary theory (§2z-cg's qualification) |
| C7 | **Structure from movement** | comments: observed ladder between the ladders each year's movement sustains; submissions head sustained far flatter. The ladder steepens down the ranks because variance rises (~0.007 to 0.013 a week) and the net slide falls (~0.005–0.011 to 0.003–0.004) | §2z-cd cells, §2z-cv | `fig_sustained_ladder.png`, `fig_why_it_curves.png` | `make_figures.py` | in sample |
| C8 | **Concentration counterfactual** | submissions top 78: 51% (2019) to 35% (2022) of the top 5,000's activity; movement sustains 16%. Comments top 195: 63% to 52%, sustained 53% | §2z-cw | `fig_concentration.png` | `runs/2026-10-15_concentration/` | index of geometric-mean activity; in sample |
| C9 | **Relaxation** | time constant of shape toward S*: comments 5–22 years, submissions 2–6 | §2z-cp | — | arithmetic on §2z-cd cells | order of magnitude |
| C10 | **A platform change** acts through drift and mobility | June 2021: onset 23–24 June; share of the first submissions position 8.3% to 4.3% in a quarter; head crossings 2.4 to 7.8 per 1,000 weeks across the three windows | §2z-bz, §2z-ca, §2z-cm, §2z-co | `fig_rate_law_and_head_mobility.png` (B) | `mts_boundary_law.py` (by start) | dated; causal magnitude not claimed |
| C11 | **Ex ante lock-in forecasts** | per-member Brier 13–21% below constant turnover on all four ladders, at par with a logistic fitted to outcomes; aggregate within ±0.07 in 75% of 126 cells; under-forecasts staying by 2–5 points at the first horizon, growing with horizon; fails against constant turnover at ≥ 3 blocks | §2z-cy, §2z-cz | `fig_lockin.png` | `mts_lockin.py`; `tests/test_mts_lockin.py` | registered: T1, T2 pass; T3 fails |
| C12 | **Instagram's censoring** modelled, validated and bounded | planted ratio 1.015 censored against 1.014 truth; default rules cannot read it; matching-post count ≤ 23–41% of movement | §2z-c, §2z-cx | `fig_instagram_censoring.png` | `ig_censoring_sim.py`; `runs/2026-10-15_instagram/` | registered (I1 pass, I2 fail, I3 pass) |
| C13 | **Forecasting structure is no better than trend** | submissions: law ≈ repeating the last change; comments: no change best; after June 2021 a quarter's movement did not beat trend | §2z-ce, §2z-co, §2z-cs | — | `mts_forward.py`, `mts_seasonal.py`, `mts_intervention.py` | registered failures, reported as a limit |
| C14 | **Entry and maturity** | entry net of exit 29–41% of the slide on comments, 8–19% on submissions; removing it would steepen comments' sustained slope 18–51% | §2z-cc, §2z-ck | — | `mts_replenish.py` | in sample |

## Scoreboard of registered predictions since §2z-cm

Each line is one prediction on one panel, scored by the rule written before
the run.

| entry | prediction | result |
|---|---|---|
| §2z-cm | B1 crossings follow σ²/2S: comments / submissions | PASS / FAIL |
| | B2 lasting slides agree: comments / submissions | PASS / PASS |
| | B3 lasting form of the law closes (pooled) | FAIL (slope 0.798 against a floor of 0.80; rms worse than the one-spacing form) |
| | B4 one-spacing pieces offset: comments / submissions | PASS / PASS |
| §2z-co | V1 estimators agree: comments / submissions | PASS / FAIL |
| | V3 crossings with the variogram: comments / submissions | PASS / FAIL |
| | Z1, Z2 seasonal forecasts on comments | FAIL, FAIL |
| | Z3 submissions lags beat no change | PASS |
| §2z-cs | J1, J2 intervention forecasts on submissions | FAIL, FAIL |
| | J3 comments unforecastable | PASS |
| §2z-ct | F1, F2 Facebook pages | PASS, PASS |
| §2z-cu | R1 comments robust to K and block length | NOT ADJUDICATED (2 of 3 variants pass; K = 25,000 not certifiable) |
| | R2 submissions body at two K | FAIL |
| §2z-cx | I1 Instagram totals; I2 per post; I3 raw unreadable | PASS, FAIL, PASS |
| §2z-cy | T1 discriminates; T2 calibrated; T3 extrapolates | PASS, PASS, FAIL |

**Tally: 15 of 26 scored items held; one not adjudicated.**
- The successes are the boundary theory where variance can be measured, the
  method, Facebook, Instagram totals, and lock-in discrimination.
- The failures cluster in forecasting structure, the submissions head and
  ranks of about 300–600, and noisy per-post Instagram.

## Known failures and open problems (the paper must say these)

1. **Reddit submissions.**
   - At the head, strata of 30–150 communities limit any estimate of lasting
     variance to about ±30%, so the theory cannot be tested there (§2z-co).
   - At ranks of about 300–600 crossings run 0.61–0.72 of prediction by every
     estimator. Neighbours moving together is ruled out; unexplained
     (§2z-cu, §2z-cv).
2. **Forecasting.** The law forecasts structure no better than the ladder's
   own trend (§2z-co, §2z-cs).
3. **Lock-in forecasts under-forecast staying.**
   - Planted worlds with plain diffusion, swings, volatility types, a static
     class, or weekly fat tails do not reproduce the under-forecast.
   - Block-scale jumps do, in size and in growth with horizon (§2z-cz).
   - A forecaster with explicit jumps is the next step.
4. **Instagram.** One year, one start. The per-post panel is too noisy, and
   level claims are out of scope (§2z-c, §2z-cx).
5. **Out of sample.** The sealed 2023–24 Reddit bridge and the Wikipedia
   panels are untouched. A revised protocol puts the boundary theory first
   (§2z-cp).
6. **Review.** Codex's review (row R5) and its planted worlds with moving
   drift (V5) are outstanding.

## Data and exposure

| ladder | data | status |
|---|---|---|
| Reddit comments | `reddit_comments_ext`, K = 12,500, Dec 2018 to Dec 2022 | DEVELOPMENT |
| Reddit submissions | `reddit_submissions_long`, K = 5,000 | DEVELOPMENT |
| Facebook pages | `facebook_a` (Era A), K = 3,500, 86 weeks | DEVELOPMENT |
| Instagram | `instagram_hm` and `instagram_pp`, K = 10,000, weeks 9–52 of 2023 | DEVELOPMENT through the §2z-c apparatus |
| Instagram, raw | `instagram` top-50k | RESTRICTED negative control, never calibrated to |

- No community, page or account name was read.
- Every contact is in `EXPOSURE_LEDGER.md`.
- Sealed folders are untouched.

## Reproduce

All from the repository root, with the registered interpreter (Python 3.11,
pandas 2.x). Heavy runs go through
`caffeinate -dims python scripts/heavy_lock.py --`.

| what | command |
|---|---|
| boundary theory | `python -u llm_fitting/mts_boundary_law.py run <panel> [block] [K]`, then `report` |
| Instagram | `python -u llm_fitting/runs/2026-10-15_instagram/ig_boundary.py <panel>` |
| lock-in | `python -u llm_fitting/mts_lockin.py run <panel>`, then `report` |
| variogram | `python -u llm_fitting/mts_variogram.py run <panel>` |
| seasonal | `python -u llm_fitting/mts_seasonal.py run <panel>` |
| intervention | `python -u llm_fitting/mts_intervention.py run <panel>` |
| figures, from saved outputs | `python llm_fitting/runs/2026-10-15_figures/make_figures.py` |
| checks before any commit | `bash scripts/health_check.sh`: both suites, the sealed-data guard, legacy guard 14/15, churn 0.013 |

## Positioning

- The law's mathematics is established: the forward equation for ordered
  levels, Fernholz's gap identity, the Atlas-model slope.
- New, as far as a targeted search found:
  - the out-of-equilibrium test of a predicted crossing term;
  - named-cohort estimation that removes the drift bias of ranking by current
    position;
  - the application to online attention and a dated algorithm change;
  - ex ante lock-in forecasts scored against outcomes.
- Must cite: Fernholz 2017; Fernholz & Koch; Itkin & Larsson 2024; Baker &
  Smyth 2026; Gabaix, Lasry, Lions & Moll 2016; Blumm et al. 2012; Iñiguez et
  al. 2022 (§2z-cn; `runs/2026-10-14_boundary_law/NOVELTY_SEARCH.md`).
