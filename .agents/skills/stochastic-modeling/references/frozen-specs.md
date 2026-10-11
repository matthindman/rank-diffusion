# Frozen specifications and flags

Read this before running or interpreting any fit, scorecard or gate, and
before proposing a change to how the model is run.

The stacks below are frozen: they are what the committed record was produced
with, and changing them is an owner decision made at a re-freeze. Numbers
are quoted from MODEL_STATUS §2z-g (the NNLS re-freeze of 2026-07-11) as
reference values for reproduction checks. MODEL_STATUS is canonical; if a
number here disagrees with it, MODEL_STATUS wins and this file needs fixing.

## Contents

1. Why there are two stacks per panel
2. Frozen stacks and reference values
3. Flag glossary with scope conditions
4. Gate v1, exactly
5. Known gaps between the stated model and the frozen simulators
6. What a new ranked system is expected to show

## 1. Why there are two stacks per panel

The structure stack (in-sample card) and the movement stack (out-of-sample
gate) differ on purpose. Long-difference moments D(h) identify slow
structure on a full panel but destabilize the short rolling train windows
the gate uses; `--md-vr` regressed the FB gate from 0.114 to 0.179 for that
reason (§2i). This is a stated sample-size scope condition. It is not a
specification search, and a paper reader must be told which stack produced
which number.

## 2. Frozen stacks and reference values

The estimator is exact NNLS in the minimum-distance solves (the default
since §2z-g, by owner adoption after a pre-registered audit).
`--legacy-clip` reproduces results recorded before that re-freeze and is a
reproduction and sensitivity arm only.

| panel (registry name, K) | structure stack → card, churn error | movement stack → gate |
|---|---|---|
| FB Era A (`facebook_a`, 3,500) | `--temperament --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long --stat-factor --two-scale --mix-hetero` → 14/15, 0.029 (the one miss is the Pers4 period-0 knife-edge) | Spec-B (centered floor) + `--conditional state` → 0.123 ± 0.033 vs baseline 0.145 ± 0.031; coverage 60%; better on 4 of 5 splits; scale 1.0 on 4 of 5 |
| Reddit comments (`reddit_comments`, 12,500) | long stack: `--md-vr-long --stat-factor --two-scale --mix-hetero` plus temperament, pooling, md6, t → 12/15, 0.044 | md6 + t + mix + `--conditional state` → 0.171 ± 0.046 vs 0.165 ± 0.062; coverage 60%; at par |
| Reddit submissions, short panel (`reddit`, 5,000) | temperament + pooling + md6 + t → 14/15, 0.052 | same + `--conditional state` → 0.164 ± 0.053 vs 0.168 ± 0.004; coverage 80%; at par |
| Instagram rescue (`instagram_hm_ts`, 10,000) | temperament + pooling + md6 + t + stat-factor → 8/15, 0.062 (20 reps) | md6 + t + `--conditional state` + exact per-origin membership → 0.320 ± 0.189 vs 0.593 ± 0.309; better on 4 of 5; pooled CRPS skill ≈ 0 |

Notes that travel with the table:

- The ± values are spread across splits, not confidence intervals for an
  advantage. "Baseline" is the train window's displacement distribution
  (historical mobility), not a no-change forecast. "Coverage" is the share
  of splits where the simulated median lies inside the empirical median's
  bootstrap interval; it is not predictive-interval coverage.
- Instagram is a breadth demonstration, not paper-primary. Its estimand is
  "of a-matching activity" (§2z-c). It is leak-free only with exact
  per-origin fixed membership (`ig_trainsafe_members.py` plus
  `--member-ids-file`, §2z-f); a pre-cut superset is not enough.
- The legacy-solve "better on 4 of 5" for the short submissions panel is
  sensitive to the solver convention. It is SI sensitivity material, never a
  main-text claim.
- The registered confirmation (E2, §2z-q) and the submissions backtest (P6,
  §2z-ae) have their own frozen commands in `CONFIRMATION_PROTOCOL.md` and
  `PREREG_2026-07-16_submissions_ig.md`.
- K = 12,500 for comments is the owner's fuller-universe choice, fixed
  before fitting (about 98.8% of activity; §2g-X P3). The 90% rule would
  give 2,500. The other panels use the 90% value.
- The reference values are for comparison after a run. They are not
  targets, and a rerun that differs is a finding to report, with reps and
  seeds checked against the section that recorded the number.

Gate commands as recorded (exact NNLS is now the default, so `--nnls` is
not needed):

```
# FB Era A (§2z-e)
python llm_fitting/rankdiff_kalman.py facebook_a --oos --top-k 3500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --spec-b --conditional state --dist-scores
# Reddit comments (§2l)
python llm_fitting/rankdiff_kalman.py reddit_comments --oos --top-k 12500 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --mix-hetero --conditional state
# Reddit submissions, short panel (§2f)
python llm_fitting/rankdiff_kalman.py reddit --oos --top-k 5000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state
# Instagram rescue (§2z-f); build the exact membership file first
python llm_fitting/ig_trainsafe_members.py
python llm_fitting/rankdiff_kalman.py instagram_hm_ts --oos --top-k 10000 --temperament \
    --min-knot-entities 8 --md-lags 6 --t-tails --conditional state --dist-scores \
    --member-ids-file llm_fitting/ig_trainsafe_members.parquet
```

Full structure-card commands are at the end of §2z-g (FB Era A) and §2d
(short submissions). The comments long stack is the FB Era A flag set run
on `reddit_comments --top-k 12500`. The registered interpreter is
`/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`, with
pandas 2.x (see `pitfalls.md`, Environment).

## 3. Flag glossary with scope conditions

| flag | what it does | scope condition |
|---|---|---|
| `--top-k K` | pre-registered coverage universe | K comes from concentration statistics alone, never from fit quality |
| `--temperament` | entity amplitude v_i | s comes from the variance-dispersion moment only |
| `--min-knot-entities 8` | pools sparse head knots | on in every working stack |
| `--md-lags 6` | minimum-distance fit to γ₀…γ₆; estimates κ(z), σ_obs | compare κ only at matched `md_lags`; γ₀…γ₆ alone is nearly flat in κ |
| `--t-tails` | Student-t fast innovations, df from within-entity kurtosis | inert where σ_trans ≈ 0 |
| `--md-vr` | adds D(h), h ∈ {2, 4, 8, 13}; identifies κ | long panels only; destabilizes short rolling train windows |
| `--md-vr-long` | adds D(26), D(52) | needs T ≥ 2.5h; separates directional slow movement from diffusive wander |
| `--stat-factor` | stationary common level, ρ_L measured | recommended for in-sample structure runs; absent from cohort and gate simulations by construction |
| `--two-scale` | medium transitory component ξ₂ | requires `--md-vr`; helped FB, not comments |
| `--mix-hetero` | scales σ_perm by v^(b/2), b = s(h*)/s(1) | b = 1 is the main model; `--mix-b-fix 1.0` imposes it under common random numbers |
| `--spec-b` | pins σ_obs to the centered daily noise floor | the centered floor pᵀCΣCp is canonical (§2r); the legacy floor is reproduction-only |
| `--conditional state\|vhat` | cohort simulation from the filtered end-of-train state | the lever that brought submissions to par; does not help FB under Spec-A |
| `--cond-home state\|trainmean` | OU anchor in the conditional simulation | `state` is the committed choice; `trainmean` is a separate long-run-home diagnostic |
| `--oos` | the rolling-origin movement gate | the adoption criterion (Gate v1 below) |
| `--dist-scores` | pooled CRPS skill, PIT coverage and W1 on the gate | descriptive; scores the pooled displacement law, not endpoint-level predictions |
| `--nnls` | exact NNLS in the MD solves | the default since §2z-g; flag kept for compatibility |
| `--legacy-clip` | clipped-OLS MD solves as before §2z-g | reproduction arm only; never in a registered evaluation |
| `--eul-level` | within-entity level-variance anchor on the MD objective | refuted by its own adoption gates (§2z-t); never a default |
| `--s-vintage W` | amplitude spread s from the trailing W weeks | harmless in-panel; kept as a declared option (§2z-v) |

## 4. Gate v1, exactly

Gate v1 is the adoption criterion and is frozen in full. Any change to it is
a new, separately named evaluation, never an edit.

- Rolling-origin, at least 5 splits; never a single split.
- The command-line defaults (`--reps 3`, `--boot 400`) are smoke-test
  values. A run that supports a recommendation, and every registered run,
  uses 20 reps and 2,000 bootstrap draws.
- Scored vector: median |ΔRank| at h = 1, 4, 13; RACF1; coll1, coll5,
  coll20. Relative error per moment, averaged; moments with |empirical| below
  `MOM_FLOOR = 0.02` are left out of the average.
- Displacement is survivor-conditional (both endpoints observed). Exits are
  examined separately by the boundary-flux rows.
- Compared with the train-window historical-mobility baseline.
- Universe membership is train-window-only inside the gate.
- Under Spec-A one σ_obs scale is calibrated on the train moment vector;
  under Spec-B σ_obs is pinned from the daily floor. "Scale 1.0" in the
  table means no calibration freedom was used on that split.
- Registered pass rules, where a protocol uses the gate, are written in that
  protocol (for example model ≤ baseline + 0.05 and median-in-interval on at
  least 60% of splits in PREREG_2026-07-16 P6, with 20 reps and 2,000
  bootstrap draws).

Its known limits are documented in `evidence-standards.md` §4 so that they
are reported, not so that the gate is quietly replaced.

## 5. Known gaps between the stated model and the frozen simulators

These are facts about the code as of 2026-10-11 (MODEL_STATUS §2z-ai). They
are open questions for the owner, not licence to change a frozen stack.

- The main law says one amplitude scales permanent and transitory volatility
  alike (b = 1). In `simulate` and in both cohort simulators v_i scales
  σ_perm only when `--mix-hetero` is on. The FB and short-submissions
  movement stacks do not list that flag.
- Homes are seeded from the sorted observed values of period 0 (`w0`), so
  unconditional head statistics inherit that week's idiosyncrasy. Prefer
  conditional simulations or rolling variants for head and identity claims.
- Fast innovations are unit-variance Student-t with no bound; head shares
  are computed on `expm1(X)`.
- The card pools rank ≤ 200 for dRank while the gate caps at 100. Harmonize
  or rename before submission (§2z).

## 6. What a new ranked system is expected to show

The claim under test on a new system is amplitude collapse (s(h) flat in h,
b ≈ 1) together with a movement gate at par or better. It is not 15/15: a
10–12/15 card with a clean gate is a confirming result.

What the record says about transport, to be used when writing bands:

- Held across panels and into unseen data: b ≈ 1, the horizon-scaling
  exponent, the shape of the Spec-B noise floor, and the frozen-parameter
  movement gate (§2z-q, §2z-ae).
- Did not hold: the level of s (it differs by metric and drifts by era:
  0.65 → 0.75 → 0.84 on comments, non-monotone on submissions) and the level
  and head/mid ordering of κ (§2z-q, §2z-s, §2z-ae). Bands on s must be
  trend-aware, and a κ comparison must use matched moments and say how well
  κ is identified at that panel length.
