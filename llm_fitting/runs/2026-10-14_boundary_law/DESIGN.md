# The boundary theory by rank: design and predictions (before any run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-14. Committed with the
runner `llm_fitting/mts_boundary_law.py` and planted-world tests
`tests/test_mts_boundary_law.py` before any panel is read._

## Why this is the test that matters

The same-year rate law of §2z-cd is partly accounting: the centre's change
cancels, so the law's fit is the fit of two approximations (§2z-cg). Exactly,
for the persisting set P and T_a, the top k of P in block a:

    U_k(b) − U_k(a) = G_k + R_k

- G_k is the mean change of T_a's members.
- R_k ≥ 0 is the reordering gain, U_k(b) minus the mean of T_a's members at b.

With E_k = −(G_k − ΔU_K), the slide of the current top k against U_K, and
L_k, the slide of the cohort fixed by permanent rank (the law's s̄_k × D):

    observed − predicted = (L_k − E_k)/D + (R_k/D − c_k)

The first piece compares the slide the law uses with the slide of whoever is
on top. The second is the **boundary theory**, that crossings at rank k
return σ_k²/(2S_k) per member per week.

The boundary theory is the law's only physics. In SPT terms it predicts the
local-time term (Fernholz 2017; notes in
`lit_review/research_notes/…/rank_dynamics_model_families.md`) from lasting
volatility and crowding instead of measuring it. That separates this
program's law from an SPT identity. It has been checked only at k = K
(§2z-by item 5, §2z-cc). Here it is checked at every rank.

## Estimators

Each comparison from one start carries one-time constants of opposite sign:

- *Passing swings.* Whoever is on top through a swing falls back, so E_k
  rises and R_k rises with it.
- *Block averaging.* Selection on a block mean picks members that were rising
  through the block. That momentum lowers E_k and lowers R_k.

A difference over two spacings from the same start, D₁ = 52 and D₂ = 104
weeks, removes both:

    R̃_k = (R_k(D₂) − R_k(D₁)) / (D₂ − D₁)

Ẽ_k and L̃_k are formed likewise. The lasting forms are then compared:
- R̃_k against c_k = σ_k²/(2S_k), with σ_k² from the variance route (cohort
  of block 0, blocks 1, 3, 5) and S_k the slope over the interval measured;
- Ẽ_k against L̃_k.

Set-up:
- 26-week blocks, eight per panel; starts a = 1, 2, 3, each with its cohort
  fixed on block a − 1; the spacings 104 → 156 and 52 → 156 from start 1 are
  reported, not pooled.
- Endpoint bootstrap, 200 draws per start. The draws are joint across spacings
  but independent across starts and of the variance draws.
- Panels: `reddit_comments_ext` (K = 12,500) and `reddit_submissions_long`
  (K = 5,000), DEVELOPMENT.

## Planted-world checks (pass)

Exponential ladders, N = 25,600, K = 1,600, S = 1.5, σ² = 0.02 a week:
- Over three seeds R̃/(σ²/2S) averages 0.97–1.16 at every k. The spread is
  about 5% at k ≥ 400 and 20% at k ≤ 100.
- Ẽ − L̃ is within 0.001 of zero.
- At one spacing the current top's slide is lower than the cohort's by about
  0.001 a week (momentum) without swings, and about equal with them.
- With the top K/8 pressed down, crossings below the press follow the theory
  and the head sinks.

The variance route is noisy in strata of about 100 members (seed spread up to
±40%). On the panels the boundary strata have at least 195 (comments) and 78
(submissions) members, so c at K/64 on submissions is the least certain.

## Predictions

| | prediction | passes if |
|---|---|---|
| B1 | crossings follow σ²/(2S) by rank | R̃_k / c_k in [0.75, 1.33] at ≥ 5 of the 7 cut-offs K/64 … K, on each panel |
| B2 | once one-time constants are removed, the slide of whoever is on top equals the cohort's | at ≥ 5 of 7 cut-offs on each panel, the 5–95% interval of Ẽ_k − L̃_k contains 0 or \|Ẽ_k − L̃_k\| < 0.0005 a week |
| B3 | the law in its lasting form closes | pooled over both panels' cut-offs below K (12 cells): slope of observed on predicted in [0.8, 1.25], and its rms error below the one-spacing form's |
| B4 | at one spacing the two pieces of the error offset, as block averaging requires | (L₁ − E₁) and (R₁ − c₁) have opposite signs at ≥ 4 of 6 cut-offs below K on each panel |

What each outcome means:
- *B1 pass.* Crossings at every rank are predicted from volatility and
  crowding: the law's non-accounting part holds.
- *B1 fail at the head only.* The small-step boundary breaks where gaps are
  wide.
- *B1 fail in the body.* The law's boundary is wrong, and the §2z-cd fit owed
  its quality to accounting.
- *B2 fail.* The lasting slide of whoever holds a rank differs from the slide
  of a cohort, for reasons other than passing swings and momentum, such as
  rank-dependent drift that the cohort averages over differently.
- *B3 pass with B1 and B2.* The over-prediction of §2z-cd and §2z-cg is a
  one-time artefact of comparing adjacent blocks.

Not scored, reported:
- E₁ − L₁ on each panel: which one-time bias dominates on real data.
- The by-start values: whether crossings by rank are stable over 2020–2022.

## Not in this design

Facebook. Any change to the law of §2z-cd. Sealed data. Ranks above K/64.
