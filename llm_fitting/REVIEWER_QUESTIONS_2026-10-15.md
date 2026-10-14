# Questions reviewers are likely to ask, and where the answers stand

_DRAFT for the owner, Claude Opus 5.5 (Anthropic), 2026-10-15. Changes no
claim language. Numbers are from MODEL_STATUS, cited by section. "Open" marks
what the record cannot yet answer. Companion documents:
`BALANCE_LAW_THEORY_NOTE_2026-10-14.md` (derivation, positioning, proposed
argument) and `runs/2026-10-15_figures/` (figures)._

## The claim in one paragraph

Attention ladders keep their form while the names on them churn. A fifth of
each top set is replaced every year, and the typical community's rank moves
20–25% a year (§2z-cu). The form is held by a balance at every rank: crossings
at the boundary of a top set return what its members lose against the pack.
The crossings are *predicted* from two measured quantities, how much names at
that rank move (lasting variance) and how crowded the ladder is there (local
slope), and the prediction holds rank by rank:
- on Reddit comments, at every rank, with two estimators, and over time;
- on Facebook pages;
- on Reddit submissions in the body (§2z-cm, §2z-co, §2z-ct).

Crossings return a steady amount each half-year. The ladder changes only when
members' drift by rank changes, as after Reddit's June 2021 Home-feed change
(§2z-cr, §2z-ca).

## Novelty and framing

**1. Isn't this stochastic portfolio theory, or Gabaix's random growth?**
- *Partly, and the paper should say so.* The law follows from the forward
  equation for ordered levels. Its stationary form is Fernholz's gap identity,
  and its one-slide form is the Atlas or Gabaix slope σ²/2g (§2z-cn).
- *What is new, as far as a targeted search found:*
  - a test out of equilibrium in which the crossing term is predicted from
    measured lasting volatility and slope, not measured from rank switching;
  - named-cohort estimation that removes the drift bias of ranking by current
    position (SPT calibrations condition on current rank);
  - the first application to online attention and a dated algorithm change.
- *Cite and position against:* Fernholz 2017; Fernholz & Koch; Itkin &
  Larsson 2024; Baker & Smyth 2026; Gabaix, Lasry, Lions & Moll 2016; Blumm et
  al. 2012; Iñiguez et al. 2022.

**2. Is "crossings = σ²/2S" not true by construction?**
- *No.* The two sides are measured from different things:
  - σ² is the lasting variance of a cohort fixed earlier, by permanent rank;
  - S is the ladder's local slope;
  - the crossings are reordering gains of the top set, read off the ladder at
    two spacings.
- *It can fail, and does in planted worlds.* Fixed volatility types still
  sorting toward the head give 1.2–1.3 in the body and 2–4 at the head
  (§2z-cq).
- *It also fails on real data where it should.* On submissions the head's
  ratio is 1.6–1.9, where the variance estimate is noise (§2z-cm).
- *What it rests on.* That lasting movement near each boundary is
  diffusion-like, and that the cohort's variance is the boundary occupants'
  variance. Heavy tails, rare jumps and heterogeneous drifts do not break it
  (§2z-cq).

**3. Why does it matter for computational social science?**
- It turns "stable inequality amid churn" from a description into a
  measurable balance. Each term is a behaviour of named communities, so a
  platform change can be read as a change in drift or in mobility by rank.
- *June 2021 on Reddit submissions.* The drift of the very top fell within
  days (§2z-ca), and crossings at the head roughly tripled over the next year
  (§2z-cm, §2z-co).
- *Counterfactuals.*
  - What slope current movement sustains: the submissions head, 0.3–0.5
    against 0.9 observed.
  - How long a ladder takes to get there: 2–6 years on submissions, 5–22 on
    comments (§2z-cp).
  - That falling entry steepens ladders, which matters as platforms mature
    (§2z-ck).
  - That a newcomer's climb to the head takes decades at measured rates
    (§2z-ck).

## Measurement

**4. Karma is a sum of scores, not a count of attention.**
- Yes; it is approval-weighted activity.
- The archive's score capture is checked by day: a guard and a flag list,
  opt-in, with one damaged week found (§2z-ca, §2z-cb). Results do not move
  with flagged weeks left out (§2z-cc).
- Facebook pages use interactions, a different metric, and the law holds
  there (§2z-ct).

**5. Regression to the mean and selection on current rank.**
- This is the program's central methodological point (§2z-a onwards).
- Drifts and variances are measured on cohorts fixed by permanent rank in an
  earlier window, with absence-penalised membership.
- Comparisons from one start carry two one-time biases of opposite sign:
  whoever is on top through a passing swing falls back, and ranking by block
  averages builds in momentum, about σ²L/(6S·D).
- A difference of two spacings removes both. Then the current top and the
  cohort slide at the same lasting rate on all three ladders (§2z-cm, §2z-ct).

**6. Passing swings, seasonality, sampling interval.**
- Block means and the variance route remove passing swings. Lags a year
  apart remove the annual cycle (§2z-co).
- A second estimator, a variogram, agrees on comments (§2z-co).
- With 13-week blocks, swings inflate the absolute rates but not the tested
  ratio (planted, §2z-ct).

**7. Dependence on K, block length and estimator.**
- *Estimator:* two estimators bracket comments' ratios, 0.85–1.22 (§2z-co).
- *K:* at a given rank the ratio does not depend on K (§2z-cu).
- *Block length:* comments holds with 13-week blocks over eleven starts
  (§2z-cu).
- *Open:* submissions crosses only 0.61–0.72 of prediction at ranks of about
  300–600, by every estimator. Candidates: neighbours moving together, or the
  June 2021 step of that band (§2z-cu).
- *Open:* the head of submissions, where strata of 30–150 communities limit
  any estimator to about ±30% (§2z-co).

**8. Entry, exit, bans, platform growth.**
- Entry and exit are exact terms. Entrants are mostly communities new to the
  record, and exits are almost never disappearances (§2z-bv, §2z-cc).
- Platform growth does not track drift by rank (§2z-ch).
- Bans and quarantines are exits like any other. *Open:* whether named
  moderation waves matter. This would need names, the owner's decision.

## Inference

**9. Uncertainty, multiplicity, and what was registered.**
- Every test had its design, predictions and planted-world checks committed
  before the data were read. Failures are reported: of the predictions scored
  since §2z-cm, 9 of 17 held, and one was partly not adjudicated (§2z-cu).
- Endpoint bootstraps treat communities as independent. Cross-community
  dependence is not modelled, and the detection study notes it (§2z-ci).
- *Open:* Codex's review of the estimators (row R5).

**10. Does the law forecast?**
- *Not better than trend on this record.* On submissions the law forecasts
  as well as repeating the ladder's own last change, half the error of no
  change. On comments nothing beats no change, because its head reversed once
  (§2z-co, §2z-cs).
- *Also not after June 2021.* A quarter of movement did not beat the trend,
  because the change sped up an existing trend rather than reversing it
  (§2z-cs).
- *So the claim is explanation and counterfactual,* not prediction. An
  outcome-blind test is proposed with the boundary theory as its primary test
  (§2z-cp; sealed 2023–24 Reddit data, owner-gated).

**11. Out of sample.**
- *Open,* by design: the sealed Reddit bridge and the Wikipedia panels are
  untouched.
- A revised protocol is drafted; the sealed data would be the first
  confirmatory test (§2z-cf, §2z-cp).

## Mechanism

**12. What sets drift by rank? Is the June 2021 effect causal?**
- *By the owner's framing, the paper is agnostic about what sets drift
  (§2z-cj).* It measures drift and shows how structure follows from it.
- The June 2021 change is dated by Reddit's own announcement. Its onset is
  dated by day, before the archive's file boundary (§2z-bz, §2z-ca).
- The paper can say the change acted through the top's drift and mobility.
  It cannot separate that change from other events of mid-2021 without a
  control. Comments moved the opposite way in the same half-year: a broad gain
  of the head, not a few communities (§2z-cr). *Open:* why.

**13. Is volatility a trait of a community or a response to its size?**
- Variance falls with size on all three ladders. Codex's finite-age theorem
  says fixed types cannot produce that under conditions that a mature
  platform with falling entry violates (§2z-cj, §2z-ck).
- The boundary ratio is a new handle on it. Fixed types still sorting
  toward the head give ratios of 2–4 at the head; comments shows 1.0–1.2
  (§2z-cq).
- *Open:* worlds calibrated to the data (Codex V5); an external count-based
  test lacks power at our gradient (§2z-cl).

## Reproducibility

**14. Code, tests, data.**
- Every registered analysis is a module with planted-world tests in the
  repository's suite, and a run folder with its design, outputs and report.
  The post hoc analyses are scripts in their run folders.
- The Reddit data derive from public archives (Pushshift). The Facebook data
  are from CrowdTangle, now discontinued, which needs a data-availability
  statement.
- The analyses read no community names.

## Proposed figures

1. **The puzzle** (`fig_puzzle.png`): the ladders by year on three ladders,
   120 community paths, and turnover of the top sets.
2. **The balance** (`fig_shape_decomposition.png`): each half-year's change of
   shape split exactly into members' movement and steady crossings.
3. **One law, three ladders** (`fig_three_ladders.png`): measured crossings
   against σ²/2S rank by rank, and the ratio by rank.
4. **Structure from movement** (`fig_sustained_ladder.png`): the ladder each
   year's movement sustains, against the observed ladder.
5. **An intervention** (`fig_rate_law_and_head_mobility.png`): the same-year
   law over 60 cells, and the head's crossings around June 2021.

**SI:**
- the boundary theory by start and by estimator (`fig_boundary_theory.png`);
- planted-world checks (§2z-cm, §2z-cq);
- robustness to K and block length (§2z-cu);
- the detection study (§2z-ci);
- entry accounting (§2z-cc).
