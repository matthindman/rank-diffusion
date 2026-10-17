# Questions reviewers are likely to ask, and where the answers stand

_DRAFT for the owner, Claude Opus 5.5 (Anthropic), 2026-10-15. Changes no
claim language. Numbers are from MODEL_STATUS, cited by section. "Open" marks
what the record cannot yet answer. Companion documents:
`BALANCE_LAW_THEORY_NOTE_2026-10-14.md` (derivation, positioning, proposed
argument) and `runs/2026-10-15_figures/` (figures)._

_**Corrected 2026-10-18** after three external reviews
(`runs/2026-10-18_external_critiques/SYNTHESIS.md`, MODEL_STATUS §2z-da).
Corrected passages are marked [c]._

## The claim in one paragraph

Attention ladders change form slowly while the names on them churn [c]. A fifth of
each top set is replaced every year, and the typical community's rank moves
20–25% a year (§2z-cu). The form is held by a balance at every rank: crossings
at the boundary of a top set return what its members lose against the pack.
The crossings are *predicted* from two measured quantities, how much names at
that rank move (lasting variance) and how crowded the ladder is there (local
slope), and the prediction holds rank by rank:
- on Reddit comments, K/64 … K, with two estimators, and over time;
- on Facebook pages, K/16 … K (0.71 at K/64);
- on Instagram accounts, with the "a"-query censoring modelled, K/16 … K
  (displacement variance at 14–28 weeks) [c];
- on Reddit submissions in the body (§2z-cm, §2z-co, §2z-ct, §2z-cx).

On comments crossings return a steady amount each half-year, so changes in
the ladder's shape track changes in members' drift by rank (§2z-cr). On
submissions the reordering term varies threefold [c].

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
- [c, added 2026-10-18] **Gomez (2023, *Econometrica*)** splits the growth of
  top wealth shares into within, between (rank mobility) and demography terms,
  with closed forms for random-growth models, estimated on the Forbes 400. It
  is the balance law's closest precedent in economics. Whether its between
  term is tested against an independently measured variance, as ours is, must
  be checked in the full text before any novelty sentence is written. Also:
  - Ichiba et al. 2011 (hybrid Atlas models: drifts by name and by rank,
    which is the frame for A against B);
  - Kolli & Shkolnikov 2018 (finite-N fluctuations: a null for R̃);
  - Batty 2006 (rank clocks: stable rank-size over churning membership,
    already documented);
  - Duranton 2007;
  - Jones & Kim 2018;
  - Stanley et al. 1996 and Riccaboni et al. 2008 (size–variance scaling);
  - Guvenen et al. 2021 (kurtosis of earnings changes: the two-speed fact).

**2. Is "crossings = σ²/2S" not true by construction?**
- [c] *Not by construction, but close to an identity for any model in which
  levels diffuse.* On a locally exponential ladder with increments that do not
  depend on position, R̃ = S·κ(1/S) = σ²/(2S)·[1 + κ₃/(3σ²S) + …]. The
  ratio leaves 1 only through position-dependent increments, cumulants at the
  scale S, a mismatch between the cohort and the boundary's occupants, or a
  misread density. Agreement therefore says that lasting movement is level
  diffusion at the measured variance. It cannot pick between the level models
  anyone would fit (Opus 5.5 and Fable 5.1 reviews).
- The two sides are measured from different things:
  - σ² is the lasting variance of a cohort fixed earlier, by permanent rank;
  - S is the ladder's local slope;
  - the crossings are reordering gains of the top set, read off the ladder at
    two spacings.
- *It can fail, and does in planted worlds.*
  - Fixed volatility types still sorting toward the head give 1.2–1.3 in the
    body and 2–4 at the head (§2z-cq).
  - Rank-only dynamics fail badly: random displacement in rank gives 2.4–3.6,
    and neighbour swaps 0.3–1.7 (§2z-cv).
  - So agreement on the data says that lasting movement behaves like
    diffusion of levels, not like exchanges of rank.
- [c] On submissions the head's ratio is 1.6–1.9, where strata of 30–150
  names make the variance estimate unreliable (§2z-cm). This is a limit of
  measurement, not evidence for the theory.
- *What it rests on.* That lasting movement near each boundary is
  diffusion-like, and that the cohort's variance is the boundary occupants'
  variance. Heavy tails, rare jumps and heterogeneous drifts do not break it
  (§2z-cq).

**3. Why does it matter for computational social science?**
- It turns "stable inequality amid churn" from a description into a
  measurable balance. Each term is a behaviour of named communities, so a
  platform change can be read as a change in drift or in mobility by rank.
- *June 2021 on Reddit submissions.* [c] The drift of the very top fell from
  23–24 June, days before the announced date of 28 June (§2z-ca). Crossings at
  the head rose from 2.35 (0.80–3.79) to 7.82 (3.75–13.69) per 1,000 weeks.
  That rise is unregistered, rests on strata of 31–78 communities, and its
  intervals touch. The archive's capture regime changes on 1 July 2021. Placebo
  years and control ladders: §2z-da.
- *Counterfactuals.*
  - What concentration current movement sustains. The top 78 submissions
    communities held 51% of the top 5,000's activity in 2019 and 35% in 2022;
    their own movement sustains 15–16%. On comments it is 63%, then 52%,
    sustaining 53% (§2z-cw; an index of geometric-mean activity).
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

**4a. Instagram's panels hold only posts whose caption contains "a".**
- *The censoring is modelled* in three measured layers (§2z-c):
  - thinning noise of about (1 + c²)/M;
  - instrument dropout;
  - low-q ghost accounts.
- *Why a persistent share cannot create the result.* A persistent "a"-match
  share q_i is a fixed offset in log activity. It cancels in changes and leaves
  the exponential slope unchanged. [c] A *drifting* q is not excluded. q is
  chiefly language and caption style, and 2023 brought shorter captions with
  Reels, so drift in q may correlate with engagement change.
- *Planted validation.* Worlds with the measured layers give the same ratio
  as the truth (1.015 against 1.014); the default rules cannot read them at
  all (§2z-cx).
- [c, WITHDRAWN 2026-10-18] The claim that the matching-post count *bounds*
  what q drift could touch is withdrawn. log Y = log M + log Ȳ, so the
  variance of the total includes 2·Cov(log M, log Ȳ), and drift in q can enter
  both parts (gpt-6-astra review). The split, with the count at 23–41% of
  movement and per-post engagement at 60–70%, stays as a description.
- *Result.* On Instagram totals the crossings are 0.94–1.12 of prediction at
  K/16 … K. The per-post panel (free of q only if q is persistent) is too noisy to confirm (3 of
  5); the raw panel cannot be read (§2z-cx).
- *Scope:* "a-matching engagement" of the reliably measured accounts. No
  level-ladder claim.

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
- *K:* at a given rank the ratio does not depend on K (§2z-cu). [c] This is an
  algebraic property of the estimator, because crossings and strata at rank k
  do not involve K. It is not a robustness result.
- *Block length:* with 13-week blocks over eleven starts, comments has 5 of 7
  ranks in the band. [c] Two values fall outside it, 0.72 at K/64 and 1.41 at
  K/16; §2z-cu's prose said one.
- *Open:* submissions crosses only 0.61–0.72 of prediction at ranks of about
  300–600, by every estimator (§2z-cu). Neighbours moving together is ruled
  out (§2z-cv). The shortfall survives on one survivor population; drift by
  current rank biases the statistic down by this much in planted worlds
  (§2z-da). Not established for the data.
- *Open:* the head of submissions, where strata of 30–150 communities limit
  the variance route to about ±40% (§2z-cm) [c: was ±30%].

**8. Entry, exit, bans, platform growth.**
- Entry and exit are exact terms. Entrants are mostly communities new to the
  record, and exits are almost never disappearances (§2z-bv, §2z-cc).
- Platform growth does not track drift by rank (§2z-ch).
- Bans and quarantines are exits like any other. *Open:* whether named
  moderation waves matter. This would need names, the owner's decision.

**3a. Platforms say there is little lock-in at the top. What can the model say before the fact?**
- *The forecast.* For each member of today's top k, the probability it is
  still there after h, from measured movement alone: no retention outcomes
  (§2z-cy).
- *Who stays.* On Reddit comments and submissions, Facebook pages and
  Instagram, it ranks members by their chance of staying 13–21% better than
  constant turnover (Brier). [c] A three-parameter logistic on log rank and
  log horizon, fitted to earlier outcomes, does slightly better on all four
  ladders: Brier 0.1244 against 0.1252, 0.1309/0.1357, 0.1587/0.1607 and
  0.1615/0.1621. Its aggregate error is about half the model's. Rank alone
  carries the discrimination.
- *Calibration.* Within ±0.07 in 75% of cells.
- *The direction.* Incumbents are stickier than the band-average forecast
  says, by 2–5 points within a year and more later.
- *Why.* Movement is two-speed: most incumbents drift slowly and a few move
  a lot. With the empirical shape of past changes, still outcome-free, the
  under-forecast halves. Its Brier then beats the logistic on three ladders
  and loses on submissions, while its aggregate error stays above the
  logistic's (post hoc, §2z-cz, §2z-da; to be registered) [c].
- *The exception.* The very top of Reddit lost members faster than forecast
  around the June 2021 change.
- [c] *What the model adds over rank.* A forecast under *changed* mobility,
  which a logistic fitted to the past cannot give. Untested. The under-forecast
  says that Gaussian diffusion understates staying. It is not by itself
  evidence of cumulative advantage. No platform statement has been sourced or
  tested, so the paper should not frame this as an answer to one.

## Inference

**9. Uncertainty, multiplicity, and what was registered.**
- [c] Registered tests had their design, predictions and planted-world checks
  committed before the run. The development panels had been read many times
  before that (§2z-cd: "not blind"), and C2, C8, C9, the by-start r = 0.94,
  the June 2021 mobility contrast and the lock-in repair are post hoc.
  Tallies:
  - 15 of 26 scored items held since §2z-cm (evidence packet);
  - 13 of 24 without the two "not readable" predictions;
  - the earlier "9 of 17" counted only §2z-cm … §2z-cu.
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
3. **One law, four ladders** (`fig_four_ladders.png`): measured crossings
   against σ²/2S rank by rank on Reddit comments and submissions, Facebook
   pages and Instagram accounts, and the ratio by rank.
4. **Structure from movement** (`fig_sustained_ladder.png`, with
   `fig_why_it_curves.png`): the ladder each year's movement sustains,
   against the observed ladder, and why it steepens down the ranks.
5. **Lock-in, forecast ex ante** (`fig_lockin.png`): retention of the top
   sets by horizon on four ladders, forecast against observed and constant
   turnover, with calibration.
6. **Where concentration is going** (`fig_concentration.png`): the head's
   share by year and the share its own movement sustains.
7. **An intervention** (`fig_rate_law_and_head_mobility.png`): the same-year
   law over 60 cells, and the head's crossings around June 2021.

**SI:**
- the boundary theory by start and by estimator (`fig_boundary_theory.png`);
- Instagram's censoring, validated and bounded (`fig_instagram_censoring.png`, §2z-cx);
- planted-world checks (§2z-cm, §2z-cq);
- robustness to K and block length (§2z-cu);
- the detection study (§2z-ci);
- entry accounting (§2z-cc).
