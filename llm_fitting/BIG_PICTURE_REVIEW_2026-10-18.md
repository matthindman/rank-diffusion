# Where the PNAS article stands: a big-picture review

_Claude Opus 5.5 (Anthropic), 2026-10-18, at the owner's request ("review
everything we have … where we are big picture-wise with this PNAS
article"). Planning only; it changes no claim, default, protocol or record
entry. Numbers are quoted from `MODEL_STATUS.md` with their section._

## 1. The single most important fact: there are two papers in this repository

**Paper A, the owner-adopted binding claim set** (§6 addendum, 2026-07-12;
outline `paper/pnas_outline.tex`, "Eulerian stability with Lagrangian churn
in digital attention rankings"). A permanent–transitory rank model:
- a shared rank-conditional process times one persistent amplitude per name,
  with measurement noise identified from daily replication;
- validated out of sample: relative moment error 0.032 against 0.171 for
  historical mobility, transported across eras without recalibration;
- MIXED on parameters (s and κ fail transport). It "generated an excessively
  concentrated stationary head", proper scores at par with persistence, upper
  movement tails under-dispersed.

The outline's results are R1 the fact, R2 the model, R3 identification, R4 one
amplitude and R5 validation. Its text is mostly `\todo`.

**Paper B, the movement-to-structure work** (agenda adopted 2026-10-13,
§2z-bp; results §2z-bq … §2z-cz; draft frame
`PAPER_FRAME_DRAFT_2026-10-15.md`). A balance at every rank, d shape_k/dt =
σ_k²/(2S_k) − s̄_k (+ entry). Its one non-accounting claim, crossings at a
boundary = σ²/(2S), predicted and measured separately:
- holds rank by rank on Reddit comments (0.85–1.22 by two estimators),
  Facebook pages (0.77–1.17) and Instagram totals (0.94–1.12). _(Corrected
  after the reviews: the Facebook and Instagram ranges are for K/16 … K; at
  K/64 the values are 0.71 and 2.49. The Reddit pooled ratios are not
  time-aligned; §2z-da.)_
- holds partly on Reddit submissions (body 0.7–1.0; head unmeasurable;
  shortfall 0.61–0.72 at ranks 300–600);
- crossings are a steady restoring force, so shape moves only with drift by
  rank (§2z-cr);
- ex ante lock-in forecasts discriminate who stays (Brier 13–21% below
  constant turnover) but under-forecast staying (§2z-cy, §2z-cz). _(Corrected:
  a logistic on rank does slightly better on all four ladders; §2z-da.)_

**They are compatible, and the paper is stronger as one.** A is a model of
*individual movement* (lasting plus passing parts, one amplitude); B is the
*law by which lasting movement makes structure*. The integration points:
- The lasting/passing decomposition is central to both. B's spacing
  differences and variance route are the structural counterpart of A's
  permanent–transitory model.
- **B bears on A's recorded failure, but does not explain it.**
  - A's simulator produces an "excessively concentrated stationary head":
    the top share is about 2.7–2.9× the observed on Facebook and Instagram
    (§2z-d).
  - B finds the real head out of balance. On submissions the movement
    sustains a head slope of 0.3–0.5 against 0.9 observed, relaxing over
    years (§2z-cd, §2z-cp, §2z-cw). So a stationary head law is a
    questionable target.
  - The directions differ: A's steady state is more concentrated than
    observed, B's less.
  - Reconciling them needs B's decomposition (σ² and slides by rank) run on
    A's simulated panels, to find which term over-concentrates its head.
    Not done. _(Corrected 2026-10-18 shortly after writing; an earlier
    sentence said B explains A's failure.)_
- A hypothesis, not a finding: A's "upper movement tails under-dispersed"
  and B's under-forecast of staying may both reflect two-speed movement,
  with most names drifting slowly and a few making block-scale jumps
  (§2z-cz).
  - They concern different distributions, conditioning and horizons.
  - §2z-cz shows block jumps are sufficient among the planted rivals tried,
    not that they are the cause.
  - _(Corrected 2026-10-18 after the gpt-6-astra review; it said "the same
    fact seen twice".)_

**Decision for the owner:** one integrated paper (recommended) or two. The
binding claim paragraph would need an amendment; that is the owner's.

## 2. Data

| ladder | panel | span | universe | status |
|---|---|---|---|---|
| Reddit comments | `reddit_comments_ext` | Dec 2018 – Dec 2022, 213 weeks | K = 12,500 | DEVELOPMENT |
| Reddit submissions | `reddit_submissions_long` | Dec 2018 – Dec 2022, 212 weeks | K = 5,000 | DEVELOPMENT |
| Facebook pages | `facebook_a`, Era A | 86 weeks | K = 3,500 | DEVELOPMENT |
| Instagram accounts | `instagram_hm`, through the §2z-c censoring apparatus | 2023, weeks 9–52 | K = 10,000 | DEVELOPMENT |
| Reddit bridge | | 2023-01 … 2024-06 | | SEALED |
| Wikipedia | T9 | | | SEALED |

- Sealed data are untouched. The owner's word is needed to open either.
- No community, page or account name has been read in the B analyses.
- Karma is approval-weighted activity, not an event count. CrowdTangle
  (Facebook, Instagram) is discontinued, which matters for a data-availability
  statement.
- Data limits that bound what can be claimed:
  - Facebook has one start and Instagram one year.
  - The submissions head has strata of 30–150 names, so its lasting variance
    is about ±40% by the variance route (§2z-cm). _(Corrected from ±30%.)_
  - The Reddit window holds dated platform changes, so no era is stationary.

## 3. Models and estimators

- **A's model:** `minimal_rankdiff.py` and `rankdiff_kalman.py`. These are
  the frozen stacks, with the movement gate as the adoption criterion.
- **B's machinery:**
  - exact accounting (`balance_accounting.py`);
  - the rate law (`mts_balance_law.py`);
  - the boundary test with spacing differences (`mts_boundary_law.py`), with
    opt-in options for instrument dropout;
  - the variogram (`mts_variogram.py`);
  - forecasting attempts: `mts_forward.py`, `mts_seasonal.py`,
    `mts_intervention.py`;
  - the lock-in forecaster (`mts_lockin.py`);
  - the Instagram censoring simulator (`ig_censoring_sim.py`).
- **Testing.** Every registered analysis has planted-world tests, and the
  suite has about 565 tests. _(Corrected after the reviews.)_ Every
  registered test had its design and predictions committed before the run.
  The development panels had been read many times before, and several
  headline results are post hoc (§2z-da).

## 4. Strength of evidence, as I would rate it for a referee

**Strong:**
- the exact decomposition (C2);
- the boundary theory on comments, across ranks, estimators and starts
  (C3);
- its replication on Facebook pages and Instagram totals;
- the method result (C5): current-rank and cohort slides agree once one-time
  constants are removed;
- planted rivals showing the test is not automatic (C4).

**Moderate:**
- submissions;
- the sustained-ladder counterfactual and relaxation times (C7–C9), which rest
  on slides measured in one year;
- the June 2021 change acting through drift and mobility (C10), which is dated
  but has no control;
- lock-in discrimination (C11).

**Weak or failed:**
- forecasting structure better than trend (C13);
- lock-in calibration at long horizons;
- seasonal and intervention forecasts;
- the per-post Instagram panel.

**Untested:**
- anything out of sample in the strict sense;
- a second platform with a multi-year span;
- the cause of drift by rank, by owner's framing.

## 5. The referee reports I expect

1. **"This is stochastic portfolio theory applied to Reddit."** Answer: the
   maths is, and is cited. The tests are new: the out-of-equilibrium
   predicted crossing term, named-cohort debiasing and lock-in forecasts
   (§2z-cn). The risk is real; the framing must lead with what is learned.
2. **"The balance is an identity."** The same-year law is partly accounting
   (§2z-cg), and only the boundary term is a prediction. The paper must make
   that line bright.
3. **"So what for social science?"** The paper needs a sharper substantive
   payoff:
   - lock-in and incumbency;
   - how platform design moves inequality;
   - how far concentration has to fall (§2z-cw);
   - how long that takes (§2z-cp).
4. **"Generality."** Four ladders across three platforms, but Facebook and
   Instagram are short, and the strongest case rests on Reddit comments.
5. **"Causality of June 2021."** No control group. Comments moved the other
   way in the same half-year (§2z-cr).
6. **"Measurement."**
   - Karma is not views.
   - Instagram is "a"-filtered.
   - The universes are truncated top-K. Choosing K by coverage is defended,
     but the upper tail of the population is what the claims are about.
7. **"Selection and survivorship."** Addressed by permanent-rank cohorts and
   presence rules. The entry and exit terms must be shown.
8. **"Forecasting."** The law does not beat trend. Reviewers who want
   prediction will be disappointed unless the framing is explanation plus
   counterfactual plus ex ante lock-in.
9. **"Two models, one paper?"** Section 1 above.
10. **"Out of sample."** A sealed, outcome-blind test is in hand and
    proposed. Running it, owner-gated, would be the most persuasive single
    addition.

## 6. What I think is most important that we are under-using

- **The integration of A and B** (section 1), through an executable bridge:
  B's diagnostics run on A's simulated panels to localise A's structural
  error. Not done.
  - §2z-av already attributes A's Reddit head error largely to
    initialisation.
  - Its Facebook and Instagram excess is largely attributed to passing-shock
    spike weeks.
  - _(Corrected 2026-10-18; an earlier sentence said B explains A's
    failure.)_
- **The ex ante lock-in result as the policy-relevant headline.** _(Corrected
  after the reviews; not a headline.)_ T1 and T2 passed and T3 failed. A
  logistic on rank discriminates slightly better, and the aggregate forecast
  is biased low on every ladder. The under-forecast stays a finding about the
  shape of movement, with rare large jumps (§2z-cz, §2z-da).
- **Two-speed movement.** Rare block-scale jumps plus slow drift appear in A
  (under-dispersed tails), in B (lock-in under-forecast) and in the
  Instagram decomposition. A jump-diffusion view could unify them. Not
  built.
- **The sealed tests**, owner-gated: the Reddit bridge for the boundary
  theory and lock-in; Wikipedia as a fifth ladder with a long span.

## 7. Open questions on the owner's desk

- one paper or two;
- the binding-claim amendment;
- opening the sealed data and adopting the revised 2023 protocol (§2z-cp);
- reading community names, for the event analyses;
- claim language in the draft frame;
- Codex's reviews R5 and R6 and its planted worlds V5, all outstanding.

## 8. After the external reviews (added 2026-10-18)

Three independent critiques: gpt-6-astra, Fable 5.1 and a fresh Opus 5.5.
They are summarised and checked in
`runs/2026-10-18_external_critiques/SYNTHESIS.md`, with the record in §2z-da.
The corrections above are marked. What they changed in this review's
judgement:
- C3 is a near-identity for level diffusion. It establishes that movement
  happens in level space at the measured variance; it is not a law of the
  dynamics.
- Lock-in is complementary evidence, not a headline.
- Concentration is an index unless stated as an arithmetic share.
- "Stable form" is retired.
- Gomez (2023) and Batty (2006) are precedents that a novelty statement must
  face.
- Capture-delay deletion is an open alternative explanation for drift by rank
  after July 2021.
