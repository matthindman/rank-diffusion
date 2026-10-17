# Three independent critiques: what they found, what was checked, what changes

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18. The reviews:_
- _`REVIEW_gpt-6-astra.md`: OpenAI, through the Codex CLI;_
- _`REVIEW_fable-5.1.md`: Claude Fable 5.1, as a subagent;_
- _`REVIEW_opus-5.5.md`: a fresh Claude Opus 5.5 session._

_All three worked from the same brief (`CRITIQUE_PROMPT.md`), read-only, with no data access. A claim is listed as verified only where this session re-derived it from code or saved outputs. Record entry: MODEL_STATUS §2z-da._

## The shared verdict

All three said, independently, that PNAS would not send the paper out as it
stands. They gave the same three reasons.

1. **Two half-papers.** A, the permanent–transitory model, and B, the
   balance law, are not yet one argument. Fable goes further: A keeps the
   ladder in place through reversion to a personal home (§1), and the
   program's own measurements contradict that mechanism:
   - §2z-ay: switching the home off breaks the ladder, so the home "is doing
     two jobs";
   - §2z-bu: "no return toward an endpoint's own level at a year".

   So A and B currently disagree about the mechanism the paper is about.
   _Verified_ from the two headlines.
2. **No load-bearing result is both non-mechanical and out of sample.** The
   reviewers listed three:
   - the boundary test is close to an identity for any model in which levels
     diffuse;
   - the structure forecasts do no better than trend;
   - the lock-in forecaster loses to a logistic on rank.
3. **The claim language is ahead of the numbers.** They gave four examples:
   - "stable form", against 51% → 35% in our own data;
   - "share", which is actually an index of geometric means;
   - "at every rank";
   - "at par with a logistic".

## Load-bearing findings, checked

| # | finding (who) | check | verdict |
|---|---|---|---|
| 1 | The registered lock-in forecaster is *worse* than a three-parameter logistic on log rank and log horizon (gpt-6-astra, Opus, Fable). | Recomputed from `lockin__*.json`. Brier, model against logistic: 0.1252/0.1244, 0.1357/0.1309, 0.1607/0.1587, 0.1621/0.1615. Aggregate error 0.035/0.022, 0.069/0.031, 0.073/0.028, 0.030/0.018. Within ±0.07: model 95/126, empirical-shape variant 106/126, constant turnover 111/126, logistic 117/126. The post hoc empirical-shape variant beats the logistic's Brier on three ladders, all but submissions. | **Verified.** "At par" and "as well as" are wrong. |
| 2 | The concentration "share" is an index of geometric-mean activity, not a share of karma (all three). | §2z-cw says so. The abstract and the figure axis do not. Arithmetic shares recomputed: comments top 195 went from 61.0% to 49.7% (index 63.2% → 51.9%). Submissions top 78: 50.2% → 34.0% (index 51.1% → 34.8%). | **Verified.** The label is fixed, and the fall is real in arithmetic shares. |
| 3 | The registered pooled boundary ratio takes σ² from cohort 0 at blocks 1, 3 and 5, but crossings from starts 1–3 at blocks a, a+2 and a+4, up to block 7. The two sides are not aligned in time (Opus, Fable). | `mts_boundary_law.py` lines 114–142. Time-aligned ratios (`by_start_post_hoc_output.txt`): comments head 1.01, 0.90 and 0.86 by start, against 1.22 pooled. | **Verified.** A registered design choice, not a bug, but it must be disclosed beside every pooled number. |
| 4 | With 13-week blocks on comments, two values fall outside the band, 0.72 and 1.41; §2z-cu says one (gpt-6-astra, Fable). | `boundary__reddit_comments_ext__block13__K12500.json` | **Verified.** Correction note at §2z-cu. |
| 5 | C3's ranges (Facebook 0.77–1.17, Instagram 0.94–1.12) are K/16…K ranges quoted under a K/64…K label. At K/64 the values are 0.71 (Facebook) and 2.49 (Instagram) (Fable, Opus). | Boundary JSONs. | **Verified.** |
| 6 | The spacing difference is exactly R_ac − R_ab = R_bc + C. It mixes the later reordering with a composition term, and its two halves sit on different survivor sets (gpt-6-astra). | Identity checked to 10⁻¹⁵. On one survivor population (§2z-da) the ratios barely move, except submissions at K (0.87 → 0.98). C is +25–35% of the statistic on Reddit and −30% on Facebook. | **Verified.** Disclosed as the estimand. |
| 7 | The Instagram bound ("variance of log M bounds q drift") ignores a covariance term (gpt-6-astra). | Algebra. | **Verified.** Withdrawn in §2z-da. |
| 8 | Scoreboard tallies disagree across files: 15 of 26, 9 of 17, 11 of 19. Two predictions of non-adjudication are counted as "held" (Opus, Fable). | Evidence packet and reviewer questions. | **Verified.** Reconciled in §2z-da. |
| 9 | The lock-in forecaster stratifies by *current* rank, departing from AGENTS.md, and the departure is undeclared (Opus, Fable). | `mts_lockin.py` docstring. | **Verified.** Declared in the lock-in DESIGN.md. |
| 10 | Gomez (2023, *Econometrica* 91:979–1024) splits top-share growth into within, between (rank mobility) and demography terms, with closed forms for random-growth models, and estimates them on the Forbes 400 (Fable). | The abstract confirms the three-term split and the closed forms. Fable says the between term is half the growth variance times the threshold density; the full text has not been checked here. | **Verified in part.** This is the balance law's closest precedent and is now the first item for the owner's novelty check. |
| 11 | Batty (2006, *Nature* 444:592) documented stable rank-size distributions over churning membership. The outline's claim that the joint regularity "has not been documented as a single empirical object" is wrong (Fable, Opus). | Abstract confirmed. | **Verified.** That claim is dropped from the draft frame. |
| 12 | The archive's capture delay changes on 1 July 2021, from about 20 days to about 360. Removals and deletions between posting and capture would then produce a drift by rank that follows the collection calendar. Nothing in the record rules this out (Fable). | `runs/2026-10-13_external_evidence/REPORT.md` D4; §2z-cd item 6 lists capture delay as unmeasured. | **Verified as an open risk.** The control analysis is in §2z-da. The audit of the raw archive needs the WD drive and a long run. |
| 13 | The boundary test is a near-identity for level diffusion. On a locally exponential ladder, R̃ = S·κ(1/S) = σ²/(2S)·[1 + κ₃/(3σ²S) + …]. It can leave 1 only through position-dependent increments, cumulants, a mismatch between cohort and occupants, or a misread density. So it rejects rank exchange but cannot separate the models anyone would fit (Opus, Fable). | Matches the planted results: §2z-cq gives 0.85–1.25 for every level variant, and §2z-cv shows the rank-exchange toys fail. | **Agreed.** The claim is reframed: the test establishes that movement happens in level space at the measured lasting variance, with stated power. It is not a "law of the dynamics". |

## Where I disagree or qualify

- **"Out of equilibrium adds no difficulty" (Opus).** True of the formula.
  The empirical point that survives is narrower: the formula holds while drift
  by rank is far from its stationary value. That is what separates it from a
  steady-state fit. It is still not a test of mechanism. Agreed.
- **Jensen's inequality as part of the concentration fall (Opus).** Measured
  on comments, it is not. Arithmetic shares fall by the same amount as the
  index (61.0% → 49.7% against 63.2% → 51.9%), and the Jensen gap at the head
  is 0.10–0.15 log units every year. Submissions: 50.2% → 34.0%, against the index's 51.1% → 34.8%.
- **"Nothing transports except E2" (Opus).** Fair for A. For B, the
  replication on three platforms is cross-sectional transport, though not
  out-of-time confirmation. The paper should say exactly that.
- **The A–B contradiction (Fable).** It is real for A as specified, with a
  home ladder. A's *movement measurement* (E2, the one-amplitude
  factorisation, the noise floor) does not depend on the home mechanism. The
  proposed repair, B as the spine with A as the measurement of movement, is
  the right one. It needs the owner's amendment of the binding paragraph.

## What each reviewer added that the others did not

- **gpt-6-astra.**
  - The exact finite-horizon identity and the survivor-set point.
  - The "contestability" frame: who occupies the top, against how much
    advantage the top commands.
  - A joint prospective test of shape and retention.
  - Arithmetic shares with uncertainty.
  - The June 2021 split into production and engagement per item.
- **Fable 5.1.**
  - The contradiction between A's home mechanism and B.
  - Gomez (2023) and Ichiba et al. (2011, hybrid Atlas).
  - The capture-delay deletion mechanism.
  - Unifying the lasting variance by rank from four estimators (A's
    σ_perm(z), the variance route, the variogram, 2S·R̃) in one figure.
  - The within-community comments/submissions contrast in June 2021.
- **Opus 5.5.**
  - The cumulant expansion and the four exit routes from ratio = 1.
  - The time-alignment finding.
  - Gabaix–Lasry–Lions–Moll speed of transition as the frame for relaxation.
  - Size–variance scaling (Stanley 1996, Riccaboni 2008).
  - Guvenen et al. (2021) on kurtosis as the "two-speed" precedent.
  - An event study with placebo years and difference-in-differences
    controls.
  - The sealed bridge's primary tests redesigned to be discriminating.

## What was done tonight, and what is left (details in §2z-da)

**Done, with the data on hand:**
- the corrections above, in the evidence packet, reviewer questions, big
  picture, figures, lock-in labels, design note and the record;
- the common-survivor split of the boundary statistic;
- arithmetic shares with weekly spread and Jensen gaps;
- a June 2021 event study with placebo years and two control ladders:
  - the submissions head's fall is not distinctive (X1 FAIL);
  - what is distinctive is a shift to the next tier and to the same
    communities' comments;
- the lasting variance by rank from several estimators;
- a revised draft frame (`PAPER_FRAME_DRAFT_2026-10-18.md`, not adopted).

**Not done; needs time, a drive or the owner:**
- fitted rival models run through the identical estimators (Iñiguez,
  Yule–Simon, two-type GLLM, block jumps);
- a block-over-time and joint bootstrap;
- the raw capture-delay and deletion audit (WD drive);
- the sealed bridge;
- Wikipedia;
- names for event analyses;
- the binding-claim amendment;
- reading Gomez (2023) in full.

## Sources for the two literature checks

- Gomez, M. (2023) [Decomposing the Growth of Top Wealth Shares](https://www.econometricsociety.org/publications/econometrica/2023/05/01/Decomposing-the-Growth-of-Top-Wealth-Shares), *Econometrica* 91(3), 979–1024.
- Batty, M. (2006) [Rank clocks](https://ideas.repec.org/a/nat/nature/v444y2006i7119d10.1038_nature05302.html), *Nature* 444, 592–596.

## Follow-up, 2026-10-18 (owner decision 3): Gomez read in full

His working paper (June 2019) already contains six things:
- the displacement law, ½(ζ − 1)ν² for arithmetic shares;
- its extensions for scale dependence, heterogeneity, aggregate risk and
  jumps (the cumulant series);
- a demography term;
- the exact accounting with panel data;
- the stationary balance;
- a test of predicted against measured displacement on the Forbes 400, out
  of equilibrium.

Our novelty claim (a) is therefore withdrawn as stated. Theory note §9 and
§2z-db give what remains ours.
