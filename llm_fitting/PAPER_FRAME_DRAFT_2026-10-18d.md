# Paper frame "d": churn creates and maintains the hierarchy (the working frame after the owner's title decision)

_Claude Opus 5.5 (Anthropic), 2026-10-18. Written under the owner's
decisions (MODEL_STATUS §6 addendum; record §2z-dg). It supersedes frame c
(`PAPER_FRAME_DRAFT_2026-10-18c.md`, kept as the record).

**The text is the manuscript itself:** `paper/pnas_draft.tex`, the first
full draft, which compiles with `paper/references.bib`. Its figures are in
`runs/2026-10-18_pnas_push/` (`make_pnas_figures.py`). This file holds what
the manuscript does not: the title options, the claim list with record
sections, and what is still open._

## Title (the owner's direction: the contradiction, and how churn creates and maintains the structure)

Picked at the owner's request ("pick a headline that follows the norms and
revealed prefs of successful PNAS articles") from a study of the 50 most-cited
PNAS research titles of 2020–2024 (`runs/2026-10-18_pnas_push/TITLE_GENRE_STUDY.md`).

1. **Continual turnover sustains a slowly changing hierarchy of online
   attention** (used in the draft). A present-tense declarative whose verb
   is the measured result, with the contradiction stated literally;
   10 words, 75 characters.
2. Turnover sustains the hierarchy of online attention and can build it from
   equality. This carries the owner's "creates", hedged with "can", because
   creation rests on theory and the model.
3. Earlier: "The churn that unseats online leaders creates the hierarchy they
   lead". The unhedged "creates" is model-only, and "online leaders" reads as
   people.

## The argument in five claims (each with its record section)

| | claim | evidence | status |
|---|---|---|---|
| 1 | The puzzle: a fifth to a third of every top set is replaced each year; the top turns over fastest; the hierarchy changes slowly | §2z-cu; §2z-dg A1, A7-5 | measured |
| 2 | Size gives little protection, so churn is scale-free, and turnover at every scale follows from churn and slope, f = E[1 − e^(−D⁺/S)] | β §2z-dc, §2z-dg A7 (submissions unstable); kinematics §2z-dg A1 (exact form post hoc; all 48 cells within 0.82–1.18) | measured; post hoc formula labelled |
| 3 | Churn sustains the hierarchy. Every name slides; crossings return σ²/(2S) (pooled 0.96); the imbalance is the change (60 cells, corr 0.90, an accounting); the stationary steepness is σ²/(2s̄); relaxation takes years | §2z-cm, §2z-da, §2z-cd, §2z-cg, §2z-cp | measured; the same-year law is accounting with a theory of the boundary |
| 4 | A generative model with three ingredients (wandering lasting level, drift by rank, passing swings) builds the hierarchy from equality with steepness σ²/(2s̄) and forecasts held-out movement | §2z-dd, §2z-dg A3 (flat start), A5/A5b (long gates: frozen form FAILS submissions; V4 correction pending), A6 (pending) | partly pending |
| 5 | Where the balance tips: persistent decline in participation (shared trend on submissions, community-specific on comments; none on Facebook); displacement steady, the members' decline moves concentration (50% → 34%) | §2z-df, §2z-dg A4, A8 | measured |

## Does not claim

- that the mathematics is new (Fernholz, Gabaix, Gomez including Appendix F);
- prediction of the 2023–24 hierarchy, or anything out of sample on sealed
  data;
- that the generative model reproduces Reddit's annual structure;
- that persistence is growth (it is mostly decline) or that it is
  algorithmic amplification;
- the cause of the slide or of the platform-wide trend;
- that size never protects (the submissions exponent is unstable; Facebook
  post counts do steady with size).

## Open before submission

- The long-panel gates (A5, A5b) and the out-of-period structure test (A6).
- The joint bootstrap (A9).
- A round-3 referee review.
- Data availability (Pushshift terms; CrowdTangle access ended).
- Authorship and affiliations.
- The owner's choice of title, and approval of the claim language.
