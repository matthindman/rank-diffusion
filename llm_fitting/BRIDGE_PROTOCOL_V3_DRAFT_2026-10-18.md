# The Reddit bridge, 2023-01 … 2024-06: protocol v3, redesigned around discriminating tests — DRAFT, NOT ADOPTED

_Claude Opus 5.5 (Anthropic), 2026-10-18. Written after the owner's decision
of 2026-10-18 ("4; yes": redesign the sealed-bridge protocol around the tests
the external reviews say would discriminate; MODEL_STATUS §6 addendum and
§2z-db). It replaces, as the proposal on the table, both
`BRIDGE_FORECAST_PROTOCOL_DRAFT_2026-10-14.md` and its revision of
2026-10-15. Those are kept unchanged; their fixed forecasts become a
secondary test here. **Adopting this protocol, and the first value-level
contact with the bridge, are the owner's decisions.**_

**Exposure at writing.** No file under a sealed folder (`SEALED_PATHS.json`)
has been opened or read. Everything here was computed from:
- development panels (`reddit_comments_ext`, `reddit_submissions_long`,
  2018-12 … 2022-12);
- planted worlds.

The development panels have been read many times. Every rule below was chosen
knowing their results, and the rules say where that knowledge shaped a choice.

## Why a redesign

Three external reviews (`runs/2026-10-18_external_critiques/`) and Gomez
(2023) (theory note §9) agree on four points.

1. **The 2026-10-15 primary test was too weak.** It was the boundary theory
   on new data with the band [0.75, 1.33]. For level diffusion the test is a
   near-identity (theory note §8), and that band admits the one plausible
   rival, volatility types still sorting, which gives 1.2–1.3 in the body
   (§2z-cq).
2. **It used no out-of-time information.** Variance and crossings came from
   the same blocks.
3. **Its secondary forecasts did not discriminate.** On submissions the fixed
   forecasts differ from "repeat last change" by less than one forecast SD.
4. **It had no test of a dated shock,** and its lock-in arm had no
   comparison with the stronger baseline, a logistic on rank (§2z-da).

## Design common to every test

| | rule |
|---|---|
| Ladders | `reddit_comments_ext` (K = 12,500) and `reddit_submissions_long` (K = 5,000), extended into 2023–2024 by the registered pipeline with the same universe rule, presence rule (0.9), tracking (4K) and certification |
| Blocks | 26 weeks: 2021 H2 (cohort block, development); 2022 H1 (origin a, development); 2022 H2 (development); 2023 H1, 2023 H2, 2024 H1 (bridge). Spacings 2 and 4 blocks from a give 52 and 104 weeks, as §2z-cm |
| Cohorts | permanent rank, absence-penalised, as in §2z-cm |
| Code | `mts_boundary_law.py`, `mts_lockin.py`, `runs/2026-10-18_event_study/event_study.py`, at the commit that adopts this protocol; no edits after adoption except as a dated amendment that only tightens |
| Intervals | 5–95%, bootstrap over communities. A joint block bootstrap is not built; that is disclosed |

## Stage 0 — the instrument, before any outcome

Recorded in order, before any test statistic is computed. Only instrument
fields are read at this stage:
- `created_utc`, `retrieved_on` / `retrieved_utc`, removal and deletion
  markers, file coverage.

Community-size deciles are fixed from 2022 development activity. No bridge
activity totals are formed.

1. **Coverage.**
   - Every daily file through 2024-06-30 is present and complete.
   - The source switches from Pushshift (through March 2023) to Arctic Shift
     (from April 2023). Overlap and continuity are checked at the switch.
   - `RC_2023-05` (comments, May 2023) must be fetched fresh. The WD copy
     sits on a bad disk region (§2z-ao). Without it the comments 2023 H1 block
     follows the common-hole rule (Conditions, item 3).
2. **Capture delay**, retrieval minus creation, by day. Any change of regime
   inside the bridge is dated, and the blocks it touches are reported
   separately in every test.
3. **Removal and deletion share** by month × 2022 size decile. This is
   Fable 5.1's alternative explanation for drift by rank. If the share rises
   with size after a capture-regime change, the drift-sensitive tests (P4,
   S2) are reported with and without the affected months.
4. **The flagged-week guard** of `instrument_eras`. Weeks it flags are left
   out as well as in.

## Primary tests

| | test | rule (fixed now) | why it discriminates |
|---|---|---|---|
| **P1** | Boundary theory, in period | Pooled ratio R̃/c, the geometric mean over cut-offs, inside **[0.85, 1.18]**: comments over K/8 … K, submissions over K/4 … K. Variance by the variance route, on the 2021 H2 cohort, over blocks a, a + 2, a + 4. Crossings: spacing difference 2–4 from a. | The margin is set by power, not convention (see Power). Plain and jump-diffusion worlds pass. The volatility-sorting rival falls inside it in 0 of 20 planted cases (mean 1.38). Rank exchange (2.4–3.6) is excluded. |
| **P2** | Boundary theory, out of time | The same ratio with **c predicted from development only**: σ² by the variance route on the 2019 H2 cohort over 2020 H1, 2021 H1 and 2022 H1, and S from the 2022 H1 ladder. Crossings from the bridge as in P1. Margin [0.85, 1.18]. | No constant is taken from the bridge. It passes only if lasting volatility by rank stays stable across the transition. It is at risk: on development, variance rose 1.16–1.55× after June 2021 (§2z-co). |
| **P3** | The blackout of 12 June 2023 as a dated shock (passing or lasting) | The §2z-da registered event-study design, with the event week = the week containing 12 June 2023. Placebo years 2019–2022, same calendar weeks, from development. **(a)** Submissions head (top K/64): Δ over weeks +1 … +8 is negative and below every placebo year by more than its half-width. **(b)** The shock is passing: the same contrast over weeks +17 … +24 lies within ±0.05 of zero. **(c)** Comments head: (a) and (b) hold as for submissions. | (a) checks the design on a shock known to have happened. (b) is the substantive claim: shocks to the head are passing, as the lasting/passing decomposition says of most large moves. A head that stays down is evidence against it. |
| **P4** | Lock-in: does the shape of movement add to rank? | Origins 2022 H1 and 2022 H2; top K/64, K/16, K/4; horizons 1–4 blocks into the bridge. The empirical-shape forecaster (`mts_lockin.py`, variant `empirical`) is **non-inferior** to the logistic on log rank and log horizon: pooled Brier no more than 0.001 above the logistic's on each Reddit ladder. Aggregate retention within ±0.05 of observed in ≥ 80% of cells. | On development the empirical variant beat the logistic's Brier on comments and lost on submissions (§2z-da). This is the first out-of-sample comparison against the strong baseline. The registered Gaussian forecaster is reported but not scored. |

**Verdicts, fixed now.**
- **Boundary physics confirmed on new data:** P1 passes on both ladders.
- **Volatility by rank predicts later crossings:** P2 passes on both.
- **Head shocks are passing:** P3(b) passes on both.
- **Movement adds to rank for retention:** P4 passes on both.

Each is reported as PASS, FAIL or NOT ADJUDICATED, at equal prominence, with
every condition below applied.

## Secondary tests

| | test | rule |
|---|---|---|
| **S1** | Deceleration of the submissions head | The change of shape at K/64, K/32 and K/16 from the 2022 block to 2023 is smaller in magnitude than the 2021→2022 change. The ratio of the two lies in [0.55, 0.95], against 1 for linear trend. The relaxation time τ ≈ 3–4 years of §2z-cp gives e^{−1/τ} ≈ 0.72–0.78. Power is weak: the difference is about one forecast SD. Also reported on the 18 months to 2024 H1. |
| **S2** | The fixed 2023 forecasts | As in the 2026-10-14 draft: A and B against "no change" and "repeat last change". |
| **S3** | Variogram sensitivity | P1 and P2 with the variogram estimator, reported and not scored. Development estimators differ by about ±10%. |
| **S4** | The composition term | The split R_bc + C on the bridge, against development (+25–35% on Reddit). |

## Power (planted, `runs/2026-10-18_bridge_power/`)

- **World.** The test-suite world, N 25,600 and K 1,600, with one start as
  the bridge gives. Pooled ratio over K/8 … K, 20 seeds each:

  | world | mean | SD | 5–95% |
  |---|---|---|---|
  | plain walk | 1.007 | 0.029 | 0.955–1.044 |
  | rare jumps | 1.047 | 0.057 | 0.979–1.156 |
  | volatility types still sorting (the rival) | 1.383 | 0.081 | 1.249–1.504 |

- **The margin [0.85, 1.18]:**
  - it keeps every plain-walk seed and every jump seed (20 of 20);
  - the rival falls inside in none of 20 seeds;
  - the old band [0.75, 1.33] admits the rival in 5 of 20 (25%).
  - A tighter [0.88, 1.14] would reject 3 of 20 jump seeds, so it is not
    used.
- **Scale.** Real strata at K/8 … K hold more names than the planted world
  (comments 1,562–12,500 against 200–1,600), so sampling spread on the bridge
  should be smaller. The margin is wider than sampling needs, so that level
  models with fat tails (jumps) pass.
- **The development data under this rule.**
  - Comments K/8 … K: 0.978, 0.992, 1.109, 1.217, geometric mean ≈ 1.07.
    Passes.
  - Submissions K/8 … K: ≈ 0.83, which would fail. That is why submissions
    is scored over K/4 … K, where it gives ≈ 0.91.
  - **This range choice was made knowing the development result**, and the
    shortfall at ranks 300–600 is a recorded, unexplained failure (§2z-cu,
    §2z-da). It is disclosed here and must be disclosed in the paper.

## Conditions

1. **The blackout in P1 and P2.** The 2023 H1 block ends inside the blackout
   (12 June – 16 July 2023). P1 and P2 are computed with and without the
   block's last three weeks. A pass on one version only is reported as
   exactly that.
2. **The source switch** inside 2023 H1. If Stage 0 finds a discontinuity,
   the block is also computed from April 2023 only.
3. **Comments' missing May 2023,** if it cannot be fetched fresh. Block means
   are taken over the available weeks, with the presence rule applied to
   those weeks. This is a declared deviation that the owner may refuse, in
   which case comments are NOT ADJUDICATED.
4. **2024 H1** needs the bridge complete through June 2024. That is checked
   in Stage 0.
5. **Nothing else is computed from the bridge** before P1–P4 and S1–S4 are
   recorded.

## What it spends, and what it would mean

- **What it spends.** The first value-level contact with the Reddit bridge,
  on both kinds of file. This is the owner's decision.
- **If P1 and P2 pass:** the displacement relation holds rank by rank on new
  data, against a margin that excludes the plausible rival. Lasting
  volatility by rank also predicts a later period's crossings. This is the
  paper's one out-of-sample sentence.
- **If P3(b) fails:** a large exogenous shock to the head lasted. That bears
  directly on the passing/lasting reading of the paper.
- **If P4 fails:** rank is enough to forecast retention, and the paper says
  so.
- **Not covered:** Wikipedia, which needs its own protocol written against
  its structure, not yet inspected, and owner-gated. Also not covered: any
  model fit, and the frozen stacks.
