# Forecast of the 2023 Reddit ladders — DRAFT protocol, NOT ADOPTED

_2026-10-14, Claude (Opus 5.5). Drafted at the owner's request to pursue the
rate-law line "aggressively and rigorously"; not adopted. The owner adopts or
rejects it. The forecasts it scores are already fixed: they were computed
from development data only and committed with this draft, before any contact
with the sealed archive._

**Exposure at the time of writing.** No file under the sealed bridge folder
(`SEALED_PATHS.json`) has been opened, listed beyond what the ledger records,
or read. The forecasts use `reddit_submissions_long` and `reddit_comments_ext`
(DEVELOPMENT, 2018-12 to 2022-12) only. This draft spends nothing; adopting it
and then building the 2023 panel would be the first value-level contact with
the bridge, which needs the owner's word in the conversation.

## What is forecast

From the 2022 block (52 weeks from 2021-12-27) to the 2023 block (52 weeks
from 2023-01-02): the change of the ladder's shape, the top-k mean minus the
top-K mean of block levels, at K/64, K/32, K/16, K/8, K/4 and K/2, with the
same K, presence rule, tracking and certification as step 1 (§2z-bu).
Submissions K = 5,000; comments K = 12,500.

The numbers are in `runs/2026-10-14_bridge_forecast/forecast.md` and
`forecast.json`, produced by `make_forecast.py` in that folder. Two
forecasters and two baselines, all fixed:

- **A, forward simulation** (§2z-ce): the 2021→2022 transition applied to
  the 2022 block, 200 draws. The 2020→2021 transition is reported as a
  sensitivity and not scored.
- **B, rate law** (§2z-cd): slides of the 2021 cohort over 2021→2022, lasting
  variance from 2020–2022, slope of the 2022 ladder; the persisting set's
  rate plus the 2021→2022 entry term, over 52 weeks.
- **Baselines**: no change; repeat the 2021→2022 change.

In short, for submissions both forecasters say the head goes on sinking
against the top 5,000: by 0.19 at K/64, 0.14–0.22 at K/32, 0.12–0.15 at K/16
and about 0.09 at K/8 log units over 2023. They disagree below K/8: A says
nearly no change, B says the lower half of the top K rises. For comments A
says almost no change and B says the middle rises by up to 0.10.

## Scoring, fixed now

For each panel and forecaster: the rms error over the six cut-offs of the
forecast change against the observed change, and the same for the two
baselines.

- **Primary**: on submissions, forecaster A beats "no change". This is the
  test of the claim that movement measured before the year predicts where
  structure goes in it.
- **Secondary**: A and B beat "repeat last change"; signs agree at the four
  cut-offs from K/64 to K/8 on submissions; A's 2023 local slopes are within
  15% of the observed ones at all six cut-offs.
- Comments are scored the same way, as secondary, and only if the 2023 block
  can be built complete (see below).
- No other statistic of the 2023 data is computed before these are recorded.

## Conditions before scoring

The 2023 data are not a continuation of the 2018–2022 instrument by default.
Each of these is checked, in order, and recorded, before the score:

1. **Source.** Pushshift files through March 2023 and Arctic Shift files from
   April 2023 (§2z-ah). The capture delay of scores, retrieval time minus
   creation time, is measured by day across the year from the raw files, and
   the score-capture guard of `instrument_eras` is applied. Weeks it flags
   are reported and the score is computed with them left out as well as in.
2. **The June 2023 blackout.** From 12 June 2023 many large subreddits went
   private or read-only in protest at Reddit's API pricing, some for weeks.
   This is a dated exogenous shock that neither forecaster knows about.
   The score is computed on the full block and, as declared here, on the
   block with the weeks of 12 June to 16 July 2023 left out. A failure on the
   full block and a pass on the reduced one is reported as exactly that.
3. **Completeness.** The comments archive for May 2023 (`RC_2023-05`) is not
   on hand. Without it the comments 2023 block is not built and comments are
   NOT ADJUDICATED.
4. **Build.** The 2023 weekly panel is built by the registered pipeline from
   raw files, with the same universe rule; nothing about it is looked at
   before the score but the checks above.

## What the result would mean

A pass on the primary test is one dated, out-of-sample instance of structure
following measured movement, on the panel where structure moves most. It
does not separate forecaster A from "repeat last change", which gives similar
numbers here; the secondary tests and the disagreement between A and B below
K/8 are where the forecasters part. A failure, with the conditions met, is
evidence against the claim at the one-year horizon on this platform.

## Not covered

Wikipedia. The first half of 2024. Any model fit. The frozen stacks.

---

## Proposed revision, 2026-10-15 (Claude; NOT ADOPTED, for the owner)

_Added after §2z-cm and §2z-co. Nothing above is changed, and nothing here is
adopted. No sealed file has been opened._

**Why revise.** Two findings change what an outcome-blind test of this program
should be:
- *The boundary theory is the part that is not accounting* (§2z-cm, §2z-co).
  Crossings at a rank's boundary return lasting variance over twice the local
  slope, predicted rather than measured. On comments it holds at every rank
  with two variance estimators.
- *Forecasts lack a clear edge.* At the half-year and one-year scales the
  forecasters do no better than repeating the ladder's own last change
  (§2z-co; §2z-cd). A pass against "no change" alone would not show that
  movement predicts structure beyond trend.

**Proposed primary test: the boundary theory on new data.**
- *Set-up.* 26-week blocks: the 2022 H1 block (DEVELOPMENT), and the 2023 H1
  and 2024 H1 blocks (bridge).
  - The persisting top k at the 2022 H1 block and its reordering gains to
    2023 H1 (52 weeks) and to 2024 H1 (104 weeks) give the lasting crossing
    gain R̃_k as in §2z-cm.
  - The cohort fixed on the 2021 H2 block, over the same three blocks, gives
    σ² by the variance route. The slope is that of those blocks.
- *Rule, as §2z-cm B1.*
  - Comments: R̃_k / (σ_k²/2S_k) in [0.75, 1.33] at ≥ 5 of the 7 cut-offs K/64 … K.
  - Submissions: the same at the 4 cut-offs K/8 … K only, since its head
    cannot be measured (§2z-cm, §2z-co).
- *Second rule, as §2z-cm B2.* The lasting slide of whoever holds the top k
  equals the cohort's.
- *Why it suits the sealed data.* It needs no forecast and is immune to the
  2023 shocks' effect on drift, because it relates crossings to volatility and
  crowding in the same period. It is the out-of-sample replication of the
  law's physics.

**Forecast tests become secondary** and are scored against both baselines,
"no change" and "repeat last change", with the forecasts fixed above. A
forecaster that beats only "no change" is reported as not separating from
trend.

**Conditions, added to the four above:**
- *The blackout of 12 June – 16 July 2023* straddles the 2023 H1 and H2
  blocks. For the boundary test, the H1 block is computed with and without its
  last five weeks.
- *Comments' missing May 2023* is a hole common to every community. Proposed:
  comments block means over the available weeks, with the presence rule
  applied to those weeks. This is a declared deviation that the owner may
  refuse, in which case comments stay NOT ADJUDICATED as above.
- *2024 H1 needs the bridge through June 2024*, which the sealed folder's name
  says it covers. Its completeness is checked first.

**What it spends.** First value-level contact with the Reddit bridge (both
kinds), the owner's decision. The sealed Wikipedia panels could take the same
primary test as a second system. That needs a protocol written against their
structure, which has not been inspected.
