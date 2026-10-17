# Following the prompt: altitude, range, depth, and the paper that results

_Claude Opus 5.5 (Anthropic), 2026-10-18. The answer to `PROMPT.md` in this
folder. The tests were committed before their runs: `DESIGN_depth.md` at
320e1e2 and aace364. Record: MODEL_STATUS §2z-dc. Claim language is a draft
for the owner._

## 1. Altitude

**Who reads it, and what question they bring.**
- *Computational social scientists and network scientists* who study
  popularity: preferential attachment, cumulative advantage, the
  ranking-dynamics models of Iñiguez et al. and Blumm et al.
- *Inequality economists* who study how concentration evolves: Gabaix;
  Gabaix, Lasry, Lions and Moll; Gomez; Jones and Kim.
- *Platform-governance researchers* who need to know whether attention
  markets entrench their leaders.

All three carry the same question, and the paper should answer it in its
first paragraph: **are the leaders of online attention entrenched, and what
sets how concentrated attention is?** The two folk answers conflict:
- critics: rich get richer, so leaders lock in;
- platforms: anyone can rise, citing the collapses.

**What the reader believes now that these data bear on.**
1. *Rich get richer.* In random growth a negative within term is generic (§3,
   test A). So the useful contrast is not "leaders decline". It is whether
   size buys stability, and whether movement is growth or rank exchange.
2. *Size brings stability,* as it does for firms: sd ∝ size^(−β) with
   β ≈ 0.15–0.2. That is not established for attention.
3. *Zipf-like ladders are stable.* They are not: the arithmetic share of the
   top 78 submissions communities fell from 50% to 34% in three years
   (§2z-da).
4. *Rankings change by exchanges of place.* This is the rank-displacement
   model class.

**Why now.**
- The EU Digital Services Act asks for systemic-risk assessment of
  recommender systems (Art. 34) and gives vetted researchers data access
  (Art. 40). A method that reads concentration dynamics from public ranked
  data is directly useful.
- CrowdTangle is closed and Pushshift is restricted, so multi-year public
  ranked panels like these may not be assembled again.
- Gomez (2023) gave the inequality literature a displacement decomposition.
  Applying it at every rank of a second domain is a cross-domain test that
  economists will read.

**The unreasonable target, argued for.**
- *The target:* "From one period's public ranked data, measured mobility
  tells you, ahead of time and at every rank, how fast leaders will be
  displaced. Because leaders are as mobile as anyone, online attention is
  contestable at every scale, unlike markets for firms."
- *Why it is not absurd:*
  - the displacement relation is a near-identity in period (theory note §8);
  - its predictive content is whether lasting volatility by rank persists;
  - lasting volatility nearly independent of size is the one parameter that
    makes contestability scale-free.
- *The solvable version, now:*
  - an out-of-time test inside the development window, with volatility from
    2019 to mid-2020 and crossings from late 2020 to 2022, on blocks that do
    not overlap;
  - the size–variance exponent measured as the firm literature measures it;
  - rank-model rivals calibrated to the data's turnover.

## 2. Range: three headlines

| | headline (one sentence) | evidence needed | in hand | missing | the hostile expert's first attack | enthusiasm × defensibility |
|---|---|---|---|---|---|---|
| **A** | Online leaders lose ground on their own, and concentration survives only because risers replace them: the reverse of wealth, where the rich get richer | within term < 0, measured without regression to the mean; Gomez's wealth numbers (within +1.9%, displacement +2.3% a year) | the members' lasting slide > 0 (§2z-cm) | a within estimator that is unbiased under passing swings | "Regression to the mean, and anyway a negative within term is what any stationary power law needs." | high × **low**: **killed by its method check** (§3, test A) |
| **B** | How fast leaders are displaced at every rank follows from how much communities there move and how crowded the ranks are, ahead of time, and only if movement is growth in levels | the out-of-time test; rivals calibrated to turnover | the in-period test on four ladders (§2z-cm, §2z-ct, §2z-cx) | out of time; calibrated rivals | "An identity in period; any rank model passes." | medium × high |
| **C** | Size does not protect the leaders of online attention: lasting volatility barely depends on size, so leaders are displaced at nearly the same rate at every scale, unlike firms | β measured as the firm literature measures it, with intervals, on three ladders; turnover by scale; B as the mechanism | β from the crossing-implied curve (§2z-da); turnover 0.79/0.82/0.84 (§2z-cu) | raw β with intervals; a link to the firm benchmark | "Your size range is narrow; karma is not attention; firms' β is for sales." | high × medium-high (pending Z) |
| (D) | A platform's design change steered attention concentration | an event study against placebo years | §2z-da | a control that survives placebo years | "X1 failed." | low × low: supporting, not headline |

**Choice: C as the headline, with B as its mechanism.** "Size does not
protect" is the sentence a reader carries away and the contrast with firms
that an editor can see. B is what makes it more than a descriptive exponent:
displacement at each rank follows from that volatility, ahead of time. A is
dropped and recorded as a negative result, because a reviewer would raise
it. D stays as an illustration.

## 3. Depth: the tests, run

### Test A: the method check that killed headline A (`planted_within.py`, planted only)

**Set-up.**
- A Zipf-like ladder (S 1.0) with passing swings (10-week half-life,
  variance 0.3).
- Drift by current rank, graded inside the top K: +, 0 or − about 0.08 a year
  at rank 100.
- The within term of arithmetic shares, per year, three seeds:

  | world, top 100 | naive one-step | lagged (selected a block earlier) |
  |---|---|---|
  | rich get richer (+0.078) | +0.028 | −0.021 (−0.076 to +0.053) |
  | neutral (0) | −0.029 | **−0.082** (−0.143 to +0.004) |
  | decay (−0.078) | −0.092 | −0.155 |

**Verdict.**
- The arithmetic within term under passing swings is biased downward by about
  as much as the drift it is meant to detect. Lagging the selection does not
  fix this: with heavy-tailed shares, the swings at the selection block
  persist into the next one.
- And in theory a negative within term balanced by displacement is the
  generic condition for a stationary power law (Gabaix 1999; Gomez's
  stationary case). So "leaders lose ground" is neither measurable cleanly
  here nor a finding.
- **Headline A is dropped.**

### Test O: displacement predicted ahead of time (`out_of_time.py`)

**Set-up.** 13-week blocks. Volatility from 2019 to mid-2020 (blocks 1, 3,
5). Crossings from late 2020 to 2022 (starts 6–11, blocks 6–15, with no
overlap). Slope at each window's origin.

| ladder | out-of-time ratio by cut-off, K/64 … K | pooled (scored range) | rule [0.85, 1.18] |
|---|---|---|---|
| comments | 0.86, 1.34, 1.39, 0.85, 0.83, 1.02, 1.11 | **0.944** (K/8 … K) | **O1 PASS** |
| submissions | 0.36, 0.69, 0.68, 0.59, 0.68, 0.59, 0.59 | **0.617** (K/4 … K) | **O2 FAIL** |

- **O3 is NOT ADJUDICATED.** The saved and new runs both take volatility from
  the early window only, so there is no in-period reference with the same
  blocks. My expectation for O3 (early volatility under-predicts) was wrong
  in direction on submissions.
- **Reading.**
  - On comments, volatility measured in 2019–20 predicts the displacement of
    2020–22 at every rank to within the margin when pooled. Single cut-offs
    scatter from 0.83 to 1.39, and the two high values sit where the 13-week
    variance strata are smallest.
  - On submissions, early volatility over-predicts later displacement by
    40–60% at every rank. That is the ladder's standing shortfall
    (in period 0.80, §2z-da) made worse.
  - Prediction ahead of time therefore holds on one ladder of two.

### Test V: rank-model rivals calibrated to the data's turnover (`rivals_calibrated.py`, planted)

Each rival is tuned so that the one-year retention of the top K/4 is 0.80,
the data's value.

| rival | parameter | retention | ratio by cut-off, K/64 … K | pooled K/8 … K | expectation |
|---|---|---|---|---|---|
| jumps to random ranks (Iñiguez-type) | p = 0.0042 a week | 0.807 | 0.90, 1.01, 1.08, 1.10, 1.39, 1.77, 2.20 | **1.56** (1.47–1.62) | V1 PASS: excluded |
| log-rank diffusion, fixed ladder | s = 0.070 a week | 0.782 | 0.86, 1.37, 1.32, 1.03, 1.03, 1.06, 1.08 | **1.05** (1.01–1.11) | V2 PASS: *not* excluded |
| neighbour swaps | — | 0.993 | 1.80, 0.75, 1.01, 1.19, 0.80, 1.34, 1.24 | 0.88 (0.35–1.70) | V3 PASS: erratic |

**Reading.**
- The calibrated displacement model is excluded, but only by the body of the
  ladder. At the head (K/64 … K/8) its ratios are 0.90–1.10, so a head-only
  test would not reject it.
- Rank diffusion in log rank with the ladder held fixed passes the test and
  cannot be told apart from level growth by it.
- The claim narrows to: "excludes displacement to random ranks and local
  swaps; level growth and fixed-ladder rank diffusion both pass". What
  separates those two is whether the ladder moves. It does, and its movement
  is what this paper explains (§2z-cw, §2z-da).

### Test Z: does size protect? (`size_variance.py`)

β is the exponent in sd(one-year change) ∝ size^(−β), over octave strata of
permanent rank K/64 … K. Intervals are 5–95% from 400 bootstrap draws over
names.

| ladder | size range (log activity, ranks K/64 … K) | raw β, year 1 | raw β, year 2 | lasting β | expectations |
|---|---|---|---|---|---|
| Reddit comments | 6.5–13.0 (×650) | 0.075 (0.061–0.093) | 0.059 (0.038–0.091) | 0.047 (0.025–0.078) | Z1, Z2, Z3 PASS |
| Reddit submissions | 9.0–14.2 (×180) | 0.045 (−0.013 to 0.143) | 0.082 (0.039–0.150) | 0.070 (0.032–0.128) | Z1, Z2 PASS; Z3 bound 0.150, borderline |
| Facebook pages | 10.4–14.4 (×55) | 0.008 (−0.032 to 0.063) | — | — | Z1, Z3 PASS |

**For scale.** Over the comments range, firms' β = 0.2 would make the sd of
one-year change fall 3.7-fold from the bottom stratum to the top one. It
falls 1.6-fold (0.67 → 0.42).

**Strata.**
- On submissions the top 78 are the *most* volatile stratum in 2020→21 (sd
  0.79), the year of the feed change.
- On Facebook the sd is 0.91–0.92 in every stratum.

**Verdict.** Size barely protects. The exponent is 0.01–0.08 on three
ladders, every upper bound is at or below 0.15, and the firm range is
0.15–0.2. Headline C stands.

**Limits.**
- *Survivorship.* Members absent in ≥ 10% of a block's weeks are dropped:
  2–7% of the lower strata, fewer at the top. Dropping collapses at the
  bottom lowers the bottom's sd, which biases β *down*. The size of that bias
  is not measured.
- *Range.* Two to three decades of size, against six for firms.
- *Units.* Karma is approval-weighted activity, not views.

One-year change split exactly into items (posts or comments) and engagement
per item. γ is the exponent of the sd against the item count. Independent
items would give 0.5.

| ladder | items per year, bottom → top stratum | γ, activity | γ, items | γ, engagement per item | items' share of variance | expectations |
|---|---|---|---|---|---|---|
| Reddit comments | 13,753 → 1,936,813 | 0.072 (0.054–0.098) | 0.065 (0.046–0.088) | **0.042** (0.015–0.069) | 0.86 | P1 PASS, P2 PASS |
| Reddit submissions | 6,797 → 170,290 | 0.117 (0.078–0.171) | 0.078 (0.033–0.138) | **0.120** (0.085–0.179) | 1.05 (items and engagement per item move against each other, corr −0.1 to −0.5) | P1 PASS, P2 PASS |
| Facebook pages (quarterly posts) | 800 → 2,401 | 0.068 (−0.051 to 0.251) | 0.187 (0.067–0.395) | **0.072** (−0.035 to 0.250) | 0.24 | P1 PASS, **P2 FAIL** (0.24 against 0.25) |

**Reading.**
- On Reddit, a community's activity changes mainly because the *number* of
  posts and comments it draws changes. That volume fluctuates about as much
  for a community with two million comments a year as for one with fourteen
  thousand.
- The engagement each comment earns moves by a near-constant 12–15% a year
  at every size. Independent items would make it all but constant for the
  largest communities.
- Attention to a community therefore moves as a unit, not as a portfolio of
  independent items. The firm-style explanation of size–variance scaling,
  diversification across sub-units, does not operate here.
- **Not measured: the alternative of extremely heavy-tailed engagement per
  item.** With a tail index α, sums fluctuate as n^(1/α − 1). γ ≈ 0.04 on
  comments would need α ≈ 1.04, which is implausibly heavy for comment
  karma. This is argued, not measured. Per-item scores from the raw archives
  would settle it.
- On Facebook the range of post counts is narrow (×3), and engagement per
  post dominates the variance.



## 4. The paper that results

Written in `PAPER_FRAME_DRAFT_2026-10-18b.md`, beside the 10-18 frame. It
does not overwrite that frame.

- **Title (recommended):** "Size does not protect the leaders of online
  attention".
- **Significance:** 118 words. **Abstract:** 234 words, every number
  checked against its section.
- **Figure 1:** `fig_contestable.png`, four panels:
  - (A) size does not protect;
  - (B) displacement at the same rate at every scale;
  - (C) predicted ahead of time, with comments within the margin and
    submissions short, shown as it is;
  - (D) calibrated rank rivals.
- **First paragraph.** It opens on the debate (entrenchment against "anyone
  can rise") and the firm benchmark. It asks the question before any method
  appears.

**Why this is stronger than the 10-18 frame.**
- The 10-18 frame led with an identity, "a balance at every rank", that
  Gomez had already used.
- This frame leads with a measured, falsifiable contrast to a famous
  regularity, the size–variance law for firms. Readers in three fields
  already know that law, and the contrast bears on the lock-in debate.
- The balance becomes the mechanism: why scale-free mobility gives
  scale-free displacement.
- Concentration becomes the consequence: with mobility nearly scale-free,
  only drift by rank can move it.

**What it gives up.**
- "A law" is no longer the headline.
- The paper now rests on one exponent and its contrast with firms. A hostile
  reviewer will go after the size range and the units (§5).

## 5. What would still stop an enthusiastic endorsement, and what fixes it

Ordered by how likely each is to sink the paper, each with what would fix it.

1. **No result is out of sample.** Ahead-of-time prediction holds on one
   ladder of two (comments 0.944, submissions 0.617).
   - *Fix:* the sealed Reddit bridge (2023–24), set aside. Its tests would
     be β, the balance in period and ahead of time, and the shift by tier.
     Wikipedia pageviews would answer the units objection at the same time.
     Both are owner-gated.
2. **Why does size not protect?**
   - The portfolio test (§3 P) says engagement per item barely steadies with
     the number of items (γ 0.04–0.12 against 0.5). That rules out the
     firm-style explanation (diversification over independent units).
   - It cannot yet separate two causes: shocks common to a whole community
     (exposure by the recommender), or engagement so heavy-tailed that one
     hit dominates.
   - *Fix:* item-level data (scores per post) from the development raw
     archives on the WD drive. This is a tail index of per-post engagement
     within community-years.
3. **Size range and survivorship** (§3 Z, Limits).
   - *Fix:* extend the strata to the whole tracked universe (ranks to 4K),
     and treat absences as collapses at a floor. Both are cheap, on
     development data.
4. **Units: karma and interactions are approval-weighted activity, not
   attention.**
   - *Fix:* Wikipedia (owner-gated), or Instagram's per-post engagement
     through the censoring apparatus as a check on the *slope* only.
5. **Precedence.**
   - Gomez for the balance.
   - Gabaix and Gibrat for random growth: "Gibrat holds in attention" is not
     new as a model assumption. What is new is a measured exponent against
     the firm benchmark, with displacement as its consequence.
   - *Fix:* say so in the first page.
6. **Submissions.** The ladder with the most movement in concentration fails
   the displacement balance's prediction ahead of time, and in its body.
   - *Fix:* none in hand. Report it as the ladder where drift by rank,
     unmeasured volatility at the head, and the June 2021 change coincide.

**Verdict on the prompt's question.** With items 1 and 2 fixed, the paper
has three things:
- a headline a journalist can write;
- a contrast with a famous law that economists and network scientists will
  check;
- a mechanism and a consequence for concentration.

That is an enthusiastic-endorsement profile. Without item 1 it is a strong
in-sample paper, and referees will ask for item 1 first.
