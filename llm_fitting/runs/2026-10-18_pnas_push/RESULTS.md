# The PNAS push: results, scored against PLAN.md as they arrive

_Claude Opus 5.5, 2026-10-18. Expectations committed at 3f336f6 (A1, A3,
A4, A5, A7, A8) and b45a714 (A6) before the runs. Development data only.
Record: MODEL_STATUS §2z-dg._

## A1. Turnover from churn and slope (`turnover_kinematics.py`)

**Registered formula, v_k + (1 − v_k)·E[(Δθ − Δ)⁺]/S_k.** It over-predicts
on every ladder, by a median of 1.30 (submissions), 1.27 (comments) and
1.45 (Facebook) (`turnover_output_registered_run.txt`).

| | expectation | result |
|---|---|---|
| A1-1 | ratio within [0.8, 1.25] in ≥ 80% of cells | **FAIL:** 11%, 39%, 25% of cells |
| A1-2 | CV across cut-offs of the predicted share < 0.20 on each Reddit ladder | **PARTLY:** submissions 0.13/0.15/0.26, comments 0.10/0.14/0.18 (5 of 6 pairs) |
| A1-3 | CV of the boundary sd and of S < 0.25 | **PARTLY:** sd yes in 7 of 8 pairs (0.04–0.28); S no on comments (0.26–0.29), yes elsewhere (0.14–0.21) |

**The reason, and the exact form (POST HOC, labelled).** The registered
formula assumed a constant density of names above the boundary. On a ladder
that is locally linear in log rank, the number of names more than u above
the rank-k level is k·exp(−u/S_k). The density therefore falls off
exponentially, and the exact kinematics is

    replaced share = v_k + (1 − v_k) · E[1 − exp(−(Δθ_k − Δ_i)⁺ / S_k)]

It has no parameter. It was derived after seeing the registered formula fail,
and it is reported as post hoc.

| ladder | cells | within [0.8, 1.25] | ratio range | median | rmse | corr |
|---|---|---|---|---|---|---|
| submissions | 18 | **100%** | 0.83–1.18 | 0.98 | 0.018 | 0.68 |
| comments | 18 | **100%** | 0.85–1.05 | 0.98 | 0.009 | 0.91 |
| Facebook (26 weeks) | 12 | **100%** | 0.82–1.10 | 0.98 | 0.024 | 0.87 |

**Reading.**
- Turnover at every scale is fixed by two measured quantities: how far names
  near the boundary move in a year, and how steep the ladder is there.
- The mean downward move at the boundary, E[D⁺], is almost flat across scale
  (0.25–0.40 log units). That is the scale-free churn.
- The slope S rises down the ladder (comments 0.95 → 2.1).
- Turnover is therefore highest at the head, where the hierarchy is flattest:
  top 10 27–33%, top k = K about 16–18% a year.
- **Leaders are the most exposed, not the most protected.**

## A7. The exponent, made robust (`exponent_robustness.py`)

| | expectation | result |
|---|---|---|
| A7-1 | comments β within 0.03–0.10 in every leave-one-out fold, both years | **PASS:** 0.068–0.081 (year 1), 0.049–0.070 (year 2) |
| A7-2 | β to rank 4K, absences at the floor: < 0.12 on every ladder | **PASS:** submissions 0.067 / 0.109; comments 0.042 / 0.013; Facebook 0.057 |
| A7-3 | Facebook lasting β < 0.10 | **PASS:** 0.019 (lasting sd 0.44–0.51 per 13 weeks in every stratum) |
| A7-4 | β from item counts < 0.10 on all ladders (§2z-dc's portfolio exponents) | **FAIL on Facebook:** Reddit 0.065 / 0.078; Facebook 0.187. Facebook's post counts do steady with size; its engagement per post does not (0.072) |
| A7-5 | one-year retention of the top 10 and 25 ≤ 0.90 on both Reddit ladders | **PASS:** top 10 0.6–0.8, top 25 0.64–0.84; Facebook (half-year) 0.6–0.72 |
| A7-6 | fold change of the sd < 2.0 on comments and Facebook | **PASS:** comments 1.58 / 1.33 end to end (max/min 1.58 / 1.50); Facebook 1.03 (1.21) |

**Submissions.**
- Year 1 leave-one-out runs 0.00–0.19, year 2 0.058–0.14.
- To rank 4K with absences: 0.067 / 0.109.
- It is reported as unstable, as §2z-df said.

## A3. Churn creates and maintains: the counterfactual (`churn_counterfactual.py`)

**Setup.** The lasting core alone, with σ_L(z) estimated on the whole panel
and drift by rank from B's measured slides. Shape changes are per year, at
k = K/64 … K/2, the mean of 20 seeds over 104 weeks.

**Comments** (σ_0 0.114, β 0.126; drift −0.0054 a week at the head and
−0.0029 at K):

| world | shape change a year at K/64, K/32, K/16, K/8, K/4, K/2 | c − slide in the simulation, × 52 |
|---|---|---|
| (i) churn + slide | −0.080, −0.071, −0.031, +0.013, +0.031, +0.036 | −0.062, −0.061, −0.016, +0.036, +0.059, +0.075 |
| (ii) slide only | −0.038, −0.031, +0.005, +0.040, +0.049, +0.037 | −0.033, −0.026, +0.011, +0.049, +0.063, +0.064 |
| (iii) churn only | +0.027, +0.028, +0.037, +0.048, +0.050, +0.048 | +0.036, +0.041, +0.050, +0.067, +0.071, +0.088 |

| | expectation | comments |
|---|---|---|
| A3-1 | (i): shape change within 25% of c − slide at K/64 … K/4 | **PARTLY.** Head within 30%, with the sign right everywhere. Deeper, c − slide over-predicts about 2×: the prediction omits the exit term (names leaving within the top K), which the simulation has |
| A3-2 | (ii): shape at K/64 falls ≥ 0.15 a year | **FAIL:** −0.038 a year. The direction is right; the size is a quarter |
| A3-3 | (iii): shape at K/64 rises ≥ 0.10 a year | **FAIL:** +0.027. The direction is right; the size is a quarter |
| A3-4 | (iv) flat start, 30 years: slope within 25% of σ²/(2·slide) at K/16 … K/2 | **PASS:** 0.83/0.82, 1.11/1.12, 1.54/1.62, 1.97/2.17. Also within 10% at K/64 and K/32 (0.54/0.52, 0.65/0.62) |

**Reading.**
- **Churn creates the hierarchy.** From complete equality, the measured
  churn and slide build a ladder within about a decade. The shape at K/64 is
  1.68 log units after 1 year, 2.98 after 5, 3.41 after 10 and 3.87 after 30.
  Its steepness at every rank is σ²/(2·slide), within 10%.
- Against the observed ladder (S 0.99, 1.21, 1.32, 1.38, 1.74, 2.13), the
  sustained body is within 20% (1.11, 1.54, 1.97 at K/8 … K/2). The
  sustained head is flatter (0.54 against 0.99). The comments head sits above
  the level its movement sustains, as §2z-cd's cells say (S* 0.63–0.64
  against S 0.89–0.91), and relaxes over years (§2z-cp: 5–22 years).
- **The hierarchy is slow.** Removing churn or slide moves the head by only
  0.03–0.04 log units a year, against a span of about 3–4. A hierarchy built
  by fast churn carries memory for years. That is the "stable-ish": out of
  balance, it drifts at the size of the imbalance, not at the speed of the
  names.
- The expectations A3-2 and A3-3 were set from the data's cohort slide
  (0.004–0.011 a week). In the model, drift by rank applies to whoever holds
  a rank, and the head's slide against U_K is a few ten-thousandths a week.
  The expectations confused the cohort quantity with the occupant quantity.

**Submissions** (σ_0 0.100, β 0.045; drift −0.0103 a week at the head and
−0.0036 at K):

| world | shape change a year at K/64 … K/2 | c − slide × 52 |
|---|---|---|
| (i) churn + slide | −0.160, −0.113, −0.069, −0.042, −0.010, +0.006 | −0.186, −0.103, −0.049, −0.043, −0.002, +0.019 |
| (ii) slide only | −0.259, −0.166, −0.110, −0.085, −0.047, −0.017 | −0.258, −0.165, −0.110, −0.084, −0.045, −0.012 |
| (iii) churn only | +0.073, +0.059, +0.052, +0.046, +0.042, +0.028 | +0.080, +0.068, +0.074, +0.045, +0.050, +0.042 |
| (iv) flat start, 30 years: slope | 0.53, 0.66, 0.82, 0.95, 1.13, 1.37 | σ²/(2·slide) 0.50, 0.65, 0.81, 0.93, 1.09, 1.37; observed 0.85, 0.93, 1.02, 1.26, 1.32, 1.55 |

**A3 scored on both ladders:**
- **A3-1, PARTLY.** Submissions within 25% at K/64, K/32 and K/8, 40% at
  K/16. Comments within 30% at the head. Deeper, the omitted exit term
  dominates.
- **A3-2, MIXED.** Submissions −0.26 a year passes; comments −0.04 fails.
- **A3-3, FAIL on both.** Comments +0.03 and submissions +0.07. The sign is
  right.
- **A3-4, PASS on both.** Within 4% (submissions) and 10% (comments) of
  σ²/(2·slide) at every rank.

The hierarchy forms from equality in about a decade on both ladders. The
shape at K/64 reaches 2.9 (submissions) and 3.4 (comments) by year 10. The
sustained head is flatter than the observed one on both ladders.

## A8. Gomez's decomposition of the arithmetic shares (`gomez_decomposition.py`)

Annual relative change of the top-k arithmetic share = within + between
(exact):

| ladder, k | 2019→20 | 2020→21 | 2021→22 |
|---|---|---|---|
| submissions 78 | −0.009 = −0.061 + 0.052 | −0.161 = −0.237 + 0.076 | −0.186 = −0.259 + 0.073 |
| submissions 312 | −0.022 = −0.080 + 0.059 | −0.099 = −0.162 + 0.064 | −0.109 = −0.165 + 0.056 |
| submissions 1,250 | −0.022 = −0.064 + 0.042 | −0.042 = −0.100 + 0.058 | −0.047 = −0.097 + 0.049 |
| comments 195 | −0.072 = −0.125 + 0.053 | −0.072 = −0.150 + 0.078 | −0.054 = −0.123 + 0.069 |
| comments 781 | −0.052 = −0.104 + 0.052 | −0.036 = −0.097 + 0.061 | −0.023 = −0.076 + 0.054 |
| comments 3,125 | −0.023 = −0.067 + 0.044 | −0.010 = −0.059 + 0.049 | −0.006 = −0.043 + 0.037 |

**Scored:**
- **A8-1** (the fall is dominated by the within term, |within| > 2·|between|):
  - **PASS for the submissions head:** over three years, within −0.56
    against between +0.20, a factor of 2.8;
  - **borderline for the comments head:** −0.40 against +0.20, 2.0, not
    above;
  - by cell, 5 of 18.
- **A8-2** (measured between within ×2 of ½(ζ − 1)ν²): **FAIL.** The
  implied term is −0.14 to +0.04 (negative where ζ < 1, that is S > 1),
  while the measured term is +0.04 to +0.08 everywhere.

**Reading.**
- Displacement is steady, +0.04 to +0.08 of the share a year, at every
  cut-off and year: the same steadiness as the reordering gain of §2z-cr.
- What moves concentration is the members' own drift relative to the total.
  The submissions head's members lost about a quarter of their relative
  activity a year in 2020–22.
- Gomez's arithmetic formula, applied to block-level attention data,
  under-predicts measured displacement everywhere. Passing swings inflate
  measured displacement, which is Gomez's measurement-error bias, here
  first-order. That is why the paper's test uses permanent-rank cohorts, the
  mean-log form and spacing differences, under which measured over predicted
  is 0.96.

## A4. What carries the momentum (`momentum_decomposition.py`)

The gapped statistic of §2z-df, with the gaps as stated:

| | submissions | comments | Facebook |
|---|---|---|---|
| D6 activity, g 0/13/26 | +0.201 / +0.174 / +0.140 | +0.163 / +0.154 / +0.141 | −0.114 / −0.067 / −0.031 (gaps 0/4/8) |
| D6 items (posts or comments) | **+0.188 / +0.186 / +0.173** | **+0.138 / +0.136 / +0.125** | −0.103 / −0.038 / −0.001 |
| D6 activity per item | +0.015 / +0.018 / +0.035 | +0.045 / +0.058 / +0.071 | −0.114 / −0.078 / −0.036 |
| D7 raw (origins ≥ 52) | +0.188 / +0.149 / +0.112 | +0.175 / +0.158 / +0.126 | −0.083 (g0) |
| D7 net of each community's loading on the platform aggregate (loadings from weeks 0–51) | **+0.149 / +0.054 / +0.006** | +0.168 / +0.161 / +0.130 | −0.071 |
| D7 net of the co-movement cluster mean (k = 20) | +0.187 / +0.147 / +0.108 | +0.174 / +0.158 / +0.127 | −0.083 |
| D8 Spearman within change-1 risers / fallers (gap 13; FB gap 4) | **+0.016 / +0.204** | **−0.016 / +0.189** | −0.070 / −0.016 |
| D8 mean rank of change 2: top / bottom change-1 quintile | 0.547 / 0.399 | 0.532 / 0.403 | 0.466 / 0.517 |
| D9 lag profile, gaps 0 / 26 / 52 (origin week 26) | +0.194 / +0.150 / **+0.046** | +0.135 / +0.136 / **+0.108** | — |
| D10 at Facebook's settings from 2020-12-28, gaps 0 / 4 / 8 | +0.019 / +0.054 / +0.080 | −0.037 / +0.024 / +0.077 | (−0.114 / −0.067 / −0.031) |

**The PLAN readings, scored:**

| reading | prior | submissions | comments |
|---|---|---|---|
| items carry it (≥ +0.10, per item < +0.05) | 55% | **YES** | items yes (+0.136), per item just above the line (+0.058): **MOSTLY** |
| shared trends explain at least half (residual < half) | 40% | **YES**, for the aggregate loading: 0.149 → 0.054 at g13, 0.112 → 0.006 at g26; clusters remove nothing | **NO:** survives fully (≥ 75%) |
| asymmetry favouring risers (risers > fallers by 0.05) | 35% | **NO, the reverse:** fallers persist, risers do not | **NO, the reverse** |
| decays by 52 weeks (< +0.05) | 50% | **YES** (+0.046) | **NO:** flat (+0.108) |
| Reddit > +0.05 at Facebook's settings | 65% | **PARTLY:** 2 of 3 gaps | **PARTLY:** 1 of 3 |

**Reading.**
- On both Reddit ladders the persistence is persistent **decline in
  participation**. It is carried by the number of posts or comments, not by
  the engagement each earns. Communities that lost participants kept losing
  them; communities that gained did not keep gaining. This is the opposite
  of amplification of risers.
- **The two ladders differ in origin.**
  - On submissions, persistence is heterogeneous exposure to a platform-wide
    trend. Each community's loading on the aggregate, estimated from weekly
    co-movement in 2019, removes it, and it fades within a year. This is
    Gomez's aggregate-risk term (his Prop. 4): differences in exposure to
    common shocks.
  - On comments, it is specific to each community, not removed by the
    aggregate or by co-movement clusters, and still +0.11 at a year's gap.
- **Facebook** shows short-memory reversion throughout, with no asymmetry.
- At Facebook's short horizon and dates (26-week changes from December
  2020), Reddit's persistence is weak (−0.04 to +0.08). The platform contrast
  is clearest at the annual horizon, where Facebook has no data. This
  qualification goes into the paper.

## A5 / A5b. Long-panel gates (Gate v1, 20 reps, 2,000 draws; one harness)

| arm | home random walk | drift by rank | `reddit_submissions_long` | dRank13 err | `reddit_comments_ext` | dRank13 err |
|---|---|---|---|---|---|---|
| persistence | | | 0.163 ± 0.048 | — | 0.167 ± 0.039 | — |
| A (`home`) | no | no | 0.188 ± 0.063 (frozen A in Codex's harness 0.194) | 0.199 | **0.123 ± 0.051** | 0.085 |
| (b) `homedrift` | no | yes | **0.171 ± 0.035** | 0.174 | (queued) | |
| (a) `movinghome_nodrift`, frozen variance | yes | no | 0.267 ± 0.091 | 0.370 | (running) | |
| RDM frozen (`movinghome`) | yes | yes | 0.246 ± 0.085 | 0.370 | 0.145 ± 0.057 | 0.162 |
| RDM corrected (`movinghome_vp`, post hoc V4), drift applied | yes | yes | (rerunning) | | (rerunning) | |
| (a′) corrected, no drift (`movinghome_vp` at fc28c25, BUG: no drift; confirmed by `movinghome_vp_nodrift`) | yes | no | 0.175 ± 0.072 (reproduced exactly) | 0.227 | **0.100 ± 0.047** | 0.070 |

Paired splits, submissions: frozen RDM better than A in 2/5; corrected in
3/5; (b) in 4/5. Comments: frozen RDM better than A in 1/5; corrected in 4/5,
and better than persistence in 5/5.

**Expectations, submissions:**
- **A5-1** (frozen RDM ≤ A + 0.05): **FAIL** (0.246 against 0.238).
- **A5-2** (frozen RDM's dRank13 error below A's): **FAIL** (0.370 against
  0.199).
- **A5b-1** (corrected ≤ A + 0.05): **PASS** (0.175).
- **A5b-2** (13-week median within ±1 in at least 4 of 5 splits): **FAIL**
  (2 of 5; it now overshoots by 0–3).
- **A5b-3** (corrected below frozen): **PASS.**

**Comments**:
- frozen form: A5-1 PASSES (0.145 ≤ 0.173) and A5-2 FAILS (dRank13 0.162
  against A's 0.085);
- corrected: A5b-1 PASSES (0.100, the best arm). A beats persistence on
  both measures.

**Reading.**
- The frozen form's long-panel failure is the V4 algebra, and the correction
  removes it.
- The corrected model is at par with A (0.175 against 0.188).
- **Neither model beats persistence on this panel on average** (0.163). A
  had already lost to persistence here (GATE_LEDGER row 84). The weekly
  movement of the top-200 cohort on long submissions is not better forecast
  by either generative model than by carrying the training statistics
  forward.

## A9. Joint bootstrap of the pooled boundary ratio (`joint_bootstrap.py`)

| ladder | point (K/16 … K) | registered 5–95% | joint 5–95% |
|---|---|---|---|
| comments | 1.069 | 1.011–1.127 | 0.986–1.152 (+43% wider) |
| submissions | 0.802 | 0.680–1.004 | 0.633–1.009 (+16%) |
| Facebook (one start) | 0.977 | 0.802–1.202 | 0.840–1.119 |
| three ladders | 0.942 | — | **0.855–1.040** |

**Scored:**
- **A9-1 PASS:** the point values reproduce §2z-da exactly.
- **A9-2 PARTLY:** comments +43%, submissions +16%, against ≥ 20% expected.
- **A9-3 PASS:** the three-ladder interval contains 1.

The four-ladder figure of §2z-da, 0.955 (0.889–1.054), used independent
draws. The joint three-ladder figure, 0.94 (0.86–1.04), is the one to quote
for Reddit and Facebook. Instagram's apparatus has no joint version.


**CORRECTION (same day).** The "corrected" rows first recorded (0.175
submissions, 0.100 comments) ran without drift by rank. At fc28c25 the drift
line applied drift only for `lasting == "movinghome"`. The deliberate
no-drift arm reproduces 0.175 to every digit, and the comments run started
(16:55:57) before the fix (committed 17:00). They are arm (a′).
- The fix is at b2836e6, with the guard test
  `test_corrected_moving_home_applies_drift_by_rank`.
- The drift-applied corrected model is rerun. A5b's scores for "the
  corrected variant" are withdrawn until then.
- The (a′) numbers stand as their own result. A random-walk home with the
  variance correction and NO drift by rank is the best arm on comments at
  1–13 weeks (0.100 against A 0.123), and at par on submissions.

## A6. Out-of-period structure (`structure_oop.py`; fit on 2019–20, forecast 2021–22; frozen moving home, drift applied)

| | submissions data | A | moving home | baseline | comments data | A | moving home | baseline |
|---|---|---|---|---|---|---|---|---|
| one-year retention 2021→22 at K/64, K/16, K/8, K | 0.833, 0.830, 0.818, 0.878 | 0.873, 0.796, 0.808, 0.890 | 0.847, 0.896, 0.894, 0.873 | carried forward 0.795, 0.772, 0.806, 0.838 | 0.785, 0.807, 0.825, 0.858 | 0.867, 0.836, 0.843, 0.894 | 0.833, 0.812, 0.817, 0.874 | 0.795, 0.826, 0.823, 0.840 |
| retention MAE | — | **0.024** | 0.040 | 0.037 | — | 0.041 | 0.019 | **0.013** |
| arithmetic share of top K/64, 2022 (2020 → 2022 observed) | 0.508 → 0.356 | 0.528 (0.45–0.69) | 0.495 (0.47–0.52) | 0.508 | 0.569 → 0.505 | 0.559 | 0.598 | 0.569 |
| momentum (adjacent blocks; data include about +0.24 from averaging) | +0.449 | −0.079 | +0.102 | | +0.431 | −0.259 | +0.026 | |
| excess kurtosis of 26-week changes | 22.1 | 3.2 | 5.1 | | 3.2 | 2.5 | 2.4 | |

**Scored:**

| | expectation | submissions | comments |
|---|---|---|---|
| A6-1 | RDM MAE ≤ A's | **FAIL** (0.040 against 0.024) | **PASS** (0.019 against 0.041) |
| A6-2 | neither arm beats carrying retention forward | **FAIL:** A beats it (0.024 against 0.037) | **PASS** |
| A6-3 | both forecast less than half the observed change of the head share | **PASS:** observed −0.152; A +0.020, RDM −0.013 | **PASS:** observed −0.064; A −0.010, RDM +0.029 |
| A6-4 | both arms' momentum within ±0.05 | **FAIL:** A −0.08, RDM +0.10 | RDM pass (+0.03), A fail (−0.26) |
| A6-5 | entrants kept against dropped change retention < 0.02 | **PASS:** identical | **PASS:** identical |

**Reading.**
- Out of period, the models forecast one-year retention to within 0.02–0.04
  at four cut-offs.
- The moving home is better than A on comments and worse on submissions.
  Carrying the last year's retention forward is as good or better on
  comments.
- Neither stationary model forecasts the fall of concentration at the head.
  That fall is the persistent decline of §A4/§A8, which neither model
  contains.
- The round-2 V3 concern (entrants dropped from simulations) does not matter
  here: no entrant reaches a top set within the window.


## A5c. The 2×2 around the corrected model: final

| arm (corrected variance where it applies) | home random walk | drift by rank | submissions | comments |
|---|---|---|---|---|
| A `home` | no | no | 0.188 ± 0.063 | 0.123 ± 0.051 |
| (b) `homedrift` | no | yes | **0.171 ± 0.035** | 0.114 ± 0.056 |
| (a′) `movinghome_vp_nodrift` | yes | no | 0.175 ± 0.072 | **0.100 ± 0.047** |
| RDM′ `movinghome_vp` (drift applied) | yes | yes | 0.183 ± 0.074 | 0.130 ± 0.054 |
| persistence | | | 0.163 ± 0.048 | 0.167 ± 0.039 |
| (frozen variance, for the record) `movinghome` / `movinghome_nodrift` | | | 0.246 / 0.267 | 0.145 / 0.150 |

**Scored:**
- _[CORRECTED 2026-10-18 by §2z-dh W6, restated 2026-10-19 after the round-5 referees: the comments spread is 0.13048 − 0.09994 = 0.0305 > 0.03, so A5c FAILS by 0.0005. The SI and the record score it so.]_
- **A5c PASS:** the spread across the four arms is 0.017 on submissions and
  0.030 on comments, the latter exactly at the margin.
- **A5b-1 restated for RDM′, PASS on both panels:** 0.183 ≤ 0.238 and
  0.130 ≤ 0.173.
- **A5b-3 PASS:** 0.183 < 0.246.
- RDM′ against A, paired: submissions 3/5, comments 2/5.
- Against persistence: comments 4/5 for every corrected arm. On submissions
  no arm's mean beats persistence.

**Reading.**
- With the variance bookkeeping corrected, the wandering home and drift by
  rank neither help nor hurt held-out movement at 1–13 weeks. The gate
  cannot see the lasting process. That was expected (A5-3) and is the
  referees' point.
- The model's claim to the main text is therefore structural:
  - it builds the hierarchy (A3);
  - it removes A's false reversion (§2z-dd);
  - it forecasts one-year retention out of period to within 0.02–0.04
    (A6).

  It is not a claim to better weekly forecasts. The paper says "at least as
  well as".

## R3-A / R3-B. Null worlds and rank change (`null_worlds.py`)

The expectations were written in PLAN.md (addendum R3) before any world ran.
They were committed at 845f71a, after the first world (Gibrat) had run; that
ordering is disclosed. The same estimators are used throughout.

| statistic | (i) Gibrat, size-blind | (ii) firm-like, β = 0.2 planted | (iii) neutral copying | (iv) data-calibrated model | data |
|---|---|---|---|---|---|
| turnover a year: top 10 / 25 / K/64 / K/16 / K/4 / K/2 | 0.33 / 0.27 / 0.23 / 0.24 / 0.25 / 0.22 | 0.17 / 0.13 / 0.13 / 0.19 / 0.23 / 0.24 | 0.03 / 0.08 / 0.15 / 0.30 / 0.31 / 0.30 | 0.30 / 0.21 / 0.25 / 0.19 / 0.16 / 0.14 | submissions 0.33 / 0.29 / 0.21 / 0.20 / 0.18 / 0.18 (A1, A7) |
| Eq. 1 within [0.8, 1.25] (median) | 100% (1.01) | 92% (1.00) | 100% (1.01) | 100% (0.96) | 100% (0.98) |
| β estimated | −0.005 | 0.114 (attenuated by passing noise) | 0.357 | 0.070 | 0.01–0.08 |
| gapped persistence, g 13 / 26 | −0.032 / −0.025 | −0.073 / −0.063 | −0.106 / −0.100 | −0.098 / −0.099 | +0.17 / +0.14 (submissions) |
| pooled boundary ratio, K/16 … K | 1.057 | 1.067 | **0.578** | 1.081 | 0.94 (joint, three ladders) |
| mean crossing gain c / slide s̄ / shape change, a week | 0.0053 / 0.0044 / +0.0006 | 0.0027 / 0.0048 / −0.0021 | 0.0044 / 0.0048 / −0.0011 | 0.0030 / 0.0037 / −0.0004 | 0.0040 / 0.0055 / −0.0016 |
| slide s̄_k > 0 at every cut-off | yes (min 0.0032) | yes | yes | yes | yes |
| corr(shape change, c − s̄) / corr(·, −s̄) | 0.18 / 0.25 | 0.80 / 0.06 | −0.26 / −0.82 | 0.80 / 0.83 | 0.90 / 0.97 |
| weekly rank change C(R) at R = 1 / 3 / 10 / 100 | 0.13 / 0.65 / 0.94 / 0.99 | 0.09 / 0.76 / 0.92 / 0.99 | 0 / 0 / 0.36 / 0.94 | 0.03 / 0.30 / 0.73 / 0.98 | submissions 0.07 / 0.36 / 0.79 / 0.99; comments 0.03 / 0.46 / 0.87 / 0.99; Facebook 0.52 / 0.74 / 0.94 / 1.00 |

**Scored:**
- **NW-1 PASS:** Eq. 1 holds in every world. It is kinematics, not
  evidence.
- _[CORRECTED 2026-10-19 by MODEL_STATUS §2z-dj (round-4 referee gpt-6-astra, verified): NW-2 had two rows. Row 1 (worlds i, ii, iv inside [0.85, 1.18]) PASSES; row 2 (the same in world iii) FAILS (0.578). The neutral world's ratio by cut-off is 0.76, 0.44, 0.54, 0.82, 1.04, 1.17, 0.12, so its pooled exclusion rests on the k = K cell.]_
- **NW-2 PASS:** the boundary ratio is about 1 for the level diffusions
  (1.06, 1.07); neutral copying gives 0.58, outside the band. The ratio
  discriminates against copying, not among level diffusions.
- **NW-3 PASS:** the slide is positive in every world, driftless Gibrat
  included. "Every community loses ground" is largely definitional.
- _[CORRECTED 2026-10-19 by §2z-dj: NW-4 required world ii in 0.12–0.28; 0.114 is outside it, so NW-4 FAILS as written.]_
- **NW-4 PASS:** Gibrat 0, firm-like 0.11, neutral 0.36, the
  data-calibrated model 0.070 (within ±0.05 of the data). The data's
  0.01–0.08 sits with Gibrat and the model. With planted β = 0.2 the estimator
  returns 0.11 under this passing noise, so the data's raw β also
  understates the lasting β somewhat.
- _[CORRECTED 2026-10-19 by §2z-dj: NW-5 required every world within ±0.05. Worlds ii, iii and iv give −0.07, −0.11 and −0.10, so NW-5 FAILS as written. The contrast with the data (+0.14 to +0.20) is unaffected.]_
- **NW-5 PASS:** no null world persists (−0.03 to −0.11); the data's
  +0.14 to +0.20 discriminates.
- **NW-6 PASS:** C(R) rises with R in every world and on every ladder, a
  stable top in Iñiguez's sense.
  - Top-10 set replacement exceeds top-K/2 replacement only in the
    size-blind world (0.33 against 0.22) and in the data.
  - The firm-like and neutral worlds protect the top (0.17 against 0.24;
    0.03 against 0.30).
- **NW-7 MIXED:** −s̄ carries the correlation in Gibrat and neutral, c − s̄
  in the firm-like world.

**The data-calibrated model (iv)** reproduces:
- the data's turnover profile, top fastest;
- β;
- the boundary ratio;
- C(R) at the top: 0.03 / 0.30 against submissions' 0.07 / 0.36;
- a mild flattening.

It lacks persistence (−0.10).

**Reading (what is automatic and what is discovered).**
- **Automatic in any random-growth ladder:**
  - Eq. 1;
  - a positive slide;
  - turnover replenishing at about σ²/(2S) (the boundary ratio about 1);
  - a top stable in rank order (C(R) rising).
- **Discovered, because the data side with the size-blind world:**
  - β about 0;
  - the top set turning over fastest. A firm-like β or neutral copying
    would make the top the most protected set;
  - the boundary ratio about 1, which excludes neutral copying.
- **Discovered, against every null world:**
  - persistence (+0.14 to +0.20);
  - a balance that flattens (c < s̄; shape −0.0016 a week), where the
    size-blind world steepens (+0.0006).
- **Rank change and set replacement are two views of one ladder.** The
  occupant of rank 1 rarely changes, because the gap to rank 2 is about S;
  yet a third of the top-ten set is replaced each year, because the slope
  is flattest at the top. This reconciles the data with Iñiguez et al.
  (2022): different statistics, the same kinematics.

## R3-D. Persistence, repaired (`persistence_r3.py`)

| | submissions, full membership | submissions, prefix membership | comments, full | comments, prefix |
|---|---|---|---|---|
| gapped, g 0 / 13 / 26 | +0.201 / +0.174 / +0.140 | +0.207 / +0.183 / +0.148 | +0.163 / +0.154 / +0.141 | +0.163 / +0.154 / +0.142 |
| true sign: share rising in change 1 | 0.64 | 0.62 | 0.66 | 0.65 |
| Spearman within risers / fallers | +0.031 / +0.177 | +0.027 / +0.189 | +0.003 / +0.158 | +0.002 / +0.161 |
| mean change 2: risers / fallers (all) | −0.140 / −0.331 (−0.209) | −0.156 / −0.374 (−0.238) | +0.042 / −0.163 (−0.028) | +0.044 / −0.160 (−0.027) |
| mean rank of change 2 by change-1 quintile | 0.41, 0.49, 0.52, 0.53, 0.54 | 0.40, 0.50, 0.52, 0.53, 0.54 | 0.41, 0.49, 0.53, 0.55, 0.52 | 0.41, 0.49, 0.53, 0.55, 0.52 |
| matched origins (≥ 52), g 0 / 13 / 26: raw | +0.188 / +0.149 / +0.112 | +0.194 / +0.157 / +0.118 | +0.175 / +0.158 / +0.126 | +0.173 / +0.157 / +0.127 |
| net of cohort-mean loading | +0.149 / +0.054 / +0.006 | +0.159 / +0.112 / +0.102 | +0.168 / +0.161 / +0.130 | +0.167 / +0.159 / +0.130 |
| net of platform-total loading | +0.132 / +0.020 / −0.029 | +0.167 / +0.063 / +0.010 | +0.191 / +0.177 / +0.141 | +0.178 / +0.166 / +0.135 |

**Scored:**
- **D-1 PASS:** prefix-only membership changes the gapped statistic by
  ≤ 0.008.
- **D-2 PARTLY:**
  - fallers persist more than risers on both ladders (+0.16 to +0.19
    against 0.00 to +0.03);
  - the conditional means are on the same side as change 1 on comments
    (+0.04 / −0.16);
  - on submissions everyone fell in change 2 and risers fell less
    (−0.14 against −0.33). The persistence there is relative.

**Reading.**
- The persistence is concentrated in declines on both ladders. The bottom
  quintile's mean rank of change 2 is 0.40–0.41, the top's 0.52–0.54.
- **Submissions: the shared-trend reading holds with the platform-total
  factor** under both membership rules (+0.01 to +0.02 at a half-year gap,
  and −0.03 with full membership).
  - With the cohort-mean factor it depends on membership: +0.006 with full
    membership, +0.102 with prefix membership.
  - The paper quotes the platform-total factor and states the dependence.
- **Comments:** neither factor removes it (+0.13 to +0.14); it is
  community-specific.

## R3-C. One model: A6 with the corrected moving home (`structure_oop.py … home,movinghome_vp`)

**Submissions:**
- **retention MAE:** A 0.024; RDM′ (corrected, drift applied) **0.028**;
  frozen 0.040; carried forward 0.037;
- **share of the head in 2022:** RDM′ 0.537 (0.48–0.72), A 0.528, against
  0.356 observed;
- **momentum:** RDM′ +0.026; **kurtosis:** RDM′ 2.9, against 22.1 in the
  data.

**C-1** (RDM′ within 0.01 of the frozen MAE): FAIL by 0.002. RDM′ is better
(0.028 against 0.040), so the frozen form's A6 result understated the model.
Comments are pending.

## R3-E. Gomez with the general displacement term (`gomez_r3.py`)

**Implied displacement** = ½ν²·(a_k/ā_top)/S_k, valid at any slope.

| cut-off | measured between / implied, by year pair (current-year cohort) |
|---|---|
| submissions 78 | 0.67, 1.07, 0.82 |
| submissions 312 | 2.17, 2.51, 1.70 |
| submissions 1,250 | 1.71, 1.88, 1.26 |
| comments 195 | 1.21, 1.49, 1.17 |
| comments 781 | 1.55, 1.49, 1.12 |
| comments 3,125 | 3.30, 2.52, 1.67 |

**Scored:**
- **E-1 PASS:** where ζ > 1 (the heads, S < 1) the ratio is 0.67–1.49,
  within ×2.5. Deeper it runs to 3.3, measured above implied: plausibly
  passing swings, which are not removed in this block-level accounting.
- **Permanent-rank cohorts** leave the within term essentially unchanged.
  - Submissions top 78: −0.074, −0.227, −0.256 against −0.061, −0.237,
    −0.259.
  - Comments top 195: −0.126, −0.138, −0.099 against −0.125, −0.150,
    −0.123.
  - The members' decline is not an artefact of selecting each year's
    leaders. The between term of a permanent cohort is larger (it is not
    the year's top set).

**Reading.** A8-2's failure was the misapplied Pareto form. With the general
formula, Gomez's displacement prediction holds at the head of both ladders,
within a factor of 1.5.

**R3-C, comments:**
- retention MAE: A 0.041; RDM′ **0.022**; frozen 0.019; carried forward
  0.013;
- head share in 2022: RDM′ 0.597 against 0.505 observed;
- **C-1 PASS** on comments (within 0.003 of frozen). On submissions it is
  0.012, with RDM′ better.

**R3-C, the flat start at 20 seeds** (`churn_counterfactual.py … flat20`):

| ladder | slopes after 30 years, K/64 … K/2 | 90% band | observed |
|---|---|---|---|
| comments | 0.55, 0.65, 0.83, 1.13, 1.53, 1.97 | 0.50–0.65, 0.60–0.72, 0.78–0.88, 1.08–1.17, 1.49–1.57, 1.91–2.01 | 0.99, 1.21, 1.32, 1.38, 1.74, 2.13 |
| submissions | 0.55, 0.67, 0.80, 0.96, 1.14, 1.37 | 0.35–0.86, 0.56–0.76, 0.72–0.95, 0.88–1.01, 1.08–1.22, 1.31–1.43 | 0.85, 0.93, 1.02, 1.26, 1.32, 1.55 |

- **C-2 PASS:** within 4% of the 10-seed values.
- The observed slope lies outside the band at every cut-off except the
  submissions head (0.85 against an upper band of 0.86).

## R4. Persistence after the 2020 surge (`persistence_post2020.py`; post hoc; expectations in PLAN.md addendum R4)

Gapped Spearman, mean over origins (number of origins in brackets). The
cohort is the top K by mean level over weeks 0–25. Post-surge origins start in
the week of 6 July 2020 (index 84 on comments, 83 on submissions).

| | W = 13, gaps 0 / 13 / 26 (all origins) | W = 4, gaps 4 / 8 / 13 (all origins) | W = 4, gaps 4 / 8 / 13 (post-surge) |
|---|---|---|---|
| submissions, full membership | +0.201 / +0.174 / +0.140 (5, 4, 3) | +0.187 / +0.183 / +0.170 (18, 17, 16) | +0.150 / +0.157 / +0.153 (4, 3, 2) |
| submissions, prefix membership | +0.207 / +0.183 / +0.148 | +0.189 / +0.186 / +0.173 | +0.145 / +0.156 / +0.148 |
| comments, full membership | +0.163 / +0.154 / +0.141 | +0.137 / +0.139 / +0.134 | +0.133 / +0.137 / +0.136 |
| comments, prefix membership | +0.163 / +0.154 / +0.142 | +0.139 / +0.140 / +0.134 | +0.134 / +0.137 / +0.134 |
| world: size-blind Gibrat | −0.044 / −0.007 / −0.003 | −0.055 / −0.034 / −0.017 | −0.047 / −0.031 / −0.024 |
| world: data-calibrated model | −0.106 / −0.098 / −0.099 | −0.096 / −0.092 / −0.089 | −0.081 / −0.079 / −0.070 |

The post-surge triples are consistent at every origin:
- **submissions (prefix):** gap 8, +0.153, +0.157, +0.159; gap 13, +0.138,
  +0.157;
- **comments:** gap 8, +0.132, +0.142, +0.136; gap 13, +0.127, +0.141.

About 4,600 and 11,200 communities enter each correlation.

**Scored:**

| | expectation | result |
|---|---|---|
| R4-1 | W = 4, all origins, gap 13: positive and ≥ half the W = 13 value | **PASS** (submissions 0.170 against 0.174; comments 0.134 against 0.154) |
| R4-2 | W = 4, post-surge: positive at gaps 8 and 13 on both ladders | **PASS** (+0.153 to +0.157; +0.136 to +0.137) |
| R4-2b | the same, ≥ +0.05 | **PASS** |
| R4-3 | both worlds ≤ +0.02 at gaps 8 and 13 with W = 4 | **PASS** (−0.017 to −0.092) |

**Reading.**
- Annual persistence is present in triples that begin after the 2020 surge.
  It is about 0.02 lower on submissions and unchanged on comments, so it is
  not an artefact of the surge.
- The weak half-year result from December 2020 (−0.04 to +0.08) is therefore
  a matter of horizon, not of period.
- **Caveat:** the post-surge triples straddle the July 2021 change in archive
  capture: change 1 ends in July 2021, and change 2 lies after it. This check
  does not separate persistence from that change. The earlier evidence that
  persistence predates the change (§2z-de D1) uses adjacent blocks, which
  averaging inflates. A gapped check on triples that end before July 2021 is
  R4b.
- The sealed 2023–2024 test (P5 of the draft protocol) is unchanged and
  remains the out-of-sample test.

### R4b. Triples that end before the July 2021 capture change (`persistence_post2020.py … pre`; post hoc)

Setup:
- 4-week windows and annual changes.
- The cohort is the top K over weeks 0–12; origins are weeks 13 and 17.
- Every week used is before 1 July 2021 (index 136 on comments, 135 on
  submissions).

| | gap 4 (origins 13, 17) | gap 8 (origin 13) |
|---|---|---|
| submissions, full membership | +0.104, +0.103 | +0.114 |
| submissions, prefix membership | +0.130, +0.129 | +0.137 |
| comments, full membership | +0.109, +0.038 | +0.108 |
| comments, prefix membership | +0.110, +0.040 | +0.110 |
| world: size-blind Gibrat | mean −0.057 | −0.051 |
| world: data-calibrated model | mean −0.163 | −0.147 |

About 4,800–5,000 and 11,700–12,200 communities enter each correlation.
Prefix membership here uses weeks 0–25, which overlap the first window. Full
membership uses the whole panel. Neither enters the cohort, which uses weeks
0–12.

**Scored:**
- **R4-4: PASS.** Both ladders are positive and at least +0.05 at both gaps,
  as means. One comments origin at gap 4 gives +0.04.
- **R4-5: PASS** (−0.05 to −0.16).

**Reading (R4 and R4b together).**
- Gapped annual persistence appears in triples that end before the capture
  change: +0.07 to +0.14. These contain the 2020 surge.
- It also appears in triples that begin after the surge: +0.13 to +0.16.
  These contain the capture change.
- No simulated world gives a positive value.
- Neither confounder alone accounts for it. No development triple excludes
  both; the sealed 2023–2024 data do (P5).

## R5. Who the head is, and who fell (`head_named.py`; named, descriptive; owner permission §2z-dh §10)

Blocks are 52 weeks, labelled 2019–2022. Shares are of the year's top-K
total.

| | submissions (head = top 78) | comments (head = top 195) |
|---|---|---|
| rank-1 occupant | r/memes 196 of 212 weeks (r/dankmemes 10, r/funny 2, r/place 2); 14 changes | r/AskReddit 209 of 213 weeks (r/politics 2, r/wallstreetbets 2); 6 changes |
| head share by year (that year's head) | 0.502, 0.498, 0.418, 0.340 | 0.610, 0.566, 0.526, 0.497 |
| the 2019 head's share in 2022 (same names) | 0.250 | 0.402 |
| share of the incumbents' loss replaced by the 2022 head's newcomers | 0.36 (31 entrants) | 0.46 (81 entrants) |
| share of 2019 head members whose share fell | 0.74 | 0.77 |
| the ten largest losers' part of the incumbents' summed loss | 0.66 | 0.44 |
| largest losers (2019 → 2022 share) | memes 0.070 → 0.018; dankmemes 0.056 → 0.008; PewdiepieSubmissions 0.027 → 0.000; aww 0.021 → 0.007; funny 0.020 → 0.007 | AskReddit 0.113 → 0.049; politics 0.019 → 0.008; dankmemes 0.010 → 0.002; The_Donald 0.007 → 0 |
| largest gainers | MadeMeSmile 0.003 → 0.012; nextfuckinglevel; Unexpected; Damnthatsinteresting | AmItheAsshole 0.017 → 0.033; facepalm; mildlyinfuriating; interestingasfuck |
| 2019 head members no longer in the top K by 2022 | The_Donald, ChapoTrapHouse (both banned by Reddit in June 2020), Cringetopia | The_Donald, ChapoTrapHouse, IncelTears, MGTOW |
| entrants to the 2022 head, examples (2019 rank; −1 = outside the 2019 top K) | shitposting (4,787), 196 (−1), Superstonk (−1), antiwork (1,326), ukraine (−1), Eldenring (3,020) | antiwork (1,079), Superstonk (−1), Eldenring (2,542), ukraine (6,631) |

**Scored:**

| | expectation | result |
|---|---|---|
| R5-1 | comments' rank-1 occupant is one community in ≥ 95% of weeks | **PASS** (98.1%) |
| R5-1b | the same for submissions, ≥ 90% | **PASS** (92.5%) |
| R5-2 | more than half of the 2019 submissions head lose share by 2022 | **PASS** (74%) |
| R5-3 | broad decline: the ten largest losers carry less than 60% of the submissions head's loss | **FAIL** (66%). The decline is broad in incidence: three quarters of members fell. In size it is concentrated at the very top: r/memes and r/dankmemes alone account for half. Comments, not registered: 44% |

**Reading.**
- **The thesis in names.** The 2019 leaders of submissions lost half their
  share (50.2% → 25.0%). Newcomers restored about a third of the loss, so
  the 2022 head holds 34.0%. On comments the 2019 leaders fell from 61.0% to
  40.2%, and newcomers restored about half (head 49.7%).
- **The leaders are the least secure in share, not in rank.** r/memes held
  rank 1 in 92% of weeks while its share fell by three quarters. r/AskReddit
  held it in 98% while its share fell by more than half.
- **Some departures are administrative.** Two 2019 head members on both
  ladders were banned by Reddit in June 2020.
- **Some entry is tied to events.** Examples are Superstonk (2021), ukraine
  (2022) and Eldenring (a 2022 game release).

## R6. Long-run counterfactuals from the observed ladder (`churn_counterfactual.py <panel> long30`; expectations in PLAN.md addendum R6)

The model's lasting core as in A3, from the observed end-2019 ladder. 30
years, 20 seeds. Head shape = U_{K/64} − U_K in log units. Bands are 5–95%
across seeds.

| | world | head shape at 0 / 10 / 20 / 30 years | change in 30 years | local slopes at 30 years, K/64 … K/2 |
|---|---|---|---|---|
| submissions | churn + slide | 4.51 / 3.93 / 3.98 / 3.70 (3.57–3.82) | −18% | 0.58, 0.74, 0.92, 1.07, 1.30, 1.58 |
| | slide only (σ0 = 0) | 4.51 / 2.38 / 0.92 / 0.47 (0.45–0.48) | −90% | 0.06, 0.07, 0.10, 0.15, 0.19, 0.28 |
| | churn only (no drift) | 4.51 / 5.41 / 6.49 / 7.51 (7.26–7.67) | +67% | 1.30, 1.50, 1.65, 1.87, 2.52, 3.60 |
| comments | churn + slide | 5.36 / 4.77 / 4.38 / 4.15 (4.08–4.24) | −23% | 0.59, 0.68, 0.88, 1.25, 1.65, 2.12 |
| | slide only | 5.36 / 5.12 / 3.77 / 2.08 (2.08–2.09) | −61% | 0, 0, 0, 2.15, 1.06, 0.09 (the head collapses into a flat top above a cliff) |
| | churn only | 5.36 / 5.87 / 6.34 / 6.86 (6.77–6.93) | +28% | 0.98, 1.06, 1.36, 1.85, 2.70, 3.85 |

The observed starting slopes are 0.85 … 1.55 (submissions) and 0.99 … 2.13
(comments).

**Scored:**

| | expectation | result |
|---|---|---|
| CF-1 | slide only: the head shape loses at least half in 30 years, both ladders | **PASS** (−90%, −61%) |
| CF-2 | churn only: the head shape grows by more than 50% and is still growing in the last decade, both ladders | **FAIL on comments** (+28%; still growing, +0.52 in the last decade against +0.51 in the first). **PASS on submissions** (+67%; +1.02 against +0.90) |
| CF-3 | churn + slide: the 30-year slopes are within 15% of the flat start's | **PASS on comments** (ratios 1.05–1.11). **FAIL on submissions at K/2** (1.157; 1.05–1.15 elsewhere) |

**Reading.**
- With the measured ingredients, volatility alone steepens the hierarchy
  without limit, the slide alone collapses it over decades, and together
  they hold it.
- From equality the hierarchy rises to its balance. From the observed
  ladder, which is steeper, it falls toward it. After 30 years the two runs
  bracket the stationary slope within 5–16%.
- The two-year counterfactuals (A3) could not show this. On comments the
  slide-only head barely moves in the first decade (−0.23 in log units) and
  falls by −1.68 in the third.
- The answer to "where does the hierarchy come from" is therefore a balance
  that operates over decades. Membership turns over in years.

## R7. Null worlds at 20 seeds, calibrated worlds, β calibration, Gomez noise baseline (`null_worlds_r7.py`, `summarize_r7.py` → `null_worlds_r7_summary.json`; expectations in PLAN.md addendum R7)

Means over 20 runs, with 5–95% in brackets.
- **Constant-slope worlds** start from a Zipf ladder.
- **Calibrated worlds** use the model estimated on Reddit submissions,
  started from the data's filtered ladder with its passing parts and exits,
  with a planted β and no drift by rank.
- **Disclosure:** within-seed bootstraps are cut to 5 replications.
- **Not yet run when this section was written:** the model world (rdm, 20
  seeds) and the β = 0.05 and 0.10 calibration worlds. They had been queued
  under a memory constraint (the machine was swapping) and are reported
  below when done.

| world | top-10 turnover | K/64 | K/16 | K/4 | K/2 | raw β | gapped persistence (13 wk) | pooled boundary ratio | c/s̄ (share of seeds with c < s̄) | C(1), C(10) |
|---|---|---|---|---|---|---|---|---|---|---|
| size-blind, constant slope | 0.243 [0.133, 0.367] | 0.249 | 0.244 | 0.241 | 0.232 | −0.002 [−0.018, 0.021] | −0.021 | 1.09 [1.03, 1.15] | 1.11 [0.87, 1.35] (35%) | 0.27, 0.91 |
| firm-like (β 0.2), constant slope | 0.127 [0.065, 0.167] | 0.145 | 0.184 | 0.232 | 0.246 | 0.129 [0.112, 0.151] | −0.069 | 1.12 [1.05, 1.19] | 0.51 [0.43, 0.56] (100%) | 0.27, 0.91 |
| neutral copying | 0.043 [0.000, 0.100] | 0.157 | 0.287 | 0.307 | 0.299 | 0.358 | −0.104 | 0.69 [0.58, 0.79] | 0.82 [0.75, 0.91] (100%) | 0.00, 0.15 |
| size-blind, data's ladder (planted β 0) | 0.205 [0.100, 0.333] | 0.211 | 0.192 | 0.165 | 0.137 | 0.062 [0.043, 0.084] | −0.071 | 1.10 | 1.21 (5%) | 0.09 |
| firm-like, data's ladder (planted β 0.2) | 0.192 [0.130, 0.268] | 0.195 | 0.181 | 0.164 | 0.138 | 0.094 [0.077, 0.107] | −0.103 | 1.04 [0.97, 1.16] | 0.62 [0.52, 0.74] (100%) | 0.12 |
| data, Reddit submissions | 0.33 | 0.209 | 0.204 | 0.181 | 0.178 | 0.045, 0.082 | +0.174 | 0.80 | 0.72 (pooled cells) | 0.07, 0.79 |

Neutral copying's boundary ratio by cut-off (K/64 … K) is 0.74, 0.64, 0.68,
0.85, 1.08, 1.24, 0.22. Its pooled exclusion comes from the edge at K.

**Gomez noise baseline (head, k = K/64, all year pairs).**
- Size-blind world on the data's ladder:
  - within term with the current-year cohort −0.053 (−0.042 in pairs
    where the share changes by less than 5%);
  - with the prior-year cohort −0.023.
- Constant-slope size-blind world: −0.018.
- **Data (`gomez_r7_prior.py`), submissions head:**
  - current-year cohort −0.061 (2019–20), −0.237 and −0.259 (2020–22);
  - prior-year cohort −0.253 and −0.307 (2020–22).
- **Data, comments head:**
  - current-year cohort −0.150 and −0.123;
  - prior-year cohort −0.151 and −0.140.

**Scored:**

| | expectation | result |
|---|---|---|
| R7-1 | constant-slope size-blind: the top-10 band lies above the K/2 mean. Firm-like and neutral: top-10 below K/2 | **FAIL** for size-blind: the band [0.13, 0.37] contains 0.23, and the means are 0.24 against 0.23. The single seed's 0.33 (R3) was luck. **PASS** for firm-like (0.13 < 0.25) and neutral (0.04 < 0.30) |
| R7-2 | calibrated size-blind: top-10 above K/2, bands apart | **FAIL** (means 0.21 against 0.14; the top-10 band reaches down to 0.10) |
| R7-2b | calibrated size-blind turnover within ±0.05 of submissions at K/64 … K/2 | **PASS** (0.211/0.209, 0.192/0.204, 0.165/0.181, 0.137/0.178). It under-predicts the top ten (0.21 against 0.33, the band's upper end) |
| R7-3 | calibrated firm-like top-10 at least 0.05 below calibrated size-blind | **FAIL** (0.192 against 0.205) |
| R7-4 | β estimator: estimated rises with planted; planted 0.2 → 0.10–0.16; the data's raw maps below 0.12 | **FAIL as written.** Planted 0 → 0.062 and planted 0.2 → 0.094: the calibration line is nearly flat, and the data's raw 0.045–0.082 cannot be placed on it. Monotonicity awaits β 0.05/0.10 |
| R7-5 | c < s̄ in ≥ 2 of 3 non-Gibrat R3 worlds in ≥ 80% of seeds | **PASS** (firm-like and neutral 100%; the model world pending) |
| R7-6 | calibrated size-blind current-cohort head within term in −0.03 … −0.10 | **PASS** (−0.053) |
| R7-6b | data prior-year cohort: submissions head within term below −0.15 in 2020→21 and 2021→22 | **PASS** (−0.253, −0.307) |

**Reading.**
- **The top turns over fastest because the head is flattest** (Eq. 1). On the
  data's ladder, size-blind and firm-like volatility give the same profile
  within noise. "Size-blindness exposes the top" is withdrawn.
- **The raw size exponent is dominated by passing parts that differ by
  rank,** so it cannot place Reddit against firms. The lasting exponent from
  the structure function, which recovers a planted β within ±0.03 at
  T ≥ 160 (`runs/2026-10-18_rdm/recovery_output.txt`), is the comparison:
  - submissions 0.045;
  - comments 0.126 on the whole panel and 0.056–0.092 on gate windows;
  - Facebook 0.11, biased up by about 0.045 at 86 weeks; a two-step lasting
    estimate gives 0.02.

  This is weaker than firms' 0.15–0.2, but not size-blind.
- **Flattening (c < s̄) is not a discovery.** It appears in every world that
  starts steeper than its balance.
- **Persistence is discovered against every world** (−0.02 to −0.10, tight
  bands).
- **The head's within term in 2019–20 (−0.06) is the size expected from
  selection on noise,** which the size-blind calibrated world puts at −0.05.
  The 2020–22 decline (−0.24 to −0.31, also with prior-year cohorts) is five
  times that.

## R8. What fell at the head: posting or engagement per post (`head_components.py`; expectations in PLAN.md addendum R8)

The head's karma share is split exactly: share_karma = share_items × relative
karma per item. Shares are of the year's top-K by karma; blocks and universe
are those of R5.

| | 2019 | 2020 | 2021 | 2022 | Δlog 2019 → 2022: karma = items + per item |
|---|---|---|---|---|---|
| submissions head (re-selected each year): karma share | 0.502 | 0.498 | 0.418 | 0.340 | |
| … its share of submissions (items) | 0.224 | 0.191 | 0.134 | 0.087 | |
| … relative karma per submission | 2.25 | 2.61 | 3.12 | 3.92 | −0.390 = −0.945 + 0.556 |
| submissions, the 2019 head (fixed names): karma, items, per item | 0.502, 0.224, 2.25 | 0.472, 0.176, 2.69 | 0.352, 0.103, 3.42 | 0.250, 0.073, 3.44 | −0.697 = −1.122 + 0.425 |
| comments head (re-selected): karma, items, per item | 0.610, 0.413, 1.48 | 0.566, 0.360, 1.57 | 0.526, 0.338, 1.56 | 0.497, 0.309, 1.61 | −0.205 = −0.292 + 0.087 |
| comments, the 2019 head (fixed names) | 0.610, 0.413, 1.48 | 0.534, 0.338, 1.58 | 0.453, 0.274, 1.65 | 0.402, 0.239, 1.68 | −0.419 = −0.547 + 0.128 |

**Scored:**

| | expectation | result |
|---|---|---|
| R8-1 | 2019 submissions head: the per-item change is larger in magnitude than the item-share change | **FAIL.** Items −1.12, per item +0.43, in the opposite direction |
| R8-2 | re-selected submissions head: item share changes by less than 20% | **FAIL** (−61%: 22.4% to 8.7%) |
| R8-3 | comments: the karma-share fall is mostly in item share | **PASS** (−0.29 of −0.21; per item +0.09) |

**Reading.**
- **The head lost participation, not engagement per item.** Its share of
  posts and comments fell steeply, and karma per item relative to the top K
  rose. On submissions the head's share of posts fell from 22% to 9%.
- **The June 2021 step in votes per post** (§2z-ca) is a short-run movement
  inside a three-year rise of relative karma per post.
- **The abstract's causal clause is replaced by a descriptive one.** The
  head's fall came through participation, the component that carries
  persistence (§2z-dg A4). The analysis does not show that the communities
  whose declines persist are the head's decliners. Opus R4's reading (votes
  per post) is corrected by this split.

### R7, model world at 20 seeds (completed after the section above)

| world | top-10 | K/64 | K/16 | K/4 | K/2 | raw β | gapped (13 wk) | boundary | c/s̄ (c < s̄) | C(1) |
|---|---|---|---|---|---|---|---|---|---|---|
| the model (hold closure; its own β 0.045) | 0.228 [0.132, 0.302] | 0.200 | 0.204 | 0.166 | 0.140 | 0.067 [0.044, 0.087] | −0.098 | 1.11 [1.01, 1.17] | 0.75 [0.65, 0.88] (100%) | 0.10 |

- **R7-5: PASS in full.** c < s̄ in 100% of seeds in all three non-Gibrat
  R3 worlds.
- The R3 single run's top-10 turnover of 0.30 was at the band's upper end.
- The β = 0.05 and 0.10 calibration worlds are still running. They are
  reported when done.

### R7, β calibration complete (worlds β 0.05 and 0.10, 5 seeds each)

| planted lasting β (data-calibrated world, no drift) | 0 (20 seeds) | 0.05 (5) | 0.10 (5) | 0.20 (20) |
|---|---|---|---|---|
| raw β, the paper's estimator | 0.062 [0.043, 0.084] | 0.072 [0.055, 0.082] | 0.078 [0.060, 0.097] | 0.094 [0.077, 0.107] |
| top-10 / K/2 turnover | 0.205 / 0.137 | 0.167 / 0.138 | 0.207 / 0.137 | 0.192 / 0.138 |

- **R7-4, monotonicity:** PASS. The raw estimate rises with the planted
  value, at a slope of about 0.16. The rest of R7-4 fails as scored above.
- On this line the data's raw submissions values (0.045, 0.082) map to a
  planted β from below 0 to about 0.13. The raw estimator cannot separate
  Reddit from firms.
- The turnover profile does not depend on planted β within the seed bands.

## R9. Geometry isolated (`null_worlds_r7.py sb_flat|firm_flat`; expectations in PLAN.md addendum R9)

The calibrated dynamics (submissions model, no drift, the same passing
parts, exits and seeds) are held fixed. Only the starting ladder changes: the
data's, or a constant-slope top K (slope 1.07, the same levels at ranks 1 and
K). 20 seeds each.

| world | top 10 | top 25 | K/64 | K/16 | K/4 | K/2 | raw β |
|---|---|---|---|---|---|---|---|
| size-blind, data's ladder | 0.205 | 0.243 | 0.211 | 0.192 | 0.165 | 0.137 | 0.062 |
| size-blind, constant slope | 0.158 | 0.208 | 0.190 | 0.207 | 0.213 | 0.177 | 0.068 |
| firm-like, data's ladder | 0.192 | 0.196 | 0.195 | 0.181 | 0.164 | 0.138 | 0.094 |
| firm-like, constant slope | 0.127 | 0.169 | 0.170 | 0.199 | 0.218 | 0.183 | 0.103 |

**Paired over seeds, data's ladder minus constant slope:**
- (top 10 − K/2): size-blind +0.087 [−0.072, 0.203]; firm-like +0.110
  [0.008, 0.249];
- (K/64 − K/2): size-blind **+0.061 [0.031, 0.091]**; firm-like **+0.070
  [0.022, 0.115]**.

**Scored:**
- **R9-1: PASS.** Constant slope, size-blind: top 10 − K/2 = −0.019.
- **R9-2: PASS** as a mean (+0.087), with a wide band. The K/64 contrast is
  robust (band excludes 0).
- **R9-3: PASS**, at the margin: firm-like 0.127 against size-blind 0.158
  on a constant slope, a difference of 0.031.

**Reading.**
- With identical dynamics, the data's ladder makes the head turn over faster
  than the body (K/64 against K/2: +0.06 to +0.07). A constant-slope ladder
  does not.
- Size dependence lowers top-10 turnover by about 0.03 on a constant slope
  and by about 0.01 on the data's ladder.
- The top-10 point estimates are noisy (30 name-years): the top ten turn
  over no faster than the top 25 in any calibrated world.
- The data's top ten (0.33) are at or above every calibrated world's 95th
  percentile. The data's K/2 (0.178) is above the data-ladder worlds' bands
  (about 0.13–0.14). The worlds therefore over-predict the fall of turnover
  from K/64 to K/2 and under-predict the very top.

### Correction (2026-10-19, round-5 referees)
- R5's reading said that r/memes and r/dankmemes "alone account for half"
  of the loss. Their combined loss, 0.100, is 34% of the gross incumbent
  loss (0.299) and 40% of the net loss (0.252), not half.
- R7's prior-year Gomez terms in the simulated worlds are unreliable. A
  prior-year cohort can contain names that exited or were reborn, with
  near-zero base activity (the model world gives +231,412). Only the
  current-year baseline (−0.053, size-blind world) is used in the paper.
- The prior-year decomposition in the data is not exact: within plus
  between is −0.114 against a share change of −0.161, because the base is
  the prior cohort's activity. This is now stated.

## R10. The slide by persistence class (`slide_by_persistence.py`; post hoc; expectations in PLAN.md addendum R10)

**Design.** Cells are start week × cut-off (K/64, K/16, K/4): 9 cells per
ladder in the data, and 45 in the worlds (5 seeds). Slides are per week.
- **Q1:** the bottom fifth of the prior change, measured in 13-week windows
  a year apart that end 13 weeks before the slide's start.
- **Q5:** the top fifth.
- **rest:** everything but Q1.

| | slide | Q1 | Q5 | rest | contrast Q1 − rest (share of cells > 0) |
|---|---|---|---|---|---|
| submissions, data | 0.0056 | 0.0077 | 0.0068 | 0.0050 | +0.0027 (56%) |
| submissions, driftless calibrated world | 0.0019 | 0.0004 | 0.0038 | 0.0023 | −0.0019 (18%) |
| submissions, model world (drift by rank, hold) | 0.0034 | 0.0010 | 0.0052 | 0.0040 | −0.0030 (0%) |
| comments, data | 0.0036 | 0.0048 | 0.0042 | 0.0025 | +0.0023 (100%) |
| comments, driftless calibrated world | 0.0011 | −0.0010 | 0.0049 | 0.0016 | −0.0026 (11%) |

**By cut-off (data):**

| ladder | k | slide | Q1 | rest |
|---|---|---|---|---|
| submissions | 78 | 0.0082 | 0.0116 | 0.0074 |
| submissions | 312 | 0.0053 | 0.0063 | 0.0048 |
| submissions | 1,250 | 0.0034 | 0.0052 | 0.0028 |
| comments | 195 | 0.0045 | 0.0053 | 0.0039 |
| comments | 781 | 0.0033 | 0.0044 | 0.0020 |
| comments | 3,125 | 0.0029 | 0.0047 | 0.0017 |

**Scored:**

| | expectation | result |
|---|---|---|
| R10-1 | data contrast > 0 in ≥ 80% of cells on both ladders | **FAIL on submissions** (5 of 9). **PASS on comments** (9 of 9) |
| R10-2 | driftless worlds: mean contrast ≤ 0.0005 | **PASS** (−0.0019, −0.0026; the model world −0.0030) |
| R10-3 | 0.2 × (s̄_Q1 − s̄_rest) in the data ≥ a quarter of the excess over the driftless world | **FAIL** (0.15 submissions, 0.19 comments; 0.24 against the model world) |
| R10-4 | the data's slide exceeds the driftless world's on both ladders | **PASS** (0.0056 against 0.0019; 0.0036 against 0.0011) |

**Reading.**
- **Prior decliners rebound in every world without persistence.** Passing
  losses reverse, so their slide is the lowest of any class. In the data
  they keep falling, so their slide is the highest.
- **Post hoc accounting** of the excess by class, comparing each class
  with the same class in the world (0.2 × (Q1_data − Q1_world) as a share of
  the cohort's excess):
  - against the driftless world: 39% on submissions, 47% on comments;
  - against the model with drift by rank: 60% on submissions.

  The bottom fifth of prior change carries 40–60% of the slide's excess
  over random growth from the same ladder.
- **R10-3 as registered** measures the contrast within the data and so
  understates this. The rest of the cohort also slides faster in the data
  than in the worlds.
- **What it means.** The hierarchy's measured decline beyond random growth
  is carried disproportionately by the communities whose decline persists.
  This unifies the mechanism and the discovery: random growth sets the shape,
  and persistent decline moves it faster.
- **Limits.**
  - Few cells: 3 starts per cut-off.
  - The submissions contrast is positive in only 5 of 9 cells.
  - The model world is calibrated to submissions only.
  - Post hoc.

## R11. Entry and exit counted once in the model's drift (`churn_counterfactual.py <panel> flat20|long30 noentry`; expectations in PLAN.md addendum R11, committed 50e36aa before the outputs were read)

**Run.** The same runs as before, 20 seeds each, 30 years:
- the flat start;
- the three worlds from the observed 2019 ladder.

The only change is the drift by rank. It is now built from the persisters' slide s̄ alone, not s̄ − e, because the simulator produces its own entry and exit.

**Slope at 30 years from the flat start.** Each cell gives s̄ − e → s̄, then the observed slope. Cut-offs: K/64, K/32, K/16, K/8, K/4, K/2.

| | K/64 | K/32 | K/16 | K/8 | K/4 | K/2 |
|---|---|---|---|---|---|---|
| comments | 0.553 → 0.605 (obs 0.991) | 0.647 → 0.708 (1.207) | 0.826 → 0.949 (1.321) | 1.126 → 1.340 (1.377) | 1.531 → 1.783 (1.744) | 1.966 → 2.176 (2.128) |
| submissions | 0.547 → 0.542 (0.854) | 0.667 → 0.719 (0.932) | 0.804 → 0.863 (1.024) | 0.958 → 0.968 (1.257) | 1.137 → 1.205 (1.320) | 1.366 → 1.406 (1.550) |

**Shortfall against the observed slope, 1 − S/S_obs, after the correction:**
- **Comments:** 0.39, 0.41, 0.28, 0.03, −0.02, −0.02. Before: 0.44, 0.46, 0.38, 0.18, 0.12, 0.08. The observed slope lies inside the 90% band of 20 runs at K/8, K/4 and K/2.
- **Submissions:** 0.37, 0.23, 0.16, 0.23, 0.09, 0.09. Before: 0.36, 0.28, 0.22, 0.24, 0.14, 0.12. The observed slope is outside the band at every cut-off.

**Thirty years from the observed ladder.** Change in head shape (K/64) over 30 years, before → after:

| | volatility and slide | slide only (exits and entry kept) | volatility only |
|---|---|---|---|
| comments | −22% → −9% | −61% → −31% | +28% → +28% (no drift involved) |
| submissions | −18% → −17% | −90% → −90% | +67% → +67% |

- **Comments, slide only:** the head still collapses to zero slope above rank 1,560, in two tiers. The shape measured at K/64 falls less because the cliff below the head holds it up. In the first two years without volatility, the comments head rises (+0.034 a year).
- **The two starts:** the slopes after 30 years from the observed ladder agree with the flat start's within 2–16% at every cut-off.

**Scores.**

| | expectation | result |
|---|---|---|
| R11-1 | flat start: K/64 slope ≥ 15% steeper on comments, ≥ 5% on submissions | **FAIL** (+9% on comments, −1% on submissions; deeper on comments +15% to +19%) |
| R11-2 | the K/64 shortfall shrinks on both ladders | **FAIL as written** (comments 0.44 → 0.39; submissions 0.36 → 0.37, unchanged within the band) |
| R11-3 | collapse without volatility, steepening without the slide, starts within 20% | **PASS** (the comments collapse in 30 years is −31%, not −61%) |

**Reading.**
- **The double count was real.** The simulator's own exits make an entry effect, so building the drift from s̄ − e counted it twice.
- **On comments it was the whole of the body's shortfall.** Below K/16 the corrected model matches the observed ladder.
- **It explains little of the head's shortfall:**
  - about a tenth on comments (0.44 → 0.39);
  - none on submissions.
- **The model is flatter than the data at the head of both ladders, by 37–41% at K/64.** That remains a fact about the head, not a bookkeeping artifact.
- **The qualitative counterfactuals survive.** The comments hierarchy, run from the observed ladder with both ingredients, is more durable than before: −9% in 30 years.
- **For the manuscript:**
  - the corrected drift should replace s̄ − e in Results 4, Fig. 4 and the SI;
  - "flatter at every rank, by 8–46%" becomes: flatter at the head of both ladders (37–41% at K/64) and through the submissions ladder (9–37%), while matching the comments body;
  - the comments collapse becomes −31%.

  This change is to be made after the round-6 reviews finish, since they read v4.
