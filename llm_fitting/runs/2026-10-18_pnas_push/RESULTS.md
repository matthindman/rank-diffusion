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
- **NW-2 PASS:** the boundary ratio is about 1 for the level diffusions
  (1.06, 1.07); neutral copying gives 0.58, outside the band. The ratio
  discriminates against copying, not among level diffusions.
- **NW-3 PASS:** the slide is positive in every world, driftless Gibrat
  included. "Every community loses ground" is largely definitional.
- **NW-4 PASS:** Gibrat 0, firm-like 0.11, neutral 0.36, the
  data-calibrated model 0.070 (within ±0.05 of the data). The data's
  0.01–0.08 sits with Gibrat and the model. With planted β = 0.2 the estimator
  returns 0.11 under this passing noise, so the data's raw β also
  understates the lasting β somewhat.
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
