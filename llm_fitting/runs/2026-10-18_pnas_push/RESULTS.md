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
