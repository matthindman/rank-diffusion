# Proof 7 — constants, sensitivity and error budget (Stage C)

2026-10-12 · Claude Opus 5.5 · design fixed beforehand in `DESIGN.md`.

Stage A found no gap, so nothing here is conditional on an unresolved proof step. Stage B
found that no instrument supplies the experiment, so **no number here is a statement
about a platform**. The eight benchmarks are theoretical examples. Units are days and
natural-log intensity.

## Verdict

No useful numerical precision is established, and the reason is structural, not just
loose constants.

1. **The certificate as written is vacuous.** At the proof's suggested nominal point the
   ball on which the theorem speaks has radius `10^-402,641`, the stability constant is
   `10^1,326,345`, and the sampling term of Theorem 2 does not fall below what the ball
   already guarantees until the retained sample exceeds `10^3,457,972`.
2. **Sharp constants would not rescue this form of certificate.** Putting the true
   smallest singular value and the true first- and second-order sizes at the base point
   into the same formulas gives a radius of at most `1.2e-7` and a break-even sample of at
   least `10^48` (`10^25` with a variance-based concentration step). A certificate that
   linearizes once at a known point, and then requires the truth to be given within the
   radius where that linearization holds, cannot be made useful by tightening.
3. **The five features carry little information about movement even asymptotically.**
   In the small-epsilon limit, with iid retained records and no certificate at all, a
   95% interval of ±10% of scale on the binding movement parameter needs `7e9` retained
   records at the unit benchmark and `8e12` to `6e16` at the slow ones. Rank feedback is
   the weak direction, and it drags the drift and the diffusion with it.
4. **That weakness belongs to the instrument, not to the problem.** The Fisher
   information in the latent path over the same fourteen days allows the same precision
   on rank feedback with about `1e4` records at the slow benchmark. Section 6 lists what
   closes part of that gap of nine orders of magnitude.

## 1. The certificate exactly as written (C1)

`certificate.py`, mpmath at 60 digits; recomputing at 120 digits changes no printed
digit, so arithmetic error is nil. Base-10 logarithms.

| id | `a*, k*, v*, lambda, mu_0` | `s` | `epsilon` | `r` | `L` | retained `N` at which (7.2) first beats `4r` | retention `rho_14` |
|---|---|---|---|---|---|---|---|
| B1 | 1, 1, 1, 1, 0 | -322,109 | -502,117 | -402,641 | 1,326,345 | 3,457,972 | 5.3e-7 |
| B2 | 1, 1, 1, 0.05, 0 | -322,109 | -502,117 | -402,641 | 1,326,345 | 3,457,972 | 0.48 |
| B3 | 0.01, 1, 0.25, 0.01, 0 | -82,188 | -127,819 | -102,739 | 337,826 | 881,131 | 0.87 |
| B4 | 0.001, 1, 0.25, 0.01, 0 | -81,684 | -127,032 | -102,109 | 335,748 | 875,713 | 0.87 |
| B5 | as B3, `mu_0` = 3 | -82,190 | -127,824 | -102,743 | 337,838 | 881,162 | 0.87 |
| B6 | as B3, `mu_0` = -3 | -82,187 | -127,813 | -102,735 | 337,814 | 881,100 | 0.87 |
| B7 | as B3, `k*` = 0.1 | -1,316 | -1,969 | -1,649 | 5,255 | 13,809 | 0.87 |
| B8 | as B3, `k*` = 10 | -6.43e7 | -1.00e8 | -8.03e7 | 2.65e8 | 6.91e8 | 0.87 |

`v_0 = 1` throughout. Reading the B1 row: the theorem assumes the truth is within
`10^-402,641` of the nominal point in every coordinate. That assumption alone fixes the
movement parameters to `4r = 10^-402,640.6`. The data improve on it only for
`N > 10^3,457,972`. A forward-map evaluator would need an error below
`t / (2L)`, about `10^-1,728,986` for `t = r`. The count of registered births adds the
factor `1 / rho_14`.

Where the size comes from: (3.7) bounds `E exp(r sup Y)` by
`64 exp((H + 1)(r E_X + 1)^2)` with `H = 15` and `E_X = 53` at B1, because `E_X` adds
the coefficients of five absolute Gaussians and the OU path is bounded through
`j_0 = 1 + k_+ H`. Then `s` divides by the fourth power of that bound and `L` by `s`
and by `epsilon^2`. The dependence on `k*` (B7, B8) is this effect and nothing else.

## 2. Sharpenings inside the same proof (C2)

`REPAIRS_AND_PROPOSALS_v1.md`, Part A. Logarithms of `r`, `L` and the break-even `N`.

| id | horizon 4 | horizon 4 and `j_0 = 2` |
|---|---|---|
| B1 | -15,486; 50,806; 132,590 | -3,007; 9,776; 25,565 |
| B3 | -4,144; 13,507; 35,302 | -893; 2,852; 7,490 |
| B7 | -394; 1,229; 3,245 | -427; 1,338; 3,531 (worse: `1 + 4 k_+ < 2`; use the minimum) |
| B8 | -1.86e6; 6.13e6; 1.60e7 | -3,574; 11,632; 30,414 |

All eight rows are in `certificate.json`. Still vacuous everywhere.

## 3. The true low-order Jacobian at the base point (C3)

`sensitivity.py`. `A*` is the derivative of
`(E C_1, E C_2, E C_3, E C_1 C_2, E C_2 C_3)` in `(g, v, a, k, d)` at `d = 0`. These
are numerical estimates. "Relative" means rows divided by the moment and columns
multiplied by the declared scales `(a*, v*, a*, k*, 2 sqrt(a* lambda))`, so an entry is
the change in a log-moment per scale unit of a parameter.

| id | smallest singular value, raw | condition number, raw | condition number, relative | weakest movement direction, relative: nuisance known | same after nuisance compensation |
|---|---|---|---|---|---|
| B1 | 0.025 | 5.5e4 | 5.5e2 | 0.055 | 0.018 |
| B2 | 0.0068 | 2.2e5 | 7.0e3 | 0.0035 | 0.0011 |
| B3 | 1.8e-4 | 4.0e5 | 2.0e5 | 8.5e-4 | 2.9e-6 |
| B4 | 1.9e-5 | 3.7e6 | 1.1e7 | 1.4e-4 | 5.1e-8 |
| B5 | 3.5e-3 | 8.0e6 | 2.0e5 | 8.5e-4 | 2.9e-6 |
| B6 | 8.8e-6 | 4.5e4 | 2.0e5 | 8.5e-4 | 2.9e-6 |
| B7 | 2.7e-4 | 3.1e5 | 2.5e5 | 8.6e-4 | 2.9e-6 |
| B8 | 1.3e-6 | 4.7e7 | 1.6e5 | 8.6e-4 | 2.9e-6 |

- The certified `s` is `10^-322,109` at B1; the true value is 0.025. The proof's own
  inequality `s >= det / ||A||_F^4` with the true matrix would give `4.7e-10`.
- The weak direction is a movement direction, not a nuisance one. At B3 the weakest
  right singular vector is 0.84 in `d`, 0.42 in `a`, -0.33 in `g`, and nothing in `v`.
  The brief notes that a weak nuisance direction need not cost movement information.
  Here the reverse holds: after allowing the OU parameters to compensate, the weakest
  movement sensitivity falls by a factor of 3 at B1 and of 300 at B3.
- The cause is in Lemma 5. Rank feedback is separated from drift only by the curvature
  of the day means over three days, `chi`, which is 0.025 at B1 and `4.1e-4` at B3 and
  scales with `a`. Over the whole admissible range of `d` the three-day curvature of the
  mean count changes by about one part in `1e5` at B3.
- Raw condition numbers depend on units and on `mu_0` (compare B3, B5, B6). The
  relative ones do not.

Checks on the evaluator (all in `REVIEW.md` §3): closed forms to 1e-15, determinant
identity to 1e-14, two planted errors detected, the `d` column against an independent
finite-difference solver to 2e-6. Raising every quadrature order changes the Jacobian
by at most 6e-9 and its smallest singular value by at most 1.2e-7, relative
(`convergence_check.json`).

No verified nonsingular derivative is claimed beyond the proof's analytic bound: the
entries have no rigorous enclosure. No inverse on a region is claimed: that needs a
bound on derivative variation, which is the subject of the next section.

## 4. What this form of certificate could give with sharp constants

Theorem 1 needs `epsilon <= s / (20 E_F)` and `r <= s / (40 sqrt5 H_F)`. I replaced
`s` by the true smallest singular value, `E_F` by the first-order size of
`(DF_eps - DF)/eps` at the base point, and `H_F` by the largest second derivative of
`F` at the base point, both without the `d` direction. Each replacement understates
what a valid constant must be, so the results bound the form from the favourable side.

| id | `epsilon` at most | `r` at most | `log10 L` at least | `log10` break-even `N`, Hoeffding as in Theorem 2 | same, variance-based (Part C) |
|---|---|---|---|---|---|
| B1 | 7.6e-11 | 4.5e-8 | 22.4 | 60.1 | 31.8 |
| B2 | 1.6e-11 | 1.1e-8 | 24.3 | 65.1 | 34.3 |
| B3 | 5.7e-9 | 7.2e-9 | 20.8 | 58.5 | 30.1 |
| B4 | 6.5e-10 | 7.9e-10 | 23.7 | 66.1 | 33.9 |
| B5 | 1.4e-11 | 3.6e-10 | 24.8 | 68.9 | 35.3 |
| B6 | 2.3e-6 | 1.2e-7 | 16.9 | 48.3 | 25.7 |
| B7 | 6.3e-9 | 9.6e-9 | 20.6 | 57.7 | 29.7 |
| B8 | 6.0e-11 | 6.0e-11 | 26.9 | 74.8 | 38.2 |

So the looseness has three layers. The envelopes cost a factor of about `10^1,326,000`
in `L`. The worst-case concentration on features that occupy a sliver of `[0, 1]` costs
roughly half of the remaining exponent. What is left, a radius between `6e-11` and `1.2e-7`, is
intrinsic to linearizing once at a known point: the radius can be no larger than the
smallest singular value divided by the curvature. A prior region has to be about as
wide as what is not known, which for these parameters is of the order of the scale
itself. The certificate is seven or more orders of magnitude short of that under the
most favourable accounting.

**The remaining obstacle, stated exactly.** A usable regional statement needs (i) a
bound on the second derivative of the feature map over a region of the size of prior
uncertainty, including the `d` direction, where the self-consistent density has no
closed form (its contribution to the `d` derivative is 0.5% at the edge of the proof's
box and 23–24% at half the admissible bound, `pde_check.json`); and (ii) an inverse
argument that does not rest on one fixed Jacobian. At `d = 0` the report's own (4.4)
gives such an argument: `g`, `v`, `a`, `k` follow one after another from monotone
scalar equations. Extending that triangular inversion to `d != 0` is a new theorem. I
did not attempt it.

## 5. Sampling precision of the five features themselves (C4, C5)

Delta method at the base point with the exact covariance of the raw statistics. This is
the small-epsilon limit of the theorem's features, with iid retained records, and it is
asymptotic. Entries are the retained `N` at which the 95% half-width equals 10% of the
declared scale.

| id | `g` | `v` | `a` | `k` | `d` | births for the binding movement parameter, 14-day / 3-day retention |
|---|---|---|---|---|---|---|
| B1 | 1.3e6 | 3.0e5 | 6.8e9 | 8.2e10 | 1.6e7 | 1.3e16 / 2.2e11 |
| B2 | 1.7e6 | 3.8e5 | 8.2e9 | 1.1e11 | 5.7e9 | 1.7e10 / 9.8e9 |
| B3 | 3.9e12 | 3.7e5 | 6.3e12 | 2.1e10 | 2.5e13 | 2.9e13 / 2.6e13 |
| B4 | 3.3e16 | 3.7e5 | 5.9e16 | 2.1e12 | 1.5e16 | 6.8e16 / 6.1e16 |
| B5 | 1.2e12 | 1.5e5 | 2.0e12 | 6.8e9 | 8.0e12 | 9.2e12 / 8.2e12 |
| B6 | 5.7e13 | 4.7e6 | 9.2e13 | 3.2e11 | 3.7e14 | 4.2e14 / 3.8e14 |
| B7 | 3.0e12 | 3.0e5 | 4.8e12 | 2.4e11 | 1.9e13 | 2.2e13 / 2.0e13 |
| B8 | 3.5e12 | 3.3e5 | 5.6e12 | 5.1e12 | 2.2e13 | 2.6e13 / 2.3e13 |

Two things drive these. At the slow benchmarks `d` is nearly unidentified, and because
`g` is read from the same day means and `a` from `2g + a`, its uncertainty passes to
both. At the unit benchmarks the cross moments have enormous relative variance (the
coefficient of variation of `C_2 C_3` is 4,300 at B1), because raw moments of an
intensity with log-variance of several units are dominated by rare records.

Declared variants, same units (retained `N` for 10% of scale):

| id | W2: OU parameters known; `g`, `a`, `d` | W3: and `g` known; `a`, `d` | W4: fourteen day means added; `g`, `a`, `d` |
|---|---|---|---|
| B1 | 2.8e5, 1.1e8, 2.6e6 | 1.0e8, 2.6e4 | 2.7e5, 6.8e9, 3.6e6 |
| B2 | 3.3e5, 1.1e8, 8.8e8 | 1.1e8, 7.2e6 | 3.3e5, 8.2e9, 1.3e9 |
| B3 | 1.2e9, 2.6e9, 7.6e9 | 6.8e7, 6.7e6 | 2.0e9, 3.5e9, 1.3e10 |
| B4 | 1.0e11, 2.4e11, 4.6e10 | 6.2e9, 4.6e7 | 1.5e13, 2.7e13, 7.0e12 |
| B5 | 1.0e9, 2.3e9, 6.6e9 | 6.3e7, 4.3e6 | 1.3e9, 2.4e9, 8.7e9 |
| B6 | 4.9e9, 1.2e10, 3.2e10 | 1.8e8, 2.8e7 | 1.4e10, 2.5e10, 9.5e10 |
| B7 | 1.2e9, 2.3e9, 7.8e9 | 5.1e7, 5.6e6 | 1.7e9, 2.9e9, 1.1e10 |
| B8 | 5.2e8, 1.0e9, 3.3e9 | 2.7e7, 5.0e6 | 9.9e8, 1.7e9, 6.5e9 |

- W1 (three-day retention) divides births by `exp(11 lambda)`: 60,000 at B1, 1.7 at
  B2, 1.12 elsewhere.
- W2 helps by three to five orders of magnitude at the slow benchmarks and W4 by about
  three. None of the declared variants brings a slow benchmark below `4e6` for any movement
  parameter.
- Fixed positive `epsilon` (0.01, 0.1, 0.5): **NOT RUN.** It needs a deterministic
  evaluator of the Laplace functional `E exp(-eps int exp(Y))`, a two-dimensional
  Feynman–Kac problem, with an error estimate. I did not build one. The limit above
  says what the small-epsilon end can do. It does not say that a moderate `epsilon`
  would be better or worse.

Post hoc, not in the design (added after the owner's mid-task request; `posthoc_variants.py`,
and the last block of `sensitivity.py`):

- **Entry mean unknown.** With `mu_0` added as a sixth parameter the information matrix
  is exactly singular at the base point, for the five features and for the sixteen
  moments of W4. Every one of these moments sees `mu_0`, `v` and `k` through two
  quantities only: `exp(mu_0 + v/2)` and one adjacent-lag integral. The theorem's
  assumption of a known entry law and exposure is load-bearing in this precise sense.
- **Rank feedback known to be absent.** With `d = 0` imposed, `g` needs 7e3 (B1) to
  2e6 (B3) and `a` 6.8e9 (B1), 1.1e8 (B3), 9.9e9 (B4). The slow benchmarks gain five
  to eight orders of magnitude on `g` and `a`; the unit ones gain nothing for `a`.

## 6. What would tighten practical use

The owner asked, during the task, for assumptions or stylized facts that could make an
approach of this kind usable. Two reference calculations were added for that purpose
(`info_bounds.py`, post hoc). Neither concerns the five features.

**A floor.** The counts are a function of the latent path and of noise whose law does
not involve `d`, so the Fisher information about `d` in the complete path of `X` over
the record bounds what any estimator from the record can do. With drift unknown and
diffusion known it is
`(1/a) [ int_0^T E u_0(X_t)^2 dt - (int_0^T E u_0(X_t) dt)^2 / T ]`.

**An idealized high-count benchmark.** Log-intensity seen once a day without count
noise for fourteen days: the information in its mean and covariance about
`(c, a, k, v, d)`. Rank feedback enters the covariance through
`Cov(X_s, int_0^t u_0(X_r) dr)`: endpoints that start higher are pushed differently
from endpoints that start lower. This is not the count instrument and not a bound for
it.

Retained `N` for 10% of scale:

| id | floor for `d`, 3 days / 14 days | idealized benchmark, 14 days: `a`, `d` | same, entry law unknown | idealized, 3 days: `a`, `d` | five features: `a`, `d` |
|---|---|---|---|---|---|
| B1 | 3.0e2 / 75 | 1.5e3, 2.4e2 | 1.8e3, 3.6e2 | 3.1e4, 6.0e3 | 6.8e9, 1.6e7 |
| B2 | 5.1e4 / 3.7e3 | 1.3e3, 8.6e3 | 1.4e3, 1.0e4 | 3.5e4, 1.2e6 | 8.2e9, 5.7e9 |
| B3 | 5.1e4 / 1.2e4 | 1.8e4, 5.8e4 | 1.8e4, 6.7e4 | 7.9e6, 5.7e6 | 6.3e12, 2.5e13 |
| B4 | 3.5e4 / 8.4e3 | 9.9e5, 2.8e5 | 1.0e6, 3.4e5 | 7.5e8, 3.8e7 | 5.9e16, 1.5e16 |
| B7 | 5.1e4 / 1.2e4 | 1.1e5, 1.2e5 | 1.6e6, 1.8e6 | 1.7e6, 2.3e6 | 4.8e12, 1.9e13 |
| B8 | 5.1e4 / 1.2e4 | 5.5e3, 3.7e4 | 5.5e3, 4.0e4 | 1.9e7, 1.4e7 | 5.6e12, 2.2e13 |

(B5 and B6 equal B3 in the first four columns: neither calculation involves `mu_0`.)
The idealized benchmark never beats the floor, as it must not.

Read together, the tables say that the problem is not hopeless at ten thousand to a
million records, and that the five features are eight to eleven orders of magnitude away
from that at slow scales. The opportunities, in the order I would weigh them. Each is
a proposal. None is proved here, and a theorem using any of them is a new restricted
claim.

1. **Use within-endpoint contrasts instead of raw moments.** The idealized benchmark
   gets its information from differences of an endpoint's own log-activity across days
   and from the covariance of its starting level with its later change. Those remove
   the entry heterogeneity that dominates the variance of raw moments, and they do not
   need the entry law: the benchmark's numbers barely move when `mu_0` and `v_0` are
   unknown, except when the OU component is slow (B7). Bounded versions exist, for
   example `(C_j - C_i) / (C_j + C_i + 1)`, and need no small `epsilon`. The assumption
   that makes this defensible is that counts are not small. That holds for established
   endpoints and generally not for newborns, which is a reason to prefer item 3.
2. **Use the whole record and longer lags.** Fourteen days against three is worth a
   factor of 15 to 3,400 on `a` and 20 to 400 on `d` in the idealized benchmark at slow
   scales, and about 2,000 on `d` for raw moments (W4 against the five features at B3).
   Information about slow movement accrues with the span of ages observed. The record
   points the same way: lasting variance was measured from 13- to 52-week differences
   (§2z-bf).
3. **Follow established endpoints, not newborns.** At `d = 0` the increments of the
   lasting component have variance `a h` at every age, so `a` does not need a birth
   mark. Rank feedback shows up as a covariance between level and later change that
   grows linearly in the lag, while the OU contribution to the same covariance
   saturates at `-v` once the lag exceeds a few multiples of `1/k`. Two assumptions
   carry this: the transitory component mixes within the window (`k h >> 1`), and
   attrition over the lag is small and independent of state (`lambda h << 1`). It needs
   stable identifiers and counts, which the instruments have, and no birth registry,
   entry law or death registry, which they lack. It is a different theorem. It is also
   the direction §2z-bn already proposed for the program (matched finite-horizon
   moments).
4. **Bound the transitory variance.** Proposition 8 shows that without a bound on
   `v_Z` no instrument gives uniform precision. The program measures transitory
   variance by rank band, so an upper bound taken from those measurements is a
   defensible restriction, provided it is fixed before the estimate it supports.
5. **Take the OU parameters from elsewhere** (W2). Three to five orders of magnitude at
   slow scales. Established endpoints supply far more data on short-lag structure than
   newborns do. The assumption is that newborns and incumbents share those parameters.
6. **A restriction on rank feedback.** If `d = 0` could be justified, the constant-drift
   formulas (4.4) apply, they invert globally, and `g` and `a` gain five to eight orders
   of magnitude at slow scales. The record has a stylized fact that leans this way: year
   over year an endpoint's level moves like a random walk, and reversion to a home is
   contradicted (§2z-aw, §2z-ay). I do not think it licenses `d = 0`. It concerns
   weekly `log1p` karma inside a top-K universe, and it speaks to reversion toward an
   endpoint's own level, which is not the same thing as drift that depends on rank. A
   bound on `|d|` from that evidence would have to be derived, not asserted.
7. **Drop the death registry by keeping the window short.** With three-day retention
   and small early mortality, treating all registered births as retained mislabels at
   most `1 - exp(-4 lambda)` of records. This trades a missing instrument for a stated
   contamination, and it works against item 2.
8. **Replace worst-case concentration by a variance bound** (Part C). Worth about half
   the exponent of the break-even sample in §4. Necessary for any honest finite-sample
   statement at small `epsilon`, and nowhere near sufficient.
9. **Replace the single linearization by a triangular inversion** (end of §4). This is
   what would let a certified region be as wide as prior uncertainty.

Stylized facts in the record that cut the other way and that any of the above would
have to face: changes are fat-tailed at every horizon (§2z-au); an endpoint's
volatility changes over time and Reddit has an annual cycle (§2z-aw); a common
per-period factor exists (§1); endpoints differ persistently in amplitude (§1).

## 7. Error budget

| source | treatment | size |
|---|---|---|
| Certificate arithmetic | mpmath, 60 digits; recomputed at 120 | no change in any printed digit |
| Infinite age domain | Gauss–Laguerre in `lambda * age`, 96 nodes (160 in the convergence run); no truncation | change below 6e-9 relative |
| Time truncation | none: ages are bounded by 4 or 15 exactly | 0 |
| Quadrature in time and phase | Gauss–Legendre; pairs of points in one bin integrated on the ordered simplex so the covariance kink is not crossed | Jacobian change at most 6e-9, covariance at most 4e-13, relative, on raising every order |
| Derivatives in `g, v, a, k` | complex step, 1e-30: no truncation and no cancellation | rounding only |
| Derivative in `d` | analytic at `d = 0` (tilt), checked against a finite-difference solver that recomputes the density | 2e-6 relative on the fine grid; second-order convergence observed |
| Finite-difference solver | domain `[mu_0 - 40, mu_0 + 30]`, steps 0.01 in space and 0.002 in time, central differences in `d` with Richardson | diagnostic only; used for checks and for the self-consistent share, not for the tables |
| Second derivatives in §4 | central differences of the complex-step Jacobian, step 1e-4 of scale | adequate for an order of magnitude; the `d` direction is omitted, which understates |
| Floating point | double precision outside the certificate; the worst raw condition number is 4.7e7 | sample sizes good to several digits, reported to two |

None of the numbers in §§3–6 is a certified enclosure. Agreement between resolutions
and between two implementations is evidence that they are right, not a bound on their
error. The only certified statements in this audit are the analytic ones reviewed in
Stage A and the arithmetic of §1–2.

## 8. Not run, and why

- Fixed positive `epsilon`: no evaluator built (see §5).
- A rigorous enclosure of `A*`: would need interval bounds on 25 explicit integrals.
  Not done; the conclusion does not turn on it.
- Any comparison with an available sample size: Stage B found no eligible records, so
  there is nothing to compare with.
- Any simulation, fit, or use of the research model: not authorized, not needed.
