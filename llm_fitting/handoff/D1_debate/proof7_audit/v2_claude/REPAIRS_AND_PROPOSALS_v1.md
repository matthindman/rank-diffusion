# Proof 7 — sharpenings and proposed variants, version 1

2026-10-12 · Claude Opus 5.5 · companion to `REVIEW.md`.

No repair is required for Theorems 1 and 2 to hold as submitted. This file keeps apart
three kinds of thing that must not be folded silently into the reviewed theorem:

- **Part A** — sharpenings of constants that the submitted argument already licenses.
  The statement keeps its form; the numbers `s`, `epsilon`, `r`, `L` change.
- **Part B** — the three-day retention design. Proved here. A different sampling
  experiment, so a separate proposed design.
- **Part C** — a variance-based version of Theorem 2. A restricted new claim with a
  different estimator.

Nothing here has been reviewed by anyone else. Each item is PROPOSED.

## Part A. Constants

**A1. Envelope horizon 4 instead of 15.** In Lemma 4 the processes are bounded by
suprema over `[0, H]` with `H = 15`. The five features are functions of `C_1, C_2, C_3`,
which depend on the paths on `[0, U + 3]`, a subset of `[0, 4)`. Every inequality in the
proof of Lemma 4 holds with `H` replaced by 4 wherever it bounds a supremum over time or
a length of integration: in `C_X`, `X_1`, `X_2`, `j_0`, `j_1`, `j_2`, `t_1`, `t_2` and in
the factor `(H + 1)` of (3.7). Lemma 7 and Theorem 1 use only the resulting `B_F`,
`H_F`, `E_F`. The retention horizon is untouched: this is not Part B.

**A2. `j_0 = min(2, 1 + k_+ H)`.** From (3.10),
`|J_t| <= sup|B| (1 + k int_0^t e^{-k(t-s)} ds) <= 2 sup|B|`.

Effect at the proof's suggested nominal point (`a* = k* = v* = 1`, with
`lambda = 1, mu_0 = 0, v_0 = 1`), base-10 logarithms from `certificate.json`:

| | `log10 r` | `log10 L` | `log10` of the retained `N` at which (7.2) first beats `4r` |
|---|---|---|---|
| as submitted | -402,641 | 1,326,345 | 3,457,972 |
| A1 | -15,486 | 50,806 | 132,590 |
| A1 and A2 | -3,007 | 9,776 | 25,565 |

The improvement is a factor of `10^1,316,569` in `L` and changes nothing practical. Where
`1 + 4 k_+ < 2` (benchmark B7 in `SENSITIVITY.md`), A2 with the plain value 2 would be
worse than A1; the minimum avoids that.

**A3. Smaller slack that does not matter numerically.** Inverse norm `4/3` instead of 2
in `P_1`, `P_2`; coordinate map norm 1.281 instead of 2, so `L = 2.57 / (s epsilon^2)`
and diameter `2.56 r`.

**A4. Wording.** `REVIEW.md` §4, items 1 and 2.

Do these change the theorem? They change its constants and therefore the set on which
it speaks (a larger ball). The proof is the submitted one with a number replaced. I
regard A1–A3 as a second version of the same theorem and recommend that any citation
say which constants are meant.

## Part B. Three-day retention (proposed design W1)

**Setting.** `Delta = 1`. Calendar time is measured from the start of the birth bin.
Birth is at calendar time `R`, uniform on `[0, 1)`; `U = 1 - R` is the age at the start
of the first complete bin. Complete interior bin `j` is calendar `[j, j + 1)`, ages
`[U + j - 1, U + j)`. For an integer `m >= 1` let

    E_m = { T > m + U }  =  { alive at calendar time m + 1 },

the end of the `m`-th complete interior bin. The submitted theorem conditions on
`E_14`.

**Proposition B1.** For every `m >= 3`:

1. Conditional on `E_m`, `U` has density `lambda e^{-lambda u} / (1 - e^{-lambda})` on
   `(0, 1)` and is independent of the un-killed paths `(X, Z)` and of the Poisson
   randomization. This conditional law does not depend on `m`.
2. The conditional joint law of `(C_1, C_2, C_3)` given `E_m` does not depend on `m`.
   In particular it is the same for `m = 3` and `m = 14`.
3. Consequently `M(beta) = E[(f_1, ..., f_5)(C) | E_m]` is the same map for `m = 3` and
   `m = 14`, and Theorems 1 and 2 hold word for word, with the same constants, for `N`
   independent records retained under `E_3`.
4. `P(E_m) = rho_m = e^{-lambda m} (1 - e^{-lambda}) / lambda`, so
   `rho_3 / rho_14 = e^{11 lambda}`, and `E_14` is a subset of `E_3`. The expected
   number of registered births per retained record falls from
   `lambda e^{14 lambda} / (1 - e^{-lambda})` to `lambda e^{3 lambda} / (1 - e^{-lambda})`.

*Proof.* (1) is Lemma 1 of the report with general `m`: before selection `U` is uniform
and independent of everything else; given `U = u`, `E_m` has probability
`e^{-lambda(m + u)}` and involves only `(U, T)`; the factor `e^{-lambda m}` cancels in
the normalization. (2) On `E_m` with `m >= 3` the endpoint is alive throughout
`[0, U + 3]`, so on that event the recorded `C_1, C_2, C_3` are the counts that (1.5)
generates from the un-killed path, that is, a fixed measurable function of
`(U, X|[0, U+3], Z|[0, U+3], Poisson randomization)`. By (1) the conditional law of
that tuple given `E_m` is the same for all `m >= 3`. (3) follows because the five
functions depend on `(C_1, C_2, C_3)` only and the proofs of Theorems 1 and 2 use the
record law only through `M`. (4) is the integral in Lemma 1. ∎

**Indexing, stated so it cannot be read two ways.** The survival flag must be true at
calendar time 4, the *end* of the third complete bin, which is age `U + 3`. Survival to
the start of that bin (calendar 3) is not enough: `C_3` would then mix counts of living
and dead endpoints. The minimum horizon for the joint law of all five features is
`m = 3`.

**What it buys.** A factor `e^{11 lambda}` in registered births: about 60,000 at
`lambda = 1` per day, 9 at 0.2, 1.7 at 0.05, 1.12 at 0.01. It matters only when
mortality is appreciable within two weeks.

**What it does not change.** `N` retained records are still needed in the same number.
Propositions 8 and 9 were stated for the fourteen-count record; for a three-count
record their bounds hold with 15 replaced by 4 in (8.1) and 14 by 3 in (8.8). Any later
instrument that uses days 4–14 needs the longer cohort again.

**Remark (not proposed).** `f_1` needs only `E_1`, and `f_2, f_4` only `E_2`. Using a
different cohort for each coordinate keeps every coordinate mean unbiased and the union
bound valid, with gains of `e^{lambda}` and `e^{2 lambda}` on those coordinates. It
complicates the sampling design for a small gain.

**Status.** A separate design. It is not applied to any data and defines no empirical
cohort.

## Part C. A version of Theorem 2 without `epsilon^{-2}` (restricted new claim)

In (7.2) the factor `L = 4 / (s epsilon^2)` multiplies a worst-case deviation of
features that lie in `[0, 1]`. For small `epsilon` the features are far from filling
that interval: `0 <= f_j <= epsilon C_j` for `j <= 3` and
`0 <= f_4 <= epsilon^2 C_1 C_2`, `0 <= f_5 <= epsilon^2 C_2 C_3`, because
`1 - (1 - epsilon)^C <= epsilon C`.

**Estimator.** With `Lambda = diag(1/eps, 1/eps, 1/eps, 1/eps^2, 1/eps^2)`, minimize
`|| Lambda (M(x) - M_hat_N) ||_2` over the ball `K` (a weighted distance; the submitted
estimator uses the unweighted one).

**Proposition C1.** Let
`Sigma_bar = sup_{x in K} ( E C_1^2 + E C_2^2 + E C_3^2 + E C_1^2 C_2^2 + E C_2^2 C_3^2 )`,
finite by the envelopes of Lemma 4 extended to fourth order. If the truth lies in `K`,
then with probability at least `1 - alpha`

    || theta_hat - theta_0 ||_2  <=  min{ 4r,  (8 / s) sqrt( Sigma_bar / (N alpha) ) }.

*Proof.* The variance of a coordinate of `Lambda f` is at most its second moment, which
is at most the corresponding count moment by the inequalities above. So
`E || Lambda (M_hat - M(x_0)) ||^2 <= Sigma_bar / N`, and by Markov's inequality the
norm is at most `sqrt(Sigma_bar / (N alpha))` with probability `1 - alpha`. The
minimizer satisfies `|| F_eps(x_hat) - F_eps(x_0) || <= 2 || Lambda (M_hat - M(x_0)) ||`.
Theorem 1 gives `|| x_hat - x_0 || <= (2/s) || F_eps(x_hat) - F_eps(x_0) ||`, and the
coordinate map costs a factor 2. ∎

**What it buys and what it costs.** The sample size no longer contains `epsilon^{-4}`:
`N >= 64 Sigma_bar / (s^2 t^2 alpha)`. The dependence on confidence is `1/alpha`
instead of `log(1/alpha)`; a median-of-means version would restore the logarithm at the
price of a constant, not pursued here. `epsilon` must still satisfy (6.1), because
Theorem 1 needs it. With the submitted envelopes `Sigma_bar` and `s` are still
astronomically conservative, so this does not make the certificate usable. Its use is
in `SENSITIVITY.md`, where it shows how much of the looseness is the concentration step
(about half of the exponent) and how much is the radius.

**Status.** New restricted claim; assumes nothing beyond the submitted model; not
reviewed.
