# Proof 7 — independent review of the submitted argument (Stage A)

2026-10-12. Reviewer: Claude Opus 5.5 (Anthropic), Claude Code. I did not write Proof 7
and had not seen it, its derivation or any other review of it before this session. A
second audit sits in `../` (written by another session earlier today); I have not opened
it. Reviewed text: `proof_prompts/PROOF_7_RESULT.md`, SHA-256
`ba3b4ef140e13c244002bc39cbd61abc05d06b3d5c9377a760594c194be60be5`, against the question
in `PROOF_7_FINITE_SAMPLE_IDENTIFICATION.md` (`1252a732…`) and the standard in
`PROOF_VERIFIER.md` (`4d3a5236…`). The hash was the same at the end of the session.

A review by a model is not a certificate. "No gap found" below means: I re-derived the
step by hand, and where a step is an identity or an explicit inequality I also tested it
numerically in a way that could have failed.

## 1. Outcome

**NO GAP FOUND UNDER STATED ASSUMPTIONS** for the positive result (Lemmas 1–7, Theorems 1
and 2) and for the two lower bounds (Propositions 8 and 9). Two side remarks in §8 are
asserted with abbreviated proofs and are marked below. Nothing in the report is false as
stated, and no repair is required for the theorem to hold. The corrections in §4 are
wording and unnecessary looseness.

What the theorem says, read literally, is narrow. It gives a Lipschitz inverse for five
bounded count features on a ball of radius `r` around the single point
`(g, v, a, k, d) = (0, v*, a*, k*, 0)`, and an error bound for an estimator that is told
in advance that the truth lies in that ball. The report says this itself (§0, §7, §10).
How narrow is a question of numbers, answered in `SENSITIVITY.md`: at the proof's
suggested nominal point the ball has radius about `10^-402,641`.

| claim | location | outcome | how checked |
|---|---|---|---|
| Retention probability, phase density, independence from un-killed paths | Lemma 1, (1.4)–(1.6) | no gap | re-derived; numerical integration (check C) |
| Existence and uniqueness of the renewal fixed point for `d^2 < 4 a lambda` | Lemma 2, (2.1) | no gap | re-derived, including the entropy bound and Pinsker constant |
| Resolvent identity, contraction ball, first and second parameter derivatives, spatial and mixed regularity, positivity and mass | Lemma 3, (3.1)–(3.4) | no gap | every constant recomputed by hand; kernel checks A, B |
| Pathwise derivatives including changes in diffusion, envelopes, differentiation under the expectation | Lemma 4, (3.5)–(3.11) | no gap | every term of `X_1`, `X_2`, `Z_1`, `Z_2`, `B_{r,n}`, `B_F`, `H_F`, `E_F` recomputed by hand |
| Constant-drift moment identities with unknown OU parameters | §4, (4.1)–(4.4) | no gap | re-derived from the covariance function; check D to 1e-15 |
| Gaussian tilt, signed second difference, lower bound `chi*` | Lemma 5, (5.1)–(5.3) | no gap | re-derived; checks F, G; independent finite-difference solver (check I) |
| Determinant factorization and lower bound; singular-value bound | Lemma 6, (5.4), (5.5) | no gap | re-derived; full 5×5 matrix assembled numerically (check H) |
| Exact bounded-feature identities; derivative error | (6.2), (6.3), Lemma 7 | no gap | re-derived |
| All-pairs inverse on the ball; scaling; change of coordinates | Theorem 1, (6.1), (6.4), (6.5) | no gap | arithmetic of (6.5) and both operator norms recomputed |
| Estimator, measurable selection, concentration, sample size | §7, Theorem 2, (7.1)–(7.4) | no gap | re-derived |
| Slow OU with large variance hides a change in diffusion | Proposition 8, (8.1)–(8.3) | no gap | re-derived |
| Fast OU: total-variation comparison with the averaged model | §8.3, (8.4), (8.5) | no gap for (8.4), (8.5); the sentence "the finite-feature transfer argument applies to that limiting model" is not proved in the text | re-derived the two displays |
| Zero OU variance: `k` unidentified; separate movement-only certificate | §8.4, (8.6), (8.7) | no gap in the algebra of (8.6) and in (8.7); the regularity constants for the reduced model are described, not written out | re-derived |
| Predominantly zero records hide movement | Proposition 9, (8.8), (8.9) | no gap | re-derived |

## 2. The seven points the brief asked to be checked

**(1) Renewal existence, resolvent identity, regularity.** The fixed-point map of
Lemma 2 contracts in total variation with factor `|d| / (2 sqrt(a lambda))`: the drifts
differ by at most `|d| TV`, path entropy to time `t` is at most `d^2 TV^2 t / (2a)`, the
exponential-time average is `d^2 TV^2 / (2 a lambda)`, and Pinsker with
`TV = sup_A |P(A) - Q(A)|` gives the stated factor. The resolvent form `p = q - d K(p Hp)`
follows from applying `(lambda - (a/2) d^2/dy^2 + c d/dy)^{-1}` to the stationary
equation; the symbol `lambda + a xi^2/2 + i c xi` has no zero, so an integrable solution
is unique, and the probabilistic density is such a solution. The `L^1` contraction on the
ball of radius 2 then identifies the Banach-space fixed point with the probabilistic
density, which settles positivity and unit mass without assuming them for iterates.

Kernel bounds, recomputed. The parameter derivative of the translated Gaussian kernel is
`(t/2)^i (-t)^j` times its `(2i + j + l)`-th spatial derivative, whose `L^1` norm is at
most `sqrt(n!) (at)^{-n/2}`. The remaining power of `t` is `(j - l)/2 > -1` in all twelve
cases used (check B), so (3.1) is right as written. I also compared with what is exactly
computable: the resolvent kernel is `exp((c y - |y| rho)/a) / rho` with
`rho = sqrt(c^2 + 2 a lambda)`, the `L^1` norm of its derivative is `2 / rho`, and
`K_0 = sqrt(pi) / sqrt(a_- lambda)` bounds it (1.33 against 2.51 at the unit point).
`P_1`, `P_2` and `P_x`: differentiating `p = q - d K N(p)` once and twice gives exactly
the terms listed in the report, with `||N(p)|| <= 4`, `||DN(p)|| <= 4`, `||D^2 N|| <= 2`
and inverse norm at most `4/3` (the report uses 2). The spatial recursion
`L_{n+1} <= n_1 + D K_0 (2 L_n + P_0^2)` is right and its fixed point is `P_x`. The mixed
derivative needed in Lemma 4 is bounded by `P_0 + D n_0 P_1`, which uses the sup-norm
half of the norm on `E`; that is why the space has to be `L^1 ∩ L^infinity`.

**(2) The sampling experiment.** Selection depends on `(U, T)` only, so the phase
density and its independence from the un-killed paths follow from Lemma 1 as stated. One
phase is drawn per record and used in every bin: the cross moment (4.3) carries
`T(2g + a)`, not a product of two phase factors. I checked that this matters and that an
error here would have been caught: a version of my evaluator with a separate phase per
bin changes `E A_1 A_2` by 14–38% at the test points. Daily integration is over fixed
unit bins `[U + j - 1, U + j]`. The OU component starts in its stationary law at birth.
The proof uses actual birth throughout: age enters through `exp(g t)`, through `a s` in
(4.2) and through the age-dependent rank position in Lemma 5. First appearance is never
substituted for it, and the argument would not survive that substitution.

**(3) The benchmark and the rank-feedback sensitivity.** With `d = 0`,
`E exp(Y_s + Y_t) = C_0 exp(g(s + t) + a s + v e^{-k(t-s)})` for `s <= t`, from
`Var(W_s + W_t) = 3s + t`. The ratios (4.4) are exact. For Lemma 5 I derived the tilt
independently: under the weight `exp(X_t)/E exp(X_t)`, `X_s` is normal with mean
`mu_0 + (c + a)s + v_0` and variance `v_0 + a s`; an independent draw from the renewal
density at age `r` has mean `mu_0 + c r` and variance `v_0 + a r`; at `c = -a/2` the
difference has mean `-(2 v_0 + a(s + r))/2` and variance `2 v_0 + a(s + r)`, which is the
normal tail in (5.1). `h` is strictly decreasing, so the cumulative response is strictly
concave and the second difference of the day means in `d` is strictly negative, while
the `g` and `v` columns are affine in the day index. That is the whole mechanism by
which `d` is separated. The lower bound holds because all arguments lie in `[0, 4]` and
the integrand of (5.3) is decreasing in `s + r`. The determinant: after the second
difference only the `d` entry of row 3 survives, the `(g, v)` block of rows 1–2 has
determinant `-K^2/2`, and the `(a, k)` block of rows 4–5 has determinant `-e^a Q Q_k`;
the other entries of rows 4–5, including their `d` derivatives, do not enter. Time
ranges: `1 <= l <= 2` on the rectangle used for `-Q_k`.

**(4) Derivatives when diffusion changes.** The proof couples all parameters on one
Brownian path and differentiates `X_t = X_0 + int b + sqrt(a) W_t` pathwise; the `a`
derivative contributes `W_t / (2 sqrt a)` and `-W_t / (4 a^{3/2})`. No likelihood ratio
is used for a change in `a`. Girsanov appears only at fixed diffusion (Lemma 2,
Proposition 8). The self-consistent density is differentiated, not frozen: `D_w p`
enters `b_1`, `b_2` and `b_{x1}`. At `d = 0` its contribution to the first derivative
vanishes because it is multiplied by `d`; away from zero it does not. I measured it with
an independent solver (check I): at `d = D`, the edge of the proof's box, it is 0.5% of
the `d` derivative of the day means; at half the admissible bound it is 23–24%.

**(5) The bounded features and the inverse.** `E[(1 - eps)^C | A] = exp(-eps A)` and
conditional independence across bins give (6.3) exactly. Lemma 7's bound follows from
`0 <= A - h_eps(A) <= eps A^2/2` and `|DA| <= G_1 V A`. In Theorem 1,
`||DF_eps(x) - A*||_2 <= 5 (eps E_F + 2 sqrt5 r H_F) <= s/2` with the choices (6.1), the
scaling back to `M` loses at most `eps^2` because `eps <= 1/2`, and the map from `x` to
the original parameters has norm 1.281 (the report uses 2). The ball of radius `2r` lies
inside the box because `r <= r_b/4`.

**(6) The estimator.** The minimum-distance argument, the countable approximate
selection and the Hoeffding step are right, and (7.3)–(7.4) are the correct inversions.
The theorem assumes that the truth lies in the prescribed ball. That assumption alone
already bounds the movement error by `4r` (by `2.56 r` with the sharper norm). The
statistical term of (7.2) is smaller than that only when
`N > (5/8) L^2 log(10/alpha) / r^2`. The numbers are in `SENSITIVITY.md`.

**(7) Lower bounds and boundaries.** Proposition 8: the two alternatives share `c` and
`S = a_i + 2 q_i`; removing the OU drift costs relative entropy `q_i H / (4v)` per record
and leaves a common reference law of `X + Z`; Pinsker and the triangle inequality give
(8.1). Proposition 9: the intensity is dominated by `exp(-r_i t)` and (8.8) is the exact
phase average. Both are statements about the whole record and do not depend on the five
features. Neither is part of the positive theorem, and neither contradicts it: both live
far outside the ball. The fast-OU display (8.4) and the small-variance display (8.7) are
correct comparisons; the report is right that they limit recovery of `k`, not of
movement.

## 3. Executable checks

Run in `stageA_checks.py` (43 of 43 pass) and `pde_check.py`. Test points: the unit
point `a = k = v = lambda = v_0 = 1, mu_0 = 0` and a generic point
`a = 0.3, k = 0.7, v = 0.5, lambda = 0.2, mu_0 = -1, v_0 = 0.6`.

| | check | result |
|---|---|---|
| A | `L^1` norm of the n-th Gaussian derivative against `sqrt(n!)`, n = 1..6 | 0.80, 0.97, 1.51, 2.80, 5.91, 13.8 against 1, 1.41, 2.45, 4.90, 10.95, 26.8 |
| B | time exponents of (3.1); closed-form resolvent kernel; its mass; `K_0` | exponents equal `(j - l)/2` in all 12 cases; kernel agrees to 1e-13; mass 1; `2/rho <= K_0` |
| C | (1.4) by numerical integration, `m` = 3 and 14, three death rates | agree to 1e-12 |
| D | (4.1), (4.3), (4.4) against an evaluator built from the covariance function, with the phase integrated numerically and in closed form, at `g` = 0, 0.15, -0.4 | worst relative difference 1.1e-15 |
| E | mutations: separate phase per bin; mean-one `exp(Z)` | both detected (14–38% and 22–39%) |
| F | the tilt identity against a direct integral that uses no tilt | relative difference below 2e-16 |
| G | (5.3) against a central difference of `h`; `chi >= chi*`; `-h' >= chi*` on `[0, 4]` | `chi` = 0.0254 against `chi*` = 0.0099 (unit); 0.0121 against 0.0024 (generic) |
| H | determinant of the full numerical Jacobian, `d` column included in all five rows, against Lemma 6's formula and bound | agree to 1e-14; 1623 against the bound 3.70 (unit) |
| I | `d` derivative of the day means from a finite-difference solution of the stationary equation and the backward equation, density recomputed at each `d`, against `K D_j` | agree to 2e-6 on the fine grid and 9e-6 on the coarse one (second-order convergence); `chi` 0.0253711 against 0.0253709 |

Check I uses neither the tilt nor the pathwise derivative, so it is the one that could
have exposed a wrong sign, a missing factor or a wrong normalization in Lemma 5.
Agreement across two grids is a diagnostic, not an error bound.

## 4. Corrections and omissions (none changes the theorem)

1. **§0 and (6.1).** The region is a Euclidean ball in `x = (g, v, a, k, d)`. In the
   original coordinates it is an ellipsoid. The verdict sentence calls it a ball about
   `(a, c, d, kappa, v_Z)`.
2. **Lemma 4, statement.** "A neighborhood of the interior of the box" should read "the
   interior of the box". Only the open ball of radius `2r` is used.
3. **Lemma 4, envelopes.** The horizon `H = 15` is used in every envelope, but the five
   features involve ages below 4 only. Replacing 15 by 4 in (3.5)–(3.8) is licensed by
   the same argument. It changes the constants, not the statement (see
   `REPAIRS_AND_PROPOSALS_v1.md`).
4. **(3.6).** `|J_t| <= 2 sup|B|` holds for every `k`, so `j_0` can be
   `min(2, 1 + k_+ H)`.
5. **Lemma 3.** The inverse of `I + d K DN(p)` has norm at most `4/3`; the stated 2 is
   valid and looser. `||p||_1 = 1`, so the ball of radius 2 is never approached.
6. **Theorem 2.** The diameter bound `4r` can be `2.56 r`.
7. **§8.3, last paragraph.** That the finite-feature argument transfers to the
   averaged-intensity model is asserted, not shown. It is plausible and not
   load-bearing. Outcome for that sentence: UNRESOLVED.
8. **§8.4.** The movement-only certificate at `v = 0` is a separate restricted claim
   whose regularity constants are described in words. I checked (8.6). I did not rebuild
   Lemma 4 for the reduced model.
9. **§9, [R1].** See §5 below: the cited proposition covers less than the sentence in
   Lemma 2 suggests, and the report's own table already says so.

Not an error, but a reader should not miss it: the identification of the OU variance
`v` from the first moment rests entirely on the convention `E Z = 0` together with a
known entry mean and known exposure, because the day means see `mu_0` and `v` only
through `mu_0 + v/2`. At the base point the five features cannot separate `v` from an
error in `mu_0` (they see `mu_0`, `v` and `k` through two quantities). This is within
the stated assumptions. It matters for applicability and is taken up in
`INSTRUMENT_AUDIT.md`.

## 5. External dependencies, with my own reading tags

| source | tag | what it supports and whether its hypotheses hold here |
|---|---|---|
| Scheutzow, *Stochastic Processes III / Wahrscheinlichkeitstheorie IV*, lecture notes, TU Berlin, version of 2 April 2019: Theorem 1.4, Theorem 1.7 and its proof, Remark 1.8, Definitions 3.1–3.6, Proposition 3.8 and its proof, Remark 3.9 ([PDF](https://page.math.tu-berlin.de/~scheutzow/WT4main.pdf)) | READ THIS SESSION (pages 1–5 and 9–12) | Theorem 1.4: if the stochastic exponential is a martingale, the shifted process is a Brownian motion on each fixed `[0, T]`. Theorem 1.7 (Novikov) is stated in general and proved there only under `E exp((p/2)<N>_T) < infinity` for some `p > 1`. Every use in Proof 7 meets the stronger condition: bounded integrands in Lemma 2; in Proposition 8 the strict inequality `q_i H < 2v`, or subdivision. Proposition 3.8 gives existence of a weak solution for bounded jointly measurable drift, unit diffusion, a deterministic initial point and a fixed horizon. Remark 3.9 says that random initial conditions and uniqueness in law are **not** proved in the notes and refers to Karatzas–Shreve. Lemma 2 supplies both by short arguments of its own, which are the standard ones. |
| Karatzas and Shreve, *Brownian Motion and Stochastic Calculus*, Propositions 5.3.6 and 5.3.10 (weak existence with random initial law; uniqueness in law for bounded drift) | KNOWN FROM TRAINING AND NOT RE-READ | Would close the two points above by citation. Not retrieved. The in-text arguments are short enough that I checked them directly. |
| Pinsker's inequality; Hoeffding's lemma | proved in the report; proofs checked | Not external dependencies. |
| Differentiability of ODE solutions in parameters for `C^2` right-hand sides; implicit function theorem in Banach spaces; reflection principle; Hermite orthogonality; Poisson total-variation coupling; chain rule and data processing for relative entropy | KNOWN FROM TRAINING AND NOT RE-READ | Textbook facts. Their hypotheses are met: the drift is `C^2` jointly by Lemma 3, the fixed-point map is a quadratic polynomial in `p` with `C^2` kernel dependence, and the processes are Gaussian or bounded as required. |
| The report's [R2]–[R7], [R9] | not re-read | The report uses none of them as a premise, and I found no step that needs them. |

## 6. The three-day retention question

Yes. Replacing the fourteen-day retention rule by survival to the end of the third
complete interior bin leaves the joint law of `(C_1, C_2, C_3)`, and therefore the five
feature means and Theorems 1–2, unchanged, and raises the retention probability by the
factor `exp(11 lambda)`. The statement, its proof and the calendar indexing are in
`REPAIRS_AND_PROPOSALS_v1.md` as a separate proposed design. The reviewed theorem is not
amended.

## 7. Coverage

Checked line by line: §§1–7, Propositions 8 and 9, and displays (8.4), (8.5), (8.7),
(8.6). Checked numerically: every identity and explicit inequality in §§1, 4, 5 and the
kernel constants of §3. Not checked numerically: the envelope constants of Lemma 4
(checked by hand only; they are upper bounds with enormous slack), and nothing in
Theorem 2 beyond arithmetic. Not verified: the two sentences marked in §4 items 7 and 8;
the Karatzas–Shreve propositions; any claim of novelty, which the report does not make.
A mathematician should read Lemma 3 and Lemma 4 before either is cited in a paper.
