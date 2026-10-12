# Review of `PROOF_1_RESULT.md` (the estimand bridge)

_Claude (Opus 5.5), 2026-10-12, at the owner's request. Reviewed: Codex's
note at commit 8dcf233 on `codex/d1-debate`. This is one review pass by a
second model that had the whole context of the debate; it is not the blind
referee session the package's README describes, and it is not expert
certification. Agreement between two models is not evidence. What counts
below is that each step was re-derived and the constructions were checked
numerically._

## Verdict

**NO GAP FOUND UNDER STATED ASSUMPTIONS**, for all eleven claims.

Coverage. I re-derived every displayed identity and inequality in Claims 1
to 11, the three audits and §§7–8. I checked by deterministic computation
(`proof_1_checks_claude.py`, output in `proof_1_checks_claude.json`; linear
algebra and quadrature, no data, no model code, no stochastic simulation):

- Claim 3: K² = −I, both stationarity equations, e^{A₁Δ} = e^{A₀Δ}, the lag
  covariance (10) and E[β²], for six pairs of Δ and κ, to 1e-15;
- the OU decomposition (18)–(20) by quadrature;
- the three-date inversion (26), the tanh identity of §7, and the overlap
  variance (31).

Not covered. I did not re-read the cited sources [R1]–[R10]; the note's
statements about what they contain are taken as given. Claim 7's step 2
(uniqueness of a self-adjoint nonpositive generator given its semigroup at
one time) I checked as an argument from the spectral theorem, not against
Gobet, Hoffmann and Reiß. No novelty question was examined.

## Claim by claim

| claim | what it says | review |
|---|---|---|
| 1 | The stationary balance S = u·a/(2G) holds with a and the drift averaged over whoever currently occupies a position; the named process need not be Markov | Correct. Itô on a test function, stationarity, integration by parts. The boundary and no-flux conditions are stated, not assumed away |
| 2 | Slow real movement and slowly varying observation error can swap roles with every observable unchanged | Correct, and elementary: two independent stationary OU processes with one stationary variance and different rates |
| 3 | With no observation error at all, two models with the same weekly law and the same ladder have instantaneous variances 2κ and κ | Correct. Verified numerically. See remark 1 on what the construction requires |
| 4 | Exact decomposition of the variance of an h-week change into bracket, drift, drift-by-martingale covariance, error and signal-by-error terms | Correct. Needs the cohort to be known at the start of the change; see remark 5 |
| 5 | A worst-case enclosure of the two-lag statistic's error | Correct. Loose at the project's lags; see remark 3 |
| 6 | Two sufficient noise instruments: a known joint error law, or two independent measurement channels of the same latent vector | Correct. (b) is Kotlarski's argument with an explicit derivative formula |
| 7 | Within observed types, a stationary reversible scalar Markov diffusion is identified from one positive lag once the error law is identified | Correct as stated. See remark 4 on which assumption does the work |
| 8 | Typed stationary OU with serially uncorrelated error is identified from three dates | Correct. Its formula explains a known problem; see implication 4 |
| 9 | Without a Markov assumption, noise-managed observations at lags tending to zero identify the occupancy-band variance | Correct. A different experiment from a weekly panel, as the note says |
| 10 | Error of transporting cohort coefficients to current occupancy, in three separate terms | Correct |
| 11 | How variance and drift errors propagate to slope and ladder; a band average gives only an integrated restriction | Correct |

## Remarks on the proof

1. **What the alias of Claim 3 requires.** The hidden state oscillates at
   exactly the sampling frequency. For weekly sampling and κ between 0.005
   and 0.02 per week, the latent level's correlation with itself half a
   week later is −0.99, and the variance of a half-week change is four
   times the stationary variance, against 0.01 to 0.04 times for a whole
   week. The level would sit on the opposite side of its mean every
   midweek and be back by the next observation. The note says a bound on
   sub-weekly drift excludes it; I add that the project's daily panels
   exclude this particular alias by direct observation, and push any
   surviving one to oscillations within a day. The general lesson stands:
   nothing sampled weekly constrains what happens between samples.
2. **Neither counterexample changes anything observable.** In both pairs
   the ladder and the whole law of named weekly movement are identical. In
   the alias pair the variance and the restoring drift are both halved and
   their ratio is the same, which the note's own audit shows. What is not
   identified is a continuous-time coefficient that no observable quantity
   depends on. The note treats this as an obstacle to be removed by
   restricting the model or sampling faster. I read it differently; see
   implication 2.
3. **Claim 5 is too loose to use at the project's lags.** With the drift
   bound set to g = 0.003 per week and a = 2gS for S = 0.5, both
   deliberately small, the enclosure's half-width at lags 13 and 26 is 1.7
   times the quantity enclosed. The bound on the drift-by-martingale
   covariance drives it. In a reverting example the actual error of the
   statistic is 9% to 18% at κ of 0.005 to 0.01 (eq. 19). The note says the
   enclosure can be vacuous; at these lags it is.
4. **In Claim 7 "reversible" adds little.** A stationary scalar diffusion
   on the line with no flux at its boundaries is reversible. The assumption
   doing the work is that, within a type, the level is Markov in itself
   with no hidden state. An unobserved spread of homes or of reversion
   rates inside a type breaks it, as the note says.
5. **Claim 4 needs cohorts fixed before the change starts.** A9, A10 and
   Codex's recomputation all formed cohorts by permanent rank over the full
   window. Under that conditioning martingale increments need not have mean
   zero, so the decomposition does not apply to those statistics as
   computed. This is a statement about what the earlier measurements
   license, not a gap in the proof.

## Implications (the reviewer's reading; labelled where it goes beyond the note)

1. **The empirical regularity stands; one reading of it does not.** The
   variance growth of permanent-rank cohorts over the ladder's local slope
   is close to constant across ranks (§2z-bf, §2z-bj). That is a fact about
   two measured profiles. It was read as the a of the formula over 2S. By
   Claims 4 and 10 the statistic is a cohort average with finite-horizon
   corrections and no occupancy transport. Under reversion at the fitted
   rates it understates the instantaneous variance by 9% to 46% at 13 to 52
   weeks for κ of 0.005 to 0.02, and by more where κ is larger; under
   differing endpoint trends it overstates it.
2. **Beyond the note: the instantaneous variance is the wrong target, and
   not only an unreachable one.** The ladder is the stationary state of the
   weekly law of movement. It depends on variance and restoring drift only
   through their ratio on a common clock. So the measurable version of the
   relation uses a matched pair: the mean change and the mean squared
   change over one stated horizon, for one cohort fixed beforehand. At a
   finite horizon the exact statement is a balance of crossings: in a
   stationary cross-section as many endpoints cross any level downward as
   upward. The variance-and-drift form is its small-step approximation. In
   a reverting example it is off by 6% to 18% at κh = 0.13 and by 11% to
   34% at κh = 0.26, depending on position (`matched_horizon_balance_ratio`
   in the check output), so at 13 to 26 weeks the crossing form should be
   the one measured.
3. **Beyond the note: A10's one-week result reads differently.** A10 found
   that the one-week variance gives a constant 9 to 19 times larger and a
   ratio that rises with rank, and I recorded that the relation "does not
   appear" without noise handling (§2z-bh). On the matched-pair reading
   nothing fails there: at one week the restoring drift is mostly passing
   swings decaying, and it should be larger by the same factor. This is a
   prediction and not a result: one-week drift has not been measured. It is
   cheap to test, and on observed positions the one-week balance has to
   close in a stationary cross-section, so it doubles as a check of the
   measuring code.
4. **Claim 8 explains why the reversion rate is so badly determined.** In
   the class nearest the frozen model's core, κ = −log(γ₂/γ₁)/Δ, the log of
   a ratio of two nearly equal covariances. A 1% error in that ratio moves
   κ by 200% at κ = 0.005, 100% at 0.01 and 50% at 0.02. That is the size
   of failure Codex's recovery study found (§2z-bc). In that class the
   instantaneous variance a = 2κv inherits the problem and the stationary
   variance v does not. And the ladder of a home model does not depend on
   κ or a at all, only on the homes and v.
5. **The two model families put the information in different places.**
   With personal homes, lasting variance and reversion rate are hard to
   measure and irrelevant to structure. With drift by position, their ratio
   is the structure. What separates the families is whether an endpoint's
   movement depends on its own history beyond its current position. The
   proof shows that question cannot be settled in continuous time from
   weekly data (Claim 3); at the weekly level it can be examined with
   several lags, and the weekly level is all that structure needs.
6. **What the data in hand can supply.** Daily panels exist for Facebook
   Era A and the Reddit comments window. They do not give the limit of
   Claim 9, they are daily totals, and they carry a day-of-week cycle; they
   do rule out week-period aliases and shorten the lag by a factor of
   seven. Spec-B gives a noise variance, not a joint error law; by Claim 2
   that suffices only with the added assumption that observation error is
   uncorrelated from week to week. The split between lasting and passing
   movement is by declared time scale. It is a convention the data cannot
   certify in an unrestricted class, and it should be stated as one.
7. **For the paper.** "The ladder follows from measured movement" cannot
   mean that the diffusion coefficient of a continuous process was measured
   and inserted in a formula. With weekly panels it can mean: a weekly law
   of named movement, estimated without the ladder, simple in its
   dependence on position and limited in its dependence on identity, has
   the observed ladder as its stationary state and predicts ladders and
   changes it was not fitted to. The stationary state of a law estimated on
   the same stationary sample is that sample's own ladder, so the last
   clause carries the evidence. No claim language is changed by this
   review.

## What this changes in the proposed agenda

Step 1 of `RESEARCH_AGENDA_MOVEMENT_TO_STRUCTURE_2026-10-12.md`: the
estimands become finite-horizon and matched (mean change, mean squared
change and crossings in each direction, at stated horizons, for cohorts
fixed on earlier windows), in place of an instantaneous variance by current
position. The one-week balance on observed positions is added as a check of
the instrument. Step 2's planted rivals gain one case: a slow and a fast
component of equal stationary variance (the pair of Claim 2), on which the
measurement must return the same answer whichever is called the signal.

## Not done

A blind referee pass with only the statement and proof. A check of the
cited sources. Any measurement: every number above comes from a formula in
the note or from the check script. The predictions in implications 2 and 3
are untested.
