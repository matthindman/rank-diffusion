# R4 — review of the movement-to-structure protocol

2026-10-13. **PLANNING ONLY; nothing adopted. Recommend revision before any
empirical M1 run.** OpenAI Codex (GPT-6). Target commit
`a75b9c204d648a9da5dcf90ca1db9469658a6bab`; design committed as `a0251b1`.

The exact endpoint accounting is useful and its algebra is correct. The
earlier-window cohort definition, declared secondary runs, and blinded
recovery study are sensible. However, the proposed carrier interpretation is
false in general, and the four-part stopping rule cannot establish that
slopes are identified. These are substantive changes to make before running,
not qualifications to add after seeing the panels.

## Findings, in consequence order

### F1 — P1: earlier predictors do not identify the carrier

Protocol lines 142–157, 192–195, 207–211 and 274–276;
`balance_accounting.py:195–249`; `mts_step1.py:125–129`.

The statement that noise cannot manufacture a restoring slope because the
predictor precedes the interval is false for serially correlated noise. For
a stationary AR component at the observation scale,
`Cov(e[t-1], e[t+1]-e[t]) = Var(e) * (rho^2-rho) < 0`.
Using a block average reduces this covariance; it does not set it to zero.
Adjacent selection and measurement blocks also retain this dependence. The
adopted agenda explicitly asked for a sufficient gap after selection; this
protocol has no such gap or bound on residual dependence.

There are two separate checked counterexamples:

* **Exact population calculation, no simulation:** all endpoints follow the
  same stationary position-only OU, with weekly covariance
  `exp(-0.01*abs(s-t))` and no endpoint-specific homes. On four 52-week block
  means, regressing `M3-M2` on `M1,M0` gives coefficients **−0.344160 and
  +0.073999**. The positive older coefficient arises from aggregation of a
  Markov process; block means are not its Markov state. Thus the sign table
  itself does not distinguish position drift from personal homes. This
  population illustration has no cohort truncation; it refutes the asserted
  general sign interpretation, not a numerical error-rate claim for the
  complete runner.
* **Actual adjacent-block cohort/regression functions:** 6,000 synthetic
  endpoints over 208 weeks, fixed starting offsets, a lasting random walk
  with weekly innovation SD 0.005, and independent stationary AR noise with
  `phi=0.98`, SD 0.6. Seed 1000, K=1000, four 52-week blocks, first-block
  absence-penalized permanent-rank membership. The lasting level has **no
  restoring force**. The recent coefficient is **−0.2482**, endpoint
  bootstrap 5–95% **[−0.3076, −0.2032]**; the older coefficient is **+0.2297
  [0.1787, 0.2944]**, which would fail B4 and invoke the proposed personal-home
  reading. The earlier selected cohort also has raw mean change −0.0986
  between blocks 1 and 2 despite zero expected latent drift.

The second example is one constructed counterexample, not an estimate of
false-positive frequency, not calibrated to any empirical panel, and not V4.
Its numerical interval describes endpoints in that one synthetic draw.
The existing random-walk test uses `phi=0.5`, 26-week means, a gap, and a
different prediction horizon; it does not establish the primary design's
claim. Passing noise can imitate a lasting home, and temporal aggregation
can imitate the coefficient sign pattern even without a distinct noise layer.

**Before running:** call the slopes predictive associations. Withdraw the
sign-to-mechanism table and unconditional protection claims. Fix gap and
observation assumptions, then validate the exact primary block/cohort design
against position-only OU, persistent-noise random walks, and nuisance
sensitivity. Interpret a personal home only if those alternatives are
separated at the declared panel lengths. A positive earlier coefficient is
not by itself sufficient.

### F2 — P1: finite-interval reordering is not accumulated local time

Protocol lines 46–61; `balance_accounting.py:28–31, 111–118`.

The endpoint identity keeps the *initial* top-k names fixed in N. The
continuous ranked-semimartingale identity instead integrates increments of
whoever occupies the top k **at each instant**. Its local-time term balances
that dynamically changing integral. These are different named-change terms
and therefore different residual gains at a finite horizon.

Executable counterexample to additivity: for two endpoint levels
`(1,-1) -> (-1,1) -> (1,-1)`, the code gives R=0 over the whole interval,
but R=2+2=4 over its subdivisions. Local-time increments are additive.
The example checks the algebraic distinction; it is not a simulated
diffusion. The primary formula likewise uses a time-varying rank indicator:
[Fernholz and Fernholz, equation (2.1)](https://arxiv.org/pdf/1707.04285).

**Before running:** retain R as *endpoint reordering gain*. Separate its
exact accounting identity from any high-frequency or expected approximation
to local time, and state conditions/error bounds for the latter. At
26/52-week block horizons, replacing the instantaneous terms by matched
finite differences is not a proof of a variance-to-slope formula. Where
`R != kv/(2S)`, R is an exact descriptive fact but does not supply a missing
inverse relation for step 3. The adopted agenda's finite-horizon occupancy
balance does not automatically give that inverse relation either.

### F3 — P1: the four-part rule can pass without identifying a usable slope

Protocol lines 229–248, with B3/B4 at 188–195.

Criterion 2 constrains only the **absolute width** of G_pred/R. A very precise
share near zero satisfies it even though the drift cannot account for R and
cannot safely be inverted to predict a slope. The rule does not require the
predicted share's magnitude (B5), shape stability (B7), flow adequacy (B6), or
a validated finite-horizon bridge (B9 or a replacement). Exact algebra,
repeatability, precision, and classification of a finite chosen rival set
cannot by themselves establish identification of the slope estimand.

The cited theory bound is also reversed. The theory note gives
`|S_hat/S - 1| <= (delta_a+delta_G)/(1-delta_G)` under stated positive,
bounded-error conditions, **not** that slope relative error is at least
drift relative error. Equal proportional errors in a and G cancel exactly.

There are additional adjudication problems:

* B3/B4 count a confidence interval overlapping zero/the reference as
  success. Low power therefore helps a prediction pass. Equivalence needs
  an interval inside a declared practical margin; otherwise mark it
  inconclusive.
* At `k=k_centre`, the shape term is zero by definition for any process.
  That cell of A4/B7 is an identity, not evidence of stable shape.
* Four yearly blocks give three distinct change pairs, but the runner emits
  four selection/pair combinations, including two versions of the same
  final pair. Specify which calendar comparisons count; do not count a
  reused outcome twice. Facebook has no earlier-home coefficient in the
  primary design; missing statistics must not silently pass.
* The relabelled equal-variance slow/fast example is an invariance check,
  not two distinguishable mechanism labels. Condition 4 needs to distinguish
  prediction-relevant equivalence classes and report uncertainty on error
  rates, rather than require discrimination between identical observable
  laws.

**Before running:** name §8 a *feasibility/power screen*. Specify the actual
ladder-free prediction map, its assumptions, nonzero denominator and flow
treatment, permissible approximation error, and interval-based failure or
inconclusive rules. Require recovery of held-out slopes from movement-only
inputs across the rival/nuisance design before claiming identification
within that stated class. Fix V4's seed count, classes, and confusion-matrix
precision before empirical interpretation; do not use a success on a finite
menu as a general identification theorem.

### F4 — P2: attrition changes the drift estimand and breaks the weighting

Protocol lines 59–61 and 131–141;
`balance_accounting.py:205–216, 252–277`.

Cohort drift averages only endpoints with both block levels, while
predictable_loss multiplies those survivor means by **all initial top-k
occupants**, including exits. G itself includes only survivors. Thus G_pred
and G do not cover the same endpoints when attrition is nonzero, contrary
to the matching claim. An attrition count does not identify the missing
changes or repair selection on survival.

Two deterministic checks: with a cohort of 100 and equal numbers of latent
+1 and −1 changes, removing the 50 negative endpoints gives a reported drift
+1 with bracket [1,1], although the original cohort's mean is zero. In a
four-endpoint accounting example with one top-2 exit and identical changes
among survivors, G_pred is −1.3333 while G is −0.6667: the survivor drift is
multiplied by two initial occupants instead of one surviving occupant.

**Before running:** define the carrier drift as survivor-conditional, match
its weights to G's survivor population, and keep missing outcomes in a
separate, explicitly unidentified/flow account. If the target is the full
cohort's drift, state an observation model or bounds and validate them under
informative missingness; reporting F alone is not that correction.

### F5 — P2: the precision rule uses an interval with no stated coverage

Protocol lines 134–141, 222–227 and 236–239;
`balance_accounting.py:252–277`.

predictable_loss constructs its limits by weighting and adding the marginal
bin q05/q95 values. Those are not generally the quantiles of the weighted
sum. With two independent standard-normal estimates and weights one, the
function's construction yields ±3.2897; the actual 90% interval of their sum
is ±2.3262. This is an exact distributional check, not Monte Carlo noise.

The computation also conditions on the realized centre, bin occupancy and
R. That can be a declared conditional uncertainty target, but cannot be
silently presented as total uncertainty in the common slide or a future
slope. Dependence across endpoints and time is not cured by increasing the
number of endpoints. A range of three calendar pairs is not a substitute
with known coverage.

**Before running:** specify the inferential target. Keep shared bootstrap
draws and compute the entire weighted numerator and, where random in that
target, centre/occupancy/denominator in each draw; validate coverage under
the planted panel design. If a conservative rectangle is desired instead,
name and calibrate it as such rather than calling its endpoints 5–95%.
Specify zero/small-R behavior and suppress the §8 verdict when uncertainty
cannot be estimated.

### F6 — P2: failed tracking certification still produces ordinary results

Protocol lines 91–96 and 280–282; `mts_step1.py:91–96, 109–132, 151–170`.

The maximum of the weekly outside thresholds is a valid conservative upper
bound for an untracked endpoint's present-week block mean. However, the
runner only saves it and a 2K level. It neither checks the inequality nor
marks affected cells invalid, and report() does not display the certification.

Synthetic fixture: K=12, 30 weeks, 10-week blocks; 480 endpoints rotate
through the top 48 with values near 10 and are otherwise zero; another is
always 9, so never enters the tracked set. That omitted endpoint is actually
the largest block mean. The runner records outside bound 9 and tracked
2K level 1.0045, **then returns a normal accounting pair**. The protocol says
this result is void, but its reporting path does not enforce that.

At the largest boundary, diffusion_gain also uses ranks through
`sqrt(2)*2K`, beyond the stated top-2K certification; the validity of the
centre alone is insufficient for that statistic. An unavailable bound and
ties at the bound need explicit handling.

**Before running:** fail closed or mark every dependent statistic
NOT ADJUDICATED unless the tracked population is certified at the centre,
the scored rank, and the full diffusion band. Display these flags in the
report and test this fixture.

## Further specifications to finish

These need resolution in the same revision; they are not evidence of an
empirical failure:

* The ever-top-4K tracking rule uses future weeks. Membership of an inside
  cohort is protected because a sufficiently high permanent rank in its
  selection block entails tracking already. The “outside” bin is not:
  it contains future top-4K visitors, not everything outside the cutoff in
  the first block. Since that bin enters G_pred when its members climb in,
  declare its retrospective selection or give it a past-only population.
* diffusion_gain estimates variance by **current** start rank. The agenda
  permits an observed-position accounting check, but this must be named a
  selection-affected diagnostic rather than a permanent-rank structural
  parameter. Its role in B9/step 3 needs explicit validation.
* A block mean is a filter, not an endpoint level. A block-to-block difference
  divided by block length has its own transfer function. Stationary annual
  cycles average away only with complete, appropriately aligned coverage;
  the 70%-presence rule can leave substantial seasonality.
* The declared 26-week Reddit secondary is “read only” at 52-week separation,
  but analyse() and report() compute/print all separations. Filter and label
  the declared secondary before interpretation. report() prints only the
  primary boundary and does not generate REPORT.md, score the 13 predictions,
  or apply §8. Either implement the adjudicator or specify the manual
  aggregation, absolute-value conventions, missing-cell rules, and exact
  pair/boundary weights before outcomes are seen.

## Recommended disposition

Keep the exact accounting and permanent-rank cohort scaffolding. Correct
F1–F6, freeze the revised estimands and adjudication, and then run the blind
recovery map through those statistics. Carry unresolved nonidentification
forward as a requirement for an observation model or additional information.
Do not treat this review as answering the objections or enabling --reviewed.
V4's full study was not started: its required statistic version depends on
the author's answer to R4.

## Reproduction and scope

Read: AGENTS, relevant skills/references, coordination protocol/queue and
R4/V4 briefs, adopted agenda, PROGRAM_STATE, MODEL_STATUS §§2z-bp–br,
the original THEORY note, reviewed protocol/modules/tests, exposure ledger
and sealed-path declarations. Primary-source check: equation (2.1) of the
linked Fernholz paper. No empirical M1 output was read.

Run from the isolated worktree using the registered interpreter:

```
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 \
  llm_fitting/handoff/R4_mts_protocol/checks.py \
  > llm_fitting/handoff/R4_mts_protocol/CHECK_RESULTS.json
bash scripts/health_check.sh
```

`CHECK_RESULTS.json` contains all check outputs. The first check-script
launch failed immediately because its import root was one directory too
high. Corrected only that path; the completed run then exited 0. No failed
launch output was interpreted and no experiment setting was changed.

Initial health check: research **499 passed** (2 warnings, 13 subtests),
package **8 passed**, sealed guard wired, legacy **14/15, churn 0.013**.
The eight reviewed balance tests are included in the research suite. Health
checks use the existing DEVELOPMENT legacy panel; no M1 empirical analysis,
new parameter fit for this review, Gate v1 evaluation, full V4 recovery run,
or sealed-data contact occurred. Final health-check status is recorded in
STATUS_ENTRY.md. No reviewed file, default, stack, gate, protocol, or paper
claim was edited. Nothing was pushed.
