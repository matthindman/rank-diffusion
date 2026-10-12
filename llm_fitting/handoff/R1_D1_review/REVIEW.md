# R1 — D1 artifact review
_OpenAI Codex, 2026-10-12. One review round; no candidate adopted. Reviewed base main 191422962f3ba22fe6db11d19d860fd779cc42f0; design/check code committed at 15ef4ced45a200cc535a0d39081b875dc116e15c before execution. The subsequent queue and signed candidate-2 consensus on main were read. Candidate 2 is closed and outside this review._

The approved structure-only seeding option is properly contained. The most consequential new finding is a coordinate error in A4's split-half reliability calculation. D1 also needs a current decision list, narrower uncertainty language, and a fully specified comparison procedure. None of these findings justifies adopting a replacement model.

## Findings, ranked by consequence

### 1. A4's half fits use the wrong coordinate for the full-population profile
**Verified code and deterministic counterexample; empirical effect not measured.**

In `phase1_a4.py:48–55`, interleaved sets of identities retain their original full-population ranks. In `minimal_rankdiff.py:746–760`, estimation recomputes N from the number of retained entities per period and assigns permanent-rank coordinates as log(clip((mean rank − 0.5)/N)). Copying dataframe attributes does not preserve this denominator. A4 then interpolates both half profiles onto the full profile's coordinate and uses their correlation to compute reliability and line/reliability ratios (`phase1_a4.py:66–79`).

The committed check applies the production coordinate routine to a balanced 200-endpoint, 12-week fixture. Each half has N=100 instead of 200; unclipped coordinates shift by log(2)=0.69314718056, and 50% of each half clips at z=0, versus 0% for the full panel. The estimator uses the same coordinate formula; this is not a claim based on a separately invented estimator.

Consequently, the reliability comparison in §2z-at is not comparing estimates of the same full-population profile. It cannot yet establish how much reproducible structure a simple curve misses. This affects D1's narrowing of compression, independently of the withdrawn candidate 2. It does not invalidate full-fit curve R² or grid-bound counts by itself, and the direction or magnitude of the empirical reliability error is unknown.

**What settles it:** preserve a common full-universe rank reference for both halves, restrict comparisons to common coordinate support, and rerun A4. Also assess whether shared weeks/common shocks and smoothing permit the independent parallel-error assumptions behind the Spearman–Brown interpretation. An explicit equivalent rank transformation could work, but merely copying attributes does not. No reviewed file was edited.

### 2. D1's operative recommendations still use superseded premises
**Verified document inconsistency; the historical status is disclosed, not concealed.**

The afternoon header explicitly preserves the morning text, but the final decision list and candidate dependencies still read as actionable. Candidate 1 is already approved opt-in (§2z-ba/bb); candidate 2 is now withdrawn under the signed consensus; V2 has landed (§2z-bc/bd). Nevertheless, the body requests the old approvals, argues that the annual cycle causes fitted reversion, proposes wording based on “not one amplitude,” and makes candidates 3 and 4 wait for candidate 2.

In particular, candidate 3's rationale (`D1_CANDIDATES_CLAUDE_2026-10-12.md:183–191`) says candidate 2 removes the cycle's estimator distortion. V2's planted cycles instead lower fitted reversion, and candidate 2 is withdrawn. This no longer supports that dependency or the suggestion that the cycle would cease to affect fitting.

**What settles it:** retain the morning document as history, but present a current decision list and candidate rationale governed by §2z-ba–be. Seasonal structure may remain deferred as a model addition while seasonal controls are required in the proposed lasting-dynamics diagnostics. “No scored statistic asks for it” is not a demonstration that omitted seasonality cannot bias an estimate. This review proposes no new paper claim or agenda adoption.

### 3. The margin proposal mixes an operational threshold with statistical evidence
**Verified scope problem; no new margin estimated or selected.**

D1 proposes common 40-seed comparisons, candidate minus incumbent error ≤0.05 on each panel, and “better” only beyond measured noise. V1 (§2z-ax) measures conditional simulation variation for a fixed fitted stack and observed panel. That is useful precision information, but it does not determine scientifically acceptable harm, refitting uncertainty, future-panel uncertainty, or uncertainty from selecting a candidate.

The ≤0.05 point-estimate rule can be an owner-chosen operational rule. It alone is not evidence of statistical noninferiority. “Exceeds measured noise” also leaves the resampling procedure, sidedness, quantile and treatment of calibration unspecified. Reusing overlapping origins does not turn seed precision into sampling precision.

**What settles it before a comparison:** specify the seed list, paired difference, retained primitives, resampling unit and interval rule; preserve joint origin structure when resampling a simulation member. State which fitting/calibration choices are held fixed. Distinguish “passes the operational margin” from an inferential noninferiority claim. A stronger no-harm statement would require an appropriate upper uncertainty bound against a margin justified in advance. The owner decides any gate amendment; this review does not change 0.05, 20/40 reps, or any registered rule.

### 4. Candidate 1 is correctly isolated, but “harms no panel” exceeds the bands
**Implementation containment verified; general no-harm claim unsupported.**

The current time-median option affects the structure simulation's initial ladder without mutating fitted p.w0. It is absent from gate arguments. The existing flag-off/explicit-ladder/helper/CLI tests in `tests/test_seed_ladder.py` pass in the full health check. This supports the approved opt-in implementation (§2z-bb); changing gate calibration or re-freezing remains a separate decision.

The in-sample macro-index paired seed brackets (§2z-az) include worsening on short submissions, −0.040 [−0.064,+0.009], and Instagram rescue, −0.013 [−0.096,+0.057]. They are 5th–95th percentile brackets conditional on these fits/data, from 20 shared seeds. The point estimates improve, but “harms no panel” should be “all five point estimates improve; changes on short submissions and Instagram are unresolved at this precision.” The already disclosed selection between two seedings should remain disclosed.

Time-median seeding also retains observation/order-statistic spread; it is not automatically an estimate of latent homes. This does not undo its practical value. **What settles the wording:** report the existing bands and precise conditioning. A latent-home interpretation would require a separate validated measurement model.

### 5. The reference-seed description is factually wrong for long submissions
**Verified against V1's complete gate rows and §2z-ax.**

D1 says all four recorded reference gates used three simulations. Facebook, comments and short submissions did; long submissions already used 20, seeds 0–19, with 2,000 bootstrap draws. The V1 banks use 80 independent seeds 1000–1079. Therefore the whole long-submissions reference-to-bank difference cannot be described as a three-replicate correction. The independent seed set also changes.

**What settles it:** state the panel-specific reference precision and separate finite-ensemble bias evidence from realized differences between seed sets. Changing the recorded reference values remains an owner decision.

## Candidate dispositions

| Item | Review disposition |
|---|---|
| 1: time-median ladder | Approved opt-in containment is supported. Narrow no-harm/latent-home language. Re-freezing is still separate. |
| 3: annual cycle | A model addition can remain deferred, but remove its dependency on withdrawn candidate 2. Include seasonal confounding in the design of lasting-dynamics diagnostics. |
| 4: raw passing-jump limit | Keep at design stage. Define the identifying moment, observation mapping, and mean/variance convention before simulation or a gate. |
| Comparison margin | Common random numbers and retained outputs are sensible. Complete the procedure and distinguish operational tolerance from inferential claims before adopting it. |

Candidate 4 needs two specific qualifications. V2's bounded-fast arm clips standardized innovations and renormalizes variance; it does not test a state-dependent raw jump ceiling. Its failed 20% head-tail-reduction prediction (§2z-bc, P8) neither validates nor refutes the proposed raw cap. Conversely, native-parameter recovery with absolute s bias below 0.01 does not support shrinking s as a repair under the model's own observation/tail assumptions. Tail-estimator recovery remains a real concern.

The worst-seed maxima quoted in §2z-av are descriptive. They are not, by themselves, a stable identifying moment for a cap: compare maxima at matched numbers of endpoints, weeks, presence patterns and simulation exposures, or declare a tail functional independent of the score. A raw state-dependent cap changes moments differently from a variance-normalized standardized clip. Specify and recover those moments rather than selecting a cap to improve a card. No cap is recommended here.

## Assessment of §2z-bd and the closed consensus

I agree with withdrawing “not one amplitude” as an interpretation of this statistic, withdrawing the cycle-causes-fitted-reversion account, and restricting the s-overstatement story under the native model. Two qualifications remain:

- “Recovered without bias” in D1 is stronger than the evidence: §2z-bc establishes small absolute bias at three native s values, while true s=0 returns positive estimates on every seed. The size/range of that floor has not been mapped between zero and the native values. Nor has a smaller seasonal cycle been fitted to show it explains the empirical amplitude-correlation statistic.
- The planted volatility ratio provides useful discrimination within this fixed-nuisance candidate set, not unique identification against all alternatives. The literal “0.95–0.98 under every other mechanism” range in §2z-bd is approximate: the long-profile annual_cycle_1 mean is 0.992472, with seed 5–95% range [0.989365,0.997698]. This strengthens, rather than reverses, the contrast with strong log-volatility, 0.703484 [0.688487,0.725923]. All 54 panel/condition rows are retained in `V2_MIDDLE_VOLATILITY_ALL.csv`; no favorable subset was selected.

The correction appended to §2z-bd already retracts the universal claim that no endpoint statistic separates random walk from current-rank attraction. Our full-table audit exactly confirms its strong-arm counts: 55/649 separated on Facebook, 42/763 on comments, and 68/781 on long submissions (165 of 2,193 total rows). Those counts were visible before the audit, as disclosed in DESIGN; this is artifact verification, not blind replication. V2's labels lack family-wide error control and fix nuisance parameters. Neither an unresolved middle-group contrast nor some separated rows identifies a mechanism. The signed consensus correctly limits the conclusion and allows weak long-run reversion or durable identity effects. There is no new candidate-2 debate in this review.

One ancillary measurement check is relevant to future structural diagnostics: `ladder_drift` measures absolute log-activity level as well as shape. Multiplying a fixed rank-size profile by e produces drift 0.999998498 while normalized shares change by at most 2.1e−17 and identities never change rank. This is a deterministic counterexample to using drift alone as a shape-specific identifying moment; it is not evidence that the observed A8 drift is all common growth. Decompose common level and normalized shape in any future design. No frozen metric was changed.

## Evidence and limits

`CHECKS.json` records source hashes and deterministic results; the three `V2_STRONG_PAIR_*.csv` files retain the complete pair audit. Exact reproduction and validation commands are in REPORT. No empirical refit, candidate comparison, new gate call, raw-panel analysis, or sealed-data contact was performed. Corrected A4 empirical reliability and a calendar-aligned V3 verdict remain unmeasured; the latter has its own already committed question.
