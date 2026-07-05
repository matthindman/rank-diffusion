# External review — GPT 5.5 Pro (2026-07-05)

_Verbatim report from the static review (prompt + attached files per the review
protocol). Adjudication: MODEL_STATUS §2q. A parallel GPT 5.6 Pro review may
follow; keep them separate._

---

# EXECUTIVE SUMMARY

The core model is more defensible than I expected, but the paper must narrow and sharpen its claims. The covariance algebra in `_md_partition` and `_md_partition2` is correct; the later `D(h)` additions are not ad hoc curve-fitting but the mathematically right way to identify slow mean reversion when short-lag autocovariances have near-zero signal. The log says exactly this: the `gamma_0..gamma_6` objective was empirically flat in (a), while adding (D(2,4,8,13)) made the SSE profile sharply identified. That is consistent with the mixture-of-exponentials theory.

The largest threat is not the stochastic-process algebra. It is the evidentiary package. The best **in-sample** stack is not the same as the best **OOS** stack: FB Era A's 15/15 card is explicitly in-sample, while the operational FB OOS result uses the P1 md6 calibrated specification; comments' best in-sample and OOS stacks also differ. The paper should not imply that one frozen full stack simultaneously achieves every headline number.

The strongest new mathematical problem I found is in **Spec-B**. The Toeplitz estimator fits the covariance only after week-mean centering, (C\Sigma C), but then maps the uncentered reconstructed (\Sigma) into (p^\top\Sigma p). Since (CJC=0), the all-ones Toeplitz direction is unidentified by the centered covariance but changes (p^\top\Sigma p) by a constant. The code therefore does not uniquely identify the claimed floor without an additional convention. The invariant map should be (p^\top C\Sigma C p), or an explicitly justified restriction on the common within-week component. This does not invalidate the project, but it weakens the "σ_obs identified" claim until the magnitude is checked.

The (b\approx1) factorization is a good first-order empirical law, not an exact law. The log's own test rejects exact (b=1): comments (b=1.079,[1.065,1.093]), FB (b=1.024,[1.008,1.040]). Results are practically similar under (b=1), so the right paper language is "one amplitude to first order, with small super-unit permanent-share dispersion," not "the same amplitude exactly."

For PNAS, the next budget should go to confirmation and breadth, not more dynamics. Freeze the pipeline; reserve the unprocessed comments extension as a true confirmation panel; add at least two non-Facebook/Reddit ranking systems. Replace the 15/15 threshold card with metric-specific sampling/MC bands or a covariance-weighted omnibus distance.

---

# A. VALIDITY THREATS

**A1. One-stack overclaim — SEVERITY: major. BASIS: EVIDENCE.**
The central claim should not be written as if a single final stack wins every test. The log says FB Era A's 15/15 stack is in-sample and that its OOS behavior is "untested and expected fragile"; FB's operational OOS result remains the P1 md6 calibrated specification. Comments similarly use md-vr/stat-factor/mix for in-sample and md6+mix+conditional state for the best OOS gate. The attack a referee will make is simple: "You selected different specifications for different tables." Cheapest decisive check: run the in-sample stack through the OOS gate, report it even if it fails, and define a single "paper-primary" stack per estimand.

**A2. "Identified, not tuned" is overstated — SEVERITY: major. BASIS: JUDGMENT + EVIDENCE.**
Many parameters are tied to declared moments, but the sequence of adding `--temperament`, `--md-vr`, `--stat-factor`, `--two-scale`, `--mix-hetero`, and `--md-vr-long` was conducted on the same panels. The log is honest about rejected fixes and retractions, but a referee will still treat the selected stack as the result of specification search. The decisive protection is a frozen confirmation protocol on the unprocessed comments extension and at least two new systems, with no model changes except pre-declared data adapters. The current log itself recommends breadth and confirmation rather than more dynamics.

**A3. Spec-B identification has a static algebraic vulnerability — SEVERITY: major. BASIS: PROVEN.**
The daily noise-floor estimator fits a Toeplitz covariance through the week-mean centering projection and then maps daily shares through the reconstructed covariance. But the centered covariance (C\Sigma C) cannot identify the all-ones Toeplitz direction (J), because (CJC=0), while (p^\top Jp=1). Therefore (p^\top\Sigma p) is not identified from the centered residual covariance alone. The code should either compute the invariant centered floor (p^\top C\Sigma C p), or explicitly impose and defend a zero-common-within-week-noise convention. This is the cheapest high-value audit in the whole project.

**A4. (b=1) is statistically rejected — SEVERITY: major for wording, minor for model performance. BASIS: EVIDENCE + PROVEN.**
The log reports that exact (b=1) is rejected for both comments and FB, though the result-level differences are small. The mathematical claim that (b=1) factorizes the process is correct; the empirical claim that (b=1) holds exactly is not. Cheapest decisive check: repeat (b) after per-entity detrending and on the confirmation panel.

**A5. The OOS gate is useful but not yet inferentially clean — SEVERITY: major. BASIS: EVIDENCE + JUDGMENT.**
The OOS gate uses five rolling origins, train-only estimation, train calibration, Wasserstein summaries, and bootstrap median coverage. That is much stronger than a single split. But the five splits are heavily dependent, the relative-error vector includes near-zero denominator artifacts, and the bootstrap coverage is not a formal coverage statement over rolling-origin dependence. The comments coll1 denominator artifact is already documented. Cheapest fix: report split-level results, use a pre-declared moment floor for relative errors, and add a block/bootstrap-over-origin sensitivity rather than treating the five split mean as an independent sample.

**A6. The 15/15 scorecard is not a statistical test — SEVERITY: major. BASIS: PROVEN + EVIDENCE.**
The scorecard thresholds are deterministic tolerances: 20% relative for VRs, ±0.08 for ACF/RACF/R2, and a persistence tolerance based on top-k size. The code implements these thresholds directly. There is no covariance among metrics, no MC uncertainty band, and no multiple-testing adjustment. This does not make the scorecard useless; it makes "15/15" a descriptive engineering result, not a hypothesis-test result.

**A7. Head-collision rows are too noisy for point-reading — SEVERITY: major for goal-2, minor for goal-1. BASIS: EVIDENCE.**
The log retracts the apparent (b)-sensitivity of FB coll1 as Monte Carlo noise, reporting coll1 seed SD ±0.15 and about ±0.07 SE at five reps. That means any five-rep coll1 discrepancy near 0.1 is not interpretable without MC bands. The conditional diagnostic largely closes the head churn problem, but the paper should not read head-collision point estimates as precise.

**A8. Cross-platform universality is underpowered — SEVERITY: major. BASIS: EVIDENCE + JUDGMENT.**
The project has two platforms and three metric/era panels. Several features replicate: slow rank-dependent home shape, daily noise-floor shape, common amplitude structure. But the log also shows important non-universal quantities: comments (s=0.692) versus FB/submissions near 0.9, Spec-A/Spec-B divergence growing with depth on both platforms, and conditional forecasting working differently across panels. This supports a shared model class, not yet a universal law.

**A9. FB is not a platform census — SEVERITY: major for framing, minor for estimation if disciplined. BASIS: EVIDENCE.**
CrowdTangle is a censored fixed tracked panel with instrument eras; FB claims must be "of tracked activity." The code and log correctly segment Era A as primary and bar windows from straddling broken eras. The paper must keep this discipline everywhere, especially in coverage, entry, and boundary language.

**A10. "Directional lifecycle arcs" is plausible but not unique — SEVERITY: major for interpretation, minor for fit. BASIS: PROVEN + EVIDENCE.**
The demeaning evidence in §2p is real: empirical per-entity demeaning removes much more slow variance than the simulation. But the inference "therefore lifecycle arcs" is not unique. A stationary Gaussian process with stronger low-frequency power, such as a near-unit-root mixture or fractional process, can produce the same demeaning loss over a finite window. The paper should write "consistent with lifecycle arcs" unless it adds phase/asymmetry evidence.

---

# B. MATHEMATICAL AUDIT

**B1. Moment formulas in `_md_partition` are correct — SEVERITY: positive finding. BASIS: PROVEN.**
For a stationary AR component (Z_t=qZ_{t-1}+u_t) with stationary variance (S), define (\Delta Z_t=Z_{t+1}-Z_t). Then Var(ΔZ)=2S(1−q), and for lag k≥1, Cov(ΔZ_t,ΔZ_{t+k}) = −S(1−q)²q^{k−1}. For iid observation noise, the variance contribution is 2σ_ε², lag-1 covariance −σ_ε², higher lags vanish. Therefore the code's rows γ0 = 2W(1−a)+2V(1−φ)+2σ_ε², γ1 = −W(1−a)²−V(1−φ)²−σ_ε², γk = −W(1−a)²a^{k−1}−V(1−φ)²φ^{k−1} are right. The D(h) formula, D(h)=2W(1−a^h)+2V(1−φ^h)+2σ_ε², is also right.

**B2. `_md_partition2` is algebraically correct, but it is a hard Prony problem — SEVERITY: minor for code, major for inference. BASIS: PROVEN.**
With three decays, the lag-k covariance tail is a three-exponential mixture. Generic identification requires distinct roots, nonzero weights, and enough consecutive lags (m exponentials from ~2m tail moments via Hankel/Prony). Ill-conditioned when roots are close, a root has small weight, or q≈1 makes S(1−q)² tiny. The grid label separation (a≥0.93, φ2∈[0.70,0.95], φ1≤0.65) is principled.

**B3. The §2i flat-SSE degeneracy is exactly what theory predicts — SEVERITY: positive finding. BASIS: PROVEN.**
For slow OU with δ=1−a≪1, the tail weight A = W(1−a)² ≈ σ_η²δ/2 → 0, even when the long-horizon contribution to D(h) remains first-order. Adding D(h) is principled because it aggregates slow innovations.

**B4. Grid search plus clipped nonnegative least squares is consistent only under fixed-grid/model-correct conditions — SEVERITY: major. BASIS: PROVEN.**
Consistent if the grid contains the truth, the population objective has a unique minimizer, and true nonnegative coefficients are interior. Off-grid truth → best grid approximation; zero coefficients → boundary asymptotics. Identity weighting is consistent but inefficient; under misspecification it can choose a different compromise than optimal GMM. Cheapest honest fix: bootstrap bands for knot curves; compare identity to feasible GMM/diagonal precision weighting on one platform.

**B5. Moment covariance should be estimated, not assumed — SEVERITY: major. BASIS: JUDGMENT.**
Moments have very different precision (γ0 vs D(52)); overlapping D(h) estimates are strongly autocorrelated. Practical: entity-level influence functions + time-block correction, or two-level bootstrap (entities within knots × moving-block weeks). Report curve bands.

**B6. The temperament estimator's correction chain is basically right — SEVERITY: minor. BASIS: PROVEN.**
E[log(χ²_ν/ν)] = ψ(ν/2) − log(ν/2); Var = ψ₁(ν/2); ν_i=(n_i−1)/κ with κ=1+2Σρ_k². Correct as implemented.

**B7. The temperament estimator likely overstates persistent heterogeneity if long-lag dependence is ignored — SEVERITY: minor to major depending on magnitude. BASIS: PROVEN + JUDGMENT.**
κ uses only lags 1-2. Longer autocorrelation → κ underestimated → ν overstated → ψ₁ understated → s² biased upward. Split-half evidence already implies v_i drifts slowly. The right claim: "persistent volatility heterogeneity with slow drift."

**B8. The (b=s(h*)/s(1)) statistic is exact under (b=1), approximate otherwise — SEVERITY: minor for first-order law, major for exact inference. BASIS: PROVEN.**
Under b≠1 the horizon variance is a mixture Q_i(h)=c_f(h)v_i+c_p(h)v_i^b/E[v^b]; the log-variance slope lies between 1 and b, weighted by shares — residual fast share biases b toward 1; lifecycle arcs and drift can bias s(h*) upward; slow drift in v_i lowers split-half stability.

**B9. The entity bootstrap CI for (b) is valid for cross-entity sampling, not for time-window uncertainty — SEVERITY: minor to major. BASIS: JUDGMENT.**
Add split-window b estimates, block bootstrap over weeks, per-entity detrending. Matters because the CIs are narrow enough to reject b=1 while the log itself suspects upward lifecycle bias.

**B10. `_sqw` is correct — SEVERITY: positive finding. BASIS: PROVEN.**
E[v^b]=exp{b(b−1)s²/2}; sqrt(w)=sqv^b/exp{b(b−1)s²/4}. Verified, including tests.

**B11. The stationary common-factor innovation scaling is correct — SEVERITY: positive finding. BASIS: PROVEN.**
Var(L)=σ_F²/(2(1−ρ)); Var(ΔL)=σ_F². Verified, including tests.

**B12. Spec-B's projection algebra is not fully identified as currently mapped — SEVERITY: major. BASIS: PROVEN.**
`_toeplitz_floor` fits CΣC where C=I−11ᵀ/7. Σ_ℓ T_ℓ = J and CJC=0, so c_ℓ ↦ c_ℓ+δ leaves CΣC unchanged while the reported floor pᵀΣp changes by δ (pᵀJp=1). The floor depends on the lstsq min-norm convention. The invariant floor is pᵀCΣCp. Until the code reports both, "Spec-B identifies σ_obs" should be weakened.

**B13. Zero-inflated/intermittent days can plausibly explain depth-growing Spec-A/Spec-B divergence, but not until B12 is fixed — SEVERITY: major. BASIS: JUDGMENT + EVIDENCE.**

**B14. The variance-ratio demeaning functional removes low-frequency power, not only "lifecycle arcs" — SEVERITY: major for interpretation. BASIS: PROVEN.**
The per-entity mean subtraction adds a zero-frequency notch; a deterministic linear trend is removed exactly, but stationary long-memory / near-unit-root processes also lose much more variance than short-memory OU. §2p evidence = excess low-frequency/directional structure, not unique proof of arcs.

**B15. Overlapping VR small-sample bias does not fully cancel — SEVERITY: minor to major for h=52. BASIS: PROVEN.**
Identical functionals cancel bias only at the truth. At h/T=52/136 this is not negligible. Report VR metrics with empirical and simulated sampling bands.

**B16. The tests are useful but not complete — SEVERITY: minor. BASIS: EVIDENCE.**
Not pinned: Spec-B projection uniqueness, moment-weighting robustness, real-data standard errors, rolling-split dependence, per-entity reversion-rate heterogeneity.

---

# C. METHODOLOGY AUDIT: FORKING PATHS AND CONFIRMATION

**C1. The sequential log is honest but still selected — SEVERITY: major.** Lean into "discovery on Panels A–C; confirmation on held-out Panels D–F." The unprocessed comments extension is the cleanest confirmation panel.

**C2. The "best card ever" phrase is vulnerable — SEVERITY: major.** Use "best descriptive in-sample card under the working diagnostic stack."

**C3. Current thresholds should be replaced by uncertainty bands — SEVERITY: major. BASIS: PROVEN.**
Metric-level acceptance bands (empirical sampling + MC) or one omnibus distance Q=(m_sim−m_emp)ᵀΩ̂⁻¹(m_sim−m_emp) with Ω̂ from entity/time-block bootstrap and simulation replicates. Keep the 15-row card as a visual diagnostic.

**C4. The OOS gate's relative-error metric needs a denominator rule — SEVERITY: minor to major.** Absolute tolerance floor, symmetric percentage error, or exclude moments below a predeclared empirical floor.

**C5. Five rolling splits are not five independent replications — SEVERITY: major.** Report as a trajectory; for confirmation use non-overlapping calendar blocks or a single untouched future block.

**C6. Train calibration is acceptable only if described as calibration — SEVERITY: minor.**

**C7. Conditional forecasting changes the estimand — SEVERITY: minor.** Report both unconditional and conditional; state that conditioning mainly supplies real gap structure.

**C8. The clean confirmation protocol should be strict — SEVERITY: major.**
Freeze code and flags; register K/B/universe and Spec-B projection fix; process comments 2021-07..2022-12; estimate on the existing panel or pre-declared rolling windows; evaluate all gates on the extension; no threshold changes after looking. Add a one-page "model discovery chronology" in SI.

---

# D. MODEL-THEORETIC ALTERNATIVES AND FALSIFIABILITY

**D1. Per-entity κ_i heterogeneity remains insufficiently tested — SEVERITY: major.**
Fine-bands rejects deterministic σ(rank), not entity-specific reversion rates. Cheapest test: EB-shrunken per-entity VR/ACF curvature after conditioning on rank and volatility; split-half stability; OOS improvement when κ_i is included.

**D2. Lifecycle-stage mixtures are a serious alternative — SEVERITY: major.**
Classify entities by monotone rise/decline/turning-point shape in train data; test whether held-out VR/RACF residuals concentrate by phase.

**D3. Fractional or long-memory homes can reproduce the demeaning evidence — SEVERITY: major for interpretation. BASIS: PROVEN.**
ARFIMA 0<d<1/2 or scale mixtures of very slow OU have excess near-zero spectral power. Falsification: phase-randomized surrogate panels preserving the empirical spectrum; if surrogates reproduce the demeaning loss and RACF, the lifecycle interpretation is not unique.

**D4. Pooled-moment versus median-entity mismatch can be a mixture problem, not a dynamics problem — SEVERITY: major.**
Align the diagnostic population and missingness model before adding latent dynamics.

**D5. Multiplicative noise in levels versus additive noise in logs remains open — SEVERITY: minor to major in the tail.**
Test: intermittency-aware daily count floor; see whether the weekly σ_obs depth divergence disappears.

**D6. Boundary/rebirth alternatives should be tested with flux, not assumed — SEVERITY: minor.**

**D7. Instrument effects can masquerade as dynamics — SEVERITY: major for FB.**
Era machinery is a strength; make instrument forensics a main SI section.

---

# E. THE PAPER

## E1. Mock referee report

**Referee #2: ranking-dynamics / Barabási-school voice.**
[Contribution potentially substantial; strongest element is decomposition into rank-conditional process + measurement noise + persistent amplitude; b≈1 connects to Q-model. Concern: too narrow for its rhetoric — two platforms; require at least two additional ranked systems, preferably one not social-media. Worry: final model is a sequence of repairs; require frozen confirmation; report discovery vs confirmation.]

**Referee #2: econometrician voice.**
[Covariance-structure estimator plausible; D(h) addition well motivated. Inference incomplete: no standard errors on knot curves, identity weighting over heterogeneous-precision moments, threshold scorecard treated as inferential. Largest technical objection: the Spec-B Toeplitz/centering projection issue — floor not uniquely identified without a convention; must be corrected or bounded. Recommendation: revise.]

## E2. 150-word abstract (verbatim)
Digital attention rankings are stable in shape but unstable in membership: the same ranks persist while the pages, subreddits, and endpoints occupying them churn. We model this "Eulerian stability with Lagrangian churn" using a rank-conditional stochastic process with rebirth at the lower boundary, independently identified measurement noise, and one persistent entity-level amplitude. Across Facebook pages and Reddit subreddit attention, weekly log activity is well described by a slow rank-dependent home, short-run transitory movement, a stationary platform level, and lognormal endpoint temperament. A horizon-dispersion test shows that the same amplitude scales transitory and permanent movement to first order (b≈1), with small super-unit deviations. The model reproduces rank-size stability, occupant turnover, and held-out displacement distributions, beating or matching persistence baselines in rolling-origin tests. The remaining discrepancies concentrate in measurement alignment and lifecycle-scale movement. The results suggest a parsimonious factorized law for the head of digital attention rankings.

## E3. Figure plan
Fig 1: Eulerian stability, Lagrangian churn (rank-size spaghetti; collision rates; outflux/return).
Fig 2: Identification, not just fit (γ tail; flat-to-V SSE(a); D(h) curves incl. 26/52).
Fig 3: Measurement noise from daily replication (Spec-B curve; Spec-A vs Spec-B with head-identified/tail-bracketed shading; before/after pin).
Fig 4: Entity-amplitude collapse (corrected log-variance distribution vs lognormal; s(h) flatness; b with CI vs b=1).
Fig 5: OOS movement gate (held-out displacement distributions vs model vs persistence; split trajectory; coverage/Wasserstein).
SI-1 instrument forensics; SI-2 universe/membership; SI-3 covariance derivations/identifiability; SI-4 Spec-B derivation + projection audit; SI-5 scorecard definitions + bands; SI-6 OOS by split; SI-7 rejected alternatives + sequential log.

## E4. Breadth
Minimum to defuse n=2: two additional systems through the unchanged pipeline, at least one outside social media. Stronger: four (Wikipedia pageviews, GitHub stars, app-store charts, YouTube/podcasts). Key is the same factorized architecture and amplitude collapse, not 15/15 everywhere.

## E5. Language audit
- "Every parameter identified; nothing tuned" → acknowledge train-only calibration and head-only Spec-B identification.
- "b=1 is the law" → "first-order factorization b≈1; exact b=1 statistically rejected; deviations small."
- "σ_obs is identified" → "identified/bracketed at the head; below the head requires sensitivity bounds."
- "Facebook 15/15" → in-sample under working stack; OOS uses simpler calibrated stack; report separately.
- "Comments passes/beats" → at par with 100% coverage; submissions beats 4/5.
- "Census" → Reddit/Pushshift census of available panel; FB "of tracked activity."
- README "s ≈ 0.9 on both platforms" → stale; metric-dependent.

---

# F. PRIORITIZED RECOMMENDATIONS

F1 Fix and bound Spec-B projection (low cost, very high payoff).
F2 Freeze one paper-primary stack per estimand; run through all gates.
F3 Reserve unprocessed comments extension as confirmation.
F4 Replace 15/15 pass counts with uncertainty-aware diagnostics.
F5 MC bands on all head-collision/churn rows.
F6 Re-estimate b after entity detrending + block-time uncertainty.
F7 Population-matched scoring / missingness alignment.
F8 Add 2–4 new ranked systems before submission.
F9 Do not add lifecycle states yet.
F10 Tighten all FB wording to "of tracked activity."

---

# APPENDIX R: RUNS REQUIRED

R1 Spec-B projection audit (all three platforms; compare pᵀΣp vs pᵀCΣCp by band; decision rule: same qualitative head/tail conclusions or weaken "identified").
R2 FB full in-sample stack through OOS (facebook_a --oos + full stack flags).
R3 Comments long-horizon stack through OOS (md-vr-long + two-scale + mix + conditional).
R4 MC bands for FB head collisions (--reps 30, full stack).
R5 b=1 detrending bound (add per-entity detrending before estimate_mix_b; comments should move toward 1 more than FB if lifecycle drift biases b).
R6 Frozen confirmation on unprocessed comments extension (aggregate 2021-07..2022-12 from the WD drive, then frozen gates).
R7 Population-matched scoring (apply empirical observation/missingness floor to sims; prediction: comments VR block narrows without new dynamics).
R8 Breadth smoke tests (new PLATFORMS entries; s, b, OOS vs persistence, boundary flux; at least two systems should show the same qualitative collapse).

# APPENDIX D: DERIVATIONS
D1 Change autocovariances (verifies code formulas). D2 Identifiability as
mixture-of-exponentials/Prony (conditions + ill-conditioning). D3 MD estimator
consistency conditions, boundary asymptotics, weighting. D4 Temperament
correction chain. D5 Leading bias of b (mixture-slope formula). D6 Demeaned VR
as spectral functional with zero-frequency notch. D7 Spec-B projection
nonidentification (CJC=0 null direction; invariant floor pᵀCΣCp).
