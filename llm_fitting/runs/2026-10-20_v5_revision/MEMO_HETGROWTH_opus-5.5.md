<!-- Provenance: methods memo on heterogeneous mean growth (brief BRIEF_HETEROGENEOUS_GROWTH_METHODS.md), Claude Opus 5.5 sub-agent (Claude Code; WebSearch/WebFetch), 2026-10-20. Saved verbatim by the lead agent. Note: its section 9 says no 2026 journal version of Mallmin et al. was found; the round-6 verification (VERIFICATION_GPT_QUOTES_AND_REFS.md, R6) confirmed Ecology Letters 29(2):e70333 via Crossref. -->

# Heterogeneous mean growth: standard specifications, and a recommended design for R12

*Methods memo. Claude Opus 5.5 (Anthropic), 2026-10-20. This is literature research only. Nothing was run, no data file was opened, and nothing in `SEALED_PATHS.json` was touched. No repository file was modified; the only files written were temporary page images in the session scratchpad. Numbers about our panels are quoted from the brief and from `paper/pnas_si.tex` (Persistence §, decomposition table). Each work carries an access label: [full text], [abstract only], [secondary] or [memory, unverified]. Every quotation from a source has been paraphrased. Sections 1–6 report what the sources say. Sections 7–8 are my derivations and proposals, and are marked that way.*

---

## 0. Summary

1. **Every tradition uses the same basic object.** Each unit has a growth component that is fixed, or slowly varying, on top of a random walk and short-memory noise. That component is identified from covariances between growth over periods that do not overlap, at lags beyond the memory of the transitory parts. Our gapped statistic is the long-difference version of the standard HIP moment. That moment is Cov(Δy_t, Δy_{t+k}) = σ_β² for k beyond the transitory order (Hryshko 2012 eqs. 8 and 12; Guvenen 2009 eq. 7).
2. **The standard estimator is equally weighted minimum distance (EWMD)** on the autocovariance function. Altonji & Segal (1996) found that optimal weighting is biased toward zero in small samples; Guvenen 2009, Hryshko 2012, Baker & Solon 2003 and Pugsley–Sedláček–Sterk all follow this. When selection or non-normality matters, the standard is simulated moments or indirect inference, applying the identical sample selection to the simulated data (GKOS 2021; PSS 2021; Guvenen & Smith 2014).
3. **Distribution.** Covariance matching identifies only the second moments; Hoffmann 2019 calls it semiparametric. When simulating, the convention is a zero-mean normal component (Hryshko; GKOS; PSS). Its covariance with the initial level is estimated freely and is usually negative (corr ≈ −0.2 to −0.5). Regime models use two types (GLLM 2016).
4. **Persistence.**
   - Life-cycle work uses fixed heterogeneity (HIP): finite working lives keep the cross-section stationary.
   - Stationary worlds use decaying heterogeneity: Markov switching (GLLM), geometric convergence to heterogeneous steady states (PSS; Luttmer 2011), or an Ornstein–Uhlenbeck growth rate (Mallmin, Traulsen & De Monte, arXiv 2025).
   - An OU process and a symmetric two-state switch with the same rate have the same autocovariance function. Second-moment estimators cannot tell them apart.
5. **Stationary shape versus speed.**
   - Fernholz 2017 (Thm 2.3) holds whatever the name-level heterogeneity: the stationary shape is set by rank-based relative growth and gap volatility.
   - Drift heterogeneity adds nothing to gap volatility, but it does change the measured rank-based growth through who occupies each rank (Fernholz–Ichiba–Karatzas 2013, eq. 3.3).
   - So R12 leaves the stationary slope unchanged only if the drift by rank is re-solved to reproduce the measured slides.
   - GLLM's result is that heterogeneity changes the speed of transitions.
6. **Three warnings for R12** (my derivations, §7):
   - **Bias in lasting variance.** Under fixed heterogeneity our lasting-variance estimator returns σ² + 3σ_b²d, where d is the block spacing and σ_b the weekly drift spread. At a spread of 0.3 per year and d = 52 weeks this adds about 0.005 a week. That may be a large fraction of the measured lasting variance. This is G15, and it also feeds into c_k and β.
   - **Fixed normal heterogeneity is not stationary at the measured spread.** It breaks the second-order stability condition: a name more than about one standard deviation above the mean outgrows the head's slide. Thirty-year runs therefore need decaying (or bounded) heterogeneity.
   - **The platform factor includes the head itself.** On submissions the top 78 communities hold up to half the activity, so a loading estimated on the platform total partly measures the head's own movement. Use a leave-one-out factor.
7. **Recommendation.** Treat R12 as a second-order (hybrid Atlas) version of the model, in three variants:
   - **(a)** fixed normal b_i, calibrated to the covariances at gaps 0 and 13;
   - **(b)** an AR(1)/OU drift, calibrated to the full lag profile;
   - **(c)** heterogeneous loadings on a leave-one-out platform factor, calibrated only to 2019 weekly co-movement. This makes the gapped statistic a genuine out-of-sample test for (c).

   In all three, re-solve g so the simulated slide profile matches the data, and re-estimate σ₀ and β by indirect inference. Validate by recovery and misspecification runs in the style of Hryshko and Guvenen.

---

## 1. Specifications by tradition (from the sources)

### 1.1 Income dynamics: HIP against RIP

**Guvenen 2009** [full text: NBER WP 13394, 2007].
- **Model.** Log income y_h^i = β^i h + z_h^i, with z_h^i = ρ z_{h−1}^i + η_h^i (eq. 1, p. 2). The general form adds α^i + β^i h with zero-mean (α, β), variances σ_α², σ_β² and covariance σ_αβ, plus an AR(1) term and an i.i.d. transitory term, both with time-varying loadings (eq. 2, pp. 5–7).
- **Moments** (eq. 3, p. 7):
  - var = σ_α² + 2σ_αβ h + σ_β² h² + var(z_h) + …
  - cov(ŷ_h, ŷ_{h+n}) = σ_α² + σ_αβ(2h+n) + σ_β² h(h+n) + ρⁿ var(z_h)
- **In differences** (eq. 7, p. 24): cov(Δy_h, Δy_{h+n}) = σ_β² − ρ^{n−1}[(1−ρ)/(1+ρ)]σ_η² for n ≥ 2.
- **Estimates** (Table 1 row 4, PSID): ρ = 0.82 under HIP against 0.99 under RIP; σ_β² = 0.00038; corr(α, β) = −0.23 (not significant), and −0.70 for college graduates.
- **Weighting.** Identity weighting following Altonji & Segal, with each moment weighted by its own number of observations (App. B).
- **Sample.** At least 20 years of data per man; a 4-year rule moves the estimates little (row 7).
- **Identification** (§4): from the convexity of the variance profile in age and from the shape of lag profiles. Higher-order autocovariances are informative, but fewer people contribute to them, which raises selectivity concerns. Using only the first 10 lags raises ρ to 0.90 (row 8).
- **On earlier tests** (§5): a HIP process with ρ < 1 produces negative autocovariances of growth up to about lag 11. In a Monte Carlo at the PSID's sample size (≈27,700 observations), no autocovariance up to lag 18 is significant even though HIP is true (Table 3). Earlier rejections of HIP were therefore uninformative.

**MaCurdy 1982; Abowd & Card 1989** [secondary via Guvenen §5 and Hryshko §1; Abowd–Card abstract]. These are the RIP side. MaCurdy tested whether autocovariances of income growth at lags ≥ 2 are positive and did not reject σ_β² = 0. Abowd & Card found that growth is MA(2) (a random walk plus MA(1) transitory term plus measurement error) and tested lags 2–10 jointly. Guvenen (p. 27) notes that their sum-of-squares test cannot tell the sign of a deviation.

**Lillard & Weiss 1979; Hause 1980; Baker 1997; Haider 2001** [abstracts plus secondary; Lillard–Weiss NBER WP 121, pp. 4–11, scanned and seen].
- Lillard & Weiss: earnings have a persistent individual effect in level, a persistent individual effect in growth, and a serially correlated transitory part, estimated by maximum likelihood.
- Hause: a variance-components model tied to on-the-job training.
- Baker: the profile-heterogeneity model fits better than a unit-root model, and MaCurdy's test has low power.
- Haider: σ_β² = 0.00041 and ρ = 0.64 (per Guvenen p. 12).

**Baker & Solon 2003** [full text: 1998 WP]. The encompassing specification:
- y_ibt = p_t[α_ib + β_ib(t−b−26) + u_ibt] + ε_ibt, where u is a random walk, ε is AR(1) with loading λ_t, and the innovation variance is quadratic in age (eqs. 6–9, p. 17).
- Fitted by EWMD to 2,077 autocovariance moments, citing Altonji–Segal (pp. 20–21).
- **Both the random walk and growth heterogeneity are significant.** σ̂_β = 0.0095 per year, and σ_αβ is significantly negative (p. 23).
- Their remark: earlier studies had too little data and had to choose one or the other.

**Hryshko 2012** [full text].
- **Model** (eqs. 1–3, p. 180): y = α_i + β_i h + p (random walk) + τ (θ(L)ε) + measurement error.
- **In differences** (eq. 4, p. 184), γ_k = E[Δy_t Δy_{t−k}]:
  - with an MA(1) transitory term, γ_k = σ_β² for k ≥ 3 (eqs. 5–8);
  - with an AR(1) transitory term, γ_k = σ_β² − φ^{k−1}(1−φ)/(1+φ)σ_ε² (eq. 12).
- **Separability.** The measurement-error variance and the MA coefficient are not separately identified (p. 185).
- **Misspecification result.** If the truth is a random walk and HIP is fitted, σ̂_β² ≈ σ_ξ²/T (eq. 14, p. 186).
- **Monte Carlo.** It mimics the PSID design (1,916 people, 29,753 person-years, at least 9 consecutive observations), assumes random attrition (fn. 6, citing Fitzgerald, Gottschalk & Moffitt 1998), runs 100 replications and uses EWMD (fn. 9: most of the literature uses identity weighting).
- **Result.** σ̂_β² = 0 in the PSID; the random-walk model is not rejected.

**Hoffmann 2019** [full text, partly read].
- **Main result** (abstract, p. 1279): age-dependent heteroskedasticity can rationalise any age pattern in covariance structures. If it is omitted, HIP is biased upward, by a factor of 13 in German administrative data.
- **What credibly identifies HIP:** the shape of the tails of lag profiles. That requires long individual series and low attrition.
- **Method notes:** EWMD is nonlinear least squares, so identification can be checked by collinearity (p. 1281). Covariance matching makes no assumption about the distribution of β_i (fn. 12, p. 1286).

**Guvenen, Karahan, Ozkan & Song 2021** [full text: NBER WP 20913, 2015; published version not seen].
- **Model** (§5.1, eqs. 2–8, pp. 30–31): y = α + βt + γt² + three AR(1) components, each receiving innovations with a probability that depends on age and lagged earnings, + ε. (α, β, γ) is multivariate normal.
- **Estimation.** Simulated method of moments on 1,432 moments with scaled deviations and fixed weights by moment group (eq. 9, p. 33). They simulate 200,000 people and apply exactly the same sample-selection criteria to the simulated data (p. 32).
- **Estimates.** corr(α, β) ≈ −0.50 (§6). The linear HIP fits almost as well as the quadratic.

### 1.2 Firm dynamics

**Pugsley, Sedláček & Sterk 2021** [full text: CES WP 18-30, 2018; AER version abstract only].
- **Model** (eq. 1, p. 8): ln n_{i,a} = u + v (ex ante) + w + z (ex post).
  - u_{i,a} = ρ_u u_{i,a−1} + θ_i
  - v_{i,a} = ρ_v v_{i,a−1}
  - w_{i,a} = ρ_w w_{i,a−1} + ε (an AR(1) of ex-post shocks)
  - z i.i.d.
- **Interpretation.** Each firm moves toward its own steady state θ_i/(1−ρ_u). Ex-ante growth heterogeneity therefore decays geometrically with age. The long-lag autocovariance tends to (1−ρ_u^{h+1})σ_θ²/(1−ρ_u)² (eq. 2 and p. 10).
- **Estimation.** EWMD on the upper triangle of the autocovariance matrix by age, for a balanced panel of firms surviving to age 19 (p. 11). Autocorrelations in the balanced and unbalanced panels are almost the same (p. 6).
- **Structural model.** Exit is endogenous. It targets the balanced-panel autocovariances plus the age profiles of exit and average size: 249 moments, 10 parameters (§3). In it, selection makes the ex-ante and ex-post parts negatively correlated.
- **Estimates** (Table 1): ρ_u ≈ 0.21, σ_θ ≈ 0.55–0.60.

**Luttmer 2011** [abstract only].
- Gibrat-type growth driven by many independent shocks implies that very large firms would be centuries old.
- In his model, firms with better ideas grow faster, and their growth slows as idea quality falls back toward the average. That is decaying heterogeneity in mean growth.
- GLLM (fn. 2) summarise him as needing persistent heterogeneity in mean growth to explain why large firms are young.

**Haltiwanger, Jarmin & Miranda 2013** [full text: NBER WP 16300].
- Classifying firms by base-year size biases the size–growth relation toward a negative one through regression to the mean. End-year classification biases it the other way. Davis–Haltiwanger–Schuh classification by average size is the compromise (pp. 6–7).
- The DHS growth rate, (E_t − E_{t−1})/(0.5(E_t + E_{t−1})), accommodates entry and exit and is now standard (§3.2).
- This is the firm-dynamics form of our permanent-rank rule.

**Selection corrections** [abstracts].
- **Hall 1987:** models exit with size and Tobin's Q, and corrects growth regressions for selection.
- **Evans 1987:** results are robust to assumptions about sample censoring.

### 1.3 Inequality dynamics

**Gabaix, Lasry, Lions & Moll 2016** [full text: NBER WP 21363, 2015].
- **Model** (eq. 29, p. 24): dy = μ_j dt + σ_j dZ + jumps + injection − death, with growth regimes j and switching rates φ_{j,k}.
- **Two-regime case** (eq. 31, p. 26): a fraction θ of entrants starts in the high regime H; H switches to L at rate ψ; L is absorbing apart from death at rate δ.
- **Transitions** (Prop. 4) run on two time scales:
  - λ_H(ξ) = ξμ_H − ξ²σ_H²/2 + ψ + δ
  - λ_L(ξ) = ξμ_L − ξ²σ_L²/2 + δ
  - With ψ large, the short run is fast.
- **Calibration** (p. 31; §5.4 continues on p. 32):
  - δ = 1/30 and σ_H = 0.15;
  - ψ = 1/6 is assumed (a 6-year expected stay), with sensitivity runs at 1/3 and 1/12;
  - μ_H is set to match a Pareto exponent of 2.56;
  - μ_H − μ_L = 0.06.
- They propose Hamilton (1989) regime-switching estimation on panel data as future work. Their parameters are calibrated, not estimated.

**Gabaix 2009** [full text: NBER WP 14299].
- With size-dependent g(S) and v(S), the local Zipf exponent is ζ(S) = 1 − 2g(S)/v²(S) + (S/v²) ∂v²/∂S, with g measured relative to the overall mean (§3.4.3, eqs. 24–26).
- A text search found no name-specific growth types. Its relevance here is the Eulerian counterpart of Fernholz's result.

**Gomez 2023** [full text of the 2019 WP "Displacement and the rise in top wealth inequality"; published version abstract only].
- **Group heterogeneity in drift** (Prop. 3, eqs. 10–11, p. 11) enters the *within* term.
- **Heterogeneous exposure σ_n to aggregate shocks** (Prop. 4, eqs. 12–13, p. 12) adds the variance of exposures at the threshold to the *displacement* term: ((ζ−1)/2)(E[ν²|q] + Var[σ_n|q]).
- **Measurement error** (App. C.2): transitory measurement error biases displacement by ≈ −2ρ, where ρ is the AR(1) coefficient of growth.

**Fagereng, Guiso, Malacrino & Pistaferri 2020** [abstract only]. Returns to wealth have a persistent individual component. The standard deviation of average returns is about 22%. Returns are correlated across generations.

### 1.4 Rank-based models and stochastic portfolio theory

**Ichiba et al. 2011, hybrid Atlas** [full text, arXiv].
- **Model** (eq. 2.1, p. 4): dY_i = (Σ_k g_k 1{rank k} + γ_i + γ)dt + Σ_j ρ_ij dW_j + Σ_k σ_k 1{rank k} dW_i.
- **Components:** rank-based drift g_k, fixed name-based drift γ_i, a common drift γ, name-based correlations ρ_ij (common shocks) and rank-based volatility.
- **Stability:** Σg_k + Σγ_i = 0 (eq. 2.2).

**Fernholz, Ichiba & Karatzas 2013, second-order model** [full text, arXiv].
- **Model** (eq. 3.1, p. 7): d log X_i = (γ_i + g_rank)dt + σ_rank dW_i.
- **Stability.** For every permutation π, the partial sums Σ_{k≤m}(g_k + γ_π(k)) must be negative.
- **Observed rank-based growth** is ĝ_k = g_k + Σ_i θ_ki γ_i (eq. 3.3), where θ is the occupation-time matrix. Also γ = −θ′g (eq. 3.5).
- **Estimation.** g comes from forward and backward "flows" and expected ranks four years on (§4). γ_i is the time-averaged growth net of rank drift, averaging forward and backward time (eq. 4.7, p. 11). Point estimates for six large US stocks, 1990–99, run from −1.67% to +0.26% a year (Table 1). No intervals are given. Only stocks with a full ten-year history are used (p. 9).
- **Motivation.** First-order models are ergodic; second-order models keep large stocks large.

**Fernholz 2017** [full text: peer-reviewed version].
- **Definitions:** α_k is the time-averaged growth of the rank-k occupant relative to the population (eq. 2.17). σ_k² is the time-averaged variance of the gap between adjacent ranks (eq. 2.18). σ_k includes any shock that moves neighbours differently (p. 10).
- **Theorem 2.3** (eq. 2.21, p. 11): a stationary distribution exists iff α_1 + … + α_k < 0 for all k, and then E[log θ_(k) − log θ_(k+1)] = σ_k²/(−4(α_1 + … + α_k)).
- The abstract says this holds however growth rates and volatilities vary across agents.

### 1.5 Ecology

**Kalyuzhny, Kadmon & Shnerb 2015** [full text: arXiv 1406.2926 preprint].
- **Model** (eq. 1, p. 3): an offspring belongs to species k with probability N_k f_k / Σ N_i f_i.
- **Fitness:** every τ steps each species' fitness is redrawn independently from a lognormal with mean 1 and variance A. Fitness is piecewise constant and equal on average across species ("time-averaged neutral").
- **Test:** against Barro Colorado Island fluctuations, fluctuation scaling and the decay of similarity over time.

**Mallmin, Traulsen & De Monte** [full text: arXiv 2505.01376 v2, 3 Dec 2025, read through the fetch tool's summary of the HTML, so check the equations before quoting].
- **Model:**
  - dn_i/dt = n_i[r_i(t) − μN − εn_i] + λ (eq. 1);
  - τ dr_i/dt = −(r_i − r_i*) + √(2σ_r²τ) dW_i (eq. 3), an OU fitness;
  - γ = 2σ_r²τ (eq. 4).
- **Heterogeneity:** by default all r_i* are equal; a heterogeneous variant draws r_i* uniformly.
- **Correlation time:** with γ held fixed, τ matters little for the main trends.

**Grilli 2020** [full text, PMC, via tool summary].
- **Model:** the stochastic logistic model with environmental white noise, dx_i/dt = (x_i/τ_i)(1 − x_i/K_i) + √(σ_i/τ_i) x_i ξ_i(t). I wrote this form from memory; the tool's transcription of the noise term was garbled, so verify before quoting.
- **Heterogeneity is in carrying capacity K_i (lognormal)**, not in growth rate.
- **Fitting:** σ from Taylor's law, τ ≈ 19 h from how fast the conditional mean decays, then validation on untargeted patterns.

### 1.6 Finance: momentum as persistence

**Lo & MacKinlay 1990** [full text: NBER WP 2977, scanned pp. 6–8].
- **Decomposition** (eqs. 3.4–3.9, p. 8): E[π_t(k)] = C_k + O_k − σ²(μ).
  - σ²(μ) = (1/N)Σ(μ_i − μ_m)², the cross-sectional variance of mean returns;
  - O_k = −((N−1)/N²) tr(Γ_k), the own autocovariances;
  - C_k = (1/N²)[ι′Γ_kι − tr(Γ_k)], the cross-autocovariances.
- For momentum the signs flip, so dispersion in mean returns adds +σ²(μ) at every lag.
- The three terms map onto our variants (a)–(c).

**The fixed-dispersion debate** [abstracts; JT 2001 NBER abstract page seen].
- Conrad & Kaul 1998 attribute momentum to σ²(μ).
- Jegadeesh & Titman 2002 find small-sample bias in those tests. Corrected, cross-sectional differences in expected returns explain little.
- Jegadeesh & Titman 2001 find reversals 4–5 years after formation, which rejects a pure dispersion explanation. **That is a lag-profile test.**

**Factor exposure** [abstracts].
- Grundy & Martin 2001: momentum portfolios carry time-varying factor exposures, and ranking on stock-specific components pays more.
- Blitz, Huij & Martens 2011 ("residual momentum"): ranking on residuals from 36-month factor regressions halves the time-varying exposure.
- Heterogeneous exposure of individual earnings ("worker betas"): Guvenen, Schulhofer-Wohl, Song & Yogo 2017 [abstract only].

### 1.7 Summary table

| Tradition | Key specification (eq./page) | Distribution | Persistence | Estimator | Key references (access) |
|---|---|---|---|---|---|
| Income, HIP | α_i + β_i h + AR(1) + i.i.d. (Guvenen eqs. 1–3, pp. 2–7) | zero-mean (α, β) with σ_αβ free; normal in simulations | fixed for life | EWMD on the levels autocovariance matrix (App. B); ML (Lillard–Weiss) | Lillard–Weiss 1979 [abs + sec]; Hause 1980 [abs]; Baker 1997 [abs + sec]; Haider 2001 [abs + sec]; Guvenen 2009 [full] |
| Income, RIP | random walk + MA(1)/ARMA + measurement error; growth MA(2) | — | — | MD on autocovariances of differences; lag ≥ 2 tests | MaCurdy 1982 [sec]; Abowd–Card 1989 [abs + sec] |
| Income, encompassing | HIP + random walk + transitory (Hryshko eqs. 1–12; Baker–Solon eqs. 6–9) | normal in Monte Carlo; semiparametric in estimation | fixed | EWMD in differences; Monte Carlo matching the sample design | Baker–Solon 2003 [full WP]; Hryshko 2012 [full]; Hoffmann 2019 [full] |
| Income, rich | α + βt + γt² + mixture of AR(1)s (GKOS eqs. 2–8) | multivariate normal; corr(α, β) ≈ −0.5 | fixed | MSM, 1,432 moments, same selection in simulation | GKOS 2021 [full WP]; Guvenen–Smith 2014 [abs] |
| Firms | ex-ante u = ρ_u u + θ_i, plus AR(1) and i.i.d. (PSS eqs. 1–2) | i.i.d.; normal in the structural model | geometric decay toward heterogeneous steady states | EWMD on autocovariances by age (survivors), then a structural model with exit | PSS 2021 [full WP]; Luttmer 2011 [abs]; HJM 2013 [full WP]; Hall 1987, Evans 1987 [abs] |
| Inequality | regimes μ_j, σ_j with switching ψ (GLLM eqs. 29, 31) | two types | Markov switching; L absorbing | calibration (ψ assumed, μ_H set by the tail) | GLLM 2016 [full WP]; Gomez 2023 [full WP 2019]; Gabaix 2009 [full WP]; Fagereng et al. 2020 [abs] |
| Rank-based (SPT) | rank drift g_k + name drift γ_i + correlations ρ_ij (Ichiba eq. 2.1; FIK eq. 3.1) | estimated name by name | fixed | ĝ = g + θγ; flows; time-averaged residual growth | Ichiba et al. 2011 [full]; FIK 2013 [full]; Fernholz 2017 [full] |
| Ecology | fitness redrawn every τ (Kalyuzhny eq. 1); OU fitness (Mallmin eq. 3); heterogeneous K (Grilli) | lognormal redraws; normal OU; uniform r*; lognormal K | renewal / OU / fixed level | fit to fluctuation, abundance and turnover patterns | Kalyuzhny 2015 [full preprint]; Mallmin et al. 2025 [full arXiv]; Grilli 2020 [full] |
| Finance | E[π] = C_k + O_k − σ²(μ) (Lo–MacKinlay eq. 3.9) | — | fixed dispersion against time-varying exposure | post-holding lag profile; residualising factors | Lo–MacKinlay 1990 [full WP]; Conrad–Kaul 1998, JT 2002, Grundy–Martin 2001, Blitz et al. 2011 [abs]; JT 2001 [abs] |

---

## 2. The distribution of the heterogeneous component (Q2)

**What the sources show:**
- **Estimators are semiparametric.** Covariance matching identifies only σ_β² and σ_αβ (Hoffmann fn. 12).
- **Simulations use the normal.** Hryshko's Monte Carlo (Table 1), GKOS's multivariate-normal (α, β, γ) and PSS's structural model all draw normal components.
- **Discrete types appear where the question is regimes:** GLLM's high and low growth regimes, and Gomez's groups.
- **Ecology varies:** uniform r* (Mallmin), lognormal fitness redraws (Kalyuzhny), lognormal carrying capacities (Grilli).
- **The normal is chosen for convenience.** Only second moments are identified, and it is easy to simulate.

**Covariance with the initial level:**
- **In income dynamics it is estimated freely and is usually negative:**
  - significantly negative in Lillard–Weiss, Hause, Baker and Baker–Solon (Baker–Solon p. 23);
  - corr −0.23 overall in Guvenen, not significant, and −0.70 for college graduates;
  - about −0.50 in GKOS.
  - The usual interpretation is human-capital investment: low early earnings bought with faster growth.
- **In rank-based work the sign goes the other way.** FIK's estimates suggest that names at higher ranks have higher γ, which is what keeps them there.
- **Our setting differs.** The relation between level and growth *across* ranks is already the drift by rank g(r). Only the covariance *within* rank strata is a free choice.

## 3. Persistence: fixed, AR(1)/OU, or switching (Q3)

**What the sources show:**
- **Fixed heterogeneity** is used in the HIP literature, in the hybrid Atlas and second-order models, and in fixed-effect returns. Finite lifetimes make it workable: working lives in income, death and retirement at rate δ in GLLM.
- **Decaying heterogeneity takes three forms:**
  - geometric convergence to heterogeneous steady states (PSS, rate ρ_u ≈ 0.2 a year; Luttmer's quality reversion);
  - Markov switching (GLLM, ψ = 1/6 a year assumed);
  - OU or renewal fitness (Mallmin; Kalyuzhny).
- **Not to be confused:** an AR(1) with ρ < 1 in income *levels* is the transitory part, not growth heterogeneity.

**How papers choose:**
- **Fit of nested specifications to the autocovariance function:** GKOS compare objective values (16.65 against 17.17 for quadratic against linear HIP); PSS compare RMSE against an AR(1) model (0.010 against 0.039).
- **Misspecification Monte Carlo:**
  - fitting RIP when HIP is true biases ρ upward (Guvenen §2.3);
  - fitting HIP when the truth is a random walk gives σ̂_β² ≈ σ_ξ²/T (Hryshko eq. 14);
  - leaving out age effects inflates HIP (Hoffmann).
- **Long-lag tests:** MaCurdy and Abowd–Card, and the post-holding-period reversals of JT 2001.
- **External requirements:** GLLM chose switching to get fast transitions. Stationarity rules out unbounded fixed drift in an infinite-horizon world (the FIK stability condition).

**My derivation (not from a source).** An OU drift with rate κ and a symmetric two-state switch with total switching rate κ both have autocovariance ∝ e^{−κ|t|}. Minimum distance on second moments cannot tell them apart. Asymmetric statistics can.

## 4. Estimation (Q4)

**What the sources show:**
- **Minimum distance on the autocovariance function** (Chamberlain 1984 [sec]) is the standard.
  - **Equal weights.** Altonji & Segal (1996; NBER abstract) found that optimal weighting is biased toward zero in small samples, because sampling errors in the moments are correlated with those in the weighting matrix. EWMD usually has the lower RMSE.
  - **Inference:** sandwich standard errors (Guvenen App. B), fourth-moment matrices (Baker–Solon), or the spread across Monte Carlo replications (Hryshko).
  - **Differences or levels.** Hryshko prefers first differences because levels depend on initial conditions (p. 183); Guvenen uses levels.
- **Likelihood:** Lillard–Weiss used maximum likelihood. The state-space form of a local linear or damped trend (Harvey 1989 [memory, unverified]) is the natural likelihood route, but it rests on Gaussian errors.
- **Simulation-based estimation:** MSM in GKOS, and indirect inference (Gourieroux, Monfort & Renault 1993; Guvenen & Smith 2014 [abs]). These are used when moments cannot be written in closed form, or when selection, non-normality and the estimator itself must be reproduced.

**Transitory parts and measurement error, estimated jointly:**
- Classical measurement error is absorbed into the i.i.d. transitory term; autocorrelated error is absorbed into the AR(1) (Guvenen p. 7).
- Measurement-error variance and the MA coefficient are not separately identified (Hryshko p. 185).
- Non-stationarity is handled with time-varying loadings on the persistent and transitory components (Baker–Solon p_t and λ_t; Guvenen's time effects).

**Short panels and power:**
- **Identification comes from long lags,** which are noisy and selected (Guvenen §4; Hoffmann: long series and low attrition are needed).
- **A random walk masquerades as HIP in proportion to 1/T** (Hryshko eq. 14).
- **The sign tests have low power:** none of the first 18 autocovariances is significant at PSID size even when HIP is true (Guvenen Table 3).
- **Heteroskedasticity can mimic HIP in variance profiles** (Hoffmann).

## 5. Selection (Q5)

**Income:**
- Long-presence rules: at least 20 years (Guvenen), at least 9 consecutive observations (Hryshko).
- Attrition is assumed random, on the evidence of Fitzgerald, Gottschalk & Moffitt (via Hryshko fn. 6).
- Guvenen warns that the long-lag moments that identify HIP come from a shrinking, possibly selected group.

**Firms:**
- Model exit and correct for it: Hall 1987 (Tobin's Q); Evans 1987 (robustness to censoring).
- Compare a balanced panel of survivors with the unbalanced panel, then fit a structural model with endogenous exit that targets the survivors' autocovariances together with exit and size by age (PSS).
- Use average-size classification and growth rates that include exit (DHS; HJM).

**Rank-based work:** FIK keep only full-history stocks.

**The general rule across these works:** reproduce the selection in the model and compute the statistics identically (GKOS p. 32; PSS §3).

**Our practice already matches this standard.** We use permanent rank on a disjoint block (the DHS logic), an absence-penalised universe, and identical code for data and simulations.

## 6. Calibrating and validating simulated heterogeneous worlds (Q6)

**What published papers do:**
- **GKOS:** MSM over life-cycle, cross-sectional and impulse-response moments, 200,000 simulated people, the same selection, and a global optimiser.
- **PSS:** 10 parameters fitted to 249 moments.
- **GLLM:** fix one parameter (ψ), calibrate another (μ_H) to a target (the tail exponent), and report sensitivity over ψ.
- **Recovery Monte Carlo:** replicate the sample design and estimate known parameters (Hryshko, 100 replications; Guvenen Table 3; Hoffmann).
- **Misspecification runs:** estimate the wrong model on data simulated from another (Guvenen; Hryshko).
- **FIK** give no intervals and suggest a jackknife.

---

## 7. What the standard moments mean in our design (my derivations; proposal)

**Notation.**
- b_i is the extra weekly drift; s = 52σ_b is the annual spread.
- Windows are w weeks long and changes span h weeks: D1 = x̄(W2) − x̄(W1), and D2 is the same change starting g weeks after W2 ends.

**Moments under each variant:**
- **(a) Fixed:** Cov(D1, D2) = h²σ_b² (= s² when h = 52) at every g.
  - The random-walk part contributes zero, because the increments do not overlap.
  - The passing parts contribute small negative terms that fade with g.
  - This is Hryshko's γ_k = σ_β², for long differences.
- **(b) AR(1) drift,** b_t = φb_{t−1} + e with variance v_b: Cov ≈ v_b φ^{w+g+1}[(1−φ^h)/(1−φ)]², so Cov(g)/Cov(0) = φ^g.
  - **Comments (illustrative):** the profile 0.135 / 0.136 / 0.108 implies φ⁵² ≈ 0.8, a half-life of about three years.
  - **Submissions:** 0.194 / 0.150 / 0.046 is not a single geometric decay. The 26-week ratio of 0.77 predicts 0.60 at 52 weeks; the data give 0.24. A path-dependent factor term fits that better.
  - **Caveat:** these ratios are of correlations, not covariances, and the gaps may not use matched origins.
- **(c) Factor,** x ⊃ λ_i f_t: Cov(D1, D2) = Var(λ)·Δf₁·Δf₂ for the realised factor path. It is positive while the platform trend continues and negative when it reverses.
- **Implied correlation (a):** ρ ≈ s²/(s² + V_rest), where V_rest is the variance of an annual window change without heterogeneity.

**The lasting-variance estimator under heterogeneity (G15).** The SI estimator is [var(x_c−x_a) − ½(var(x_b−x_a) + var(x_c−x_b))]/(b−a). Block-mean differences carry the drift exactly, so:
- **(a):** with equal spacing d it returns σ² + 3σ_b²d, and in general σ² + σ_b²[(d₁+d₂)² − ½(d₁² + d₂²)]/d₁.
  - **Example:** s = 0.3 a year and d = 52 weeks give a bias of about 0.005 a week. By comparison, σ² ≈ 2cS ≈ 0.008 at c = 0.004 and S ≈ 1.
  - **Signature:** the estimate rises linearly with spacing, which is itself a diagnostic.
- **(b):** the h² term becomes the integrated-OU variance 2v_b(κh − 1 + e^{−κh})/κ², which is smaller.
- **(c):** the bias is Var(λ)[(Δf_ca)² − ½(Δf_ba² + Δf_cb²)]/d. It can be **negative** if the platform total rose and then fell.
- **Knock-on effects:**
  - **On c_k:** predicted crossings inherit the bias, while measured crossings carry the composition term C. A boundary ratio near 1 could be two inflations cancelling. R12 should check this.
  - **On β:** a σ_b that is the same in every stratum attenuates β toward 0. A σ_b that is larger at small sizes inflates it.

**Fernholz in our model.**
- Drift heterogeneity adds no quadratic variation, so σ_k is unchanged.
- It changes α_k through who occupies each rank (FIK eq. 3.3). Holding the measured slides fixed therefore requires re-solving g.
- Our slides are cohort (Lagrangian) quantities. Fernholz's α_k is a rank-occupant (Eulerian) quantity. R12 should report both.

**Stability.**
- At s = 0.3 a year, a 1-sd name has an extra drift of ≈ 0.0058 a week. That is about the head's slide of ≈ 0.0055 a week.
- A 2–3.5 sd name grows away from the universe. With fixed normal b, the FIK permutation condition fails, and a 30-year run would not be stationary.

---

## 8. Recommendation for R12 (proposal)

### 8.0 Common frame

**R12 is a second-order (hybrid Atlas) version of the model:**
- H_i(t+1) = H_i(t) + g̃(r_i(t)) + b_i(t) + σ(H_i)η_i(t)
- x_it = H_it + [λ_i f_t]_(c) + passing parts

**Shared steps:**
1. **Re-solve g̃** so the simulated cohort slide profile matches the data's. This applies FIK's ĝ = g + θγ logic and avoids counting drift twice.
2. **Re-estimate σ₀ and β by indirect inference.** Choose them so the world's structure-function estimates equal the data's; this is the G15 correction.
3. **Hold everything else fixed:** start from the 2019 ladder, keep the same passing parts, exits and rebirth, and let newborns draw b from the stationary distribution.
4. **Compute** every statistic with the same code and the same universe selection.
5. **Run** 20 seeds with 90% bands.
6. **Write expectations in PLAN.md before running** (project rule; label R12 post hoc, development data).
7. **Report two estimators side by side:**
   - EWMD on covariances (of normal-score-transformed changes, because of heavy tails), with a community-and-origin block bootstrap;
   - indirect inference with the Spearman gapped statistic as the auxiliary statistic.
8. **Estimate σ_b by octave stratum,** as Guvenen does by education group.
9. **Use Facebook as a negative control.** σ̂_b should be ≈ 0 there.

### 8.1 Variant (a): fixed heterogeneity (HIP / second-order model)

- **Specification:**
  - b_i ~ N(0, σ_b²) per ladder, demeaned over the universe (Ichiba eq. 2.2).
  - Independent of level within permanent-rank strata, so σ_αβ = 0 at the start.
  - Sensitivity at within-stratum corr = −0.25 and −0.5 (the range in Guvenen and GKOS) and +0.25 (the FIK sign).
  - Truncate at ±2.5σ_b.
- **Estimator:** EWMD on stratum- and origin-demeaned covariances at gaps 0 and 13, giving s² directly.
- **Calibrate to:** those two covariances only.
- **Test on:**
  - the 26/52-week lag profile (fixed heterogeneity predicts it flat);
  - risers against fallers (normal b predicts symmetry; the data give +0.16–0.19 against 0.00–0.03);
  - R10;
  - the composition term C (data +25–35%);
  - 2- and 3-year turnover and retention;
  - the head's 2020–22 share path;
  - the boundary ratio.
- **Should change:**
  - gapped persistence (positive and flat);
  - the R10 sign;
  - C;
  - multi-year turnover (variance 4s² against 2s² at two years);
  - faster short-run relaxation (GLLM).
- **Should not change:** one-year turnover, if the annual variance is held.
- **Run length:** four years only. **Do not run it for 30 years:** it is non-stationary at this spread (§7).

### 8.2 Variant (b): persistent but decaying (AR(1)/OU growth rate)

- **Specification:**
  - b_i(t+1) = φ_b b_i(t) + √(1−φ_b²) σ_b ξ_i(t), with stationary variance σ_b².
  - This is the second-moment equivalent of GLLM-style switching (§3).
  - Sensitivity variant: a two-state "decline regime" with switching, the only form that can produce the riser/faller asymmetry.
- **Estimator:** EWMD for (σ_b, φ_b) on the covariance profile at g = 0–52 on matched origins, using the closed form in §7 plus the passing-part terms from the existing model, estimated jointly.
- **Calibrate to:** the full lag profile, which is then no longer a test.
- **Test on:** R10, the asymmetry, C, multi-year turnover, the head path, the boundary ratio, and recovery.
- **Should not change:** **the 30-year slope at re-solved g̃ and fixed σ(H) should equal the homogeneous model's within its bands** (Fernholz Thm 2.3). Any difference means a calibration error, or comes from the σ₀ correction.
- **Should change:** faster relaxation of the head (GLLM's two time scales).
- **Identification limit:** half-lives beyond about three years probably cannot be told apart from (a) with these panels. Recovery runs should check this.

### 8.3 Variant (c): heterogeneous exposure to a common factor

- **Specification:**
  - x_it gains λ_i(f_t − f̄), where f_t is the realised log platform total **leaving out community i** (or the universe's equal-weighted mean).
  - λ_i ~ N(λ̄_stratum, σ²_λ,stratum).
  - This is the one-factor case of Ichiba's ρ_ij, and Gomez's Prop. 4.
- **Estimator:** first-stage regressions of weekly changes on Δf in 2019 only (as for the SI's adjustment), with robust regression. Correct for measurement error: σ²_λ = Var(λ̂) − mean(se²).
- **Calibrate to:** 2019 co-movement only.
- **Test on:**
  - **the raw gapped statistic and its lag profile;**
  - its value net of loadings (it should fall to ≈ 0 in the world);
  - the contrast between the ladders (on comments, the world should under-produce persistence);
  - R10 and the head path.
- **Should change:** submissions persistence, following the realised path. Displacement gains Var(λ | threshold)·Var(Δf) (Gomez Prop. 4), and σ_k gains (λ_k − λ_{k+1})²Var(Δf) (Fernholz p. 10). These are small if loadings vary smoothly with rank, and transient when f trends.
- **Recommended combination:** run (b)+(c) together on submissions.

### 8.4 Recovery and misspecification checks (Hryshko and Guvenen style)

1. **Recovery of σ_b:** plant s ∈ {0, 0.15, 0.30, 0.45} a year and estimate it back.
2. **G15:** estimate σ₀, β, c_k and C in worlds with known σ and σ_b.
3. **Misspecification:** fit (a) to (b)-worlds and to (c)-worlds.
4. **False positives:** run homogeneous worlds whose medium-swing half-life is lengthened to 26 weeks or more. Slow reversion biases σ_b downward.
5. **Selection:** compare the gapped statistic with exiters dropped against exiters held at the floor. Truncation on low b attenuates σ̂_b.

### 8.5 Pitfalls

- **Time effects stand in for (c).** The 2020 surge and the July 2021 capture change act like a common factor. Demean by stratum × origin.
- **Passing parts must be estimated jointly.** If they are only assumed negligible, σ_b is biased.
- **Do not identify σ_b from the curvature of the variance profile.** Rank-dependent σ(H) and passing variance mimic h² terms (Hoffmann). Use the covariances of non-overlapping changes.
- **Avoid in-sample community means.** Correlating with means estimated in the same sample creates the small-sample bias Jegadeesh & Titman found in Conrad & Kaul. Keep the windows of R10 disjoint, as they already are.
- **Overlapping origins shrink the effective sample.** Some subsets rest on 1–4 origins. Use a block bootstrap over origins.
- **Exits truncate the distribution of b.** Use identical selection in simulation, or indirect inference.
- **The platform factor needs care.** Use a leave-one-out factor and correct the loadings for measurement error.
- **Stability.** Bound b in (a), and use (b) for long runs.

---

## 9. Corrections to the brief and items I could not verify

- **Mallmin, Traulsen & De Monte:** the OU-fitness paper is arXiv 2505.01376 (v2, December 2025). I found no 2026 journal version. Their 2024 PNAS paper uses a different, Lotka–Volterra, model.
- **"Gomez Appendix C.1" for σ²h + σ_b²h²:** not verified. In the 2019 working paper, Appendix C.1 is about imputing Forbes data. The formula itself is standard HIP algebra (Guvenen eqs. 3 and 5; Hryshko eq. 4).
- **Gabaix 2009:** a text search found no explicit growth types. Its relevant specification is size-dependent g(S) and v(S) (eqs. 24–26).
- **Hryshko's DOI** is 10.3982/QE112.
- **Kalyuzhny's DOI** is 10.1111/ele.12439.
- **Jegadeesh & Titman 2002 authorship:** Crossref lists both authors; IDEAS lists Jegadeesh only.
- **Not seen in full text:** Luttmer 2011, Baker 1997, Haider 2001, MaCurdy 1982, the Conrad–Kaul and Grundy–Martin specifications, and the published versions of PSS, GKOS and GLLM.
- **Equation numbers and pages** in §1 refer to the working-paper versions as labelled.

---

## 10. References

- Abowd, J. & Card, D. (1989). On the covariance structure of earnings and hours changes. *Econometrica* 57, 411–445. doi:10.2307/1912561 [abstract + secondary]
- Altonji, J. & Segal, L. (1996). Small-sample bias in GMM estimation of covariance structures. *JBES* 14, 353–366. doi:10.1080/07350015.1996.10524661 (NBER t0156) [abstract + secondary]
- Baker, M. (1997). Growth-rate heterogeneity and the covariance structure of life-cycle earnings. *JOLE* 15, 338–375. doi:10.1086/209836 [abstract + secondary]
- Baker, M. & Solon, G. (2003). Earnings dynamics and inequality among Canadian men, 1976–1992. *JOLE* 21, 289–321. doi:10.1086/345559 [full text, 1998 WP]
- Blitz, D., Huij, J. & Martens, M. (2011). Residual momentum. *J. Empirical Finance* 18, 506–521. doi:10.1016/j.jempfin.2011.01.003 [abstract]
- Conrad, J. & Kaul, G. (1998). An anatomy of trading strategies. *RFS* 11, 489–519. doi:10.1093/rfs/11.3.489 [abstract]
- Evans, D. (1987). Tests of alternative theories of firm growth. *JPE* 95, 657–674. doi:10.1086/261480 [abstract]
- Fagereng, A., Guiso, L., Malacrino, D. & Pistaferri, L. (2020). Heterogeneity and persistence in returns to wealth. *Econometrica* 88, 115–170. doi:10.3982/ECTA14835 [abstract]
- Fernholz, R., Ichiba, T. & Karatzas, I. (2013). A second-order stock market model. *Annals of Finance* 9, 439–454. doi:10.1007/s10436-012-0193-2 (arXiv:1302.3870) [full text]
- Fernholz, R. T. (2017). Nonparametric methods and local-time-based estimation for dynamic power law distributions. *JAE* 32, 1244–1260. doi:10.1002/jae.2573 [full text]
- Gabaix, X. (2009). Power laws in economics and finance. *Annu. Rev. Econ.* 1, 255–294. doi:10.1146/annurev.economics.050708.142940 [full text, NBER w14299]
- Gabaix, X., Lasry, J.-M., Lions, P.-L. & Moll, B. (2016). The dynamics of inequality. *Econometrica* 84, 2071–2111. doi:10.3982/ECTA13569 [full text, NBER w21363]
- Gomez, M. (2023). Decomposing the growth of top wealth shares. *Econometrica* 91, 979–1024. doi:10.3982/ECTA16755 [full text of 2019 WP; published version abstract only]
- Gourieroux, C., Monfort, A. & Renault, E. (1993). Indirect inference. *JAE* 8, S85–S118. doi:10.1002/jae.3950080507 [Crossref metadata; content from memory]
- Grilli, J. (2020). Macroecological laws describe variation and diversity in microbial communities. *Nat. Commun.* 11, 4743. doi:10.1038/s41467-020-18529-y [full text via tool summary]
- Grundy, B. & Martin, J. S. (2001). Understanding the nature of the risks and the source of the rewards to momentum investing. *RFS* 14, 29–78. https://ideas.repec.org/a/oup/rfinst/v14y2001i1p29-78.html [abstract]
- Guvenen, F. (2009). An empirical investigation of labor income processes. *RED* 12, 58–79. doi:10.1016/j.red.2008.06.004 [full text, NBER w13394]
- Guvenen, F., Karahan, F., Ozkan, S. & Song, J. (2021). What do data on millions of U.S. workers reveal about lifecycle earnings dynamics? *Econometrica* 89, 2303–2339. doi:10.3982/ECTA14603 [full text, NBER w20913]
- Guvenen, F., Schulhofer-Wohl, S., Song, J. & Yogo, M. (2017). Worker betas. *AER* 107(5), 398–403. https://www.nber.org/papers/w23163 [abstract]
- Guvenen, F. & Smith, A. (2014). Inferring labor income risk and partial insurance from economic choices. *Econometrica* 82, 2085–2129. doi:10.3982/ECTA9446 [abstract]
- Haider, S. (2001). Earnings instability and earnings inequality of males in the United States: 1967–1991. *JOLE* 19, 799–836. doi:10.1086/322821 [abstract + secondary]
- Hall, B. (1987). The relationship between firm size and firm growth in the US manufacturing sector. *J. Ind. Econ.* 35, 583–606. https://www.nber.org/papers/w1965 [abstract]
- Haltiwanger, J., Jarmin, R. & Miranda, J. (2013). Who creates jobs? Small versus large versus young. *REStat* 95, 347–361. https://www.nber.org/papers/w16300 [full text, WP]
- Harvey, A. (1989). *Forecasting, Structural Time Series Models and the Kalman Filter.* Cambridge UP. [memory, unverified]
- Hause, J. (1980). The fine structure of earnings and the on-the-job training hypothesis. *Econometrica* 48, 1013–1029. doi:10.2307/1912945 [abstract]
- Hoffmann, F. (2019). HIP, RIP, and the robustness of empirical earnings processes. *QE* 10, 1279–1315. doi:10.3982/QE863 [full text, partly read]
- Hryshko, D. (2012). Labor income profiles are not heterogeneous: evidence from income growth rates. *QE* 3, 177–209. doi:10.3982/QE112 [full text]
- Ichiba, T., Papathanakos, V., Banner, A., Karatzas, I. & Fernholz, R. (2011). Hybrid Atlas models. *Ann. Appl. Probab.* 21, 609–644. doi:10.1214/10-AAP706 [full text, arXiv]
- Jegadeesh, N. & Titman, S. (2001). Profitability of momentum strategies: an evaluation of alternative explanations. *JF* 56, 699–720. doi:10.1111/0022-1082.00342 [abstract, NBER w7159]
- Jegadeesh, N. & Titman, S. (2002). Cross-sectional and time-series determinants of momentum returns. *RFS* 15, 143–157. doi:10.1093/rfs/15.1.143 [abstract]
- Kalyuzhny, M., Kadmon, R. & Shnerb, N. (2015). A neutral theory with environmental stochasticity explains static and dynamic properties of ecological communities. *Ecol. Lett.* 18, 572–580. doi:10.1111/ele.12439 [full text, arXiv 1406.2926 preprint]
- Lillard, L. & Weiss, Y. (1979). Components of variation in panel earnings data: American scientists 1960–70. *Econometrica* 47, 437–454. doi:10.2307/1914192 [abstract; NBER w0121 pp. 4–11 scanned and seen]
- Lo, A. & MacKinlay, A. C. (1990). When are contrarian profits due to stock market overreaction? *RFS* 3, 175–205. doi:10.1093/rfs/3.2.175 [full text, NBER w2977 pp. 6–8]
- Luttmer, E. (2011). On the mechanics of firm growth. *REStud* 78, 1042–1068. doi:10.1093/restud/rdq028 [abstract]
- MaCurdy, T. (1982). The use of time series processes to model the error structure of earnings in a longitudinal data analysis. *J. Econometrics* 18, 83–114. doi:10.1016/0304-4076(82)90096-3 [secondary]
- Mallmin, E., Traulsen, A. & De Monte, S. (2025). Fluctuating growth rates link turnover and unevenness in species-rich communities. arXiv:2505.01376 v2 [full text via tool summary]
- Pugsley, B., Sedláček, P. & Sterk, V. (2021). The nature of firm growth. *AER* 111, 547–579. doi:10.1257/aer.20190748 [full text, CES WP 18-30]
- Secondary only, cited through the works above: Chamberlain (1984) through Guvenen and PSS; Fitzgerald, Gottschalk & Moffitt (1998) through Hryshko; Davis, Haltiwanger & Schuh (1996) through HJM.