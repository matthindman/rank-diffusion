# Novelty search: the rank-based rate law and digital attention

_A background research agent (Claude, general-purpose subagent) ran this
search on 2026-10-14 at Claude Code's request. Its brief was the rate law of
§2z-cd and four candidate claims, (a)–(d), listed below. Its report follows
verbatim; Claude saved it after reading it. The agent read no repository data.
Reading tags: [FT] full text read (arXiv HTML through tool extraction), [AB]
abstract only, [SS] search snippet only. The search is broad but not
exhaustive. Its corrections are applied in `BALANCE_LAW_THEORY_NOTE_2026-10-14.md`._

Candidate claims:
- (a) an out-of-equilibrium test with the boundary term predicted from
  lasting volatility and slope, checked rank by rank;
- (b) named-cohort drift estimation with transient and averaging constants
  removed;
- (c) exact entry and exit accounting;
- (d) application to online attention and a dated platform change.

---

## 1. Stochastic portfolio theory and random growth (items 1 and 4)

**Fernholz, R.T., arXiv 1602.00159 (v3 title: "Empirical Methods for Dynamic Power Law Distributions in the Social Sciences"), https://arxiv.org/abs/1602.00159 [FT]**
- I did not check the JAE title or volume on the publisher's page.
- Eq. 2.20 gives E[gap_k] = σ_k²/(−4(α_1+…+α_k)), where σ_k² is the variance of the gap process. With κ_k = −2Σα, that is your σ²/(2κ_k), so your identification is right.
- α_k is the growth rate of whoever holds rank k at the time (current rank).
- κ_k is measured from rank switching (eq. 3.5, the gap between the top-k sum taken by name and taken by rank). α then follows from κ through Lemma 3.1.
- Everything is stationary. The paper says the identity holds only if parameters "do not change drastically and frequently."
- The smoothing count is chosen to minimise the gap between predicted and observed shares, so the fit is tuned to the score.
- Verdict on (a): partial (equilibrium only). (b) and (c): no overlap.

**Fernholz & Koch, "Why Are Big Banks Getting Bigger?", Dallas Fed WP 1604 (2016), https://www.dallasfed.org/research/papers/2016/wp1604 [FT]**
- This is comparative statics across two stationary regimes. The break date is chosen to minimise the deviation between prediction and data.
- Local time appears only inside the α estimator. The paper has no dynamic law for the change in concentration.
- So your phrase "decompose changes in concentration into rank-based growth and local-time terms" overstates it. What it actually does is attribute the change between regimes to σ_k versus the reversion rates.
- Verdict: partial on (d) in structure (a dated policy change, with concentration explained by rank factors). No overlap on (a).
- Companions: AER P&P 107(5):603–07 (2017) [AB]; *European Economic Review* 135 (2021), US vs European banks [SS].

**Itkin & Larsson, "Calibrated rank volatility stabilized models for large equity markets", arXiv 2403.04674, Finance & Stochastics (to appear), https://arxiv.org/abs/2403.04674 [FT]**
- Growth parameters come from a stationary identity (3.9–3.12). Collisions are measured with Fernholz's leakage estimator.
- Remark 3.5 explicitly considers and rejects the occupation-density estimator of local time (quadratic variation times time spent near zero gap), as "data inefficient" and needing a tuning ε. That is the micro version of your σ²n/2 prediction.
- They compare model-implied collisions with measured ones out of sample, but in equilibrium.
- **Correction for (b):** Appendix B.1 is about the *volatility* estimator. Taking increments of the rank-k series is biased downward at a fixed sampling interval, compared with taking the increments of the name that held rank k. Their heuristic is the omitted C² term. They call making it rigorous an open question.
- B.1 says nothing about drift, permanent rank or transient swings. Their preferred estimator in fact conditions on the start-of-interval holder, which is current-rank conditioning.
- They also leave open the statistical properties of the collision estimator.
- Verdict: partial on (a) and (b).

**Baker & Smyth, "Traveling Waves in Equity Markets with Rank-Based Entry and Exit", arXiv 2608.27156, 27 Aug 2026, https://arxiv.org/abs/2608.27156 [FT]**
This is very recent, it is the closest competitor, and it must be cited.
- The CDF follows a porous-medium equation with a reaction term built from rank-dependent entry and exit intensities, f̃(q) = f(q) − q f(1), the second term being dilution.
- The wave identity includes φ = ½σ²g, the same σ²×density/2 flux as your boundary term. Without turnover the slope is Ξ′ = σ²/(2(B(s) − sB(1))).
- They "treat the drift as the unknown in the wave identity", so drift is inferred, not measured.
- Volatility is measured by rank bin. Waves are fitted decade by decade on CRSP.
- Relaxation timescales predicted from the reaction term (about 6 months at the bottom, 53 years at the top) are compared with the persistence of shape deviations (7 months and 37 years).
- They note that drift estimated from survivors is biased by survival. They do not treat local time or named cohorts.
- Verdict: anticipates much of (c). Partial on (a): it is out of equilibrium, but runs the inference the other way round.

**Theory: the forward equation for ordered levels already exists; none of it is tested on data**
- Shkolnikov, SPA 122 (2012), https://arxiv.org/abs/1008.4611 [AB]. The CDF limit is a generalised porous-medium equation with convection.
- Jourdain & Reygner, SPDE Anal. Comput. 2013 and Ann. Finance 11 (2015), https://arxiv.org/abs/1312.5660 [AB].
- Cabezas, Dembo, Sarantsev & Sidoravicius, "Brownian particles with rank-dependent drifts: out-of-equilibrium behavior", CPAM 72 (2019), https://arxiv.org/abs/1708.01918 [AB]. They solve a Stefan problem for the profile out of equilibrium.
- Fernholz & Fernholz, "Zipf's law for Atlas models", J. Appl. Probab. 57 (2020), https://arxiv.org/abs/1707.04285 [AB].
- Banner, Fernholz & Karatzas, AAP 2005, and Banner & Ghomrasni, SPA 2008 (local times of ranked semimartingales) [SS].
- Open markets: Karatzas & Kim, Math. Finance 31 (2021) [AB]; Itkin & Larsson, AAP 34 (2024), https://arxiv.org/abs/2110.14046 [AB].
- Verdict: the rate law is derivable from this body of work, so (a) cannot be claimed as new mathematics.

**Economics**
- Gabaix, Lasry, Lions & Moll, "The dynamics of inequality", *Econometrica* 84 (2016) [AB]. Standard random growth transitions too slowly; the remedy is scale or type dependence. Partial on (d) conceptually. Type dependence is also a rival to your permanent-rank reading.
- Luttmer, REStud 78 (2011) [SS]. Gibrat growth makes large firms implausibly old, which parallels your "a climb to the head takes decades."
- Fernholz, "The Distributional Effects of Progressive Capital Taxes" (WP, 2017), ricardofernholz.com/Research/capitalTaxes.pdf [FT]. Non-stationarity is handled by projecting the implied stationary distribution, following Gabaix et al. No test. Verdict: no overlap.

**Measurement error (item 4).** I found nothing on transient components or block-averaging bias in estimating local time by rank. The nearest are Itkin & Larsson's discretisation heuristic and smoothing, and Baker & Smyth's survivorship note. The search was broad but not exhaustive [SS].

## 2. Online attention (item 2)

- **Blumm et al., PRL 109, 128701 (2012) [FT].** Each item follows its own Langevin equation with fitness A_i and noise B; the data include hourly Wikipedia views and Twitter hashtags. A phase diagram from B separates stable from unstable rankings, and rank is stable when the gap exceeds the rank fluctuation. They say "a precise empirical determination of the drift term is difficult." It links noise to rank stability and only qualitatively to the breadth of the top. There is no balance law and no test against the slope. Partial on (d).
- **Iñiguez, Pineda, Gershenson & Barabási, Nat. Commun. 13, 1646 (2022), https://arxiv.org/abs/2104.13439 [FT].** A rank-space-only model (displacement and replacement) applied to 30 lists, including GitHub by daily popularity. It uses no scores. It states that an extension "explicitly considering the links between score and rank" may help, which is a usable gap statement. Its flux (openness) parallels (c). No quantitative link between movement and the shape of the curve.
- **Ratkiewicz et al., PRL 105, 158701 (2010) [SS]:** Wikipedia bursts and a rank-shift model. **Wu & Huberman, PNAS 104 (2007) [AB]:** novelty decay on Digg. **Zhu & Lerman, arXiv 1601.07200 [SS]:** a static Gini of Twitter attention. **Vranić et al., J. Stat. Mech. (2022) 123402 [AB]:** subreddit sizes are lognormal and Gibrat fails. All four: no rank-based law.
- **Bentley et al., Evol. Hum. Behav. 28 (2007), and Evans & Giometto, arXiv 1105.4044 [SS].** They predict turnover across a top-y chart boundary from neutral-copying parameters (z ≈ √μ·y), for music charts and baby names. This is a predicted boundary crossing by a different mechanism. Partial on (a) in spirit.
- None of these links individual movement to the slope of the rank-size curve quantitatively.

## 3. Algorithm changes (item 3)

- **Huszár et al., PNAS 119 (2022) [SS]:** a randomized comparison of algorithmic and chronological feeds, measuring amplification. No concentration dynamics.
- **Moehring, Reddit news-feed paper (2026 draft) [FT, first pages]:** a rank discontinuity within r/politics.
- Searches on Reddit's 2021 Home feed and on the 2018 Facebook News Feed change found no rank-size or transition-speed analysis of an algorithm change [SS].
- Verdict: no overlap.

## Assessment

- **(a) New as an empirical test, not as mathematics.** I found no out-of-equilibrium test that pairs measured named drifts with a *predicted* boundary term σ²/(2S_k) and checks it rank by rank against measured switching. Present it as the first test, built on existing theory.
  - Disclose that n(x_k) = k/S_k stands in for the gap density at zero, which is exact only for exponential gaps.
  - Note that Itkin & Larsson rejected the density-based estimator on efficiency grounds. Your use is a prediction, not an estimator, which is a defensible distinction.
- **(b) Largely new**, but rewrite the Itkin & Larsson sentence.
  - Their B.1 is a discretisation bias in a *volatility* estimator.
  - The drift analogue (regression to the mean under current-rank conditioning, and transient or block-averaging bias) is not addressed in any calibration I found. Fernholz, Fernholz & Koch, and Itkin & Larsson all condition on current rank.
- **(c) Mostly anticipated.** Rank-dependent entry and exit in an open system are treated by Baker & Smyth (2026) and by the open-market papers. At most, claim an exact accounting for a finite top-K universe, positioned against them.
- **(d) New.** I found no stochastic-portfolio-theory or Gabaix-style rank-size decomposition of online attention, and no rank-based or transition-speed analysis of a platform algorithm change.

**Must cite and position against:**
1. Fernholz (JAE 2017; arXiv 1602.00159)
2. Fernholz & Koch (WP 1604 / AER P&P 2017 / EER 2021)
3. Itkin & Larsson (arXiv 2403.04674)
4. Baker & Smyth (arXiv 2608.27156)
5. Gabaix, Lasry, Lions & Moll (2016)

For the domain, add Blumm et al. (2012) and Iñiguez et al. (2022).
