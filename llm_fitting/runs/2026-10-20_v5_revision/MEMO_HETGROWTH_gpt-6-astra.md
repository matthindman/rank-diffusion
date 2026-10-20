<!-- Provenance: methods memo on heterogeneous mean growth (brief BRIEF_HETEROGENEOUS_GROWTH_METHODS.md), GPT-6 Astra via Codex CLI 0.163.0 with --search, read-only, effort xhigh, 2026-10-20. Saved verbatim. -->

**Methods memo: heterogeneous mean growth for R12**  
**20 October 2026**

**Recommendation.** Use a Gaussian, community-specific addition to weekly log growth as R12’s principal heterogeneous world. Estimate its population variance jointly with the lasting innovation variance and passing components through a declared set of covariance moments. Compare it with two separately estimated alternatives: an AR(1) growth rate and heterogeneous loadings on an observed platform factor. These are established modeling choices, but they represent different explanations of persistence.

Do not calibrate any variant to the annual gapped rank correlation, the prior-decliner contrast, or the head’s subsequent decline. Reserve those outcomes for model assessment. Because these development results have already informed the proposed mechanisms, successful reproduction would be an untargeted development check, not fresh confirmation.

Two qualifications are central. First, four years can support an estimate of growth heterogeneity conditional on a specified model, but cannot establish that heterogeneity is permanent. Second, Fernholz’s stationary-distribution results do **not** imply that adding name-specific growth leaves the hierarchy unchanged when the programmed rank drift and innovation volatility are held fixed.

This memo draws on the requested manuscript and SI, the research instructions, and the canonical record, particularly MODEL_STATUS §§2z-dg, 2z-dk and 2z-dl. It proposes methods; no analyses were run, repository files modified, or raw or sealed data opened.

**What the traditions actually specify.** There is no universal distribution or persistence convention. The common practice is to separate systematic differences between entities from innovations, specify how both evolve, and estimate their distinct implications for longitudinal moments.

| Tradition | Specification | Distribution of heterogeneity | Persistence | Estimator or calibration | Key references |
|---|---|---|---|---|---|
| Income: heterogeneous profiles | Individual intercept and slope, plus persistent and transitory shocks | Frequently moments only; Gaussian random effects in simulation or likelihood models | Fixed slope over the modeled career | Minimum distance on level or growth covariances; simulated moments | [Guvenen 2009](https://doi.org/10.1016/j.red.2008.06.004); [Hryshko 2012](https://doi.org/10.3982/QE112) |
| Firms: heterogeneous growth profiles | Firm-specific trajectories toward different sizes, plus shocks | Reduced-form means and variances need not specify a full distribution | Persistent profiles; not necessarily permanent linear growth | Age-by-age size covariances; structural estimation with exit | [Sterk, Sedláček & Pugsley 2021](https://doi.org/10.1257/aer.20190748) |
| Firms: growth types | Product birth/death rates depend on firm type | Discrete types | High-growth type can deteriorate | Structural calibration | [Luttmer 2011](https://doi.org/10.1093/restud/rdq028) |
| Inequality | Log random growth with type-dependent drift and volatility | Finite types; Brownian innovations | Type transitions, retirement and replacement | Tail and growth evidence; transition experiments | [Gabaix et al. 2016](https://doi.org/10.3982/ECTA13569) |
| Rank-based models | Rank drift plus name drift | Fixed coefficient vector; no required population distribution | Name effects fixed, ranks change | Rank occupation and local-time measurements | [Fernholz, Ichiba & Karatzas 2013](https://doi.org/10.1007/s10436-012-0193-2) |
| Ecology: correlated environments | Fitness changes through time | Lognormal fitness redraws or Gaussian OU fitness | Finite environmental memory | Simulation calibration or analytical model study | [Kalyuzhny et al. 2015](https://doi.org/10.1111/ele.12439); [Mallmin et al. 2026](https://doi.org/10.1111/ele.70333) |
| Ecology: stochastic logistic growth | Density regulation plus multiplicative environmental noise | Heterogeneous carrying capacities; white environmental noise | Stable abundance differences, rapidly fluctuating growth | Abundance and sampling moments | [Grilli 2020](https://doi.org/10.1038/s41467-020-18529-y) |
| Finance | Entity-specific exposure to common return factors | Loadings generally estimated without a population-distribution assumption | Loadings fixed over estimation windows | Time-series factor regressions | [Carhart 1997](https://doi.org/10.1111/j.1540-6261.1997.tb03808.x) |

**Income dynamics provides the closest statistical template.** In Guvenen’s specification, after observable earnings profiles are removed,

\[
y_{i,h,t}=\alpha_i+b_i h+z_{i,h,t}+\phi_t\epsilon_{i,h,t},
\qquad
z_{i,h,t}=\rho z_{i,h-1,t-1}+\pi_t\eta_{i,h,t}.
\]

Here \(h\) is experience, \(t\) calendar time, and \((\alpha_i,b_i)\) have estimated variances and covariance. The heterogeneous-income-profile model, HIP, allows \(\operatorname{Var}(b_i)>0\); the restricted-profile alternative, RIP, suppresses it. The checked specification declares second moments rather than requiring Gaussian coefficients. Estimation uses the covariance structure of earnings levels. Its identification argument warns that reverting shocks can conceal slope heterogeneity in growth autocovariances. See §2.2, equation (2), and §§4–5 of the [2007 working version of Guvenen 2009](https://ifs.org.uk/sites/default/files/output_url_files/wp1307.pdf).

Hryshko deliberately allows both growth heterogeneity and permanent shocks:

\[
y_{it}=\alpha_i+b_i h_{it}+p_{it}+\tau_{it}+e_{it},
\qquad
p_{it}=p_{i,t-1}+\xi_{it},
\]

with AR, MA or ARMA specifications for \(\tau\) and explicit measurement error. Equations (1)–(3), pp. 180–181, define this encompassing model. Equally weighted minimum distance uses growth covariance moments. His empirical conclusion favors no slope heterogeneity, and his simulations show that omitting permanent shocks can generate misleading evidence for HIP. Those experiments mimic a much longer annual panel than ours. Thus this literature supplies competing identification arguments, not consensus that persistent observed growth proves fixed types. [Hryshko 2012, full text](https://www.econstor.eu/bitstream/10419/150333/1/v3-i2-p177-209-QE112.pdf).

Several earlier contributions sharpen this distinction:

- Lillard and Weiss report persistent individual differences in earnings levels and growth. Baker compares profile heterogeneity against a unit-root alternative and favors the former. Their accessible abstracts establish that historical lineage, but do not justify attributing an identical Gaussian specification to both. [Lillard–Weiss working-paper abstract](https://www.nber.org/papers/w0121); [Baker 1997](https://doi.org/10.1086/209836).
- Abowd and Card study earnings and hours jointly. Their three-component representation combines a common component, differenced measurement errors and additional contemporaneous disturbances; equations (5a)–(5b), pp. 432–433, make that decomposition explicit. Their evidence concerns restrictions on a bivariate covariance structure, not a universal prohibition on heterogeneous slopes. [Abowd & Card 1989](https://davidcard.berkeley.edu/papers/covar-earn-hour.pdf).
- Haider explicitly models deviations from individual profiles as ARMA processes—equation (5), p. 808—and uses equally weighted minimum distance, Appendix A, p. 830. He notes that mean-reverting measurement error enters the estimated instability component. [Haider 2001, indexed article text](https://citeseerx.ist.psu.edu/document?doi=a80d34c7fc545bbe6461c23784b1ab77f3787678&repid=rep1&type=pdf).

The modern extension is considerably richer. Guvenen, Karahan, Ozkan and Song specify

\[
Y_{it}=(1-\nu_{it})
 \exp\{g(t)+\alpha_i+b_it+z_{it}+\epsilon_{it}\}.
\]

The intercept and slope are jointly Normal with estimated covariance; persistent and transitory innovations use Normal mixtures, and \(\nu\) represents nonemployment. Their simulated-moment targets include one- and five-year changes, conditional responses, lifecycle profiles and nonemployment. This establishes a clear precedent for Gaussian **profile heterogeneity alongside non-Gaussian shocks**. It does not make all those components necessary for R12. See §6.1, equations (2)–(8), pp. 26–28 of the [2019 working version of Guvenen et al. 2021](https://www.newyorkfed.org/medialibrary/media/research/staff_reports/sr710.pdf).

I could not verify the complete original specifications of Hause (1980) and MaCurdy (1982) from accessible full texts. They are listed below with that limitation, rather than assigned equations from recollection.

**Firm models distinguish different growth paths from permanently different growth rates.** Sterk, Sedláček and Pugsley’s reduced form is

\[
\log n_{ia}=u_{ia}+v_{ia}+w_{ia}+z_{ia},
\]
\[
u_{ia}=\rho_u u_{i,a-1}+\theta_i,\quad
v_{ia}=\rho_v v_{i,a-1},\quad
w_{ia}=\rho_w w_{i,a-1}+\epsilon_{ia}.
\]

Initial conditions and \(\theta_i\) produce ex-ante heterogeneous paths. When \(\rho_u<1\), the first component tends toward \(\theta_i/(1-\rho_u)\); it is not an indefinitely accumulating slope. Equation (1), p. 553, specifies the components through moments. Their reduced-form estimation fits 210 size covariance moments across ages 0–19 with identity weighting. It initially uses firms surviving through age 19; their structural model subsequently addresses endogenous exit. These are useful precedents for covariance calibration and explicit selection, but a twenty-year firm panel is not a power benchmark for four years of Reddit. [Full article, §§I.C–D and II](https://discovery.ucl.ac.uk/id/eprint/10109806/1/Sterk_aer.20190748.pdf).

Luttmer instead gives firms high- or low-quality blueprints. With \(n\) blueprints, creation and destruction rates are \(n\mu_j\) and \(n\lambda_j\); high quality deteriorates permanently at a Poisson rate. Entrants draw a type. The mean product-count growth contribution is \(\mu_j-\lambda_j\), not a directly specified Gaussian log-growth coefficient. See §3.3, pp. 16–17 of the [accepted manuscript](https://www.mcgill.ca/economics/files/economics/luttmer.pdf). This supports type-dependent growth and finite growth episodes, rather than an arbitrary two-state approximation chosen to improve R12’s score.

**Inequality models likewise distinguish shocks from types.** Gabaix’s review presents the Kesten recurrence

\[
S_{t+1}=A_{t+1}S_t+B_{t+1},
\]

whose tail exponent satisfies \(E|A|^\zeta=1\), subject to additional conditions. This is a random multiplicative-growth mechanism; it does not prescribe a lognormal distribution of permanent mean growth. See §3.3, equation (12). [Gabaix 2009](https://gabriel-zucman.eu/files/teaching/Gabaix09.pdf).

Gabaix, Lasry, Lions and Moll allow type-dependent log-income dynamics,

\[
dy_{it}=\mu_jdt+\sigma_jdW_{it}
\]

with switching, retirement/rebirth and extensions including jumps and aggregate exposure. Their principal two-type example has high-growth individuals becoming low-growth individuals; low growth is absorbing until retirement. It is not a symmetric, recurrent two-state chain. See §5.1, equation (26), and §5.2. Their calibration uses tail information and external growth evidence, with sensitivity to uncertain switching duration. Because R12 must leave hierarchy outcomes untargeted, its calibration should not copy their use of the income tail as a target. [Gabaix et al. 2016](https://benjaminmoll.com/wp-content/uploads/2019/07/dynamics.pdf).

**Rank-based models offer the closest structural analogue.** Fernholz, Ichiba and Karatzas write

\[
d\log X_i(t)=\{\gamma_i+g_{r_i(t)}\}\,dt
            +\sigma_{i,r_i(t)}\,dW_i(t).
\]

The name coefficient \(\gamma_i\) is fixed; no Gaussian population distribution is imposed. Their estimation combines rank occupation frequencies with rank-based growth measurements. Equation (3.1) defines the model; §§3–4 give its stability restrictions and estimation. An important distinction follows: stable interaction between name and rank effects can equalize asymptotic growth rates even though names have different drift coefficients. A fixed additive drift is therefore not automatically an unconditional lifetime growth advantage. [Fernholz, Ichiba & Karatzas 2013](https://arxiv.org/pdf/1302.3870).

The broader hybrid Atlas model adds a common drift and a name-based covariance component:

\[
dY_i=(g_{r_i}+\gamma_i+\gamma)dt
     +\sum_j\rho_{ij}dW_j+\sigma_{r_i}dW_i .
\]

See equation (2.1), §2, and the ergodicity conditions in §3. [Ichiba et al. 2011](https://arxiv.org/pdf/0909.0065). Estimating thousands of unrestricted name effects through long-run occupation frequencies would be unsuitable here; a low-dimensional random-effects distribution is the practical analogue.

**Ecology offers distinct models of finite memory.** In Kalyuzhny, Kadmon and Shnerb, local replacement favors species according to

\[
P(i)=\frac{N_if_i}{\sum_jN_jf_j}.
\]

Fitness is redrawn at environmental intervals from a lognormal distribution with mean one. Species share the long-run fitness distribution. The lognormal variable is a positive reproductive weight, not a fixed additive log-growth slope. See equation (1) and model description, pp. 3–4 of the [2014 preprint](https://si-forestgeo01.si.edu/Public/pdfs/KaluzhnyEtAl2014.pdf). Its simulation calibration uses abundance fluctuations and ecological parameters; matching those targets is different from estimator recovery.

Mallmin, Traulsen and De Monte explicitly use OU growth rates:

\[
\frac{dn_i}{dt}=n_i[r_i(t)-\mu N(t)-\varepsilon n_i]+\lambda,
\]
\[
dr_i=-\frac{r_i-r^\ast}{\tau}dt
       +\sqrt{\frac{2\sigma_r^2}{\tau}}\,dW_i.
\]

The stationary growth-rate distribution is Gaussian with variance \(\sigma_r^2\) and correlation time \(\tau\). See §§2.1–2.2, equations (1) and (3). This is a direct precedent for the decaying-growth variant, although the density regulation and immigration mechanisms differ from Reddit. [Mallmin et al. 2026](https://doi.org/10.1111/ele.70333).

Grilli’s stochastic logistic model instead uses white environmental noise:

\[
dx_i=\frac{x_i}{\tau_i}(1-x_i/K_i)dt
      +\sqrt{\sigma_i/\tau_i}\,x_i\,dW_i.
\]

Equation (5) describes that process. Its lognormal variation in mean abundances and associated carrying capacities should not be cited as evidence for lognormal persistent growth effects. [Grilli 2020](https://www.nature.com/articles/s41467-020-18529-y).

**Finance supplies the common-factor alternative.** Carhart’s equation (3), p. 61, is

\[
r_{it}=\alpha_i+b_iRMRF_t+s_iSMB_t+h_iHML_t
       +p_iPR1YR_t+e_{it}.
\]

Loadings attribute performance persistence to shared exposures, including a momentum factor; no Gaussian distribution of loadings is required. [Carhart 1997](https://r.jordan.im/download/investing/carhart1997.pdf). The useful lesson for submissions is that persistent relative performance may reflect stable exposure to a changing common environment. Factor adjustment alone does not establish a causal explanation, and financial momentum is not itself evidence for a fixed latent drift.

**Distribution and initial conditions.** Gaussian additive log-growth effects are the most defensible simple baseline: they permit both growth and decline, require only a variance under independence, and have an explicit income-dynamics precedent. Second moments alone, however, cannot identify Gaussianity. Two-point, Gaussian and other distributions can share the same covariance implications.

Lognormality is appropriate for strictly positive quantities such as fitness weights or volatility scales. An unshifted lognormal additive growth effect cannot represent both positive and negative deviations. A centered lognormal introduces skewness and should be a declared sensitivity, not the default response to the observed decline asymmetry.

There is also no universal sign for the intercept–slope covariance. Its interpretation depends on the time origin: if \(\alpha_i'=\alpha_i+cb_i\), then

\[
\operatorname{Cov}(\alpha_i',b_i)
=\operatorname{Cov}(\alpha_i,b_i)+c\operatorname{Var}(b_i).
\]

For R12, define the initialization date explicitly. Use independent Gaussian effects for the minimal comparison, but estimate or profile a correlated-initialization sensitivity. Independence is an assumption about the selected starting population, not a consequence of standard practice.

**Identification and estimation.** The following is an identification illustration, not a replacement for the simulator’s coupled observation equations. Let observed residual levels be a random walk plus \(b_it\), a stationary passing component \(T_{it}\), and independent measurement error. For weekly changes and \(k\geq1\),

\[
\gamma_{\Delta y}(k)
=\sigma_b^2+
2C_T(k)-C_T(k-1)-C_T(k+1)
-\sigma_e^2\mathbf1_{\{k=1\}}.
\]

A stationary AR(1) **level** component with covariance \(V\phi^k\) contributes

\[
-V(1-\phi)^2\phi^{k-1},
\]

whereas an AR(1) **growth-rate** component contributes positive covariance \(\sigma_u^2\rho^k\). Lasting independent innovations contribute to lag zero. These signatures motivate joint estimation: estimating each component separately and adding their variances afterward can count the same variation twice.

Use minimum distance,

\[
\widehat\theta
=\arg\min_\theta
[\widehat m-m(\theta)]^\top W[\widehat m-m(\theta)].
\]

Identity weighting is a reasonable primary choice for a prespecified, consistently scaled moment vector. Altonji and Segal show that estimating an “optimal” weighting matrix can introduce substantial finite-sample bias because its errors correlate with errors in the moments. This supports EWMD as a defensible baseline, not a theorem that equal weighting always wins. [Altonji & Segal 1996](https://www.tandfonline.com/doi/abs/10.1080/07350015.1996.10524661).

For R12, compute model moments by simulation through the actual rank, exit and observation machinery. Analytical covariance formulas are useful starting values and checks. A Gaussian state-space likelihood is a useful sensitivity only where its observation and selection assumptions hold. Indirect inference is another option: fit the same auxiliary covariance model to real and simulated panels.

Include separated level–future-growth moments as an initialization sensitivity. Druedahl and Munk-Nielsen show that these moments can overturn apparent HIP evidence from growth covariances. Their identifying horizons are much longer than our panel, so their result strengthens the case for checking specification rather than promising identification here. [Druedahl & Munk-Nielsen 2018](https://doi.org/10.1016/j.jedc.2018.07.003).

Weekly sampling provides information about fast components; it does not create more years of low-frequency variation. Large cross sections can help identify population parameters under strong assumptions, but common shocks, selection and nearly permanent latent processes reduce that advantage. A very slow OU process can resemble a fixed effect throughout four years. Report profile uncertainty, including an effectively unbounded persistence timescale where warranted. Neither a noisy insignificant long-lag covariance nor a positive annual correlation settles the issue alone.

**The three proposed R12 variants.** In each case retain the existing functional forms and explicit observation process:

\[
\Delta H_{it}
=g(r_i(t))+a_{it}
+\sigma_0e^{-\beta(H_{it}-H_0)}\eta_{it}.
\]

The alternatives concern \(a_{it}\). They should initially be fitted separately.

1. **Fixed heterogeneity:** \(a_{it}=b_i\), with \(b_i\sim N(0,\sigma_b^2)\). Draw an effect once for each identity; a genuinely new identity receives a new draw. Estimate \(\sigma_b^2\) jointly with affected nuisance parameters. Report \(52\sigma_b\) as the standard deviation of annual deterministic log-growth contributions. The brief’s value 0.4 is a planted demonstration, not an estimate.

2. **Decaying heterogeneity:** \(a_{it}=u_{it}\), with
   \[
   u_{i,t+1}=\rho u_{it}
       +\sigma_u\sqrt{1-\rho^2}\,\xi_{it},
   \quad 0\leq\rho<1.
   \]
   Initialize from its stationary distribution unless modeling entry explicitly. Estimate \((\sigma_u,\rho)\); report the half-life \(-\log2/\log\rho\). This component changes growth, whereas existing passing components revert levels. Do not initially combine fixed and very slow growth effects: their separate variances may be weakly identified. If implemented as continuous-time OU, integrate growth over each week consistently rather than treating endpoint samples as exact integrals.

3. **Common-factor exposure:** \(a_{it}=\lambda_i f_t\), where \(f_t\) is a declared weekly change in log platform activity, with a fixed centering convention. Estimate loadings using the training window’s weekly co-movement, allowing serially correlated residuals. Estimate the loading distribution hierarchically or correct its empirical variance for estimation noise; the raw dispersion of short-window OLS estimates overstates heterogeneity. Separate average exposure from relative loadings. Check mechanical self-inclusion in platform totals, especially for large communities. Simulating the observed shared factor path produces a conditional historical world; it does not supply a forecast model for future platform activity.

For covariance calibration, propose weekly growth covariance lags \(0\!:\!8,13,26,39,52\), supplemented by selected 4-, 8- and 26-week change variances and initialization moments. Freeze the precise vector and scaling before fitting. Estimate within declared permanent-rank strata; pool heterogeneity parameters initially rather than fitting dozens of rank-specific spreads. Do not subtract each community’s estimated mean growth before computing these moments—that removes the component being estimated.

Exclude the annual 13-week-window Spearman correlations, prior-decliner contrasts, head-share changes and turnover outcomes from the new calibration vector. Different moments calculated from the same panel remain statistically related; “not targeted” does not mean “independent.”

Revisit the inherited variance and drift estimates within these opt-in worlds. In particular, measured slide can already contain the consequences of persistent composition. Adding \(b_i\) to an unadjusted drift estimate risks double counting. Use declared first moments to reconcile rank drift with name effects. The R11 entry/exit accounting correction in [MODEL_STATUS §2z-dl](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md:13098) also requires a matched homogeneous control; comparisons must not mix old and corrected accounting.

**G15 requires recovery before interpretation.** In a local approximation,

\[
V(h)=qh+Bh^2+C(h),
\]

where \(q\) is lasting innovation variance, \(B=\operatorname{Var}(b_i)\), and \(C\) collects passing and observation effects. Even if those effects cancel,

\[
\frac{V(2h)-V(h)}{h}=q+3Bh.
\]

Thus the two-spacing estimate is not automatically an estimate of diffusion variance under heterogeneous growth. Averaging levels into blocks does not eliminate a linear slope.

If \(B\) is constant across size and \(q(H)=\sigma_0^2e^{-2\beta(H-H_0)}\), the local exponent inferred from the contaminated variance is

\[
\beta_{\rm apparent}
=\beta\frac{q(H)}{q(H)+3Bh}.
\]

This illustration predicts attenuation. With size-dependent heterogeneity, selection or correlated components, even the direction need not hold. Recovery must therefore cover both \(\sigma_0\) and \(\beta\), using the actual estimator and window construction.

**Selection is part of the model.** A top-\(K\) boundary is not an economic death boundary. Decliners can leave the observed universe while continuing to exist; temporary absences and subsequent returns must not automatically redraw their growth effects.

Balanced-survivor estimation changes the population. The firm study above makes this limitation explicit before introducing endogenous exit; income studies likewise impose employment and observation requirements. For Reddit, the recommended strategy is to simulate the entire tracked population and then apply the exact membership, absence penalty, window completeness and ranking rules used on the data.

Retain the documented buffer where available. State separately what is inferred about the selected universe and what, if anything, is inferred about unseen communities. A fixed empirical missingness mask is insufficient when observation depends on simulated activity. Inverse-probability weighting requires defensible observation assumptions and adequate overlap; it cannot recover never-observed entities without additional structure.

For forecasting exercises, select permanent-rank strata and membership from training information. Reproduce the published full-period selection separately for direct descriptive comparison. Do not quietly change the estimand, and do not stratify parameter estimation by current rank.

**What should change in the hierarchy.** Fernholz’s theorem relates stationary rank gaps to effective ranked characteristics. In the stable rank-gap representation,

\[
E[\log\theta_{(k)}^\ast-\log\theta_{(k+1)}^\ast]
=\frac{\sigma_k^2}{-4\sum_{j=1}^k\alpha_j}.
\]

Here \(\alpha_j\) is the long-run drift of the occupant of rank \(j\), relative to aggregate growth, and \(\sigma_k^2\) is the adjacent log-gap variance rate, including covariance effects. These are not simply the input arrays called `g` and `sigma` in an arbitrary simulator. See Theorem 2.3, equation (2.21), of [Fernholz 2017](https://www.ricardofernholz.com/Research/powerLaws.pdf).

The implication for R12 is that persistent effects can change rank occupancy and effective ranked drift. Unequal exposure to a stochastic common factor can also change relative gap variance. Holding idiosyncratic innovation volatility fixed therefore does not hold stationary shape fixed. Moreover, four-year transient behavior with exits and censoring is not a stationary theorem.

Measure shape, movement and turnover in every variant. Check long-run stability before interpreting long simulations as equilibria; unbounded Gaussian name effects can conflict with stability restrictions under a fixed restoring drift. Do not silently truncate or redraw inconvenient effects.

**Calibration and recovery should be separate deliverables.** Published practice ranges from covariance fitting to targeted simulation exercises; there is no universal recovery protocol transferable without adaptation. R12 needs its own because its selection and variance instruments are distinctive.

Before substantive interpretation, use a frozen synthetic design containing homogeneous, fixed-effect, decaying-effect and factor-only truths. Include difficult cases: slow decay, correlated initial conditions, heavy-tailed passing shocks, and endogenous exits. Reproduce both panel lengths, universes, windowing and estimation procedures.

Record parameter bias, interval coverage, boundary estimates, confusion between mechanisms, and recovery of the lasting variance and size exponent. Recovery needs substantially more replications than a presentation ensemble—roughly 200 or more is a reasonable proposed starting point, with Monte Carlo uncertainty reported and the final count fixed in advance.

For substantive comparisons, use at least 20 common seeds and report bands for head outcomes. Distinguish simulation variability from parameter uncertainty. Preserve shared calendar shocks when assessing uncertainty, and resample or simulate the complete estimation pipeline rather than treating overlapping windows as independent.

The principal scientific result may be that several mechanisms reproduce the annual correlation while implying different movement or shape consequences. Report that ambiguity if it occurs. A failed recovery exercise, an unidentified decay time, or failure to reproduce the decline asymmetry is a substantive result, not a reason to retune the reserved outcomes.

**References and access record.** “Full text” below means the indicated article or working version was available for checking relevant sections; version differences are identified. No technical recommendation depends on an unverified specification.

- **Abowd, J. M., and D. Card (1989).** “On the Covariance Structure of Earnings and Hours Changes.” *Econometrica* 57:411–445. [DOI](https://doi.org/10.2307/1912561). **[full text]**
- **Altonji, J. G., and L. M. Segal (1996).** “Small-Sample Bias in GMM Estimation of Covariance Structures.” *JBES* 14:353–366. [DOI](https://doi.org/10.1080/07350015.1996.10524661). **[abstract only]**
- **Baker, M. (1997).** “Growth-Rate Heterogeneity and the Covariance Structure of Life-Cycle Earnings.” *Journal of Labor Economics* 15:338–375. [DOI](https://doi.org/10.1086/209836). **[abstract only]**
- **Carhart, M. M. (1997).** “On Persistence in Mutual Fund Performance.” *Journal of Finance* 52:57–82. [DOI](https://doi.org/10.1111/j.1540-6261.1997.tb03808.x). **[full text]**
- **Druedahl, J., and A. Munk-Nielsen (2018).** “Identifying Heterogeneous Income Profiles Using Covariances of Income Levels and Future Growth Rates.” *JEDC* 94:24–42. [DOI](https://doi.org/10.1016/j.jedc.2018.07.003). **[abstract only]**; publisher introduction also accessible.
- **Fernholz, R., T. Ichiba, and I. Karatzas (2013).** “A Second-Order Stock Market Model.” *Annals of Finance* 9:439–454. [DOI](https://doi.org/10.1007/s10436-012-0193-2). **[full text]**, arXiv version.
- **Fernholz, R. T. (2017).** “Nonparametric Methods and Local-Time-Based Estimation for Dynamic Power Law Distributions.” *Journal of Applied Econometrics* 32:1244–1260. [DOI](https://doi.org/10.1002/jae.2573). **[full text]**, author manuscript.
- **Gabaix, X. (2009).** “Power Laws in Economics and Finance.” *Annual Review of Economics* 1:255–293. [DOI](https://doi.org/10.1146/annurev.economics.050708.142940). **[full text]**
- **Gabaix, X., J.-M. Lasry, P.-L. Lions, and B. Moll (2016).** “The Dynamics of Inequality.” *Econometrica* 84:2071–2111. [DOI](https://doi.org/10.3982/ECTA13569). **[full text]**
- **Grilli, J. (2020).** “Macroecological Laws Describe Variation and Diversity in Microbial Communities.” *Nature Communications* 11:4743. [DOI](https://doi.org/10.1038/s41467-020-18529-y). **[full text]**, indexed publisher sections.
- **Guvenen, F. (2009).** “An Empirical Investigation of Labor Income Processes.” *Review of Economic Dynamics* 12:58–79. [DOI](https://doi.org/10.1016/j.red.2008.06.004). **[full text]**, 2007 IFS working version.
- **Guvenen, F., F. Karahan, S. Ozkan, and J. Song (2021).** “What Do Data on Millions of U.S. Workers Reveal about Lifecycle Earnings Dynamics?” *Econometrica* 89:2303–2339. [DOI](https://doi.org/10.3982/ECTA14603). **[full text]**, 2019 NY Fed working version.
- **Haider, S. J. (2001).** “Earnings Instability and Earnings Inequality of Males in the United States: 1967–1991.” *Journal of Labor Economics* 19:799–836. [DOI](https://doi.org/10.1086/322821). **[full text]**, indexed PDF sections.
- **Hause, J. C. (1980).** “The Fine Structure of Earnings and the On-the-Job Training Hypothesis.” *Econometrica* 48:1013–1029. [DOI](https://doi.org/10.2307/1912945). **[abstract only]**, related NBER precursor; final specification unverified.
- **Hryshko, D. (2012).** “Labor Income Profiles Are Not Heterogeneous: Evidence from Income Growth Rates.” *Quantitative Economics* 3:177–209. [DOI](https://doi.org/10.3982/QE112). **[full text]**
- **Ichiba, T., V. Papathanakos, A. Banner, I. Karatzas, and R. Fernholz (2011).** “Hybrid Atlas Models.” *Annals of Applied Probability* 21:609–644. [DOI](https://doi.org/10.1214/10-AAP706). **[full text]**, arXiv version.
- **Kalyuzhny, M., R. Kadmon, and N. M. Shnerb (2015).** “A Neutral Theory with Environmental Stochasticity Explains Static and Dynamic Properties of Ecological Communities.” *Ecology Letters* 18:572–580. [DOI](https://doi.org/10.1111/ele.12439). **[full text]**, 2014 preprint.
- **Lillard, L. A., and Y. Weiss (1979).** “Components of Variation in Panel Earnings Data: American Scientists, 1960–70.” *Econometrica* 47:437–454. [DOI](https://doi.org/10.2307/1914192). **[abstract only]**, NBER working version.
- **Luttmer, E. G. J. (2011).** “On the Mechanics of Firm Growth.” *Review of Economic Studies* 78:1042–1068. [DOI](https://doi.org/10.1093/restud/rdq028). **[full text]**, accepted manuscript.
- **MaCurdy, T. E. (1982).** “The Use of Time Series Processes to Model the Error Structure of Earnings in a Longitudinal Data Analysis.” *Journal of Econometrics* 18:83–114. [DOI](https://doi.org/10.1016/0304-4076(82)90096-3). **[memory, unverified]** for model details; bibliographic identity checked, specification not relied upon.
- **Mallmin, E., A. Traulsen, and S. De Monte (2026).** “Fluctuating Growth Rates Link Turnover and Unevenness in Species-Rich Communities.” *Ecology Letters*. [DOI](https://doi.org/10.1111/ele.70333). **[full text]**
- **Sterk, V., P. Sedláček, and B. Pugsley (2021).** “The Nature of Firm Growth.” *American Economic Review* 111:547–579. [DOI](https://doi.org/10.1257/aer.20190748). **[full text]**