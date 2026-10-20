_Round-7 review by GPT-6 Pro (OpenAI), run by the owner in the ChatGPT interface with `PROMPT_GPT6PRO_R7.md` and the compiled PDFs of manuscript v5 (61b6bbe; the PDFs compiled 2026-10-20 21:14), and pasted into the Claude Code session on 2026-10-20. Saved verbatim by the orchestrating session; the reviewer had no repository access. Model output: checked in `SYNTHESIS_R7.md` (addendum)._

# 1. Editor’s decision

**My decision is: send out, but not with enthusiasm in its present form.** The paper addresses an important distinction between persistent inequality and persistent incumbency, and its measurement of movement provides more substance than another demonstration of a heavy-tailed distribution. The fact that the underlying mathematics is established is entirely compatible with a PNAS contribution, provided the application produces a convincing empirical advance. However, the quantitative durability argument currently mixes different time scales, the most consequential empirical departure crosses an incompletely audited measurement change, and the heterogeneous-growth conclusions exceed what the fitted specifications establish. I would repair those points before submission because each could support a justified negative referee report.

I read the full main text, pnas_draft(1).pdf, and Supporting Information, pnas_si(1).pdf, including the figures and tables. Page references below are PDF page numbers. I checked the algebra and arithmetic recoverable from the PDFs; raw data, code, saved outputs, and the exposure ledger were unavailable for independent verification. I have ignored the TODO markers.

**Classification.** I would submit under **Social Sciences**, using the general Social Sciences classification; **Economic Sciences** is a reasonable alternative if you want the handling editor’s expertise centered on growth and distributional dynamics. I would not lead with Applied Mathematics or choose a dual classification merely because the argument uses diffusion models: the principal contribution is empirical and substantive. These are available PNAS topic classifications; my preference among them is an editorial judgment. [pnas.org](https://www.pnas.org/author-center/submitting-your-manuscript?utm_source=chatgpt.com)

**The single change with the greatest potential benefit:** rebuild the central durability comparison around the **actual calibrated model and the stated horizons**—membership retention at one and two years, structural change and response over one to five years—with uncertainty in the estimated ingredients propagated through the comparison. The current time-scale argument is the bridge between the familiar mathematics and the paper’s principal claim. That bridge needs to be dependable.

# 2. Significance and positioning

## What I understand the contributions to be

- **The paper separates three outcomes that platform research can conflate:** persistence of a steep distribution, persistence of membership in its leading sets, and persistence of individual leaders’ activity shares. Reddit illustrates why these distinctions matter: a community can retain first place while losing most of its share.

- **It adapts rank-based growth accounting to noisy platform activity.** The substantive measurement problem is separating lasting movement from temporary fluctuations before estimating volatility, incumbent decline, boundary crossings, and entry and exit.

- **It demonstrates that ladder geometry changes turnover profiles under unchanged dynamics.** A flatter upper ladder can make smaller top sets turn over faster, without requiring a special growth process at the top.

- **It identifies specific failures of calibrated benchmarks.** The models miss the speed of the submissions head’s decline and do not jointly reproduce Reddit’s persistence, its distribution across ranks, and its asymmetry between gains and losses.

Those contributions could justify PNAS. The most persuasive paper would present them as a connected empirical argument: **measure movement, derive a conditional benchmark, and locate consequential departures.** At present, the reader must work too hard to distinguish the accounting identities, the independently evaluated predictions, and the descriptive discoveries.

## Position relative to the requested literatures

| Literature | What is already established | Where this paper can add value |
|---|---|---|
| **Stochastic portfolio theory and rank-based diffusions: Fernholz; Banner, Fernholz, and Karatzas** | Stable distributions with changing occupants, rank-dependent growth, and the relationship between volatility, relative growth, and rank gaps are established. Fernholz’s empirical methods also estimate distribution-shaping components from panels. | The adaptation to highly noisy platform activity and its finite-horizon empirical evaluation. **Fernholz (2017), currently confined to the SI, belongs in the main text.** [Intech](https://www.intechinvestments.com/resources/atlas-models-of-equity-markets/?utm_source=chatgpt.com) |
| **Gabaix (2009)** | Random growth is an established explanation of size distributions across economic settings. The existence of concentration under random growth is background, not the paper’s empirical discovery. | Measuring whether the relevant ingredients and their implications describe online communities. The current citation is appropriate. [Annual Reviews](https://www.annualreviews.org/content/journals/10.1146/annurev.economics.050708.142940?utm_source=chatgpt.com) |
| **Gabaix, Lasry, Lions, and Moll (2016)** | A model that explains a distribution’s stationary shape can nevertheless change that distribution too slowly. Type dependence and scale dependence provide distinct routes to faster transitions. | Applying that question to online attention: how fast should the hierarchy move given measured individual dynamics? The frame is strong; transferring a particular convergence formula requires additional justification. [xgabaix.scholars.harvard.edu](https://xgabaix.scholars.harvard.edu/sites/g/files/omnuum7761/files/xgabaix/files/dynamics_of_inequality.pdf?utm_source=chatgpt.com) |
| **Firm and city dynamics** | Stable size distributions, leadership turnover, and the difficulty of reconciling large units with their observed ages have substantial precedents. Luttmer’s firm-growth problem is especially close to the paper’s concern with incompatible individual and aggregate time scales. | The joint comparison of movement, membership, and concentration in platforms. Retain Sutton and Batty, and add Luttmer (2011). [Federal Reserve Bank of Minneapolis](https://www.minneapolisfed.org/research/working-papers/on-the-mechanics-of-firm-growth?utm_source=chatgpt.com) |
| **Cultural evolution: Bentley and Acerbi** | Neutral copying can produce regular turnover; deviations in turnover profiles can help distinguish specified transmission models. | Your controlled geometry comparison identifies an additional consideration when interpreting profiles across empirical systems. It does not establish that turnover profiles contain no information about copying: copying dynamics can themselves shape the ladder. [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S109051380600095X?utm_source=chatgpt.com) |
| **Platform concentration and online communities** | Concentrated attention without stable popularity has already been studied directly on Reddit. Community decline, competition, and mutualism also have relevant empirical literatures. | A different unit of analysis, explicit lasting-versus-passing measurement, rank-resolved accounting, and calibrated transition comparisons. These distinctions need to appear prominently. [academic.oup.com](https://academic.oup.com/joc/advance-article-pdf/doi/10.1093/joc/jqac032/45974939/jqac032.pdf?utm_source=chatgpt.com) |

### The most important omission: Hagar and Shaw

**Hagar and Shaw (2022) is the missing reference most likely to trouble a communication referee.** Their study reports concentrated news-source attention on Reddit without the stable source popularity associated with their cumulative-advantage model. That is close enough to your motivating distinction that the Introduction must engage it directly. [academic.oup.com](https://academic.oup.com/joc/advance-article-pdf/doi/10.1093/joc/jqac032/45974939/jqac032.pdf?utm_source=chatgpt.com)

Their units are **news sources within Reddit communities**; yours are principally **communities within Reddit**. Your measurement and transition analyses provide additional contributions. Nevertheless, “Platform research rarely uses this theory” followed by “therefore lack a benchmark” on main p. 2 currently sweeps too broadly. The stronger positioning explains exactly what prior benchmarks did not measure.

I verified Hagar and Shaw’s publication details and publisher abstract; I did not independently obtain their complete article.

### References to add, promote, or update

1. **Essential addition:** Hagar, N., & Shaw, A. (2022). Concentration without cumulative advantage: The distribution of news source attention in online communities. *Journal of Communication, 72*(6), 675–686. [https://doi.org/10.1093/joc/jqac032](https://doi.org/10.1093/joc/jqac032). [academic.oup.com](https://academic.oup.com/joc/advance-article-pdf/doi/10.1093/joc/jqac032/45974939/jqac032.pdf?utm_source=chatgpt.com)

2. **Promote from SI to main text:** Fernholz, R. T. (2017). Nonparametric methods and local-time-based estimation for dynamic power law distributions. *Journal of Applied Econometrics, 32*(7), 1244–1260. [https://doi.org/10.1002/jae.2573](https://doi.org/10.1002/jae.2573). This is the closest methodological predecessor to the rank-by-rank measurement argument. [ricardofernholz.com](https://www.ricardofernholz.com/Research/powerLaws.pdf?utm_source=chatgpt.com)

3. **Add to the discussion of convergence:** Ichiba, T., Pal, S., & Shkolnikov, M. (2013). Convergence rates for rank-based models with applications to portfolio theory. *Probability Theory and Related Fields, 156*, 415–448. [https://doi.org/10.1007/s00440-012-0432-5](https://doi.org/10.1007/s00440-012-0432-5). It addresses a closer mathematical class than a reflected single-particle growth process, although it does not automatically validate your numerical rates. [Springer Nature Link](https://link.springer.com/article/10.1007/s00440-012-0432-5?utm_source=chatgpt.com)

4. **Add beside GLLM:** Luttmer, E. G. J. (2011). On the mechanics of firm growth. *The Review of Economic Studies, 78*(3), 1042–1068. [https://doi.org/10.1093/restud/rdq028](https://doi.org/10.1093/restud/rdq028). [Federal Reserve Bank of Minneapolis](https://www.minneapolisfed.org/research/working-papers/on-the-mechanics-of-firm-growth?utm_source=chatgpt.com)

5. **Useful addition for unresolved community dynamics:** TeBlunthuis, N. (2025). Niche dynamics in complex online community ecosystems. *Proceedings of the International AAAI Conference on Web and Social Media, 19*(1), 1880–1892. [https://doi.org/10.1609/icwsm.v19i1.35907](https://doi.org/10.1609/icwsm.v19i1.35907). Its evidence of episodic competition and mutualism provides a substantive alternative to independent community growth components. [ojs.aaai.org](https://ojs.aaai.org/index.php/ICWSM/article/view/35907?utm_source=chatgpt.com)

6. **Update the banking application:** Fernholz, R. T., & Koch, C. (2021). The rise of big U.S. banks and the fall of big European banks: A statistical decomposition. *European Economic Review, 135*, 103723. [https://doi.org/10.1016/j.euroecorev.2021.103723](https://doi.org/10.1016/j.euroecorev.2021.103723). Consider replacing or supplementing the 2016 working-paper citation with this published treatment. [Ricardo T. Fernholz](https://www.ricardofernholz.com/Research/?utm_source=chatgpt.com)

The recent Baker–Smyth and Mallmin–Traulsen–De Monte references are real and relevant; I found no basis for challenging their existence or the basic way they are invoked. Their results should remain comparisons, rather than support for transferring a convergence rate to your particular model. [arxiv.org](https://arxiv.org/pdf/2608.27156?utm_source=chatgpt.com)

# 3. The frame and the title

## The GLLM frame is earned as a question; the numerical transfer needs repair

Your strongest framing is: **a model can explain why a hierarchy persists and still fail to explain how quickly it changes.** That puts the comments benchmark and the submissions departure within one argument. It also avoids presenting coexistence of concentration and turnover as a mathematical surprise.

The problem is the movement between three different quantities:

1. A characteristic diffusion time, \(S_k^2/\sigma_k^2\).
2. A local relaxation time toward a proposed balance.
3. An exponential-equivalent membership time inferred from one-year replacement.

They are related, but they are not interchangeable measurements of “how long members take to turn over.”

For example, using SI pp. 3 and 6:

| Location | Characteristic movement time | Membership time inferred from annual replacement | Local relaxation time |
|---|---:|---:|---:|
| Comments, \(K/64\) | 2.3 years | 4.24 years | 9 years |
| Comments, \(K/2\) | 5.9 years | 5.74 years | 5–10 years |

At the comments head, relaxation is about **four times the diffusion scale**, but only **twice the membership equivalent**. At \(K/2\), neither comparison establishes a consistent several-fold separation. These are calculations from the printed numbers, not estimates from additional data.

### The consequential algebraic correction

Write \(q=\sigma_k^2\), \(m=\bar s_k-e_k\), and \(\ell=\ln(K/k)\). Your definitions imply

\[
S_k^*=\frac{q}{2m},
\qquad
T_{\mathrm{move}}=\frac{S_k^2}{q},
\qquad
\tau_k=\frac{2(S_k^*)^2\ell}{q}
      =\frac{q\ell}{2m^2}.
\]

Therefore, **holding measured net decline fixed, correcting an inflated diffusion variance lengthens the movement time but shortens the local relaxation time.** The statement that the correction makes all the times longer is wrong.

The potential consequence is substantial. Applying the SI’s illustrative 79% variance overstatement to the comments-head calculation would change 2.3 years to approximately 4.1 years and 9 years to approximately 5.0 years. The ratio becomes about 1.2, rather than 3.9.

**That is a conditional sensitivity calculation, not a re-estimation of your data.** A full correction could also change estimated decline. But it shows why variance contamination cannot be presented as a uniformly conservative qualification.

## How strongly to pitch durability

I would retain **durability as an expected property of calibrated homogeneous growth near balance**, while making the extent of durability conditional on rank and parameter estimates.

Also define *homogeneous* explicitly. Your model permits growth and volatility to depend on rank or size. Here, the intended distinction appears to be the absence of a persistent community-specific growth component, not identical drift and variance at every position.

Keep the formulas as intuition. Let the actual model establish the finite-horizon quantitative claim. This does not require a new convergence theorem.

## Title

The current title is good: restrained, intelligible, and centered on the substantive question. Its main weakness is that “outlasts its members” can suggest community lifetimes rather than membership in ranked sets. Define that distinction immediately.

Three alternatives:

1. **Why online attention hierarchies can outlast their members** — my preference.
2. **Random growth and the durability of online attention hierarchies**
3. **Durable hierarchies and persistent decline in online attention**

I prefer the first because “can” states the benchmark’s implication without suggesting that the paper has established its quantitative strength everywhere.

# 4. Claims against evidence

## “Reddit’s leading communities turn over while the shape of the hierarchy changes slowly” — main p. 2

**Assessment:** the descriptive coexistence is supported. The distinction between rank stability and loss of share is particularly effective.

**First attack:** “slowly” depends on the metric. A fall from 50.2% to 34.0% in the submissions head’s share is a **32% relative decline**, even if a mean-log span changes modestly. A referee could argue that the chosen shape statistic makes a consequential redistribution sound negligible.

**Answer:** show the log-span and arithmetic-share changes together and define durability relative to the benchmark. The claim should be that the distribution remains strongly unequal while membership changes, with substantial deconcentration in a particular part of it.

Also revise the explanation of two-year replacement. The 1.3–1.7 ratio does not itself identify temporary swings: diffusion across a boundary can produce approximately square-root scaling with horizon, including a ratio near \(\sqrt{2}\), without a separate transitory component. Compare lasting-only and full-observation-model retention curves if you retain that explanation.

## “Random growth, measured rank by rank, implies a hierarchy that outlasts its members” — main pp. 2–3

**Assessment:** this contains the paper’s central contribution, but also its most vulnerable quantitative claim.

**First attack:** the time-scale separation relies on a variance estimator shown to be contaminated by persistent growth and on relaxation formulas whose assumptions differ from the actual model. The crossing estimator also contains a material composition term.

**Answer:** estimate the finite-horizon comparison in the actual curved, rank-dependent model, with entry and exit, and propagate joint uncertainty in variance and decline. Separate three things in the presentation: the accounting relation, the measurement check, and the substantive prediction.

The geometry experiment is valuable, but its conclusion should be **“geometry can generate the observed ordering under unchanged dynamics.”** The simulated data-ladder example gives replacement of 0.21 versus 0.14, while the corresponding empirical values are approximately 0.21 versus 0.18. It explains the ordering more convincingly than the complete magnitude of the profile.

The comparison with firm volatility exponents also needs caution: the SI’s recovery results do not support treating every estimated exponent as directly comparable across data-generating processes.

## “Built from the measured ingredients, a model forms a hierarchy, but one flatter at the head” — main pp. 3–4

**Assessment:** supported as a generative demonstration, including its failure to reproduce the observed head.

**First attack:** “the model” refers to different drift calibrations. SI §§6, 9, and 11 distinguish drift measured from slides from drift chosen to hold the training ladder. The blanket statement “None is fitted to the statistics we test” invites a circularity objection.

**Answer:** add an explicit calibration map identifying the inputs and evaluated outcomes for each experiment. The from-equality exercise is not rendered circular by the existence of a separate ladder-calibrated world; the main text simply needs to preserve that distinction.

Similarly, describe retention forecasts as having small absolute errors and **mixed incremental value**. On comments, carrying the previous year forward has MAE 0.013 against the model’s 0.022. “Forecasts membership well” is defensible descriptively, but does not establish predictive superiority.

## “The head of submissions fell faster than calibrated homogeneous growth forecasts” — main p. 4

**Assessment:** the observed outcome is clearly below the displayed conditional model projections. This is potentially the paper’s most important departure.

**First attack:** the decline is concentrated in **recorded posting**, while the archive changed during the relevant period. The manuscript explicitly says capture is not fully audited. In addition, intervals from 20 simulations with parameters fixed omit calibration uncertainty.

**Answer:** audit capture by date and rank; repeat the principal findings under defensible alternative rosters and tracking thresholds; examine sensitivity to the documented bans and exceptional collapses; and quantify how large a differential capture change would be required to explain the result.

An illustrative calculation makes that last task concrete. The reported head-versus-rest odds of a recorded submission change by

\[
\frac{0.087/(1-0.087)}{0.224/(1-0.224)}\approx0.33.
\]

Under a simple detection model with unchanged true shares, relative detection of head versus other submissions would need to fall by about two thirds to account for the entire item-share change. This does not establish that capture caused the result; changing head membership also complicates the comparison. It identifies a sensitivity quantity worth estimating carefully.

## “On Reddit, changes persist beyond homogeneous growth” — main pp. 4–5

**Assessment:** the disjoint-window construction is a real strength. It addresses the positive correlation induced by adjacent averages of a random walk.

**First attack:** the exclusion checks do not establish that persistence survives both major calendar confounders. SI §7.4, p. 8, explicitly says none excludes both; the restricted comparisons use only one to four temporal origins. Thousands of communities do not create thousands of independent realizations of the platform’s history.

**Answer:** show rank-resolved persistence and uncertainty, identify the effective temporal replication, and state precisely which noise process and fitted model family are rejected. Preserve the matched-horizon Facebook comparison: its shorter record is not an independent annual-horizon replication.

The last sentence of this subsection—“We have not shown that the communities whose declines persist are the ones that drove the head’s fall”—is essential. Keep it. The proximity of the findings otherwise encourages a causal or accounting connection that has not been demonstrated.

# 5. Heterogeneous growth

## Recognizable and serious, but not yet a broad rejection

An income-dynamics economist should recognize the specifications and appreciate several choices: indirect inference through the same measurement pipeline, joint attention to covariance and variance, recovery experiments, the grid extension, and explicit reporting of successes and failures. Repeating the earlier criticism that heterogeneity was never tested would be unfair.

The remaining objection is **which restrictions the failures reject**.

First, the fixed and autoregressive models impose one spread of persistent growth across ranks. SI §10 explicitly says this produces excessive persistence at the head and insufficient persistence below it. That is evidence against the imposed dispersion structure; it is not evidence against persistent heterogeneity generally.

Second, the initial relationship between size and type is unclear. Independently assigning growth types to an observed ladder differs from initializing a population in which types have already sorted into sizes. Forecasts should also condition each community’s latent type or state on its training history.

Third, the factor model loses most community-level variation through shrinkage. Its failure is weak evidence against platform exposure as an explanation, especially when the empirical platform-total adjustment removes much of submission persistence.

Fourth, Gaussian primitives do not guarantee symmetry of the final observed statistic after rank drift, boundaries, exits, and selection. Your simulations establish an opposite-signed contrast; use that result instead of asserting that Gaussian heterogeneity necessarily predicts none.

This matters because the income literature already documents substantial nonnormality and asymmetric persistence. Guvenen and colleagues’ paper, already cited as reference 31, reports persistent negative changes and transitory positive changes in parts of the earnings distribution and estimates richer mixture processes. The comparison should engage those findings rather than make persistent decline sound alien to standard earnings dynamics. [papers.serdarozkan.me](https://papers.serdarozkan.me/assets/GKOS_2021.pdf?utm_source=chatgpt.com)

A defensible conclusion is:

> The three fitted specifications do not jointly reproduce the observed persistence, its distribution across ranks, and the evolution of concentration.

Avoid interpreting the ratio of two correlations as the fraction of a mechanism explained.

## Is a reversible decline state the right next analysis?

**Yes. I would do it before submission if the strong heterogeneous-growth conclusion remains central.** It targets the observed asymmetry and recovery question much better than another symmetric persistent component.

GLLM’s main type example has high- and low-growth states, with movement from high to low; footnote 49 discusses bidirectional switching. Your proposal is a natural adaptation, although it is not literally the same model with signs reversed. [xgabaix.scholars.harvard.edu](https://xgabaix.scholars.harvard.edu/sites/g/files/omnuum7761/files/xgabaix/files/dynamics_of_inequality.pdf?utm_source=chatgpt.com)

A minimal specification is

\[
dH_i(t)=
\left[g(r_i(t))-\delta\{Z_i(t)-\pi_D\}\right]dt
+\sigma(H_i(t))\,dW_i(t),
\]

where \(Z_i\) indicates a decline state, entry into decline has intensity \(a\), recovery has intensity \(b\), and \(\pi_D=a/(a+b)\).

There is an important identification constraint. For stationary switching,

\[
\operatorname{Cov}\!\left[\delta Z_i(t),\delta Z_i(t+h)\right]
=
\delta^2\pi_D(1-\pi_D)e^{-(a+b)h}.
\]

An OU growth component can have exactly the same covariance. **Fitting only the present covariance targets would therefore miss the reason to introduce switching.**

The useful evaluation would include:

- Persistence and recovery following negative changes.
- Sign-conditioned transitions and change-distribution asymmetry.
- Persistence by initial-rank stratum.
- Explicit initialization from training histories.
- Joint recovery of diffusion variance and growth-state persistence.

Leave some lag, sign, and concentration outcomes outside calibration. Also allow a limited sensitivity check on the existing models’ rank-independent dispersion restriction.

A successful persistence model need not forecast an unforeseen platform shock. Keep “accounts for persistent decline” and “forecasts the aggregate fall” as separate questions. None of this requires opening the sealed extension.

# 6. Internal consistency

The most consequential discrepancies are below. I distinguish confirmed errors from quantities that might use different samples but are not sufficiently documented.

## Confirmed errors and overstatements

| Issue | Passages and locations | Required correction |
|---|---|---|
| **Pooled statistic labeled as submissions** | Table 1, main p. 5: “The data (Reddit submissions),” with “crossings / predicted” **0.94**, caption interval **0.86–1.04**. SI §5.3, p. 5: submissions **0.802**, joint interval **0.633–1.009**; three ladders **0.942 (0.855–1.040)**. | Use the submissions value and interval, or explicitly identify this row as pooled. |
| **Entry/exit omitted from a slope identity** | SI §5.1, p. 5 defines \(S^*=\sigma^2/[2(\bar s-e)]\), then states \(c/\bar s=S^*/S\). Main p. 3 correctly uses \(c/(\bar s-e)\). The omission recurs in SI p. 10. | Retain \(e\) consistently. |
| **Incorrect balance diagonal** | Figure 2C caption, main p. 4: “On the diagonal the shape holds,” although axes are \(c\) and \(\bar s\). Main Eq. 2 gives \(\dot U=c-\bar s+e\). | Plot crossing gain against **net decline**, \(\bar s-e\), or explicitly describe the omitted term. At the present diagonal, change equals \(e\). |
| **Wrong direction of relaxation correction** | Main p. 2: variance inflation “would lengthen the time scales below.” SI p. 6: “The times would then be longer.” | As derived in §3 above, movement lengthens but local relaxation shortens when corrected variance falls and net decline is held fixed. |
| **Uniform log rank is the wrong sampling statement** | SI §5.5, p. 5: “a member placed uniformly in log rank … lies on average one e-fold above its boundary.” | A uniformly selected member is approximately uniform in **rank**; its log-distance has mean one. Call \(S^2/\sigma^2\) a characteristic diffusion time, not an expected hitting time. |
| **“Several times” is not uniform over comments** | Significance, main p. 1: shape takes “several times longer … than the members take to turn over.” SI p. 6: comments \(K/2\), relaxation **5–10 years**, movement **5.9 years**. | Specify the ranks and comparator. The displayed membership equivalent at \(K/2\) is approximately **5.74 years**. |
| **“At most three fifths” exceeds its demonstrated scope** | Abstract p. 1 and Discussion p. 6 use this unrestricted phrase. SI p. 12: fixed differences match comments at a 52-week gap, **0.09 versus 0.10**. | Restrict the comparison to designated shorter gaps, or report the correlations directly and say the full pattern is not reproduced. |
| **Five-year response uses an unexplained different rate** | Discussion p. 6: “a sixth to a third” of the effect appears within five years. SI p. 6 gives head relaxation times **9 years** for comments and **3–4 years** for submissions. | Those local exponential rates imply \(1-e^{-5/\tau}\), approximately **43%** and **71–81%**. If using the different current-slope spectral time, identify and justify it. |
| **Stable parameters do not guarantee stable measured slide** | Main p. 4: “Homogeneous random growth with stable parameters does not change its slide.” | Finite-horizon cohort slide depends on rank occupations, current gaps, and the reselected universe. State that the observed acceleration exceeds the fitted stable-parameter projections. |
| **Abstract turnover range excludes the smallest sets** | Abstract p. 1: “a sixth to a quarter of each Reddit top set.” Main p. 2: top-ten replacement **27–33%**. | Qualify the cutoff range or broaden the stated range. |
| **Minor inconsistent rounding** | Main p. 4 reports observed comments forecast-universe share **50%**; p. 5 reports **51%**. SI pp. 7 and 12 give **0.505**. | Use **50.5%** consistently. |

## Unresolved definitions and provenance

**Riser/faller contrast.** Main p. 5 gives faller correlations of “+0.16 to +0.19” and riser correlations of “0.00 to +0.03.” SI §7.3, p. 8 gives submissions **0.177 versus 0.031** and comments **0.158 versus 0.003**, differences of **0.146 and 0.155**. SI p. 12’s assessment table instead gives “fallers minus risers” as **+0.07** on both ladders. A different sample or aggregation may explain this, but the PDFs do not identify it. **Unresolved, not safely correctable by guessing.**

**Heterogeneous forecasts’ training information.** Main p. 5 and SI p. 12 say “Fitted to 2019–20” or “weeks before 2021.” SI p. 13 assigns heterogeneous-growth calibration to the “whole panel; loadings from 2019.” The annual nonoverlapping-change covariance design also needs more time than a simple two-year training panel provides. **I cannot verify from the PDFs whether those forecasts re-estimated every relevant parameter using training data alone.** This is a documentation gap, not proof of leakage.

**Historical factor path versus forecast.** SI pp. 11–12 correctly identifies the observed-factor-path world as conditional historical analysis and uses the realized 2021–22 path. Main statements that none of the three models “forecasts” the fall should preserve this distinction.

**Which model uses the ladder as an input?** Main p. 3 says “None is fitted to the statistics we test”; SI pp. 6, 10, and 13 identify a model world whose drift holds the training ladder. Specify the experiment whenever making a noncalibration claim.

**The opening’s top-12,500 figure.** Main p. 1 says a sixth of the top 12,500 communities is replaced annually. The displayed SI turnover table supports approximately 0.16 at **\(K/2=6,250\)**. The top-\(K\) value may exist in the outputs, but it is **unverifiable from the PDFs**.

**Completeness and selection.** SI p. 2 says tracking every community ever inside weekly \(4K\) makes every top-\(k\) set complete. That guarantees coverage of observed weekly top sets, not generally of sets ranked by annual totals. Main p. 6’s “selection on noise is excluded by design” also overstates what disjoint cohort blocks remove: whole-panel roster selection and 90%-presence eligibility remain. Actual effects require sensitivity analysis.

The main p. 5 claims of roughly 30% variance inflation in an assessment comparison and 40–60% attribution of excess slide are also not fully reproducible from the displayed tables. They need explicit output references or compact supporting tables.

## Differences that should not be “corrected” as errors

- **34.0% versus 35.6% submissions share**, and **49.7% versus 50.5% comments share**, use descriptive versus forecast universes. Figure 4’s caption acknowledges this; make it conspicuous in the prose.
- **0.64 crossings/net decline versus approximately 0.72 crossings/slide** can be consistent because their denominators differ.
- **0.942 versus 0.955 pooled boundary ratios** distinguish the three-ladder result from the four-ladder calculation that includes the Instagram censoring model.
- Different “constant-slope” simulation results need not conflict: the SI includes both illustrative worlds and matched geometry experiments.

Several earlier repairs also check out: the adjacent-block averaging correlation, the corrected one-step variance bookkeeping, and the \(q+3Bh\) contamination formula. The remaining concerns are specific; they do not justify dismissing all the mathematics.

# 7. Writing and structure

## Opening

The opening reaches the right question, but mixes four levels of analysis: individual content items, communities, households, and websites. Attention to an item decaying within days is not evidence that a community loses its position. Start with the community-level distinction, then introduce the broader literature.

The sentence “Yet the leaders do not stay” is also stronger than your excellent examples of leaders retaining rank one while losing share. Those examples are more interesting than the simplified assertion.

## Topic sentences and flow

The descriptive subsection and the submissions-decline subsection have clear organizing claims. The second Results subsection is overloaded: accounting, selection, estimator validation, heterogeneity bias, geometry, firm scaling, funnel plots, balance, and time scales all appear under one heading.

I would separate **the measured balance**, **turnover geometry**, and **the finite-horizon benchmark**. Put estimator qualifications next to the quantities they qualify, but move the detailed recovery diagnostics to a clearly signposted SI section.

The Discussion’s second paragraph—“That the balance is automatic … That persistence is fixed heterogeneity …”—reads like a response letter. Reorganize it around the substantive findings and unresolved alternatives. Readers should not need to reconstruct the earlier referee rounds.

## Density

The problem is conceptual compression more than sentence length. The present abstract is approximately **236 words**, and the significance statement **119**; both already satisfy your requested limits. Cutting words alone will not solve the density. Reduce the number of results each paragraph is asked to carry.

## Figures and tables

- **Figure 1 is effective**, especially panel D’s distinction between retained rank and lost share.
- **Figure 2A needs the substantive revision discussed above.** Figure 2C needs the corrected net-decline axis.
- **Figure 3B should emphasize years 0–5.** Twenty-five of its thirty displayed years lie beyond the substantive horizon. Move the long continuation to the SI.
- **Figure 4C should use its vertical space more effectively** and distinguish matched raw/residual comparisons. Most relevant correlations lie between roughly −0.1 and +0.2.
- **Figure 4D needs explicit ladder labels and a symmetric comparison layout**, or an explanation of why the comments calibrated-model comparison is absent.
- **Table 1 is not readable at ordinary page size.** Rebuild it across the full page width or split it. The SI provenance and uses tables have a similar scaling problem.

## Ready-to-paste replacements

These versions use claims supported by the present PDFs and avoid the disputed blanket time-scale and heterogeneity statements. They do not substitute for the analyses recommended above.

### Title

Why online attention hierarchies can outlast their members

### Abstract — 214 words

Online attention remains concentrated even as leading communities change. We study this coexistence using weekly activity in Reddit communities from 2019 to 2022, with Facebook pages from 2020 to 2022 providing a shorter comparison. Among the larger Reddit top sets analyzed, 15–26% of members are replaced annually. We measure lasting volatility, the relative decline of incumbent members, gains from communities crossing each ranking threshold, and entry and exit. Rank-based random-growth theory links these movements to changes in the hierarchy. In homogeneous benchmarks calibrated to these measurements, offsetting flows allow the hierarchy to change more slowly than its membership, particularly for comments. Controlled simulations show that a flatter ladder near the top can produce faster turnover there under unchanged growth dynamics. The calibrated models nevertheless miss important changes. In the training-defined universe, the leading submission communities hold 35.6% of activity in 2022, compared with a homogeneous-model projection of 53.7%. Their loss of share reflects fewer recorded submissions rather than lower karma per submission. Annual changes also remain positively correlated across disjoint observation windows on Reddit, especially among communities with prior declines. The fitted fixed, autoregressive, and platform-exposure growth variants do not jointly reproduce these patterns. These development-data results provide a benchmark for evaluating entrenchment and concentration without inferring a platform mechanism from the hierarchy’s shape.

### Significance statement — 108 words

Stable concentration is often taken as evidence that online leaders are entrenched. Yet a hierarchy can persist while its members change. We use Reddit activity and rank-based random-growth models to measure the movements that sustain this pattern. Near balance, communities crossing into top sets offset much of their incumbents’ decline, allowing the hierarchy to change slowly. A flatter ladder also makes small top sets easier to enter and leave. The same benchmarks reveal their limits: they miss Reddit’s faster decline in concentration and persistent losses below the head. These results help researchers distinguish stable inequality from stable incumbency and identify departures that require explanations beyond the fitted models.

### First paragraph of the Introduction

The citation number below preserves your existing reference to *The Internet Trap*. The following literature paragraph should then engage Hagar and Shaw explicitly.

Online attention can remain concentrated even as the communities receiving it change. This distinction matters because unequal attention and entrenched leadership are different outcomes: the first describes a distribution, while the second concerns the continued advantage of particular communities. In Reddit’s larger top sets, roughly one fifth of communities is replaced each year; even a community that remains ranked first can lose most of its activity share. Such movement need not dissolve the hierarchy. The question, posed for web traffic in earlier work (15), is how random growth can sustain a concentrated distribution while changing its membership, and what departures from that growth would move concentration faster.

### First paragraph of the Discussion

Random growth provides a benchmark for distinguishing a durable hierarchy from durable incumbency. At the parameters measured here, the homogeneous model allows substantial membership turnover while the broad distribution changes slowly, particularly for Reddit comments. The departures are equally informative. The submission head lost share faster than the calibrated models projected, and annual changes persisted across disjoint windows, especially among communities already in decline. The three fitted forms of heterogeneous growth reproduce some features but do not jointly explain the persistence, its distribution across ranks, and the change in concentration. These findings identify limits of the benchmarks without establishing why decline accelerated or whether the communities with persistent losses drove the head’s fall.

# 8. Uses and non-uses

This is one of the paper’s strongest sections. The proposed uses on main p. 6 are coherent and potentially valuable. They need a little more operational precision.

| Proposed use | What the researcher should specify | What the result can establish |
|---|---|---|
| **Entrenchment benchmark** | Whether entrenchment means retaining rank, staying in a top set, retaining share, or sustaining favorable growth. | Whether that particular statistic departs from the calibrated benchmark. |
| **Selection baseline** | Selection window, observation noise, eligibility rules, and forecast horizon. | The expected regression associated with that selection procedure. The approximately 5% figure is not a universal correction. |
| **Turnover interpreted through geometry** | The ladder slope, cutoff, horizon, and movement distribution. | Whether differing turnover profiles require differing dynamics under the specified comparison. |
| **Accounting for concentration change** | A consistent universe, incumbent set, entry/exit definition, and activity measure. | Which measured components contribute to the change, without establishing their causes. |

Three additions would improve the section for platform and policy readers.

**First, agreement with a benchmark does not establish absence of platform influence.** Calibrating the model to an existing platform incorporates the consequences of its design into estimated volatility, decline, and entry. A match means the chosen statistic does not distinguish additional mechanisms; it does not show that recommendation systems or favoritism are irrelevant.

**Second, distinguish concentration across communities from platform market power.** A platform can become less concentrated internally while retaining or increasing its control over users, distribution, or monetization. Turnover among communities also need not imply turnover among owners, political interests, or social groups.

**Third, make the observation requirements usable.** State which tasks need activity levels below the cutoff, which require stable identifiers and an entry/exit record, and which cannot be supported by a short panel. The SI contains much of this information, but a compact practical paragraph would be more useful than the present long table.

The non-uses are convincing: no demonstrated advantage in individual forecasting, no mechanism identification from shape, no long-run forecast, and no intervention effect from deleting a model component. Retain them.

The remaining exception is the language about how quickly concentration “can respond.” Until the rate inconsistency is resolved, that is the least secure advertised use. Afterwards, call it a **conditional model response**, not a limit on what a policy or platform redesign could accomplish.

# 9. The five most valuable changes before submission, ranked

| Rank | Change | Why it matters | New analysis or rewriting? |
|---|---|---|---|
| **1** | **Rebuild the durability comparison using the actual model over the stated horizons.** Correct the variance-sensitivity algebra, distinguish the three time scales, and propagate uncertainty in variance and decline. | This supports the title, significance statement, and principal theoretical interpretation. The present numerical argument gives a mathematical referee a direct objection. | **New analysis**, plus rewriting. |
| **2** | **Audit archival capture and quantify sensitivity of the concentration and persistence departures.** Examine date-by-rank capture changes, alternative rosters, exceptional communities, and the differential missingness needed to erase the findings. | The most interesting substantive departure crosses a measurement change. Acknowledging that problem does not resolve it. | **New analysis.** |
| **3** | **Make the heterogeneous comparison more informative and its forecast provenance explicit.** Test a parsimonious reversible decline state, address initial type–size sorting and the common-dispersion restriction, and distinguish historical factor-path analysis from forecasting. | This addresses the strongest remaining economist’s objection while preserving the useful failures already found. | **New analysis and methods clarification.** |
| **4** | **Sharpen the marginal contribution against the closest prior work.** Add Hagar–Shaw and Luttmer; promote Fernholz’s empirical methods; engage rank-based convergence and asymmetric earnings dynamics. | The paper needs to establish why this application changes what platform researchers can measure or infer. | **Only rewriting and reference revision.** |
| **5** | **Reorganize the central Results, rebuild the tables, and reconcile the reporting.** Separate accounting, geometry, and time scales; emphasize five-year figures; correct Table 1 and explain the persistence and universe differences. | Referees need to see exactly which evidence supports which claim. Presentational ambiguity currently makes legitimate experiments look less trustworthy. | **Rewriting and figure/table regeneration from verified outputs.** |

I would not require a causal explanation of platform change or opening the sealed extension before this paper can be submitted. I would require the present development evidence to support a clearly defined benchmark and a defensible account of where it fails.
