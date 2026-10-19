<!-- Provenance: Task 2 (stochastic-model methods) report by Claude Opus 5.5 (claude-opus-5-5) (Claude Code sub-agent, default effort, WebSearch/WebFetch; returned as text because sub-agents may not write report files), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

**The report file was not written.** The harness refused the Write to `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-19_literature_and_design_reviews/REPORT_T2_opus-5.5.md` because sub-agents may not write report files. I did not try another route. The full report is below; save it at that path. Nothing in the repository was changed.

**The five most important findings (about 145 words)**
1. **The closest competitor is cited but not engaged.** Baker & Smyth (arXiv, Aug 2026) pose the same puzzle for equities, predict the curve without fitting it, and test their time scales. They conclude "Turnover, not drift, stabilizes the calibrated market."
2. **"Outlasts its members" is close to generic random growth.** GLLM's rate σ²/(8S²) and membership turnover both scale as S²/σ². With the draft's numbers that gives about 19 years at the head, in line with the model's 18–27 years.
3. **Persistence belongs to the income-dynamics HIP/RIP literature** (MaCurdy; Abowd & Card; Baker 1997; Hryshko 2012). It has a standard estimator for the spread of mean growth, and heterogeneous growth may bias the draft's variance-based estimates of the crossing gain and β.
4. **The draft has every piece of the GLLM/Luttmer arc** (slow homogeneous model, then heterogeneity as the fix) but leads with the balance, which is a theorem (Fernholz & Fernholz 2020, eq. 2.17).
5. **One sentence links the draft to cultural evolution.** The ladder's geometry alone produces the turnover profile that field reads as anti-conformist copying (Acerbi & Bentley 2014).

---

# Task 2 report: the methods tradition of stochastic models of rank and size

**Model and interface.** Claude Opus 5.5 (model ID `claude-opus-5-5`), run as a Claude Code sub-agent at default reasoning effort. **Tools used:** WebSearch, WebFetch, local Read and read-only Bash (`cat`, `sed`, `grep`, `pdftotext`). **Date:** 2026-10-19.

## 0. Access check

**Local files I read in full.**
- `llm_fitting/runs/2026-10-19_literature_and_design_reviews/COMMON_BRIEF.md`, `T2_STOCHASTIC_METHODS.md`, `00_PRACTICES_ADOPTED.md`, and the first part of `T1_PLATFORM_LITERATURE.md`, read only for its tier definitions.
- `paper/pnas_draft.tex`, all 219 lines. `git diff 42955ac HEAD -- paper/` is empty, so this is v4.
- `paper/pnas_si.tex`, all 260 lines.
- `paper/references.bib`, all 38 entries.
- The five figures `fig1_puzzle.png` … `fig5_tipping.png`, viewed as images.
- `llm_fitting/runs/2026-10-19_external_critiques_round5/SYNTHESIS_R5.md`.
- `llm_fitting/PROGRAM_STATE.md`: lines 1–330 and the owner-decision lines (about 515–540). I did not read the rest.
- `llm_fitting/SEALED_PATHS.json`, read only to know what to avoid.

**What I did not open.**
- The compiled PDFs. I read the `.tex` sources instead.
- `llm_fitting/lit_review/`. It is untracked and not named in my brief, and leaving it closed keeps this report independent.
- Any raw data or sealed path.
- These paywalled texts, for which I saw only abstracts, records or secondary accounts:
  - Luttmer 2011; Baker 1997; Abowd & Card 1989; MaCurdy 1982; Ijiri & Simon 1974;
  - Denrell & Liu 2012; Henderson et al. 2012; Jones & Kim 2018; Keitt & Stanley 1998; Chisholm et al. 2014; Kalyuzhny et al. 2014/2015;
  - *The Internet Trap* itself.

**How quotations were checked.** WebFetch passes pages through a summarizer that limits verbatim output to about 125 characters.
- **Longer quotations** were checked in the PDF text itself. WebFetch saved each PDF to the session's tool-results folder, outside the repository; I extracted the text with `pdftotext` into my scratchpad and searched it. This covers GLLM, Baker & Smyth, Fernholz & Fernholz, Hryshko, Mitzenmacher, Acerbi & Bentley and the Guthro review.
- **Shorter quotations** labelled "verified via abstract or page fetch" were returned verbatim by the fetch tool from the page named. Treat them as one notch below the PDF-checked ones.

**Repository writes.** None. The intended report file was refused by the harness; see the note above.

---

## Summary: the five things that matter most

1. **The closest competitor is cited but not engaged.** Baker & Smyth (arXiv, Aug 2026) ask the draft's question for equity markets: why the size distribution "keeps roughly the same shape over decades despite regular firm turnover". They differ from the draft in three ways:
   - they predict the curve without fitting it;
   - they test the relaxation time scales they derive against a century of data;
   - they conclude the opposite about what holds the curve. Their abstract says "Turnover, not drift, stabilizes the calibrated market."

   The draft cites them in one clause (l. 93). This needs a paragraph and a notation map (§3, item 1).
2. **"Outlasts its members" is close to a property of random growth itself, not a finding.** GLLM's spectral-gap formula gives the cross-section's relaxation rate as σ²/(8S²). A member's lasting moves cross one slope unit on a time of order S²/σ². Both scale with S²/σ², so near balance the shape outlasts membership by a roughly fixed factor (§5, threat 2; my derivation, to be checked).

   With the draft's own numbers, the formula puts the relaxation time at the head at about 19 years. That is in line with the model's 18–27 years. The draft should state this up front and present the comments/submissions-head contrast as distance from balance (S/S*).
3. **The persistence result belongs to an existing identification literature.** The draft's gapped correlation is the long-lag autocovariance test of income dynamics (MaCurdy; Abowd & Card), and rival (b) is the HIP/RIP debate (Baker 1997; Guvenen 2009, already cited; Hryshko 2012).
   - That literature has a standard estimator: minimum distance on the whole autocovariance function. It would report the spread of mean growth with an interval.
   - It also warns that heterogeneous growth makes variance grow quadratically with horizon, which would bias the draft's lasting variance, and hence c_k and β (the round-5 issue G15).
4. **The strongest papers in this tradition share one arc, and the draft has every piece of it but leads with the wrong one.** The arc:
   - a canonical model calibrated to measured ingredients explains the level but is too slow for the observed change;
   - a minimal heterogeneity fixes the speed.

   GLLM's "order of magnitude too slow" and Luttmer's "hundreds of years" are the canonical cases. The draft already has the slow model (l. 160: no forecast of the head's fall), the heterogeneity (persistence) and its share of the excess slide (R10). The abstract nonetheless leads with the balance, which is a theorem (Fernholz & Fernholz 2020, eq. 2.17).
5. **One cheap result speaks to another field.** Cultural-evolution studies read a turnover profile in which the most popular items change fastest as a sign of anti-conformist copying (Acerbi & Bentley 2014). The draft's R9 shows the ladder's geometry produces that profile under identical dynamics. One sentence turns a defensive result into a contribution (§3, item 5).

---

## 1. What these models are good for, and where they are weak

| use | strongest example(s) | what the draft does | comment |
|---|---|---|---|
| **Explaining a stationary distribution** | Gabaix 1999 (cited). Fernholz & Fernholz 2020: an Atlas model is exactly Zipfian under conservation and completeness, with slope parameter s_k = σ²/(2g) (eq. 2.17). Grilli 2020: macroecology | Eq. 3 and S* = σ²/(2(s̄−e)) | The relation is a theorem. Its value lies in measuring the ingredients, not in deriving it (§5). |
| **Counterfactuals** | GLLM 2016: transitions after a change in the environment. Jones & Kim 2018: creative destruction caps top inequality. Baker & Smyth 2026: removing turnover from the calibrated model removes convergence | 30-year removals of volatility or slide (Fig. 4C) | The removals illustrate the forward equation. The best counterfactual papers ask a substantive "what if" with policy content, such as a tax change, entry policy or index rules. |
| **Decomposing a change** | Gomez 2023 (cited). Fernholz & Koch 2016: the rise in US bank concentration came from lower reversion for holding companies but higher volatility for commercial banks. Blanchet 2022: wealth-distribution change from repeated cross-sections. Baker & Smyth: decade-by-decade growth accounting | Gomez split (Fig. 5C), items × per-item split, R10 | Fernholz & Koch is the nearest template for "volatility or reversion moved concentration". |
| **Measuring a hidden parameter** | Fernholz 2017 (cited): rank-based reversion from local times. Guvenen 2009 / Hryshko 2012: variance of individual growth rates. Sinatra et al. 2016: a stable personal parameter Q. Dennis et al. 2006: process versus observation noise | lasting σ_k, β, gapped persistence | The draft never reports a measured spread of mean growth with an interval. This is the obvious hidden parameter (§3, item 3). |
| **Detecting departures from a null** | Stanley et al. 1996 (cited): β ≠ 0 rejects Gibrat. Newberry et al. 2017: drift versus selection in language. Acerbi & Bentley 2014: turnover profiles against neutral copying. Kalyuzhny et al. 2015: neutral theory fits statics but needs environmental noise for dynamics | 20-seed null worlds; gapped statistic | Strong. The null-world battery is better than most papers in the tradition. |
| **Separating luck from lasting differences** | Henderson, Raynor & Ahmed 2012: Markov-chain nulls for sustained superior performance. Denrell & Liu 2012: extreme performance is weakly informative. Blumm et al. 2012 (cited): ranking phases set by noise against fitness | permanent-rank cohorts; passing versus lasting components | Underexploited (§4, item 2). |
| **Time scales** | GLLM 2016, eq. 13. Luttmer 2011. Ichiba, Pal & Shkolnikov 2013: convergence rates for rank-based models. Baker & Smyth: relaxation from months at the bottom to about 50 years at the top, tested by direct persistence of shape deviations | linearized relaxation (3–4 or 9 years); the model's decades | Needs the analytic anchor (§3, item 2). |
| **Forecasting** | Mostly weak. Wang, Mei & Hicks 2014 found the Wang–Song–Barabási citation model forecast worse than short-term citations. Gleeson et al. 2014 used dynamics to choose mechanisms rather than to forecast | gate: no better than simpler models | The draft's honest negative is the norm for this tradition. Claim explanation and time scales, not forecasts. |
| **Measurement from partial observation** | Fernholz & Fernholz 2020: truncating a complete system at the top 100 breaks quasi-Zipf behaviour. Open-markets SPT (Karatzas & Kim 2021; Itkin & Larsson 2024). Blanchet 2022. Knape 2008: observation error and reversion are jointly weakly identified | top-K universes with c_K correction; Instagram censoring | Cite the SPT open-markets work for the re-selected universe. Knape explains why the gate cannot see the lasting process. |

**Where these models are weak, and why.**
- **Equifinality.** Many mechanisms produce the same stationary shape. Mitzenmacher (2005) put the point bluntly: the field had stopped at observing and modelling and avoided validation. Gleeson et al. (2014) showed that only temporal statistics separate mechanisms that match the long-run distribution.
- **Drift is poorly identified from short series.** Baker & Smyth argue that the growth rate can only be learned from the length of the window. Their Remark 5.1 recovers drift from a cross-sectional identity instead. The draft's 30-year runs take drift from the 2020–22 slides (G6).
- **Reversion, process noise and observation noise trade off** (Knape 2008; Dennis et al. 2006). This is why short-horizon movement gates cannot see a slow lasting process.
- **Size–variance exponents from short or binned series are fragile.** Kalyuzhny et al. 2014 show Taylor-law exponents depend on series length and that sampling error alone can mimic a power law. The draft's own calibration (planted 0 measured as 0.06) shows the same thing.
- **Individual forecasting is generally poor** (Wang, Mei & Hicks 2014; the draft's gate).

---

## 2. The anatomy of the strongest articles

Across economics, SPT, ecology and cultural evolution, the papers that last share eight features.

**(a) A paradox stated as numbers.**
- Baker & Smyth open by noting that the capital distribution "keeps roughly the same shape over decades despite regular firm turnover" (p. 1) [verified: https://arxiv.org/pdf/2608.27156].
- GLLM open with the fast rise in top income inequality.
- Luttmer's abstract says that with many independent shocks "it can take hundreds of years for a few large firms to emerge" [verified via abstract page: https://ideas.repec.org/a/oup/restud/v78y2011i3p1042-1068.html].
- **The draft has this.** Its l. 90 states the puzzle well.

**(b) A canonical model whose calibrated failure is the result.** GLLM's working-paper abstract states it directly: "We show that standard theories, which build on a random growth mechanism, generate transition dynamics that are an order of magnitude too slow relative to those observed in the data." (NBER WP 21363, abstract page) [verified: https://www.nber.org/system/files/working_papers/w21363/w21363.pdf]. The published abstract, by a secondary account, says only "too slow".
- In ecology, the neutral model fits static abundance patterns but fails on fluctuations. Kalyuzhny, Kadmon & Shnerb (2015) and Chisholm et al. (2014) make temporal environmental variance the fix [abstract only].
- **The draft has the failure:** the homogeneous model "does not forecast the fall of the submissions head" (l. 160). But it places it in a forecasting paragraph rather than at the centre.

**(c) An analytic result linking measured quantities.**
- GLLM's eq. 13 gives the convergence rate as σ²/(8η²) with η = 1/ζ, which is the draft's S. They add that "high inequality goes hand in hand with slow transitions" (NBER WP p. 14) [verified: same URL].
- Fernholz & Fernholz give the Atlas slope s_k = σ²/(2g) (arXiv v7, eq. 2.17, p. 7) [verified: https://arxiv.org/pdf/1707.04285].
- **The draft has Eqs. 1–3.** It lacks a time-scale formula.

**(d) Ingredients measured independently, and a different statistic predicted.**
- Baker & Smyth measure volatility by rank and fit birth and death intensities to event counts. They then predict the curve, stressing that "no parameter is fitted to the curve itself" (p. 2) [verified].
- Grilli shows "the stochastic logistic model, quantitatively predicts the three macroecological laws, as well as non-stationary properties of community dynamics" (Abstract) [verified via page fetch: https://pmc.ncbi.nlm.nih.gov/articles/PMC7506021/].
- Maillart et al. "present three tests of the usually assumed ingredients of stochastic growth models" (abstract) [verified via page fetch: https://ar5iv.labs.arxiv.org/html/0807.0014].
- **The draft does this partly.** Its stationary slope from measured σ and s̄ is a genuine prediction, and it fails: 8–46% flatter (l. 157). The draft reads that failure as the ladder being out of balance. A provenance table would make the prediction-versus-fit status visible (§3, item 6).

**(e) A minimal departure that fixes the failure.** GLLM add heterogeneous mean growth (a high-growth regime). Luttmer adds firms with better ideas that grow fast and then revert. Ecology adds environmental noise. **The draft identifies the departure (persistence) but never puts it into the model** (§3, items 3–4).

**(f) Comparison across systems, data collapse or phase diagrams.**
- Iñiguez et al. (2022, cited) rescale two model parameters across 30 rankings and report that the values "collapse onto the universal curve" (Fig. 3a caption) [verified via page fetch: PMC8960905].
- Blumm et al. (2012, cited) define three ranking phases by noise. Fisher & Mehta (2014) give a niche/neutral phase diagram.
- **The draft has three ladders and eight worlds but no dimensionless coordinates** (§3, item 7).

**(g) Temporal statistics that separate models with the same stationary state.**
- Gleeson et al.: although several mechanisms match the long-run behaviour, "the only models that reproduce the observed temporal dynamics are those that strongly emphasize" recent over cumulative popularity (abstract, arXiv 1305.7440) [verified via page fetch: https://ar5iv.labs.arxiv.org/html/1305.7440].
- Mallmin et al. note that "neutral theories predict astronomically large turnover timescales" (Discussion) [verified via page fetch: PMC10945849].
- **The draft's gapped statistic is exactly this kind of discriminating statistic.** It is the paper's strongest methodological element.

**(h) Limits stated plainly.**
- Baker & Smyth: "We caveat that a century of data contains barely two half-lives of the slow mode." (p. 31) [verified].
- Grilli's Discussion lists patterns the model does not predict.
- **The draft is unusually strong here.** Its four-year record cannot test decade-scale relaxation, and it should say so in the same plain terms (§3, item 2).

**Overall.** The draft has (a), (c), (g) and (h). It has (b) and (d) only in weaker forms, and it lacks (e) and (f). The fastest gain is to reorganize the existing results around (b) → (e), the GLLM arc. This changes framing, not data.

---

## 3. What the draft should emulate (ranked; at most eight)

### Item 1. Engage Baker & Smyth as the closest predecessor, and map the notation

**Why.**
- Same question, a different system, and a conclusion that appears to be the opposite.
- In their usage "turnover" means listings and delistings. In the draft it means replacement within top sets plus crossings.
- They also state that time series cannot usefully identify drift (p. 2, Remark 5.1). That bears directly on G6, where the draft takes drift from the 2020–22 slides.

**Where.**
- l. 93: replace the clause "rank-based models with entry and exit describe the balance for equity markets" with two sentences.
- l. 137: one clause after "the gap identity of stochastic portfolio theory".
- SI §"The balance at every rank": a short notation map, as follows.
  - Their wave identity corresponds to the draft's Eq. 3 at stationarity.
  - Their reaction term corresponds to e_k.
  - Their "translation of the wave" corresponds to the universe's own gain c_K.

**Smallest change (my wording).** "Baker and Smyth ask the same question of equity markets and find that, with volatility measured by rank, listings and delistings rather than rank-dependent drift hold the curve. In attention ladders the measured effect of entry and exit is small against the slide (Fig. 3B), so the balance is carried by the slide." This sentence is safe only after the SI mapping confirms that e_k and their reaction term are comparable quantities. If G7 (double counting of e_k) is unresolved, say so.

**Cost and data.** About 60 words in the main text and about half a page in the SI. No new data.

### Item 2. Give the separation of time scales its analytic anchor

**Why.** This pre-empts threat 2 in §5 and supplies element (c) of §2 for time scales.

**The derivation (mine; please check).**
- In GLLM's reflecting-barrier model at balance, the relaxation rate is λ = σ²/(8S²), from their eq. 13 with η = S.
- A member's lasting displacement reaches one slope unit after a time of order S²/σ². This is also where the small-move form of Eq. 1, f ≈ E[D⁺]/S with E[D⁺] ∝ σ√t, becomes order one.
- So near balance the ratio of shape time to membership time is a fixed number of order 8, whatever σ and S are.

**Arithmetic from the draft's numbers (mine; please check).**
- Mean crossing gain c = 0.0040 a week (l. 147) with S ≈ 1 gives σ² ≈ 0.008 a week, about 0.42 a year.
- At S = 1, that gives λ ≈ 0.05 a year, a half-life near 13 years and 1/λ ≈ 19 years.
- This matches the model's 18–27-year relaxation at the head (SYNTHESIS_R5 G3) and the "two to three decades" at l. 147.
- Because λ ∝ 1/S², relaxation slows down the ladder where S rises to about 2. That matches the comments pattern of nine years at the head and up to two decades lower down.
- In Baker & Smyth the top is slowest, because entry and exit are weakest there. The draft's contrary ordering is itself informative.

**Where.**
- l. 147: after "the hierarchy outlasts its members".
- SI §"Shape change and the stationary steepness": add the formula and a table of λ_k from the measured σ_k and S_k.
- Restate the comments-versus-submissions-head contrast as S/S*. The submissions head has S ≈ 0.84–1.12 against S* ≈ 0.25–0.62 (SI), so it is far from balance and its linearized relaxation is fast.

**Smallest change.** One equation in the SI. In the main text: "In random growth near balance, the hierarchy's shape relaxes about an order of magnitude more slowly than its members turn over [GLLM]; the comments ladder is near balance, the submissions head is not."

**Cost and data.** Small. No new data. Also add one plain limit, modelled on Baker & Smyth: a four-year record cannot test a decade-scale relaxation time.

### Item 3. Estimate persistence as income-dynamics papers do

**Why.**
- The gapped correlation is a long-lag autocovariance test. Hryshko (2012) explains the logic: "Intuitively, σ_β² should be identified from higher-order autocovariances—when the contribution of the transitory component toward the autocovariances approaches zero." (p. 185) [verified: https://www.econometricsociety.org/publications/quantitative-economics/2012/07/01/Labor-income-profiles-are-not-heterogeneous-Evidence-from-income-growth-rates/file/36-239-1-CE.pdf].
- He also records that "Baker (1997) and Guvenen (2009) argued that such a test has low power" (p. 181) [verified, same file].
- The standard fix is minimum distance on the whole autocovariance function. Its output is σ_β, the spread of mean growth across units, with a confidence interval and a decay parameter.
- That is the hidden parameter the paper needs. The draft's SI only says that "a spread of 0.4 a year gives +0.17 to +0.19" in a synthetic check.

**What to fit.** Use the autocovariances of 13-week-window changes at lags 1…L within the four-year panels.
- **Fixed heterogeneity (HIP):** a positive, flat floor at σ_β².
- **Persistent growth shocks:** a geometric decay.
- **Common factor with heterogeneous loadings:** removed by the factor adjustment already in the SI.

The SI lag profile already points somewhere. At gaps of 0, 26 and 52 weeks it reads +0.194, +0.150, +0.046 on submissions (decaying) and +0.135, +0.136, +0.108 on comments (flat). Comments looks like HIP; submissions looks like decaying or factor-driven persistence. That matches the draft's own reading (l. 171) and would give it a standard vocabulary.

**A second payoff (addresses G15).** Under heterogeneous growth, a cohort's variance grows as σ²t + σ_β²t², not linearly. The lasting-variance estimator (SI §4.2) and the structure-function β both assume linear growth. Fit the quadratic term across the three spacings and report c_k and β both ways.

**Where.**
- l. 168–171: replace "a spread of 0.4 a year gives +0.17 to +0.19" with the estimate.
- Methods, "Estimators": add one sentence.
- SI §Persistence: add a table.

**Cost and data.** Moderate analysis on existing panels; one SI table. No new data. Write the expectations before running.

### Item 4. Add one calibrated heterogeneous-growth world

**Why.**
- Every world in the battery has homogeneous mean growth, and the draft concedes this (G9).
- GLLM's and Luttmer's fix was to put the heterogeneity into the model and show it speeds transitions.

**Analysis.**
- Draw each community's mean growth from the σ_β (and decay) estimated in item 3.
- Keep the measured σ_k and the drift by rank. Start from the 2019 ladder; 20 seeds.
- Score:
  - (i) the gapped statistic;
  - (ii) the R10 split, that is, whether prior decliners carry 40–60% of the excess slide;
  - (iii) the head's share path in 2019–22 against the homogeneous model (l. 160);
  - (iv) the 30-year slope.

**Expectation to state in advance.**
- (i)–(iii) should move toward the data. This is the out-of-sample element: the parameters come from the autocovariances, and the targets are R10 and the head's fall.
- On (iv), Fernholz's result (arXiv 2016 version of the cited JAE paper) says the stationary distribution depends only on rank-specific volatility and reversion rates "regardless of how growth rates and volatilities vary across different economic agents" (abstract) [verified via abstract page: https://arxiv.org/abs/1602.00159]. So if the rank-based σ_k and s̄_k are held at their measured values, heterogeneity need not close the 8–46% gap. Say this before running.

**Where.** Fig. 5A crosses (one more world), the SI null-world table, and one sentence at l. 174 or l. 186.

**Cost and data.** One world × 2 ladders × 20 seeds. Moderate compute; no new data.

### Item 5. Speak to the cultural-evolution turnover-profile literature

**Why.** Acerbi & Bentley (2014) use the shape of turnover against list size to infer transmission biases. In their manuscript, anti-conformity "produces a 'concave' turnover, where popular traits change faster than what would be expected" (accepted manuscript, p. 7) [verified: https://bura.brunel.ac.uk/bitstream/2438/20332/2/FullText.pdf].
- The draft's Fig. 1C has exactly that shape.
- R9 shows that the ladder's curvature produces it under identical dynamics.

**Where.** End of the first paragraph of "The top turns over fastest…" (l. 114).

**Smallest change (my wording).** "Studies of cultural evolution read such a profile, in which the most popular items are replaced fastest, as evidence of biased copying; with dynamics held fixed, the ladder's shape alone produces it here, so a turnover profile identifies a bias only together with the ladder." Add one reference.

**Cost and data.** About 50 words. No new data.

### Item 6. Add a parameter-provenance table, as the strongest calibration papers do

**Why.** Baker & Smyth and Grilli both make it impossible to miss which quantities were inputs and which were predictions. The draft asserts "none fitted to the statistics we test it on" (l. 155). A referee cannot check that from the text.

**Content.** One SI table with one row per quantity:
- **Inputs:** σ_0, β, the drift profile, κ, swing, noise and exit rates. For each, the source moment and the window.
- **Outputs:** the stationary slope, turnover by k, the boundary ratio, persistence, held-out movement and the head share. For each, pass or fail, with the failure rows kept.
- **Calibrations:** mark each outcome as a calibration (for example the noise scale recalibrated per training window) or a prediction.

**Where.** SI §"The generative model"; one pointer sentence at l. 155.

**Cost and data.** Small. No new data.

### Item 7. Put ladders, cells and worlds in dimensionless coordinates

**Why.** This brings in element (f): a phase-plane figure lets readers place a new platform.

**The coordinates.**
- **x:** S/S* = c/(s̄−e), the distance from balance. Fig. 3B already shows this as the distance from its diagonal.
- **y:** a ratio of time scales, either the yearly turnover of top sets times 1/λ_k from item 2, or the ratio of passing to lasting variance at one year.
- **Points:** the three ladders by cut-off and year, and each world.

This is the format of Iñiguez et al.'s collapse and Blumm et al.'s phases.

**Option.** Replace Fig. 3C, the two ingredients of Eq. 1, with a collapse. Plot observed turnover against E[1−e^{−D⁺/S}] for all ladders and worlds on one diagonal. The ratios now sit in the SI (0.82–1.18).

**Cost and data.** A figure redraw; no new data. It saves space if it replaces a panel.

### Item 8. Read β through Taylor's law

**Why.** The weak size dependence (β ≈ 0.05) has a standard cross-field reading.
- β = 0 is Taylor's exponent 2: fluctuations proportional to size, as under shared or environmental shocks.
- Independent sub-units give β = 1/2, Taylor exponent 1.
- Grilli rules out demographic noise because it "would fail in reproducing Taylor's law with exponent 2, as it would predict and exponent 1" [sic] (Methods, "Ruling out demographic stochasticity") [verified via page fetch: PMC7506021].
- The draft's own item-count result already says attention to a community moves as a unit (PROGRAM_STATE; SI table "activity per item").

**Where.** l. 116, after the de Menezes/Eisler/Sangiorgio sentence. Both of those are already cited and make the same point for web traffic.

**Cost and data.** One sentence. No new data.

---

## 4. If the model works: the most interesting questions it could answer

The ranking combines scientific importance, feasibility with data on hand, and fit to a broad PNAS readership.

### 1. How fast can attention inequality change?

**Analysis.**
- From the measured σ_k and S_k, compute the homogeneous model's relaxation rate by rank (item 2) and the largest two-year change in head share it can produce from any drift change the data allow.
- Compare with the observed fall at the submissions head: share 0.498 → 0.340 in 2020–22 (SI), with the Gomez within term at −0.24 and −0.26 a year.
- Then add the heterogeneous growth from items 3–4.

**What would be interesting.** "Churn alone cannot move an attention hierarchy faster than over a decade; when concentration moves in a few years, it moves through persistent decline of particular communities." This is GLLM's result transposed to attention, with a falling rather than rising concentration. It bears on how fast platform changes or regulation can be expected to show up in concentration.

**What could go wrong.**
- A common shock with heterogeneous loadings (rival c) is also a "fast" mechanism in GLLM's sense. On submissions the factor adjustment removes the persistence.
- The reflecting-barrier formula need not hold for rank-based drift. Baker & Smyth note that without a reaction term the mean-field equation has no spectral gap. So compute the rate numerically in the draft's own model rather than only from the formula.

### 2. Luck or lasting position: who is at the top by chance?

**Analysis.**
- Using the state-space model's filtered lasting level, estimate for each top set the share of members whose lasting level is above the boundary.
- Compare with Henderson et al.-style Markov-chain benchmarks, giving false positives for sustained top membership.

**What would be interesting.** A number such as "a third of a year's top 100 are there because of passing swings". It speaks to trending lists, recommendation and fairness, and it is broadly legible.

**What could go wrong.**
- Weak joint identification of reversion and observation noise (Knape 2008).
- The model's thin tails (kurtosis 2.9 against 22.1) would misstate the chance of extreme passing swings.
- Report the result under at least two noise specifications.

### 3. How long do leaders stay? A survival collapse

**Analysis.**
- Build survival curves of top-k membership, at block level to suppress passing swings, for each k and ladder.
- Rescale time by S_k²/σ_k², the membership time scale from item 2.

**What would be interesting.** A collapse of all curves onto one, analogous to Stanley et al.'s collapse of firm growth distributions, with persistence showing as a systematic deviation in the tail.
- It ties to the physics of lead changes: Krapivsky & Redner found the number of lead changes grows logarithmically with system size.
- The draft's finding that rank one almost never changes on Reddit while its share halves is the striking special case.

**What could go wrong.**
- Four years gives heavy right-censoring at the head, and few events at k ≤ 10.
- Bans truncate tenures.

### 4. Mobility of attention, measured as for income

**Analysis.**
- Build transition matrices between rank strata over one to three years.
- Report Shorrocks-type indices and rank–rank slopes by horizon, for Reddit, Facebook and the worlds.

**What would be interesting.** A familiar scale for readers, for example how attention mobility compares with year-to-year earnings mobility. Also a decomposition of mobility into passing, lasting and persistent components, using the model.

**What could go wrong.**
- Mobility depends on block length and on how the universe is defined.
- Comparisons with income are only illustrative.
- Cowell & Flachaire (2025) show common indices violate basic principles, so choose the measure carefully.

### 5. Platform interventions as shocks to σ or drift

**Analysis.**
- Represent the June 2020 ban wave, or the June 2021 step at the submissions head, as a change in drift at the affected ranks.
- Use the rate law to predict the delay with which concentration responds.

**What would be interesting.** The claim at l. 188 that effects "unfold long after the change" would become a number of years. It could be registered for the sealed 2023–24 data, which include the June 2023 blackout.

**What could go wrong.**
- Identification against other concurrent changes (rival c).
- One or two events.
- Post hoc choice of dates.

### 6. A dimensionless comparison of platforms

**Analysis.** Item 7's coordinates, plus the persistence share σ_β²/σ_L².

**What would be interesting.** It would show whether Facebook pages differ from Reddit communities in distance from balance, in time scales, or only in persistence. Facebook has none, and its rank one changes in 52% of weeks.

**What could go wrong.** Two platforms in one window, and Facebook's 86 weeks.

### 7. Detecting entrenchment

**Analysis.** Define entrenchment by its dynamical signature:
- drift at the head above the drift below it (the opposite of a slide);
- a lasting β approaching the firm range;
- heterogeneous growth concentrated among incumbents.

**What would be interesting.** A test that separates incumbency by gap width (rank one stable, share falling) from incumbency by growth advantage. The draft already shows the first and not the second.

**What could go wrong.**
- β is weakly identified.
- Drift by rank is measured on two years only.

### 8. First passage of newcomers to the top

**Assessment.** Low feasibility. No simulated entrant reaches a top set within the window (SI), and entries into the top sets are rare in four years. Leave it for the bridge data.

---

## 5. Threats from this literature, and how to pre-empt them

### Threat 1. The balance and the counterfactuals are theorems

**The literature.**
- S* = σ²/(2g) is Fernholz & Fernholz's eq. 2.17 for Atlas models (p. 7) [verified]. It is also Banner et al.'s and Pal & Pitman's result, both cited.
- Fernholz & Fernholz state the stronger form: an Atlas model's stationary distribution "will follow Zipf's law if and only if two natural conditions, conservation and completeness, are satisfied" (abstract, p. 1) [verified: https://arxiv.org/pdf/1707.04285].
- Removing volatility or drift from a diffusion trivially collapses or spreads it.

**The risk.** A referee from stochastic portfolio theory will read Fig. 4A–C as an illustration of a known result. The draft half-concedes this ("That much is automatic", l. 186). But the significance statement and abstract still present "random growth held in check" as the answer.

**Pre-empt.**
- Cite the equation by number at l. 137.
- Rephrase the abstract's second-to-fourth sentences so the claim is measurement: the first rank-by-rank estimates of each term in attention data with heavy passing noise, and the time scales they imply.
- Note that the ladders are "quasi-Zipfian" in Fernholz & Fernholz's sense. Their p. 2 describes log–log curves that are concave, with a tangent of slope −1 somewhere along them. The ladders fit that description: S ≈ 0.7–1.0 at the head, rising below.

### Threat 2. "Outlasts its members" is generic

**The literature.** GLLM eq. 13 makes relaxation scale as S²/σ², and membership turns over on the same scale (item 2). A referee who knows GLLM can say the hierarchy outlasts its members in every random-growth model near balance.

**Pre-empt.**
- State the generic ratio yourself.
- Attribute the comments/submissions-head difference to distance from balance.
- Keep the title's claim on comments, where the time scales are measured. The draft already does this at l. 147.

### Threat 3. Baker & Smyth reach the opposite conclusion with a higher standard of evidence

**The literature.** They predict the equity curve "with no parameter fitted to the curve itself" and test the predicted time scales directly. They conclude "Turnover, not drift, stabilizes the calibrated market." (abstract, p. 1) [verified: https://arxiv.org/pdf/2608.27156].

**The risk.** A referee will ask why attention ladders should be held by drift when equity markets are held by turnover. They will also ask why the draft's model, run from measured inputs, misses the observed slope by 8–46% when theirs tracks every decade.

**Pre-empt.** Item 1, plus three statements:
1. **The universes are open top-K sets.** Fernholz & Fernholz warn that a complete system "cut off after the top 100 stocks" is no longer quasi-Zipfian (p. 2) [verified]. This is why c_K matters.
2. **The draft's ladder is measured as out of balance (S/S* = 0.64), so a mismatch at 30 years is expected rather than a failure of the model.** Fig. 4B shows the model reaching its own balance.
3. **Drift identification.** Baker & Smyth say that time series cannot usefully identify drift, so the 30-year runs inherit the uncertainty of two years of slides (G6).

Also cite the SPT calibration literature that targets the same statistics:
- Itkin & Larsson calibrate to rank-based volatilities and to turnover "as measured by market weight collisions", the analogue of the draft's C(R).
- Campbell, Song & Wong (2025) document the macroscopic stylized facts these calibrations target.
- Itkin, Larsson & Zhang's work in progress on listings and delistings (Oxford seminar, Feb 2025; secondary) may appear before the paper does.

### Threat 4. Persistence is a known test with a known rival reading

**The literature.**
- Labour economists have used higher-order autocovariances of growth to test heterogeneous growth since MaCurdy (1982) and Abowd & Card (1989). Hryshko (p. 181) records that both rejected growth-rate heterogeneity in the PSID [verified]. Baker (1997) and Guvenen (2009) argued that this test lacks power. Hryshko's own minimum-distance estimate of the variance of deterministic growth is zero (abstract, p. 177) [verified].
- In finance, cross-sectional momentum (Jegadeesh & Titman 1993) is the same phenomenon. In firms, serially correlated growth is well documented (Coad 2007, cited).

**The risk.** A referee may say persistence is neither new nor surprising, and that the gapped statistic cannot tell fixed types from slowly decaying shocks.

**Pre-empt.**
- Item 3: estimate σ_β with an interval and test flat against decaying lag profiles.
- Present rival (b), fixed heterogeneity in growth, as one reading of persistence rather than a rival to it.
- Claim the novelty only for attention, and for the result that persistence carries the excess slide (R10). That result is post hoc, so say what would confirm it on the bridge data.

### Threat 5. Turnover profiles are already used to diagnose mechanism

**The literature.** In cultural evolution a profile where the top changes fastest means anti-conformity (Acerbi & Bentley).

**The risk.** A referee from that field may read Fig. 1C as evidence of novelty-seeking audiences.

**Pre-empt.** Item 5. The draft's R9 is the counter-evidence.

### Threat 6. Binning and time-averaging artefacts

**The literature.**
- The draft handles Working's (1960) averaging bias.
- Parallel warnings exist elsewhere:
  - Karjus et al. (2020) found the language "frequency increment test" sensitive to temporal binning [secondary].
  - Premo (2014) found that most neutrality tests fail on time-averaged assemblages [abstract only].
  - Kalyuzhny et al. (2014) found Taylor exponents depend on series length [abstract only].

**Pre-empt.** One sentence in the SI citing these alongside Working, so the 13-week-window and gap design reads as a known remedy.

### Threat 7. The flux of new elements

**The literature.** Iñiguez et al. (cited) report that "for high flux only the top of the list is stable, otherwise top and bottom are equally stable" (abstract) [verified via page fetch: https://arxiv.org/abs/2104.13439].

**The risk.** The absence-penalized universe definition sets the flux. A referee may ask whether top-set turnover depends on that choice.

**Pre-empt.** The SI shows the boundary ratio does not depend on K (§2z-da). Add one sentence connecting it to the flux result.

### Threat 8. The lead author's own earlier treatment of web traffic

**The source.** The 2009 Oxford talk description says Hindman used "new models adapted from financial mathematics" on daily traffic and found the web audience distribution "actually extremely stable" [verified via page fetch: https://podcasts.ox.ac.uk/more-things-change-more-they-stay-same-online-audiences-and-paradox-web-traffic].

A review of *The Internet Trap* reports (Guthro 2019, *J. Intellectual Freedom & Privacy* 4(2), p. 10) [verified: https://journals.ala.org/index.php/jifp/article/download/6992/9815; secondary account of the book]:
- that the book found "traffic for large sites is more stable than for small sites";
- that sites below the top 25 churn more.

**The risk.** A single-blind referee may see a tension with "the top turns over fastest" and "size gives limited protection".

**Pre-empt.**
- Cite the book's chapter on the dynamics of web traffic and reconcile the two in one sentence.
- The draft's own Fig. 2C (rank one rarely changes) fits "the top ten are stable" as a statement about rank order.
- Set turnover is a different statistic, and websites are not communities.
- The authors know what model the book used; whether it was SPT should be said.

### Threat 9. Platform-wide shocks are a modelled class in SPT

**The literature.** Rank-based models with common noise exist: Kolli & Sarantsev 2019; Shkolnikov & Yeung 2026 [secondary].

**Pre-empt.** On submissions, the persistence is a heterogeneous loading on the platform total. Naming that model class turns rival (c) into a specified alternative rather than a vague worry.

### Precision points in the current text

**l. 93.** "Random-growth theory explains … a drift that holds back the larger ones keeps the distribution from spreading [gibrat1931, champernowne1953, gabaix1999…]."
- Gibrat's own process has no stationary distribution.
- In Gabaix (1999) the stabilizer is a lower reflecting barrier, not a drift that acts on the large.
- The "drift holding back the large" phrasing belongs to the rank-based tradition: Fernholz and Banner et al.
- Smallest fix: split the citation list by mechanism.

**l. 118.** "Ranking studies find the top stable by how often the occupant of a rank changes [iniguez2022]." This is correct for high-flux lists. Iñiguez et al.'s main point, that flux sets which part is stable, is more useful to the draft (threat 7).

**Citations engaged only in passing.** bakersmyth2026 (l. 93), gllm2016 (l. 93, 171, 186), fernholz2017 (l. 93, 137), iniguez2022 and blumm2012 (l. 93, 118), guvenen2009 (l. 171) and ichiba2011 (l. 93, in a list). Items 1–4 and threats 1–4 above say how to use each.

---

## 6. Annotated reading list

**Organization.** The Tier 1 entries are the works whose absence or thin engagement a referee would notice, or that change a claim. Tiers 2–3 supply methods and analogies by field. Already-cited works appear only where the draft uses them in passing.

### Tier 1: must engage (9)

**T1-1. Baker, G. & Smyth, C. (2026). Traveling waves in equity markets with rank-based entry and exit. arXiv:2608.27156.** https://arxiv.org/abs/2608.27156 — **[full text]** (sections 1, 1.1, 5.1–5.3 and references read). *Cited; engaged only in passing (l. 93).*
- **Model.** Geometric Brownian firms that enter and exit at rank-dependent rates. In the many-firm limit this gives a reaction–diffusion equation.
- **Calibration.** Fitted on CRSP. The reaction term is bistable, and the long-run capital distribution is a traveling wave. Volatility is measured by rank; birth and death intensities come from delisting codes; drift is recovered from a cross-sectional identity.
- **Results.** The curve is predicted, not fitted, in every decade. The relaxation time scales (about six months at the bottom, about 53 years at the top) are checked against directly measured persistence of shape deviations.
- **Relevance.** Same puzzle, a higher evidentiary standard, and an apparently opposite conclusion on what holds the shape. It also argues that drift is poorly identified from time series.
- **Relation to the draft.** Precedes and partly challenges it.
- **Action.** Items 1 and 2; threat 3.

**T1-2. Gabaix, X., Lasry, J.-M., Lions, P.-L. & Moll, B. (2016). The dynamics of inequality. *Econometrica* 84, 2071–2111.** https://doi.org/10.3982/ECTA13569 — **[full text, NBER WP 21363 version]**. *Cited; engaged in one sentence (l. 186).*
- **Results.** An analytic convergence rate for random growth (eq. 13: λ = σ²/(8η²), with a reflecting barrier). Calibrated, it is too slow for the observed rise in top inequality. Heterogeneous mean growth or departures from Gibrat restore fast transitions.
- **Relevance.** This is the template for the paper's strongest arc: the slow homogeneous model, persistence as the fix, and the head's fast fall.
- **Action.** Item 2 and Q4 §1. Use the formula to anchor "outlasts its members" and to compute how fast churn alone could move concentration.
- **Version note.** The "order of magnitude" wording is in the working paper; by a secondary account the published abstract says only "too slow". Quote the published version or label the WP.

**T1-3. Fernholz, R. T. & Fernholz, R. (2020). Zipf's law for Atlas models. *Journal of Applied Probability* 57(4), 1276–1297.** https://doi.org/10.1017/jpr.2020.64 ; arXiv:1707.04285 — **[full text, arXiv v7: abstract, §1, §2]**. *Not cited.*
- **Results.**
  - An Atlas model's slope parameter is s_k = σ²/(2g) (eq. 2.17), which is the draft's S*.
  - Exact Zipf holds if and only if the system satisfies "conservation" and "completeness".
  - Truncating a complete system to its top 100 breaks quasi-Zipf behaviour.
  - Real systems are usually "quasi-Zipfian": concave log–log curves with slope −1 somewhere.
- **Relevance.** It gives the exact equation behind the draft's Eq. 3 at stationarity and the right language for open top-K universes.
- **Relation to the draft.** Precedes Eq. 3 and supports the c_K correction.
- **Action.** Cite at l. 137 by equation number; threat 1.

**T1-4. Fernholz, R. T. (2017). Nonparametric methods and local-time-based estimation for dynamic power law distributions. *Journal of Applied Econometrics* 32, 1244–1260.** Working-paper version arXiv:1602.00159 — **[abstract only]**. *Cited (l. 93, 137) as an estimator source.*
- **Result.** In random growth the stationary distribution depends only on rank-specific volatilities and reversion rates, whatever the heterogeneity across units.
- **Relevance.**
  - Heterogeneity (persistence) affects the stationary shape only through what it does to σ_k and s̄_k. Its distinctive role is in dynamics and speed.
  - This sharpens what persistence can and cannot explain, for example the 8–46% gap.
  - The local-time estimator is also a natural second estimator of the crossing term.
- **Action.** Item 4 expectation (iv); threat 1. The exact sentence should be rechecked in the published JAE version.

**T1-5. Fernholz, R. T. & Koch, C. (2016). Why are big banks getting bigger? Federal Reserve Bank of Dallas WP 1604.** https://www.dallasfed.org/research/papers/2016/wp1604 — **[abstract only]**. *Not cited.*
- **Method.** Rank-based nonparametric methods split the change in US banking concentration into changes in reversion rates and in idiosyncratic volatilities.
- **Results.**
  - Holding-company concentration rose because of lower mean reversion: "the greater concentration of U.S. bank-holding company assets is a result of lower mean reversion" (abstract) [verified via page fetch].
  - Commercial bank and thrift concentration rose through higher volatility.
- **Relevance.** This is the closest published template for asking whether concentration moved through volatility or through the slide. The draft answers "the slide (members' decline)" through Gomez's split.
- **Relation to the draft.** Precedes the decomposition.
- **Action.** One citation at l. 93 or l. 182. Its framing could organize the decomposition of the head's fall. Check the later published version: secondary sources disagree on its volatility findings.

**T1-6. Hryshko, D. (2012). Labor income profiles are not heterogeneous: Evidence from income growth rates. *Quantitative Economics* 3, 177–209** — https://doi.org/10.3982/QE112 — **[full text]**. **With Baker, M. (1997). Growth-rate heterogeneity and the covariance structure of life-cycle earnings. *Journal of Labor Economics* 15, 338–375** — https://doi.org/10.1086/209836 — **[abstract only]**. *Guvenen 2009 is cited; these are not.*
- **The debate.** Fixed differences in income growth (HIP) against random-walk income (RIP).
- **Identification.**
  - Long-lag autocovariances of growth carry the heterogeneity, and testing them alone lacks power.
  - Minimum distance on the whole function is the standard estimator.
  - Hryshko finds the growth-heterogeneity variance is zero once a random walk is allowed; Baker finds sizeable heterogeneity.
- **Relevance.** The draft's gapped statistic and rival (b) are this debate in another domain.
- **Relation to the draft.** Precedes and qualifies the persistence claim.
- **Action.** Item 3; threat 4.

**T1-7. Luttmer, E. G. J. (2011). On the mechanics of firm growth. *Review of Economic Studies* 78(3), 1042–1068.** https://doi.org/10.1093/restud/rdq028 — **[abstract only]**. *Not cited.*
- **Argument.** If firms grow by many independent shocks, Gibrat-type growth takes "hundreds of years" to produce the observed large firms [verified via abstract page]. Firms with better ideas that grow fast and then revert fix the time scale.
- **Relevance.** The firm-side twin of GLLM, and the closest analogue to persistent heterogeneous decline as the fast channel.
- **Relation to the draft.** Supports the GLLM arc.
- **Action.** One clause in the Discussion (l. 186) beside GLLM.

**T1-8. Acerbi, A. & Bentley, R. A. (2014). Biases in cultural transmission shape the turnover of popular traits. *Evolution and Human Behavior* 35(3), 228–236.** https://doi.org/10.1016/j.evolhumbehav.2014.02.003 — **[full text, accepted manuscript]**. *Not cited; Bentley et al. 2007 is cited.*
- **Method.** Defines the "turnover profile", turnover against list size. Biases against popular traits make the profile concave, so the top changes fastest; biases for them make it convex.
- **Data.** Colour terms, baby names and Last.fm.
- **Relevance.** The draft's Fig. 1C is a concave profile, and its R9 shows geometry can produce one.
- **Relation to the draft.** The draft qualifies this literature.
- **Action.** Item 5; threat 5.

**T1-9. Hindman, M. (2018). *The Internet Trap*. Princeton University Press, chapter on the dynamics of web traffic.** ISBN 978-0-691-15926-3 — **[secondary]**: Guthro (2019) review and the 2009 Oxford talk description, both retrieved. *Not cited.*
- **Content, by the secondary accounts.** Daily Hitwise data for the top 300 sites, 2005–2008, modelled with a stochastic-dynamics model "adapted from financial mathematics". It finds a very stable audience distribution, large sites more stable, and churn below the top 25.
- **Relevance.** It is the closest predecessor on attention specifically. A referee will notice the apparent tension with the draft's findings.
- **Relation to the draft.** Precedes it and may appear to challenge it.
- **Action.** Threat 8: cite and reconcile in one sentence.

### Tier 2: should cite

- **Itkin, D. & Larsson, M. (2024). Open markets and hybrid Jacobi processes.** *Annals of Applied Probability* 34(3), 2940–2985, https://doi.org/10.1214/23-AAP2030 — **[secondary]**, via Baker & Smyth's reference list. **With Karatzas, I. & Kim, D. (2021). Open markets.** *Mathematical Finance* 31(4), 1111–1161, https://doi.org/10.1111/mafi.12294 — **[abstract only]**.
  - SPT's treatment of a top-N universe whose membership changes.
  - Cite once where the re-selected universe and c_K are introduced (l. 147; SI §5).

- **Itkin, D. & Larsson, M. (2024/2026). Calibrated rank volatility stabilized models for large equity markets.** arXiv:2403.04674, https://arxiv.org/abs/2403.04674 — **[abstract only]**.
  - Calibrated to rank volatilities, collision-based turnover and the capital distribution curve.
  - Shows SPT already uses rank-switching rates like the draft's C(R) as targets.
  - Cite at l. 118 or in the SI rank-change section.

- **Campbell, S., Song, Q. & Wong, T.-K. L. (2025). Macroscopic properties of equity markets: stylized facts and portfolio performance.** *Quantitative Finance* 25(9), https://doi.org/10.1080/14697688.2025.2541859 ; arXiv:2409.10859 — **[abstract only]**.
  - The equity "stylized facts" paper, including a discrete-time construction of rank-switching local times (per Baker & Smyth).
  - A model for a concise stylized-facts panel.

- **Ichiba, T., Pal, S. & Shkolnikov, M. (2013). Convergence rates for rank-based models with applications to portfolio theory.** *Probability Theory and Related Fields* 156, 415–448, https://doi.org/10.1007/s00440-012-0432-5 — **[abstract only]**.
  - Convergence rates for rank-based diffusions.
  - Cite with item 2 when stating the time scales.

- **Malevergne, Y., Saichev, A. & Sornette, D. (2013). Zipf's law and maximum sustainable growth.** *Journal of Economic Dynamics and Control* 37, 1195–1212, https://doi.org/10.1016/j.jedc.2013.02.004 — **[abstract]**.
  - Zipf holds "under a balance condition between the effective growth rate of incumbent firms" (including their demise) and the growth of investment in entrants (abstract) [verified via page fetch: https://ar5iv.labs.arxiv.org/html/1012.0199].
  - The entry-inclusive version of the balance; relevant to e_k.

- **Maillart, T., Sornette, D., Spaeth, S. & von Krogh, G. (2008). Empirical tests of Zipf's law mechanism in open source Linux distribution.** *Physical Review Letters* 101, 218701, https://doi.org/10.1103/PhysRevLett.101.218701 — **[abstract]**.
  - Tests each ingredient of the growth model separately in data: proportional growth, diffusive scaling, entrants.
  - A precedent for "measure each ingredient", and one of few such tests on a digital system.

- **Grilli, J. (2020). Macroecological laws describe variation and diversity in microbial communities.** *Nature Communications* 11, 4743, https://doi.org/10.1038/s41467-020-18529-y — **[full text, sections fetched]**.
  - A minimal stochastic logistic model parameterized from a few moments predicts several macroecological patterns. The Discussion lists what it does not predict.
  - Anatomy exemplar (§2(d)); Taylor-law reading of β (item 8).

- **Gleeson, J. P., Cellai, D., Onnela, J.-P., Porter, M. A. & Reed-Tsochas, F. (2014). A simple generative model of collective online behavior.** *PNAS* 111, 10411–10415, https://doi.org/10.1073/pnas.1313895111 — **[full text, ar5iv sections]**.
  - Facebook app adoption, 2007. Many mechanisms match long-run behaviour; only temporal dynamics choose among them.
  - A PNAS exemplar on an online platform for the draft's temporal-statistics strategy. Cite at l. 168.

- **Mitzenmacher, M. (2005). Editorial: The future of power law research.** *Internet Mathematics* 2(4), 525–534, https://doi.org/10.1080/15427951.2005.10129112 — **[full text]**.
  - Argues that "power law research must move from focusing on observation, interpretation, and modeling of power law behavior to instead considering the challenging problems of validation of models and control of systems" (p. 525) [verified: https://www.eecs.harvard.edu/~michaelm/postscripts/im2006a.pdf].
  - Frames why the null-world battery matters. One citation in the Introduction or Discussion.

- **Knape, J. (2008). Estimability of density dependence in models of time series data.** *Ecology* 89(11), 2994–3000 — **[abstract]**. **With Dennis, B. et al. (2006). Estimating density dependence, process noise, and observation error.** *Ecological Monographs* 76(3), 323–341 — **[abstract/secondary]**.
  - Reversion, process noise and observation noise are jointly weakly identified in state-space models; independent estimates of observation variance fix it.
  - Explains the gate's blindness to the lasting process (l. 160) and supports recalibrating noise per window. SI citation.

- **Henderson, A. D., Raynor, M. E. & Ahmed, M. (2012). How long must a firm be great to rule out chance?** *Strategic Management Journal* 33(4), 387–406, https://doi.org/10.1002/smj.1943 — **[abstract]**. **With Denrell, J. & Liu, C. (2012). Top performers are not the most impressive when extreme performance indicates unreliability.** *PNAS* 109, 9331–9336, https://doi.org/10.1073/pnas.1116048109 — **[abstract/secondary]**.
  - Markov-chain nulls for sustained top performance, and why extremes mislead.
  - Q4 §2; also supports the permanent-rank design (rival d).

- **Blanchet, T. (2022). Uncovering the dynamics of the wealth distribution.** arXiv:2211.15509 — **[abstract]**.
  - A continuous-time decomposition of distributional change estimated from repeated cross-sections.
  - A methodological cousin of Eq. 3. SI citation.

- **Beare, B. K. & Toda, A. A. (2022). Determination of Pareto exponents in economic models driven by Markov multiplicative processes.** *Econometrica* 90(4), 1811–1833, https://doi.org/10.3982/ECTA17984 — **[abstract]**.
  - With Markov-switching growth types, the tail exponent and the types at the top follow from a spectral condition.
  - The formal version of how persistent types shape steepness (item 4).

- **Jones, C. I. & Kim, J. (2018). A Schumpeterian model of top income inequality.** *Journal of Political Economy* 126(5), 1785–1826, https://doi.org/10.1086/699190 — **[abstract]**.
  - Incumbent growth against displacement by entrants sets the Pareto tail; policy changes are counterfactuals.
  - Shows how a balance becomes a substantive counterfactual (§1).

### Tier 3: useful background

- **Simon, H. A. (1955). On a class of skew distribution functions.** *Biometrika* 42, 425–440, https://doi.org/10.1093/biomet/42.3-4.425 — **[secondary]**. The origin of entry-plus-proportional growth. If cumulative advantage is invoked (l. 93), this is the model-side citation.
- **Ijiri, Y. & Simon, H. A. (1974). Interpretations of departures from the Pareto curve firm-size distributions.** *Journal of Political Economy* 82(2), 315–331 — **[memory, unverified: bibliographic record confirmed, content not]**. I recall that it links concavity of the log–log curve to serially correlated growth. The authors should check this before citing it for the persistence-to-shape link.
- **Sutton, J. (1997). Gibrat's legacy.** *Journal of Economic Literature* 35(1), 40–59 — **[secondary]**. The standard review of Gibrat in industrial organization.
- **Bouchaud, J.-P. & Mézard, M. (2000). Wealth condensation in a simple model of economy.** *Physica A* 282, 536–545, https://doi.org/10.1016/S0378-4371(00)00205-3 — **[secondary]**. The physics version of the balance: multiplicative noise against exchange sets the Pareto exponent.
- **Keitt, T. H. & Stanley, H. E. (1998). Dynamics of North American breeding bird populations.** *Nature* 393, 257–260, https://doi.org/10.1038/30478 — **[abstract]**. Firm-growth methods applied to birds: "The variance of the population over a time series increases as a power-law with increasing time lag" [verified via abstract page: IDEAS]. A cross-system precedent for variance-growth diagnostics (item 3).
- **Kalyuzhny, M., Kadmon, R. & Shnerb, N. M. (2015).** *Ecology Letters* 18, 572–580 — **[abstract/secondary]**. **Kalyuzhny et al. (2014). Temporal fluctuation scaling in populations and communities.** *Ecology* 95(6), 1701–1709, https://doi.org/10.1890/13-0326.1 — **[abstract]**. Neutral theory needs environmental noise for dynamics; Taylor exponents are fragile in short series.
- **Chisholm, R. A. et al. (2014). Temporal variability of forest communities.** *Ecology Letters* 17, 855–865, https://doi.org/10.1111/ele.12296 — **[secondary]**. More than 4,000 tree species; temporal environmental variance dominates.
- **Mallmin, E., Traulsen, A. & De Monte, S. (2024). Chaotic turnover of rare and abundant species in a strongly interacting model community.** *PNAS* 121(11), e2312822121, https://doi.org/10.1073/pnas.2312822121 — **[full text, sections fetched]**. A conserved abundance distribution with continual overturning of dominant species; a PNAS analogue of the draft's paradox from ecology.
- **Fisher, C. K. & Mehta, P. (2014).** *PNAS* 111(36), 13111–13116, https://doi.org/10.1073/pnas.1405637111 — **[abstract]**. **Hu, J. et al. (2022).** *Science* 378, 85–89, https://doi.org/10.1126/science.abm7841 — **[abstract]**. Phase-diagram exemplars (item 7).
- **Newberry, M. G. et al. (2017). Detecting evolutionary forces in language change.** *Nature* 551, 223–226, https://doi.org/10.1038/nature24455 — **[secondary]**. **With Karjus, A. et al. (2020), *Glossa*, on binning sensitivity [secondary]**. Drift against selection from time series (threat 6).
- **Premo, L. S. (2014). Cultural transmission and diversity in time-averaged assemblages.** *Current Anthropology* 55(1), 105–114 — **[abstract]**. Time-averaging defeats neutrality tests (threat 6).
- **Evans, T. S. & Giometto, A. (2011). Turnover rate of popularity charts in neutral models.** arXiv:1105.4044 — **[abstract]**. The neutral turnover formula refined.
- **Krapivsky, P. L. & Redner, S. (2002). Statistics of changes in lead node in connectivity-growing networks.** *Physical Review Letters* 89, 258703; arXiv:cond-mat/0207370 — **[abstract]**. Lead changes grow logarithmically with system size (Q4 §3).
- **Sinatra, R. et al. (2016).** *Science* 354, aaf5239, https://doi.org/10.1126/science.aaf5239 — **[secondary]**. **With Wang, J., Mei, Y. & Hicks, D. (2014). Comment on "Quantifying long-term scientific impact".** *Science* 345, 149, https://doi.org/10.1126/science.1248770 — **[secondary]**. Luck against a lasting parameter; a forecasting failure in a sister model.
- **Jegadeesh, N. & Titman, S. (1993). Returns to buying winners and selling losers.** *Journal of Finance* 48, 65–91, https://doi.org/10.1111/j.1540-6261.1993.tb04702.x — **[abstract]**. Momentum, the finance name for persistence (threat 4).
- **Kolli, P. & Sarantsev, A. (2019). Large rank-based models with common noise.** *Statistics & Probability Letters* 151, 29–35 — **[abstract]**. Common shocks in rank-based models (threat 9).
- **Shorrocks, A. F. (1978). The measurement of mobility.** *Econometrica* 46, 1013–1024 — **[abstract]**. **With Cowell, F. A. & Flachaire, E. (2025). Mobility and mobility measures.** arXiv:2502.19608 — **[abstract]**. Q4 §4.
- **Avolio, M. L. et al. (2019).** *Ecosphere* 10(10), e02881 — **[secondary]**. Rank-abundance-curve change split into reordering and shape: the ecological version of separating member turnover from shape change.
- **Dodds, P. S. et al. (2023). Allotaxonometry and rank-turbulence divergence.** *EPJ Data Science* 12, 37, https://doi.org/10.1140/epjds/s13688-023-00400-x — **[abstract]**. A general measure for comparing ranked lists over time.
- **Vranić, A., Smiljanić, J. & Mitrović Dankulov, M. (2022). Universal growth of social groups.** *J. Stat. Mech.* 123402; arXiv:2206.06732 — **[abstract]**. Reddit and Meetup group sizes with a growth model; nearby Reddit-specific modelling.
- **Ballis, A. & Ioannidis, C. (2026).** arXiv:2610.23283 — **[abstract]**. Mobility across the size distribution with entry and exit, for 8,970 cryptocurrencies. A recent method parallel for Q4 §4; not a competitor.

**Searched for and not found.** No paper applying Atlas/SPT models, or GLLM-type speed analysis, to online communities, pages or channels. The closest found are the lead author's own earlier work (T1-9) and generic online-popularity models. Absence from my searches is not proof of absence.

---

## 7. Checked versus proposed

**Checked in this session.**
- All Tier 1–2 works exist, with the identifiers given.
- Every quotation marked [verified] was read in the retrieved text at the location given.
- The draft's v4 text was read directly: line references are to `paper/pnas_draft.tex` at HEAD, which is identical to 42955ac for `paper/`.
- Numbers I quote from the draft were read from the `.tex` files, not recomputed.

**Proposed for the authors to check.**
1. **My time-scale arithmetic** (item 2): σ² ≈ 0.42 a year, λ ≈ 0.05 a year, 1/λ ≈ 19 years, and the order-8 ratio. It is a sketch from the reflecting-barrier model, and the rank-based model may differ (Baker & Smyth on the absence of a spectral gap without a reaction term). Compute it in the draft's own model.
2. **Whether Baker & Smyth's reaction term and the draft's e_k are comparable** before writing the contrast (item 1). This depends on G7.
3. **What *The Internet Trap* actually modelled and found.** I saw only secondary accounts.
4. **Ijiri & Simon (1974)'s content.** Unverified.
5. **Whether the published JAE version of Fernholz (2017) keeps the abstract sentence** quoted from the arXiv version.
6. **Whether GLLM's published abstract omits "an order of magnitude".** Secondary report.
7. **Fernholz & Koch's final published findings.** Secondary sources disagree on the volatility result.

---

## 8. Search log (2026-10-19)

**Tools.** WebSearch, standard mode except where marked extended; WebFetch on arXiv, ar5iv, PMC, IDEAS/RePEc, Crossref API, publisher and author pages. PDFs that WebFetch saved were read with `pdftotext`.

**Main queries.**
1. Baker & Smyth arXiv 2608.27156. Itkin "listings and delistings". Itkin & Larsson 2403.04674. Campbell, Song & Wong 2409.10859.
2. Rank-based diffusion with online attention, subreddits, Zipf turnover and SPT (extended). Atlas model applied to social media, websites or YouTube. Gibrat's law for subreddits (extended). Stable rank-size with high churn in online communities, 2025 (extended). Online audience concentration with random-growth convergence.
3. Fernholz & Koch, big banks. Fernholz & Fernholz, Zipf's law for Atlas models. Fernholz 1602.08533 and 1602.00159. Fernholz 1998, crossovers.
4. GLLM: NBER, Harvard and Princeton copies (two returned 403; the NBER PDF was used). Luttmer 2011. Malevergne, Saichev & Sornette (Crossref). Maillart et al. 2008. Jones & Kim 2018. Beare & Toda 2022. Blanchet 2022. Bouchaud & Mézard 2000. Ijiri & Simon 1974 (content not found). Sutton 1997. Simon 1955.
5. HIP/RIP: Hryshko 2012 (full text), Baker 1997, Abowd & Card 1989.
6. Ecology: Grilli 2020 (PMC), Mallmin et al. 2024 (PMC), Kalyuzhny 2014 and 2015, Chisholm 2014, Keitt & Stanley 1998, Fisher & Mehta 2014, Hu et al. 2022, Avolio 2019, Dennis et al. 2006, Knape 2008.
7. Cultural evolution and linguistics: Acerbi & Bentley 2014 (full text, Brunel), Evans & Giometto 2011, Bentley 2004 and 2007, Ruck et al. 2017, Newberry 2017, Karjus 2020, Premo 2014, Petersen et al. 2012.
8. Ranking and popularity: Blumm 2012, Iñiguez 2022 (PMC and arXiv), Gleeson 2014 (ar5iv; PMC blocked by a reCAPTCHA, not bypassed), Krapivsky & Redner 2002, Sinatra 2016, Wang, Mei & Hicks 2014, Lorenz-Spreen 2019, Candia 2019, Pecile et al. 2026, Pessa et al. 2026, Lech, Lehmann & Juul 2024, Dodds 2023.
9. Luck and skill: Henderson et al. 2012, Denrell & Liu 2012. Momentum: Jegadeesh & Titman 1993. Mobility: Shorrocks 1978, Cowell & Flachaire 2025, Ballis & Ioannidis 2026.
10. Lead author's prior work: "Internet Trap" "dynamics of web traffic"; the Oxford podcast description; Guthro 2019 review (full text PDF).
11. Mitzenmacher 2005 (full text). SPT background: Ichiba, Pal & Shkolnikov 2013; Karatzas & Kim 2021; Kolli & Sarantsev 2019.

**Not used.** Google Scholar, Web of Science, Elicit and any other external review services.