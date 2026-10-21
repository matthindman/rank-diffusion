_Round-9 iteration-4 review by gpt-6-astra (OpenAI) through the Codex CLI (codex-cli 0.163.0, model_reasoning_effort xhigh), read-only sandbox, run 2026-10-21 on manuscript v9.0 (commit d75b68e), the owner's general frame, with `PROMPT_R9_iter4.md`. Saved verbatim by the orchestrating session (Claude Opus 5.5)._

**1. Verdict: send out, 7/10.**

The Facebook-led general frame is defensible and gives the paper a coherent scientific purpose: explaining how concentrated attention can coexist with changing leadership. Facebook contributes substantial evidence on turnover, hierarchy shape and weak size protection, but the abstract and significance statement currently extend several findings to Instagram that were never measured there. The strongest numerical agreement is a conditional reconstruction using contemporaneous movements, while the crossing relation measures a gain in mean log activity rather than the number of replacements. The longer-horizon Reddit results remain an interesting departure from the benchmark, with appropriately disclosed limitations and no demonstrated counterpart on Facebook. I would argue for external review, but enthusiasm requires a sharper distinction among accounting, conditional reconstruction and prediction, together with a figure that makes Facebook’s contribution immediately visible.

I reviewed d75b68e, the PDFs and sources, figures and plotting code, R17 documentation, relevant records and saved outputs. The main PDF has seven pages; the abstract and significance statement have 234 and 101 words. I made no changes and opened no raw data or panels.

**2. What blocks enthusiasm, ranked**

**1. The paper’s opening promises more cross-platform replication than it contains.**

The problem is concentrated in the [significance statement and abstract](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:80), the [contribution paragraph](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:105), and the [Discussion opening](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:225). They attribute turnover, flatter-head geometry and weak size protection to Facebook, Reddit and Instagram alike.

The actual scope is:

| Claim | Facebook | Reddit | Instagram |
|---|---|---|---|
| Stable hierarchy alongside membership turnover | Supported over 86 weeks in tracked Pages | Supported, with measurable flattening | Not established here |
| Crossing gain agrees with diffusion benchmark | Pooled ratio 0.98 | 1.07 comments; 0.80 submissions, or 1.04 in the simulation-compatible frame | 1.00 under the censoring model, body ranks and 14–28-week displacements |
| Turnover reconstruction | 12 cells | 36 cells | Not evaluated |
| Weak size protection | Interactions; post counts differ | Karma, with estimator qualifications | Not evaluated |
| Controlled geometry experiment | No Facebook-calibrated experiment reported | Reddit-submissions calibration | Not evaluated |
| Long-horizon asymmetry \(D\) | Cannot be measured with this panel | Supported conditionally on the studied population and period | Not evaluated |

Instagram’s own [SI description](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:29) explicitly excludes its head, level claims, persistence statistics and model from that analysis. Facebook’s item-count exponent is **0.187**, versus **0.019** for the reported lasting interaction measure; “little protection” therefore needs an outcome, not just a platform name. See the [size table](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:38).

The general frame survives these distinctions. Generality can reside in the framework and in replicated Facebook–Reddit measurements, with Instagram supplying one narrower extension.

**Score effect:** about +0.3. **Fix now:** text throughout; no new analysis.

**2. “Accounts for turnover” currently combines three different evidential achievements.**

Equation 1 predicts a **crossing gain**, in log-activity units per unit time. It does not predict the fraction of members replaced. Yet the abstract translates its ratios into “risers cross into each top set at the rate … predict.”

The separate turnover formula is useful, but its inputs include the **realized boundary shift, realized movements and realized disappearances from the very interval being reconstructed**. This is explicit in [turnover_kinematics.py, lines 34–49](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/turnover_kinematics.py:34). It is not an exact identity: local log-linearity and the treatment of movements within the boundary band impose restrictions. Nevertheless, its agreement is a conditional kinematic check, not an independent forecast from estimated volatility alone.

My strongest skeptical reading would be: *the crossing result is nearly automatic for diffusions; the replacement formula uses realized movements; and genuinely forward-looking performance is mixed.* The repository supplies that last qualification: earlier-variance crossing ratios are **0.944 and 0.617**, and turnover forecast errors are **0.028 versus 0.037** and **0.022 versus 0.013** for the carry-forward baseline. See [SI crossing results](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:117) and [main forecast results](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:175).

The answer is to explain what each result establishes. The controlled geometry experiment is especially valuable because it changes the ladder while holding simulated dynamics fixed. Give that experiment its proper, calibration-specific scope.

**Score effect:** about +0.3 through greater credibility. **Fix now:** wording and figure labels. Stronger prospective turnover validation would require another analysis.

**3. Facebook is central in the new prose but remains secondary in the visual argument.**

The saved outputs support a stronger presentation. I recomputed the Facebook turnover ratios: **0.8206–1.1030**, median **0.9841**, across 12 cells; the replacement-fraction RMSE is **0.02385**. These are useful quantitative results, currently compressed into one sentence. The main model-comparison table, meanwhile, is entirely Reddit submissions.

Make these two changes:

- Replace Fig. 2A’s detailed time-scale display with **observed versus reconstructed turnover for all 48 cells**, distinguishing Facebook, comments and submissions. Identify Facebook’s 12 cells as **six nested cutoffs across two transitions**, rather than implying 12 independent replications.
- Consider replacing Fig. 1D’s named Reddit losers with the saved **rank-occupant change profiles for Facebook and Reddit**. Preserve the distinction between weekly occupant changes and block-level top-set replacement.

These require only saved outputs and plotting. Move detailed time constants to the SI while retaining one main-text sentence about the five-year response. The Facebook sample should be described on the first page as **public Pages tracked by CrowdTangle**, not as a representative census of Facebook.

**Score effect:** about +0.3. **Fix now:** figures and text. These gains overlap with the preceding items; together I would expect approximately 7.5–8, not their mechanical sum.

**4. The most distinctive departure still lacks an independent period or platform.**

The asymmetry survives several useful checks, but community resampling does not supply independent calendar histories. Six overlapping origins do not become six replications, and two Reddit activity measures share platform events and collection processes.

The weakest relevant saved result deserves visibility: under the draft protocol’s construction, submissions **item-count** asymmetry is **0.034 [−0.026, 0.099]**, although the main six-origin construction gives **0.098 [0.065, 0.129]**. This does not erase the finding; it limits how confidently “participation asymmetry” transfers across constructions. The failure is properly recorded in [SI, R17-B3](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:369).

**Score effect:** modest from clearer presentation now; a convincing independent confirmation could add substantially more. **Fix now:** expose the construction sensitivity. **Needs new evidence:** independent calendar history, preferably eventually on Facebook.

**3. Errors and discrepancies**

These are the corrections I would make before circulation.

- **Crossing gain versus replacement rate.** At [main line 86](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:86), replace “Risers cross into each top set at the rate…” with “The pooled crossing gain in mean log activity broadly agrees with…”. The quoted ratios pool \(K/16\) through \(K\); they do not establish agreement at every head cutoff. Saved submissions ratios at \(K/64\) and \(K/32\) are approximately **1.64 and 1.86** on the full frame. Instagram’s head is explicitly unmeasurable.

- **Instagram scope.** Remove its implied support for turnover profiles, hierarchy stability and size protection from lines 80, 86, 105, 109, 126, 148 and 225. Replace “every platform” with the relevant measured populations.

- **“Facebook’s settings and dates” is inaccurate.** The [comparison code](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/momentum_decomposition.py:175) starts the Reddit cohort at **2020-12-28**. Facebook Era A is defined from **2020-11-02**, and its calculation selects the first 13 weeks of that panel. See [era definitions](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/instrument_eras.py:80). Correct [main line 188](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:188) and [SI line 311](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:311) to “Facebook’s settings over an overlapping period.” Exact date matching needs a new panel calculation, although not a long simulation.

- **Comments’ decline timing mixes estimands.** In the arithmetic-share paragraph, [main line 181](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex:181) says the comments slide peaked in 2019–20. Its arithmetic within terms are **−0.125, −0.150, −0.123**, so the largest is **2020–21**. The largest mean-log shape fall occurred in 2019–20. Either change the year or explicitly switch estimands; the [SI arithmetic table](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:383) is correct.

- **Figure horizons.** Fig. 1C’s axis says “in a year” although Facebook is half-year. Use “over the stated horizon.” Fig. 3A’s caption should explicitly identify Reddit’s **13-week windows/52-week changes** and Facebook’s **4-week windows/26-week changes**.

- **Registration language.** The [SI held-out comparison](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex:208) calls the specification ladder “registered.” [MODEL_STATUS §2z-ap](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md:5388) describes a prospective design whose scripts and run folder were uncommitted when execution began, and an exploratory diagnostic. “Prospectively specified exploratory ladder” is more exact. Its reported gate numbers match the record.

- **Model-class scope.** Change “What no symmetric form produces” at line 194 to “None of the evaluated symmetric models reproduces…”. The following paragraph’s “best evaluated specifications” is appropriately bounded.

The numerical repairs largely hold. All six Table 1 rows match the generated output. R17’s \(D\) summaries, objective comparisons, event decomposition and durability summaries agree with saved results. The revised time-scale paragraph correctly separates the fifth-block response from retained membership and reports the unidentified long-run volatility response. Call \(S^2/\sigma^2\) a **characteristic diffusion time**, rather than an expected crossing time; also, the saved comments estimate **6.5487** rounds directly to **6.5**, not 6.6, at one decimal. These checks concern [R17/§2z-ds](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md:13893), with the later manuscript qualifications retained.

**4. The asymmetry as the discovery**

**\(D\) is now well defined.** It compares tail-specific ratios of subsequent to initial mean rank-relative changes, averaged across origins. It is neither a probability of continuing decline nor a duration of decline. The zero-under-linear-persistence statement is correctly qualified by a common coefficient across strata.

The robustness evidence supports a conditional descriptive finding:

- Excluding terminal collapses and assigning absent weeks the floor leave \(D\) positive.
- The matched homogeneous worlds address regression to the mean under their simulated selection and noise processes.
- Whole-window membership and strata still use future information. Applying the same rule to simulations makes the comparison fairer; it does not make it prospective or representative of all communities.
- Demeaning removes common additive movement, but heterogeneous exposure to shared events can remain.
- Capture checks address abrupt boundaries and terminal collapses, not smoothly changing differential capture.
- The submissions head has no detected asymmetry. The manuscript correctly avoids assigning the head’s decline to persistent decliners below it.

**“About three times as strongly” is now correctly null-relative.** From the unrounded outputs, the excess-persistence ratios are approximately **3.51 for submissions and 2.47 for comments**. Keep the explicit comparison with the matched homogeneous control. Do not shorten this to “declines are three times as persistent,” which would describe a different ratio.

The novelty claim is appropriately strongest as a **quantified, rank-relative asymmetry in online-community activity, tested against matched growth models**. Sign-dependent persistence is established in earnings processes, while downgrade momentum and community rise–stabilization–decline are established precedents; they should remain visible. The current citations serve this purpose. See [Guvenen et al.](https://onlinelibrary.wiley.com/doi/abs/10.3982/ECTA14603), [Lando and Skødeberg](https://www.sciencedirect.com/science/article/pii/S037842660100228X), and the [community life-cycle study](https://doi.org/10.1145/3173574.3173929).

Using saved outputs, add a compact SI display of **\(D\) by origin, activity measure and construction**, including the protocol-style item-count result. Preserve the negative comments item-count origin and the submissions head’s interval across zero. That would be more persuasive than another pooled headline number.

**5. Framing: title, abstract and significance**

The present title can survive if “accounts for” clearly means agreement with a benchmark. The abstract currently gives it a stronger, uniformly replicated interpretation than the evidence warrants.

Two declarative alternatives:

- **Random-growth benchmarks account for turnover in online attention**
- **Rank geometry links concentration and turnover in online attention**

I prefer the first for continuity with the intended paper. The first page should distinguish the empirical contributions: Facebook establishes the short-horizon puzzle and quantitative turnover relation; Reddit supplies longer-horizon departures; Instagram extends one measurement.

Suggested abstract, **222 words**:

> Online attention remains concentrated even as its leaders change. We study weekly interactions of public Facebook Pages tracked by CrowdTangle in 2020–2022 and Reddit community activity in 2019–2022. Facebook’s leading page changes in 52% of weeks, and 18–35% of membership in the evaluated top sets changes within half a year, while the rank–size curve changes little. On Facebook and Reddit, a turnover formula using local slopes, observed movements and boundary shifts gives 0.82–1.18 times observed replacement across 48 cells, including 12 Facebook cells. The crossing gain in mean log activity also broadly agrees with a diffusion benchmark; a censoring-adjusted Instagram calculation extends this check to a third platform. A controlled simulation shows how a flatter head accelerates turnover, and measured size protection is weak for Facebook interactions and Reddit karma. Longer-horizon departures can be measured only on Reddit. There, large declines relative to communities of similar rank persist more strongly than gains: the contrast is 0.22 and 0.13 on two activity measures, against −0.07 and −0.05 in matched homogeneous growth. Evaluated fixed growth differences reproduce much of the persistence but miss the asymmetry. Changes in Reddit’s hierarchy are accounted for by changing declines among its leaders, which stable model parameters do not forecast. Random growth provides a useful benchmark for changing leadership; the evidence for departures depends on the horizon and platform.

Suggested significance statement, **92 words**:

> A concentrated platform need not have entrenched leaders. Among public Facebook Pages tracked by CrowdTangle, the page receiving the most interactions changes in roughly half of all weeks, yet the hierarchy retains much of its shape. Facebook and Reddit connect this turnover to the hierarchy’s slope and the size of movements. Random-growth benchmarks make these connections measurable and expose departures: in Reddit’s longer panels, large declines persist more strongly than gains. The framework separates turnover, concentration and persistent decline, helping researchers assess which features of online attention require explanations beyond random growth.

**6. Instagram**

**A net positive as a narrowly identified measurement extension; a net negative as presently advertised in the opening.**

The censoring apparatus and planted-world recovery make the crossing calculation scientifically useful. They do not establish uncensored Instagram levels, top-account turnover, size protection or long-horizon persistence. The calculation also uses finite-horizon displacement variances, whereas the central Facebook–Reddit interpretation concerns lasting components.

Keep the Results 2 sentence and Methods explanation, with these restrictions explicit. The historical fixed-home movement gate can remain in the SI, identified as a separate analysis. Reconcile that inclusion with the statement that Instagram “enters only the crossing ratio.”

**7. Three paragraphs to rewrite**

Replace the contribution paragraph at line 105 with:

> We use random growth as a quantitative benchmark for changing leadership. Facebook Pages and Reddit communities let us measure how turnover relates to the hierarchy’s slope and the size of movements; a censoring-adjusted Instagram calculation extends the crossing measurement. Reddit’s longer panels reveal departures: changes persist across disjoint periods, and declines persist more strongly than gains. We also separate changes in the hierarchy into gains from crossings and declines among its members. These measurements provide a common framework whose conclusions can be compared across platforms and horizons.

Replace the opening of the turnover paragraph at line 145 with:

> A top set loses members when their movements carry them below its changing boundary. Using each interval’s observed movements, boundary shift and disappearances, a formula for a locally log-linear hierarchy reconstructs replacement at 0.82–1.18 times observed values across 48 cells, including 0.82–1.10 across Facebook’s 12 cells. This is a contemporaneous check of turnover kinematics, derived after the registered linear approximation failed. A separate controlled simulation isolates geometry: with Reddit-calibrated dynamics held fixed, flattening the starting ladder removes most of the excess turnover of small top sets.

Replace the Discussion opening at line 225 with:

> Facebook and Reddit show how concentrated attention can coexist with changing leadership. Their turnover is consistent with a benchmark built from the hierarchy’s geometry and measured movements; Instagram extends the crossing calculation under a model of censoring. The platforms’ observed dynamics also differ across the horizons available: Facebook changes reverse over the shorter comparisons, while Reddit’s longer panels reveal persistence and an asymmetry between declines and gains. Neither that asymmetry nor the observed changes in Reddit’s hierarchy are reproduced jointly by the evaluated models. The framework locates these departures without identifying their causes.

These replacements shorten the existing prose. Move the detailed time-constant and retention discussion to the SI to accommodate the stronger Facebook figure. Preserve the GLLM motivation and the open acknowledgment of the 2018 chapter.

**8. For the owner: further work ranked by value**

1. **A longer, instrument-consistent Facebook history.** This would connect the central dataset’s short-horizon reversal to the paper’s long-horizon questions. The present 86 weeks cannot do that; combining incompatible collection eras would not solve it.

2. **Prospective confirmation on the sealed Reddit extension.** Finalize the protocol before opening it, addressing the source change, blackout and the weak development result for protocol-style submissions counts. This tests reproducibility across time, not platform generality.

3. **A sufficiently long YouTube panel.** This offers the most direct extension from Pages and communities to channels, provided coverage, outcome definitions and horizons are settled prospectively.

4. **Wikipedia pageviews.** This would broaden the instrument from engaged activity toward viewing. Its value is the different measurement and population, not simply adding a platform to the count.

5. **Existing-data follow-ups:** exact calendar matching for the Facebook–Reddit comparison, and asymmetry calculations with prior-period membership and a design addressing shared calendar shocks. These require new panel calculations, not necessarily long simulations, and fall outside this saved-output-only iteration.

6. **Additional long model runs only for stronger model claims.** Full-pipeline calibration and broader profiling would matter if the paper sought to reject an entire heterogeneous-growth family. The general benchmark paper does not need another large grid merely to report that its evaluated models miss the joint pattern.