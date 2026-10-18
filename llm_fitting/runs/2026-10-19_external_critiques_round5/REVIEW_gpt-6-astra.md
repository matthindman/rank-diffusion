_Round-5 review by gpt-6-astra (OpenAI) through the Codex CLI codex-cli 0.163.0 (`codex exec -m gpt-6-astra -s read-only`), 2026-10-19, from `CRITIQUE_PROMPT_R5.md` (committed c203b99 before the review). Written by the CLI; header added by the orchestrating session. Model output: checked in `SYNTHESIS_R5.md`._

My editorial decision is **send out, without enthusiasm**. The paper now presents a credible empirical puzzle, useful measurements, and unusually candid evidence about failed explanations. Named communities and the participation decomposition make the substantive stakes much clearer. Enthusiasm is held back because the opening still claims more causal identification than the counterfactuals establish, while the revised balance argument contains a consequential algebraic inconsistency. The single change most likely to raise the odds is a matched comparison that includes heterogeneous community growth and common shocks, then makes the result that distinguishes those explanations the abstract’s central claim.

I reviewed the manuscript sources, existing seven-page main PDF and nine-page SI, all five figures, relevant scripts, saved outputs, round-4 synthesis, §2z-dj, and the proposed bridge protocol. I recomputed lightweight summaries from saved outputs; I did not access raw or sealed data, run heavy computations, or write files. Fresh compilation was unavailable under the read-only filesystem. References below to “main” and “SI” mean [pnas_draft.tex](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_draft.tex) and [pnas_si.tex](/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas_si.tex); the manuscript remains unchanged from 576a8e4.

**The title is not fully earned.** Its weakest phrase remains **“holds up.”** It equates volatility, boundary crossings, top-set replacement, and demographic turnover too readily. These quantities are related, but the experiments do not intervene on them interchangeably.

The four supporting arguments have different evidential standing:

- **Flat start:** meaningful evidence of sufficiency. The measured dynamics generate a hierarchy without seeding its upper ladder. But they retain empirical size scales, bottom replacement levels, exit hazards, and a closure translating permanent-cohort slides into current-rank drift. This establishes that these dynamics *can generate* a hierarchy, not that they generated the observed one.
- **Thirty-year counterfactuals:** the finite-horizon numbers check. Removing volatility produces head-shape changes of −89.58% on submissions and −61.09% on comments; removing rank drift produces +66.56% and +27.97%. However, all arms retain exit and rebirth, and volatility is clipped outside its estimated size range. “Still steepening after 30 years” is established; “steepens without limit” requires an argument about this particular resetting process.
- **Measured balance:** useful accounting, but the revised identity inconsistently handles entry and exit, as detailed below.
- **Time scales:** the qualitative separation is persuasive. The numerical relaxation times are approximations under fixed slides and a uniform-slope ladder, not directly measured equilibration times.

I would propose **“How a hierarchy of online attention outlasts its members.”** It preserves the owner’s question and permits a mechanism account without asserting that measured turnover is an independently identified sustaining cause. “Durable” can remain in the text if defined through slowly changing mean-log shape, alongside the substantial decline in arithmetic concentration.

The five Results sections need different repairs.

1. **The puzzle and named leaders: sufficient, with a sharper definition of durability.**

   The named results check: r/memes leads in 196/212 weeks and r/AskReddit in 209/213; their shares fall substantially. The 50.2%→34.0% submissions-head result and the approximately three-quarters incidence of incumbent share loss also check.

   The first attack will be: **“You call a hierarchy durable while its head loses one-third of its share.”** Answer this immediately by separating rank-size shape, arithmetic concentration, membership, and rank-one occupancy. These are four different persistence statements.

   The social-science opportunity is stronger than the current presentation: entrenched rank can coexist with substantial redistribution. Also disclose that broad incidence is not diffuse magnitude—the ten largest submissions losers account for 66% of gross incumbent losses. That failed R5 expectation belongs near the named examples, not only in the research record. Main:100 currently supplies the incidence without the concentration of losses.

2. **Why the top turns over fastest, and size: compatible evidence, incomplete causal isolation.**

   The geometry argument is plausible, and the turnover formula works well as a local approximation. Calling it “exact” needs its stated assumptions: locally exponential density and approximately height-independent moves within the relevant range.

   The first attack will be: **“Your comparison changes more than the ladder.”** Constant-slope worlds have homogeneous passing components; calibrated worlds inherit fitted rank-dependent components, exits, and different latent dynamics. The constant-slope firm-like world also has drift and a reflecting floor. Compare `null_worlds.py:35–63` with `null_worlds_r7.py:58–76`. The clean test changes only the initial ladder under identical dynamics.

   Within the calibrated pair, changing β has little effect, which undermines size dependence as the explanation. It does not uniquely establish geometry. Across saved seeds, the paired top-ten-minus-\(K/2\) turnover contrast is 0.068 with a 90% range of −0.038 to 0.195 for size-blind growth, and 0.054 with −0.012 to 0.128 for firm-like growth.

   **“Weaker than firms” remains provisional.** Switching from raw Reddit growth to latent lasting volatility changes the estimand; the cited firm literature measures observed annual-growth dispersion conditional on size. Recovery of the latent estimator does not make those quantities equivalent. [Amaral et al.](https://arxiv.org/abs/cond-mat/9702082) support the annual-growth benchmark, not a directly comparable permanent-innovation exponent.

3. **What turnover returns, and durability: useful measurement, but the lead needs mathematical repair.**

   The first attack will be the paper’s own equations. Main:137 defines
   \[
   S_k^*=\frac{\sigma_k^2}{2(\bar s_k-e_k)}.
   \]
   Consequently,
   \[
   \frac{S_k^*}{S_k}=\frac{c_k}{\bar s_k-e_k},
   \]
   whereas main:147 and SI:64 assert \(c_k/\bar s_k=S_k^*/S_k\).

   This is material, not cosmetic. One saved comments-head cell has \(c=0.0037465\), \(\bar s=0.0036000\), and \(e=-0.0018384\): \(c/\bar s=1.041\), but \(S^*/S=0.689\)—opposite sides of equilibrium. The values are in [the saved balance output](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-14_balance_law/cells/law__reddit_comments_ext.json:128).

   Across the 60 half-year-start cells, I reproduce the quoted ratio of mean crossings to mean slide, 0.724. Including entry/exit in that denominator gives 0.641. Explicitly distinguish the persister balance from the full-universe balance, including which one Fig. 3B depicts.

   The pooled 0.94 boundary ratio is worth reporting as an instrument check, with the composition term and submissions shortfall visible. It cannot carry mechanism identification, as the manuscript now acknowledges.

4. **Where the hierarchy comes from: conditional sufficiency established; empirical necessity unestablished.**

   The first attack will be: **“You simulated the consequences of a closure estimated from the same observed hierarchy.”** This is not fatal to a generative demonstration, but it limits its interpretation.

   The long-run script turns permanent-cohort net slides into current-rank drift and estimates parameters on the whole panel. Its ablations change only `sigma0` or `drift_g`; [the simulator retains exit and rebirth](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/rankdiff_rdm.py:412). Thus “without volatility” is not “without turnover.”

   Fig. 4B also uses volatility and slides measured in the simulated final year to form its horizontal axis (`churn_counterfactual.py:77–89,130–135`). It checks the simulated balance, rather than independently predicting the observed slope. Keep the acknowledged mismatch—observed slopes outside 11 of 12 flat-start bands—prominent.

   The predictive failures are honestly presented. They nonetheless constrain the model’s scientific role: it neither clearly improves short-horizon movement nor predicts the head’s decline. Present it as a conditional explanation of structure, with a separate unresolved empirical residual.

5. **Persistence and the head’s participation share: strongest empirical section, overbroad null claim.**

   The first attack will be **“What random growth lacks.”** A random walk with heterogeneous fixed community drifts can produce positive cross-sectional correlation between disjoint changes despite independent increments within each community:
   \[
   \Delta_1X_i=h\mu_i+\epsilon_{i1},\quad
   \Delta_2X_i=h\mu_i+\epsilon_{i2}
   \quad\Rightarrow\quad
   \operatorname{Cov}_i(\Delta_1X_i,\Delta_2X_i)=h^2\operatorname{Var}_i(\mu_i).
   \]
   SI:153 already reports a planted drift example producing +0.17 to +0.19. “Unlike any random-growth world” is therefore false; “unlike the tested null worlds” is defensible.

   The gapped statistic survives the averaging objection. Its concentration in item counts and declining communities is substantive. However, overlapping origins and common shocks require empirical uncertainty, not just simulation bands. The submissions factor adjustment supports heterogeneous exposure as an explanation; it does not uniquely identify that explanation.

   **R8 is a strong addition.** The exact arithmetic checks: submissions-head item share falls from 0.224 to 0.087 while relative karma per item rises from 2.25 to 3.92. Preserve the distinction between *share of posting* and absolute posting, and retain the explicit admission that persistent decliners have not been linked to the head’s decliners.

What remains potentially sufficient for PNAS is **the combination**, not four independent discoveries.

Persistence is the strongest residual, but it rejects a specified family of homogeneous-growth explanations, not random growth generally. The balance and its time scales offer a quantitative account of coexistence, but their basic theory is established; Gomez already connects turnover, displacement, and top-share dynamics. [Gomez’s paper](https://onlinelibrary.wiley.com/doi/abs/10.3982/ECTA16755) makes the prior-art boundary clear.

The participation split is descriptive rather than causal, but it is undersold: stable leaders lose posting share while becoming relatively more productive in karma per item. That is legible, surprising, and closely tied to the owner’s question. It deserves a main figure.

The weaker-than-firms claim is currently the least secure novelty claim. Beyond the estimand mismatch, the structure-function estimator subtracts the overall cross-sectional mean change and then averages squared changes within bands. Heterogeneous persistent drifts therefore introduce quadratic-in-horizon terms, while the default fit uses a saturating component plus a linear lasting-variance term. See [rankdiff_rdm.py:101](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/rankdiff_rdm.py:101). The paper’s discovered growth heterogeneity can contaminate the quantity subsequently called lasting volatility. Recovery under that heterogeneity is needed.

The short seller’s case is consequently straightforward: **the model explains an established balance, misses the strongest empirical departure from its nulls, and supplies the latent quantity used for the remaining cross-system novelty claim.** That case can be answered, but not by adding more repetitions of the existing null worlds.

The round-4 disposition is:

| Status | Points |
|---|---|
| **Resolved** | Named leaders; headline nulls expanded to 20 seeds; withdrawal of flattening as a discovery; raw-β failure exposed; prior-year Gomez cohorts; noise baseline supplied; persistence→head-decline causal clause withdrawn; exact participation split; distinction between rank occupancy and set replacement; false PASS verdicts corrected. |
| **Half-resolved** | Observed-ladder controls exist, but do not isolate geometry; long-run counterfactuals address the two-year sign objection, but remain conditional; latent β replaces raw β without adequate relevant recovery; persistence excludes each historical confounder separately; several figures were repaired but Fig. 5 remains stale. |
| **Still open** | Heterogeneous-drift/common-exposure rival; comparable firm estimand; empirical uncertainty for persistence; capture audit; valid bridge size test; independent evidence for the causal title. |
| **Needs correction to the prior audit** | F18’s assertion that crossing gain is time-constant by construction is incorrect. The revised equilibrium identity also drops entry/exit inconsistently. |

Additional concrete errors and discrepancies deserve a final mechanical pass:

- **Crossing gain is not constant across years by construction.** Only its variance input is fixed. `mts_balance_law.py:185–186` recomputes slope and \(c=\sigma^2/(2S)\) for every pair. Saved submissions-head \(c\) ranges from 0.002717 to 0.003442, a 27% max/min change. Correct main:147.

- **The relaxation ranges are mischaracterized.** [MODEL_STATUS §2z-cp](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/MODEL_STATUS.md:10128) explicitly calls them order-of-magnitude calculations using uniform slope, linearization, and fixed slides. The 2–6 and 5–22 years span six cutoffs; the head entries are approximately 3–4 years for submissions and nine years for comments. Main:147 labels the former range as the submissions head and drops the assumptions.

- **Recovery “within about 0.03” overstates the evidence.** The cited experiment uses six replications, Facebook calibration, 86/160 weeks, planted β of zero or approximately 0.113, and the earlier `movinghome` simulator. It does not demonstrate accuracy across Reddit-like heterogeneous-drift worlds or planted β=0.2. The 0.029 is a standard deviation, not an error bound; one 160-week estimate misses by +0.050. See [recovery.py:24](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_rdm/recovery.py:24).

- **The membership-robustness sentence is too broad.** Main:171 says changes are below 0.01. SI:172 reports submissions platform-total residuals changing from +0.020 to +0.063 at gap 13 and −0.029 to +0.010 at gap 26. Restrict the sentence to the statistics for which it is true.

- **Fig. 5A still plots old single-run worlds.** [The generator:311](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-18_pnas_push/make_pnas_figures.py:311) reads `null_world__*.json`, not the R7 summaries. Its caption says “four simulated worlds of Fig. 2,” although Fig. 2 now uses five different worlds. Regenerate from the intended means and bands.

- **Fig. 2A visually preserves the withdrawn raw comparison.** It prominently juxtaposes raw β with the firm benchmark despite the surrounding text saying that estimator cannot support the comparison.

- **The SI title remains v2.** SI:10 does not match the main title. SI:129 also describes zero slope through rank 1,560, whereas the saved slope there is 2.146; the near-zero sampled slopes extend through rank 780.

- **One named-head record statement is wrong.** `RESULTS.md:634` says r/memes and r/dankmemes alone account for half the incumbent loss. Their combined loss is 0.1002: approximately 34% of gross incumbent losses or 40% of net incumbent losses. The manuscript does not repeat this error.

- **Registration language needs consistent scope.** The SI correctly discloses that R4–R8 expectations were written before runs but committed with results. They are documented exploratory expectations, not independently timestamped preregistration. SI:20 and :187 nevertheless call the unadopted bridge tests “registered.” Their proposed status should remain explicit.

- **One citation sentence needs precision.** Main:116’s web-traffic statement should specify *relative* fluctuations. The cited de Menezes–Barabási result has absolute traffic standard deviation approximately proportional to mean traffic; weak size dependence concerns normalized dispersion. [Primary paper](https://www.barabasi.com/media/pub_imports/files/144.pdf).

The existing PDFs render without an obvious major layout failure. Fig. 4 is unnecessarily small at 70% text width, and several SI tables are difficult to read at normal size. The added literature improves attribution; the remaining citation problem is mainly the inference drawn from those sources.

The five most valuable changes before submission, using data already available, are:

1. **Repair and narrow the central answer.** Separate persister and full-universe balances; include entry/exit consistently; qualify relaxation times; replace “without limit” with the observed simulation horizon; distinguish diffusion removal from turnover removal. These are necessary corrections, not optional robustness.

2. **Run one controlled rival comparison.** Hold passing components, exits, volatility implementation, and random numbers fixed while varying ladder geometry; include heterogeneous community drift and aggregate exposures. Score persistence, turnover, the balance statistic, and the size estimator through the same pipeline. This would replace the current collection of partially matched comparisons with a discriminating experiment.

3. **Validate the lasting-size estimator under the paper’s own empirical alternative.** Plant drift heterogeneity and common-factor loadings, quantify bias and uncertainty, and report whether weak innovation-size dependence remains identifiable. Until then, describe β as model dependent and remove an unqualified firm comparison from the abstract and Fig. 2.

4. **Make persistence and the participation split the empirical centerpiece.** Add uncertainty respecting common periods, show the exact item-share/per-item decomposition, and test whether the named head’s decliners carry the persistence. Include a paired multi-year Gomez null comparison rather than relying on the −0.053 average noise baseline.

5. **Complete a manuscript-to-artifact audit.** Rebuild Fig. 5 from R7, correct titles and captions, synchronize every estimate and denominator, distinguish sampling uncertainty from seed variation, and attach a compact table mapping major claims to code, outputs, and exploratory status.

The owner must confirm the title and revised claim language, resolve the remaining data-availability and authorship placeholders, and decide whether to adopt a revised bridge protocol. **The present P6 should not be adopted unchanged:** it still tests raw β against firm thresholds, although planted firm-like β=0.2 produces raw β≈0.094—below the submissions point cutoff of 0.12. See [protocol:109](/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/BRIDGE_PROTOCOL_V3_DRAFT_2026-10-18.md:109).

P5 also needs instrument conditions explicitly covering its 2023 Q1→2024 Q1 change across the source transition and blackout. New data can establish transport, but cannot rescue an estimator that does not discriminate under known truth. The most valuable new evidence would be independently measured persistence in item counts and forecast crossings under an adopted, instrument-aware protocol; a causal platform-design claim would additionally require a credible intervention or identification design.