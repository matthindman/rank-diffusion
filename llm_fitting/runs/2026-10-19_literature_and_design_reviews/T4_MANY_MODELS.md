# Task 4: one hero model, or all of the plausible models?

## Background

So far the program has built a single best model, a "hero model". That work was needed and useful. The owner now asks whether a different presentation would be better. Their reasoning, paraphrased:

- **The single hero model may be a pre-AI habit.** With current tools it is feasible to show how a whole host of plausible models handles the same data. The overview could go in the SI, and perhaps partly in the main text.
- **The authors are agnostic about which model wins.** Each model simplifies reality on purpose: it captures key parts of the problem and omits the less important ones.
- **The data are fixed.** CrowdTangle no longer exists, so no more Facebook data can be collected, and the past cannot be rerun.
- **Referees fear cherry-picking.** Their skepticism is often calibrated to the worry that the researcher is presenting a single preferred model.
- **The draft's comparisons are already one of its best features.** It compares the model with competing baseline models. In many ways the model matches them or does slightly worse, but it captures key features they miss.
- **The way models fail is itself interesting and important,** and the authors want to be upfront about it.
- **An ablation already exists.** A model explainer starts from the simplest rank diffusion and adds one feature at a time, showing how each addition improves the fit, or does not.

The owner gives the obvious objections substantial weight:
- **Choice and filtering cannot be avoided,** even with many models.
- **The format fits research papers badly,** and raises practical problems.
- **Reviewers are unfamiliar with it,** so it may generate more skepticism, not less.

The owner asks for the idea to be considered formally, even if it is not adopted. The same spirit might also help in thinking through further robustness checks.

## Materials

In addition to the common brief, read:
- `llm_fitting/runs/2026-10-12_ladder/DESIGN.md`, `explainer_text.json` and `explainer.html`. This is the ablation ladder: from a rank diffusion (R0), through noise, home, swings, amplitude and tails, to the frozen model stacks, with a held-out movement score and a long-run structure score on five panels.
- `paper/pnas_si.tex`: the section on the simulated worlds and the null-worlds table, at 20 seeds.
- `llm_fitting/runs/2026-10-18_pnas_push/null_worlds_r7_summary.json`, `RESULTS.md` (sections R7, R9 and R10) and `PLAN.md`.
- `llm_fitting/GATE_LEDGER.md`: every held-out adoption test run, including failures.
- `llm_fitting/PROGRAM_STATE.md`.

## Questions

1. **Should the paper take an "all of the models" approach?** Give the strongest case for it, then the strongest case against it, then a recommendation. Weigh the owner's objections seriously. "No" and "only in a limited form" are acceptable answers.
2. **Precedents.**
   - Where have multi-model presentations been published in strong journals, and how were they received? Consider, among others:
     - model intercomparison projects, such as CMIP and ecological model comparisons;
     - forecasting ensembles: Cramer et al. 2022 in PNAS is in our exemplar set;
     - many-analysts studies (Silberzahn et al.; Breznau et al. in PNAS);
     - multiverse and specification-curve analysis;
     - comparisons of power-law alternatives (Clauset, Shalizi and Newman; Broido and Clauset);
     - neutral-versus-niche tests in ecology;
     - Levins's "robustness analysis";
     - Bayesian model averaging and stacking.
   - What made the credible ones credible rather than confusing? Use page-cited quotations where they make the point.
3. **Design options.** Rank the concrete options for this paper, for example:
   - **(a)** A main-text matrix of stylized facts × models: which models reproduce which facts, and how each fails.
   - **(b)** An SI model atlas with a common scorecard.
   - **(c)** The ablation ladder as a figure.
   - **(d)** Registering forecasts from every model, not only the hero, for the sealed 2023–24 Reddit test, so that the confirmatory test becomes a model comparison.
   - **(e)** A multiverse over the analyst's choices rather than over models: the universe size K, the metric, cohort rules, windows and membership rules.
   - **(f)** Other options you think better.

   For each, give:
   - what it would show;
   - its space cost against a 6–7 page main text;
   - the compute and authoring cost;
   - its risk with referees;
   - what it would need that the program does not yet have.
4. **Choosing which models and facts.**
   - How should the unavoidable selection be made defensible? Consider:
     - stating the selection rule before scoring;
     - drawing models from rival traditions, for example SPT/Atlas, GLLM-type heterogeneous growth, Yule–Simon and preferential attachment, neutral copying, and fitness or aging models;
     - declaring the list of facts in advance;
     - reporting everything.
   - Which models are missing from the current set that a referee would most expect?
5. **Failure as information.** How should the draft present failures? Two examples:
   - the hero model's hierarchy is flatter than observed;
   - it does not forecast the head's fall.

   How should it present the ways the null worlds fail? Present this so that the failures inform the reader rather than weaken the paper.
6. **Robustness in this spirit.**
   - List the 5–10 most valuable robustness checks possible with the data on hand.
   - For each: the inference it tests, and what result would change the paper.
   - Note which are already done; the SI and RESULTS.md list many.
7. **Recommendation.** The minimal version that would capture most of the benefit, and exactly where it would go in the paper.

## Deliverables

An access check, then answers to 1–7.

## Length

About 3,000–6,000 words.
