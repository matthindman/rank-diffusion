# Brief: critique the structure of manuscript v5 before it is written

**Read these:**
- `llm_fitting/runs/2026-10-20_v5_revision/OUTLINE_v5.md`: the proposed structure.
- `paper/pnas_draft.tex`: v4, the current text.
- `llm_fitting/runs/2026-10-19_literature_and_design_reviews/SYNTHESIS.md`: what eight reviews found and what the lead agent judged.
- `REPORT_T3_opus-5.5.md` and `REPORT_T3_gpt-6-astra.md` in the same folder: the evidence on PNAS exemplars.

**The owner's directions of 2026-10-20, paraphrased:**
- **Goal.** The research aims to understand where online concentration comes from: the processes that generate it, and its durability both in structure and for individual communities. That includes lock-in in aggregate and for individual communities. The owner also wants to know what medium-term structure follows from current growth rates, variance and drift.
- **Horizons.** These are 1–2 years at most for individual units and about 5 years for system-level understanding and policy. Present 20–30-year horizons only carefully: noise compounds, and the paper must not overclaim.
- **Durability as a contribution.** If durability is a natural result of the homogeneous random-growth model, that is an important finding in itself. It is not widely known among platform researchers. Do not overclaim novelty, but make clear that a plausible model researchers will accept has these non-obvious results.
- **The fast-transition frame** of Gabaix, Lasry, Lions & Moll is approved as the organizing idea.
- **A calibrated heterogeneous-growth world** is approved, using the most standard methods with references.
- **Less dense writing.** The owner cares a lot about the structure of sections, subsections and paragraphs, because it will survive a detailed human rewrite.
- **More models in the SI**, as plausible eligible models tuned to the existing data, are approved.
- **References and corrections:** add the new main-text references and make all the corrections.
- **Uses of the model.** Explain how scholars use these models, and how they avoid using them. Include clear applications for platform studies.
- **The precursor.** The lead author's 2018 book chapter is the direct precursor. The final version must draw a through line from it. Handle it carefully in the submission, because it may be personally identifying.

**Questions.** Answer in at most about 1,500 words, ranked and specific:
1. **Argument.** Is the order of the argument right, with each subsection answering one question with one comparison? What would you move, merge or split?
2. **Results 2.** Does it make the durability point clearly, as a property of a model readers will accept? Is it placed where a broad reader will see it?
3. **The fast fall.** Is Results 4 placed and sized correctly? Should it come before the model (Results 3)?
4. **Figures.** Is there one per question, with panels well chosen? What is missing or redundant?
5. **Discussion.** Is it well designed, especially the "what this model is for and not for" paragraph?
6. **Gaps.** What will a referee find missing from the structure?
7. **Budgets.** Any word-budget or paragraph-count changes, to reach 7 pages with 4 figures.

**Rules:**
- Read-only. Do not modify files.
- Do not open raw data or anything in `llm_fitting/SEALED_PATHS.json`.
- Return the critique as your final message.
