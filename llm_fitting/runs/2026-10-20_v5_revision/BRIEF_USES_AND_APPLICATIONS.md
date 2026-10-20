# Brief: what stochastic random-growth and rank models are used for, how scholars avoid misusing them, and clear applications for digital platform studies

The owner asks three things:
- how other scholars use these kinds of stochastic models;
- what the models are useful for;
- "just as importantly, how we generally avoid using them".

The owner also wants other potential uses of this model for scholars, judged from similar models in other disciplines. "If there are clear wins, clear useful applications of this model for digital platform studies, make sure that they are included in the revised draft."

## Read first

- `paper/pnas_draft.tex`, `paper/pnas_si.tex`.
- `llm_fitting/runs/2026-10-19_literature_and_design_reviews/SYNTHESIS.md`.
- `REPORT_T2_opus-5.5.md` and `REPORT_T2_gpt-6-astra.md` in the same folder. These already cover much of the uses side. Build on them; do not repeat them.

## Owner constraints on horizons (binding for the applications)

- The paper's useful time frame is 1–2 years at most for individual units, and about 5 years for system-level understanding and policy.
- 20- or 30-year horizons may appear only carefully, as model-implied rates, because noise compounds at every step of a stochastic simulation or an estimated process.
- The paper must not overclaim.

## Questions

1. **Uses.** For random growth (Gibrat, Kesten, Gabaix), rank-based diffusions and stochastic portfolio theory, neutral models, preferential attachment, and permanent–transitory panel models:
   - Give the main scientific uses with one or two exemplary papers each, across economics, finance, ecology, cultural evolution, scientometrics and the study of online popularity.
   - Say which uses are mainstream in each field.
2. **Non-uses and misuses.** What do careful scholars avoid using these models for, and why? Give citations for each caution. Examples to examine, and others you find:
   - point forecasts of individual trajectories;
   - long-horizon extrapolation, and parameter instability;
   - inferring mechanism from a stationary distribution, that is equifinality (Stumpf & Porter 2012; Clauset, Shalizi & Newman 2009; Mitzenmacher 2005; McGill et al. 2007);
   - causal claims about interventions, a Lucas-critique logic (coefficients change when policy changes);
   - reading a fitted power law or Zipf exponent as evidence of a specific process;
   - treating calibration as validation;
   - ignoring measurement error and selection, the survivorship of top lists.

   Write each as an explicit "how to avoid it" practice that our paper can follow, and note where the draft already follows it or does not.
3. **Clear applications for digital platform studies,** given what our framework measures and the data platform researchers typically have: ranked top-K lists, weekly activity, restricted access. For each, give:
   - the question it answers for platform scholars;
   - the analysis with our model;
   - the data it needs, and whether it is feasible with our data on hand now;
   - the horizon, within the owner's constraint;
   - a precedent from another field;
   - risks.

   Rank them, and mark which are clear wins that belong in the main text or SI now, and which are future work. Candidates, to judge, not assume, plus any you find:
   - a null benchmark for claims of algorithmic amplification, winner-take-all dynamics or entrenchment;
   - individual-level lock-in: the probability that a top-k member remains in 1–2 years, from measured volatility and slope;
   - medium-term (1–5 year) projections of the ladder's shape, with bands;
   - decomposing a change in concentration into volatility, decline, entry and exit, and persistent heterogeneity (as Fernholz & Koch do for banks, and as Gomez does);
   - controlling turnover profiles for geometry;
   - measurement from top-K lists alone;
   - separating passing from lasting rises (luck or position);
   - detecting platform interventions as changes in drift or volatility;
   - comparing platforms in dimensionless coordinates;
   - the time for concentration to respond to a change.
4. **Relevant results already on hand.** For the main-text applications, which of our existing results (in the manuscript, the SI and `llm_fitting/runs/2026-10-18_pnas_push/RESULTS.md`) already answer them? Which need a small new calculation from saved outputs, and which need new runs?

## Standards

- Give a DOI or stable URL for every work.
- Label access: [full text], [abstract only], [secondary] or [memory, unverified].
- Keep quotations to short exact phrases with page numbers, labelled verified or unverified.
- Keep checked facts separate from proposals.
- Read-only: do not modify repository files.
- Do not open raw data or anything in `llm_fitting/SEALED_PATHS.json`.

## Deliverable

A memo of about 3,000–5,000 words in three parts:
1. **Uses:** a table.
2. **Avoidances:** a table of each caution, the citation, the practice that avoids it, and the draft's status.
3. **Applications:** ranked, each with a recommendation and a placement (main text, SI or future).
