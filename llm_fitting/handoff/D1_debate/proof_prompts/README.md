# Mathematical proof work: targets and prompting conditions

2026-10-12 · OpenAI Codex · **PROPOSED PROMPTS; no proof run or model adoption.**

Prepared at the owner's request after the [research agenda](../RESEARCH_AGENDA_CODEX_2026-10-12.md). These are mathematical research tasks, not a request to confirm the observed variance–slope relationship. None of the proposed extensions is certified to be an open problem. The [theory note](../variance_structure/THEORY.md) identifies established results and earlier literature reading.

## Recommended order

| Priority | Prompt | Decisive result | Research consequence |
|---|---|---|---|
| 1 | [Estimation and identification bridge](PROOF_1_ESTIMAND_BRIDGE.md) | A proved bridge, or observational equivalence plus a sufficient additional observation design | Determines whether our movement instrument measures the coefficient needed to predict structure |
| 2 | [Named types, renewal, and structural stability](PROOF_2_NAMED_RENEWAL.md) | A conditional stability theorem or counterexample for an explicit process | Determines whether independently specified movement and turnover can generate a stable ladder and at what rate |
| 3 | [Discrete observation of crossings](PROOF_3_COLLISIONS.md) | A justified sampling correction, or an impossibility/bias result | Determines what weekly crossings can tell us about relative diffusion and structural balance |

Start with one target in one solver session. Target 2's mathematical analysis can proceed independently, but its empirical application depends on Target 1. The elementary stationary identity and its elementary ratio-error bound are baselines, not proposed discoveries. Do not begin with an unrestricted theorem covering every type of drift, noise, turnover, common shock, and boundary.

## How to run

Use Astra with a high reasoning setting available in the selected interface; for a difficult attempt, the highest available setting is a reasonable starting choice, not a demonstrated optimum. Record the exact model, effort, date, enabled tools, resource limit, complete prompt, revisions, and outcome. A sentence inside a prompt does not configure compute. No particular token or time budget is authorized by this document.

Supply the selected prompt, the necessary project instructions, and the theory note as a source map. Avoid loading the entire debate or its favorable numerical results into the mathematical problem: empirical agreement is not a premise of a theorem. Each prompt is otherwise self-contained. Permit primary-source browsing; exact symbolic checks can be useful, but this task does not authorize data analysis, stochastic simulation, model implementation, or gate evaluation.

Ask for a serious proof attempt, with room to choose the mathematical method. Checkpoint the exact statement, established lemmas, unresolved obligations, and references before a context or resource limit. Resume from those artifacts rather than repeatedly asking for a fresh essay. An unresolved result should show substantive work and the precise obstruction; it is not an excuse to stop at a plan.

Then give the **statement, finished proof, and references** to a fresh session using [the referee prompt](PROOF_VERIFIER.md). Do not include the solver's confidence or exploratory narrative. This separation reduces shared context; it does not make errors statistically independent. Ask for specific objections, not agreement. Return substantive objections to the solver for repair; a changed statement receives a new version. Re-review the resulting proof and affected dependencies. Stop repetitive review when it produces no new evidence. A mathematical expert must assess any result used in the paper; model approval is not a proof certificate.

Formal verification is a useful later route for a tractable theorem or critical lemma. It must verify the intended statement and its dependencies, not a weakened translation. A compiling LaTeX file, numerical check, or Lean file containing unproved assumptions does not establish the full result. The present prompts request mathematical text, not a new formalization infrastructure project.

## What the recent research supports

**Astra-specific guidance.** OpenAI recommends clear authorization to pursue the task, explicit deliverables, and attention to conflicting instructions. Its September discussion also advises removing accumulated workflow scaffolding that no longer helps. We therefore specify the mathematical question and acceptance conditions while leaving proof strategy open. These are general model instructions, not an experimental comparison of probability-theorem prompts. [READ THIS SESSION: “Prompting best practices,” including initiative and instruction-following guidance, in [Using GPT-6](https://developers.openai.com/api/docs/guides/latest-model#prompting-best-practices); September 11 article [Rethinking skills and prompts for GPT-6 Astra](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra).]

**Published prompt examples.** OpenAI's February First Proof appendix describes idea generation, solution attempts, checking both arguments and references, and bounded revision. Its accompanying report acknowledges human selection and corrections. The June First Proof second batch includes a short proof-focused prompt for GPT-5.5 Pro and expert refereeing. These are useful examples, not evidence that their exact wording is best for Astra. We retain precise statements and checked intermediate results, but replace an unconditional demand for a proof with permission to disprove the claim or identify a remaining gap. [READ THIS SESSION: Appendix A, especially A.1–A.3, in [First Proof?](https://cdn.openai.com/pdf/26177a73-3b75-4828-8c91-e8f1cf27aaa0/oai_first_proof.pdf); methodology and corrections in [Our First Proof submissions](https://openai.com/index/first-proof-submissions/); §4.3 and §5 refereeing description in [First Proof Second Batch](https://arxiv.org/html/2606.18119v1). The individual mathematical solutions were not audited.]

**Separate generation and verification.** Aletheia reports benefits from a generator/verifier/reviser workflow, explicit unsuccessful outcomes, and tool-assisted literature use. Its results concern its own models and harness; they do not establish equivalent gains for our task. Our separate referee session is a practical adaptation, not a replication of that system. [READ THIS SESSION: abstract, §§2–2.3 and §5.1 excerpts of the March 16 version of [Towards Autonomous Mathematics Research](https://math.berkeley.edu/~fengt/Aletheia.pdf).]

**Verification still needs scrutiny.** A 2026 verification paper reports prompt sensitivity and a possible mismatch between improved proof metrics and actual mathematical validity. This supports requiring checkable objections and dependencies rather than a model confidence score. [READ THIS SESSION: full abstract only, not methods or results tables, of [Scaling Generative Verifiers for Natural Language Mathematical Proof Verification and Selection](https://proceedings.mlr.press/v306/mahdavi26a.html).]

**The newest frontier results are not a prompt guarantee.** The October 6 OpenAI release and repository describe results from an unreleased internal model, at differing verification stages, with significant compute and many attempted problems. They provide a reason to attempt serious mathematics, but do not establish that the available Astra model has the same capabilities or that this field is easy. We did not audit those proofs or infer a success probability for our targets. [READ THIS SESSION: release text in [Sharing AI progress in mathematics](https://openai.com/index/sharing-ai-progress-in-mathematics/); repository README, especially verification status and production method, at [openai/math](https://github.com/openai/math).]

**Research output should remain understandable.** The September recommendations from the Advisory Group on Mathematics and Artificial Intelligence emphasize attribution, conventional theorem/proof exposition, transparent provenance, and explicit formalization status. Our deliverables follow those principles without treating them as evidence that any proposed theorem is true. [READ THIS SESSION: introduction and §2, especially initial-release recommendations, in [AGMAI recommendations](https://agmai.org/general-sep29/).]

This search found useful recent workflows and concrete prompts; it did not establish a universally best prompt or an Astra-specific controlled study for these stochastic-analysis problems. The prioritization, target statements, and detailed error checks below are our project-specific judgment. Searches also surfaced AlphaProof and ProofBench work, but full relevant texts were not retrieved successfully; they are not used to support recommendations here.

## Project scope

Saving these prompts does not start proof agents, adopt the agenda, register an experiment, change a model, or authorize new data contact. Future repository use must remain within the owner's allowed working copy and output directory. The initial solver output should be a research note with theorem statements and proofs, or a precise account of what remains unresolved, plus the observation requirements implied by the mathematics.
