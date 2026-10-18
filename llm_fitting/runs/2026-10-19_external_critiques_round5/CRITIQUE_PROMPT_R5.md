This is round five. You, or a model like you, reviewed this project four
times. The round-4 reports, and the check of every claim in them, are in
`llm_fitting/runs/2026-10-18_external_critiques_round4/`; start with
`SYNTHESIS_R4.md`. The record of what changed is `MODEL_STATUS.md` §2z-dj
(find it with `grep -n '^## 2z-dj'`).

Manuscript v3 (`paper/pnas_draft.tex`, `paper/pnas_si.tex`, commit 576a8e4)
answers round 4 with new analyses. Each has expectations written into
`runs/2026-10-18_pnas_push/PLAN.md` before it ran (addenda R5–R8), and each is
scored in `RESULTS.md`, failures included:
- **R5:** the Reddit leaders named. The owner allows this; community names
  are now used.
- **R6:** 30-year counterfactuals from the observed ladder: volatility plus
  slide, slide only, volatility only.
- **R7:** the null worlds at 20 seeds, plus:
  - calibrated size-blind and firm-like worlds started from the data's
    ladder;
  - a planted-β calibration;
  - a Gomez noise baseline;
  - prior-year Gomez cohorts.
- **R8:** the head's share split exactly into item share and karma per item.

Several round-3 "discoveries" are withdrawn:
- flattening, now restated as c/s̄ = S*/S;
- "size-blindness exposes the top", now attributed to the ladder's flat head;
- the raw size exponent as a comparison with firms.

The title is now "How constant turnover holds up a durable hierarchy of
online attention". The owner set the question (where a durable hierarchy
comes from when churn is so high) and is to confirm the wording.

The outcome I care about is unchanged: a paper a PNAS editor sends out with
enthusiasm and referees cannot sink. **Review the manuscript as a referee
would review a submission.**

## Read, in this order

1. `paper/pnas_draft.tex` and `paper/pnas_si.tex`. Compile them if you can.
2. The figures, `runs/2026-10-18_pnas_push/fig1_puzzle.png`,
   `fig2_size_blind.png`, `fig3_replenish.png`, `fig4_model.png` and
   `fig5_tipping.png`, with `make_pnas_figures.py`.
3. `PLAN.md` (addenda R4–R8) and `RESULTS.md` (sections R4–R8), then the
   scripts and saved outputs behind any number you doubt:
   - `null_worlds_r7.py`, `summarize_r7.py`, `null_worlds_r7_summary.json`;
   - `churn_counterfactual.py` (long30) and its JSONs;
   - `head_named.py`, `head_components.py`, `gomez_r7_prior.py`.
4. `MODEL_STATUS.md` §2z-dj; `SYNTHESIS_R4.md`.
5. `llm_fitting/BRIDGE_PROTOCOL_V3_DRAFT_2026-10-18.md`: the tests registered
   for the sealed 2023–2024 data. They are not adopted.

## How to work

As before:
- Check claims against code and saved outputs, and recompute where you can.
  Look for:
  - an identity posing as a test;
  - selection;
  - look-ahead;
  - tuning to a score;
  - post hoc results presented as registered;
  - numbers that disagree between the manuscript, the SI and the outputs.
- Keep the panel:
  - a theorist of rank-based diffusions and stochastic portfolio theory;
  - a computational social scientist of online attention;
  - an econometrician;
  - an inequality economist;
  - a PNAS editor;
  - a short seller, who builds the case against the weakest load-bearing
    claim from the repository's own numbers.

## Rules (binding)

- Read-only: do not create, modify or delete repository files, and do not
  commit.
- Do not open raw data files, and never touch the folders in
  `llm_fitting/SEALED_PATHS.json`. Code, documents, figures and saved
  outputs under `llm_fitting/runs/` are open.
- No heavy computation. Be specific: files, lines, numbers.

## What I need back (one report, about 2,000–3,500 words)

1. **Editor's decision now.** Desk reject, send out, or send out with
   enthusiasm? In five sentences, why? What single change would most raise
   the odds?
2. **The title and the answer.** Is "How constant turnover holds up a
   durable hierarchy of online attention" earned now? The evidence offered
   is:
   - the flat start;
   - the 30-year counterfactuals;
   - the measured balance;
   - the time-scale argument.

   Which word is weakest? Propose a title that keeps the owner's question.
3. **Each Results section.** Is the evidence sufficient for the sentence
   the section leads with? Name the claim a referee would attack first in
   each:
   - (1) the puzzle and the named leaders;
   - (2) why the top turns over fastest, and size;
   - (3) what turnover returns, and durability;
   - (4) where the hierarchy comes from: the model and the counterfactuals;
   - (5) persistence and the head's fall in participation.
4. **What remains discovered.** After round 4, is the remaining claim of
   novelty sufficient for PNAS? It is:
   - persistence against every world;
   - the measured balance and its time scales;
   - the participation split of the head's fall;
   - weaker-than-firm size dependence.

   Is anything still overstated, or is anything undersold?
5. **Round-4 points.** Which are resolved, half-resolved or still open?
6. **Errors.** Numbers, derivations, code, figure captions, citations, and
   the SI against the main text, especially in the passages rewritten for
   v3.
7. **The five most valuable changes before submission,** ranked, with the
   data on hand. Then what needs the owner or new data.
