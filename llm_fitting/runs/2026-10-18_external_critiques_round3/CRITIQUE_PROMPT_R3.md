This is round three. You, or a model like you, reviewed this project twice
today. The round-2 reports and our responses are in
`llm_fitting/runs/2026-10-18_external_critiques_round2/`; start with
`SYNTHESIS_R2.md`.

Since round two the project has become a manuscript:
- **The owner set the title's direction:** the contradiction between fast
  turnover and a slowly changing hierarchy, and how churn creates and
  maintains that hierarchy. The core of the generative model stays in the
  main text.
- **The first full draft is `paper/pnas_draft.tex`,** with its figures in
  `llm_fitting/runs/2026-10-18_pnas_push/fig*.png`.
- **New analyses, with expectations committed before each run,** are in
  `runs/2026-10-18_pnas_push/`: `PLAN.md` holds the expectations and
  `RESULTS.md` the scored results. Record: `MODEL_STATUS.md` §2z-dg.
- **Several round-2 findings changed the paper.** The persistence statistic
  now uses changes that share no weeks. "Momentum" turned out to be
  persistent decline in participation. The model's variance error (your V4)
  failed it on the long panel, and a corrected variant was tested.

The outcome I care about is unchanged: a paper a PNAS editor sends out with
enthusiasm and referees cannot sink. **Review the manuscript as a referee
would review a submission.**

## Read, in this order

1. `paper/pnas_draft.tex`: title, significance, abstract, main text,
   Methods. Compile it if you can, or read the source.
2. The figures, `runs/2026-10-18_pnas_push/fig1_puzzle.png` …
   `fig5_tipping.png`, with `make_pnas_figures.py`.
3. `runs/2026-10-18_pnas_push/PLAN.md` and `RESULTS.md`, then the scripts
   and saved outputs behind any number you doubt.
4. `MODEL_STATUS.md` §2z-dg (find it with `grep -n '^## 2z-dg'`), and
   §2z-df for the round-2 response.
5. `llm_fitting/PAPER_FRAME_DRAFT_2026-10-18d.md` (claims, title options,
   what is not claimed); `BRIDGE_PROTOCOL_V3_DRAFT_2026-10-18.md`, amendment
   v3.1.

## How to work

As before:
- Check claims against code and saved outputs, and recompute where you can.
  Look for:
  - an identity posing as a test;
  - selection;
  - look-ahead;
  - tuning to a score;
  - post hoc results presented as registered;
  - numbers that disagree between the manuscript and the outputs.
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
2. **The title and the central claim.** Is "the churn that unseats online
   leaders creates the hierarchy they lead" earned by the evidence? Which
   word is weakest? Give a better title if you have one.
3. **Each Results section.** Is the evidence sufficient for the sentence
   the section leads with? Name the claim a referee would attack first in
   each:
   - (1) the puzzle;
   - (2) scale-free churn and the turnover formula;
   - (3) the balance;
   - (4) the generative model, including the long-panel gates and the
     corrected variant;
   - (5) persistence and the falling head.
4. **The model in the main text.** Is the core stated correctly and
   minimally? Is anything there contradicted by the code (`rankdiff_rdm.py`)
   or by the gates?
5. **Round-2 points.** Which are resolved, half-resolved or still open?
6. **Errors.** Numbers, derivations (check Eq. 1 and the variance-preserving
   correction), code, figure captions, citations.
7. **The five most valuable changes before submission,** ranked, with the
   data on hand. Then what needs the owner or new data.
