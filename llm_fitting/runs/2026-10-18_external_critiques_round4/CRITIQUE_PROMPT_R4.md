This is round four. You, or a model like you, reviewed this project three
times today. The round-3 reports and our responses are in
`llm_fitting/runs/2026-10-18_external_critiques_round3/`; start with
`SYNTHESIS_R3.md`.

Since round three the manuscript is version 2:
- **The owner set the question and approved the framing.** Where does a
  durable, quasi-stable hierarchy come from when churn is so high? The title
  is now "Constant turnover holds up the durable hierarchy of online
  attention". The owner said the framing is fine and the title had to be on
  that question.
- **Null worlds now separate the automatic from the discovered.** Every
  headline statistic is computed with the same code in four simulated
  worlds: size-blind random growth, firm-like growth, neutral copying, and
  the data-calibrated model. The paper treats Eq. 1, a positive slide, a
  stable rank-1 occupant and a boundary ratio near one as automatic. It
  treats as discoveries:
  - size-blindness, which makes the top set turn over fastest;
  - persistence;
  - a balance in which crossings fall short of the slide.
- **One model.** The corrected moving home (`movinghome_vp`, with drift
  applied) carries the gates, the out-of-period test and the model world. The
  flat start has 20 seeds and bands.
- **Persistence repaired:** prefix-only membership, a split at the true sign,
  and a platform-total factor on matched origins.
- **Gomez with his general displacement term.**
- **New since the v2 commit** (`PLAN.md` addendum R4, `RESULTS.md` R4):
  - a post hoc check of persistence in triples that start after the 2020
    surge;
  - the SI flat-start table at 20 seeds;
  - the model's parameter structure stated;
  - a data-availability statement built on the CrowdTangle terms and on the
    practice of careful CrowdTangle studies.

The outcome I care about is unchanged: a paper a PNAS editor sends out with
enthusiasm and referees cannot sink. **Review the manuscript as a referee
would review a submission.**

## Read, in this order

1. `paper/pnas_draft.tex`: title, significance, abstract, main text,
   Methods. Then `paper/pnas_si.tex`. Compile them if you can, or read the
   source.
2. The figures, `runs/2026-10-18_pnas_push/fig1_puzzle.png`,
   `fig2_size_blind.png`, `fig3_replenish.png`, `fig4_model.png` and
   `fig5_tipping.png`, with `make_pnas_figures.py`.
3. `runs/2026-10-18_pnas_push/PLAN.md` (addenda R3 and R4) and
   `RESULTS.md`, then the scripts and saved outputs behind any number you
   doubt. These include `null_worlds.py`, `persistence_r3.py`,
   `gomez_r3.py`, `persistence_post2020.py` and their JSONs.
4. `MODEL_STATUS.md` §2z-dh and §2z-di (find them with
   `grep -n '^## 2z-d[hi]'`).
5. `llm_fitting/BRIDGE_PROTOCOL_V3_DRAFT_2026-10-18.md`, amendment v3.1, the
   registered tests on the sealed 2023–2024 data. The owner said "the
   protocol looks fine"; it is not yet adopted.

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
2. **The title and the question.** The question is where a durable hierarchy
   comes from when churn is so high. Does the paper answer it, and is
   "holds up" earned? The evidence offered:
   - crossings return about three quarters of the slide;
   - without churn the hierarchy collapses, slowly;
   - from a flat start churn and the slides build a hierarchy, flatter than
     the observed one.

   Which word is weakest? Keep the owner's question when you propose a
   title.
3. **Each Results section.** Is the evidence sufficient for the sentence
   the section leads with? Name the claim a referee would attack first in
   each:
   - (1) the puzzle;
   - (2) size gives little protection; Eq. 1 as decomposition; C(R)
     against set replacement;
   - (3) the balance and the null worlds;
   - (4) the generative model;
   - (5) persistence, including the post-2020 check, and the falling head.
4. **Automatic against discovered.** Is the split right? Is anything called
   a discovery that a null world would produce if calibrated differently,
   for example a size-blind world with the data's passing parts? Is anything
   called automatic that is in fact informative?
5. **Round-3 points.** Which are resolved, half-resolved or still open?
6. **Errors.** Numbers, derivations, code, figure captions, citations, and
   the SI against the main text.
7. **Data availability.** Is the statement in Methods adequate for PNAS? Is
   it within the CrowdTangle and Reddit/Pushshift terms as the repository
   documents them?
8. **The five most valuable changes before submission,** ranked, with the
   data on hand. Then what needs the owner or new data.
