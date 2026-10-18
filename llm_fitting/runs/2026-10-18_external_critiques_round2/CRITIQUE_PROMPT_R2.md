This is round two. You, or a model like you, reviewed this paper earlier
today. The round-1 reports and our responses are in
`llm_fitting/runs/2026-10-18_external_critiques/` (start with
`SYNTHESIS.md`).

Since then the paper has changed in four ways:
- a new headline (`PAPER_FRAME_DRAFT_2026-10-18c.md`, "size does not
  protect");
- a generative model built and validated (the "moving home",
  `llm_fitting/rankdiff_rdm.py`);
- a new empirical regularity the model fails to reproduce (momentum on
  Reddit);
- the owner's decisions to make the balance law the spine and to read Gomez
  (2023), who turns out to have the displacement law.

I want a deeper second look, not a longer one. The outcome I care about is
unchanged: a paper a PNAS editor sends out with enthusiasm and referees cannot
sink.

## Read, in this order

1. `llm_fitting/PAPER_FRAME_DRAFT_2026-10-18c.md`: the working draft
   (significance, abstract, argument, figures, what it does not claim).
2. `llm_fitting/MODEL_STATUS.md`, sections §2z-da through §2z-de (find them
   with `grep -n '^## 2z-d' llm_fitting/MODEL_STATUS.md`).
3. `llm_fitting/runs/2026-10-18_rdm/DOSSIER.md`, with its three addenda, then
   `llm_fitting/rankdiff_rdm.py`, `fig_rdm.png` and the saved outputs in that
   folder (gate, structure, recovery, velocity).
4. `llm_fitting/runs/2026-10-18_altitude/MEMO.md` and `fig_contestable.png`;
   `llm_fitting/runs/2026-10-18_momentum/` (the design and the output).
5. `llm_fitting/BALANCE_LAW_THEORY_NOTE_2026-10-14.md` §8–§9 (exact estimand;
   Gomez); `paper/pnas_outline_v2.tex`.

## How to work

Same as round 1:
- **Check claims against code and saved outputs.** Recompute where you can.
  Look for an identity posing as a test, selection, look-ahead, tuning to a
  score, and numbers that disagree between files.
- **Keep the panel:**
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
  `llm_fitting/SEALED_PATHS.json`. Code, documents, figures and saved outputs
  under `llm_fitting/runs/` are open.
- No heavy computation. Be specific: files, sections, numbers.

## What I need back (one report, about 2,000–3,500 words)

1. **Verdict now.** Five sentences: is this closer to publishable than in
   round 1? What is now the single largest gap?
2. **Round-1 points.** Which were addressed, which half-addressed, which
   ignored? Name the ones that still matter most.
3. **The model.** Would a referee accept the moving-home model and its
   validation?
   - **Design:** the hold drift as a fixed point; jumps rejected by the
     ladder's stability; the moving home replacing the fixed home.
   - **Evidence:** the clean Reddit gates, the in-sample structure suite,
     recovery.
   - What would they attack first, and what is the strongest fair criticism?
4. **Momentum.**
   - Is the finding real as presented, and is it new?
   - What mechanism do you think produces it, and what test with data
     already on hand would decide between mechanisms?
   - Should the paper lead with it, report it, or wait?
5. **The headline.** Is "size does not protect" the right lead for PNAS,
   given the size range (two to three decades), the units (karma), and
   Gomez's precedence for the balance? Name a better one if you have it.
6. **The five most valuable next steps** with the data on hand, ranked.
   Then what needs the owner, sealed data or new data.
7. **Errors.** Anything wrong in the new material: numbers, claims, code.
