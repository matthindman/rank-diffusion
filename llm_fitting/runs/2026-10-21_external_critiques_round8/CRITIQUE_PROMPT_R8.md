This is referee round eight. You, or a model like you, reviewed this project as a referee in round seven, on manuscript v5.
- **Round 7.** The reports, the GPT-6 Pro review run by the owner, and the check of every claim are in `llm_fitting/runs/2026-10-20_external_critiques_round7/`. Start with `SYNTHESIS_R7.md`, findings G1–G33 and the GPT-6 Pro addendum P1–P13.
- **Records.** §2z-do (the round), §2z-dp (Tiers 1–2: R15 and manuscript v6) and §2z-dq (Tier 3: R16 and manuscript v7). Find them with `grep -n '^## 2z-d[o-q]' llm_fitting/MODEL_STATUS.md`.

## What changed since round 7

**Manuscript v7.** `paper/pnas_draft.tex` and `paper/pnas_si.tex`, with compiled PDFs `paper/pnas_draft.pdf` (8 pages) and `paper/pnas_si.pdf` (22 pages), at the commit named in §2z-dq. It revises v5 in two steps.

1. **v6, Tiers 1–2 (§2z-dp):**
   - every confirmed round-7 error corrected;
   - claims restated;
   - R15's diagnostics:
     - **D1:** the observed shape's path against the rate law and the model;
     - **D2:** how much of a planted growth spread reaches the statistics;
     - **D3:** a placebo for the platform loadings;
     - **D4:** the durability factor from measured crossings;
     - **D5:** the composition term in simulated worlds.

2. **v7, Tier 3 (§2z-dq, the owner's instruction "try all the tier 3 items").** R16 has expectations committed before any run, in `PLAN.md` addenda R16 and R16-G:
   - **R16-A:** a reversible decline state, a mirror of GLLM's type switching.
   - **R16-B:** a joint re-estimation of fixed growth differences, with the passing swing freed and a separate spread for the head, on 24 moments.
   - **R16-C:** the two registered A1 recovery runs, plus a calibration of the indirect-inference objective under correct specification.
   - **R16-D:** recovery of the size exponent with each panel's own passing parts.
   - **R16-E:** a capture audit of the Pushshift archive by date and rank, from the daily panels.
   - **R16-F:** the durability comparison rebuilt in the calibrated model: the shape's response to a step in decline, against membership turnover.
   - **R16-G** (post hoc, registered before its run): the decline state added to the joint estimate.

**What R16 changed in the paper's claims:**
- **Persistence as such.** Jointly estimated fixed differences reproduce most of it. R12's "at most three fifths" is withdrawn.
- **The departure is the asymmetry:** declines persist and gains do not. No symmetric form produces it. A skewed decline state gets its sign but little persistence, and the two do not combine.
- **No separation of time scales.** In the calibrated model the head's shape responds to a change in decline on the membership time scale. Durability is the balance, not a slow transition.
- **Capture.** The head's fall does not sit at the archive's file boundaries.
- **Size exponent.** The comments exponent is not understated. The submissions exponent cannot be told from zero.

**Owner decisions still binding** (§2z-dm, verbatim there):
- the GLLM frame;
- horizons of one to two years for communities and five for the system;
- the author's 2018 book chapter cited openly;
- Reddit communities may be named; CrowdTangle aggregates and Page IDs only.

The title is "When the hierarchy of online attention outlasts its members" (owner to confirm).

**What I care about.** A paper a PNAS editor sends out with enthusiasm, and that referees cannot sink. Review the manuscript as a referee would review a submission.

## Read, in this order

1. **The manuscript:** `paper/pnas_draft.pdf` and `paper/pnas_si.pdf`, with their `.tex` sources.
2. **The figures:** `llm_fitting/runs/2026-10-18_pnas_push/fig1_v5.png` … `fig4_v5.png`, with `make_v5_figures.py`.
3. **Plan and results:** `PLAN.md` addenda R15, R15-D5, R16 and R16-G, and `RESULTS.md` sections R15 and R16. Then the scripts and saved outputs behind any number you doubt:
   - `r16_tier3.py` and `r16_capture.py` → `r16_*.json`, `out_r16_*.txt`, `r16_durability_scores.json`;
   - `r15_*.py` → `r15_*.json`;
   - `r12_*` (R12's grids, worlds and recoveries);
   - the opt-in model terms in `llm_fitting/rankdiff_rdm.py` and their tests in `tests/test_rankdiff_rdm.py`.
4. **The record:** `MODEL_STATUS.md` §2z-dp and §2z-dq, and `SYNTHESIS_R7.md`.

## How to work

As before:
- Check claims against code and saved outputs, and recompute what you can from saved outputs. Look for:
  - an identity posing as a test;
  - selection;
  - look-ahead;
  - tuning to a score;
  - post hoc results presented as registered;
  - numbers that disagree between the manuscript, the SI and the outputs.
- Keep the panel:
  - a theorist of rank-based diffusions and stochastic portfolio theory;
  - a computational social scientist of online attention;
  - an econometrician of heterogeneous income processes and indirect inference;
  - an inequality economist who knows GLLM (2016);
  - a PNAS editor;
  - an editor who cares about prose and structure;
  - a short seller, who builds the case against the weakest load-bearing claim from the repository's own numbers.

## Rules (binding)

- **Read-only.** Do not create, modify or delete repository files, and do not commit.
- **Data.** Do not open raw data files or any parquet panel, and never touch the folders in `llm_fitting/SEALED_PATHS.json`. Code, documents, figures and saved outputs under `llm_fitting/runs/` are open.
- **No heavy computation.** Do not run any model, simulation or estimation script, and do not load a data panel. This machine's memory is limited and the owner forbids sustained swapping. Small scripts that read saved JSON or text outputs are fine.
- **Be specific:** files, lines, numbers.

## What I need back (one report, about 2,500–4,000 words)

1. **Editor's decision now.** Desk reject, send out, or send out with enthusiasm? Why, in five sentences? What single change would most raise the odds?
2. **The paper after R16.** The claims shifted:
   - durability is the balance, not a slow transition;
   - persistence is consistent with heterogeneous growth;
   - the asymmetry is the departure.

   Is the paper now stronger or weaker as a PNAS paper? Is its main contribution clear? What should the title and abstract lead with?
3. **R16's designs and readings.** For each of A to G: is the design standard and sufficient? Is the reading right, at the right strength? Pay particular attention to these:
   - the joint re-estimation's edges (the swing multiplier and head multiplier are at the grid's edge);
   - the decline state's edges;
   - the post hoc lag-aware fit in R16-F, after the registered fit was misspecified;
   - what the capture audit can and cannot exclude;
   - R16-G's design, which holds the joint optimum fixed.
4. **Each Results section.** Is the evidence sufficient for the sentence each heading claims? Name the claim a referee would attack first in each subsection.
5. **The asymmetry as the discovery.**
   - Is it robust: statistic, selection, mean reversion, capture, survivorship?
   - Is it new against Guvenen et al. (2021) on asymmetric earnings dynamics and against the online-community life-cycle literature?
   - What would make it convincing?
6. **Round-7 points.** Which are resolved, half-resolved or still open (G1–G33, P1–P13)?
7. **Errors.** Numbers, derivations, code, figure captions, citations, and the SI against the main text, especially in passages rewritten for v6 and v7.
8. **Writing and structure.**
   - section, subsection and paragraph structure;
   - topic sentences;
   - density;
   - the figures and Table 1;
   - the SI's organization.

   Give concrete rewrites for the paragraphs that most need them.
9. **The five most valuable changes before submission,** ranked, using the data on hand. Then what needs the owner or new data.
