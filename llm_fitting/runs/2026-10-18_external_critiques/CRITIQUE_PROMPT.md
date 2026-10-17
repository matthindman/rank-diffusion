I want a deeper review of this paper, not a bigger one. The outcome I care
about is not a list of comments. It is a paper that a PNAS editor sends out
with enthusiasm, that referees cannot sink, and that people working on online
attention, inequality and platform governance still cite in ten years. Tell
me what stands between this repository and that paper, and what the most
ambitious version of it is, using only the data already on hand.

## What this is

A research program (repository root: the directory you are in) builds and
tests stochastic models of rank dynamics in digital attention:
- Reddit communities ranked by weekly karma, comments and submissions,
  2019–2022;
- Facebook pages, 86 weeks;
- Instagram accounts, 2023. CrowdTangle's Instagram search needed a query,
  so the data hold only posts containing the word "a"; a measured censoring
  model handles this.

The central empirical puzzle: rank-size curves of attention keep their form,
while a fifth of every top set is replaced each year and individual ranks
move a great deal.

Two lines of work exist and may be one paper:
- **(A)** A permanent–transitory rank model of individual movement, validated
  out of sample. Its owner-adopted claim set is MIXED: movement moments
  transport across eras, parameters partly do not, and its stationary head is
  too concentrated.
- **(B)** A balance law linking movement to structure:
  d(shape_k)/dt = σ_k²/(2S_k) − s̄_k + entry. The crossing gain at a top set's
  boundary is predicted from lasting volatility σ² and the local slope S of
  the rank-size curve, and is tested rank by rank against measured crossings,
  out of equilibrium. Plus ex ante forecasts of lock-in (who stays in the
  top).

Read first, in this order:
1. `llm_fitting/BIG_PICTURE_REVIEW_2026-10-18.md`: our own overview,
   including the two-papers problem.
2. `llm_fitting/EVIDENCE_PACKET_2026-10-15.md`: every claim, its numbers,
   record section, figure, code, and the scoreboard of registered predictions,
   failures included.
3. `llm_fitting/PAPER_FRAME_DRAFT_2026-10-15.md`: draft title, significance,
   abstract and captions.
4. `llm_fitting/REVIEWER_QUESTIONS_2026-10-15.md` and
   `llm_fitting/BALANCE_LAW_THEORY_NOTE_2026-10-14.md`.
5. `paper/pnas_outline.tex` and MODEL_STATUS.md §6 ("Framing for the
   paper", with the binding claim paragraph of 2026-07-12).
6. The canonical record, `llm_fitting/MODEL_STATUS.md`. Use the section
   headings; the newest work is §2z-bp … §2z-cz.
7. Figures: `llm_fitting/runs/2026-10-15_figures/*.png`,
   `llm_fitting/runs/2026-10-15_comments_2021_head/fig_shape_decomposition.png`.
8. Code: `llm_fitting/mts_boundary_law.py`, `mts_balance_law.py`,
   `mts_lockin.py`, `ig_censoring_sim.py`, `balance_accounting.py`; tests in
   `tests/`.
9. Saved outputs (JSON and text) under `llm_fitting/runs/2026-10-1*/`.

## How to work

- **Do not judge the work by its own documents.** Check load-bearing claims
  against the code and the saved outputs:
  - recompute a number from a saved JSON where you can;
  - read the estimator to see whether it measures what the text says;
  - look for an identity masquerading as a test, a selection effect, a
    look-ahead, a tuned threshold, or a number that disagrees between files.
- **Assemble the panel this paper needs, and have each member do what that
  person does, not write like them:**
  - a stochastic-process theorist who knows rank-based diffusions and
    stochastic portfolio theory (Fernholz, Karatzas, Ichiba, Banner;
    Itkin & Larsson; Baker & Smyth 2026);
  - a computational social scientist who studies online attention and
    platforms (Wu & Huberman; Iñiguez et al.; Blumm et al.; algorithmic
    amplification);
  - an econometrician focused on identification, selection and measurement
    error;
  - an economist of inequality dynamics (Gabaix; Gabaix, Lasry, Lions &
    Moll; Luttmer);
  - a PNAS editor deciding whether to send it out.
- **Add a short seller.** That person's job is to find the weakest
  load-bearing claim and build the case that it is wrong, using the
  repository's own code and numbers, the way a short seller researches a
  position, not the way a critic opines.
- **Name a target that seems unreasonable, then argue for it.** If the
  paper could claim something much bigger and still defend it with these
  data, say what it is and what analysis would carry it. If something seems
  impossible, look for the version that is solvable.

## Rules (binding)

- Read-only. Do not modify, create or delete files in the repository, and do
  not commit.
- Do not open raw data files, and never touch the sealed folders listed in
  `llm_fitting/SEALED_PATHS.json`. Code, documents, figures and the saved run
  outputs under `llm_fitting/runs/` are open to you.
- No heavy computation; reading and small calculations on saved outputs are
  fine.
- Be specific: file paths, record sections, numbers. No generic advice.

## What I need back (one report, about 2,500–4,500 words)

1. **Verdict.** In five sentences: would PNAS publish this as it stands? What
   is the single largest gap? What one change would most raise the odds?
2. **The ambitious version.** What this paper could claim, and the analysis
   or figure that would carry each claim, that would make it field-changing
   rather than incremental. Include how (A) and (B) should relate, and what
   the headline result should be.
3. **The short seller's case.** The five weakest load-bearing claims, each
   with:
   - the specific evidence against it from the repository;
   - how a hostile referee would phrase it;
   - what would fix or retire it.
4. **Fair criticisms of the models themselves:** the balance law and its
   estimators, the permanent–transitory model, the lock-in forecaster, and
   the Instagram censoring treatment. Which assumptions carry the most weight
   and are least tested?
5. **What we are not seeing:**
   - blind spots;
   - alternative explanations of the main results;
   - literatures we have missed;
   - data features we have ignored;
   - mechanisms that would explain the same patterns differently.
6. **What experts would require of a PNAS-class paper of this type that we
   do not have:** missing analyses, controls, robustness checks, theory,
   figures, out-of-sample tests, data-availability and reproducibility.
7. **A prioritised change list (at most 15 items).** For each:
   - what to do;
   - with which data and code;
   - the expected payoff for the paper;
   - effort and risk.

   Separate what can be done now with the data on hand from what needs the
   owner, sealed data or new data.
8. **Errors.** Anything in the repository that is wrong: numbers that
   disagree between files, overclaims, estimator bugs, mislabelled figures.
   Give file and line or section.
