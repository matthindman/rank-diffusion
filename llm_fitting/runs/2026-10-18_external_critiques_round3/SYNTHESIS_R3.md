# Round-3 referee synthesis: the manuscript (three models)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18.
- **Brief:** `CRITIQUE_PROMPT_R3.md` (69bc82e, before the reviews).
- **Reports**, saved verbatim in this folder: gpt-6-astra (Codex CLI);
  Claude Fable 5.1; a fresh Claude Opus 5.5.
- **Manuscript reviewed:** `paper/pnas_draft.tex` at 69bc82e.
- **Record:** MODEL_STATUS §2z-dh.

A claim below is CONFIRMED only after this session rechecked it._

## 1. Verdicts

| reviewer | decision | single most valuable change |
|---|---|---|
| gpt-6-astra | desk reject, encourage resubmission | separate what the empirical balance establishes from what one consistently specified model predicts |
| Fable 5.1 | send out, "without the adjective yet" | position the paper against the ranking-dynamics literature (Iñiguez et al. 2022, Blumm et al. 2012), which reports the top as the most stable part |
| Opus 5.5 | send out, without enthusiasm | null worlds for every headline statistic, so that the automatic statistics are separated from the discoveries |

**What they agree on:**
- "Sustains" is the weakest word.
- "Slowly changing" is unquantified and contradicted by arithmetic shares.
- Eq. 1 and the boundary ratio are near-identities.
- The model is three versions under one name.
- About fifteen numbers in the text disagree with the outputs.

**What they credit:**
- the size-variance result;
- the gapped persistence;
- the registration discipline;
- the disclosure of the failures.

## 2. Verified findings

| # | claim | by | check | status |
|---|---|---|---|---|
| W1 | Fig. 3C's correlation is carried by the slide alone. Over the 60 cells, corr(obs, c − s̄) = 0.904, corr(obs, −s̄) = 0.971, corr(obs, c) = 0.108 | Opus | recomputed from `runs/2026-10-14_balance_law_halfyear/cells` | **CONFIRMED, with a fuller picture.** The crossing term sets the level: rms per 1,000 weeks is 2.89 for "no change", 3.96 for −s̄ alone and 1.13 for c − s̄. A ladder-wide constant gain does better (0.58) than the rank-specific c, which is expected if σ²/S is constant across ranks (§2z-bf). Mean c is 0.0040 against mean s̄ 0.0055: crossings return about three-quarters of the slide. The rank-by-rank variation is the slide's |
| W2 | The slide is measured against the re-selected top-K mean, so a driftless Gibrat world also "loses ground"; "every community loses ground" is close to definitional | Opus | `mts_balance_law.py`, slide against ΔU_K | **CONFIRMED in code.** The size in a null world is to be measured (R3-A) |
| W3 | Eq. 1 is a contemporaneous decomposition, not a prediction. The exact form is the Taylor-corrected registered form, derived post hoc | all three | `turnover_kinematics.py` uses the same pair's Δθ and Δ | **CONFIRMED.** Recast it as a decomposition: turnover is churn over slope |
| W4 | Eq. 1 is printed without its v_k term | gpt, Opus | draft line 79 | **CONFIRMED** |
| W5 | The model claims mix versions: the 30-week 0.097 and A6 are the frozen `movinghome`; the long gates are `movinghome_vp`; the flat start is the lasting core with measured slides | all three | `structure_oop.py` arms; GATE_LEDGER | **CONFIRMED** |
| W6 | A5c fails narrowly: 0.13048 − 0.09994 = 0.0305 > 0.03 | gpt | recomputed | **CONFIRMED.** Rescore it as FAIL by 0.0005 |
| W7 | Drift by rank raises held-out error (comments 0.100 → 0.130; submissions 0.175 → 0.183); RDM′ beats persistence on submissions in 2/5 splits | Opus | gate JSONs | **CONFIRMED** |
| W8 | The gate scores a top-200 cohort and recalibrates an observation-noise scale per split and arm (RDM′ submissions scales 1.0, 0.05, 0.05, 0.25, 0.0); not in Methods | Opus | `rankdiff_rdm.py` `oos_rdm`, `cohort_k=200` | **CONFIRMED** |
| W9 | The main-text model equations misdescribe the code. The code's lasting level is H; L is an OU deviation toward H with A's κ(z), σ_perm(z), coupled to H's shocks. "No home" is wrong for `movinghome`; the model is not minimal | all three | `_State.step` | **CONFIRMED** |
| W10 | The variance-preserving correction holds the one-step variance only; Var(L − H) and increments at h ≥ 2 rise, consistent with the 13-week overshoot (A5b-2 failed) | Opus, gpt | algebra re-derived by two reviewers | **CONFIRMED** (agrees with A5b-2) |
| W11 | D8's "risers/fallers" split is at the median, not the sign; the quintile means show risers persist about half as strongly as fallers | gpt, Fable, Opus | `momentum_decomposition.py` line 153 subtracts medians | **CONFIRMED.** "Among those that rose it is zero" is withdrawn |
| W12 | The momentum runners restrict the universe using full-panel membership, then select an early cohort; future information enters membership | gpt | `restrict_universe(..., buffer_mult=4)` without `member_window` | **CONFIRMED in code;** size unknown (R3-D) |
| W13 | Item counts are not participants | gpt | — | **CONFIRMED** (wording) |
| W14 | Gomez's ½(ζ − 1)ν² was applied where ζ < 1 (12 of 18 cells), outside its domain. The general expression is ½ f(q)q²/(p·ā_top)·ν². The "passing swings" explanation is untested | gpt, Opus | `gomez_decomposition.py` | **CONFIRMED.** A8-2 is withdrawn as evidence |
| W15 | The between term is non-negative by construction. The within term of a current-year top set includes regression to the mean (2019→20: −0.061 + 0.052, share unchanged) | Fable | code | **CONFIRMED** |
| W16 | "Slowly changing": arithmetic shares fall in all 18 cells; the claim has no statistic | Opus | `out_gomez_decomposition.txt` | **CONFIRMED** |
| W17 | The ranking-dynamics literature is uncited. Iñiguez et al. (2022) define rank change C(R) as the probability that the element at rank R changes between consecutive observations, and find the top most stable in most systems | Fable, Opus | read the arXiv text (S3.5) | **CONFIRMED.** Different statistic; reconcilable through the gap S/R (R3-B) |
| W18 | The stationary steepness in the code includes the entry term, σ²/(2(s̄ − entry)); the text drops it | Opus | `mts_balance_law.py` | **CONFIRMED** |
| W19 | Number errors: per-year rates written as two-year; "within 0.02–0.04" (these are MAEs); displacement 0.037–0.078; slope ranges; "a fifth or more"; Facebook half-year folded into the annual range; "body within 20%"; 10 seeds; the Methods' "no joint bootstrap"; "small composition term"; registration overstated; P7 names the frozen form; SI TODOs; nine uncited references; `fernholz2017` against Robert Fernholz | all three | each against outputs | **CONFIRMED** (each listed in §2z-dh) |

## 3. Plan

Expectations go in `runs/2026-10-18_pnas_push/PLAN.md` addendum R3, before
the runs.
- **R3-A, null worlds.** Every headline statistic in four worlds, to
  separate automatic statistics from discoveries:
  - (i) driftless Gibrat in a re-selected top-K;
  - (ii) Atlas with firm-like β = 0.2;
  - (iii) neutral copying;
  - (iv) the data-calibrated model.

  The statistics: turnover by k and Eq. 1, the boundary ratio by cut-off,
  the slide and the crossing gain, β, gapped persistence, Iñiguez's C(R).
- **R3-B, rank change C(R)** on the three ladders against top-k set
  replacement, reconciled by the gap S/R.
- **R3-C, one model.** Rerun A6 with `movinghome_vp`; flat start with 20
  seeds and bands; a specification table; drop the frozen 0.097, or label
  it.
- **R3-D, persistence.** Prefix-only membership; a split at the true sign
  with conditional means; matched origins for the factor comparison.
- **R3-E, Gomez.** The general displacement formula, used only where it is
  valid; a noise baseline; permanent-rank cohorts.
- **R3-F, text.** A retitle; Eq. 1 as a decomposition; the near-identities
  stated; the model equations corrected; the gate disclosures; the
  literature; every W19 number; P7 updated to the corrected form (the
  protocol is a draft, so the owner adopts).
