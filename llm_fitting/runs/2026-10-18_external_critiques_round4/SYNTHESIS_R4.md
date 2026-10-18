# Round-4 referee synthesis: manuscript v2 (three models)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-19._
- **Brief:** `CRITIQUE_PROMPT_R4.md` (6590c29, before the reviews).
- **Reports**, saved verbatim in this folder: gpt-6-astra (Codex CLI
  0.163.0); Claude Fable 5.1; a fresh Claude Opus 5.5.
- **Manuscript reviewed:** `paper/pnas_draft.tex` at 6590c29.
- **Record:** MODEL_STATUS §2z-dj.

A claim below is CONFIRMED only after this session rechecked it.

## 1. Verdicts

| reviewer | decision | single most valuable change |
|---|---|---|
| gpt-6-astra | send out, without enthusiasm | resolve the maintenance counterfactual: diffusion, replacement and exit separated, with bands, and an explanation of the comments counterfactual's opposite sign |
| Fable 5.1 | send out, "still without the adjective" | null worlds at 20 seeds with bands; a size-blind world from the data's ladder; a planted-β calibration; drop the flattening balance from the discoveries |
| Opus 5.5 | send out, without enthusiasm | rebuild the null worlds (20 seeds, stationary and data-ladder starts, the data's passing parts, a heterogeneous-drift world) and rewrite the ledger from them |

**What they agree on:**
- "Holds up" is the weakest word, and "durable" the second.
- "Crossings fall short of the slide" is not a discovery.
- The null worlds are single seeds from a Zipf start.
- β is compared with firms on an attenuated estimator.
- The abstract's causal clause linking persistence to the head's fall is
  unsupported.
- The Gomez "selection" check uses the start block.
- About fifteen text, SI and figure errors.
- Reviewer access to the CrowdTangle exports is not supported by the
  documented terms.

**What they credit:**
- persistence (all three: the discovery that survives everything);
- weak size-dependence of volatility, Facebook especially;
- the C(R) reconciliation;
- the registration record.

**Opus's reframing:**
- Durability is a separation of time scales: names cross boundaries within
  a year, while the shape relaxes over 2–22 years.
- The three supporting facts (crossings return three quarters of the
  slide; the flat start is flatter than observed; the hierarchy flattens)
  are one inequality, c/s̄ = S*/S.

## 2. Verified findings

| # | claim | by | check | status |
|---|---|---|---|---|
| F1 | Two-year counterfactual, comments: churn + slide moves the K/64 shape −0.080 a year; slide only −0.038. "Without churn it collapses" is false there | gpt, Opus | `churn_counterfactual__reddit_comments_ext.json` | **CONFIRMED.** The mechanism is in the simulation's own slide: 0.0029 a week with churn, 0.0006 without. The slide is measured against the re-selected U_K, so it contains the universe's crossing gain c_K. Churn's net effect on U_k − U_K is c_k − c_K, which is negative at the comments head. Long-run runs answer the question the title asks (R6, §3) |
| F2 | "Crossings fall short of the slide" appears in three of four null worlds (atlas 0.0027/0.0048; neutral 0.0044/0.0048; model 0.0030/0.0037) | all three | `null_world__*.json` | **CONFIRMED.** It is the observed ladder's position (steeper than its own balance), not a mechanism. c/s̄ = σ²/(2S s̄) = S*/S, so "three quarters", "flatter by 8–46%" and "flattening" are one fact (Opus; algebra checked) |
| F3 | The null worlds are single seeds; top-10 turnover rests on 1–10 of 30 name-years | Fable, Opus, gpt | `null_worlds.py` | **CONFIRMED.** R7: 20 seeds |
| F4 | The size-blind world starts from a Zipf ladder (S = 1), and its turnover is flat beyond k = 25 (0.23–0.25) | Fable, Opus | `null_world__gibrat.json` | **CONFIRMED.** R7: calibrated worlds from the data's ladder |
| F5 | β: the estimator turns a planted 0.2 into 0.114. The model's structure-function β on comments is 0.126, against the SI's lasting 0.047 and the gate windows' 0.056–0.092 | all three | `out_r6_long30.txt` (β +0.126 comments, +0.045 submissions); `null_world__atlas.json` | **CONFIRMED.** R7: planted-β ladder |
| F6 | Expectations scored PASS that failed as written: NW-2 row 2 (neutral 0.578), NW-4 (firm-like 0.114 < 0.12), NW-5 (three worlds below −0.05) | gpt | PLAN R3 against RESULTS R3-A | **CONFIRMED.** Corrected in RESULTS and §2z-dj |
| F7 | The abstract's "persistence … cut the share" is contradicted by the record: the June 2021 head step is in votes per post (§2z-ca), while persistence is in item counts | Opus, gpt, Fable | §2z-ca table; §2z-dg A4 | **CONFIRMED.** R8 splits the head's share |
| F8 | Gomez "permanent-rank cohorts" use the start block (`gomez_r3.py`:53) | gpt, Opus | code | **CONFIRMED.** A prior-year cohort (`gomez_r7_prior.py`) makes the members' term larger: submissions head −0.253 and −0.307 against −0.237 and −0.259 |
| F9 | "The size expected from selecting the year's leaders on noise" was never computed | Fable, Opus, gpt | RESULTS R3-E | **CONFIRMED.** R7 computes the Gomez terms in every world |
| F10 | "Errors within 0.03 of each other" against a 0.0305 spread | all three | GATE_LEDGER | **CONFIRMED** |
| F11 | "Moves … in one direction" is false for comments' mean-log shape. Annual primary cells: −0.047 … +0.030 and +0.025 … +0.062 a year | Fable, Opus | `law__reddit_comments_ext.json` | **CONFIRMED.** Only the arithmetic shares fall everywhere |
| F12 | Head share: 34.0% (all communities) against 0.356 (the model's tracked universe) | Fable, Opus | `head_named` against `structure_oop` | **CONFIRMED.** Both are defined in the text |
| F13 | "Excludes neutral copying" rests on the k = K cell. Neutral by cut-off: 0.76, 0.44, 0.54, 0.82, 1.04, 1.17, 0.12. Gibrat and Atlas are also structured by cut-off (0.53–1.39) | Opus | `null_world__*.json` boundary_by_k | **CONFIRMED.** Only jumps to random ranks (1.56) are robustly excluded |
| F14 | "Rank one rarely changes" fails for Facebook (52% of weeks) | gpt, Opus | `rank_change_data.json` | **CONFIRMED** |
| F15 | Stale SI: the "platform aggregate" row holds cohort-mean values; the risers/fallers row is the median split; the Gomez Pareto paragraph; SI's stationary form omits e_k; "60 year-long intervals" | all three | SI against `persistence_r3__*.json` | **CONFIRMED** |
| F16 | Items "+0.14 to +0.19": comments at gap 26 is +0.125 | Fable, Opus | SI table | **CONFIRMED** |
| F17 | The model world (iv) uses the hold closure, so its ladder is an input. Its K/2 turnover is 0.14 against the data's 0.18 | Fable, Opus | `null_worlds.py`:112 | **CONFIRMED** |
| F18 | Rate law: σ² is one estimate per stratum, the same in every pair, so c is constant over time by construction | Opus | `law__*.json` (identical sigma2 across pairs) | **CONFIRMED.** Stated in the text |
| F19 | Composition term C: +25–35% on Reddit, from persistent drift; the pooled 0.94 combines opposite-signed terms | Opus | §2z-da §4 | **CONFIRMED as description.** Stated |
| F20 | Gibrat in the mean is rejected: drift by rank −0.0103 a week at the head against −0.0036 at K (submissions) | Opus | `out_r6_long30.txt` | **CONFIRMED.** "Gibrat's law" means the second moment |
| F21 | Data availability: reviewer access to the CrowdTangle exports is not supported by the documented terms; Reddit acquisition and current terms are undocumented | all three | `DATA_AVAILABILITY_NOTES.md` | **CONFIRMED.** The statement is revised |
| F22 | Figures: 5A legend "Facebook (26-wk)" against 4-week windows; 3C title "Turnover = churn / slope"; 2C title; 4C without bands; 4D internal label; the generator's default calls the old fig2/fig3 | gpt, Fable, Opus | `make_pnas_figures.py` | **CONFIRMED** |
| F23 | Missing citations: Sangiorgio et al. 2024 (Gibrat-type growth of Facebook news pages); Huberman & Adamic 1999; Ratkiewicz et al. 2010; Riccaboni et al. 2008; TeBlunthuis & Hill 2022; `fernholz2017` uncited; Baker & Smyth's names | gpt, Opus | web | **CONFIRMED** (arXiv, journal pages) |
| F24 | P5's second change (2024 Q1 − 2023 Q1) may span the archive's 2023 source switch and the June 2023 blackout | Opus | protocol text | **For the owner** (protocol; bridge sealed, not inspected) |

## 3. Plan (expectations in `runs/2026-10-18_pnas_push/PLAN.md` before each run)

- **R6, the long-run counterfactuals.** From the observed ladder, 30 years,
  20 seeds, in three worlds: churn + slide, slide only, churn only.
- **R7, null worlds.**
  - the four R3 worlds at 20 seeds;
  - calibrated size-blind and firm-like worlds from the model fitted to the
    data (the data's ladder, passing parts and exits);
  - a planted-β ladder;
  - the Gomez terms in every world, as the noise baseline;
  - in the data, the Gomez decomposition with a prior-year cohort.
- **R8, the head's share split exactly** into item share and relative
  karma per item.
- **Text.**
  - Lead with the time-scale answer and the balance.
  - Present c/s̄ = S*/S as one fact.
  - Correct the ledger of what is automatic and what is discovered, from R7.
  - Remove the causal clause unless R8 supports it.
  - Correct every F-item.
  - Retitle so that the title is on the owner's question and earned by R6.
