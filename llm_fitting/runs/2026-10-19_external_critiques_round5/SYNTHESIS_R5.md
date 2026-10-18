# Round-5 referee synthesis: manuscript v3 (three models)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-19.
- **Brief:** `CRITIQUE_PROMPT_R5.md` (c203b99, before the reviews).
- **Reports**, saved verbatim in this folder: gpt-6-astra (Codex CLI
  0.163.0); Claude Fable 5.1; a fresh Claude Opus 5.5.
- **Manuscript reviewed:** v3 at 576a8e4.
- **Record:** MODEL_STATUS §2z-dk.

A claim below is CONFIRMED only after this session rechecked it._

## 1. Verdicts

| reviewer | decision | single most valuable change |
|---|---|---|
| gpt-6-astra | send out, without enthusiasm | a matched comparison including heterogeneous community growth and common shocks, with the distinguishing result at the abstract's centre |
| Fable 5.1 | send out, "still without the adjective" | the balance net of the universe's own crossing gain and by period; rerun the 30-year runs with drift from the earliest and latest cells |
| Opus 5.5 | send out, without enthusiasm; risk of desk rejection with a social-science editor | split the slide into its mechanical part and the excess, and test whether the excess is carried by persistent decliners. This unifies the paper's mechanism and its discovery (the GLLM fast-transition point) |

**What they agree on:**
- "Holds up" is still the weakest word. All three propose "outlasts its
  members", or a variant.
- The paper's answer is largely automatic in random-growth theory.
- Persistence is the discovery, but "every world" is a set of homogeneous
  worlds.
- The size exponent is not on one footing.
- New errors in the v3 rewrite.

**What they credit:**
- the honesty of v3;
- the reproducibility of every recomputed number;
- the participation split, which is undersold;
- rank stability against share collapse.

## 2. Verified findings

| # | claim | by | check | status |
|---|---|---|---|---|
| G1 | With entry and exit, S*/S = c/(s̄ − e), not c/s̄ | gpt | 60 half-year cells | **CONFIRMED.** Pooled c/(s̄ − e) = 0.64 (comments 0.68, submissions 0.61), median per cell 0.70. Gross c/s̄ = 0.72 |
| G2 | The crossing gain is not constant across years "by construction"; only its variance input is | gpt | `mts_balance_law.py` | **CONFIRMED.** Text corrected |
| G3 | The relaxation times are mislabelled: at the head, 3–4 years on submissions and 9 on comments (linearized, slides fixed). The model relaxes much more slowly (τ ≈ 18–27 years at the head) | gpt, Opus | §2z-cp table; R6 | **CONFIRMED** |
| G4 | The submissions head shows no separation of time scales: relaxation of 3–4 years against membership e-folding of about 4–5 years | Opus | turnover, §2z-cp | **CONFIRMED.** The text now says the hierarchy outlasts its members on comments, not at the submissions head |
| G5 | Gross accounting counts the universe's crossing gain c_K on both sides. Net: c_k − c_K is +0.0014 against a member decline of +0.0020 on comments; −0.0004 on submissions; −0.0009 to −0.0019 a week at the submissions head | Fable | cells | **CONFIRMED.** Turnover holds up the comments hierarchy and added to the submissions head's decline. Stated in text |
| G6 | The 30-year runs take drift from the 2020→21 and 2021→22 slides only | Fable, Opus | `churn_counterfactual.py` | **CONFIRMED in code.** Not rerun with other periods. Stated as a limitation |
| G7 | e_k may be counted twice: the drift is built from s̄ − e while the simulator has its own exits | Fable | code | **PLAUSIBLE, not resolved.** Stated |
| G8 | Results 2: in the calibrated worlds the top ten turn over no faster than the top 25. The data's top ten are above every world's 95th percentile, and the data's K/2 is above the worlds' bands | Opus, Fable | `null_worlds_r7_summary.json` | **CONFIRMED.** R9 (geometry isolated, identical dynamics): K/64 − K/2 rises by 0.061 (0.031–0.091) from the data's ladder. Text restated at K/64 against K/2 |
| G9 | Persistence is "against every world", but every world is homogeneous. Heterogeneous mean growth reproduces it (SI synthetic check, +0.17 to +0.19) | all | SI | **CONFIRMED.** Reframed: the statistic detects heterogeneous growth |
| G10 | Head split: the shares are relative, so the denominator may drive them | Opus | `head_components.py` with absolute counts | **CHECKED.** The submissions head's posts fell from 33M (2019) to 17M (2022), while the top 5,000's rose from 149M to 197M: a real fall. The comments head grew 21% against 61% for the top 12,500: the denominator matters there |
| G11 | The prior-year Gomez decomposition is not exact | Opus | arithmetic | **CONFIRMED.** Stated |
| G12 | Post hoc labels: R5–R8 are unlabelled in the main text; SI l. 119 says the expectations were "committed before the run" | all | git | **CONFIRMED.** Corrected |
| G13 | Abstract: "not explained by the 2020 surge or a 2021 change in capture", "forms the hierarchy", "15–33%" | Opus | text | **CONFIRMED.** Rewritten |
| G14 | β: estimators disagree; no intervals; Fig. 2A shows raw β against firms; Facebook's structure-function 0.11 omitted | all | SI; RESULTS R7 | **CONFIRMED.** Calibration line and recovery in SI; all estimators reported; Fig. 2A labelled raw |
| G15 | The structure-function β may be inflated by heterogeneous drift | gpt, Opus | estimator algebra | **PLAUSIBLE, not tested.** Stated in SI |
| G16 | "Recovers within about 0.03" overstates six Facebook-calibrated replications | all | `recovery_output.txt` | **CONFIRMED.** Restated |
| G17 | Membership robustness "< 0.01" is too broad | gpt, Opus | SI | **CONFIRMED.** Restricted to the raw statistic |
| G18 | Fig. 5A crosses are single-seed R3 worlds; the caption is wrong | all | code | **CONFIRMED.** Redrawn from R7, all eight worlds |
| G19 | SI title v2; slide-only "0 (top 1,560)"; 83 against 82 evaluations; "every number checked"; "registered" for the bridge; the claim that P5 excludes both confounders | all | SI | **CONFIRMED.** All corrected |
| G20 | Methods' "disjoint cohorts" does not hold for turnover and current-year Gomez | Opus | code | **CONFIRMED.** Qualified |
| G21 | Neutral copying "between K/16 and K/2 like a diffusion": K/16 gives 0.68 | Opus | JSON | **CONFIRMED.** Now K/8–K/2 (0.85–1.24) |
| G22 | Citations: Pal & Pitman 2008, Gaffney & Matias 2018, de Menezes's relative fluctuations, Sangiorgio's claim | gpt, Opus | web | **CONFIRMED** (journal pages). Added or corrected |
| G23 | P6 cannot fail under firm-like volatility (raw 0.094 ≤ 0.12); P5 spans the 2023 source switch and the blackout | gpt, Opus | protocol | **CONFIRMED.** For the owner: protocol amendment before adoption |
| G24 | RESULTS R5: "memes and dankmemes alone half" | gpt | head_named | **CONFIRMED wrong:** 34% of gross loss. Corrected |
| G25 | R7's prior-cohort Gomez terms in the worlds are nonsense (rdm +231,412) | Fable | summary | **CONFIRMED.** Not used; noted |
| G26 | Terminology: "churn" (figures), "volatility" (text), "turnover" (title) | Opus | figures | **CONFIRMED.** Figures now say volatility |

## 3. New analyses (expectations in PLAN.md before each run)

- **R9, geometry isolated.** Identical calibrated dynamics; the data's
  ladder against a constant-slope top K. All three expectations held.
- **R10, the slide by persistence class.** Is the slide's excess over a
  driftless world carried by prior decliners? Opus's proposal. Results in
  RESULTS R10 and §2z-dk.
