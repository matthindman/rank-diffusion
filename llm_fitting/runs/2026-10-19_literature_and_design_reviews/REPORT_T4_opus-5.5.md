<!-- Provenance: Task 4 (one model or many) report by Claude Opus 5.5 (claude-opus-5-5) (Claude Code sub-agent, default effort, WebSearch/WebFetch; returned as text because sub-agents may not write report files), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

## Summary

**Recommendation:** take the "all of the models" approach only in a limited form, built mostly from runs that already exist.

- Keep the calibrated model for the counterfactuals, but present it as one column of a short table of facts against models. The rows fall into four groups: facts every model reproduces, facts that tell models apart, facts every model fails, and ties.
- Add one new column: a calibrated world with heterogeneous mean growth (the hybrid Atlas model the paper already cites).
- Put every model the program fitted in an SI "model atlas".
- Write forecasts from every column into the sealed 2023–24 protocol. This is the owner's decision.

**Three reasons:**
1. **The paper's claims are already comparative.** Its strongest results (geometry, persistence, the exclusion of copying and jumps) came from comparing worlds. The hero model ties simpler models on held-out movement.
2. **Precedent.** Credible multi-model work (Cramer, den Boon, Grimm) states inclusion rules in advance, reports every model and tests several patterns. Confusing work left "reproduces" undefined.
3. **The missing column is the direct test of the paper's discovery.**

**Path:** `/Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-19_literature_and_design_reviews/REPORT_T4_opus-5.5.md` was **not written**. The Write tool refused in this sub-agent context with "Subagents should return findings as text, not write report files", so the full report is below for the caller to save there.

---

# Report T4: one hero model, or all of the plausible models?

**Provenance.**
- Model: Claude Opus 5.5 (`claude-opus-5-5`), running as a Claude Code sub-agent at default reasoning.
- Tools: read-only `Read` and `Bash`, `WebSearch`, and `WebFetch` (which extracts text through a helper model). For scanned PDFs that WebFetch saved, I read the page images directly.
- Date: 2026-10-19. Manuscript reviewed: v4 at commit 42955ac.

## Access check

**Repository files read in full:**
- `COMMON_BRIEF.md`, `T4_MANY_MODELS.md` and `00_PRACTICES_ADOPTED.md`.
- `paper/pnas_draft.tex` and `paper/pnas_si.tex`. I read the source, not the PDFs; line numbers below refer to the `.tex` files.
- `runs/2026-10-12_ladder/DESIGN.md` and `explainer_text.json`.
- `explainer.html`, visible text only, with scripts and SVG removed.
- `null_worlds_r7_summary.json`, with headline statistics extracted for all ten worlds.
- `RESULTS.md` sections R3-A/B, R7 (including the model world and the β calibration), R8, R9 with its correction, and R10.
- `PLAN.md` in full, including addenda R3–R11.
- `GATE_LEDGER.md` (all 83 rows), `PROGRAM_STATE.md` and `SYNTHESIS_R5.md`.
- The keys in `references.bib`.

**Repository files read in part:** `MODEL_STATUS.md`, only the §2z-dd excerpt on drift by name (lines 11870–11938) and grep hits for §2z-cv and §2z-dc.

**Figures viewed:** fig2_size_blind, fig4_model and fig5_tipping. I did not view Figs. 1 and 3.

**Deliberately not opened:**
- The R11 outputs (`*__noentry.json`, `out_r11_*.txt`). Commit 50e36aa says they are not yet read or scored.
- Anything listed in `SEALED_PATHS.json` or any raw data.
- Nothing outside the repository except this session's own fetched web files.

**Web sources, by access:**
- **Full text read at the cited pages:**
  - Levins 1966, pp. 422–423 (scan)
  - Orzack & Sober 1993, pp. 533–534 (scan)
  - Grimm et al. 2005, pp. 987–989
  - Clauset et al., arXiv v2, pp. 1 and 18–19
  - GLLM 2016, p. 2071
  - Steegen et al. 2016, pp. 702–703
  - Auspurg & Brüderl 2021, p. 1
  - Windrum, Fagiolo & Moneta 2007 (JASSS HTML)
- **Full text read on PubMed Central (PMC):**
  - Cramer et al. 2022
  - Salganik et al. 2020
  - Breznau et al. 2022 (abstract and significance statement only)
  - Broido & Clauset 2019
  - Holme 2019
  - den Boon et al. 2019
  - Engzell 2023
- **Abstract only:** McGill et al. 2007, Del Giudice & Gangestad 2021, Yao et al. 2018, Abramowitz et al. 2019, Eyring et al. 2016, Acerbi & Bentley 2014.
- **Secondary:** the rest, labelled in the reference list.

**Quotation policy.** Under this session's copyright limit, this report contains one verbatim quotation (Levins). Every other source appears as a labelled paraphrase in my own words, with its location and whether I checked it against the full text. Take exact wording from the cited location.

---

## 1. Should the paper take an "all of the models" approach?

### The strongest case for it

1. **The argument is already comparative.**
   - Every main claim is "automatic or not" relative to a set of worlds (SI, "What is automatic" and "What is not").
   - The most defensible results came from the comparisons, not from the hero model:
     - the turnover order as geometry: R9, +0.061 (0.031–0.091);
     - persistence against homogeneous worlds: −0.02 to −0.10, against +0.14 to +0.20 in the data;
     - exclusion of jumps (1.56) and of neutral copying at the universe edge (0.69).
   - The hero ties simpler arms on held-out movement (SI table; GATE_LEDGER rows 101–114). Lower is better:

     | | hero | fixed home | fixed home + drift | wandering home, no drift | carrying forward |
     |---|---|---|---|---|---|
     | submissions | 0.183 | 0.188 | 0.171 | 0.175 | 0.163 |
     | comments | 0.130 | 0.123 | 0.114 | 0.100 | 0.167 |

   - What distinguishes the hero is its use for counterfactuals (Fig. 4A–C). Any calibrated member of the homogeneous random-growth class could play that role.
2. **The data are fixed and have been looked at many times.** Everything since round 3 is post hoc, and the gate ledger records 82 evaluations. Showing every model fitted, with its failures, answers the cherry-picking worry.
   - The COVID-19 Forecast Hub scored every eligible model against a naive baseline (Cramer 2022).
   - The WHO-convened guidelines advise against any minimum-performance bar for inclusion (den Boon 2019, Table 1).
3. **One pattern cannot pick a mechanism.**
   - Many stochastic processes produce a stationary power-law cross-section (Windrum et al. 2007 §5.2, citing Brock 1999).
   - In ecology, few species-abundance models are ever rejected, because each predicts only the abundance curve (McGill 2007 abstract).
   - Pattern-oriented modelling filters rival models through several patterns at different scales (Grimm 2005, pp. 987–988).
   - The paper already has about a dozen patterns; a matrix makes the joint filter visible.
4. **Failures are diagnostic.**
   - Some failures are shared by every model and point to missing ingredients: top-10 turnover of 0.33, the fall of the head's share, and kurtosis of 22.
   - The hero's own failures follow from its theory:
     - its flatter balance is the statement c/(s̄ − e) = S*/S < 1;
     - its slow transitions are GLLM's result for homogeneous random growth (2016, p. 2071).
5. **The sealed 2023–24 data are the only prospective test left.** If every column registers forecasts, every outcome is informative, not a pass/fail for one model.

### The strongest case against it

1. **The question is wrong for the paper.** The paper asks where the durable shape comes from, not which model wins. On prediction the honest answer to "which model wins?" is "none clearly", which reads as a negative result and can bury the measurement contribution.
2. **Choice does not disappear; it moves.**
   - The fight shifts to which rivals, which facts, and what counts as "reproduces".
   - Broido & Clauset applied one procedure to every network, yet the reply still turned on definitions: Holme 2019 argues for agreeing on a precise definition first.
   - Including non-equivalent alternatives can mislead (Del Giudice & Gangestad 2021 abstract).
   - When the authors build every rival themselves, a straw-man worry replaces the cherry-picking worry.
3. **The worlds are not independent.**
   - The calibrated, model and R10 worlds share the estimator, passing parts, exits and seeds.
   - In R9 they share everything except the starting ladder.
   - Abramowitz et al. 2019 (abstract) warn that equal treatment of dependent models biases ensemble inferences.
   - Orzack & Sober (pp. 533–534) note that robustness relies on a model independence Levins never defined.
4. **Space.** The main text must go from 8 pages to 6–7. A full atlas cannot fit, and half an atlas is worse than none.
5. **Exposure.** Every fact was seen before any new rival is added. Only the sealed test can be pre-declared.
6. **Cost and mess.** The one calibrated heterogeneous-growth attempt (§2z-dd, "drift by name") shows how a rival can fail in ways that need explaining:
   - it reached +0.179 against the data's +0.373 on the adjacent-block statistic, which the averaging null inflates;
   - transported to comments, head turnover exploded;
   - the head share was 0.730 against 0.520;
   - the two-year retention error was 0.19 against 0.075 without it.

### Recommendation

**Adopt it only in a limited form, mostly by re-presenting what exists.** The owner's three objections:

- **Choice is unavoidable.** Agreed. State a selection rule before any new scoring. Include each rival tradition's signature statistic, and report all columns and rows.
- **The format fits papers badly.** True of a full atlas. A table of facts against models is a familiar ecology and complex-systems form and costs about a quarter page.
- **Reviewers are unfamiliar with it.** Frame it as a null-model or pattern-oriented test, not an "ensemble". No weights, averaging or rankings.

Keep the hero, recast in role as "the calibrated random-growth model": one column among others, used for the counterfactuals only a generative model can supply.

---

## 2. Precedents

### Multi-model comparisons and how they were received

**CMIP6** (Eyring 2016, *GMD*)
- **Design:** dozens of climate models run common experiments.
- **Reception:** institutional and standard. Model dependence is a standing critique (Knutti 2010; Abramowitz 2019).
- **Lesson:** common baseline runs are the condition of entry ("entry cards", Eyring abstract), with harmonized inputs and outputs. It confuses when dependent models count as independent votes.

**ISI-MIP** (Warszawski 2014, *PNAS*) [secondary]
- **Design:** impact models with common climate inputs.
- **Lesson:** consistent inputs.

**COVID-19 Forecast Hub** (Cramer 2022, *PNAS*)
- **Design:** 27 models, an ensemble and a naive baseline, run prospectively.
- **Reception:** highly visible.
- **What made it credible** [paraphrase; checked against the PMC text]:
  - each team designated one scored model in advance (Methods, "Forecast Model Eligibility");
  - eligibility rules were stated before scoring;
  - a baseline was included;
  - the ensemble was rarely the single best, but consistently near the top, and was the only model to beat the baseline in every location (Discussion).

**Fragile Families Challenge** (Salganik 2020, *PNAS*)
- **Design:** 160 teams, one holdout, one metric.
- **Reception:** the negative result was the finding.
- **Lesson** [paraphrase; checked]: the best predictions were only slightly better than a simple benchmark (abstract), and every team was scored by the same held-out metric (Introduction).

**Many-analyst studies** (Silberzahn 2018; Breznau 2022, *PNAS*)
- **Design:** 29 and 73 teams analysing one data set and one hypothesis.
- **Results:** in Breznau's abstract, more than 95% of the variance in results is unexplained [paraphrase; checked].
- **Reception:** high impact, with pushback.
  - Engzell 2023 argues that the teams in effect tested different hypotheses.
  - Auspurg & Brüderl 2021 (p. 1) trace Silberzahn's spread to an unclear research question and call for precise estimands.
- **Lesson:** it confuses when the apparent uncertainty is really different questions.

**Multiverse and specification-curve analysis** (Steegen 2016; Simonsohn 2020)
- **Reception:** widely adopted; Del Giudice & Gangestad 2021 warn against including non-equivalent options.
- **Lesson** [paraphrase; checked]: a multiverse shows how far conclusions move under arbitrary processing choices, and which choices matter (Steegen p. 702).

**Power-law alternatives** (Clauset 2009; Broido & Clauset 2019)
- **Reception:** the method is standard; Broido & Clauset triggered a dispute over definitions (Holme 2019).
- **Lesson** [paraphrase; checked]: one procedure was applied to every data set equally (Broido & Clauset, Results, "Preliminaries").

**Neutral versus niche** (McGill 2007; Acerbi & Bentley 2014)
- **Reception:** McGill 2007 is a field-defining synthesis.
- **Lesson** [paraphrase of abstracts]: few species-abundance models are ever rejected because each predicts only one curve (McGill). Departures of turnover from the neutral model reveal biases in cultural transmission (Acerbi & Bentley).

**Pattern-oriented modelling** (Grimm 2005, *Science*)
- **Design:** rival theories filtered by several patterns.
- **Reception:** widely used in ecology. Lessons below.

**Robustness analysis** (Levins 1966; Orzack & Sober 1993; Weisberg 2006)
- **Reception:** influential, contested, then defended. Lessons below.

**Gabaix, Lasry, Lions & Moll 2016** (*Econometrica*)
- **Design:** a canonical model, its failure, then two minimal deviations.
- **Lesson:** a benchmark that fails a named fact, followed by the smallest fix. This is the template I recommend.

**Stacking** (Yao et al. 2018)
- **Lesson** [paraphrase of abstract]: Bayesian model averaging is unreliable when the true process is not among the candidates, and stacking predictive distributions is preferred. That requires predictive distributions that actually separate the models.

**Multi-model guidelines** (den Boon 2019, *BMC Medicine*)
- **Design:** a WHO-convened consensus.
- **Lesson** [paraphrase of Table 1; checked]:
  - pre-specified inclusion and exclusion criteria;
  - no minimum performance bar for inclusion;
  - each model's results shown with its own uncertainty;
  - a display of the spread across models is usually better than a pooled estimate.

### What separated credible from confusing

1. **A common protocol and inputs fixed before scoring** (CMIP, den Boon, Cramer).
2. **Everything reported per model.** Every eligible model and a baseline, results per model, not pooled (Cramer, Salganik, den Boon).
3. **One estimand per comparison** (Auspurg & Brüderl; Engzell). The paper already computes every statistic with one piece of code in data and simulations (SI line 226).
4. **Several patterns** [paraphrase; checked].
   - A single pattern at one scale leaves model structure undetermined (Grimm p. 987).
   - Models that fail the characteristic patterns are rejected, and more falsifying patterns then separate the survivors (Grimm p. 988).
   - A large enough jointly matched set of stylized facts narrows the candidate processes even without identifying one model (Windrum §5.2).
5. **"Reproduces" defined in advance**, the lesson of the Broido–Clauset dispute.
6. **Ties treated as information** [paraphrase; checked]. Clauset (p. 19) says the test can show when the data favour neither hypothesis, and that choosing the reasonable hypotheses is the researcher's job. Salganik made a tie with a benchmark the headline.
7. **Robust results reported for what they are.**
   - Levins (p. 423) treats one problem with several models that share a core assumption: "Hence our truth is the intersection of independent lies" [verified: https://www.zoology.ubc.ca/let/pdfs/Levins1966.pdf, p. 423].
   - Orzack & Sober object that robustness is undefined and depends on an undefined independence (p. 533 abstract; p. 534) [paraphrase; checked].
   - **For this paper:** statistics every homogeneous world reproduces are robust theorems of the shared assumption, about level diffusion, not about the data's mechanism. Statistics that split the worlds are the evidence.

---

## 3. Design options, ranked

### 1. (a) Main-text table of facts against models
- **What it shows:** which results are automatic, which are evidence, and where the frontier is. Rows are grouped as reproduced by every model, discriminating, failed by every model, and ties.
- **Space:** about ¼ page.
  - **Pay for it by:** moving Fig. 2A to the SI, trimming line 116, and deleting world numbers from the text.
  - **Net cost:** about 0.
- **Compute and authoring:**
  - 1–2 days to write.
  - Most cells already exist (sketch below).
  - One new column: about one R7-style world at 20 seeds.
- **Referee risk:** low to moderate. The fight moves to the cell thresholds.
- **What the program lacks:** a written selection rule, the heterogeneous column, and percentile codes for the cells.

### 2. (d) Every column registers forecasts for the sealed 2023–24 test
- **What it shows:** a genuinely prospective comparison, including failures shared under the 2023 shocks.
- **Space:** one Discussion sentence.
- **Cost:** 1–2 days to amend the protocol; the forecasts are cheap, from parameters frozen on 2019–22.
- **Referee risk:** low.
- **What the program lacks:**
  - owner adoption of an amended protocol (P5 needs amending anyway, SI line 187);
  - a rule for each row;
  - baselines.

### 3. (b) SI model atlas
- **What it shows:** every model fitted, with its tradition, calibration, target, failures, gate count and record section. Full disclosure.
- **Space:** none in the main text.
- **Cost:** 2–3 days of compiling.
- **Referee risk:** low.
- **What the program lacks:** only the compilation.

### 4. (e) Specification curve for gapped persistence, the prior-decliner contrast and the turnover order
- **What it shows:** robustness to K, metric, presence threshold, window, gap and membership.
- **Space:** none in the main text (SI figure).
- **Cost:** 1–2 days.
- **Referee risk:** low, if only equivalent choices are included.
- **What the program lacks:** a declared grid. Parts exist: R3-D, R4, R4b, §2z-da.

### 5. (f1) Rival columns implemented independently by Codex
- **What it shows:** answers the straw-man worry. Codex implements the rivals from published specifications, blind to the cell results.
- **Space:** a Methods clause.
- **Cost:** moderate.
- **Referee risk:** low.
- **What the program lacks:** a queue item.

### 6. (c) The ablation ladder
- **What it shows:** which components of the *earlier* frozen permanent–transitory stacks matter; noise and home carry 86%.
- **Space:** SI only, or omit.
- **Cost:** none; it is done.
- **Referee risk:** high in the main text.
  - It belongs to a different model generation.
  - It uses an in-sample macro index plus the gate, not the paper's facts.
  - Its composite index would be a second scoring system.
  - Of its ten predictions, three failed and two held only partly.
- **Note:** Fig. 4D is already the relevant ablation.

### 7. (f2) "Fingerprint" heat map
- **What it shows:** the data's percentile within each world, as a heat map.
- **Space:** about ⅓ page.
- **Cost:** the same as (a).
- **Referee risk:** moderate.

### Not recommended
- **Composite scores or rankings.** The weights become the argument.
- **Bayesian averaging or stacking.**
  - The models are simulators scored on summaries, and the gate cannot separate the arms: split SD is 0.03–0.06, as large as the differences.
  - The weights would be noise.
  - The claims are mechanistic.

### Sketch of the main-text table from existing outputs

Sources: the SI null-worlds section; `null_worlds_r7_summary.json` (20 runs each); RESULTS R3, R7, R9 and R10; the SI gate and out-of-period tables; §2z-dc. These are values, not final cell codes. Code each cell by the data's position in the world's 5–95% band.

**Columns:**

| column | world |
|---|---|
| A | size-blind growth, constant slope |
| B | firm-like growth (β = 0.2), constant slope |
| C | neutral copying |
| D | size-blind growth, data's ladder |
| E | firm-like growth, data's ladder |
| F | the calibrated model (its ladder is an input) |
| G | heterogeneous growth (synthetic, 5 seeds; a calibrated version is still to run) |
| H | jumps to random ranks |

In every row below, "Not computed" covers the remaining columns.

**Facts every model reproduces (automatic)**

| fact | values | data |
|---|---|---|
| Eq. 1 cells in band | A, C, F 100%; B 92%; D, E reproduce (✓) | 100% |
| crossings short of the slide, c/s̄ | A 1.11, B 0.51, C 0.82, D 1.21, E 0.62, F 0.75 | 0.72 |
| rank-one occupant stable, C(1) | A 0.27, B 0.27, C 0, D 0.09, E 0.12, F 0.10 | 0.07 (Facebook 0.52) |

- Not computed: G and H in every row; also D and E in the C(1) row.

**Facts that discriminate between models**

| fact | values | data |
|---|---|---|
| pooled boundary ratio | A 1.09, B 1.12, C 0.69, D 1.10, E 1.04, F 1.11, H 1.56 | 0.94 (0.86–1.04) |
| turnover, K/64 against K/2 | A 0.25/0.23, B 0.15/0.25, C 0.16/0.30, D 0.21/0.14, E 0.20/0.14, F 0.20/0.14 | 0.21/0.18 |
| gapped persistence | A −0.02, B −0.07, C −0.10, D −0.07, E −0.10, F −0.10, G +0.17 to +0.19 | +0.17 (+0.14 to +0.20) |
| prior decliners' excess slide, a week | D −0.0019, F −0.0030 | +0.0027 / +0.0023 |

- Boundary ratio: G not computed.
- Turnover: G and H not computed.
- Gapped persistence: H not computed.
- Prior decliners' excess slide: A, B, C, E, G and H not computed.

**Facts that do not discriminate**

| fact | values | data |
|---|---|---|
| raw β (calibration: planted 0 → 0.06, planted 0.2 → 0.09) | A 0.00, B 0.13, C 0.36, D 0.06, E 0.09, F 0.07 | 0.045–0.082 |

- Not computed: G and H.

**Facts every model run fails**

| fact | values | data |
|---|---|---|
| top-10 turnover | A 0.24, B 0.13, C 0.04, D 0.21, E 0.19, F 0.23 | 0.33, at or above every world's 95th percentile |
| turnover at K/2 | D, E, F 0.14 | 0.18 |
| 2022 head share, forecast from 2019–20 | F 0.537 (fixed home 0.528) | 0.356 |
| kurtosis | F 2.9–5.1 | 22.1 |

- Top-10 turnover: G and H not computed.
- Turnover at K/2: A, B, C, G and H not computed.
- Head share and kurtosis: every column except F not computed.

**Ties**

| fact | hero (F) | best simpler arm | carrying forward |
|---|---|---|---|
| held-out movement, submissions | 0.183 | 0.171 | 0.163 |
| held-out movement, comments | 0.130 | 0.100 | 0.167 |

- "Not computed" is honest and shows what remains to be done.
- Columns D, E and F share machinery. Label what each column varies, and do not count their agreement as independent.

---

## 4. Choosing models and facts

### A rule to commit to PLAN.md before any new column runs

Label the rule as post hoc for the columns that already exist.

1. **Columns.**
   - One minimal published model from each tradition the introduction cites, plus the calibrated model.
   - Calibrate each on 2019–20 to the same declared moments, never to a row:
     - universe size and entry rate;
     - lasting variance at the median rank;
     - passing-part variance and half-life.
   - Where the dynamics allow, run each from both a constant-slope start and the data's ladder.
2. **Rows.** The union of every statistic the abstract or significance statement relies on and each rival tradition's signature statistic:
   - firm β;
   - neutral turnover against list size;
   - cumulative advantage's protected top;
   - Iñiguez's C(R) and its dependence on flux;
   - the SPT slope identity.
3. **Cells.** "Reproduces" means the data value lies inside the 5–95% band over 20 runs. Otherwise the cell is coded above, below, or not computable. Numbers go in the SI.
4. **Report everything** and drop nothing after scoring. This follows den Boon (Table 1).

### Missing models a referee would expect

1. **Calibrated heterogeneous mean growth.**
   - This is GLLM's "type dependence"; in SPT terms it is the hybrid Atlas model of Ichiba et al. 2011.
   - That paper is already cited, and MODEL_STATUS line 11050 names it as the frame for drift by name.
   - It tests rival reading (b) and the discovery itself. The paper currently rests on a 5-seed synthetic check of one statistic.
   - §2z-dd's own lesson: drift by name must scale with size like the lasting volatility.
   - **Variants to run:**
     - fixed types;
     - types switching with a half-life of about one year;
     - heterogeneous exposure to a common factor, which is rival reading (c) and the paper's own submissions finding.
   - **The data's lag profiles separate the variants** (SI persistence table):

     | gap | 0 weeks | 26 weeks | 52 weeks |
     |---|---|---|---|
     | submissions | +0.194 | +0.150 | +0.046 |
     | comments | +0.135 | +0.136 | +0.108 |

2. **Cumulative advantage with fitness and aging.**
   - Sources: Simon 1955; Bianconi & Barabási 2001; Wang, Song & Barabási 2013.
   - The introduction cites Merton, Salganik and van de Rijt, but no world implements cumulative advantage.
   - Neutral copying is its driftless relative, and it protects the top (0.04 against 0.30).
   - Aging is a rival account of the slide. Lifecycle drift was rejected earlier (MODEL_STATUS line 1206, "H2 … dead"); the atlas should show that.
3. **Iñiguez et al.'s displacement–replacement model.**
   - It is cited, and the paper uses its C(R).
   - It currently appears only in the boundary row, as jumps (§2z-dc).
   - Calibrate it to the data's flux and run it through the turnover-order and C(R) rows.
4. **Naming.**
   - The JSON key `atlas`, and "Atlas" in the brief, refer to the firm-like Gabaix world (PLAN R3-A), not the Banner–Fernholz–Karatzas Atlas model.
   - The genuine first-order rank-based model is the hero's lasting core with drift by rank.
5. **Lower priority:** Duranton 2007 and TeBlunthuis 2022. Both are cited; list them as not run.

---

## 5. Failure as information

**Shared failures (the frontier).** Put them in the table and in one Discussion sentence:
- top-10 turnover of 0.33, at or above every world's 95th percentile (SI line 244);
- turnover at K/2 of 0.18, against 0.13–0.14 in the worlds;
- the 2020–22 fall of the head, forecast at 0.53–0.54 against 0.356 observed;
- kurtosis of 22.1 against 2.9–5.1.

Each points to an ingredient: shocks and events, heterogeneity, or heavy tails.

**Discriminating failures (the evidence; lead with these):**
- neutral copying reverses the turnover order and loses the boundary at K;
- constant-slope firm-like growth protects the top;
- constant-slope size-blind growth turns every set over at the same rate;
- every homogeneous world gives negative persistence, because prior decliners rebound (R10).

**The hero model's own failures, shown to follow from its theory:**
- **"8–46% flatter" is c/(s̄ − e) = 0.64 < 1 seen from the model** (line 157).
  - Add that a model matching the observed slope would contradict the measured balance.
  - Caveat: R11 (entry and exit possibly counted twice) has run but is not scored.
- **"Does not forecast the head's fall" is GLLM's slow-transition result** for standard random growth, which type or scale dependence fixes (p. 2071 abstract) [paraphrase; checked].
  - Present it as the prediction of the homogeneous class.
  - Let the heterogeneous column test whether persistence supplies the speed, and report either outcome.

**Small fixes:**
- **Fig. 5A:** label each cross by its world, and add the heterogeneous world as a filled marker above zero.
- **Fig. 4D:** keep it. Its caption should say that a tie is expected for a stationary model at these horizons (DESIGN.md).
- **Discussion, line 186:** replace the restatement of what is automatic with a pointer to the table.

---

## 6. Robustness checks (data on hand)

**1. Calibrated heterogeneous worlds (fixed, switching, factor exposure), every row, 20 seeds**
- **Tests:** rival (b); whether persistence explains the excess slide and the speed of the head's fall.
- **What would change the paper:**
  - fixed types reproduce the lag profile: state persistence as type heterogeneity;
  - no variant reproduces the excess and the fall: drop the unifying sentence (line 174).
- **Status: not done.**
  - Only the synthetic check (5 seeds, one statistic) exists.
  - The §2z-dd velocity world was scored on the averaging-inflated statistic and failed transport to comments.
  - Cheapest first step: rescore the §2z-dd worlds on the gapped statistic.

**2. R10 at 20 seeds with bands and a permutation of prior-change labels; a comments-calibrated model world**
- **Tests:** the 40–60% claim.
- **What would change the paper:** if the band covers 0 on submissions (now 5 of 9 cells), restrict the claim to comments.
- **Status:** partial (5 seeds, post hoc).

**3. Score R11**
- **Tests:** the 8–46% figure and the counterfactual magnitudes.
- **What would change the paper:** a substantial shrinkage means revising line 157 and Fig. 4B.
- **Status:** run, not scored.

**4. 30-year runs with drift taken from 2019–20 and from each half-year (G6)**
- **Tests:** whether the results depend on the 2020–22 slides.
- **What would change the paper:** if the balance falls near the observed slope, "steeper than its balance" becomes specific to the period.
- **Status:** not done.

**5. Specification curve over K, karma against items, presence threshold, windows, gaps, membership and cohort block**
- **Tests:** rival (d), forking paths.
- **What would change the paper:** sign flips in a non-trivial share of equivalent specifications.
- **Status:** partial (§2z-da, R3-D, R4, R4b).

**6. R9 with comments- and Facebook-calibrated dynamics**
- **Tests:** the generality of the geometry explanation.
- **What would change the paper:** if a constant slope also gives head-fastest turnover on comments, geometry is not the whole story.
- **Status:** submissions only.

**7. Iñiguez and fitness–aging columns, every row**
- **Tests:** the rank-space and cumulative-advantage readings.
- **What would change the paper:** if either reproduces both the boundary ratio and the turnover order, the level-diffusion reading weakens.
- **Status:** only the boundary row exists, for jumps.

**8. Structure-function β recovered under heterogeneous drift (G15)**
- **Tests:** the comparison with firms.
- **What would change the paper:** inflation of ≥0.05 means dropping the claim of overlap with firms on comments.
- **Status:** not done.

**9. Top-10 turnover without the June 2020 bans and flagged event-driven entrants**
- **Tests:** the shared failure at the top.
- **What would change the paper:** either it moves into the band (explained), or it stays as an excess at the head.
- **Status:** not done; asserted in line 114.

**10. Leave-one-year-pair-out for c/(s̄ − e) and the boundary ratio**
- **Tests:** stability.
- **What would change the paper:** one pair driving 0.64 or 0.94.
- **Status:** partial.

The capture-delay audit needs the WD drive and is owner-gated.

---

## 7. Minimal version and placement

1. **Introduction, line 95.** Add one sentence stating the selection rule.
2. **New Table 1 at the end of Results** (after line 182), referenced from Discussion paragraph 1.
   - About 10 rows in the four groups; 6–8 columns plus the data.
   - Pay for it by moving Fig. 2A to the SI, cutting line 116 to two sentences, and deleting world numbers at lines 114, 139, 147, 168 and 174.
3. **One new computation.**
   - Run the calibrated heterogeneous (hybrid Atlas) column at 20 seeds on every row, under a rule committed beforehand, and label it post hoc.
   - Add the Iñiguez and fitness–aging columns if they are cheap; otherwise mark them "not run".
4. **SI.**
   - Rename the null-worlds section (line 225) "Model atlas: every model fitted, and how each fails".
   - Give it one row per model, each with its gate count from GATE_LEDGER:
     - ladder rungs R0–R5 (one line);
     - the fixed home and the fixed home with drift;
     - the moving homes, including the frozen version with the variance error;
     - drift by name (§2z-dd);
     - lifecycle drift (H2);
     - jumps and rank-only dynamics (§2z-cv, §2z-dc);
     - the planted-β worlds.
   - Put the ladder in half a page, or omit it.
   - Add the specification curve.
5. **Discussion line 190 and SI line 253**, only if the owner adopts the amendment: the sealed test registers forecasts from every column and baseline and scores them row by row. Amending a protocol is the owner's decision under AGENTS.md.
6. **Do not:**
   - rank or weight the models;
   - use a composite;
   - put the ladder in the main text;
   - use the word "ensemble".

---

## Checked versus proposed

**Checked in this session:**
- **Repository facts:**
  - the world definitions (PLAN R3-A, R7, R9; SI line 226);
  - the JSON headline values (for example top-10 turnover 0.243 / 0.127 / 0.043 / 0.205 / 0.192 / 0.228);
  - the gate rows 101–114;
  - the §2z-dd velocity results;
  - that the JSON key `atlas` is the firm-like world;
  - that R11 is unscored.
- **Web sources:** the paraphrases marked as checked, at the pages given, and the one Levins quotation.

**Proposed for the authors to check:**
- all cell codes;
- whether the rival models can be calibrated to the declared moments;
- run times;
- exact quotation wording at the cited locations.

---

## References (access label in brackets)

- Abramowitz et al. 2019, *Earth System Dynamics* 10:91–105, https://doi.org/10.5194/esd-10-91-2019 [abstract]
- Acerbi & Bentley 2014, *Evolution and Human Behavior* 35:228–236, https://doi.org/10.1016/j.evolhumbehav.2014.02.003 [abstract]
- Auspurg & Brüderl 2021, *Socius* 7, https://doi.org/10.1177/23780231211024421 [full text p. 1]
- Auspurg & Brüderl 2024, *PNAS*, https://doi.org/10.1073/pnas.2404035121 [secondary]
- Bianconi & Barabási 2001, *EPL* 54:436, https://doi.org/10.1209/epl/i2001-00260-6 [secondary]
- Breznau et al. 2022, *PNAS* 119:e2203150119, https://doi.org/10.1073/pnas.2203150119 [full text, abstract and significance only; PMC9636921]
- Brock 1999, *Industrial and Corporate Change* 8:409–446 [secondary, via Windrum et al.]
- Broido & Clauset 2019, *Nature Communications* 10:1017, https://doi.org/10.1038/s41467-019-08746-5 [full text, PMC]
- Clauset, Shalizi & Newman 2009, *SIAM Review* 51:661–703, arXiv:0706.1062 [full text, v2 pp. 1, 18–19]
- Cramer et al. 2022, *PNAS* 119:e2113561119, https://doi.org/10.1073/pnas.2113561119 [full text, PMC9169655]
- Del Giudice & Gangestad 2021, *AMPPS* 4, https://doi.org/10.1177/2515245920954925 [abstract]
- den Boon et al. 2019, *BMC Medicine* 17:163, https://doi.org/10.1186/s12916-019-1403-9 [full text, PMC6699075]
- Engzell 2023, *PNAS* 120:e2218530120, https://doi.org/10.1073/pnas.2218530120 [full text]
- Eyring et al. 2016, *Geoscientific Model Development* 9:1937–1958, https://doi.org/10.5194/gmd-9-1937-2016 [abstract]
- Gabaix, Lasry, Lions & Moll 2016, *Econometrica* 84:2071–2111, https://doi.org/10.3982/ECTA13569 [full text p. 2071; cited]
- Grimm et al. 2005, *Science* 310:987–991, https://doi.org/10.1126/science.1116681 [full text pp. 987–989]
- Holme 2019, *Nature Communications* 10:1016, https://doi.org/10.1038/s41467-019-09038-8 [full text]
- Iñiguez et al. 2022, *Nature Communications* 13:1646, https://doi.org/10.1038/s41467-022-29256-x [secondary; cited]
- Knutti 2010, *Climatic Change* 102:395–404, https://doi.org/10.1007/s10584-010-9800-2 [secondary]
- Levins 1966, *American Scientist* 54:421–431, https://www.jstor.org/stable/27836590 [full text pp. 422–423, scan at zoology.ubc.ca]
- Luttmer 2007, *QJE* 122:1103–1144, https://doi.org/10.1162/qjec.122.3.1103 [secondary]
- McGill et al. 2007, *Ecology Letters* 10:995–1015, https://doi.org/10.1111/j.1461-0248.2007.01094.x [abstract]
- Orzack & Sober 1993, *Quarterly Review of Biology* 68:533–546, https://doi.org/10.1086/418301 [full text pp. 533–534]
- Salganik et al. 2020, *PNAS* 117:8398–8403, https://doi.org/10.1073/pnas.1915006117 [full text, PMC7165437]
- Silberzahn et al. 2018, *AMPPS* 1:337–356, https://doi.org/10.1177/2515245917747646 [secondary]
- Simon 1955, *Biometrika* 42:425–440, https://doi.org/10.1093/biomet/42.3-4.425 [secondary]
- Simonsohn, Simmons & Nelson 2020, *Nature Human Behaviour* 4:1208–1214, https://doi.org/10.1038/s41562-020-0912-z [secondary]
- Steegen et al. 2016, *Perspectives on Psychological Science* 11:702–712, https://doi.org/10.1177/1745691616658637 [full text pp. 702–703]
- Wang, Song & Barabási 2013, *Science* 342:127–132, https://doi.org/10.1126/science.1237825 [secondary]
- Warszawski et al. 2014, *PNAS* 111:3228–3232, https://doi.org/10.1073/pnas.1312330110 [secondary]
- Weisberg 2006, *Philosophy of Science* 73:730–742, https://doi.org/10.1086/518628 [secondary]
- Windrum, Fagiolo & Moneta 2007, *JASSS* 10(2):8, https://www.jasss.org/10/2/8.html [full text §§4.8, 5.2]
- Yao, Vehtari, Simpson & Gelman 2018, *Bayesian Analysis*, https://doi.org/10.1214/17-BA1091 [abstract via arXiv:1704.02030; volume and pages unverified]

## Search log (2026-10-19)

**WebSearch** queries:
- Cramer 2022 PNAS; Breznau 2022 PNAS and its PMC record
- Grimm 2005 pattern-oriented modelling; McGill 2007 species-abundance distributions
- Levins 1966 "independent lies"; Orzack & Sober 1993
- Broido & Clauset 2019; Holme 2019
- Steegen 2016; Del Giudice & Gangestad 2021; Simonsohn 2020
- Salganik 2020; Silberzahn 2018; Auspurg & Brüderl 2021 and 2024
- Knutti 2010; Abramowitz 2019; Warszawski 2014
- Weisberg 2006; Windrum et al. 2007; Brock 1999
- den Boon 2019; Hofman 2021 (not used)
- Iñiguez 2022; Acerbi & Bentley 2014
- Bianconi & Barabási 2001; Wang, Song & Barabási 2013; Simon 1955; Luttmer 2007

**WebFetch:**
- PMC articles 9169655, 9636921, 9926231, 7165437, 6399239, 6399274 and 6699075
- arXiv 0706.1062 (PDF) and 1704.02030 (abstract)
- jasss.org; esd.copernicus.org; gmd.copernicus.org; digitalcommons.usu.edu; psychologicalscience.org; acerbialberto.com
- PDFs read as page images: Levins, Orzack & Sober, GLLM, Grimm, Steegen, Auspurg & Brüderl

**Fetches that failed** (403, 410, 500 or a login redirect): pnas.org, sagepub, open.bu.edu, doaj, rero, the ETH Research Collection, nature.com.

**Local:** read-only Read, grep and python text extraction on the repository files named in the access check.