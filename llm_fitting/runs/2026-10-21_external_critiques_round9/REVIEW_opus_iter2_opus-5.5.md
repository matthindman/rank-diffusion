_Round-9 Opus-track iteration-2 review by the same independent Claude Opus 5.5 sub-agent as iteration 1 (continued with its own context; no access to the orchestrating session's context or to the Astra round-9 reports), read-only, run 2026-10-21 on manuscript v9.2 (commit 091e950) with `PROMPT_R9_opus_iter2.md`. Saved by the orchestrating session (Claude Opus 5.5) from the sub-agent's final message: content verbatim, the harness's two-space indent removed._

# Referee report: round 9 (Opus track), iteration 2, on manuscript v9.2 (commit 091e950)

**What I checked.**
- The full diff from 72ec0c2 to 091e950 in `pnas_draft.tex` and `pnas_si.tex`.
- PLAN amendment R18-B, RESULTS R18-B and the `r18_facebook.py` change.
- `r18_summary.json` and the 100 Facebook world JSONs, from which I recomputed per-run turnover profiles, gradients and first-block ladders for all five Facebook arms.
- The decile curves in `r17_asym_data__*.json` and `r17_asym_worlds__*__control_num_.json`, from which I recomputed secant slopes.
- The PDFs (main text 7 pages; abstract 236 words; significance statement 118 words).

**Limits.** I ran nothing heavier than small JSON readers. No panels were opened and nothing sealed was touched.

---

## 1. Re-score

**Send out. Enthusiasm 6.5/10, up from 6.**

- v9.2 is markedly more honest:
  - the title now claims what holds on every platform;
  - the Facebook model's failure at the head is stated and partly diagnosed (R18-B is a clean, registered test);
  - the asymmetry is scoped to its tails, with a named rival;
  - the Facebook persistence is labelled post hoc, with its own rival and the matched-settings contrast.
- What keeps it below an editor's "I would argue for this" (7):
  - one new overstatement in the asymmetry paragraph. It came from my own iteration-1 wording, and I correct it below.
  - a Facebook claim ("the models reproduce the ordering") that the saved worlds contradict below the top ten.
  - Table 1 still shows Facebook worlds built on the exponent the text now calls biased.
  - the same science as before: the general positive claim is largely kinematic, and the discovery is one platform, one period and unconfirmed.

---

## 2. Status of the iteration-1 items

**Blockers.**

| # | item | status | note |
|---|---|---|---|
| 1 | Title rests on identities; Facebook model failed at the head | **Half-resolved** | Title, abstract and Results 2 are now accurate in spirit; R18-B run. Open: N2–N5 below |
| 2 | Asymmetry is a tail-shape fact; surge rival | **Half-resolved** | Tails, quintile D, skew and rival now stated. New overstatement N1; the surge world is not run (owner) |
| 3 | Facebook persistence fragile | **Resolved** in the main text | Residue in the SI (N7) |
| 4 | "Heterogeneous growth misses the asymmetry" | **Resolved** | "symmetric" throughout; the decline state credited. Table 1 still omits comments without saying why (minor) |
| 5 | Common model not common | **Mostly resolved** | Methods now say exactly what is common; Table 1's β is open (N3) |
| 6 | Development data only | **Open** | owner |
| 7 | Lead and density | **Half-resolved** | Abstract and significance are good. The asymmetry paragraph grew to 332 words and the model paragraph is 292 (N8) |
| 8 | Size claim on comments | **Resolved** | "weak rather than absent" |

**Errors.**

| errors | status |
|---|---|
| E1, E3–E17, E19–E22 | **Resolved**, checked against the new text |
| E2 | **Half-resolved.** The main text is fixed (0.028 against 0.037; 0.022 against 0.013), but Fig. S1C's panel title still reads "A year ahead, replacement is reproduced" (`make_v5_figures.py` l.332; `fig3_v5.png` dated 10:39, not regenerated) |
| E18 | **Accepted** as answered: 1,728 is the formula's cell, 1,750 the table's K/2 |
| E23 | Answered |

---

## 3. New blockers, ranked

### N1. "For most changes the share is similar in both directions" overstates what the deciles show

This sentence (main, asymmetry paragraph) is my own iteration-1 wording, and it is wrong beyond the second and ninth tenths.

- The follow-up ratio of each tenth (bottom to top) on submissions is +0.24, +0.16, **+0.09**, −0.06, −0.27, … , **+0.23**, +0.18, +0.02. On comments it is +0.16, +0.16, **+0.10**, … , **+0.25**, +0.15, +0.03.
- In the third and eighth tenths the ratio is 0.09 against 0.23 and 0.10 against 0.25. Rises "persist" more there.
- Nearer the middle the ratio is meaningless. The second change is demeaned, but the typical community's is positive (+0.05 at a first change of about 0), because a minority falls steeply. A ratio through the origin then mixes slope with that offset.
- The SI's "Where the asymmetry sits" lists the bottom three tenths but only the top two. That hides the 0.23 and 0.25 and supports "the near tails are symmetric" by selection.

**Fix (saved outputs; I computed these from the decile curves).** Describe the curve by its secant slopes, the change in the mean later change per unit of first change:

| ladder | between the bottom two tenths | third to eighth tenth | between the top two tenths |
|---|---|---|---|
| submissions, data | 0.34 | 0.15 | −0.08 |
| comments, data | 0.15 | 0.16 | −0.05 |
| matched control, submissions | −0.21 | −0.12 | −0.05 |
| matched control, comments | −0.20 | −0.08 | −0.11 |

Two clean statements follow, and both fit the main text in one sentence:
- **(a)** Ordinary changes persist at about 0.15 on both ladders. On comments the largest declines continue at that same ordinary rate. The whole raw asymmetry there is that the largest gains stop. On submissions both ends depart.
- **(b)** Against the null, the largest declines carry the most excess persistence. The null reverses them hardest (−0.21 and −0.20), presumably because in homogeneous growth large falls are mostly passing dips. The largest gains carry none: data −0.08 and −0.05 against the null's −0.05 and −0.11. So "in the data, extreme declines behave as lasting, extreme gains as transient". That is the precise, defensible form of the finding. It is also exactly what both the decline-state and the surge readings predict, which the surge sentence should say.

### N2. "The calibrated models reproduce the ordering on every ladder" is false on Facebook below the top ten

Recomputed from the world JSONs:

| arm | mean turnover, top 54 | mean turnover, top 219 | runs with top 54 > top 219 |
|---|---|---|---|
| calibrated (β 0.097) | 0.21 | 0.28 | 0 of 20 |
| matched control (β 0.097) | 0.23 | 0.27 | 2 of 20 |
| matched control, β 0.019 | 0.255 | 0.30 | 4 of 20 |
| matched control, β 0.052 | 0.25 | 0.28 | 3 of 20 |
| **data** | **0.315** | **0.258** | — |

- The Facebook models peak at K/16, not at the head.
- Even the K/64-minus-K/2 gradient that R18-2 scored is weak. In the matched control it is +0.022, with a 5% quantile over runs of −0.006, so it does not clearly differ from 0.
- The SI's "The same model reproduces … the role of geometry" overstates the same point.

**Fix (text):** "reproduce the ordering on Reddit; on Facebook the model's turnover peaks at the top 219 rather than at the head". Mention the R18-B arms, which do not fix this.

### N3. Table 1's Facebook block uses the exponent the text now calls biased

- All three Facebook world columns use β = 0.097. Results 4 and the SI now say 0.097 is biased up at 86 weeks, and that the two-step 0.019 is the instrument.
- A reader of Table 1 sees head share and turnover failures that are partly the bias. R18-B2 fixed the share: 0.25 against 0.23.

**Fix (saved outputs):** put the β 0.019 matched control in the "matched control" column. It is in `r18_summary.json`. On the table's rows:
- top K/64 0.26, data above;
- top K/2 0.22, data at or below the edge (q05 0.208);
- top 10 0.33, in;
- crossings 1.01;
- persistence −0.10, data above;
- D −0.00, data above.

Otherwise add a footnote. The calibrated (closed-form) and constant-slope arms at 0.019 were not run; that is light, and for the owner.

### N4. The residual Facebook head shortfall may be the starting state, not the leaders' dynamics

- In every non-flat Facebook arm, the first-block level of rank 54 above rank K is 4.30–4.31, against the data's 4.04. That is outside every arm's 5–95% range, and β does not change it.
- By the paper's own geometry argument, a head that starts more spread out turns over more slowly.
- The margin is thin: the data's top-54 turnover of 0.315 against q95 of 0.306 at β 0.019.
- So the abstract's "Facebook's leaders turn over faster still than its calibrated model implies" is stated more firmly than the evidence. The cause could be the simulation's starting ladder: filtered lasting levels plus freshly simulated passing parts.

**Fix now (text):** "faster at the head than the model, whose starting head is 0.26 log units more spread than the data's". Also smooth "faster still than".

**Light run (owner):** the matched control started from a ladder matched to the data's first block.

### N5. A rounding that disguises a registered failure

Results 4 says top-54 turnover "rises only to 0.26", and the SI says "0.26 [0.21, 0.31]". R18-B1 required at least 0.26 and is scored failed at 0.255. Print 0.255.

### N6. Fig. S1C title (E2, figure half)

Retitle it, for example "A year ahead: better than carrying forward on submissions, worse on comments", and regenerate.

### N7. SI inconsistencies left by the revision

- **"About three times" kept.** The asymmetry section still opens "about three times as strongly", one paragraph before saying the ratio is null-dependent, has no interval, and is 2.5–3.5. Delete the first sentence.
- **Facebook persistence explained by horizon only.** The Facebook paragraph still says the persistence is "smaller over its shorter horizon". The main text now says Reddit exceeds Facebook "even over matched horizons". Harmonize.
- **"A cohort fixed in the first half of 2019" (main).** It omits that the cohort comes from a universe selected on 2019–22, and that the construction differs (13-week and 9-week windows, a 27-week gap). One clause fixes it.
- **"Reddit's persistence exceeds Facebook's even over matched horizons".** This compares raw correlations whose homogeneous nulls differ. Say "raw correlations"; the excess over each platform's own null at matched settings is not measured.

### N8. Density

The asymmetry paragraph is now 332 words, and the model paragraph (Results 4 with R18-B) 292. Both can lose a third without losing claims:
- move the leave-one-out range, the bootstrap caveat and the excess ratios to the SI;
- in Results 4, drop the matched-control numbers already in Table 1.

That also makes room for N1's slope sentence.

---

## 4. Direct answer: what would raise the score further within the constraints?

**Text and saved outputs (N1–N8): to about 7.** These remove the last statements that a careful referee could falsify from the repository. Two framing moves would add a quarter point each.

**First, one unifying description of the departures, stated as a hypothesis.**
- On Reddit, extreme declines behave as lasting and extreme gains as transient (N1(b)).
- The data's first changes are positively skewed in the tails while every world's are symmetric. The data's kurtosis is far above the models'.
- On Facebook, rises reverse more than falls (p_rise −0.16 against p_fall −0.11), and the head churns more than the model.
- All four facts fit "attention gains arrive as transient surges; losses are lasting". The paper can say this in two sentences without claiming it. It gives the reader one memorable idea, ties Facebook to Reddit honestly, and gives the sealed test P8 a sharper target.

**Second, lead Facebook's section with its cleanest novel fact,** which is now buried in a subordinate clause:
- on Facebook, larger pages post more steadily (size exponent 0.19 in post counts) but their engagement is as volatile as small pages' (0.02);
- leadership changes half the weeks because the top is flat relative to weekly moves.

That is a Facebook-specific result the model and the data agree on, and it is the kind of statement platform researchers will cite.

**Beyond 7, nothing within these constraints.** The remaining gap is:
1. **Confirmation.** P8 and the persistence tests on the sealed 2023–24 Reddit data, after the protocol is adopted. This is owner-gated; it uses no new source.
2. **The surge world.** A skewed passing component in the joint model, calibrated without targeting D. That is a new model component plus calibration, not a quick iteration.
3. **The light Facebook starting-ladder rerun (N4)** would clean up the Facebook story. It would move the score only if it changes the head result.

If none of these is run, the honest ceiling for this draft is about 7. Further text revision will not move it past that.
