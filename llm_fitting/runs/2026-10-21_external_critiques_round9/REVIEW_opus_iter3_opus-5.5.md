_Round-9 Opus-track iteration-3 review by the same independent Claude Opus 5.5 sub-agent as iterations 1 and 2 (continued with its own context; no access to the orchestrating session's context or to the Astra round-9 reports), read-only, run 2026-10-21 on manuscript v9.3 (commit 632779b) with `PROMPT_R9_opus_iter3.md`. Saved by the orchestrating session (Claude Opus 5.5) from the sub-agent's final message: content verbatim, the harness's two-space indent removed._

# Referee report: round 9 (Opus track), iteration 3 of 3, on manuscript v9.3 (commit 632779b)

**What I checked.**
- The diffs from 091e950 to 632779b in `pnas_draft.tex` and `pnas_si.tex`.
- `RESPONSE_opus_iter2.md`.
- `r18c_headshare_diag.py` and its output.
- `r18_summary.json` and `r18_table1.json`, with every Facebook cell in Table 1 and the SI recomputed against the 5–95% bands and the "edge" rule.
- Per-run Facebook world JSONs for all seven arms.
- The per-run out-of-period head shares in `r12_oop__*.json` and `r16_oop__*.json`.
- `null_worlds_r7_summary.json`.
- `make_v5_figures.py` (titles and share inputs).

**Limits.** No panels, nothing sealed, no runs. The main text is 7 pages.

---

## 1. Re-score

**Send out. Enthusiasm 6.5/10, unchanged.** With the three fixes in §3 (all text or saved outputs) it is 7. That is the ceiling within these constraints.

- v9.3 is the most honest version so far:
  - the asymmetry is now described correctly, as lasting extreme declines and transient extreme gains;
  - Table 1 uses one size law;
  - the Facebook departures are scoped to what survives.
- The new runs cut both ways:
  - R18-C makes the Facebook persistence excess a one-gap result. At 4 weeks the calibrated arm contains the data.
  - It shows that on Facebook the simulated geometry control no longer supports the geometric account of the head (C2 failed). Read correctly, it points to the starting ladder instead (§3, B2).
- Facebook, the central dataset, still contributes vivid facts and kinematics, but no model-confirmed mechanism. Reddit carries the discoveries, which are unconfirmed out of sample.

---

## 2. Status of iteration-2 items

| item | status | note |
|---|---|---|
| N1, secant slopes | **Resolved** | Main text and SI match my recomputation (0.15/0.16 against −0.12/−0.08; 0.34/0.15 against −0.21/−0.20; −0.08/−0.05 against −0.05/−0.11; third and eighth tenths 0.09/0.23 and 0.10/0.25). One residue: "thinner" (B3) |
| N2, Facebook ordering | **Resolved** in the text | Table 1 now raises B2 |
| N3, Table 1 β | **Resolved** | Every Facebook cell checked against `r18_summary.json`: K/64 above (q95 0.307 and 0.306 against 0.315); K/2 edge (q05 0.204 and 0.208 against 0.205); gap-4 persistence "edge" in the calibrated arm (q95 −0.064 against −0.067: in fact inside); D above all three (q95 0.037, 0.035, 0.022) |
| N4, starting ladder | **Half-resolved** | Stated as a possible cause; R18-C now strengthens it (B2); rerun not done |
| N5, 0.255 | **Resolved** | |
| N6, Fig. S1C title | **Resolved** | regenerated 14:50 |
| N7, SI residue | **Resolved** | |
| N8, density | **Partly** | asymmetry paragraph 302 words, model paragraph 210 |
| Framing 1, surge-and-loss hypothesis | **Done** | Small wording point: Reddit's extreme gains do not reverse on average (p_rise +0.02, +0.03), so "do not continue" is more exact than "fade" |
| Framing 2, Facebook size fact leads | **Done**, well | |

---

## 3. New blockers, ranked

### B1. The extreme-week problem is a simulator property, not a Facebook head-share quirk, and it contaminates main-text numbers on Reddit

**Is the Facebook head-share correction right?**
- **The diagnosis is right.** One unit outside the head has a simulated week of 27.2 log units, ten above the leader's 17.3 and about 22,000 times its activity; its second-largest week is 20.25. That week holds 96% of the top-K sum.
- **The median over runs is a reasonable robust summary,** and withdrawing R18-B2's reading is correct.
- **What a spike does depends on where the spiking unit sits.** Outside the head it pushes the share toward 0, as here. Inside the head it would push the share toward 1. The median handles both.
- **The bands remain contaminated** (matched control at 0.019: [0.02, 0.33]; constant-slope arm: [0.10, 0.33]).

**Are other Facebook statistics affected?** Mostly no.
- I checked every degenerate run (head share below 0.15; one or two per arm in six of seven arms). Its rank-1 churn, D, crossing ratio and top-54 turnover all lie within the other runs' spread. For example, controlb0.019's degenerate runs have D −0.021 and −0.015 against −0.001 in the rest, and crossing ratios 1.01 and 1.03 against 1.01.
- But the SI's sentence "Every other statistic is computed from ranks or log activity and is unaffected" is too strong. A 13-log-unit week moves a 4-week window mean by about 3 log units, which enters D_FB's tail means, and moves a 13-week block mean by 1, which enters the crossing variances. The supportable statement is "not detectably affected: in the degenerate runs every other statistic lies within the other runs' range".

**The same contamination reaches Reddit.** The Reddit worlds use the same simulator. Every level-sum statistic there inherits the problem, and the paper still quotes them as means:

| arm | mean | median | max over runs |
|---|---|---|---|
| submissions, homogeneous `control` | 0.537 | 0.512 | 0.71 and 0.89 |
| comments, homogeneous `control` | 0.597 | 0.569 | 0.946 |
| comments, `fixed` | 0.613 | 0.569 | 0.999 |
| comments, `decline_hist` | 0.619 | 0.571 | — |
| comments, `joint_decline_hist` | 0.650 | 0.579 | 1.000 |

- Main text (l.180), "54% … (5–95%: 48–72%)" and "60% (55–67%)", are exactly the means and quantiles of `r12_oop__*__control.json`. In 16 of 18 out-of-period arms the run maximum is 0.85–1.00, physically a head holding all activity.
- **Fig. S2** (`figS3_v5.png`, drawn from `share_3` and `share_3_q`) plots these means and bands.
- **SI "Out of period":** the paired range "−0.42 to +0.10" is a spike artefact. "Fixed differences move the forecast away from the observed share" on comments is 0.613 against 0.574 by means, but 0.569 against 0.559 by medians. The decline state "set from its last training year … 0.62" on comments is 0.571 by median, the same as the control.
- **Selection baseline:** the Gomez within-term baselines from the worlds, behind the Discussion's "about 5% … from selection alone", are means with very wide tails (size-blind q05 −0.20, model world q05 −0.51 in `null_worlds_r7_summary.json`). Check their medians if the per-run values exist.
- **Conclusions survive:** no form forecasts either fall, and with medians the miss is about 0.13–0.16 on submissions and 0.06–0.07 on comments. The numbers and some secondary readings do not survive.

**Fix (text and saved outputs).**
- Medians, with the count of degenerate runs, for every level-sum statistic: main l.180, Fig. S2, and the SI out-of-period and decline-state passages.
- Withdraw the two secondary readings named above.
- One sentence on the cause: the fast swing's Student-t innovations, in an AR(1) with φ up to 0.99 (`rankdiff_rdm.py` l.580), generate weeks far beyond any observed. The model's passing component is symmetric but far too heavy-tailed for statistics in levels.

### B2. On Facebook, Table 1 now shows the geometry control matching the data's head, and the text does not say what that means

At β 0.019:

| world | top-54 turnover | top-1,750 turnover |
|---|---|---|
| constant-slope start | 0.31 (Table 1 "in") | 0.29 (C2 failed) |
| data-ladder worlds | 0.25 ("above") | — |
| data | 0.315 | — |

- The geometry control therefore no longer "removes the head's excess" on Facebook.
- Its turnover profile is not flat by rank (0.23, 0.31, 0.34, 0.31, 0.29 from the top 10 to the top 1,750). The model's rank-dependent passing parts shape turnover as well as the slope.
- There is a consistent reading, which supports N4. The constant-slope world's head slope, about 0.73, is close to the data's 0.7–0.8, and its head turns over like the data's. The data-ladder worlds start with a head 0.27 log units more spread (4.31 against 4.04) and turn over more slowly.
- So the Facebook shortfall looks like the simulated starting state, not the leaders' dynamics. That is good news for the title, but it is an inference from three arms, not a test.

**Fix (text, one or two sentences in Results 2 or 4).** State that on Facebook the evidence for the geometric account is:
- the kinematic reconstruction (12 cells);
- the slope profile;
- a constant-slope world whose head, as flat as the data's, turns over as fast;

while worlds started from a more spread head do not. The decisive run is listed in §4.

### B3. "The worlds' tails are symmetric and thinner than the data's" (main, asymmetry paragraph; SI): "thinner" is unsupported

This came from my iteration-1 report, and I withdraw it. At the level the statistic uses, the tenth means, the worlds' tails are as wide as the data's:

| ladder | data, bottom/top | width | matched control, bottom/top | width |
|---|---|---|---|---|
| submissions | −1.12 / +1.60 | 2.72 | −1.30 / +1.31 | 2.61 |
| comments | −1.38 / +1.86 | 3.24 | −1.59 / +1.66 | 3.25 |

- The difference is skew, not thickness.
- The kurtosis comparison (2.9–5.1 against 22.1) comes from another model run and statistic (A6, 26-week changes).
- B1 shows the simulator's weekly tails are, if anything, extreme.

**Fix:** "symmetric, where the data's are skewed toward rises". Drop "thinner" and the kurtosis clause from the SI's "Where the asymmetry sits", or scope it to A6's 26-week changes.

### B4. The Facebook departure is now marginal; keep every mention at that strength

- Persistence is above every world only at 8 weeks. At 4 weeks it is inside the calibrated arm (Table 1's "edge").
- D_FB is above every world's range, but its own interval includes zero, and it rests on three overlapping origins (+0.016, +0.075, +0.065).
- The main text handles this well. The abstract's "Facebook's 86 weeks show a weaker sign of the first departure" would be exact as "a weak, post hoc sign".

### B5. Density (N8)

At 302 words the asymmetry paragraph is still the hardest in the paper. Two cuts lose nothing:
- the "intervals resample communities within one period" parenthesis (it is in Methods);
- the list of robustness checks (keep "in item counts and in a cohort fixed in early 2019", and send the rest to the SI).

---

## 4. Final answer

**Is this the best paper these data and constraints allow?** Nearly. With B1–B3 fixed (about an hour's work on saved outputs, no runs) I see no further gain available from text, figures or saved outputs:
- the framing is right for the evidence;
- the claims are scoped by platform;
- failures and post hoc steps are labelled;
- the unifying hypothesis is stated as a hypothesis.

What separates it from an enthusiastic PNAS reception is evidence, not presentation:
- a mechanism confirmed by simulation on the central platform;
- an out-of-sample test of the departure.

**What the owner should decide next, in order of value.**

1. **Sealed confirmation on the 2023–24 Reddit data, after amending and adopting the protocol.** This is the only step that can move the paper above about 7.5: it turns the asymmetry and persistence from a development-data pattern into a confirmed finding. Before adoption, P8 needs three things:
   - co-registered secant slopes and tail-skew statistics, so that a pass can distinguish "losses last" from "surges fade";
   - a decision on karma against counts for submissions (counts in the protocol construction gave an uninformative +0.03 [−0.03, +0.10]);
   - persistence at 13- and 26-week gaps as a primary.
2. **The Facebook starting-ladder rerun:** the matched and calibrated arms at β 0.019, started from a head matched to the data's first block. It is the cheapest decisive run left (R18's world batches take seconds). It decides whether the title's geometric account holds in simulation on the central dataset. If head turnover and ordering come right, Facebook gains a model-confirmed result. If not, the title should be scoped to what the kinematics show, and Facebook's fast head becomes a reported departure.
3. **The surge world:** a positively skewed transient component added to the joint model, calibrated without targeting D. It decides the interpretation of the paper's main discovery: "losses last" or "surges fade". It would also test whether skewed surges explain Facebook's fast head and its p_rise < p_fall. This is a moderate new model component and calibration, not a quick iteration.
4. **A decision on the fast swing's tails** (truncate or re-estimate the t degrees of freedom). It would make every level statistic (head shares, Gomez baselines, out-of-period forecasts) trustworthy in the worlds, rather than rescued by medians. It is light to run but a model change, hence the owner's.
5. **Reddit's homogeneous worlds at Facebook's settings** over the overlapping period. These make the cross-platform persistence comparison like for like, instead of raw correlations against different nulls. Light to moderate.
6. **A longer, instrument-consistent Facebook history,** from the later CrowdTangle eras already held, if they can be bridged without a new source. This is the only route to year-apart persistence, heterogeneity calibration and slide measurement on Facebook, and so to making the central platform carry a discovery, not only facts and kinematics.
7. **Comments rows in Table 1.** Low value: the comments worlds are in the SI, and the page budget is binding.
