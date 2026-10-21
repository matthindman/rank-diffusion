_Round-9 Opus-track iteration-1 review by an independent Claude Opus 5.5 sub-agent (no access to the orchestrating session's context or to the Astra round-9 reports), read-only, run 2026-10-21 on manuscript v9.1 (commit 72ec0c2) with `PROMPT_R9_opus_iter1.md`. Saved by the orchestrating session (Claude Opus 5.5) from the sub-agent's final message: content verbatim, Markdown layout lightly normalised._

# Referee report — round 9 (Opus track), iteration 1, manuscript v9.1 (commit 72ec0c2)

**Read:** main text and SI (`.tex` and compiled PDFs, 7 and 31 pp.), all figures, PLAN R17/R17-B3/R18/R18-A1, RESULTS R17/R18, MODEL_STATUS §2z-ds/§2z-dt/§2z-cx, SYNTHESIS_R8 and my round-8 report, `r18_facebook.py`, `r18_table1.py`, the bridge protocol's P8. **Recomputed from saved JSON only:** every Table 1 cell (`r18_summary.json`, `r17_table1.json`); decile follow-up ratios and the tail sizes of the first change from the decile curves in `r17_asym_data__*.json` / `r17_asym_worlds__*.json`; the head-versus-body turnover gradient, data against model, on all three ladders; the durability fits (`r17_durability_scores.json`). No model run, no panel opened, nothing sealed touched. I did not audit `make_v5_figures.py` beyond annotations.

---

## 1. Verdict

**Send out, without enthusiasm. Score 6/10.**

1. The question is general and well posed (does high leader turnover imply anything beyond random growth?), the Facebook leader-churn fact is vivid, and the record (registered expectations, reported failures, one code path for data and worlds) is unusually trustworthy.
2. But the title's claim rests mainly on what the paper itself calls an identity and a contemporaneous kinematic reconstruction, while the one model-based test of leader turnover on Facebook — the central dataset — failed (R18-1: top-54 turnover 0.21–0.23 in the model against 0.31).
3. The paper's genuine discovery, the sign asymmetry of persistence, is Reddit-only, is a statement about the extreme tenth of changes, and is compared with worlds whose tails do not resemble the data's; a skewed-surge alternative is untested.
4. The cross-platform "both platforms depart in the same direction" leans on a post hoc Facebook excess in statistics dominated by passing swings, whose sign flipped when the comparator changed (v9.0 → v9.1).
5. All of this is fixable in text except the confirmations: honest scoping plus a cheap Facebook rerun would move the paper to about 7; a sealed-data confirmation of D, with the surge rival excluded, would move it to 8+.

---

## 2. What blocks enthusiasm (ranked)

**1. The title claim is tested mostly by identities, and its one model test on Facebook failed.**
*Evidence.* The crossing ratio is "close to an identity … an instrument, not a test of mechanism" (main l.142); the turnover formula is "a contemporaneous check of turnover kinematics, not a forecast" (l.145). Yet the abstract presents both as the evidence that "random growth accounts for the turnover". The non-identity test is the common model: on Facebook its top-54 turnover is 0.21 (calibrated) and 0.23 (matched) against the data's 0.31, above both 5–95% ranges [0.18, 0.25] and [0.20, 0.28] (`r18_summary.json`; R18-1 failed). I computed the head-minus-body turnover gradient (top K/64 minus top K/2) from saved outputs:

| ladder | data | matched control | model/data |
|---|---|---|---|
| Facebook | 0.110 | 0.022 (calibrated −0.001) | 0.2 |
| Reddit submissions | 0.027 | 0.055 | 2.0 |
| Reddit comments | 0.055 | 0.12 | 2.2 |

The model gets the ordering right on every ladder and the magnitude wrong on every ladder, in opposite directions. The top 10 and rank 1 are reproduced, but with wide bands (Facebook top 10 [0.20, 0.45]).
*Likely cause on Facebook (checkable):* the Facebook worlds are simulated with β = 0.097 (`r18_fit_summary.json`), which the SI says is biased up by about 0.045 at 86 weeks, while the main text reports Facebook's lasting size exponent as 0.02 (l.148). With 0.097 the head's lasting variance is about half that of rank K, which slows head turnover and inflates the head share (0.30 against 0.23; R18-8 failed). The worlds' first-block head is also 0.26 log units steeper than the data's (rank-54-above-K 4.30 [4.16, 4.40] against 4.04).
*Fix now (text):* retitle around geometry (§5); state R18-1 and the gradient mismatch in Results 2, not only in the SI; rewrite "the same control removes the faster turnover of its top 54" (l.145), since the calibrated model has no faster head turnover to remove (0.210 against 0.211).
*Needs a run (light):* rerun R18 with β set by indirect inference to the value whose estimate is 0.097 (about 0.05), or at the two-step 0.02; R18's world runs took about 10–13 s per seed batch.
*Score move:* text +0.5; a successful rerun +0.5 more.

**2. The asymmetry is a tail-shape fact with an untested rival.**
*Evidence (saved decile curves):* in the second and ninth tenths of the first change the follow-up ratios are symmetric (submissions 0.16 against 0.18; comments 0.16 against 0.15); D is carried by the extreme tenths. D at quintiles is +0.15 and +0.10 (matched control −0.045, −0.031), so it survives a wider tail but shrinks. The data's first change is positively skewed in the tails (tenth means −1.12 against +1.60 on submissions, −1.38 against +1.86 on comments), whereas every world's tails are symmetric:

| world | bottom / top tenth mean, submissions | comments |
|---|---|---|
| control | −1.30 / +1.31 | −1.59 / +1.66 |
| joint | −1.05 / +1.08 | −1.13 / +1.19 |
| decline | −1.40 / +1.39 | −1.69 / +1.67 |

The models' 26-week excess kurtosis is 2.9–5.1 against 22.1 in the data (SI, Out-of-period structure). Positively skewed, transient surges added to symmetric growth heterogeneity would give large rises that do not continue and declines that do, that is D > 0. Such surges are in the record (r/wallstreetbets and r/dogecoin in 2021; D larger in later origins, +0.28 against +0.17). Methods' "D = 0 whatever the skew" holds only if the conditional mean of y is linear in x, and a mixture of lasting and skewed passing parts breaks linearity.
*Fix now:* text with saved-output numbers (§4). *Needs a run:* a world with a skewed passing component (owner, §8). *Score:* +0.5.

**3. Facebook's "persistence beyond random growth" is fragile.**
R18-5 and R18-6 registered that Facebook would lie inside its model's range; both failed, and the bands now quoted in the main text (l.188, l.194) come from R18-A1, written after the outputs were read, which the main text does not say. v9.0 said the opposite ("reverse at every gap, as in homogeneous growth") against Reddit-calibrated worlds. The excess lives in 4-week windows at gaps of at most 8 weeks, where the model's reversal barely decays with the gap (−0.134, −0.106, −0.098) while the data's does (−0.114, −0.067, −0.031): the signature of a misfit medium swing, which the paper's own Reddit work found to be "the main lever" ("the slower swing does most of the work"). At matched settings over the overlapping period Reddit gives −0.04 to +0.08 against Facebook's −0.11 to −0.03 (SI decomposition, last row; RESULTS A4, D10), so Facebook's weaker departure is not just "the horizon available" (Discussion l.234). Facebook's changes do not "persist" (significance statement): they reverse less than the model's passing swings imply.
*Fix now (text).* *Score:* +0.3.

**4. "Heterogeneous growth misses the asymmetry" is overstated, and Table 1 hides the counterexample.**
The abstract ("evaluated differences in mean growth miss this asymmetry"), the introduction (l.105) and the Discussion (l.234) say "the evaluated forms of heterogeneous growth do not produce" it. The decline state is such a form, and on comments it gives D = +0.125 [0.102, 0.146] against the data's +0.132 (`r17_asym_worlds__reddit_comments_ext__decline_*`). Table 1 shows only Reddit submissions, where the decline state gives +0.03. Only the body (l.194) has the right qualifier, "symmetric forms".
*Fix:* "no evaluated symmetric form; the one skewed form gives the sign but under half the persistence"; add comments rows to Table 1 or say why they are omitted. *Score:* +0.2.

**5. The common model is not yet common where it matters.**
Common: the structure, the estimator, the world construction, one code path per platform. Not common: (a) β on Facebook is a known-biased estimate (item 1; the SI's size table gives Facebook's structure-function exponent as 0.11, R18 as 0.097); (b) statistics at different settings (Facebook 26-week blocks, 4-week windows, 13-week crossing blocks; Reddit 52, 13, 26), so Table 1's rows are not comparable across platforms; (c) held-out movement (Results 4) comes from an earlier, panel-specific specification ("a conditioned state on Facebook and Reddit comments", the last step of an exploratory ladder, after 82 gate evaluations), the common model was never gate-tested on Facebook or Instagram, and it fails on submissions (0.183 against 0.163); (d) Reddit's constant-slope column uses size-blind dynamics with no rank drift, Facebook's the matched control; (e) the shape measure is a point level on Facebook (rank 54 above K) and U_k − U_K on Reddit.
*Fix now:* say exactly this in Methods and change Results 4's heading ("forecasts movement"). *Score:* +0.2.

**6. Development data only, one era.** D varies by origin 0.12–0.30 and 0.02–0.22, and between early and late origins (+0.17 against +0.28; +0.08 against +0.18). The 2019–22 window holds the 2020 surge, the June 2020 bans, the 2021 surges and a capture change, and nothing is confirmed. *Fix:* owner (sealed test). *Score:* +1 if confirmed.

**7. The lead and the evidence are misaligned, and the prose is dense.** The title leads with the least informative part, and the discoveries come last. The abstract has a 63-word sentence; Results 2 paragraph 3 (l.145) runs to about 290 words with five claims. *Fix:* text (§5, §7). *Score:* +0.3.

**8. "Size gives leaders little protection" is unscoped on comments:** the paper's own preferred estimator gives β = 0.13 on comments, "nearly unbiased" (l.148), against firms' 0.15–0.2 cited in the same sentence, and the SI gives 0.047–0.126 for comments depending on estimator. *Fix:* scope the claim to Facebook (0.02) and submissions (not distinguishable from 0); comments "weak, 0.05–0.13". Text; +0.1.

---

## 3. Errors and inconsistencies

| # | Location | Problem | Fix |
|---|---|---|---|
| E1 | main l.142 vs l.172, Methods l.245 | "Instagram supplies no other result here", but Instagram enters the held-out movement claim | delete the clause or drop Instagram from l.172 |
| E2 | main l.175; SI Fig. S1C title | "no better than carrying the previous year forward". SI table: submissions model 0.028 against carried 0.037 (model better), comments 0.022 against 0.013 (worse). Fig. S1C's title "replacement is reproduced" errs the other way | "better than carrying forward on submissions, worse on comments" |
| E3 | main l.115 vs l.178 | 2022 submissions head share 34.0% vs 36% | "36% in the forecast's universe", or harmonize |
| E4 | main l.172 | "the model" numbers (0.97, 0.55, 0.23, 0.30) are the matched control's; Table 1's "calibrated model" gives 0.98, 0.21, 0.27 and rank-1 0.61 | name the column |
| E5 | main l.145 | "the same control removes the faster turnover of its top 54 relative to its top 1,750": the calibrated model has none (0.210 vs 0.211), the matched control +0.02 against the data's +0.11 | see the §7 rewrite |
| E6 | significance; abstract | "both platforms' changes persist more than random growth allows": Facebook's changes reverse (−0.11 to −0.03), and pure random growth gives 0 (l.188) | "reverse less than its calibrated model allows" |
| E7 | abstract; l.105; l.234 | "evaluated differences in mean growth / forms of heterogeneous growth" do not produce the asymmetry; the decline state does on comments | add "symmetric" (item 4) |
| E8 | abstract | "On both platforms the hierarchy holds its shape" then "where a hierarchy flattened": the Reddit heads fell 0.6 log units and the share 50% to 34% | "changes shape far more slowly than its membership" |
| E9 | main l.188 | "starting 13 or 26 weeks after … +0.14 to +0.20": +0.20 is the gap-0 value; at 13 and 26 weeks the range is +0.14 to +0.17 | "0, 13 or 26 weeks" |
| E10 | main l.115 | "on comments almost all of it in 2019–20": −0.46 of −0.62 = 74% | "three quarters" |
| E11 | main l.159 | "about half of its long-run response within five years" uses a fitted amplitude with τ [3.8, 25.0] on submissions; at τ = 25 the share would be about 20% | finite horizon only: "0.55–0.59 log units in five years, about 63% of its ten-year move (0.87–0.94), and still moving" |
| E12 | main l.159 | "a rise of a fifth in volatility changes the shape little": true of the observed shape; the lasting shape on submissions moves +0.18 in five years and +0.34 in ten | "the observed shape" |
| E13 | Discussion l.234 | "by amounts that grow with the horizon available" is contradicted by the matched-settings row (item 3); "Instagram extends … Both depart" has an ambiguous antecedent | rewrite |
| E14 | main l.188, l.194 | R18-A1 bands quoted without "post hoc"; R18-5 and R18-6 predicted no departure | label them |
| E15 | SI Instagram paragraph | omits that the registered q-free per-post test failed (I2: 3 of 5 cut-offs; §2z-cx) | report it |
| E16 | Fig. 2B | the annotation "pooled ratio 0.94 (0.86–1.04)" is hard-coded (`make_v5_figures.py` l.215) and undefined; it is the SI's three-ladder shared-weight value | say so in the caption or read it from output |
| E17 | Fig. 2D caption | the worlds are calibrated to submissions and the comments data are plotted against them; Facebook is absent although R18 holds the same worlds | state this; add Facebook from `r18_summary.json` |
| E18 | main l.112/l.145 vs l.145/Table 1 | "top 1,728" vs "top 1,750" | harmonize |
| E19 | Methods "Estimators" | blocks "Reddit 52, Facebook 26", but crossings use 26 and 13 (SI §2) | add |
| E20 | main l.194 | "Homogeneous growth reverses both kinds of change (D = −0.07 and −0.05)": D does not show that | cite the p values (−0.17/−0.10, −0.14/−0.10) |
| E21 | Results 4 heading | "forecasts movement" rests on an earlier, panel-specific model (item 5) | "reproduces turnover, not the hierarchy's falls" |
| E22 | SI size table vs R18 | Facebook structure-function exponent 0.11 vs 0.097 | note which fit |
| E23 | trivial | comments τ is 6.5 in the SI table and 6.6 in RESULTS and the record (6.549) | 6.5 |

Checked and correct: the Table 1 cells and their in/above/below/edge codes; R17-A, B3, C and D as reported; the 48-cell reconstruction; the 11–13% span fall; the Gomez terms; the five-year flat start (52–59%); c/(s̄ − e) = 0.0040/0.0062 = 0.645; the excesses +0.41, +0.30, +0.12, +0.12; the abstract at 238 words and the significance statement at 107.

---

## 4. The asymmetry as the discovery

**Well defined and robust?** Yes on the paper's own checks, and they are good: positive at every origin; unchanged without collapses (+0.21, +0.13) and with absences at the floor (+0.23, +0.13); present in item counts (+0.10, +0.10), so not a karma-capture artefact; the same sign on ladders whose platform totals moved in opposite directions. Look-ahead: the strata use whole-window rank, but the protocol construction, with a cohort fixed in 2019 H1, gives +0.24 [0.16, 0.31] and +0.20 [0.15, 0.25] on karma. That is the paper's cleanest version and belongs in the main text, with the caveat that the universe itself was chosen on 2019–22.

**What it does not yet establish.** (a) Tails: it is a statement about the extreme tenths (item 2). (b) Mean reversion is handled only relative to worlds whose tails are wrong: in the homogeneous null itself p_fall < p_rise (−0.17 against −0.10), so about a sixth of the "excess after declines" (0.07 of 0.41) is the null's own convex tail. (c) The surge rival (item 2) is not among the alternatives P8 says it separates ("symmetric heterogeneity, the 2020–21 platform episodes, and mean reversion"); a stable skewed passing component would pass P8 too.

**Is "about three times" right?** Arithmetically yes (0.407/0.116 = 3.5; 0.298/0.121 = 2.5), but it is a ratio of differences of ratios, with no band, and it depends on the null: against the jointly estimated fixed differences the excess after declines is +0.15 and after gains −0.14 (submissions), so the ratio is undefined. "Two and a half to three and a half times" is the honest phrase; better, report the excess of D over each world (+0.29 and +0.18 over the homogeneous null) with bands.

**Novelty and framing.** The current sentence (l.197) is about right in strength but buried. Sign- and position-dependent persistence is known in earnings (Guvenen et al. 2021, cited; Arellano, Blundell & Bonhomme 2017, Econometrica), credit ratings (Lando & Skødeberg 2002, cited), equity momentum concentrated in losers (Hong, Lim & Stein 2000, J. Finance), firms' "shadow of death" (Griliches & Regev 1995, J. Econometrics), online communities' life cycles (Kairam et al. 2012, in the bib but uncited; TeBlunthuis et al. 2018), and attention bursts that relax (Wu & Huberman 2007, in the bib but uncited; Crane & Sornette 2008, PNAS; Candia et al. 2019, Nat. Hum. Behav.). Verify the uncited references before adding them. The defensible novelty is narrower: the pattern measured in attention hierarchies by one banded, rank-relative statistic, against a calibrated random-growth null that reproduces turnover, where symmetric heterogeneity calibrated to the persistence gives the opposite sign. Say "a pattern known in earnings, credit and firms, here measured in attention", not "discovery".

**What saved outputs can add now:** (1) the decile ratios and the quintile D (SI table; one main-text clause); (2) the tail sizes of x in data and worlds (the table in item 2); (3) the by-origin range in the main text; (4) the protocol-construction value in the main text; (5) the excess D over each world with bands; (6) the Facebook by-origin D (+0.016, +0.075, +0.065), which shows the Facebook value rests on two overlapping origins.

---

## 5. Framing

**Title.** The current title is declarative, but it claims the paper's least informative and, on Facebook, failed result. Options:
1. **"Turnover among the leaders of online attention follows the geometry of random growth"** (recommended; true on all three platforms, Facebook's 12 kinematic cells included).
2. "Leaders of online attention change often because the top of the hierarchy is flat" (rank-1 churn reproduced at 0.55 against 0.52, and R9).
3. "The leaders of online attention churn as random growth predicts, but their largest declines persist" (only after confirmation; the second half is Reddit-only).

**First page.** The contribution is clear by l.105, but stated as "the benchmark accounts for the leaders' turnover" plus "both depart in the same direction". The first should be the geometry; the second should be scoped: Reddit clear, Facebook a weak and post hoc hint.

**Abstract (247 words):**

> Attention on digital platforms is concentrated in a few pages, communities and accounts, yet the leaders change constantly. Among public Facebook Pages tracked by CrowdTangle (2020–2022), the page with the most interactions changes in half of all weeks and a fifth to a third of each top set is replaced within half a year; among Reddit communities (2019–2022), a fifth of each top set is replaced in a year. Each hierarchy changes shape far more slowly than its membership. We compute every statistic with the same code in the data and in worlds of one model calibrated to each platform. Turnover follows the geometry of random growth. Risers cross into each top set at the rate that volatility and the local slope predict (0.98 of prediction on Facebook, 0.80–1.07 on Reddit); the slope and the observed moves reconstruct the share replaced in all 48 cells; the smallest top sets turn over fastest because the hierarchy is flattest at the top; and the largest units are only slightly more stable than the rest. Two departures appear over Reddit's four years. Changes persist across years, which no homogeneous world produces, and the largest declines continue while the largest gains do not (follow-up ratios 0.24 against 0.02 on submissions, 0.16 against 0.03 on comments), whereas symmetric growth heterogeneity gives the reverse. And where a hierarchy flattened, it did so through its leaders' decline, which stable parameters do not forecast. Facebook's 86 weeks show only a weak sign of the first departure.

**Significance (116 words):**

> Claims that platforms entrench their leaders are rarely tested against what chance alone produces. Among public Facebook Pages, the page with the most interactions changes in half of all weeks, yet the hierarchy keeps its shape. On Facebook and Reddit this turnover follows the geometry of random growth: risers replace fallers at the rate set by volatility and the hierarchy's slope, the top turns over fastest because it is flattest there, and size offers leaders little protection. Against this benchmark, what is not chance stands out: on Reddit, the largest declines in community activity continue for years while the largest gains do not. The measurements need only a ranked panel, so they transfer to other platforms.

---

## 6. Instagram

**Slight net negative in the main text as written.** It supports only the relation the paper calls near-identical, on displacement variances at 14–28 weeks (a different quantity from the other ladders), for the body only (the head is unmeasurable, while the title is about leaders); its censoring-free version failed its registered expectation (I2), unreported; it produced the l.142/l.172 contradiction; and it invites a reviewer fight over keyword sampling. Kept to one clause ("and on Instagram accounts below the head, under a model of their keyword sampling; SI"), with I2 reported in the SI, it is a mild net positive for the owner's multi-platform frame, and that is how it should enter.

---

## 7. Structure and prose

**Results 2, paragraph 3 (l.145; about 290 words to about 250):**

> The slope that sets what newcomers bring also sets how fast members leave. A member leaves a top set when its move carries it below the set's moving boundary, whose distance from a typical member scales with S_k. From each interval's observed moves, boundary shift and disappearances, a formula for a locally log-linear hierarchy reconstructs the share replaced at 0.82–1.18 times observed in all 48 cells (Fig. 2A; Facebook's 12, 0.82–1.10). It is a contemporaneous check of turnover kinematics, derived after a registered linear form failed (SI Appendix, Eq. S2), not a test of random growth. Its implication is testable. The slope is flattest at the head on every ladder (0.7–0.8 at Facebook's top 54 against 1.2 at its top 1,750; 0.7–1.0 at Reddit's heads against 1.2–2.3 lower down), while typical moves change little with rank, so the smallest top sets turn over fastest. Geometry alone produces this ordering in simulation: with Reddit submissions' dynamics fixed, the data's ladder turns the top 78 over faster than the top 2,500 (0.21 against 0.14 a year) and a ladder of constant slope does not (0.19 against 0.18; paired difference 0.06, 0.03–0.09). The calibrated models reproduce the ordering on every ladder but not its size: the head's excess over the top K/2 is 0.02 in Facebook's model against 0.11 in its data, and about twice the data's on Reddit (Table 1; SI Appendix). Facebook's leading page changes in half of all weeks and Reddit's seldom, because Facebook's leaders sit close to their neighbours relative to their weekly moves; the models reproduce both (0.55 against 0.52; 0.10 against 0.07).

(The cultural-evolution exponent sentence moves to the SI.)

**Asymmetry paragraph (l.194; about 330 words to about 260):**

> None of the evaluated symmetric forms reproduces how persistence depends on the sign of a change (Fig. 3 B and C). Sort communities by their year-long change in log activity relative to communities of similar rank, and ask what share of it continues a year later. For most changes the share is similar in both directions (0.16 and 0.18 in the second and ninth tenths on submissions; 0.16 and 0.15 on comments). The extremes differ. The largest tenth of declines continues (p_fall = +0.24 and +0.16); the largest tenth of gains, which are larger than the largest declines (mean +1.60 against −1.12 on submissions), does not (p_rise = +0.02 and +0.03). Their contrast, averaged over six origins, is D = +0.22 [0.18, 0.25] and +0.13 [0.12, 0.15], and +0.15 and +0.10 with quintiles. It is positive at every origin, without collapsed communities, with absences counted at the floor, in item counts (+0.10 on both ladders), and on a cohort fixed in the first half of 2019 (+0.24 and +0.20, one pair). Homogeneous growth gives D = −0.07 and −0.05, and fixed growth differences estimated jointly with the passing swing give −0.07 and −0.03: in both, gains persist at least as much as declines. The worlds' tails are symmetric and thinner than the data's, so the comparison is clearest in sign; transient, positively skewed surges could also produce it (SI Appendix). On submissions the asymmetry lives below the head (D = −0.00 in the top K/16); on comments it holds at every rank. On Facebook's half-year changes D is +0.05 (−0.01 to +0.10; post hoc bands) against −0.01 in its own model.

**Discussion, sentence 3 onward (l.234):**

> Reddit departs from it clearly over four years: changes persist across periods that share no weeks, and the largest declines continue while the largest gains do not. Over Facebook's 86 weeks, changes reverse less than its own model's passing swings imply, a post hoc finding that a slower swing could also explain; at matched settings Reddit's persistence still exceeds Facebook's.

**Cuts to stay at 7 pages (about 170 words):** the time-scale paragraph (l.159) to one finite-horizon sentence (about −70); the cultural-evolution sentence (about −40); merge l.178 into l.175 (about −30); shorten the capture sentence l.184 (about −30).

---

## 8. For the owner — new runs and data, ranked by value

1. **R18 with Facebook's size law corrected** (β by indirect inference from its 0.045 bias, about 0.05, or the two-step 0.02; check the starting ladder against the first block). Light (R18's runs took seconds per batch). It tests whether the Facebook head failures (R18-1, R18-8) are the size law; if so, they turn into support for "size gives no protection".
2. **Adopt the bridge protocol and run P8 on the sealed 2023–24 Reddit data**, after adding a discriminator for the surge rival (D with x from 26-week window means; the tail-size ratio of x as a co-registered statistic). Reconsider karma as primary: in the protocol's construction item counts give an uninformative +0.03 [−0.03, +0.10] on submissions.
3. **A world with a positively skewed passing component** (surges), added to the joint fixed differences and calibrated without targeting D: does it give D > 0 and the data's tail skew? A moderate run. It decides whether the asymmetry is "declines persist" or "surges fade".
4. **Reddit's matched homogeneous worlds at Facebook's settings** over the overlapping period, to make the cross-platform persistence comparison like for like (light to moderate).
5. **Facebook data:** a longer, instrument-consistent Facebook history — either the later CrowdTangle eras already held, if they can be bridged without a new source, or new data. The only route to heterogeneity and slide measurements on the central platform.
6. **YouTube and Wikipedia panels** under the same code: valuable for the frame, new data, later.
7. **Fuller joint calibration** with every spread free: low value, since the joint form is already rejected and D is untargeted.
