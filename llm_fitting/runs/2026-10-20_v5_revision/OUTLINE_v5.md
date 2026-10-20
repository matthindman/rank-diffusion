# Manuscript v5: structural outline (draft 2)

_Claude Opus 5.5, 2026-10-20._

Draft 1 is at aeda027. Draft 2 follows from:
- `CRITIQUE_OUTLINE_gpt-6-astra.md` and `CRITIQUE_OUTLINE_opus-5.5.md`;
- the correction to the round-6 synthesis, appended to SYNTHESIS.md;
- `MEMO_PNAS_POLICY_opus-5.5.md`;
- the owner's directions of 2026-10-20.

## What changed from draft 1, and why

1. **"About eight times slower" is an identity of the benchmark, not a measurement** (Opus). It becomes three statements:
   - **The model property.** Under homogeneous random growth, the shape relaxes about eight times more slowly than the time a member's lasting movement takes to cross one e-fold of rank. A member placed uniformly in a top set lies one e-fold, on average, from its boundary.
   - **The empirical check.** That movement time, 2–4 years, agrees with the observed replacement of a sixth to a quarter of each top set a year.
   - **The levels.** 13–31 years, model-implied. They are an upper bound, because exits are omitted.
2. **The fall is a departure in speed, not direction.** The balance below one predicts continued deconcentration (Opus).
   - The speed comparison uses one estimand: the head's share path against the 20-seed predictive band of the calibrated homogeneous model, plus the members' Gomez term against the noise baseline.
   - "Order of magnitude" is dropped.
   - The R4 heading becomes "…faster than calibrated homogeneous growth predicts" (GPT).
3. **Where fast change comes from is narrower than draft 1 claimed** (synthesis correction). The rate law with the measured 2020–22 slide accounts for the head's fall. What homogeneous growth with stable parameters cannot do is change its slide: the members' decline quadrupled after 2020. GLLM's lesson in its narrow form: a fast change in concentration needs the growth process to change in a way that is not homogeneous across communities.
4. **No causal link anywhere** (both). Persistence can supply speed in calibrated worlds; we have not shown that it did here. Results 5 is written to stand whichever way R12 comes out.
5. **Durability opens Results 2** (both): as its first sentence, and Fig. 2A.
6. **R2's numbers depend on heterogeneity** (Opus, GPT).
   - The composition term (25–35% of the crossing statistic on Reddit) stays in R2, with a pointer to R5.
   - The R12 recovery of σ_k sits beside the time scales. This is essential, not optional.
7. **The R1 geometry paragraph moves into R2** (Opus). One slope S_k sets both how fast members leave (Eq. 1) and what newcomers return (Eq. 2). This gives individual lock-in a theoretical home, and stops R1 from using a model it has not introduced.
8. **Figures** (both):
   - The old Fig. 3C (balance reached against observed) repeated Fig. 2B. It moves to the SI, and its failure stays in the text.
   - Fig. 2C plots the model's time scales against **observed** retention and shape change.
   - Fig. 3B draws 0–30 years with the first five years shaded and the rest labelled model-implied.
   - A new panel shows the projection band widening at 1, 2 and 5 years, to show noise compounding rather than assert it.
   - Fig. 4C adds Facebook and the factor-adjusted series.
9. **Budgets.** Draft 1 summed to 5,100 words, against its own target of 3,900–4,300. Draft 2 targets 4,300 words, with more and shorter paragraphs.
10. **The precursor** (PNAS memo). PNAS review is single-anonymous only, with author names on the title page. So the 2018 chapter is cited openly in neutral numbered form in both versions: in Intro ¶2, with its two contrasts in Results. `\iffinal` switches voice (first person, plus a one-sentence through line in the Discussion), not content. The cover letter states the relationship.
11. **Additions** (Opus, gaps):
    - a paragraph on where the decline comes from, what is known and what is not identified;
    - Facebook's role stated;
    - a 1–2-year backtest beside every medium-term statement (fit on 2019–20, project 2021–22);
    - the model eligibility rule in Methods;
    - the four unplaced references placed.
12. **The sealed test.** Forecasts from every eligible model are an owner-gated protocol amendment. Discussion ¶5 mentions them as proposed, not as a promise.

## Design rules (unchanged unless noted)

- **One question per subsection,** answered by one comparison, ending with what the comparison leaves open.
- **Declarative, scoped headings.**
- **Horizons.** 1–2 years for individual communities; up to 5 years for the system. Decades only as model-implied rates. Noise compounding is shown.
- **Sentence-length distribution near the exemplars':**

  | | median | 90th percentile | share over 40 words |
  |---|---|---|---|
  | target | about 22 | ≤ 45 | ≤ 12% |
  | v4 | 30 | 63 | 32% |

  One inferential move per sentence. Numbers follow claims.
- **Four figures** with two panel groups in Fig. 4. Captions of about 110 words.
- **Rivals sit beside their claims.** The Discussion synthesizes them; it does not re-summarize.
- **Every model is a column.** Main-text Table 1 of facts × models, coded by where the data fall in each world's 5–95% band. It replaces most of the rival-readings prose. The SI atlas holds the detail.

## Front matter

**Title** (owner to confirm): "When the hierarchy of online attention outlasts its members". Alternatives stay in comments.

**Significance** (≤ 120 words), in five sentences:
1. Stakes.
2. Puzzle.
3. A standard random-growth model, measured, implies the hierarchy's shape should change far more slowly than its members turn over.
4. On Reddit it does where the balance holds, but the head of submissions fell faster than calibrated homogeneous growth predicts, as communities' declines persisted.
5. This gives platform research a benchmark for claims of entrenchment, and a scale for how fast concentration can respond within the model.

**Abstract** (about 210 words; 4–5 numbers), in seven sentences:
1. Context.
2. Data.
3. Measurement: crossings return two thirds of what members lose.
4. Durability as the benchmark's property, checked against observed turnover.
5. The model built from the measured ingredients and its limits.
6. The head's fall: in speed, not direction.
7. Persistence, and the calibrated heterogeneous worlds. Written after R12 is scored.

## Introduction (no heading; 4 paragraphs, about 500 words)

| ¶ | function |
|---|---|
| 1 | Stakes and puzzle. Concentration (webster2012; yang2020; hindman2009). Items decay ever faster (wu2007; lorenzspreen2019). Leading communities are replaced each year. Households keep a stable allocation of attention while the sites they visit change (boik2019). The question. |
| 2 | What theory says (one sentence per tradition; mechanisms in parentheses with citations). Random growth with a stabilizer gives a stationary hierarchy (gabaix1999; batty2006). The rank-based balance (fernholz2002; banner2005; fernholzfernholz2020). Homogeneous growth moves inequality slowly (gllm2016). Earlier work on web traffic proposed that churn maintains concentration, modelled with stochastic portfolio theory (hindman2018, ch. 5). |
| 3 | The gap. Platform studies rarely use this theory, so entrenchment and winner-take-all claims lack a benchmark. Does the theory hold rank by rank in attention data, where weekly movement is mostly passing? What moves the hierarchy when it moves? |
| 4 | Here we. Approach, then three findings, one sentence each. |

## Results

### R1. "Reddit's leading communities turn over while the shape of the hierarchy changes slowly" (about 400 words, 4 paragraphs; Fig. 1)

| ¶ | function |
|---|---|
| 1 | Data. The three ladders. Activity is engagement, not exposure (C7.3). Facebook's role: an instrument check and a persistence contrast. |
| 2 | Turnover. A sixth to a quarter of each top set is replaced in a year; 27–33% of the top ten. Retention over one and two years (R13c, if the drive allows). |
| 3 | Shape. Changes of at most a third of a log unit a year against a span of 4–5. Concentration fell, fastest at the head of submissions (stated here as a fact, carried to R4). Speed is judged against the benchmark's rate (R2). |
| 4 | Leaders keep rank while losing share (named; rank alone is insufficient). Bans as discrete removals (cima2024). The web's top ten rarely changed (hindman2018). |

**Fig. 1:**
- **A** the 2019 ladder with the top-k boundary and the three flows annotated with measured magnitudes; defines ladder, top set, slide and crossing.
- **B** rank–size curves by year.
- **C** share replaced by k, by year pair.
- **D** the two leaders' rank-one share of weeks against their share, by year.

### R2. "Random growth, measured rank by rank, implies a hierarchy that outlasts its members" (about 700 words, 6 paragraphs; Fig. 2)

| ¶ | function |
|---|---|
| 1 | **The claim first.** In random growth with a drift that holds the leaders back, the shape relaxes about eight times more slowly than members move across a top set (GLLM, with applicability). The intuition: crossings and decline nearly offset. |
| 2 | The accounting. Eq. 2 (crossing gain σ²/2S) and Eq. 3 (rate law); S* as the Atlas slope (fernholzfernholz2020 eq. 2.17); "what we measure". Baker & Smyth's turnover is our e_k (bakersmyth2026). |
| 3 | Measuring it. Cohorts by permanent rank, lasting variance (Methods). Crossings match (0.94). An instrument, not a test (worlds 1.04–1.12; jumps excluded). Composition term 25–35% on Reddit, pointing to R5. Recovery under heterogeneous growth (R12c). |
| 4 | One slope sets both exit and replenishment. Eq. 1 in words, with the controlled geometry comparison (R9). Turnover profiles need a ladder control (acerbi2014; R13e). Lasting β is 0.05–0.13, below firms. Unlike the funnel described for web traffic (hindman2018), size gives little protection (R13d, if run). |
| 5 | The balance. c/(s̄ − e) = 0.64: the ladder is steeper than its balance, so the benchmark predicts continued deconcentration. Net accounting: turnover holds up comments and pulled the submissions head down. |
| 6 | **The check and the levels.** Lasting movement crosses a top set in 2–4 years, consistent with observed replacement. Relaxation toward the balance takes 8S*²/σ²: about 9 years at the comments head, 3–4 at the submissions head, where the slide is large. Over 5 years the benchmark moves the shape by [R13a band]. Model-implied, not forecasts. |

**Fig. 2:**
- **A** time scales by rank: model relaxation (8S*²/σ² and 8S²/σ²) against observed membership turnover.
- **B** measured against predicted crossings.
- **C** crossing gain against slide (the balance), with the worlds.

### R3. "Built from the measured ingredients, a model forms a hierarchy, but a flatter one at the head" (about 500 words, 4 paragraphs; Fig. 3)

| ¶ | function |
|---|---|
| 1 | The model in words, then Eq. 4. Parameters from declared moments. One of the eligible models (Table 1; SI atlas). |
| 2 | From equality, the measured ingredients build most of the head's span within five years. Over five years from 2019, removing volatility or the slide moves the head by a few percent on comments and by 9–27% on submissions. The decades-long continuation is drawn shaded and labelled model-implied. |
| 3 | Projections widen. Bands at 1, 2 and 5 years; the 2021–22 backtest from 2019–20 (retention, head share). |
| 4 | Limits. The balance it reaches is 37–41% flatter at the head and matches the comments body (R11). Held-out movement is no better than simpler models. |

**Fig. 3:**
- **A** from equality, 0–10 years, with the first 5 emphasized.
- **B** from the 2019 ladder, 0–30 years, the first 5 shaded, with three scenarios.
- **C** the projection band at 1, 2 and 5 years, and the backtest.

### R4. "The head of submissions fell faster than calibrated homogeneous growth predicts" (about 400 words, 3 paragraphs; Fig. 4A–B)

| ¶ | function |
|---|---|
| 1 | The share fell from 50% to 34% (identical populations) against the homogeneous predictive band. The members' Gomez term (−0.24, −0.26 a year) against the noise baseline (−0.05). |
| 2 | The rate law with the measured 2020–22 slide accounts for it. The slide itself rose after 2020; homogeneous growth with stable parameters does not change its slide (GLLM, narrow form). Rival (c): the surge, the ban wave, platform effects (lazer2014; veselovsky2023). |
| 3 | Where it came through: posting halved while engagement per post rose, and platform-wide submissions fell below trend. No causal claim. |

### R5. "On Reddit, changes persist beyond homogeneous growth" (about 550 words, 5 paragraphs; Fig. 4C–D)

| ¶ | function |
|---|---|
| 1 | The statistic: +0.13 to +0.20 against −0.02 to −0.10. Robust to the surge and the capture change, each alone. Facebook is negative. |
| 2 | Three readings: fixed heterogeneity (HIP; second-order rank models, fernholzichibakaratzas2013), decaying heterogeneity (kalyuzhny2015; mallmin2026), and common-factor exposure. The lag profiles and factor adjustment point to factor exposure on submissions and lasting differences on comments. |
| 3 | Calibrated worlds (R12), each calibrated from moments it is not tested on. Results on the lag profile, R10, the head's path and recovery. **Written after scoring;** both outcomes outlined. |
| 4 | Prior decliners carry 40–60% of the excess slide (R10, post hoc; the failed expectations stated). |
| 5 | What persistence is and is not. It is in participation; in decline; the life cycle (halfaker2013; teblunthuis2018). We have not shown that the head's decliners are the persistent decliners. |

**Fig. 4:**
- **A** the head's share path against the homogeneous band and the heterogeneous worlds.
- **B** items × per-item split.
- **C** the gapped correlation against the gap: data (Reddit, Facebook, factor-adjusted) and worlds (homogeneous, heterogeneous).
- **D** slide by prior-change class.

## Table 1 (main text): facts × models

**Rows:**
- Eq. 1 holds;
- crossings fall short of the slide;
- the boundary ratio;
- turnover at K/64 against K/2;
- the top ten;
- gapped persistence;
- the prior-decliner contrast;
- the head's 2020–22 path;
- held-out movement.

**Columns:**
- size-blind and firm-like growth on a constant-slope ladder;
- the same two on the data's ladder;
- neutral copying;
- the calibrated model;
- the heterogeneous worlds (R12).

**Cells:** whether the data lie within the world's 5–95% band, coded inside, above or below, or "not run".

## Discussion (5 paragraphs, about 650 words)

| ¶ | function |
|---|---|
| 1 | What we found. Durability is what plausible random growth implies, checked against turnover. Where concentration moved fast, the slide changed. Persistence can supply such speed in calibrated worlds; we have not shown that it did. |
| 2 | Where the decline comes from: what is known (finite attention, normalization against a re-selected universe, ageing and life cycles, moderation, platform effects) and what the paper cannot identify. |
| 3 | **Uses and non-uses**, as pairs backed by this paper's own evidence. Uses: a benchmark for entrenchment claims; retention at 1–2 years; decomposition of a change in concentration; how fast concentration responds within 5 years. Non-uses: individual forecasting (the gate), mechanism from shape or identity (worlds 1.04–1.12), decades ahead (drift from 2020–22), intervention effects (huszar2022). |
| 4 | Limits: data, karma against exposure, capture, horizons. |
| 5 | Outlook: the sealed 2023–24 test (protocol proposed; forecasts from every eligible model, if adopted); other platforms. Final version: the through line from the 2018 chapter. |

## Materials and Methods (about 600 words)

- Data.
- Universe and permanent rank.
- Estimators.
- Models, the eligibility rule and the atlas.
- Heterogeneous calibration.
- Registration.
- AI use: tools and versions, review models, figures rendered by matplotlib code with no generative image tool, accountability.
- Data availability.

## SI additions and moves

- **Atlas:** manifest, diagnostics and eligibility.
- **Parameter provenance.**
- **R12:** specification, estimation and recovery.
- **Time scales:** the GLLM table, applicability and exits.
- **5-year projections and backtest.**
- **Size dependence:** raw against lasting β, calibration and the funnel quantiles.
- **C(R) and the ingredients of Eq. 1.**
- **Held-out movement and retention.**
- **Uses and non-uses table.**
- **Turnover profiles (R13e).**
- **The model's balance against observed** (old Fig. 3C).
- **Relation to the 2018 chapter** (final version).
- **Failures and post hoc inventory:** R10, R11, R12, R13.

## Budget (4,300 words of main text plus about 450 of captions)

| Intro | R1 | R2 | R3 | R4 | R5 | Discussion | Methods |
|---|---|---|---|---|---|---|---|
| 500 | 400 | 700 | 500 | 400 | 550 | 650 | 600 |
