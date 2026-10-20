# Manuscript v5: structural outline (draft 1, before writing)

_Claude Opus 5.5, 2026-10-20._

**Sources:**
- the owner's directions of 2026-10-20 (MODEL_STATUS §2z-dm);
- the round-6 synthesis (`runs/2026-10-19_literature_and_design_reviews/SYNTHESIS.md`);
- the exemplar counts in REPORT_T3 (both models), and the sentence-length distributions in `sentence_lengths_output.txt`.

The owner expects a human rewrite of the prose. The **structure** (sections, subsections, paragraphs and the function of each) is what must be right here.

## Design rules

1. **One question per subsection, answered by one comparison.** Each subsection states what that comparison leaves open (T3 GPT). Headings are declarative claims, as in George, Mallmin and Sireci; there are no "What/Where" headings.
2. **The argument follows the GLLM arc,** owner-approved 2026-10-20:
   - (i) the puzzle;
   - (ii) a standard, plausible model, measured rank by rank, implies that the hierarchy outlasts its members. That property is not widely known in platform studies, and it is a contribution.
   - (iii) the model built from the measured ingredients, and what it can and cannot do;
   - (iv) where the data depart: the submissions head moved an order of magnitude faster than homogeneous growth allows;
   - (v) what supplies the speed: persistent differences in growth, tested against calibrated heterogeneous worlds.
3. **Horizons** (owner).
   - Results are stated at 1–2 years for individual communities and at up to 5 years for the system.
   - Relaxation times of decades appear only as model-implied rates, labelled as such, never as forecasts.
   - Noise compounds in every simulation, and the text says so.
4. **Density.**
   - Sentence-length distribution near the exemplars': median about 22 words, 90th percentile ≤ 45, no more than about 12% over 40 words. v4 has median 30, 90th percentile 63, 32% over 40.
   - One inferential move per sentence. Numbers in the sentence after the claim, not inside it.
   - At most about four numbers in the abstract.
5. **Four figures,** each answering one subsection's question. Captions of about 110 words.
6. **The main text is about 3,900–4,300 words** of narrative and Methods, targeting 7 pages.
7. **Rivals sit beside the claim they threaten.** A Discussion paragraph assembles them (Sireci).
8. **Every model is a column** (owner-approved). The calibrated random-growth model is one of several plausible, eligible models tuned to the same data. Its distinct role is the counterfactuals. The SI carries the atlas.
9. **The precursor.** The lead author's 2018 chapter is cited in the third person in the submission (anonymity-safe). The final version adds an explicit through line. A LaTeX switch, `\iffinal`, holds both texts.

## Front matter

**Title** (owner to confirm): "When the hierarchy of online attention outlasts its members". The alternatives stay in source comments.

**Significance** (≤ 120 words; no more than two numbers), in five sentences:
1. Stakes.
2. The puzzle.
3. What a standard model implies.
4. Where the data depart.
5. Why it matters for platform research: a benchmark for claims of entrenchment, and the time concentration takes to respond.

**Abstract** (about 210 words; at most 4–5 numbers), in seven sentences:
1. Context.
2. Data.
3. The measurement and the balance.
4. Durability as a property of the measured random growth.
5. The model, and its limits.
6. The fast fall at the head as the departure.
7. Persistence, and how a calibrated heterogeneous world fares.

## Introduction (no heading; 4 paragraphs, about 600 words)

| ¶ | function | content | key refs |
|---|---|---|---|
| 1 | stakes and puzzle | Attention online is concentrated, and concentration bears on which voices are seen and on entrenchment. Items decay fast and ever faster. Leading communities are replaced every year, yet the curve changes slowly. The question. | webster2012, yang2020, hindman2009 (third person), wu2007, lorenzspreen2019 |
| 2 | what theory implies | Random growth with a stabilizing force gives a stationary hierarchy, as for cities and firms. Rank-based diffusion states the balance at each rank: steepness = volatility / decline (Atlas, Fernholz & Fernholz). GLLM: homogeneous random growth moves inequality slowly, too slowly for observed changes. Earlier work on web traffic proposed that churn maintains concentration and modelled it with stochastic portfolio theory. Mechanisms are split by citation (barrier, drift, cumulative advantage). | gabaix1999, batty2006, fernholz2002, banner2005, fernholzfernholz2020, gllm2016, hindman2018 (third person), bakersmyth2026 |
| 3 | the gap | Platform studies rarely use this theory, so claims about entrenchment and winner-take-all lack a benchmark. Whether its predictions hold rank by rank in attention data, where most weekly movement is passing, is unknown. So is what moves the hierarchy when it does move. | (brief) |
| 4 | here we | Data and approach in one sentence: identical estimators in data and in a family of models. Three findings, one sentence each. | — |

## Results

### R1. "Reddit's hierarchy of attention changes slowly while its members turn over" (about 500 words; Fig. 1)

| ¶ | function | content |
|---|---|---|
| 1 | the data and the puzzle in numbers | The three ladders. A sixth to a quarter of each top set is replaced in a year; the shape moves by at most a third of a log unit a year against a span of 4–5. Concentration fell. |
| 2 | individual lock-in | Leaders kept rank one while losing more than half their share (named). One- and two-year retention by rank (new: R13c). Rank alone is insufficient to measure persistence of advantage. Bans as discrete removals (cima2024). |
| 3 | geometry sets which sets turn over fastest | Eq. 1 verbally or compact, and the controlled comparison (R9). Turnover profiles in cultural evolution need a ladder control (acerbi2014). The worlds also miss the top ten and the body at K/2. |

**Fig. 1:**
- **A** the 2019 ladder with the top-k boundary and its three flows annotated with measured magnitudes (slide, crossings, entry and exit). A data figure, not an illustration.
- **B** rank–size curves by year.
- **C** the share replaced by k, data against a constant-slope ladder and the data's ladder under identical dynamics.
- **D** the leaders' rank against their share.

### R2. "Random growth measured rank by rank implies a durable hierarchy" (about 850 words; Fig. 2)

| ¶ | function | content |
|---|---|---|
| 1 | the accounting | Eqs. 2–3 (crossing gain, rate law). S* as the Atlas slope, cited by equation. "This is what we measure." |
| 2 | measuring it in noisy data | Cohorts by permanent rank; lasting variance from spacing. Crossings match the prediction (0.94). It is an instrument, not a test of mechanism (worlds 1.04–1.12); jumps are excluded. |
| 3 | the balance | c/(s̄ − e) = 0.64: the ladder is steeper than its balance. The net accounting: turnover holds up comments and pulled the submissions head down. |
| 4 | **durability follows** (the contribution) | At the measured σ and S, shape relaxation (8S²/σ², 13–31 years) is about eight times slower than the time lasting moves take to cross a slope unit (2–4 years). These are model-implied rates, not forecasts. Over 5 years the balance moves the head's shape by at most X. Comments' linearized relaxation (about 9 years) agrees. This property of plausible random growth is what makes the hierarchy durable. |

**Fig. 2:**
- **A** measured against predicted crossings, with the worlds.
- **B** crossing gain against slide by cell.
- **C** time scales by rank: shape relaxation against membership time, with the 2020–22 change at the submissions head marked against them.

### R3. "A model built from the measured ingredients forms the hierarchy and holds it" (about 550 words; Fig. 3)

| ¶ | function | content |
|---|---|---|
| 1 | the model | Eq. 4 in words, then the equation. Parameters from declared moments; hundreds of profile values, none fitted to the test statistics. The calibrated random-growth model is one column among the plausible models (SI atlas). |
| 2 | what the ingredients do, at 5 years | From equality, the measured ingredients build most of the head's span within five years. From the 2019 ladder, five years without volatility and five years without the slide. The decades-long continuation goes in one clause, labelled model-implied. |
| 3 | where it falls short | The balance it reaches is 37–41% flatter than observed at the head and matches the comments body (R11). Its held-out movement is no better than simpler models'. Retention a year ahead is comparable. It does not forecast the head's fall. |

**Fig. 3:**
- **A** from equality, the first 5–10 years.
- **B** five years without volatility or without the slide, from the 2019 ladder.
- **C** the balance reached against the observed slope.

### R4. "At the head of submissions, concentration fell faster than homogeneous random growth allows" (about 400 words; Fig. 4A–B)

| ¶ | function | content |
|---|---|---|
| 1 | the fast fall | The head's share fell from 50% to 34% in three years. The homogeneous model forecasts 0.53–0.54. The members' Gomez term (−0.24, −0.26 a year) against the noise baseline (−0.05). That is GLLM's fast-transition problem. |
| 2 | where it came through | The head posted half as much while the top 5,000 posted more, and engagement per post rose. On the platform, submissions fell below trend after 2020 (veselovsky2023). No causal claim. |

### R5. "Decline persists, and communities differ in their growth" (about 750 words; Fig. 4C–F)

| ¶ | function | content |
|---|---|---|
| 1 | the statistic | Correlation of changes that share no weeks: +0.13 to +0.20 on Reddit, against −0.02 to −0.10 in every homogeneous world. Robust to the surge and to the capture change, each taken alone. |
| 2 | three readings and the calibrated worlds (R12) | Fixed heterogeneity, decaying heterogeneity, and common-factor exposure. Each is calibrated from moments it is not tested on. Results: lag profile, R10, the head's path. |
| 3 | prior decliners carry the excess | R10: 40–60%, post hoc, with the failed expectations stated. |
| 4 | what persistence is, and is not | Concentrated in participation (item counts); in decline more than growth. Life-cycle literature (halfaker2013, teblunthuis2018). We have not shown that the head's decliners are the persistent decliners. |

**Fig. 4:**
- **A** the head's share path: data against the homogeneous and heterogeneous worlds.
- **B** the items × per-item split.
- **C** the gapped correlation against the gap: data, homogeneous worlds, heterogeneous worlds.
- **D** the slide by prior-change class: data against worlds.

## Discussion (5 paragraphs, about 800 words)

| ¶ | function |
|---|---|
| 1 | What we found, in plain terms. Durability is what a standard random-growth model implies at the measured parameters; the data depart where concentration moved fast, through persistent decline. |
| 2 | **What is automatic and what is not.** Rivals (a)–(d), each with the result that answers it (Sireci). |
| 3 | **What this kind of model is for in platform studies, and what it is not for:** clear wins and the uses to avoid (from the uses memo). |
| 4 | Limits: data, platform effects, karma against exposure, horizons and the compounding of noise. |
| 5 | Outlook: the sealed 2023–24 test with forecasts from every eligible model; other platforms. (Final version only: the through line from the 2018 chapter.) |

## Materials and Methods (about 650 words)

- Data.
- Universe and permanent rank.
- Estimators: lasting variance, crossings, the gapped statistic.
- The models and simulated worlds: atlas summary, heterogeneous calibration.
- Registration and AI use: precise about figures.
- Data availability.

## SI (additions and moves)

- **Model atlas:** manifest and diagnostic table.
- **Parameter provenance** table.
- **Heterogeneous growth (R12):** specification, estimation and recovery.
- **Time scales:** the GLLM table and the 5-year projections.
- **Size dependence:** raw against lasting β, calibration, and quantiles by stratum (the "funnel" check).
- **Rank change C(R)**, and the ingredients of Eq. 1.
- **Held-out movement and retention** tables.
- **Uses and non-uses** table.
- **Relation to the earlier web-traffic analysis** (final version).
- **Registration and failures list:** R10 and R11 added.
