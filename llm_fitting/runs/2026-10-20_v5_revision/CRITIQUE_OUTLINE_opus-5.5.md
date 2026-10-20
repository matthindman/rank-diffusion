<!-- Provenance: critique of OUTLINE_v5.md (draft 1), Claude Opus 5.5 sub-agent, read-only, 2026-10-20; brief BRIEF_OUTLINE_REVIEW.md. Saved verbatim by the lead agent. -->

**Critique of OUTLINE_v5 (Claude Opus 5.5, read-only).** I read the brief, the outline, v4, the synthesis, both T3 reports, the sentence-length output, the R13 addendum in PLAN.md, gllm_relaxation_check.py and its output, and MODEL_STATUS §2z-dl. I opened no data and nothing listed in SEALED_PATHS.json.

**Verdict.** The order of the argument, which follows GLLM's arc, is right and should stay. Six problems should be fixed before any prose is written. The first is the one a theorist referee would raise first.

## Ranked changes

1. **"About eight times slower" (R2 ¶4) is an identity, not a measurement.** 8S²/σ² divided by S²/σ² is 8 at every rank by construction. You can see this in gllm_relaxation_check.py, and R13b restates it. Presenting it as holding "at the measured σ and S" reads as circular. Rewrite ¶4 as three statements:
   - **(a) The property of the model.** In homogeneous random growth, the shape relaxes 8 times more slowly than a typical member takes to cross one e-fold of rank, whatever the parameters (GLLM). For a member placed uniformly in rank within the top k, the mean of log(k/r) is exactly 1. So S²/σ² is the typical time for a member to reach the boundary, and "outlasts its members" becomes a property of the benchmark model.
   - **(b) The test.** Compare the model's S²/σ² (2–4 years) with the *observed* time members stay in each top set (R1 turnover and R13c). This comparison is what makes the model one readers can accept.
   - **(c) The levels.** 13–31 years, labelled model-implied, and the 5-year bound X.
   - GLLM's convergence rate adds a death rate δ. The check script leaves out exits, so 13–31 years is an upper bound. Say so, or include δ.

2. **Use one yardstick for "slow" and "fast."** The R1 heading says the hierarchy "changes slowly". The R4 heading says it fell faster than random growth allows. Both are true, but against different yardsticks: a span of 4–5 log units, and the model's rate.
   - State in R1 that speed is judged against the benchmark's rate.
   - State the submissions head's fall as a fact in R1, so the reader carries it forward.
   - "An order of magnitude" (design rule 2(iv)) is not supported at the head. GLLM gives 13.2 years at K/64 on submissions, against the data's linearized 3–4 years: a factor of about 4. The factor is 5–9 only against the model's 18–27 years.
   - Quote one comparison that matches the estimand: the head's share path against the 20-seed homogeneous band, plus the members' Gomez term (−0.24, −0.26) against the noise baseline (−0.05).

3. **R2 ¶3 should say that the balance predicts the direction of the fall.** c/(s̄−e) = 0.64 means the ladder is steeper than its balance, so homogeneous growth predicts continued deconcentration.
   - That makes R4 a departure in *speed*, not direction, which is exactly GLLM's frame.
   - It also answers the owner's medium-term question: slow flattening toward S*, by X in 5 years, with bands.
   - The outline has all the pieces but never draws this link.

4. **The causal link removed in C1 comes back.** It appears in Discussion ¶1 ("the data depart where concentration moved fast, through persistent decline") and in design rule 2(v) ("what supplies the speed").
   - R5 ¶4 itself says the head's decliners have not been shown to be the persistent decliners.
   - Even if R12 succeeds, it shows that persistence is sufficient (Juul's framing), not that it caused this fall.
   - Suggested wording: "persistent differences in growth can supply such speed in calibrated worlds; we have not shown that they did here."

5. **R2's numbers depend on what R5 finds, and the outline hides this.**
   - Heterogeneous growth inflates how variance grows with spacing (σ²h + σ_β²h²; G15).
   - v4 states that 25–35% of the measured crossing statistic on Reddit is a composition term coming from persistent differences in growth. The outline drops that sentence.
   - Keep it in R2 ¶2 with a pointer forward to R5, and report R12's recovery check of σ_k next to the time scales. Otherwise the durability numbers rest on the homogeneity that R5 rejects.

6. **R12 has not been run or registered, and R13 has not been run.** Several parts of the outline are written around results that do not exist yet:
   - R5 ¶2–3;
   - Fig. 4A, C and D;
   - abstract sentence 7;
   - the 5-year values in R3 ¶2 (R13a; X is still a placeholder).

   Register R12's expectations in PLAN.md first. Outline both outcomes: heterogeneous worlds reproduce the head's path, or they do not. Write R5's heading and abstract sentence 7 only after the results are scored.

## Q1. Argument

Keep the order R1→R5. Changes:
- **R1 ¶3 (geometry) relies on a model not yet introduced.** It uses "the dynamics of the model calibrated to Reddit submissions" before R3 introduces the model. Either:
  - describe that simulation generically, with a pointer to Methods; or
  - move ¶3 into R2. There, one slope S_k sets both how fast members leave (Eq. 1, individual lock-in) and how much newcomers return (Eq. 2). This gives the owner's lock-in question a theoretical home, and I prefer it if the budget allows.
- **R2 answers three questions:** whether the identity holds, what the balance is, and how durable the hierarchy is. Open it with the durability claim, so the measurement paragraphs read as what that claim needs.
- **R3 ¶3 and R4 ¶1 overlap.** "Does not forecast the head's fall" appears in both; keep it only in R4.
- **Headings that overclaim:**
  - R3 "holds it": the model holds a hierarchy 37–41% flatter at the head. Scope the heading.
  - R5 "communities differ in their growth": that is the fixed-heterogeneity reading, and the submissions factor result contradicts it. Prefer "On Reddit, changes persist beyond homogeneous growth".
- **Activity versus attention (C7.3)** has no scheduled sentence. Put it in R1 ¶1.

## Q2. Results 2 and durability

- **Front matter: well placed** (significance sentence 3, abstract sentence 4).
- **Results: buried.** Durability is the last paragraph of R2 and the last panel of Fig. 2. Make it R2's first sentence and Fig. 2A.
- **Gather the case for the benchmark in one paragraph.** The reasons readers should accept the model are scattered: geometry in R1, crossings in R2, jumps excluded, β only in the SI.
- **Keep one main-text sentence on lasting β** (0.05–0.13, below firms). The contrast with the 2018 chapter needs it too.
- **Label the contribution correctly:** measurement and a benchmark, not the mathematics. Fernholz, GLLM and Hindman 2018 come first.

## Q3. The fast fall

- **Don't move R4 before R3.** Its decisive comparison (a forecast of 0.53–0.54 against the observed fall) comes from R3's model. Putting the model between the departure and its candidate source would also separate R4 from R5.
- **Do state the fall as a fact in R1.**
- **About 400 words is right.**
- **Rival (c) must sit in R4 (rule 7), not only in the Discussion:**
  - the June 2020 ban wave: two of the three head communities that left were banned;
  - the 2020 surge.

  The ban share of the head's vanishing term is listed as not run.

## Q4. Figures

- **Fig. 3C repeats Fig. 2B.** v4 itself calls the model's flatter balance "the same fact" as the balance below 1. Replace it with something the owner asked for that no figure shows. Either:
  - one- and two-year retention by rank, data against the model's band (R13c); or
  - the projection band widening at 1, 2 and 5 years, so that "noise compounds" is shown, not just said.
- **Fig. 2C needs a data curve.** Plotting 8S²/σ² against S²/σ² gives two parallel curves. Plot model relaxation against *observed* membership time and *observed* shape change.
- **Fig. 3B may show little at 5 years.** In v4, neither removal moves the head by more than 0.3 log units a year over two years. Draw 0–30 years with the first 5 shaded and the rest labelled model-implied. The panel then shows the slowness, which is the point.
- **Fig. 4C needs two more series:**
  - Facebook, which is negative at every gap;
  - the factor-adjusted series (submissions persistence vanishes, comments survives).

  Without them, the three readings have no visible evidence.
- **Five subsections share four figures.** Say that Fig. 4 has two panel groups, A–B and C–D.
- **Fig. 1A is the place to define** ladder, top set, slide and crossing, once each.
- **Consider a small Table 1 of models against facts,** with each cell coded by where the data fall in that world's 5–95% band. It is the only way "every model is a column" becomes visible in the main text, and it could replace most of Discussion ¶2.

## Q5. Discussion

The order is right, and it ends on the outlook.

- **¶3 is a placeholder,** because the uses memo has not been written. Build it as pairs, each backed by this paper's own evidence (about 200 words).
  - **Uses:**
    - a benchmark for entrenchment claims: leaders keep rank one while their share halves, and the order of turnover comes from geometry;
    - retention probabilities at 1–2 years from σ and S;
    - a decomposition of a change in concentration into crossings, slide, entry and exit, and persistence;
    - bounds on how fast concentration can respond within 5 years.
  - **Non-uses, each shown by one of the paper's own failures:**
    - forecasting individual movement: on held-out weeks it does no better than simpler rules;
    - inferring mechanism from the shape or from the crossing identity: the identity holds in every diffusion world (1.04–1.12);
    - forecasts decades ahead: the drift comes from 2020–22 only;
    - effects of interventions: drift and volatility may themselves change (Huszár).
  - Significance sentence 5 ("the time concentration takes to respond") needs the same condition: "to a change acting through volatility or decline, in the model".
- **The through line from the 2018 chapter:** put it in Intro ¶2, with the contrasts in Results, in both versions (third person in the submission). The contrasts:
  - web sites' volatility fell steeply with size, while Reddit's lasting volatility hardly does;
  - the web's top ten rarely changed, while 27–33% of Reddit's turns over in a year;
  - if R13d holds, the "funnel" is mostly the extra mass down the tail.

  `\iffinal` should switch voice, not content.
- **Anonymity depends on the review mode.** If review is single-blind (SYNTHESIS assumes so), writing in the third person is a matter of style. If it is double-anonymous, the public repository URL in Data availability and the hindman2009 citation also identify the author.
- **Discussion ¶5 promises "forecasts from every eligible model".** That is an owner-gated protocol amendment (SYNTHESIS §4.4). Do not promise it until it is adopted.

## Q6. Gaps a referee will find

- **Where the decline comes from.** The slide is measured and fed into the model, but its source is never discussed. Candidates:
  - finite attention;
  - normalization against a re-selected universe, which is partly mechanical, as v4 notes;
  - ageing;
  - audience saturation;
  - moderation.

  The owner asks for "the processes that generate it", so one paragraph must say what is known and what the paper cannot identify.
- **Facebook's role is unstated after R1.** 86 weeks cannot test durability. Say that it serves the instrument and the persistence contrast, or move it to the SI.
- **The 5-year claims run past a 4-year record.** Backtest at the longest horizon available, beside each claim: fit 2019–20 and project 2021–22. Retention and the head's share are already on hand.
- **Model eligibility rule.** Put it in Methods in one sentence, or the atlas will read as hand-picked.
- **Four of SYNTHESIS's twelve main-text references are not placed:**
  - Boik, Greenstein & Prince 2019 (Intro);
  - Fernholz, Ichiba & Karatzas 2013 (R5, the fixed reading);
  - Lazer 2014 (rival c);
  - Huszár 2022 (Discussion ¶3).

  The Baker & Smyth contrast also needs a place in R2 ¶3. Their "turnover" is our e_k, which is not small at the comments head.
- **§2z-dm is cited but does not exist yet.** The outline cites it for the owner's directions, but it is not yet in MODEL_STATUS.

## Q7. Budgets

- **The budgets overshoot the outline's own target.** The section budgets sum to 5,100 words (600 + 500 + 850 + 550 + 400 + 750 + 800 + 650). Rule 6 says 3,900–4,300. With twelve new references, about 440 words of captions and four full-width figures, that is about 8 pages, not 7.
- **Proposed budgets (total 4,300):**

  | section | words | where the cut comes from |
  |---|---|---|
  | Introduction | 500 | ¶2 is again a crowded genealogy; put the mechanism split in a parenthetical with citations |
  | R1 | 400 | |
  | R2 | 700 | estimator construction moves to Methods |
  | R3 | 500 | |
  | R4 | 400 | |
  | R5 | 550 | R10 in three sentences, since it is post hoc |
  | Discussion | 650 | rivals in about 150 words, since they already sit beside their claims, or Table 1 instead |
  | Methods | 600 | |

- **Paragraph counts can stay;** the paragraphs get shorter.
- **If R12 and R13c each add a main-text result,** move R1's detail on named bans and R5 ¶4 to the SI first.