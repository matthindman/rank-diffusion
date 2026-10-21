# Opus track, iteration 1: what was checked, accepted and changed (Claude Opus 5.5, 2026-10-21)

Review: `REVIEW_opus_iter1_opus-5.5.md` (send out, without enthusiasm; 6/10) on v9.1 (72ec0c2).

| # | critique | check | action (v9.2) |
|---|---|---|---|
| 1 | Title claim rests on an identity and a reconstruction; Facebook's model head turnover failed (R18-1); β 0.097 biased | confirmed (`r18_summary.json`; §2z-dd recovery; two-step 0.019 in §2z-dg) | title "Turnover among the leaders of online attention follows the geometry of random growth"; the gradient mismatch stated in Results 2; **R18-B run** (registered 5631fad): at β 0.019 head share 0.25 (fixed) but top-54 turnover 0.255 (still below 0.31) → "Facebook's leaders turn over faster than its calibrated model implies" in abstract, introduction, Results 4 and Discussion |
| 2 | Asymmetry is a tail-shape fact; surge rival untested | confirmed from saved decile curves (2nd/9th tenths 0.16/0.18 and 0.16/0.15; quintile D +0.145/+0.098 vs −0.045/−0.031; tail means −1.12/+1.60 vs −1.30/+1.31) | main text: near-tail symmetry, quintile D, tail asymmetry, the surge rival; SI paragraph "Where the asymmetry sits"; the surge world listed for the owner |
| 3 | Facebook persistence fragile: post hoc bands, swing misfit rival, matched-settings contrast | agreed | main text labels R18-A1 post hoc, names the slower-swing rival and the matched-settings comparison; significance no longer says Facebook's changes "persist" |
| 4 | "heterogeneous growth misses the asymmetry" overstated (decline state on comments +0.125) | confirmed | "symmetric" throughout; "the one skewed form gives the sign"; Table 1 kept to submissions (stated in caption context) |
| 5 | Common model not common in β, settings, gate model, constant-slope column, shape measure | agreed | Methods states what is common and what is panel-specific; Table 1 rows compare within platforms; Results 4 heading changed; the gate attributed to the earlier, panel-specific model; R18-B handles β |
| 7 | Lead and density | agreed | abstract and significance rewritten along the referee's lines; Results 2 paragraph rewritten; time-scale paragraph shortened |
| 8 | Size claim unscoped on comments | confirmed | comments "weak rather than absent" (0.13) |
| E1–E23 | | each checked | E1, E2 (0.028 vs 0.037 better; 0.022 vs 0.013 worse), E3, E4, E5, E6, E7, E8, E9 (+0.14 to +0.17 at 13 and 26 weeks), E10 (three quarters), E11 (0.55–0.59 log units, 63% of the ten-year move), E12, E13, E14, E15 (I2 reported), E16, E17 (captions), E19, E20 (p values), E21, E22 (note in SI) fixed; E18 kept (1,728 is the formula's cell, 1,750 the table's K/2); E23 already 6.5 in the SI, the record is append-only |
| §4 | protocol-construction D in the main text; by-origin range; Facebook by-origin D | done (main: cohort fixed in 2019 H1, +0.24 and +0.20; SI: Facebook origins) | |
| §6 | Instagram one clause, I2 reported | done | |
| §8 | owner items | reported | the size-law rerun was done here as R18-B |
