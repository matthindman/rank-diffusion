# V2 — Independent simulation, parameter recovery and planted mechanisms

**Complete: all 1,797 expected cells, no failed scientific seed.** Nine predictions passed and four failed. The independent simulator passed all 57 discrepancy checks, but the estimator often failed to recover its generating parameters. Positive heterogeneity was inferred in 20/20 zero-heterogeneity panels on each profile.

Strong current-rank attraction and a strong random walk remained unresolved both by 26-week middle-rank dispersion and by fitted middle-rank home reversion. Annual cycles lowered fitted reversion, contrary to the declared prediction. All 20 baseline seeds on each profile triggered at least one diagnostic; the many-statistic battery has no 5% global false-alarm interpretation. These are conditional synthetic findings, not empirical mechanism verdicts.

Large tables are stored losslessly as `.csv.gz` in git; `TABLE_ARCHIVE.md` in the run folder gives restoration instructions. Local figures were rendered and visually checked; they remain outside git.

2026-10-12 · Exploratory, conditional on frozen DEVELOPMENT fits. **No change adopted.**

The independent simulator passes the fixed 57-comparison baseline discrepancy check. All native baseline and zero-heterogeneity recovery fits are included. Reversion often lands at an estimator bound; true zero heterogeneity is returned as positive on all 20 seeds of every profile. Facebook tail degrees of freedom are returned as infinity on all baseline and zero-heterogeneity fits despite finite generating tails. These are operational recovery failures, not failed execution. Diagnostic findings retain all declared strengths. A passed cross-check is not proof of equivalence; conditional diagnostic separation is not unique mechanism identification.

## Design, provenance and execution

Base main: 4ae73b43b852ac540cadd39808f27200885f1be7. OpenAI Codex. DESIGN committed at 4f823c3; independent evaluator at 0e401dd; study/reporting evaluator at 57428df; dated two-case addendum at aca704f. Protected models and frozen defaults unchanged. No movement-gate calls.

Commands (registered Python 3.11.6):

```bash
bash llm_fitting/handoff/V2_planted_mechanisms/launch_crosscheck.sh
bash llm_fitting/handoff/V2_planted_mechanisms/launch_study.sh
```

For a restart of the full added-case/diagnostic study, use `bash llm_fitting/handoff/V2_planted_mechanisms/launch_complete_efficient.sh`; it skips complete atomic cells and runs `planted_summary.py --include-extensions`. Then run `planted_audit.py` and `planted_report.py`, each with `--out llm_fitting/runs/2026-10-12_V2_planted_mechanisms`; the report also takes `--handoff llm_fitting/handoff/V2_planted_mechanisms`. All scripts live under `llm_fitting/validation/`.

Launchers use caffeinate and heavy_lock. Native profiles run through the shared lock; each seed writes an atomic, source-identified checkpoint. Full empirical seed ladders and large per-seed fitted objects stay local. Compact seed evidence, source/input hashes, tables and logs are provided. A1/A2 real-panel findings were not opened before this handoff.

| panel | weeks | K | slots |
| --- | --- | --- | --- |
| facebook_a | 86 | 3500 | 13454 |
| reddit_comments | 136 | 12500 | 42462 |
| reddit_submissions_long | 212 | 5000 | 19807 |

These are three native profile/length combinations. Counts of saved cells include repeated uses of the same baseline paths; they are not counts of independent datasets.

## Baseline cross-check

Twenty production seeds 0–19 and twenty independent seeds 1000–1019 per profile; 19 macro statistics each. All 57 pass the predeclared 20,000-permutation, Bonferroni .05 family rule. Minimum adjusted p is 1 on every profile. S1/S10/S100 are shares within the simulator's recorded top min(N,2000), not empirical platform-wide shares. The full table is crosscheck.csv; it gives both mean ± SD, 5–95% seed bands, difference/MCSE and simultaneous permutation-null bands.

### Facebook Era A

| statistic | production mean ± SD [5–95%] | independent mean ± SD [5–95%] | p_bonferroni |
| --- | --- | --- | --- |
| S1 | 0.047376 ± 0.0104 [0.034184, 0.069728] | 0.048561 ± 0.012 [0.030535, 0.064626] | 1 |
| S10 | 0.14953 ± 0.011 [0.13187, 0.16972] | 0.15168 ± 0.0138 [0.13472, 0.17635] | 1 |
| S100 | 0.45438 ± 0.00788 [0.44179, 0.46469] | 0.45806 ± 0.0133 [0.43557, 0.47897] | 1 |
| VR2 | 0.6301 ± 0.0104 [0.61314, 0.64457] | 0.6343 ± 0.00779 [0.62348, 0.64786] | 1 |
| VR4 | 0.37577 ± 0.00784 [0.36287, 0.38833] | 0.37936 ± 0.0103 [0.36636, 0.39663] | 1 |
| VR8 | 0.22001 ± 0.00543 [0.21338, 0.22904] | 0.22185 ± 0.00469 [0.21578, 0.23004] | 1 |
| VR13 | 0.14896 ± 0.00376 [0.14405, 0.15618] | 0.15015 ± 0.0051 [0.14221, 0.15804] | 1 |
| ACF1 | -0.36987 ± 0.0101 [-0.38475, -0.35582] | -0.36568 ± 0.0078 [-0.37718, -0.35279] | 1 |
| ACF2 | -0.054906 ± 0.014 [-0.076097, -0.035165] | -0.056047 ± 0.0124 [-0.073902, -0.040335] | 1 |
| rR2_1 | 0.80792 ± 0.00921 [0.7921, 0.81886] | 0.81166 ± 0.00778 [0.79987, 0.82275] | 1 |
| rR2_4 | 0.71853 ± 0.0124 [0.70047, 0.73372] | 0.72365 ± 0.0103 [0.70758, 0.73828] | 1 |
| rR2_13 | 0.60655 ± 0.0174 [0.57676, 0.62814] | 0.6155 ± 0.0131 [0.59683, 0.6352] | 1 |
| RACF1 | 0.54587 ± 0.00868 [0.53361, 0.55647] | 0.54189 ± 0.00666 [0.53136, 0.55177] | 1 |
| RACF4 | 0.32331 ± 0.00894 [0.30928, 0.33571] | 0.32036 ± 0.00928 [0.30579, 0.33425] | 1 |
| RACF13 | 0.076972 ± 0.00929 [0.060723, 0.088037] | 0.075246 ± 0.0118 [0.056671, 0.088018] | 1 |
| rPers1 | 26.951 ± 0.447 [26.349, 27.528] | 26.972 ± 0.51 [26.278, 27.496] | 1 |
| rPers4 | 24.471 ± 0.578 [23.824, 25.563] | 24.413 ± 0.55 [23.623, 25.223] | 1 |
| rPers13 | 22.091 ± 0.775 [21.184, 23.257] | 22.268 ± 0.893 [20.958, 23.569] | 1 |
| outfluxK | 0.13241 ± 0.00143 [0.13059, 0.13452] | 0.13201 ± 0.00107 [0.13055, 0.13405] | 1 |

### Reddit comments

| statistic | production mean ± SD [5–95%] | independent mean ± SD [5–95%] | p_bonferroni |
| --- | --- | --- | --- |
| S1 | 0.14578 ± 0.0514 [0.09426, 0.25891] | 0.15101 ± 0.0605 [0.074219, 0.22039] | 1 |
| S10 | 0.3302 ± 0.0384 [0.28403, 0.41411] | 0.3378 ± 0.0543 [0.27386, 0.45981] | 1 |
| S100 | 0.63224 ± 0.0216 [0.6048, 0.67541] | 0.6359 ± 0.0303 [0.59947, 0.70246] | 1 |
| VR2 | 0.67136 ± 0.0148 [0.6419, 0.69058] | 0.67259 ± 0.012 [0.65353, 0.68892] | 1 |
| VR4 | 0.44837 ± 0.0212 [0.41143, 0.47387] | 0.44965 ± 0.0208 [0.41796, 0.47225] | 1 |
| VR8 | 0.29843 ± 0.0206 [0.26933, 0.32927] | 0.30361 ± 0.0179 [0.2793, 0.32823] | 1 |
| VR13 | 0.21701 ± 0.0145 [0.20067, 0.24656] | 0.22488 ± 0.0174 [0.20566, 0.25508] | 1 |
| ACF1 | -0.32867 ± 0.0149 [-0.359, -0.31066] | -0.32715 ± 0.0118 [-0.34642, -0.31147] | 1 |
| ACF2 | -0.044498 ± 0.0131 [-0.065207, -0.030221] | -0.047073 ± 0.0157 [-0.072313, -0.022011] | 1 |
| rR2_1 | 0.94338 ± 0.00293 [0.9399, 0.94813] | 0.94366 ± 0.00316 [0.9398, 0.94881] | 1 |
| rR2_4 | 0.91112 ± 0.00465 [0.90618, 0.92016] | 0.91128 ± 0.00507 [0.90406, 0.9185] | 1 |
| rR2_13 | 0.85288 ± 0.00775 [0.84476, 0.86886] | 0.85281 ± 0.00809 [0.84232, 0.86373] | 1 |
| RACF1 | 0.71384 ± 0.00568 [0.70722, 0.7221] | 0.71485 ± 0.00546 [0.70782, 0.72394] | 1 |
| RACF4 | 0.54566 ± 0.00897 [0.53452, 0.55716] | 0.54735 ± 0.00777 [0.53568, 0.55839] | 1 |
| RACF13 | 0.27055 ± 0.00882 [0.25757, 0.28093] | 0.26675 ± 0.0127 [0.24375, 0.2866] | 1 |
| rPers1 | 111 ± 0.481 [110.23, 111.87] | 110.8 ± 0.699 [109.86, 111.8] | 1 |
| rPers4 | 106.67 ± 0.672 [105.79, 107.84] | 106.54 ± 0.814 [105.36, 107.72] | 1 |
| rPers13 | 101.87 ± 0.847 [100.54, 103.18] | 101.76 ± 1.05 [100.02, 103.25] | 1 |
| outfluxK | 0.12812 ± 0.000987 [0.12666, 0.12946] | 0.12866 ± 0.000767 [0.12761, 0.12968] | 1 |

### Long submissions

| statistic | production mean ± SD [5–95%] | independent mean ± SD [5–95%] | p_bonferroni |
| --- | --- | --- | --- |
| S1 | 0.054111 ± 0.0114 [0.039469, 0.072711] | 0.054286 ± 0.0133 [0.0398, 0.082089] | 1 |
| S10 | 0.24278 ± 0.0161 [0.21995, 0.26402] | 0.24439 ± 0.0182 [0.21341, 0.27074] | 1 |
| S100 | 0.59394 ± 0.0125 [0.57741, 0.61238] | 0.59783 ± 0.0134 [0.58158, 0.62369] | 1 |
| VR2 | 0.67901 ± 0.0047 [0.67307, 0.68724] | 0.67819 ± 0.0046 [0.67027, 0.68467] | 1 |
| VR4 | 0.46095 ± 0.00634 [0.45249, 0.47061] | 0.45852 ± 0.00787 [0.44747, 0.47253] | 1 |
| VR8 | 0.32025 ± 0.00864 [0.30638, 0.33558] | 0.31876 ± 0.0105 [0.30411, 0.33357] | 1 |
| VR13 | 0.25003 ± 0.00901 [0.23612, 0.26032] | 0.24846 ± 0.0125 [0.23344, 0.27085] | 1 |
| ACF1 | -0.32091 ± 0.00469 [-0.32716, -0.31398] | -0.32179 ± 0.00463 [-0.32978, -0.3158] | 1 |
| ACF2 | -0.042017 ± 0.00704 [-0.052775, -0.033236] | -0.042863 ± 0.0066 [-0.054756, -0.034497] | 1 |
| rR2_1 | 0.95166 ± 0.0036 [0.94486, 0.95582] | 0.95166 ± 0.00278 [0.94848, 0.95707] | 1 |
| rR2_4 | 0.92081 ± 0.00567 [0.91005, 0.92752] | 0.92086 ± 0.00417 [0.9157, 0.9287] | 1 |
| rR2_13 | 0.8697 ± 0.0092 [0.85339, 0.88193] | 0.8698 ± 0.00661 [0.86151, 0.88133] | 1 |
| RACF1 | 0.81922 ± 0.00439 [0.81184, 0.82516] | 0.81731 ± 0.00366 [0.81323, 0.82275] | 1 |
| RACF4 | 0.69054 ± 0.00683 [0.68119, 0.70059] | 0.68644 ± 0.00626 [0.67746, 0.69493] | 1 |
| RACF13 | 0.47049 ± 0.00941 [0.46178, 0.48974] | 0.47182 ± 0.00762 [0.46272, 0.48395] | 1 |
| rPers1 | 46.858 ± 0.288 [46.477, 47.271] | 46.85 ± 0.264 [46.393, 47.24] | 1 |
| rPers4 | 45.48 ± 0.419 [44.917, 46.112] | 45.432 ± 0.366 [44.899, 45.935] | 1 |
| rPers13 | 43.109 ± 0.601 [42.406, 44.016] | 43.113 ± 0.516 [42.416, 43.774] | 1 |
| outfluxK | 0.061562 ± 0.000829 [0.06066, 0.062999] | 0.061817 ± 0.000783 [0.06082, 0.063286] | 1 |

## Recovery

Twenty synthetic panels at frozen parameters and twenty at true s=0 per native profile. The real intake mapping, absence-penalized 4K membership and S4 minimum-distance estimator are used. Zero metrics are retained. Rank-varying coefficients are generated on latent-level rank, while estimation uses mean observed-rank bands. Thus this is operational recovery, including observation-floor and selection effects, not a fixed-band asymptotic test.

| panel | condition | parameter | valid | mean | sd | rate | lower | upper | failed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| facebook_a | baseline | kappa_edge_fraction | 20 | 0.67593 | 0.12452 | undefined | undefined | undefined | undefined |
| facebook_a | baseline | zero_floor_fraction | 20 | 6.0845e-05 | 1.7655e-05 | undefined | undefined | undefined | undefined |
| facebook_a | zero_temperament | kappa_edge_fraction | 20 | 0.60648 | 0.098885 | undefined | undefined | undefined | undefined |
| facebook_a | zero_temperament | zero_floor_fraction | 20 | 2.6879e-05 | 1.5845e-05 | undefined | undefined | undefined | undefined |
| facebook_a | zero_temperament | s_positive | undefined | undefined | undefined | 1 | 0.83887 | 1 | 0 |
| reddit_comments | baseline | kappa_edge_fraction | 20 | 0.48545 | 0.082396 | undefined | undefined | undefined | undefined |
| reddit_comments | baseline | zero_floor_fraction | 20 | 0.0052818 | 0.00052022 | undefined | undefined | undefined | undefined |
| reddit_comments | zero_temperament | kappa_edge_fraction | 20 | 0.47818 | 0.093289 | undefined | undefined | undefined | undefined |
| reddit_comments | zero_temperament | zero_floor_fraction | 20 | 0.0036602 | 0.00051649 | undefined | undefined | undefined | undefined |
| reddit_comments | zero_temperament | s_positive | undefined | undefined | undefined | 1 | 0.83887 | 1 | 0 |
| reddit_submissions_long | baseline | kappa_edge_fraction | 20 | 0.45648 | 0.11732 | undefined | undefined | undefined | undefined |
| reddit_submissions_long | baseline | zero_floor_fraction | 20 | 0.06905 | 0.0019205 | undefined | undefined | undefined | undefined |
| reddit_submissions_long | zero_temperament | kappa_edge_fraction | 20 | 0.44444 | 0.11986 | undefined | undefined | undefined | undefined |
| reddit_submissions_long | zero_temperament | zero_floor_fraction | 20 | 0.068065 | 0.0016693 | undefined | undefined | undefined | undefined |
| reddit_submissions_long | zero_temperament | s_positive | undefined | undefined | undefined | 1 | 0.83887 | 1 | 0 |

| panel | condition | parameter | truth | mean | bias | sd | rmse | mcse_mean | valid | positive_infinity |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| facebook_a | baseline | temper_s | 0.89013 | 0.89879 | 0.0086653 | 0.0050335 | 0.0099577 | 0.0011255 | 20 | 0 |
| facebook_a | baseline | t_df | 4.3 | undefined | undefined | undefined | undefined | undefined | 0 | 20 |
| facebook_a | baseline | tail_weight | 3.3333 | 0 | -3.3333 | 0 | 3.3333 | 0 | 20 | 0 |
| facebook_a | zero_temperament | temper_s | 0 | 0.13839 | 0.13839 | 0.0021889 | 0.13841 | 0.00048946 | 20 | 0 |
| facebook_a | zero_temperament | t_df | 4.3 | undefined | undefined | undefined | undefined | undefined | 0 | 20 |
| facebook_a | zero_temperament | tail_weight | 3.3333 | 0 | -3.3333 | 0 | 3.3333 | 0 | 20 | 0 |
| reddit_comments | baseline | temper_s | 0.6922 | 0.68989 | -0.0023184 | 0.0030216 | 0.0037481 | 0.00067564 | 20 | 0 |
| reddit_comments | baseline | t_df | 5.173 | 10.781 | 5.6077 | 1.0965 | 5.7087 | 0.24519 | 20 | 0 |
| reddit_comments | baseline | tail_weight | 0.8525 | 0.15111 | -0.70139 | 0.024002 | 0.70178 | 0.0053671 | 20 | 0 |
| reddit_comments | zero_temperament | temper_s | 0 | 0.19017 | 0.19017 | 0.0018231 | 0.19018 | 0.00040766 | 20 | 0 |
| reddit_comments | zero_temperament | t_df | 5.173 | 8.9988 | 3.8258 | 0.92579 | 3.9307 | 0.20701 | 20 | 0 |
| reddit_comments | zero_temperament | tail_weight | 0.8525 | 0.20625 | -0.64626 | 0.036111 | 0.64722 | 0.0080746 | 20 | 0 |
| reddit_submissions_long | baseline | temper_s | 1.1185 | 1.1116 | -0.0069158 | 0.0062241 | 0.0091995 | 0.0013918 | 20 | 0 |
| reddit_submissions_long | baseline | t_df | 4.3 | 5.1335 | 0.83346 | 0.43802 | 0.93644 | 0.097945 | 20 | 0 |
| reddit_submissions_long | baseline | tail_weight | 3.3333 | 0.99276 | -2.3406 | 0.33011 | 2.3626 | 0.073814 | 20 | 0 |
| reddit_submissions_long | zero_temperament | temper_s | 0 | 0.16733 | 0.16733 | 0.001573 | 0.16734 | 0.00035174 | 20 | 0 |
| reddit_submissions_long | zero_temperament | t_df | 4.3 | 4.7698 | 0.46976 | 0.23831 | 0.52404 | 0.053288 | 20 | 0 |
| reddit_submissions_long | zero_temperament | tail_weight | 3.3333 | 1.4082 | -1.9252 | 0.39455 | 1.9632 | 0.088225 | 20 | 0 |

All knot-level biases, SDs, RMSEs, quantiles, MCSEs and valid counts are in recovery_knots.csv. recovery_groups.csv averages knots without pretending knots are independent simulation replications. recovery_extras.csv includes NNLS-zero fractions, kappa-edge fractions, sample sizes and s=0 positive-estimate rates. MCSE of SD/RMSE uses the seed jackknife. Positive s frequency is not a nominal Type I error rate. The column `valid` counts finite estimates; infinite degrees of freedom are valid boundary outputs, not failed fits. Infinite t degrees of freedom are retained as an atom; finite conditional summaries and 1/(df−4) are reported separately. V=sp²/(2 kappa) and its discrete counterpart are local functionals, not claims about global stationary endpoint variance.

| panel | V median relative RMSE | kappa_z median relative RMSE |
| --- | --- | --- |
| facebook_a | 0.898 | 0.9206 |
| reddit_comments | 0.74282 | 1.7435 |
| reddit_submissions_long | 0.51429 | 0.9405 |

V recovers better than kappa by this comparison, but its absolute error remains substantial. The estimator does not yet justify interpreting fitted reversion, tail thickness or a small positive heterogeneity estimate as a well-recovered mechanism. This conclusion is limited to these native frozen profiles and observation rules.

## Fixed diagnostic display

Baseline calibration uses 39 independent seeds 2000–2038. Evaluation uses 20 paired seeds 1000–1019 per condition. Strict outside-min/max rates are per-statistic; their nominal bound of 2/40=.05 is averaged over calibration sets. The conditional binomial MCSE/Wilson interval does not include calibration-set uncertainty. Baseline evaluation seeds also entered the earlier cross-check; these intervals are descriptive references, not claimed post-selection coverage. The any-statistic baseline false-alarm rate is separately reported and is not a 5% global test.

### Facebook Era A — fraction outside baseline envelope

| display | annual_cycle_0 | annual_cycle_1 | baseline | bounded_fast_0 | bounded_fast_1 | fat_lasting_0 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 1.00 [0.84, 1.00] | 0.95 [0.76, 0.99] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Head downward retention h13 | 0.95 [0.76, 0.99] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |
| Absence exit rate | 0.15 [0.05, 0.36] | 0.85 [0.64, 0.95] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] |
| Lasting exit rate | 0.10 [0.03, 0.30] | 0.60 [0.39, 0.78] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Repeat exit rate | 0.05 [0.01, 0.24] | 0.90 [0.70, 0.97] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.10 [0.03, 0.30] |
| Cohort time outside | 0.05 [0.01, 0.24] | 0.25 [0.11, 0.47] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Middle dispersion h26 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Middle skip momentum b13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Middle volatility ratio | 0.00 [0.00, 0.16] | 0.40 [0.22, 0.61] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Head far tail q9999 | 0.00 [0.00, 0.16] | 0.25 [0.11, 0.47] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] |
| Middle amplitude coherence h13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |

| display | fat_lasting_1 | gaussian_control | ladder_attraction_0 | ladder_attraction_1 | log_volatility_0 | log_volatility_1 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 0.55 [0.34, 0.74] | 0.15 [0.05, 0.36] | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Head downward retention h13 | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.20 [0.08, 0.42] | 0.05 [0.01, 0.24] | 0.30 [0.15, 0.52] |
| Absence exit rate | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Lasting exit rate | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] |
| Repeat exit rate | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.15 [0.05, 0.36] |
| Cohort time outside | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.95 [0.76, 0.99] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Middle dispersion h26 | 0.20 [0.08, 0.42] | 0.05 [0.01, 0.24] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Middle skip momentum b13 | 0.05 [0.01, 0.24] | 0.25 [0.11, 0.47] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] |
| Middle volatility ratio | 0.10 [0.03, 0.30] | 0.00 [0.00, 0.16] | 0.15 [0.05, 0.36] | 0.45 [0.26, 0.66] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Head far tail q9999 | 0.15 [0.05, 0.36] | 0.80 [0.58, 0.92] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 1.00 [0.84, 1.00] |
| Middle amplitude coherence h13 | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |

| display | off_0 | off_1 | random_walk_0 | random_walk_1 | slow_memory_0 | slow_memory_1 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 0.05 [0.01, 0.24] | 0.30 [0.15, 0.52] | 0.20 [0.08, 0.42] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Head downward retention h13 | 0.05 [0.01, 0.24] | 0.35 [0.18, 0.57] | 0.05 [0.01, 0.24] | 0.55 [0.34, 0.74] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |
| Absence exit rate | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.15 [0.05, 0.36] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] |
| Lasting exit rate | 0.95 [0.76, 0.99] | 1.00 [0.84, 1.00] | 0.15 [0.05, 0.36] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] |
| Repeat exit rate | 0.10 [0.03, 0.30] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.45 [0.26, 0.66] | 0.30 [0.15, 0.52] | 0.85 [0.64, 0.95] |
| Cohort time outside | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.55 [0.34, 0.74] | 0.30 [0.15, 0.52] | 0.15 [0.05, 0.36] | 0.35 [0.18, 0.57] |
| Middle dispersion h26 | 0.20 [0.08, 0.42] | 0.80 [0.58, 0.92] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.90 [0.70, 0.97] | 1.00 [0.84, 1.00] |
| Middle skip momentum b13 | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.15 [0.05, 0.36] | 0.25 [0.11, 0.47] |
| Middle volatility ratio | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.15 [0.05, 0.36] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |
| Head far tail q9999 | 0.00 [0.00, 0.16] | 0.10 [0.03, 0.30] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Middle amplitude coherence h13 | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |

### Reddit comments — fraction outside baseline envelope

| display | annual_cycle_0 | annual_cycle_1 | baseline | bounded_fast_0 | bounded_fast_1 | fat_lasting_0 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.15 [0.05, 0.36] | 0.05 [0.01, 0.24] |
| Head downward retention h13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Absence exit rate | 0.15 [0.05, 0.36] | 0.70 [0.48, 0.85] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.15 [0.05, 0.36] |
| Lasting exit rate | 0.25 [0.11, 0.47] | 1.00 [0.84, 1.00] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.25 [0.11, 0.47] |
| Repeat exit rate | 0.45 [0.26, 0.66] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Cohort time outside | 0.35 [0.18, 0.57] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] |
| Middle dispersion h26 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] |
| Middle skip momentum b13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Middle volatility ratio | 0.20 [0.08, 0.42] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Head far tail q9999 | 0.00 [0.00, 0.16] | 0.70 [0.48, 0.85] | 0.00 [0.00, 0.16] | 0.10 [0.03, 0.30] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] |
| Middle amplitude coherence h13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] |

| display | fat_lasting_1 | gaussian_control | ladder_attraction_0 | ladder_attraction_1 | log_volatility_0 | log_volatility_1 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 1.00 [0.84, 1.00] | 0.45 [0.26, 0.66] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Head downward retention h13 | 0.15 [0.05, 0.36] | 0.40 [0.22, 0.61] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.20 [0.08, 0.42] | 0.10 [0.03, 0.30] |
| Absence exit rate | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 0.30 [0.15, 0.52] | 0.25 [0.11, 0.47] | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] |
| Lasting exit rate | 0.25 [0.11, 0.47] | 0.25 [0.11, 0.47] | 0.35 [0.18, 0.57] | 0.35 [0.18, 0.57] | 0.25 [0.11, 0.47] | 0.30 [0.15, 0.52] |
| Repeat exit rate | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.75 [0.53, 0.89] |
| Cohort time outside | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.95 [0.76, 0.99] | 0.85 [0.64, 0.95] | 0.15 [0.05, 0.36] | 0.10 [0.03, 0.30] |
| Middle dispersion h26 | 0.85 [0.64, 0.95] | 0.10 [0.03, 0.30] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Middle skip momentum b13 | 0.15 [0.05, 0.36] | 0.05 [0.01, 0.24] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] |
| Middle volatility ratio | 0.00 [0.00, 0.16] | 0.15 [0.05, 0.36] | 0.40 [0.22, 0.61] | 0.40 [0.22, 0.61] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Head far tail q9999 | 0.85 [0.64, 0.95] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.70 [0.48, 0.85] | 1.00 [0.84, 1.00] |
| Middle amplitude coherence h13 | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |

| display | off_0 | off_1 | random_walk_0 | random_walk_1 | slow_memory_0 | slow_memory_1 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 0.20 [0.08, 0.42] | 0.75 [0.53, 0.89] | 0.85 [0.64, 0.95] | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 0.30 [0.15, 0.52] |
| Head downward retention h13 | 0.15 [0.05, 0.36] | 0.60 [0.39, 0.78] | 0.05 [0.01, 0.24] | 0.20 [0.08, 0.42] | 0.10 [0.03, 0.30] | 0.00 [0.00, 0.16] |
| Absence exit rate | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.30 [0.15, 0.52] | 0.15 [0.05, 0.36] | 0.15 [0.05, 0.36] |
| Lasting exit rate | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.15 [0.05, 0.36] | 0.35 [0.18, 0.57] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] |
| Repeat exit rate | 0.95 [0.76, 0.99] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.85 [0.64, 0.95] | 1.00 [0.84, 1.00] |
| Cohort time outside | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.95 [0.76, 0.99] | 0.50 [0.30, 0.70] | 0.90 [0.70, 0.97] |
| Middle dispersion h26 | 0.90 [0.70, 0.97] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Middle skip momentum b13 | 0.20 [0.08, 0.42] | 0.60 [0.39, 0.78] | 0.10 [0.03, 0.30] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.20 [0.08, 0.42] |
| Middle volatility ratio | 0.20 [0.08, 0.42] | 0.45 [0.26, 0.66] | 0.20 [0.08, 0.42] | 0.50 [0.30, 0.70] | 0.05 [0.01, 0.24] | 0.20 [0.08, 0.42] |
| Head far tail q9999 | 0.05 [0.01, 0.24] | 0.35 [0.18, 0.57] | 0.15 [0.05, 0.36] | 0.00 [0.00, 0.16] | 0.15 [0.05, 0.36] | 0.15 [0.05, 0.36] |
| Middle amplitude coherence h13 | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] | 0.55 [0.34, 0.74] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.25 [0.11, 0.47] |

### Long submissions — fraction outside baseline envelope

| display | annual_cycle_0 | annual_cycle_1 | baseline | bounded_fast_0 | bounded_fast_1 | fat_lasting_0 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.00 [0.00, 0.16] | 0.50 [0.30, 0.70] |
| Head downward retention h13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |
| Absence exit rate | 0.10 [0.03, 0.30] | 0.70 [0.48, 0.85] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] |
| Lasting exit rate | 0.45 [0.26, 0.66] | 1.00 [0.84, 1.00] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] |
| Repeat exit rate | 0.15 [0.05, 0.36] | 0.55 [0.34, 0.74] | 0.15 [0.05, 0.36] | 0.15 [0.05, 0.36] | 0.15 [0.05, 0.36] | 0.05 [0.01, 0.24] |
| Cohort time outside | 0.00 [0.00, 0.16] | 0.95 [0.76, 0.99] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] |
| Middle dispersion h26 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] |
| Middle skip momentum b13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.20 [0.08, 0.42] | 0.10 [0.03, 0.30] |
| Middle volatility ratio | 0.55 [0.34, 0.74] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 0.25 [0.11, 0.47] |
| Head far tail q9999 | 0.05 [0.01, 0.24] | 0.15 [0.05, 0.36] | 0.10 [0.03, 0.30] | 0.00 [0.00, 0.16] | 0.60 [0.39, 0.78] | 0.15 [0.05, 0.36] |
| Middle amplitude coherence h13 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |

| display | fat_lasting_1 | gaussian_control | ladder_attraction_0 | ladder_attraction_1 | log_volatility_0 | log_volatility_1 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.35 [0.18, 0.57] | 0.55 [0.34, 0.74] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Head downward retention h13 | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.20 [0.08, 0.42] |
| Absence exit rate | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Lasting exit rate | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 0.30 [0.15, 0.52] | 0.35 [0.18, 0.57] | 0.05 [0.01, 0.24] | 0.25 [0.11, 0.47] |
| Repeat exit rate | 0.15 [0.05, 0.36] | 0.15 [0.05, 0.36] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.15 [0.05, 0.36] | 0.85 [0.64, 0.95] |
| Cohort time outside | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.00 [0.00, 0.16] | 0.10 [0.03, 0.30] |
| Middle dispersion h26 | 0.90 [0.70, 0.97] | 0.10 [0.03, 0.30] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Middle skip momentum b13 | 0.15 [0.05, 0.36] | 0.15 [0.05, 0.36] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.30 [0.15, 0.52] |
| Middle volatility ratio | 0.15 [0.05, 0.36] | 0.60 [0.39, 0.78] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Head far tail q9999 | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.15 [0.05, 0.36] | 0.00 [0.00, 0.16] | 1.00 [0.84, 1.00] |
| Middle amplitude coherence h13 | 0.05 [0.01, 0.24] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] |

| display | off_0 | off_1 | random_walk_0 | random_walk_1 | slow_memory_0 | slow_memory_1 |
| --- | --- | --- | --- | --- | --- | --- |
| Head anchored tail h13 | 0.15 [0.05, 0.36] | 0.60 [0.23, 0.88] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.15 [0.05, 0.36] | 0.55 [0.34, 0.74] |
| Head downward retention h13 | 0.05 [0.01, 0.24] | 0.60 [0.23, 0.88] | 0.05 [0.01, 0.24] | 0.75 [0.53, 0.89] | 0.00 [0.00, 0.16] | 0.00 [0.00, 0.16] |
| Absence exit rate | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Lasting exit rate | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.05 [0.01, 0.24] | 0.15 [0.05, 0.36] | 0.00 [0.00, 0.16] | 0.05 [0.01, 0.24] |
| Repeat exit rate | 0.70 [0.48, 0.85] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.60 [0.39, 0.78] | 0.65 [0.43, 0.82] | 1.00 [0.84, 1.00] |
| Cohort time outside | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 0.50 [0.30, 0.70] | 0.25 [0.11, 0.47] | 0.95 [0.76, 0.99] |
| Middle dispersion h26 | 0.65 [0.43, 0.82] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] | 1.00 [0.84, 1.00] |
| Middle skip momentum b13 | 0.20 [0.08, 0.42] | 0.40 [0.22, 0.61] | 0.35 [0.18, 0.57] | 1.00 [0.84, 1.00] | 0.10 [0.03, 0.30] | 0.10 [0.03, 0.30] |
| Middle volatility ratio | 0.05 [0.01, 0.24] | 0.30 [0.15, 0.52] | 0.15 [0.05, 0.36] | 0.90 [0.70, 0.97] | 0.35 [0.18, 0.57] | 0.45 [0.26, 0.66] |
| Head far tail q9999 | 0.15 [0.05, 0.36] | 0.60 [0.39, 0.78] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] |
| Middle amplitude coherence h13 | 0.40 [0.22, 0.61] | 0.90 [0.70, 0.97] | 0.05 [0.01, 0.24] | 0.10 [0.03, 0.30] | 0.05 [0.01, 0.24] | 0.00 [0.00, 0.16] |

See diagnostic_rates.csv for all statistics, means/SDs/seed bands, rates/MCSEs/Wilson intervals and missing denominators; diagnostic_effects.csv for signed baseline/control SD effects and paired MCSE; diagnostic_pairs.csv for every condition pair, its pooled-SD separation and 5–95% bands. Descriptive labels are unresolved (<1 SD), separated (>=2 SD and disjoint bands), or overlapping; degenerate/missing spreads remain labelled. Gaussian fixed-volatility controls isolate changing volatility from removal of the original passing t tail. All strengths are retained.

The separation labels are descriptive, not hypothesis tests; uncertainty in the 20-seed standardizing SD is not propagated. Lasting t innovations with df=3 have no fourth moment, so variance-based SDs and MCSEs can be unstable; quantile bands and robust diagnostics are supplied alongside them.

Baseline any-statistic false-alarm rate:

| panel | condition | n | k | rate | mcse | lower | upper |
| --- | --- | --- | --- | --- | --- | --- | --- |
| facebook_a | baseline | 20 | 20 | 1 | 0 | 0.83887 | 1 |
| reddit_comments | baseline | 20 | 20 | 1 | 0 | 0.83887 | 1 |
| reddit_submissions_long | baseline | 20 | 20 | 1 | 0 | 0.83887 | 1 |

## Added cases: fitted reversion and annual signatures

The dated addendum fixes current-rank attraction at .02/.10 and annual mean peak amplitudes .5/1.5 log units. The additional simulator is versioned separately and bit-identical on controlled original-mechanism cases. Its fitted home kappa is a misspecification diagnostic, not recovery of the current-rank attraction coefficient. Both added cases and both random-walk strengths have 20 fits per profile.

| panel | condition | mean | sd | q05 | q95 | mcse_mean | valid |
| --- | --- | --- | --- | --- | --- | --- | --- |
| facebook_a | baseline | 0.033015 | 0.012104 | 0.0083498 | 0.047099 | 0.0027066 | 20 |
| facebook_a | ladder_attraction_0 | 0.014808 | 0.0067684 | 0.0075311 | 0.028755 | 0.0015135 | 20 |
| facebook_a | ladder_attraction_1 | 0.012389 | 0.0045181 | 0.0081956 | 0.021236 | 0.0010103 | 20 |
| facebook_a | annual_cycle_0 | 0.020152 | 0.00068047 | 0.02 | 0.020152 | 0.00015216 | 20 |
| facebook_a | annual_cycle_1 | 0.0050444 | 0.00019837 | 0.005 | 0.0050444 | 4.4358e-05 | 20 |
| facebook_a | random_walk_0 | 0.023514 | 0.011897 | 0.0058375 | 0.037551 | 0.0026602 | 20 |
| facebook_a | random_walk_1 | 0.011804 | 0.003995 | 0.0072542 | 0.018251 | 0.00089331 | 20 |
| reddit_comments | baseline | 0.017459 | 0.0042751 | 0.010638 | 0.024963 | 0.00095595 | 20 |
| reddit_comments | ladder_attraction_0 | 0.0071081 | 0.001984 | 0.005 | 0.01 | 0.00044363 | 20 |
| reddit_comments | ladder_attraction_1 | 0.0070596 | 0.0019569 | 0.005 | 0.01 | 0.00043758 | 20 |
| reddit_comments | annual_cycle_0 | 0.005 | 0 | 0.005 | 0.005 | 0 | 20 |
| reddit_comments | annual_cycle_1 | 0.005 | 0 | 0.005 | 0.005 | 0 | 20 |
| reddit_comments | random_walk_0 | 0.015683 | 0.0041268 | 0.0087217 | 0.020199 | 0.00092278 | 20 |
| reddit_comments | random_walk_1 | 0.0070005 | 0.0017499 | 0.005 | 0.01 | 0.00039129 | 20 |
| reddit_submissions_long | baseline | 0.0079578 | 0.0015461 | 0.005 | 0.01 | 0.00034572 | 20 |
| reddit_submissions_long | ladder_attraction_0 | 0.005 | 0 | 0.005 | 0.005 | 0 | 20 |
| reddit_submissions_long | ladder_attraction_1 | 0.005 | 0 | 0.005 | 0.005 | 0 | 20 |
| reddit_submissions_long | annual_cycle_0 | 0.0072237 | 0.0068464 | 0.005 | 0.026787 | 0.0015309 | 20 |
| reddit_submissions_long | annual_cycle_1 | 0.005 | 0 | 0.005 | 0.005 | 0 | 20 |
| reddit_submissions_long | random_walk_0 | 0.0056476 | 0.0011672 | 0.005 | 0.0085639 | 0.000261 | 20 |
| reddit_submissions_long | random_walk_1 | 0.0050709 | 0.00031728 | 0.005 | 0.0050709 | 7.0947e-05 | 20 |

fitted_kappa_knots.csv retains every fitted profile; fitted_kappa_pairs.csv gives every pair and group under the original separation rule. annual_cycle_gap_contrast.csv gives the paired gap4-minus-gap2 slope, SD and MCSE.

## Predictions

| id | status | detail |
| --- | --- | --- |
| P1 | PASS | All 57 discrepancy checks pass; minimum Bonferroni-adjusted p=1 on each profile. |
| P2 | PASS | Mean kappa edge fraction >=.25 on >=2 profiles; facebook_a=0.67593, reddit_comments=0.48545, reddit_submissions_long=0.45648 |
| P3 | PASS | V relative RMSE < kappa on >=2 profiles; {"facebook_a": {"V": {"median": 0.8980033444682574, "included": 54, "excluded": 0}, "kappa_z": {"median": 0.9205960722217674, "included": 54, "excluded": 0}}, "reddit_comments": {"V": {"median": 0.7428220533343508, "included": 55, "excluded": 0}, "kappa_z": {"median": 1.74352605101627, "included": 55, "excluded": 0}}, "reddit_submissions_long": {"V": {"median": 0.5142939596968706, "included": 54, "excluded": 0}, "kappa_z": {"median": 0.9404954380408499, "included": 54, "excluded": 0}}} |
| P4 | PASS | Positive s at true zero in >=25% of seeds on >=2 profiles; facebook_a=1, reddit_comments=1, reddit_submissions_long=1 |
| P5 | PASS | Strong off: absence-exit detection >=.8 on >=2 profiles; facebook_a=1, reddit_comments=1, reddit_submissions_long=1 |
| P6 | PASS | Strong volatility: middle ratio <=-2 matched-control SD on >=2 profiles; facebook_a=-57.079, reddit_comments=-103.67, reddit_submissions_long=-156.2 |
| P7 | PASS | Strong fat lasting: head anchored tail >=1 baseline SD on >=2 profiles; facebook_a=2.3558, reddit_comments=5.2589, reddit_submissions_long=12.848 |
| P8 | FAIL | Strong bounded fast: head q9999 reduction >=20% on >=2 profiles; facebook_a=0.12477, reddit_comments=0.18132, reddit_submissions_long=0.19191 |
| P9 | FAIL | T86 slow-memory/RW pairs: [{"condition_a":"slow_memory_0","condition_b":"random_walk_0","label":"separated","separation":3.1813974737},{"condition_a":"slow_memory_0","condition_b":"random_walk_1","label":"separated","separation":4.2174900463},{"condition_a":"slow_memory_1","condition_b":"random_walk_0","label":"overlapping","separation":2.3289071497},{"condition_a":"slow_memory_1","condition_b":"random_walk_1","label":"separated","separation":3.2957057971}] |
| P10 | PASS | Strong current-rank attraction: fitted middle kappa <=.75 baseline on >=2 profiles; facebook_a=0.37524, reddit_comments=0.40436, reddit_submissions_long=0.62832 |
| P11 | FAIL | Strong current-rank attraction vs strong RW, h26 middle dispersion: {"facebook_a": "unresolved", "reddit_comments": "unresolved", "reddit_submissions_long": "unresolved"} |
| P12 | PASS | Strong cycle: middle b13 slope gap4-minus-gap2 >=.5 on >=2 profiles; facebook_a=1.8131, reddit_comments=1.8491, reddit_submissions_long=1.9038 |
| P13 | FAIL | Strong cycle: fitted middle kappa rises >=.005 on >=2 profiles; facebook_a=-0.027971, reddit_comments=-0.012459, reddit_submissions_long=-0.0029578 |

## Failures, limits and work not run

Failed seed records: 0. Every failed seed is preserved in the summary and denominator accounting. The original diagnostic workers stalled in quadratic warning capture and were terminated; three queued workers were cancelled. Two completed added-case fit cells from those attempts were archived unused and replayed; the 120 completed original recovery fits were retained. A warning-only wrapper bounded capture without changing mathematical functions. One long-panel restart then failed before scientific work because attempt filenames collided; panel/stage/PID names fixed this, and the worker restarted. The comments worker was later restarted after an exact eligibility optimization passed native and controlled equivalence checks. Complete seed cells were retained; no incomplete-fit cell needed archiving at that transition. A redundant queued long-panel diagnostics request was cancelled before execution. After all 120 comments added-case fits completed, its automatic follow-on diagnostic worker was cancelled while waiting on the separate original-diagnostics worker's stage lock, releasing a shared compute slot without cancelling scientific work. Immutable STARTED records from terminated processes are resolved by separate attempt-event records, not rewritten as successful. No incomplete seed or partial aggregate is interpreted. The eligibility wrapper preserves the union of the two declared cohorts, original absence floor, identity order and every numeric diagnostic/count. Runtime wrapper hashes identify mixed execution versions; the scientific producer hashes are constant. See audit.json, eligibility_equivalence.json, attempt events and launch logs.

No empirical A1/A2 comparison, candidate gate, default change, daily-instrument recovery, parameter-refitted composite mechanism comparison, isolated panel-length factorial, or new-data confirmation was run. Native profile and length are confounded. Each diagnostic's finite-sample power is conditional on the frozen nuisance parameters. Tail disappearance from a statistic does not by itself establish the absence of a mechanism.

## Reproduction and review files

Code under validation/: planted_sim.py, planted_crosscheck.py, planted_study.py, planted_extended.py, planted_extension.py, planted_summary.py, planted_extension_summary.py, planted_report.py and planted_audit.py; execution-only wrappers are planted_warning_guard.py and planted_efficient_run.py. Synthetic known-answer tests cover covariance, the baseline and all eight mechanisms, replacement identities, loader mapping, permutation logic and reporting. `planted_plot.py --out <run folder>` regenerates recovery bands and the fixed diagnostic-rate display under `output/v2_plots/`; images stay out of git. The final HEALTH log records both suites and the 14/15, churn .013 legacy guard. Sources: [ADEMP and Monte Carlo reporting](https://pmc.ncbi.nlm.nih.gov/articles/PMC6492164/); [finite randomization p values](https://gksmyth.github.io/pubs/PermPValuesPreprint.pdf).
