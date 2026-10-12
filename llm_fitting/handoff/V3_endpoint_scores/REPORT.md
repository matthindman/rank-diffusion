# V3 — endpoint scores

**supplementary, retrospective, development data. Frozen nominal-row evaluation; calendar alignment is defective.**

The scorer and all four operational tables are complete. A deterministic fixture proves that the first A6 simulation row contains two latent transitions from the filtered end-of-train state, while the observation is one week ahead. The frozen exports are retained exactly. Every h below means the export's nominal row label, not a verified calendar horizon. These results do not establish correctly aligned endpoint predictive skill and do not change any recorded gate verdict. Q1 in QUESTIONS.md asks Claude for a separate correctly timed export.

## Operational findings

Against no_change, the frozen model has lower mean coarsened-rank CRPS in 12/12 all-cohort panel/horizon comparisons. These are nominal-row comparisons, not a corrected calendar forecast verdict.

Against historical_mobility, the frozen model has lower mean coarsened-rank CRPS in 12/12 all-cohort panel/horizon comparisons. These are nominal-row comparisons, not a corrected calendar forecast verdict.

Against rank_bin, the frozen model has lower mean coarsened-rank CRPS in 12/12 all-cohort panel/horizon comparisons. These are nominal-row comparisons, not a corrected calendar forecast verdict.

The rank-bin advantage is smaller than the no-change advantage on every panel at nominal h4. It is not uniform across groups: the comments tail is worse than rank-bin at h4; long-submission middle and shell brackets include zero, as does its overall h13 bracket. The full table retains these results.

Event skill is mixed. At nominal h13 for established endpoints, the model's mean Brier score is lower than rank-bin on Facebook and comments, and higher on both submissions panels. Against no-change it is lower on Facebook, comments and long submissions, and higher on short submissions; several week-block brackets include zero. Short-panel historical/kernel comparisons at h13 have three common origins, versus five for no-change.

P7 fails in the opposite direction: predicted established outside-event frequencies exceed observed frequencies by 0.01719 on comments and0.01589 on long submissions. This is any outside event within a forecast window under the conditional movement stack. It is not the repeat-departure intensity measured with the structural stack in A2, and the extra latent transition can affect it. It does not reverse that earlier residual.

## Design and score convention

Four primary DEVELOPMENT panels; five original origins each; 20 simulation members, seeds 0–19; 2, 000 gate bootstrap draws. DESIGN was committed at e5db2b8 before any V3 run; score/baseline/runner code was committed at 291c41f before panel runs. Source, input, and full-parameter hashes fence restart caches. All simulated member ranks are scored at nominal1, 4, 13 where available. Rank 0 or rank>K is the explicit outside category K+1, scored as log(K+1). This is a coarsened outcome score, not inference about ranks below K or cause of absence.

Fair CRPS and Brier use off-diagonal ensemble corrections valid for iid predictive members conditional on fitted inputs. Fits and filtered states are shared across members. This omits parameter/state-estimation uncertainty. Outside-within-h includes any future absent/below-K week, even if the endpoint returns; origin is not part of the event. Observed absences may return, simulated deaths do not. Observed ranks count present members; simulated ranks count all occupied slots. No rescaling hides that observation-model difference.

No-change repeats the observed origin state. Historical mobility symmetrically signs the gate's training absolute displacements and anchors them at the observed rank; it is a survivor-marginal lift, has no joint path, and gets no event Brier score. The rank-bin comparator transfers full training path snippets to the endpoint's exact origin rank, preserving absences and recalculating crossings. Empty-bin fallbacks and unavailable horizons are recorded. All stratification uses training permanent rank; established additionally requires 70% presence. The origin-inside established group is separate.

## Paired operational comparisons

Positive improvement means baseline score minus frozen-model score. Equal-origin means, identical cases/common origins for each comparison. The brackets are the declared 13-week circular calendar-block2.5–97.5% percentiles; MCSE is a separate whole-member jackknife. They are not independent evidence sources or a combined confidence interval. Complete 7-group, 3-horizon score and calibration tables are in the CSV files.

| panel | h | baseline | model CRPS | baseline CRPS | improvement | 13w bracket | MCSE | origins |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| facebook_a | 1 | no_change | 0.074229 | 0.107468 | 0.033239 | [0.031831, 0.035860] | 0.000110 | 5 |
| facebook_a | 4 | no_change | 0.089068 | 0.131931 | 0.042864 | [0.042070, 0.044598] | 0.000134 | 5 |
| facebook_a | 13 | no_change | 0.107008 | 0.158506 | 0.051498 | [0.049236, 0.053065] | 0.000205 | 5 |
| reddit | 1 | no_change | 0.035682 | 0.049536 | 0.013854 | withheld | 0.000051 | 5 |
| reddit | 4 | no_change | 0.051988 | 0.072086 | 0.020098 | withheld | 0.000079 | 5 |
| reddit | 13 | no_change | 0.072543 | 0.099875 | 0.027333 | withheld | 0.000091 | 5 |
| reddit_comments | 1 | no_change | 0.030987 | 0.042220 | 0.011233 | [0.010235, 0.012059] | 0.000036 | 5 |
| reddit_comments | 4 | no_change | 0.038343 | 0.052996 | 0.014653 | [0.013451, 0.016557] | 0.000027 | 5 |
| reddit_comments | 13 | no_change | 0.050428 | 0.067290 | 0.016862 | [0.015559, 0.018647] | 0.000036 | 5 |
| reddit_submissions_long | 1 | no_change | 0.021620 | 0.029763 | 0.008143 | [0.007321, 0.008679] | 0.000036 | 5 |
| reddit_submissions_long | 4 | no_change | 0.030797 | 0.041226 | 0.010428 | [0.008849, 0.011619] | 0.000049 | 5 |
| reddit_submissions_long | 13 | no_change | 0.043489 | 0.056558 | 0.013069 | [0.010012, 0.015751] | 0.000073 | 5 |
| facebook_a | 1 | historical_mobility | 0.074229 | 0.102242 | 0.028013 | [0.026769, 0.030409] | 0.000131 | 5 |
| facebook_a | 4 | historical_mobility | 0.089068 | 0.123662 | 0.034594 | [0.033638, 0.036478] | 0.000169 | 5 |
| facebook_a | 13 | historical_mobility | 0.107008 | 0.146470 | 0.039463 | [0.037207, 0.040860] | 0.000248 | 5 |
| reddit | 1 | historical_mobility | 0.035682 | 0.048034 | 0.012351 | withheld | 0.000058 | 5 |
| reddit | 4 | historical_mobility | 0.051988 | 0.068993 | 0.017005 | withheld | 0.000073 | 5 |
| reddit | 13 | historical_mobility | 0.073131 | 0.093853 | 0.020722 | withheld | 0.000173 | 3 |
| reddit_comments | 1 | historical_mobility | 0.030987 | 0.041432 | 0.010445 | [0.009491, 0.011260] | 0.000035 | 5 |
| reddit_comments | 4 | historical_mobility | 0.038343 | 0.051869 | 0.013526 | [0.012375, 0.015380] | 0.000027 | 5 |
| reddit_comments | 13 | historical_mobility | 0.050428 | 0.065682 | 0.015253 | [0.014013, 0.017079] | 0.000039 | 5 |
| reddit_submissions_long | 1 | historical_mobility | 0.021620 | 0.028820 | 0.007200 | [0.006398, 0.007722] | 0.000039 | 5 |
| reddit_submissions_long | 4 | historical_mobility | 0.030797 | 0.039878 | 0.009080 | [0.007533, 0.010251] | 0.000057 | 5 |
| reddit_submissions_long | 13 | historical_mobility | 0.043489 | 0.054446 | 0.010957 | [0.007910, 0.013536] | 0.000087 | 5 |
| facebook_a | 1 | rank_bin | 0.074229 | 0.083240 | 0.009011 | [0.007872, 0.010918] | 0.000152 | 5 |
| facebook_a | 4 | rank_bin | 0.089068 | 0.100669 | 0.011602 | [0.010901, 0.012756] | 0.000278 | 5 |
| facebook_a | 13 | rank_bin | 0.107008 | 0.118897 | 0.011889 | [0.010486, 0.013677] | 0.000486 | 5 |
| reddit | 1 | rank_bin | 0.035682 | 0.038098 | 0.002416 | withheld | 0.000090 | 5 |
| reddit | 4 | rank_bin | 0.051988 | 0.055695 | 0.003707 | withheld | 0.000094 | 5 |
| reddit | 13 | rank_bin | 0.073131 | 0.077986 | 0.004855 | withheld | 0.000228 | 3 |
| reddit_comments | 1 | rank_bin | 0.030987 | 0.032351 | 0.001364 | [0.000505, 0.002142] | 0.000045 | 5 |
| reddit_comments | 4 | rank_bin | 0.038343 | 0.040846 | 0.002504 | [0.001940, 0.003301] | 0.000050 | 5 |
| reddit_comments | 13 | rank_bin | 0.050428 | 0.052289 | 0.001861 | [0.001092, 0.002837] | 0.000050 | 5 |
| reddit_submissions_long | 1 | rank_bin | 0.021620 | 0.022903 | 0.001283 | [0.000462, 0.001771] | 0.000050 | 5 |
| reddit_submissions_long | 4 | rank_bin | 0.030797 | 0.031933 | 0.001135 | [0.000188, 0.001668] | 0.000057 | 5 |
| reddit_submissions_long | 13 | rank_bin | 0.043489 | 0.043959 | 0.000470 | [-0.001266, 0.001496] | 0.000097 | 5 |

## Rank groups at nominal h4

| panel | group | baseline | CRPS improvement | 13w bracket | MCSE | approx MC95 band |
| --- | --- | --- | --- | --- | --- | --- |
| facebook_a | head | no_change | 0.164114 | [0.158349, 0.174723] | 0.002717 | [0.158789, 0.169440] |
| facebook_a | middle | no_change | 0.149341 | [0.138755, 0.164168] | 0.001047 | [0.147289, 0.151393] |
| facebook_a | shell | no_change | 0.112158 | [0.105755, 0.117674] | 0.000571 | [0.111039, 0.113278] |
| facebook_a | tail | no_change | 0.020680 | [0.020184, 0.021702] | 0.000095 | [0.020494, 0.020866] |
| reddit | head | no_change | 0.088374 | withheld | 0.001288 | [0.085850, 0.090898] |
| reddit | middle | no_change | 0.075668 | withheld | 0.000514 | [0.074661, 0.076676] |
| reddit | shell | no_change | 0.061804 | withheld | 0.000380 | [0.061059, 0.062548] |
| reddit | tail | no_change | 0.004868 | withheld | 0.000036 | [0.004797, 0.004938] |
| reddit_comments | head | no_change | 0.073892 | [0.069560, 0.077814] | 0.000721 | [0.072479, 0.075305] |
| reddit_comments | middle | no_change | 0.058976 | [0.054469, 0.066971] | 0.000224 | [0.058536, 0.059416] |
| reddit_comments | shell | no_change | 0.049394 | [0.042066, 0.055297] | 0.000156 | [0.049088, 0.049701] |
| reddit_comments | tail | no_change | 0.003377 | [0.003002, 0.004056] | 0.000017 | [0.003344, 0.003409] |
| reddit_submissions_long | head | no_change | 0.041199 | [0.037106, 0.045614] | 0.000589 | [0.040044, 0.042354] |
| reddit_submissions_long | middle | no_change | 0.039511 | [0.031864, 0.044100] | 0.000312 | [0.038900, 0.040123] |
| reddit_submissions_long | shell | no_change | 0.030719 | [0.025445, 0.033836] | 0.000214 | [0.030298, 0.031139] |
| reddit_submissions_long | tail | no_change | 0.003256 | [0.002700, 0.003995] | 0.000026 | [0.003206, 0.003306] |
| facebook_a | head | historical_mobility | 0.065219 | [0.058380, 0.077168] | 0.003338 | [0.058677, 0.071761] |
| facebook_a | middle | historical_mobility | 0.110162 | [0.101213, 0.125167] | 0.001223 | [0.107764, 0.112559] |
| facebook_a | shell | historical_mobility | 0.094253 | [0.087737, 0.099650] | 0.000594 | [0.093089, 0.095417] |
| facebook_a | tail | historical_mobility | 0.018377 | [0.017918, 0.019394] | 0.000100 | [0.018181, 0.018572] |
| reddit | head | historical_mobility | 0.047374 | withheld | 0.001806 | [0.043834, 0.050915] |
| reddit | middle | historical_mobility | 0.061664 | withheld | 0.000555 | [0.060576, 0.062752] |
| reddit | shell | historical_mobility | 0.056414 | withheld | 0.000361 | [0.055706, 0.057122] |
| reddit | tail | historical_mobility | 0.004574 | withheld | 0.000034 | [0.004509, 0.004640] |
| reddit_comments | head | historical_mobility | 0.052502 | [0.048555, 0.056280] | 0.000877 | [0.050783, 0.054222] |
| reddit_comments | middle | historical_mobility | 0.054625 | [0.050092, 0.062586] | 0.000232 | [0.054171, 0.055079] |
| reddit_comments | shell | historical_mobility | 0.047857 | [0.040542, 0.053831] | 0.000159 | [0.047545, 0.048169] |
| reddit_comments | tail | historical_mobility | 0.003229 | [0.002844, 0.003921] | 0.000017 | [0.003196, 0.003262] |
| reddit_submissions_long | head | historical_mobility | 0.021343 | [0.015197, 0.025292] | 0.001112 | [0.019164, 0.023522] |
| reddit_submissions_long | middle | historical_mobility | 0.033372 | [0.025852, 0.037847] | 0.000332 | [0.032722, 0.034022] |
| reddit_submissions_long | shell | historical_mobility | 0.028559 | [0.023395, 0.031455] | 0.000224 | [0.028119, 0.028999] |
| reddit_submissions_long | tail | historical_mobility | 0.003090 | [0.002519, 0.003807] | 0.000028 | [0.003034, 0.003145] |
| facebook_a | head | rank_bin | 0.112955 | [0.099949, 0.132464] | 0.005304 | [0.102559, 0.123352] |
| facebook_a | middle | rank_bin | 0.050169 | [0.045202, 0.056653] | 0.001561 | [0.047109, 0.053229] |
| facebook_a | shell | rank_bin | 0.017061 | [0.014370, 0.018959] | 0.001396 | [0.014326, 0.019797] |
| facebook_a | tail | rank_bin | 0.005301 | [0.004651, 0.006055] | 0.000147 | [0.005014, 0.005589] |
| reddit | head | rank_bin | 0.037167 | withheld | 0.001959 | [0.033327, 0.041007] |
| reddit | middle | rank_bin | 0.016081 | withheld | 0.000664 | [0.014780, 0.017382] |
| reddit | shell | rank_bin | 0.005635 | withheld | 0.000447 | [0.004760, 0.006510] |
| reddit | tail | rank_bin | 0.000919 | withheld | 0.000055 | [0.000811, 0.001028] |
| reddit_comments | head | rank_bin | 0.030741 | [0.024687, 0.034842] | 0.001118 | [0.028551, 0.032932] |
| reddit_comments | middle | rank_bin | 0.014780 | [0.013515, 0.017142] | 0.000367 | [0.014060, 0.015500] |
| reddit_comments | shell | rank_bin | 0.009603 | [0.006200, 0.012162] | 0.000263 | [0.009088, 0.010117] |
| reddit_comments | tail | rank_bin | -0.000606 | [-0.000914, -0.000119] | 0.000029 | [-0.000663, -0.000548] |
| reddit_submissions_long | head | rank_bin | 0.012383 | [0.002149, 0.017094] | 0.000745 | [0.010924, 0.013843] |
| reddit_submissions_long | middle | rank_bin | 0.003641 | [-0.001677, 0.006643] | 0.000444 | [0.002770, 0.004511] |
| reddit_submissions_long | shell | rank_bin | 0.001309 | [-0.001854, 0.002835] | 0.000277 | [0.000767, 0.001851] |
| reddit_submissions_long | tail | rank_bin | 0.000514 | [0.000291, 0.000806] | 0.000037 | [0.000442, 0.000586] |

## Outside-event scores for established endpoints

| panel | h | group | baseline | model Brier | baseline Brier | improvement | 13w bracket | MCSE | approx MC95 band |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| facebook_a | 13 | established | no_change | 0.113699 | 0.115486 | 0.001788 | [-0.004787, 0.005747] | 0.000722 | [0.000373, 0.003202] |
| facebook_a | 13 | established_origin_inside | no_change | 0.112859 | 0.115264 | 0.002405 | [-0.003968, 0.006294] | 0.000704 | [0.001024, 0.003785] |
| reddit | 13 | established | no_change | 0.042481 | 0.041537 | -0.000944 | withheld | 0.000414 | [-0.001756, -0.000132] |
| reddit | 13 | established_origin_inside | no_change | 0.042506 | 0.041580 | -0.000926 | withheld | 0.000412 | [-0.001734, -0.000119] |
| reddit_comments | 13 | established | no_change | 0.021193 | 0.023042 | 0.001849 | [0.000379, 0.002620] | 0.000087 | [0.001679, 0.002019] |
| reddit_comments | 13 | established_origin_inside | no_change | 0.020228 | 0.022583 | 0.002356 | [0.000533, 0.003627] | 0.000078 | [0.002203, 0.002509] |
| reddit_submissions_long | 13 | established | no_change | 0.024550 | 0.027618 | 0.003068 | [-0.001858, 0.006124] | 0.000239 | [0.002600, 0.003536] |
| reddit_submissions_long | 13 | established_origin_inside | no_change | 0.023838 | 0.027313 | 0.003475 | [-0.000304, 0.006143] | 0.000249 | [0.002987, 0.003963] |
| facebook_a | 13 | established | rank_bin | 0.113699 | 0.124733 | 0.011034 | [0.007436, 0.013953] | 0.000972 | [0.009129, 0.012940] |
| facebook_a | 13 | established_origin_inside | rank_bin | 0.112859 | 0.124691 | 0.011832 | [0.008140, 0.015068] | 0.000963 | [0.009945, 0.013718] |
| reddit | 13 | established | rank_bin | 0.042743 | 0.041981 | -0.000762 | withheld | 0.000652 | [-0.002040, 0.000516] |
| reddit | 13 | established_origin_inside | rank_bin | 0.042775 | 0.042022 | -0.000753 | withheld | 0.000649 | [-0.002024, 0.000519] |
| reddit_comments | 13 | established | rank_bin | 0.021193 | 0.026175 | 0.004982 | [0.003168, 0.006861] | 0.000180 | [0.004628, 0.005335] |
| reddit_comments | 13 | established_origin_inside | rank_bin | 0.020228 | 0.025737 | 0.005509 | [0.003882, 0.007248] | 0.000172 | [0.005172, 0.005846] |
| reddit_submissions_long | 13 | established | rank_bin | 0.024550 | 0.023062 | -0.001488 | [-0.006254, 0.001117] | 0.000308 | [-0.002092, -0.000883] |
| reddit_submissions_long | 13 | established_origin_inside | rank_bin | 0.023838 | 0.022730 | -0.001108 | [-0.004726, 0.000910] | 0.000319 | [-0.001733, -0.000483] |

Forecast versus observed event rates (equal-origin means):

| panel | group | h | method | quantity | rate | origins |
| --- | --- | --- | --- | --- | --- | --- |
| facebook_a | established | 13 | frozen_model | event_frequency | 0.125741 | 5 |
| facebook_a | established | 13 | frozen_model | event_probability | 0.206606 | 5 |
| facebook_a | established | 13 | no_change | event_frequency | 0.125741 | 5 |
| facebook_a | established | 13 | no_change | event_probability | 0.013913 | 5 |
| facebook_a | established | 13 | rank_bin | event_frequency | 0.125741 | 5 |
| facebook_a | established | 13 | rank_bin | event_probability | 0.286314 | 5 |
| facebook_a | established_origin_inside | 13 | frozen_model | event_frequency | 0.115264 | 5 |
| facebook_a | established_origin_inside | 13 | frozen_model | event_probability | 0.198553 | 5 |
| facebook_a | established_origin_inside | 13 | no_change | event_frequency | 0.115264 | 5 |
| facebook_a | established_origin_inside | 13 | no_change | event_probability | 0.000000 | 5 |
| facebook_a | established_origin_inside | 13 | rank_bin | event_frequency | 0.115264 | 5 |
| facebook_a | established_origin_inside | 13 | rank_bin | event_probability | 0.276551 | 5 |
| reddit | established | 13 | frozen_model | event_frequency | 0.042578 | 5 |
| reddit | established | 13 | frozen_model | event_probability | 0.078033 | 5 |
| reddit | established | 13 | no_change | event_frequency | 0.042578 | 5 |
| reddit | established | 13 | no_change | event_probability | 0.001040 | 5 |
| reddit | established | 13 | rank_bin | event_frequency | 0.042810 | 3 |
| reddit | established | 13 | rank_bin | event_probability | 0.097558 | 3 |
| reddit | established_origin_inside | 13 | frozen_model | event_frequency | 0.041580 | 5 |
| reddit | established_origin_inside | 13 | frozen_model | event_probability | 0.077190 | 5 |
| reddit | established_origin_inside | 13 | no_change | event_frequency | 0.041580 | 5 |
| reddit | established_origin_inside | 13 | no_change | event_probability | 0.000000 | 5 |
| reddit | established_origin_inside | 13 | rank_bin | event_frequency | 0.041836 | 3 |
| reddit | established_origin_inside | 13 | rank_bin | event_probability | 0.096655 | 3 |
| reddit_comments | established | 13 | frozen_model | event_frequency | 0.025297 | 5 |
| reddit_comments | established | 13 | frozen_model | event_probability | 0.042482 | 5 |
| reddit_comments | established | 13 | no_change | event_frequency | 0.025297 | 5 |
| reddit_comments | established | 13 | no_change | event_probability | 0.003351 | 5 |
| reddit_comments | established | 13 | rank_bin | event_frequency | 0.025297 | 5 |
| reddit_comments | established | 13 | rank_bin | event_probability | 0.066730 | 5 |
| reddit_comments | established_origin_inside | 13 | frozen_model | event_frequency | 0.022583 | 5 |
| reddit_comments | established_origin_inside | 13 | frozen_model | event_probability | 0.040398 | 5 |
| reddit_comments | established_origin_inside | 13 | no_change | event_frequency | 0.022583 | 5 |
| reddit_comments | established_origin_inside | 13 | no_change | event_probability | 0.000000 | 5 |
| reddit_comments | established_origin_inside | 13 | rank_bin | event_frequency | 0.022583 | 5 |
| reddit_comments | established_origin_inside | 13 | rank_bin | event_probability | 0.063657 | 5 |
| reddit_submissions_long | established | 13 | frozen_model | event_frequency | 0.031792 | 5 |
| reddit_submissions_long | established | 13 | frozen_model | event_probability | 0.047685 | 5 |
| reddit_submissions_long | established | 13 | no_change | event_frequency | 0.031792 | 5 |
| reddit_submissions_long | established | 13 | no_change | event_probability | 0.005087 | 5 |
| reddit_submissions_long | established | 13 | rank_bin | event_frequency | 0.031792 | 5 |
| reddit_submissions_long | established | 13 | rank_bin | event_probability | 0.043749 | 5 |
| reddit_submissions_long | established_origin_inside | 13 | frozen_model | event_frequency | 0.027313 | 5 |
| reddit_submissions_long | established_origin_inside | 13 | frozen_model | event_probability | 0.043652 | 5 |
| reddit_submissions_long | established_origin_inside | 13 | no_change | event_frequency | 0.027313 | 5 |
| reddit_submissions_long | established_origin_inside | 13 | no_change | event_probability | 0.000000 | 5 |
| reddit_submissions_long | established_origin_inside | 13 | rank_bin | event_frequency | 0.027313 | 5 |
| reddit_submissions_long | established_origin_inside | 13 | rank_bin | event_probability | 0.038916 | 5 |

## Calibration

Randomized finite-ensemble rank-PIT indicators integrate ties exactly; under exchangeability their expectation is .1,.5,.9 even at the outside atom. Raw quantile brackets, with continuous finite-order-statistic references, are also supplied. Departure-probability/frequency rows are diagnostics, not extra scoring rules.

| panel | h | nominal percentile | rank-PIT proportion | 13w bracket |
| --- | --- | --- | --- | --- |
| facebook_a | 1 | 0.1 | 0.098920 | [0.097340, 0.101366] |
| facebook_a | 1 | 0.5 | 0.500913 | [0.498147, 0.503163] |
| facebook_a | 1 | 0.9 | 0.899105 | [0.897127, 0.900497] |
| facebook_a | 4 | 0.1 | 0.099656 | [0.099048, 0.100355] |
| facebook_a | 4 | 0.5 | 0.500322 | [0.498353, 0.503128] |
| facebook_a | 4 | 0.9 | 0.897924 | [0.896212, 0.899066] |
| facebook_a | 13 | 0.1 | 0.103474 | [0.101514, 0.105971] |
| facebook_a | 13 | 0.5 | 0.497014 | [0.495830, 0.498682] |
| facebook_a | 13 | 0.9 | 0.895936 | [0.894511, 0.897084] |
| reddit | 1 | 0.1 | 0.090698 | withheld |
| reddit | 1 | 0.5 | 0.497707 | withheld |
| reddit | 1 | 0.9 | 0.906947 | withheld |
| reddit | 4 | 0.1 | 0.090750 | withheld |
| reddit | 4 | 0.5 | 0.500390 | withheld |
| reddit | 4 | 0.9 | 0.903934 | withheld |
| reddit | 13 | 0.1 | 0.086860 | withheld |
| reddit | 13 | 0.5 | 0.500758 | withheld |
| reddit | 13 | 0.9 | 0.903256 | withheld |
| reddit_comments | 1 | 0.1 | 0.096826 | [0.092920, 0.099287] |
| reddit_comments | 1 | 0.5 | 0.506952 | [0.505024, 0.510503] |
| reddit_comments | 1 | 0.9 | 0.903464 | [0.901014, 0.906534] |
| reddit_comments | 4 | 0.1 | 0.091499 | [0.088786, 0.094428] |
| reddit_comments | 4 | 0.5 | 0.508207 | [0.505957, 0.511176] |
| reddit_comments | 4 | 0.9 | 0.905148 | [0.902315, 0.907285] |
| reddit_comments | 13 | 0.1 | 0.084634 | [0.082927, 0.086689] |
| reddit_comments | 13 | 0.5 | 0.512050 | [0.509856, 0.514674] |
| reddit_comments | 13 | 0.9 | 0.907154 | [0.905864, 0.907847] |
| reddit_submissions_long | 1 | 0.1 | 0.085793 | [0.081943, 0.089111] |
| reddit_submissions_long | 1 | 0.5 | 0.506699 | [0.504335, 0.510071] |
| reddit_submissions_long | 1 | 0.9 | 0.909269 | [0.907631, 0.911389] |
| reddit_submissions_long | 4 | 0.1 | 0.083826 | [0.079307, 0.086831] |
| reddit_submissions_long | 4 | 0.5 | 0.509184 | [0.506040, 0.514218] |
| reddit_submissions_long | 4 | 0.9 | 0.908939 | [0.906824, 0.911406] |
| reddit_submissions_long | 13 | 0.1 | 0.079902 | [0.074809, 0.083636] |
| reddit_submissions_long | 13 | 0.5 | 0.509673 | [0.506808, 0.513204] |
| reddit_submissions_long | 13 | 0.9 | 0.908973 | [0.906335, 0.911673] |

## Uncertainty and scope

Every bootstrap draw resamples calendar blocks with all endpoints and origins' joint score bundles together. Five overlapping origins are not five experiments. The primary13-week and sensitivity26-week results are descriptive conditional resummaries of sparse forecast origins, not validated frequentist coverage for new panels. Circular wrapping adds artificial adjacency. Empty draws are counted. At least3 origins and a calendar span of2 blocks are required; otherwise brackets are withheld. The short submissions panel cannot supply such intervals. Larger entity counts do not repair its short time support.

Whole-member jackknife MCSE deletes the same ensemble index across cases/origins to retain shared-draw dependence. It describes conditional simulation precision only; 20 members make this approximate. The mc95 columns are score or paired improvement plus/minus1.96MCSE: normal-approximation simulation-error bands, not sampling or refitting confidence intervals. They are supplied explicitly for head rows including the short panel. Head groups use all 20 members and both reported uncertainty summaries where the calendar permits. No refitting, parameter bootstrap, specification-selection correction, familywise inference, or fresh confirmation was performed. The fair scores judge the actual sampler, including random amplitudes in these conditional-state stacks; they do not imply personalized amplitude estimates.

## All predictions

| prediction | verdict | result |
| --- | --- | --- |
| P1 | PASS | 11 known-answer tests pass; full health log accompanies handoff. |
| P2 | PASS | Conditional primitive/export/cache replay exact; protected model files unchanged. |
| P3 | PASS | Two transitions at nominal h1; calendar-aligned skill withheld. |
| P4 | PASS | Positive nominal h4 all-cohort CRPS improvement on 4/4 panels against no_change. |
| P5 | PASS | Positive nominal h4 all-cohort CRPS improvement on 4/4 panels against historical_mobility. |
| P6 | PASS | Advantage over rank-bin smaller than over no-change on 4/4 panels. |
| P7 | FAIL | [('reddit_comments', -0.0171855500433559), ('reddit_submissions_long', -0.015892781915462298)] |
| P8 | PASS | All short-panel week-block intervals withheld. |

## Execution and validation

Actual gate attempts: 4; completed: 4. Every attempt is in GATE_ROWS.md and the archived attempt records. Synthetic known-answer tests are unit tests, not panel gate evaluations. The full export audit checks all 20 splits, alignment, rank bounds, simulated absorbing absence, unique live ranks, baseline support and source-linked file hashes.

The unchanged five-origin gate is called through read-only wrappers. Calibration distributions are stored losslessly as per-seed histograms; conditional cohort outputs and full member ranks are cached atomically. Exact synthetic output and replay checks passed. Full exports and restart caches remain under ignored output/V3_endpoint_scores; compact provenance, all aggregate tables and logs are committed.

No jobs were killed or scientific attempts omitted unless an explicit later execution note says otherwise. Calendar-aligned evaluation is not run pending Q1. No candidate, model default, registered threshold, package code or sealed data is changed.

## Reproduction

For each panel: bash llm_fitting/handoff/V3_endpoint_scores/launch.sh PANEL (registered interpreter, caffeinate, heavy_lock, restartable). After all four: python llm_fitting/validation/endpoint_audit.py --root output/V3_endpoint_scores --out llm_fitting/runs/2026-10-12_V3_endpoint_scores/audit.json; python llm_fitting/validation/endpoint_analysis.py --exports output/V3_endpoint_scores/exports --out llm_fitting/runs/2026-10-12_V3_endpoint_scores; python llm_fitting/validation/endpoint_report.py --root output/V3_endpoint_scores --out llm_fitting/runs/2026-10-12_V3_endpoint_scores --handoff llm_fitting/handoff/V3_endpoint_scores. Use the registered Python 3.11 path for every command.

Method sources: [Ferro 2014](https://empslocal.ex.ac.uk/people/staff/ferro/Publications/ferro2013.pdf), [Gneiting and Raftery 2007](https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf).

## Final audit note

All original table columns remained exactly equal after the scope-label/header repairs. Frozen source and input hashes are unchanged after all runs; the long-panel gate result exactly reproduces V1's matched seeds. Four attempts completed, none failed or was killed. Q1 remains unanswered on main. See STATUS_ENTRY.md for the compact complete record, and audit.json/provenance_check.json for machine checks.
