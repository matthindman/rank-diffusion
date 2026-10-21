# Iteration 1: what was checked, accepted and changed (Claude Opus 5.5, 2026-10-21)

Review: `REVIEW_iter1_gpt-6-astra.md` (send out without enthusiasm, 6/10). Each critique was checked against the outputs before acting.

| # | critique | check | action (v8.2) |
|---|---|---|---|
| 1 | Title and opening tie the asymmetry to the hierarchy's fall, which is not shown (submissions head D −0.004) | confirmed (main text itself says so) | title "Declines in Reddit community activity persist more than gains"; abstract, significance, intro findings and Discussion lead with the asymmetry; "we do not show that the persistent decliners caused the leaders' losses" |
| 2 | Bands are community bootstraps conditional on period and whole-window strata | confirmed (`r17_round8.boot`) | stated in main text and SI; leave-one-origin-out ranges added (0.207–0.244, 0.115–0.154; recomputed here from `by_origin`) |
| 3 | "1.5 × membership" divides by a one-year proxy; volatility response not identified | confirmed (retention 0.413 and 0.470 at year 9; volatility fit at bounds) | finite-horizon statement (half the fitted response by year 5 while 55–57% of members remain); volatility "not identified"; GLLM as motivation only |
| 4 | "About three times" is a null-relative contrast | confirmed (3.51, 2.47) | multiplier only in one explicit null-relative sentence |
| 5 | Table 1 and Fig. 3 show the R16 joint optimum, the prose R17's | confirmed | labelled; R17 optimum's D added to Fig. 3C and the SI table; "the best evaluated specifications fail their calibrated objectives" |
| 6 | Asymmetry visually subordinate; SI tables tiny | agreed | Fig. 3 rebuilt: A persistence by gap (tighter axis), B decile curves (axes renamed), C D with bands (data, strata, item counts, five worlds), D forecast; items panel to SI Fig. S2; SI asymmetry table split at normal size. Joint and provenance tables not yet split |
| E1 | Comments history omits the last cell (slide 0.34, −0.13) | confirmed (SI table) | corrected in Results 2; abstract and Discussion say "after 2020" / "changed little" |
| E2 | SI "every time in this table" false for the fitted table | confirmed | restricted to the analytical times |
| E3 | PLAN R17-C form (amplitude tied) vs fitter (A free) | confirmed; both committed at 6006230 | disclosed in SI |
| E4 | r/ksi omitted from the five named | confirmed (`named` in r17_events) | added |
| E5 | Fig. 3D caption, axis names | confirmed (slopes through plotted means 0.2175, 0.1304 vs D) | caption "averaged over origins"; axes "initial / later year-long change" |
| E6 | SI registration "R16-B5; it fits"; old sign-split called "the asymmetry statistic" | confirmed | both corrected |
| E7 | "Edge" undefined in Table 1 | confirmed | defined |
| — | "Self-reinforcing decline is documented" overstates | agreed | "growth followed by stabilization and decline is documented" |
| — | D = 0 null needs a common linear coefficient across strata | agreed | Methods and SI |
| — | "dropping exits biases against", "little power", "persistent offset" too firm | agreed | softened in SI and protocol |
| — | Instagram: small net negative in the main text, positive in the SI | agreed | removed from Results; a one-line pointer in Methods; SI paragraph kept |
| — | Suggested abstract and significance | used their ordering, not their text | own versions within limits (245 and 104 words) |
| — | Paired seed differences D_joint − D_control | not done | worlds do not share random numbers in a way that makes pairing informative |
| owner | Calendar-aware inference, pre-period membership, capture forensics, bridge | need new computation or data | listed for the owner |
