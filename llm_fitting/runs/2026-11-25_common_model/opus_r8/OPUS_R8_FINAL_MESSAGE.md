Still running; background waits are armed and will notify me when the Comments and full-gate results land.

Waiting for the drift-active Comments origins (80, 106, 133, 160).

Comments T0=80: S3′ **0.138 against R6 S3 0.193** (better). S2′ 0.138 against R6 S2 0.121 (worse). Three origins remain on this panel.

Waiting for Comments origins 106, 133 and 160.

Still waiting on Comments T0 = 106, 133, 160.

Waiting for the remaining Comments origins.

At T0=80, S3′ improves exactly where predicted: dRank13 error 0.18 (R6 0.27), RACF1 0.14 (R6 0.23), coll1 0.41 (R6 0.61). S2′ is slightly worse on coll1, coll5 and RACF1. Waiting for the rest.

Comments T0=106: S3′ **0.092 against R6 S3 0.154**; S2′ 0.198 against R6 S2 0.136. Clear divergence: the rank law helps S3, while adding drift to S2 hurts on the gate.

The S2′ loss at T0=106 is almost entirely **coll1** (rank-1 change rate, error 0.88 against R6 0.51). That fits the risk flagged in the log: in S2′ the drift takes variance near K and the core law tilts toward a more volatile head. S3′ improves dRank13 (0.08) and coll1 (0.28 against R6 0.88).

Comments T0=133: S2′ 0.049 (R6 0.078) and S3′ 0.021 (R6 0.043), so both improve here. One Comments origin left.

Waiting for Comments T0=160.

Waiting for the last Comments origin.

Still waiting on Comments T0=160.

Comments gate, with the rank law active:

| Arm | R6 | R8 | Wins vs persistence |
|---|---:|---:|---:|
| S3′ | 0.124 | **0.087** | 5/5 (and beats S2′ at 5/5 origins) |
| S2′ | 0.113 | 0.120 | — |

S3′ at 0.087 is the best Comments gate score of any round (R5's previous best was 0.096). S2′'s +0.007 exceeds the pre-registered +0.005 tolerance, so S2′ fails on Comments. Submissions is next.

Waiting for Submissions.

Waiting for the Submissions gate origins.

Submissions gate is running; I'll report when it finishes.

Waiting on Submissions.

Waiting for the Submissions gate.

All Comments waits have closed. The remaining background wait reports when the Submissions gate summary lands.

Waiting for Submissions.

Waiting for the Submissions gate result.

Submissions T0=53 reproduces R6 for both arms (S2 0.206, S3 0.313; drift off at T<75). Four drift-active origins follow.

Waiting for Submissions origins 80–159.

Waiting.

Waiting for the drift-active Submissions origins.

The paired view sharpens the picture:
- **Comments S3′ improves at all 4 drift-active origins (4↓/0↑, −0.037 ± 0.022)** on exactly the predicted moments: dRank13, RACF1 and rank-1 collisions.
- **Submissions S3′ is 1↓/3↑.** T0=80 is the core-law flip to the β bound; T0=159 is rank-1 collisions on a 0.077 base rate (relative error 1.59).

Neither changes the pre-registered verdict. Waiting for Gold.

I ran Round 8 end to end: diagnosis, one candidate, the exact Gate v1 and Gold benchmarks, and the report. **My recommendation is to keep Round 6.** Neither change passed the decision rule I wrote down before the run.

**What the diagnosis found**
- **The brief's surge suspicion didn't hold up.** The κ₅/κ₃ estimate is dominated by the top 0.5% of observations, as the brief said. But the fitted S3 surges reproduce the data's far-tail asymmetry on Reddit closely, so I left the surge law unchanged.
- **Persistent drift depends on rank, and Round 6 ignores that.** Drift is about 0.4–0.6× as large among the top entities as near rank K, yet R6 S3 uses one value for every rank. That overstates head drift variance by 2–4×.
- **The drift estimate fires on panels with no drift.** On simulated drift-free data, Round 6's estimator picks up spurious drift from sampling noise.

**What I changed (candidate v1)**
- **Drift in both arms.** S2 now also gets persistent drift, and it switches on only when the measured effect exceeds 2 standard errors.
- **Rank law for drift.** Drift size now varies with rank, set from the covariance measured within three rank tiers. This adds one scalar to S3 and two to S2 on Reddit.
- **No change on FB/IG.** The drift never switches on there, so every Facebook and Instagram fit is exactly Round 6. With both new flags off, the module reproduces Round 6 bit for bit.
- **Tests:** 29/29 pass, including 6 new recovery tests on planted panels. The new rank slope is underestimated on planted data (it shrinks toward Round 6's uniform drift), which the tests and report state.

**Gate v1 (lower is better)**

| | R6 S2 | R8 S2 | R6 S3 | R8 S3 |
|---|---:|---:|---:|---:|
| FB | 0.089 | 0.089 | 0.091 | 0.091 |
| IG | 0.306 | 0.306 | 0.304 | 0.304 |
| Comments | 0.113 | 0.120 | 0.124 | **0.087** (better at all 4 origins where drift is on) |
| Submissions | 0.178 | 0.191 | 0.198 | 0.260 |

**Gold benchmark**
- **Drift in S2 helps aggregate structure.** Reddit persistence (gapped ρ) turns from negative to positive (+0.09/+0.10 against data +0.15/+0.16). The Submissions top share now declines like the data (0.577 → 0.436, data 0.356). Its 52-week top-entity forecast correlation goes from −0.44 to +0.37.
- **But it costs elsewhere.** It loses on the gate, worsens log-rank CRPS in 9 of 10 changed Reddit cells, and moves the Reddit asymmetry D further from the data.

**The main new finding**
- **R6 S3's lasting-variance fit isn't identified on Reddit.** After surge and drift variance are subtracted, most rank bands are left with exactly zero lasting variance. The fitted size-volatility slope then swings between its bounds on tiny changes.
- **That instability drives the Submissions S3 loss.** At the origin with the biggest loss (week 80), a 6% change in drift flipped the slope to its lower bound and doubled top-entity volatility.
- **My stabiliser didn't help.** Refitting on the variance scale just pinned the slope at the opposite bound. I didn't run further variants.
- **Proposed next step (not adopted):** fit all the component rank laws jointly to the whole variogram, then retest the drift rank law under a new pre-registration.

**Corrections and caveats**
- **Interim misstatement.** In a live update I said S2's loss at Comments week 106 came from a more volatile top entity. It's the opposite: the rank-1 occupant changes too rarely. The report is correct.
- **IG S3 comparator.** IG S3 Goal 2 differs from the brief's comparator in the 4th decimal. That comparator activated jumps at one IG origin, so it isn't exactly Round 6 source there.

I only used the four development caches. Nothing outside `opus_r8/` was written, which I checked against the session start, so I didn't save a memory note for this session either. Heavy jobs ran one at a time under the swap watchdog, which never fired.

Everything is in `opus_r8/`; start with `OPUS_R8_REPORT.md`, then `EXPERIMENT_LOG.md` and `COMPARISON_TABLES.md`. The result JSONs, the candidate module and the 29 passing tests sit alongside them.
