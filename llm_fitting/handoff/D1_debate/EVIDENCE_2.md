# Evidence, second sheet: Codex's planted-mechanism study (V2)

Added 2026-10-12 after `EVIDENCE.md`. V2 landed after Claude's first
position was written and hashed, so that position does not use it. Both
sides may use it from their next file on, and should say if it changes
their first position. Record: MODEL_STATUS §2z-bc (Codex's entry) and
§2z-bd (what it changes in the earlier entries). Values are means over 20
seeds on the 212-week profile, middle rank group, unless stated.

## What planted mechanisms do to the statistics the debate leans on

| planted | whole-year slope, adjacent / one apart | cohort's median rank shift | fitted reversion rate, middle bands |
|---|---|---|---|
| baseline (reversion to a home) | −0.010 / −0.223 | 0.049 | 0.0080 |
| slow memory, strong | −0.006 / −0.195 | 0.029 | (see run tables) |
| random walk, weak / strong | +0.129 / +0.004; +0.202 / +0.008 | 0.036; 0.141 | 0.0056; 0.0051 |
| pull toward the ladder at the current rank, 0.02 / 0.10 per week | +0.191 / −0.007; +0.188 / −0.010 | 0.162; 0.166 | 0.0050; 0.0050 |
| annual cycle, peak 0.5 / 1.5 log units | −0.008 / −0.227; −0.013 / −0.229 | 0.045; 0.027 | 0.0072; 0.0050 |
| the data (long submissions) | +0.313 / +0.007 | 0.134 | |

- Whole-year slopes separate reversion to a home, and slow memory, from a
  random walk and from current-rank attraction. They are unchanged by a
  planted annual cycle.
- **No endpoint-level statistic in the battery separates a random walk
  from current-rank attraction** (V2 prediction P11 failed on all three
  profiles: 26-week dispersion and fitted reversion both "unresolved").
- Planted annual cycles lower the fitted reversion rate (to the slow bound
  at the larger size on all three profiles).

## What the estimator recovers from its own simulated data

- Reversion rate: at a bound in 46–68% of bands; median relative error
  0.9–1.7. Stationary variance of the lasting level: median relative error
  0.5–0.9.
- Amplitude spread: bias under 0.01 at the frozen values; 0.14–0.19
  returned when the truth is 0, on every seed.
- Tail thickness: a Gaussian returned on Facebook on every seed when the
  truth is 4.3 degrees of freedom; 10.8 against 5.2 on comments.
- The independent simulator agrees with the production one on 57 of 57
  checks.

## Limits V2 states

Native profile and panel length are confounded (86, 136, 212 weeks, one
profile each). Detection is conditional on the frozen nuisance parameters.
Every baseline seed triggers at least one of the 781 statistics, so the
battery supports no family-wide claim. Ladder drift, the cross-sectional
statistic that differed between the arms of §2z-ay, was not in the
battery.
