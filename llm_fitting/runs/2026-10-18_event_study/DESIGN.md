# June 2021 with placebo years and control ladders (exploratory; expectations written before the run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18._

**Why.** All three external reviews said the June 2021 claims (§2z-ca, §2z-cn) have no control. The
dated drift change was found on daily panels (onset 23–24 June) but never compared with the same
calendar weeks of other years, with lower ranks of the same ladder, or with the comments ladder,
which comes from the same archive and the same capture regime.

**The design (weekly panels; DEVELOPMENT; no name read).**
- Event week: the week containing 28 June, in 2019, 2020, 2021 and 2022. The year 2021 is the
  event; the other three are placebos.
- Cohorts by permanent rank over the 13 weeks before the event week (absence-penalised):
  - head: top K/64;
  - upper: K/64 … K/16;
  - control: K/4 … K.
- Outcome: the cohort's mean log level, among members present in at least 90% of weeks in both
  windows, relative to the control cohort. Pre window: weeks −8 … −1. Post window: weeks +1 … +8.
  The statistic is Δ = post − pre.
- Ladders:
  - submissions (treated);
  - comments (control ladder, same archive regime).
- Within-community contrast: for the submissions head cohort, the same identifiers' comment
  activity, relative to the comments control cohort.
- Intervals: 5–95%, 1,000 bootstrap draws resampling communities within each cohort.

**Expectations.**

| | expectation |
|---|---|
| X1 | Submissions head, 2021: Δ is negative, and below every placebo year's Δ by more than the placebo's own 5–95% half-width. |
| X2 | Comments head, 2021: Δ is not negative beyond its interval. The comments head rose in April–September 2021 (§2z-cr). |
| X3 | The submissions head's own comment activity, 2021: Δ is not negative beyond its interval. Feed exposure moved votes per post, not participation. |
| X4 | The placebo years' Δ for the submissions head all lie within ±0.10. |

**What it cannot show.** The archive's capture regime changed on 1 July 2021, inside the post
window. Comments share that regime, so X2 is the check: an artefact common to both ladders would
show in both. Differential removal of submissions against comments is not excluded.
