# The boundary theory on Instagram, through the censoring apparatus: design and predictions (before any run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-15. Committed with the
runner `ig_boundary.py`, the planted censoring simulator
`llm_fitting/ig_censoring_sim.py`, its tests
(`tests/test_ig_censoring_boundary.py`) and the opt-in options of
`mts_boundary_law.analyse`, before any Instagram panel is read for this
design. Owner's request: "Now do Instagram … Think hard about how to model
and validate how this censoring process impacts our data, and incorporate
the Instagram data into our analysis."_

## The censoring, and what it does to the balance law

CrowdTangle's Instagram search had to be given a query; the program's panels
hold the posts whose caption matches "a" (§2z-c). The measurement model,
measured in §2z-c on the full 2.31-million-account panel, has three layers:

- **L1, thinning.** An account's observed weekly total sums the engagement of
  its M_it matching posts, where M_it ~ Binomial(posts, q_i). So
  log Y_obs = log Y_true + log q_i + noise of variance about (1 + c²)/M,
  with c² = 0.253 measured.
- **L2, instrument dropout.** 10–17% of account-weeks are missing, flat in
  posting rate and correlated across accounts within a week. There is also a
  collection ramp in weeks 1–8.
- **L3, ghosts.** Low-q accounts surface rarely with large totals.

What each does to the boundary law, d shape/dt = c − s̄ with c = σ²/(2S),
written in observed coordinates:

1. **A persistent q_i** is a fixed offset per account in log activity.
   - It cancels in every change, so slides and σ² are unaffected.
   - In the exponential part of a ladder an independent offset per name
     leaves the local slope unchanged: convolution preserves the
     exponential tail.
   - So the crossings and c are what they would be without censoring. The
     *level* ladder is not, and level claims stay out of scope, as §2z-c
     ruled.
2. **Thinning noise** is passing, so the block means and the difference of
   spacings remove it. It is heteroskedastic, larger for accounts that post
   less.
3. **Dropout is missingness, not behaviour.** The default rules (presence in
   90% of a block's weeks; absence counted at the floor rank) would sort
   accounts by dropout luck. The apparatus options:
   - block levels need presence in half a block's weeks;
   - the cohort's permanent rank averages the weeks present.

   Universe membership stays the §2z-c rule, absence-penalised over all
   accounts, which sinks the L3 ghosts.
4. **A drifting q_i**, from caption habits changing, would add to measured
   movement. It cannot be identified within the panel. It can be bounded:
   - log Y = log M + log Ȳ, and q enters only M, the matching-post count;
   - so the variance of log M bounds q drift together with posting volume;
   - the per-post panel (`instagram_pp`, Ȳ = Y/M) is q-free.

**Planted validation** (`ig_censoring_sim.py`, six seeds; tests use two).
True worlds with lasting variance, passing swings and log-normal posting,
then L1–L3 at the measured rates. Ratio R̃/c pooled over K/8 … K:

| world | ratio | censored minus truth |
|---|---|---|
| truth | 1.014 | |
| censored, persistent q (log-sd 1) | 1.015 | 0.001 (per-seed spread 0.17) |
| q drifting at 0.002 a week | 1.03 | |
| q log-sd 2 | 0.93 | |

- With the default rules the censored panels cannot be adjudicated, because
  presence fails.
- With one year of data the spacings are 14 and 28 weeks. Passing swings do
  not fully fade by then, so σ² and the crossings are both displacement
  variances at those horizons, not the lasting part alone. Their ratio is
  unaffected (planted, and §2z-ct).

## Data and set-up

| | panel | K | rules |
|---|---|---|---|
| totals | `instagram_hm` (DEVELOPMENT through the apparatus) | 10,000 (registered measurability K, §2z-c, §2z-d) | apparatus options |
| per post | `instagram_pp` (q-free) | 10,000, the same universe size | apparatus options |
| negative control | raw `instagram`, the per-week top-50k cut (RESTRICTED: negative control, never calibrated to) | 10,000 | default rules and apparatus options, reported |

- Weeks 9–52 of 2023: the ramp is dropped, leaving 44 weeks. Six 7-week
  blocks give one start, with the cohort on block 0.
- The decomposition splits the totals' displacement variance into log M and
  log Ȳ by the same variance route.

## Predictions

| | prediction | passes if |
|---|---|---|
| I1 | crossings follow σ²/(2S) on Instagram totals | R̃/c in [0.75, 1.33] at ≥ 4 of the 5 cut-offs K/16 … K |
| I2 | the same on the q-free per-post panel | the same rule on `instagram_pp` |
| I3 | the raw panel cannot be read with the default rules | default-rule run on raw `instagram` NOT ADJUDICATED |

**Reported, not scored:**
- the share of the totals' displacement variance carried by log M, by
  stratum. This is the room left for q drift and posting volume;
- the lasting slides (B2's rule);
- the raw panel under the apparatus options.

## Not in this design

- Level-ladder claims; the true ladder is unidentifiable (§2z-c).
- Any model fit or calibration to Instagram.
- The post-level dailies.
- 2024, which is not acquired.
