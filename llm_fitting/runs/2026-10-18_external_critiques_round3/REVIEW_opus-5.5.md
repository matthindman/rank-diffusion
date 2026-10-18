_Round-3 review by a fresh Claude Opus 5.5 session (Claude Code subagent, no prior context), 2026-10-18, from `CRITIQUE_PROMPT_R3.md` (committed 69bc82e before the review). Read-only; saved verbatim by the orchestrating session. Model output: checked in `SYNTHESIS_R3.md`._

# Round-3 referee report: "Continual turnover sustains a slowly changing hierarchy of online attention"

**How I worked.** I read the draft and compiled a scratch copy outside the repository: 7 pages, abstract 249 words, significance 115, and the only warning is a duplicate `LastPage` label. I then read:
- the five figures and `make_pnas_figures.py`;
- PLAN.md and RESULTS.md;
- §2z-dg and §2z-df;
- frame d and bridge protocol v3.1;
- `rankdiff_rdm.py` and `mts_balance_law.py`;
- theory note §8.

I recomputed from saved JSON only: `gate_*.json`, `law__*.json`, `boundary__*.json`, and the turnover and Gomez outputs. No data file or sealed path was opened, and nothing in the repository was changed.

---

## 1. Editor's decision now

**Send out, but without enthusiasm.**

1. The puzzle deserves PNAS, and the data are unusual: four years, three ladders, named units, thousands of ranks. Two findings are new and well defended:
   - size gives little protection (β 0.05–0.08 on comments, about 0 on Facebook, against about 0.2 for firms);
   - gapped persistence of decline on Reddit.
2. The title's verb rests on statistics that are identities or near-identities:
   - Eq. 1 is post hoc kinematics fed with the same period's changes;
   - the authors' own theory note (§8) says the Eq. 2 test is automatic for any level diffusion;
   - the slide alone carries Fig. 3C's correlation (0.97; see §3).
3. The "slowly changing hierarchy" is a mean-log quantity computed on persisting names. By arithmetic share, concentration fell in every one of the 18 ladder × cut-off × year cells, by a third at the submissions head.
4. The generative model is reported in three versions as if they were one. It does not beat carrying the training statistics forward on the long submissions panel, and drift by rank, its structural ingredient, makes held-out error worse.
5. A referee with the public repository will find each of these points, so the paper must say first which statistics are automatic and which are discoveries.

**The single change that would most raise the odds:** add a null-world column to Figs. 2–3. Compute every headline statistic in three worlds:
- a driftless Gibrat world inside a re-selected top-K universe;
- an Atlas world with a firm-like β = 0.2;
- neutral copying.

Then report which statistics differ from the data. That turns the accounting into evidence.

## 2. The title and the central claim

The title is not earned as written. **The weakest word is "sustains."** The evidence for it is of three kinds, none of them a test of causation.

**(a) The boundary ratio of 0.94 is a near-identity.** Theory note §8: "R̃ = S·κ(1/S) … holds at every instant, in or out of equilibrium"; the test "does not establish a mechanism, and it cannot separate level models."

**(b) The same-year rate law is accounting.** Recomputed on the 60 cells in `runs/2026-10-14_balance_law_halfyear/cells/law__*.json`, the correlation of the observed shape change with:

| predictor | correlation |
|---|---|
| c − s̄ (the paper's) | 0.904 |
| −s̄ alone | **0.971** |
| c alone | 0.108 |
| a ladder-wide constant minus s̄ | 0.969 |

The boundary theory adds nothing to Fig. 3C; it lowers the fit.

**(c) The counterfactual is model-internal.** There the Atlas balance holds by construction.

**The "slide" is largely definitional.**
- `mts_balance_law.py:114` measures s̄ against ΔU_K, the change of the re-selected top-K mean. So s̄_K equals the universe's own reordering gain.
- On comments, s̄_k − s̄_K at k ≤ K/2 is −0.0007 to +0.0012 a week, against s̄_k of 0.0022–0.0043 (`law__reddit_comments_ext.json`).
- In a driftless Gibrat world every cohort also "loses ground to the ranking." The significance and abstract sentence "Every community loses ground" is close to a tautology.

**"Slowly changing" is second-weakest.**
- It is never quantified.
- Fig. 1A's own submissions curves drop by 1–2 log units at rank/K < 10⁻³.
- The A8 arithmetic shares fall in all 18 cells: submissions top 78 from 0.502 to 0.340, comments top 195 from 0.610 to 0.497, comments top 3,125 from 0.947 to 0.910. In each cell the within term exceeds displacement, so arithmetically the inflow never balanced the losses, not even on comments.

**"Turnover" puts the outcome in the subject.** The paper's mechanism is σ², which it calls churn; turnover is what Eq. 1 derives from it.

**Better title:** *"Turnover replenishes a slowly flattening hierarchy of online attention."*
- "Replenishes" names the measured displacement, +0.04 to +0.08 of the share a year in every cell, without claiming that it holds the shape.
- "Flattening" is what all 18 arithmetic cells show, and the contradiction survives.

An alternative that keeps the strongest finding: *"Size does not protect online leaders, and their replacement replenishes the hierarchy."* If "sustains" stays, the text must define it as the measured accounting.

## 3. Each Results section

**(1) The puzzle.** The fast churn is supported: 16–21% at k ≥ 78/195 and 27–33% of the top ten. The slow hierarchy is only asserted.
- **First attack:** "slow" has no statistic, and the arithmetic shares contradict it (above).
- The top-ten figures rest on 30 Reddit name-years and 20 Facebook name-years, a standard error of about 0.08, with no bands. AGENTS.md requires bands for head claims.
- The significance says "a fifth or more of every top set," against 16% for the comments universe.

**(2) Scale-free churn and Eq. 1.** β is sufficient on comments and Facebook:
- leave-one-out 0.049–0.081;
- extended to 4K, under 0.11;
- Facebook lasting β 0.019.

**First attack:** Eq. 1 is post hoc, and the registered form failed: within band in 11%, 39% and 25% of cells. It is evaluated with the same block pair's boundary change and the same names' changes, so "no free parameter" reads as a prediction it is not. It is also the finite-horizon twin of the Eq. 2 kinematics, so Results 2 and 3 are not independent evidence.

Further problems:
- "The typical downward move is nearly constant" is overstated. On comments, E[(Δθ−Δ)⁺] runs 0.21 → 0.40 from k = 195 to 6,240 (first pair, `out_turnover_kinematics.txt`) while S runs 1.04 → 2.29. Flat turnover comes from both rising.
- The top-ten claim lies outside the formula's cells, whose smallest k is 54–195.
- A7-4's failure is omitted: Facebook post counts do steady with size (β 0.187). Frame d lists this under "does not claim," but the main text says "on Reddit" and stops.

**(3) The balance.** The section's lead sentence is not supported as worded.
- **First attack:** the Fig. 3C recomputation above.
- **Second attack:** "match … at every rank on three ladders."
  - On submissions, measured and predicted crossings are anti-correlated across cut-offs (r = −0.51; ratios 1.64, 1.86, 0.72, 0.61, 0.97, 0.90, 0.87; `boundary__reddit_submissions_long.json`).
  - The pooled ratio uses K/16…K and so excludes the two failing head cut-offs. Submissions alone is 0.80 (0.63–1.01).
  - Comments sits above 1 at 5 of 7 cut-offs (1.08–1.22).
- **The composition term is not small.** The statistic includes C, which is +25–35% of it on Reddit and −30% on Facebook (§2z-da §4). The main text calls it "a small composition term."
- **The variance route absorbs drift heterogeneity** (σ² + 3D·Var μ, theory note §8). So on Reddit the "churn" partly is the persistent drift that Results 5 says the model lacks.
- **The stationary formula drops a term.** "S_k ≈ σ²/(2s̄_k) … within about a third" is computed in code as σ²/(2(s̄ − entry)) (`mts_balance_law.py:192`).
  - With the printed formula, comments at k = 195 gives 0.95 against an observed S of 0.91; with entry it gives 0.63.
  - Entry on comments is −0.0004 to −0.0018 a week, as large as the imbalance. Eq. 3 lists it, then drops it.
- **Mild look-ahead:** S averages the start and end slopes (`mts_balance_law.py:185`).

**(4) The generative model, long-panel gates and corrected variant.**

**First attack: one name, three models.**
- The 30-week win (0.097 against 0.128) is the frozen `movinghome`; GATE_LEDGER has no `movinghome_vp` run on `reddit`.
- A6 also uses the frozen form (`structure_oop.py:117`).
- The long gates use `movinghome_vp`.
- The flat-start counterfactual uses the lasting core with measured slides.

**The gates (verified from the JSONs):**
- RDM′ scores 0.183 ± 0.074 against persistence 0.163 on submissions, and beats persistence in only 2 of 5 splits.
- Adding drift by rank to the wandering home raises error on both panels: 0.175 → 0.183 (submissions) and 0.100 → 0.130 (comments).
- RDM′ beats A in only 2 of 5 comments splits.

**Undisclosed in Methods:**
- The gate scores a top-200 cohort (`cohort_k=200`).
- An observation-noise scale is recalibrated per split and per arm on the training moments, from a grid of 1.0 down to 0.0 (`rankdiff_rdm.py:647, 698`). RDM′'s submissions scales are 1.0, 0.05, 0.05, 0.25, 0.0. This absorbs differences between arms and is one reason "the gate cannot see the lasting process."

**"Correcting that bookkeeping removed the failure" is incomplete.** A5b-2 failed: the 13-week median was within ±1 in 2 of 5 splits and overshoots by 0–3.

**The flat start.**
- A3-4 checks the Atlas stationary-gap theorem in simulation: Fig. 4B's x-axis is the simulation's own σ²/(2s̄).
- The empirical comparison is the open symbols. There the comments head is 0.54 against 0.99, and K/32 is 0.65 against 1.21, on the ladder where the paper says the balance holds.
- "Body within 20%" holds only for comments K/8…K/2. Submissions K/8 is 0.95 against 1.26.
- Figs. 4A–B use 10 seeds and show no bands.

**A6's resolving power is small.** Retention varies only 0.79–0.88 across cut-offs. A constant of 0.84, chosen with hindsight, would score MAE 0.019 on submissions (the RDM scores 0.040) and 0.030 on comments.

**(5) Persistence and the falling head.** The gapped statistic is sound and the best-defended new fact.

- **First attack: capture.**
  - Pushshift scores are snapshots taken at retrieval, and capture is unaudited (Discussion).
  - Submissions persistence is entirely a loading on the platform aggregate (0.112 → 0.006 at a 26-week gap). A change in capture delay or removal that varies by community would produce exactly that.
  - Say that counts carry the persistence and are less exposed to score capture.
- **"Among those that rose it is zero" is over-read.**
  - A Spearman correlation within one sign of change 1 is range-restricted.
  - D8's quintile row shows the top change-1 quintile at mean change-2 rank 0.547 (submissions) and 0.532 (comments), against 0.399 and 0.403 for the bottom.
  - Risers persist too, about half as strongly as fallers.
- **The abstract's platform contrast** conflicts with D10: Reddit at Facebook's settings gives −0.04 to +0.08.
- **"Persistent decline is what tips the balance" is not linked to the decline itself.** The within term is the members' decline by definition. No analysis shows that the communities whose declines persist are the ones carrying the head's fall.
- **The Gomez-formula miss is a domain problem, not shown to be passing swings.**
  - ζ < 1 in 12 of 18 cells, where ½(ζ−1)ν² is negative, outside the formula's domain.
  - Only the six cells with ζ > 1 (implied 0.023–0.042 against measured 0.052–0.078) can test "passing swings inflate displacement," and that test has not been done.

## 4. The model in the main text

The core is not stated correctly.

**The main-text equations are not the code's process.**
- The main text writes x = L + m + ε with independent parts, dL = g(r)dt + σ(L)dW, and says "the lasting level here has no home."
- In `rankdiff_rdm.py` (`movinghome_vp` branch of `_State.step`):
  - X = L + ξ + ε;
  - L follows an OU process toward a random-walk home H at A's κ(z), with A's σ_perm(z) knots;
  - H carries the size law and drift by rank;
  - the deviation L − H has innovation ε − η, so the "passing swing" is coupled to the home's shocks, and the observed level absorbs a home shock with lag 1/κ.
- The SI says that "the main text's lasting level is H." The main text should say it too, and should not say "no home" for a variant named `movinghome`.

**It is not minimal.**
- The model carries A's κ(z) and σ_perm(z) (about 110 knot values; DOSSIER §2), plus φ, σ_tr, σ_obs, t_df, temperament, mix_b and exit knots, plus σ_0, β, the hold closure and the per-split scale.
- "Three ingredients" hides this. The round-2 point V8 (more complex than A) is still unanswered in the text.

**"No parameter tuned to the structure" contradicts "solved so that the end-of-training ladder is held."** The hold closure is fitted to the structure; that is the point of it.

**Also:**
- σ_L is clipped to the estimated band range (`_sigma_L`); say so.
- The gates show that drift by rank, the ingredient that holds the hierarchy, worsens held-out error. The gates therefore do not validate the model's distinguishing parts.

**The variance-preserving correction is right.** I re-derived it, and it holds whether L reverts to H_t or to H_{t+1}: Var ΔL = [2q + κσ_L²]/(2 − κ), so q = σ_p² − κσ_L²/2. But it preserves only the one-step variance:
- Var(L − H) rises from A's σ_p²/(κ(2−κ)) to (σ_p² + (1−κ/2)σ_L²)/(κ(2−κ));
- every increment at h ≥ 2 is therefore larger than A's;
- that is consistent with the 13-week overshoot.

Round-2 V4's "σ_perm² − (1−κ²)σ_L²" is a different quantity (an innovation, not an increment variance). The SI should reconcile the two.

## 5. Round-2 points

**Resolved:**
- V1, the submissions β, disclosed;
- V2, arithmetic shares;
- V3, entrants immaterial (A6-5);
- V7, the head-share attribution withdrawn;
- V12, the gapped statistic;
- V10, V16 and V19, claims dropped;
- the momentum decomposition (A4), the long-panel 2×2 (A5c), the joint bootstrap (A9), Gomez's decomposition (A8), and the noise-corrected Facebook β.

**Half-resolved:**
- V4: fixed in code, but one-step only; A5b-2 not reported; the frozen form still used in A6, in the 30-week claim, and in protocol P7.
- V6: the 30-week win is still quoted, and it is the frozen form's.
- V11: the intro's "They do not show, for a real hierarchy … rank by rank" sits close to what Gomez's Appendix F does by percentile.
- V15: D10 was run, but the abstract still states the contrast.
- V17: the significance says "a fifth or more."
- V9: mentioned in the SI; the β = 0 model null was not run.

**Open:**
- V5, tails: comments' 90th percentile under-dispersed; A6 kurtosis 22.1 in the data against 5.1 in the RDM on submissions; absent from the paper.
- V8, the parameter count.
- V13, every pre-July-2021 triple contains 2020 H1; not addressed.
- Cross-model recovery.
- Fitted level rivals (two-type GLLM, Yule–Simon).
- Literature: Coad, Guvenen, Kairam and Cunha are still uncited.

## 6. Errors

**Equations**
1. Eq. 1 is printed without its v_k term; print v_k + (1 − v_k)E[·]. The derivation is otherwise correct: density (k/S)e^{−u/S}, independence of the change from the margin u.
2. Eqs. 2–3 are correct as the mean-log local-time term. The stationary form omits entry, which the quoted numbers include (§3).

**Numbers**

3. The significance "a fifth or more" disagrees with the intro's "one in six" and with 16%.
4. The abstract's "16–33% … each year" folds in Facebook's half-year 18–35%.
5. "Two years without churn, or without the slide, move the head by 0.03–0.26" uses per-year rates (RESULTS A3; caption 4C); two years is 0.05–0.52. §2z-dg repeats the slip, and Fig. 4C's title "Two years…" plots per-year values.
6. "Within 0.02–0.04 at cut-offs from K/64 to K" are MAEs. Single cut-offs miss by up to 0.082 (A, comments K/64) and 0.076 (RDM, submissions K/8).
7. The head share in 2022 is 34.0% in A8 and §2z-da but 0.356 in A6; in 2020 it is 0.498 against 0.508. Reconcile the denominators in the text.
8. "Displacement +0.05 to +0.08 at every cut-off" should be +0.037 to +0.078.
9. "The slope rises from about 0.8–1.0 at the head to 1.5–2.1" fails for Facebook (0.73 → 1.23) and submissions (maximum 1.46).

**Contradictions inside the paper**

10. Methods says "a joint block bootstrap over time is not built," while Results quotes A9's joint interval. The bridge protocol's "Intervals" row has the same stale line.
11. "Small composition term" in Results against "+25–35%" in Methods.
12. The 30-week result and A6 are attributed to "the model" but come from the frozen form.

**Registration**

13. The Discussion says "We have therefore registered the main claims," but v3.1 is "DRAFT, NOT ADOPTED." P7 names the frozen form, and on development data that form fails P7's own "below persistence" rule (0.246 against 0.163). So does RDM′ (0.183).

**Figures**

14. Fig. 1A is titled "The hierarchy changes slowly" while its own submissions head flattens.
15. Fig. 2A compares Facebook's 13-week block means with Reddit's 52-week means; the caption does not say so.
16. Fig. 5A's dashed line includes an OU swing, but the caption says "random walk with noise."
17. Fig. 5B sets the "activity" bar (D6, all origins, 0.175) against a residual taken from origins ≥ 52, whose raw value is 0.149.
18. Figs. 4A–B use 10 seeds and no bands.

**Citations and SI**

19. Eleven entries are in `references.bib` but uncited. The most important are:
    - Iñiguez et al. 2022: the top is more stable in many ranking systems, a direct contrast with "fastest at the top";
    - Bentley et al. 2007: a turnover-profile law for top lists under neutral copying, a rival for Eq. 1 and Fig. 1C;
    - Batty 2006, rank clocks: the same puzzle for cities;
    - Blumm et al. 2012, Ichiba et al. 2011, Wu & Huberman 2007;
    - Baker & Smyth 2026, the closest competitor, whose entry still lacks the authors' full names.
20. The SI still has TODOs for the gate table, A6, the registration list and the AI statement, although §2z-dg says only owner items remain.

## 7. The five most valuable changes before submission (data on hand), ranked

1. **Null worlds and an honest Section 3.**
   - Run the three null worlds of §1 (re-selected-K Gibrat; Atlas with β = 0.2; neutral copying) through Eq. 1, Eq. 2, the slide and Fig. 3B–C.
   - Replace Fig. 3B–C with the rank-differential budget, (c_k − c_K) against (s̄_k − s̄_K), with entry shown.
   - State the near-identity, and the 0.97 slide-only correlation, in the main text.
   - Lead with the non-accounting evidence: out of time, comments 0.94 and submissions 0.62; β; persistence.
2. **One model, everywhere.**
   - Rerun A6 and the 30-week gate with `movinghome_vp`, or drop those claims.
   - Put the per-split scale, the top-200 cohort, A5b-2 and the parameter count in Methods.
   - Fix the main-text equations (H; the coupled swing; no "no home").
   - Rewrite P7 for `movinghome_vp`.
   - Present the model as a structural counterfactual device that does not beat persistence on submissions.
3. **Reconcile "slowly changing" with concentration.**
   - Put the 18 arithmetic cells into Results 1.
   - Give "slow" a number: the shape change a year against the span, next to turnover a year.
   - Retitle (§2) and fix Fig. 1A.
4. **Report the boundary test by ladder and by rank**, with:
   - the head cut-offs included, or their exclusion justified;
   - R_bc against c, with C separated;
   - the entry-inclusive S*;
   - Facebook's post-count β.
5. **A numbers, captions and citations pass** (items 1–20 above). Also:
   - correct the risers/fallers wording with D8's quintile row;
   - test the Gomez miss only where ζ > 1.

**What needs the owner or new data**
- The title and claim language, and whether the model stays in the main text. The owner chose to keep it; on these gates I would reconsider.
- Adopting v3.1 after the corrections in item 13 above, and first contact with the sealed bridge.
- The capture-delay and removal audit on the WD raw archives. It decides whether the submissions persistence and the head's fall are real.
- Names, for interpreting the head's fall (the 2020 ban wave; WallStreetBets in 2021).
- Wikipedia (sealed), for a ladder of views rather than karma.
