_Round-2 review by a fresh Claude Opus 5.5 session (Claude Code subagent, no prior context), 2026-10-18, from `CRITIQUE_PROMPT_R2.md` in this folder. Read-only; saved verbatim by the orchestrating session. Model output: its claims are checked in `SYNTHESIS_R2.md` and MODEL_STATUS, not taken on trust._

# Round-2 review: rank-diffusion, frame "c"

I worked read-only. I opened no data files and nothing under the sealed paths. Numbers marked *recomputed* come from saved outputs under `llm_fitting/runs/`. One process note: `runs/2026-10-18_rdm/gate_runs_A.py` and `gate_A_output.txt` are uncommitted. The output file holds only a header, so G2 looks like it is still running. I did not interpret it.

## 1. Verdict now

1. **Closer than round 1.** Gomez is now cited as the framework. Concentration is reported in arithmetic shares. "Stable form" is retired. The June 2021 event study ran against placebo years, and its failure is reported. Model A's contradicted fixed home has been replaced by a model that does not revert.
2. **But the paper has traded two half-papers for four partial claims.** They are a size exponent, a Gomez balance, a generative model and a new fact, and none of them is yet both new and nailed down.
3. **The headline exponent survives on comments but not on submissions.** Comments: β 0.075 (0.061–0.093), and 0.074 without the top stratum. On submissions it rests on one stratum: drop the top 78, whose sd was 0.785 in the feed-change year, and the year-1 β rises from 0.045 to **0.19**, which is the firm value (*recomputed* from `size_variance_output.txt`).
4. **The model's "better than A" rests on a 30-week panel.** There, the fitted "lasting" parameters (σ0 0.18–0.22 a week, β 0.10–0.27) are not the lasting process the paper describes. They also sit in a regime the recovery study says is not identified.
5. **The single largest gap is momentum.** It is the most novel fact, the model's main failure, and the likely source of both the Reddit composition term and the too-sticky two-year retention. Nobody yet knows whether it is a stationary trait of names, a trend shared by topic groups, or a 2020 episode. A cheap decomposition with data on hand would decide which paper this is.

## 2. Round-1 points

**Addressed:**
- lock-in demoted, with the logistic shown;
- the index relabelled, and arithmetic shares added (50.2% → 34.0%);
- Gomez read in full and novelty claim (a) withdrawn, which is exemplary;
- Batty cited;
- the "Eulerian" title and "stationary" language retired;
- the Instagram bound withdrawn and the tallies reconciled;
- time misalignment disclosed in the record;
- the June 2021 placebo design run, with X1's failure reported;
- the size–variance exponent measured;
- calibrated rank rivals run;
- Fable's contradiction between A's home and B removed by the moving home.

**Half-addressed:**
- **Out of sample.** The within-development out-of-time test passes on comments (0.944) and fails on submissions (0.617). There is still no out-of-sample *structural* result. The RDM gate scores held-out movement over 1–13 weeks, and the out-of-period structure test is listed as not done (§2z-dd §8).
- **Claim language.** The abstract still says "Displacement at each rank follows a balance". Yet submissions runs 0.61–1.86 by cut-off, and the comments pooled ratio, 1.069 (1.011–1.127), excludes 1. Results 3 and Figure 3 are still titled "Movement in levels, not ranks", although fixed-ladder log-rank diffusion passes (1.05, §2z-dc V2).
- **Rivals.** These are three calibrated toys. There is no fitted Yule–Simon and no GLLM two-type model, and the two-type model is now the natural momentum rival.
- **The unification figure.** Three estimators are drawn; A's σ_perm(z) is left out. The RDM now supplies the model-side estimator, and it agrees well: submissions `s_band` 0.0075 → 0.0136 a week against 2SR̃ 0.0073 → 0.0133. Put it in. That figure is the paper's best evidence that model and balance describe one quantity.

**Ignored or not done, ordered by how much it still matters:**
1. **The Gomez decomposition of the arithmetic shares** (§2z-db §7). By your own theory note §9 item 3, at a Zipf-like head (ζ ≈ 1.1) arithmetic displacement is about 0.05σ². So the 50 → 34% fall is almost entirely a *within* term, and test A (§2z-dc) showed that term cannot be measured without bias. The spine therefore does not carry the headline consequence, concentration, in the way frame c implies. Results 6 needs to say this.
2. **A block-over-time, joint bootstrap and a common-factor adjustment.** These are now central to momentum.
3. **GLLM speed of transition.** Momentum is GLLM's "type dependence", which they show is needed for fast transitions.
4. **The capture-delay audit.** D1 covers only the July 2021 regime change.
5. **A data-availability and provenance package.**

## 3. The model

**Would a referee accept it?** As a candidate generative model, in the SI, consistent with the balance: yes. As a validated main-text result: not yet.

### Design

- **Hold drift as a fixed point.** This is a legitimate Atlas closure (Fernholz; Ichiba et al. 2011), but it makes the ladder an *input*: the drift is solved so that the end-of-train ladder stays stationary (`drift_hold_numeric`). Three consequences:
  - reproducing the ladder's shape is not evidence;
  - it contradicts the paper's own message that drift by rank moves and drives concentration;
  - the failure of S4 (head share 0.515 against 0.359) is built in. The measured-slide variant halves the gap (0.436).

  A theorist will ask for the solver to be checked against the closed-form stationary gap law of first-order rank-based models in the special case β = 0 with no amplitude spread. Two small asymmetries should also be stated. The drift is indexed by the rank of the latent home while exits follow the rank of the observed level. And the fixed point is solved on the lasting process alone.
- **Jumps rejected by the ladder's stability.** The check is fair: holding the ladder against kurtosis-implied jumps needs a head drift of −0.06 to −0.07 a week, against measured slides of 0.004–0.01. But it rejects *jumps identified from kurtosis*, not jumps. Submissions' kurtosis of 29 is what heterogeneous drift (momentum) and heterogeneous volatility also produce, which are exactly the features the model lacks. Stability is also imposed rather than observed. Identifying jumps from changes standardised within each entity is listed as not done; it is the real test.
- **Moving home against fixed home.** In this harness "model A" anchors its home at the *filtered end-of-train state*: `rk.sim_cohort_conditional`, `cond_home="state"`, described there as "locally persistence-anchored".
  - So A's "false reversion" is reversion to a noisy filtered state. The `trainmean` home was never compared.
  - The RDM is A plus a random-walking home plus drift by rank, and no ablation separates the two additions.
  - A random walk is not identified against slow reversion over four years: `KAPPA_GRID` starts at 0.01 a week, and see §2z-bm.
  - The only discriminating evidence is Facebook's no-return slope, which is in-sample on the development panel.
- **The σ_perm reduction is floored at 0.1·σ_perm².** In the submissions gate, σ0² is about 0.03–0.05 a week at rank K. Where σ_L² approaches A's σ_perm², the promise that short-lag increments are "unchanged" fails, and the model simply adds dispersion. Report how often the floor binds.

### Evidence

Gate v1, paired by split (*recomputed* from `gate_output.txt` and the V3 `gate.log` files):

| panel | RDM − A, mean (paired t) | RDM better than A | RDM beats persistence | A beats persistence |
|---|---|---|---|---|
| `reddit` (submissions, 30 weeks) | −0.032 (−4.5) | 5/5 | 5/5 | 5/5 |
| `reddit_comments` | +0.015 (0.4) | 2/5 | **3/5** | 4/5 |
| `facebook_a` (contaminated) | +0.020 (0.9) | 2/5 | 4/5 | 4/5 |

- **The "clean submissions gate" is a short panel.** `reddit` has T = 30 weeks, training windows of 12–17 weeks, and a structure function of 6–8 lags. Its fitted σ0 of 0.18–0.22 and β of 0.10–0.27 compare with 0.100 and 0.045 on the long panel.
  - Every gate training window is under the 160 weeks that recovery requires: 12–17, 34–102 and 21–65 weeks.
  - What the gate validates is therefore a short-run diffusion plus drift by rank, not the lasting process.
  - In the gate fits the model's own β reaches 0.41 on Facebook. Inside its validation, size *does* protect.
- **The long panel was not gated.** On `reddit_submissions_long`, A loses to persistence, 0.194 against 0.163 (GATE_LEDGER line 84). That is where the moving home should matter most, and it has no RDM gate.
- **The coll1 attribution is unsupported.** "The comments gap to A comes from coll1" cannot be shown while A's per-moment errors are unrecorded (G2 NOT ADJUDICATED). On split T0 = 51 the RDM scores 0.185 against A's 0.047.
- **Structure (in-sample).** Against A, the RDM is worse on:
  - submissions one-year retention: MAE 0.056 against 0.016 (*recomputed*);
  - the head share;
  - kurtosis.

  It is better on comments' two-year retention (0.075 against 0.112) and on Facebook's no-return slope.
- **Recovery.** Six replications, self-consistency only, σ0 and β only. R1 as registered also named π and κ_m; say they are inactive in the frozen form. There is no cross-model recovery.
- **The RDM's own boundary ratios.** S6 passes pooled (0.970), but by cut-off the ratios are structured: k = 1,250 gives 0.68–0.80 in all five seeds, and k = 2,500–5,000 gives 1.16–1.30 (`boundary_sim_output_submissions.txt`). So a world with known lasting variance departs from 1 by about ±25% in a pattern tied to rank. Two consequences:
  - the novelty claim "rank by rank" needs a null derived from a model;
  - it is a lead on submissions' unexplained shortfall at ranks 300–600, possibly the estimator interacting with the size law.

### What a referee attacks first, and the strongest fair criticism

- **First attack (the short seller):** "Your clean win is a 30-week panel. On the 212-week panel, where your lasting process is identified and A loses to persistence, you did not run the gate."
- **Strongest fair criticism:** the validation does not test the part of the model that is new. The new part, the lasting random walk with hold drift, is tested only in-sample. There it fails four of seven Reddit structure targets, and §2z-dd's own verdict is "not recommended as the structure model". Yet it is Results 4 and half of the significance statement.

## 4. Momentum

**Is it real as presented?** Yes, as a 2019–2022 fact:
- it survives outliers (Theil–Sen +0.365) and survivorship (keeping absences at the floor gives +0.371 and +0.264);
- it appears in every size stratum;
- its sign is against the bias: noise and passing swings in block B1 push the correlation of consecutive changes negative.

**Three qualifications:**
- **Timing.** All three triples that end before July 2021 contain 2020 H1, the COVID onset. Momentum peaks in the triples spanning 2020 (+0.34, +0.32, +0.31) and falls to +0.11 (submissions) and +0.16 (comments) by 2021 H2 to 2022 H2. Annually, 2019 → 2021 gives 0.32–0.37 and 2020 → 2022 gives 0.43–0.45. D1 rules out the July 2021 capture change, not a COVID-era episode.
- **D2 measures the wrong thing.** It uses first appearance in the tracked universe, not community age. "Not a young-community lifecycle" is overstated.
- **The Facebook contrast is not like for like.** Facebook is measured with 13-week blocks over December 2020 to June 2022. The Reddit 26-week triple over the same dates gives +0.25 and +0.19. The contrast probably survives, but compute Reddit with 13-week blocks over the same dates.

**Is it new?** As a measured population regularity of online communities, probably. As a class of phenomenon, no:
- heterogeneous income profiles against restricted ones (Guvenen 2009);
- GLLM (2016) type dependence, and Luttmer's high-growth types;
- serial correlation of firm growth (Coad 2007);
- predicting online-group growth (Kairam, Wang & Leskovec 2012; Cunha et al. 2019).

The GLLM link is the valuable one: it would tie momentum to the *speed* of the concentration fall.

**What I think produces it.** Shared, non-stationary trends: heterogeneous loading on the persistent 2020–21 reallocation of attention, across topic groups and platform growth. I do not think it is a stationary velocity of individual names. The evidence:
- the timing profile;
- its breadth across sizes;
- its absence on Facebook in a window after the surge;
- the composition term's sign: +25–35% on Reddit and −30% on Facebook (§2z-da). That is the momentum signature inside the balance statistic. The RDM, with no momentum, still gives 0.97, so the boundary test is blind to the feature the paper calls its main gap.

The failure of the velocity extension is partly a matter of specification. It added velocity variance on top of a fixed σ0, so momentum could only grow by wrecking turnover. A split that holds D(52), D(104) and D(156) fixed would not have that problem. "No stationary model can produce it" is premature.

**A decisive test with data on hand, no names needed:**
1. Using 26-week blocks (eight of them), estimate each community's loading on the platform aggregate and on k co-movement clusters, from weekly changes in the first year only.
2. Compute momentum by triple on three versions of the changes: raw; with the aggregate removed; and demeaned within cluster. Add the lag-2 and lag-3 autocorrelations.
   - Loading on a common trend: the residual momentum is about 0, and momentum by triple tracks the product of consecutive aggregate growth rates.
   - Topic trends: momentum within clusters is about 0.
   - Persistent trends of individual names: momentum survives both adjustments, the lag profile is flat, and it is stable across triples.
   - A 2020 episode: lag 2 is about 0, and momentum is confined to particular triples.
3. Extend the structure function to 156 weeks. Persistent name trends predict D(104)/D(52) ≈ 2(1 + ρ) ≈ 2.7.

**Lead, report or wait?** Report it now as Results 5, the main open residual, and do not lead with it. If it turns out to be stationary persistence of individual names, it becomes the mechanism and possibly the title. Either way, register it for the bridge before contact: stationary persistence predicts half-year momentum of at least 0.15 in 2023–24, while an episode predicts about 0. That would be a genuinely discriminating primary test.

## 5. The headline

"Size does not protect" is the right *kind* of lead: a measured contrast with a famous regularity. As evidenced today it is not safe:
- **Range.** Two to three decades, within the top K only.
- **Submissions.** β is one stratum: 0.045 becomes 0.19 without the top 78.
- **Facebook.** The sd of one-year change is 0.84–1.01 in every stratum. A flat line at that noise level is what an instrument dominated by passing spikes would give, and the lasting β was not measured there by this route.
- **Units.** This objection is answered better than the draft says. Item *counts* also fail to steady (γ for items 0.065–0.078), and counts are not weighted by approval. Lead the units defence with that.
- **"Leaders."** The top set means 78 or 195 communities. Show retention of the top 10 and top 25 as well; this needs only ranks.

**Precedence.** The threat is not Gomez: his Prop. 2 allows volatility to depend on scale. It is Gibrat, for whom β = 0 is the assumption, and the fluctuation-scaling literature. Menezes & Barabási (2004, *PRL*) report α ≈ 1, external driving, for web traffic; see also Eisler, Bartos & Kertész (2008). "Attention moves as a unit" is their α = 1, and both must be cited.

**A better lead**, if step 4 below holds: the chain that is most yours. Attention does not diversify, so turnover is scale-free, and the displacement balance holds rank by rank despite heavy transitory noise. For example: "Attention does not diversify: online leaders are displaced at the same rate at every scale." If the momentum decomposition finds stationary persistence of individual names, the better title is: "Persistent growth, not size, decides who leads online attention."

## 6. The five most valuable next steps with data on hand

1. **The momentum decomposition** of §4, plus Reddit at Facebook's horizon and dates. Days. It decides Results 5 and possibly the lead.
2. **Gate the frozen RDM on `reddit_submissions_long` and `reddit_comments_ext`.**
   - Run A in the same harness at 20 repetitions (G2) and report paired splits.
   - Add two ablations: the random-walk home without drift by rank, and A's fixed home plus drift by rank.
   - Add cross-model recovery: simulate A with slow κ and fit the RDM, then the reverse.
   - Log every run in GATE_LEDGER.
3. **An out-of-period structure test.** Estimate the RDM, A, and the RDM with a properly specified trend on 2019–20. Forecast 2021–22:
   - retention by scale and two-year retention;
   - the no-return slope;
   - the head share.

   Score against persistence of retention and the logistic. This is the only route to an out-of-sample structural result without the sealed data.
4. **Robustness of the headline.** All cheap:
   - β leaving out one stratum at a time, in both years;
   - strata extended to the full tracked universe (ranks to 4K), with absences treated as collapses;
   - the lasting β on Facebook;
   - β from item counts only;
   - retention of the top 10 and 25.
5. **Tie the spine to the headline and calibrate the test.**
   - Decompose the 50 → 34% fall into Gomez's within, displacement and demography terms, using permanent-rank cohorts.
   - Draw the unified variance-by-rank figure with the RDM's `s_band` included.
   - Run the boundary estimator on RDM simulations with β = 0, to see whether the structured pattern by cut-off, and perhaps the 300–600 shortfall, comes from the estimator interacting with the size law.

**Needs the owner, sealed data or new data:**
- **Owner:**
  - whether the RDM goes forward and is re-frozen;
  - the title and claim language;
  - the binding paragraph;
  - names, to interpret momentum clusters and the 2020 ban wave.
- **WD drive** (not sealed, but a long run): the capture-delay audit; tail indices of per-item scores (the portfolio alternative); subreddit creation dates (the real lifecycle test).
- **Sealed bridge:** add momentum, β and the RDM gate to protocol v3 before any contact.
- **Wikipedia (sealed):** views rather than karma, over a long span, to test β and momentum in a second unit.

## 7. Errors in the new material

1. **The head share is blamed on momentum against the model's own runs.** The §2z-dd headline and frame c's "Does not claim" both attribute the too-concentrated head to momentum. But adding velocity *raises* the head share: 0.515 → 0.572 on submissions, and 0.608 → 0.730 on comments when transported. Measured slides lower it (0.436). The cause is the hold drift's stationarity, by construction.
2. **The coll1 attribution is unsupported** (§2z-dd §4): A's per-moment errors are not recorded.
3. **The abstract and significance omit the panel length.** "Better than the previous model on Reddit submissions" refers to the 30-week `reddit` panel. Figure 4 says "(short)"; the abstract does not. The long panel was not gated.
4. **The gate runs in a regime the recovery study does not cover.** Recovery at 160 weeks or more is presented as validating the model, but every gate training window is shorter. The gate's fitted β (0.06–0.41) contradicts the headline.
5. **The submissions β is fragile:** 0.045 → 0.19 without the top stratum (*recomputed*). The abstract's "−0.01 to −0.08" needs a caveat.
6. **The momentum range is incomplete.** The abstract's 0.32–0.37 is one triple; 2020 → 2022 gives 0.43–0.45. The Facebook "none" is at 13- and 26-week horizons over a different window.
7. **Two diagnostics are overstated.** D2 measures first appearance in the panel, not age. D1's pre-July-2021 triples all include 2020 H1.
8. **A section title contradicts the record.** "Movement in levels, not ranks" (frame Results 3, Figure 3) contradicts V2, where fixed-ladder rank diffusion passes (1.05).
9. **`pnas_outline_v2.tex` is internally inconsistent:**
   - the findings preview in intro paragraph 4 omits Results 4–5 and says "no forecasting claim", while the abstract claims a forecasting advantage;
   - the figure placeholder lists the old figures, not `fig_contestable`/`fig_rdm`;
   - M&M 5 keeps A as the movement measurement while Results 4 replaces it;
   - β is given as "0–0.09 (§2z-da)" in the intro against "0.01–0.08" in the abstract, from different estimators;
   - Results 5 is still `\todo`.
10. **"Reproduces Facebook's absence of reversion"** is in-sample on the development panel, and the data's −0.076 is itself a mild reversion.
11. **Turnover figures differ by source.** The abstract's 16–21% (§2z-cu) differs from the structure suite's data for submissions (top 5,000 retention 0.868, so 13% replaced). They come from different windows; label both.
