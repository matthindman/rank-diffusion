# External review — GPT 5.5 Pro REVISED VERDICT (2026-07-05, post P0–P6)

_Verbatim revised verdict issued after reading the hardening-session record
(MODEL_STATUS §2r–§2w, commits b9317a3..1cc6617). Original review:
gpt55_pro_review_2026-07-05.md. Adjudication + revised agenda: MODEL_STATUS
§2x._

---

Reasoning from the new record: the biggest change is that several issues I treated as unresolved methodological threats have now been converted into either closed defects, documented scope conditions, or explicitly bounded residuals. The model is stronger after this run, but the paper's safe claim set is also narrower and cleaner.

# Revised bottom line

This post-review hardening materially improves the project. I would now revise my assessment from **"technically promising but still vulnerable to measurement-identification and forking-path objections"** to:

> **A credible, deeply validated model-discovery paper, with the main remaining PNAS risks concentrated in confirmation and breadth rather than internal statistical mechanics.**

The core law is now best stated as:

> The head of digital-attention rankings is well described by a shared rank-conditional stochastic process, independently bounded measurement noise, stationary common-level movement, rebirth at the boundary, and one dominant endpoint amplitude. The (b=1) factorization is not rejected under time-window uncertainty, with conservative deviations bounded roughly within (b\in[0.94,1.11]). Remaining deviations are concentrated in low-frequency structure, mild entity-level reversion heterogeneity, and a small comments residual.

That is substantially stronger and cleaner than the pre-P0 state.

---

# 1. What I would revise from my prior report

## R1. Spec-B projection: from "major algebraic vulnerability" to "resolved, with bounded-language caveat"

My prior strongest static objection was the (CJC=0) null direction in the Toeplitz daily-noise floor. The new centered-floor adoption directly addresses that objection. The important point is not only that the centered floor is algebraically invariant, but that the legacy convention produced an empirical contradiction on FB: Spec-A head σ_obs below the alleged floor. The centered convention restoring Spec-A ≥ floor everywhere makes it the right default.

Revised assessment:

**Resolved as a fatal/major threat.** I would no longer treat Spec-B as a methodological weak point, assuming the implementation is correct. The appropriate paper language is exactly the new language: **shape identified everywhere; FB head identified by two-instrument agreement; subs/comments head bounded between centered floor and Spec-A; below the head bounded.**

One caveat remains: "identified everywhere" still exceeds the evidence. The correct term is **"identified in shape and bounded in level outside the head."** This is consistent with the code's use of daily floors as an external σ_obs instrument and with the model's broader Spec-B design.

## R2. Stack-freeze issue: from "unresolved overclaim" to "documented scope condition"

The new FB full-stack OOS result is exactly the decisive check I asked for, and it came out in the most useful possible way: the full in-sample stack loses OOS. That sounds bad only if the paper insists on one stack doing all things. It is good science if framed as a scope condition.

Revised assessment:

**No longer a hidden validity threat; now a paper-framing constraint.** The paper must separate:

1. **Structure stack:** best in-sample moment reproduction.
2. **Movement stack:** best frozen OOS displacement prediction.

This distinction should be explicit in the main text, not buried in SI. The model is not weakened by having a structure stack and a movement stack, provided the paper states that long-horizon variance moments are excellent for identifying slow structure but fragile in short rolling training windows.

However, this does retire any sentence implying that the FB 15/15 full stack itself beats persistence OOS. The new primary FB movement claim is stronger in identification terms but slightly weaker in split count: **identified Spec-B + conditional state beats persistence 4/5, ties calibrated Spec-A in mean error, and uses no σ_obs calibration freedom.**

## R3. Scorecard: from "descriptive engineering card" to "descriptive card plus formal rejection"

The new (Q) values are extremely important:

* FB: (Q=298)
* submissions: (Q=217)
* comments: (Q=1275)
* χ² df=15 reference roughly 15–24

This confirms my prior warning more sharply than I could have from static review alone. The 15/15 threshold card is useful as a practical engineering diagnostic, but the covariance-aware (Q) says none of these are exact statistical models in the classical omnibus-test sense.

Revised assessment:

**The 15/15 card survives as a descriptive collapse, not as inferential adequacy.** That is acceptable for this kind of generative social-system model, but the paper should not invite a referee to interpret "15/15" as a formal goodness-of-fit test.

Best language:

> "The threshold card is a diagnostic scorecard; formal covariance-weighted distances reject exact equality, as expected with very large panels. We therefore use (Q) to localize residual structure rather than to claim the model is literally true."

This matters because FB's 15/15 surviving 20 reps is valuable, but (Q=298) prevents "statistically indistinguishable from data" language.

## R4. (b=1): from "statistically rejected but practically close" to "not rejected under the right uncertainty scale"

This is the biggest positive revision. My earlier view was driven by the entity-bootstrap intervals in §2n. The new block-bootstrap and split-window results change the inferential target. With overlapping windows, lifecycle-scale drift, and common temporal shocks, the entity bootstrap was too narrow for the law-level question.

Revised assessment:

**The (b=1) factorization is now defensible as the paper's central law.** It is not proven exact, but exact (b=1) is no longer rejected under time-window uncertainty. The new conservative statement is strong:

> (b\in[0.94,1.11]), with all central estimates in ([0.98,1.08]), comments moving toward 1 after detrending, comments (h=4) at 1.002, and second-half comments at 0.994.

That is exactly the pattern one would want if secular lifecycle drift inflated the long-horizon estimate. I would now recommend putting (b=1) in the main conceptual model, with measured (b) as a robustness/refinement table.

## R5. "Lifecycle arcs": my criticism was correct, and the new surrogate result should change the paper language

The phase-randomized surrogate result reproducing the h=52 demeaning loss is decisive against the strong lifecycle-arcs interpretation. The paper should not claim unique evidence of deterministic rise–fall arcs.

Revised assessment:

**Replace "directional lifecycle arcs" with "excess low-frequency structure."**
You can still discuss lifecycle arcs as one interpretation, but the identified object is spectral/low-frequency structure, not deterministic phase geometry.

The unpredicted surrogate failure is also useful: if the Gaussian spectrum-preserving surrogate gives VR13 = 0.176 versus data 0.136, then part of the residual is not missing linear dynamics. It is a functional/marginal/non-Gaussian issue. That reduces the legitimate target from the old comments residual to roughly +0.04.

## R6. Population-matched scoring: from "high-priority next step" to "dead end"

I previously recommended population-matched scoring as a cheap likely fix. P6 cleanly falsifies that. The comments census population is 99.0% present and censoring moves nothing.

Revised assessment:

**Remove population-matched scoring from the priority list.** It is no longer a plausible explanation of the comments residual. The residual now lives in low-frequency/non-Gaussian functionals and possibly bounded (\kappa_i) heterogeneity, not missingness.

## R7. Per-entity (\kappa_i): upgraded from hypothetical alternative to measured second-order structure

This is the main newly strengthened caveat. The (\kappa_i) probe found conditioned split-half 0.346 and true log-SD 0.302. That means reversion-rate heterogeneity is real, not merely a referee-invented alternative.

Revised assessment:

**This does not require immediate implementation, but it must be acknowledged.** It weakens the strongest literal form of "same rank-conditional process up to one amplitude." A more accurate statement is:

> "A single endpoint amplitude captures the dominant heterogeneity; residual entity-level reversion heterogeneity is measurable but bounded and does not yet justify an additional model layer under the OOS/parsimony criterion."

This is an important distinction. The law is now **dominant one-amplitude factorization**, not **all heterogeneity is only amplitude**.

---

# 2. Revised validity-threat ranking

## A1. Confirmation still unrun — SEVERITY: major

This remains the largest threat. The confirmation protocol being registered before reading the extension is a major improvement, but it is not evidence yet. The WD drive not being mounted is now the main gate between "excellent model-development record" and "clean confirmation."

My revised view: **do not submit to PNAS before the registered comments extension is run**, unless the paper is explicitly framed as model discovery and the confirmation protocol as future work. For PNAS, future-work confirmation is not enough.

## A2. Breadth remains the venue-level weakness — SEVERITY: major

The internal hardening shifts the bottleneck even more clearly toward breadth. You now have a stronger story on measurement, uncertainty, (b=1), and scope conditions. That makes the (n=2) universality objection more visible, not less.

Minimum to defuse: **two additional ranked systems through the unchanged pipeline**, with Wikipedia pageviews as first choice. Four systems would be much stronger. The research notes already identify breadth as the main PNAS/NatComms gap relative to the ranking-dynamics literature.

## A3. (Q) rejects exact fit — SEVERITY: major for wording, minor for model utility

The huge (Q) values mean the paper cannot say "the model passes formal goodness-of-fit." It can say:

> "The model gives a compact, identified approximation that reproduces the main rank, churn, and movement functionals; formal omnibus distances identify remaining localized residuals."

This is not fatal. In large social panels, exact-fit rejection is expected. But a hostile econometrician will use (Q=298) and (Q=1275) against any overclaim.

## A4. Structure stack vs movement stack — SEVERITY: major for presentation, minor for science

The stack matrix solves the hidden-forking-path problem only if the paper makes the distinction central. Do not present all numbers in one table without a "target" column. I would use:

| Target                  | FB primary                 | Comments primary    | Subs primary         |
| ----------------------- | -------------------------- | ------------------- | -------------------- |
| Structural reproduction | full/long stack            | long/mix stack      | locked md/spec stack |
| OOS movement            | Spec-B + conditional state | md6+mix+conditional | conditional Spec-A   |

This is defensible because long-horizon moments are a structural identification device, while OOS rolling windows punish high-dimensional or long-horizon moment fits on short train windows.

## A5. (\kappa_i) heterogeneity — SEVERITY: moderate

This is now the main model-theoretic caveat. It is not yet a reason to add complexity, but it is evidence against the most literal law. I would not implement (\kappa_i) before confirmation/breadth unless the comments extension reproduces the residual and the (\kappa_i) effect predicts it.

## A6. Comments residual — SEVERITY: moderate

The comments residual is now much less threatening. The measured VR block remains statistically huge in SD units, but the interpretable dynamic residual appears closer to +0.04 after surrogate adjustment. That is below the level that should drive a new lifecycle-state model before confirmation and breadth.

---

# 3. Revised mathematical assessment

## B1. Spec-B projection is now mathematically disciplined

The centered floor is the invariant quantity. Adopting it is not a cosmetic fix; it removes a nonidentified common-within-week direction. The fact that the legacy floor violated Spec-A on FB is strong empirical evidence that the old convention was not merely conservative but incoherent.

One implementation detail other auditors should check: the documentation in `spec_b_sigma_obs.py` still described the legacy map as (p^\top\Sigma p) in the uploaded version. The new implementation should update that prose so the method text, default output, and tests all agree.

## B2. (b=1) now belongs in the model, not only in the discussion

Mathematically, (b=1) gives the clean factorization:

[
X_{it}=\mu_{it}+\sqrt{v_i},\eta_t(z_i)+\lambda(z_i)L_t+\epsilon_{it}
]

with one amplitude scaling permanent and transitory variance. The old entity-bootstrap rejection was too narrow for a process-level law because it ignored time-window uncertainty and drift contamination. The new block-bootstrap, detrending, common-horizon, and split-window checks are the right adjudication.

Recommended paper treatment:

* Main model: (b=1).
* Empirical refinement: measured (b) estimates and conservative bounds.
* SI: entity bootstrap versus block bootstrap, explaining why the latter is the relevant uncertainty scale for the law-level claim.

## B3. Long-horizon demeaning identifies low-frequency structure, not lifecycle arcs

The new surrogate result confirms the spectral interpretation. Formally, the demeaning functional removes finite-window low-frequency power regardless of whether that power comes from deterministic lifecycle arcs, a mixture of slow OU components, fractional dependence, or phase-structured careers. The revised phrase "excess low-frequency structure" is exactly right.

## B4. (\kappa_i) heterogeneity is now a real alternative, but not yet a better model

A log-SD around 0.30 in reversion heterogeneity is not trivial. But the right adoption criterion is not "is it real?" It is "does it improve frozen OOS movement or materially close the confirmed residual?" Until that is shown, parking it is methodologically correct.

A minimal future experiment would be: train-only EB (\hat\kappa_i), shrink hard, add no new free score-tuned parameters, evaluate whether comments extension OOS and VR residual improve. If it does not move OOS, keep it as a limitation.

## B5. (Q) should be used as a localization device

The new (Q) diagnostic is useful, but a naive χ² interpretation will make every large-panel model look "rejected." I would report (Q), but decompose it by block:

* VR block
* ACF/RACF block
* persistence block
* churn block
* boundary block

This turns "the model fails χ²" into "the only practically meaningful residual is localized here."

---

# 4. Revised paper strategy

## 4.1 Main claim

Use this:

> We identify a parsimonious factorized model for the head of digital attention rankings: a shared rank-conditional stochastic process, independently bounded measurement noise, stationary platform-level movement, rebirth at the boundary, and one dominant endpoint amplitude. The (b=1) amplitude factorization is not rejected under time-window uncertainty; remaining deviations are localized to low-frequency structure and bounded entity-level reversion heterogeneity.

Avoid this:

> "Every endpoint follows the same process up to one amplitude."

That is now slightly too strong because (\kappa_i) heterogeneity is measured.

Better:

> "To first order, endpoint heterogeneity collapses to one amplitude; residual reversion-rate heterogeneity is measurable but second-order under the current validation gates."

## 4.2 FB movement claim

Do not lead with the calibrated Spec-A 5/5 number anymore if the new paper-primary movement spec is identified Spec-B + conditional state. The cleanest sentence is:

> "On FB Era A, the identified Spec-B conditional movement specification matches the calibrated Spec-A benchmark in mean error and beats persistence on 4/5 rolling splits without σ_obs calibration freedom."

Then, in parentheses or SI:

> "The calibrated Spec-A movement stack beats persistence on 5/5 splits but uses train-calibrated σ_obs scaling."

This ordering preserves the identification discipline.

## 4.3 Scorecard claim

Use:

> "FB Era A achieves a 15/15 descriptive scorecard under the structural stack, and this survives 20 simulation reps; formal covariance-weighted distances reject exact equality but localize the residuals."

Avoid:

> "The model passes all tests."

## 4.4 Comments claim

Use:

> "Reddit comments, the longest and hardest panel, is at par with persistence in the conditional OOS gate with full bootstrap median coverage; its remaining in-sample residual is a localized mid-horizon VR block, reduced to a small practical target after spectrum-preserving surrogate adjustment."

Avoid:

> "Comments passes."

## 4.5 Lifecycle language

Replace all "directional lifecycle arcs" lead language with:

> "excess low-frequency structure"

Then add:

> "Lifecycle arcs are one substantively plausible source, but spectrum-preserving surrogates show that the demeaning signature is not unique to deterministic lifecycle trajectories."

---

# 5. Revised recommendations

## 1. Run the registered confirmation extension before any new model work

This is now the highest-value task by a large margin. The confirmation protocol has already converted the forking-path problem into a testable design. The project should not spend another week on model refinements before mounting the WD drive and running the frozen extension.

Expected payoff: decisive.

## 2. Add two external systems through the unchanged pipeline

The model is now internally strong enough that breadth is the main PNAS bottleneck. Wikipedia pageviews should be first because it gives a different institutional measurement regime and likely clean daily data. GitHub stars, app charts, YouTube, podcast rankings, or news-site rankings would each add different credibility.

Expected payoff: decisive for "law" language.

## 3. Make (b=1) the main model and measured (b) the refinement

This is a parsimony win. It also makes the paper legible. The factorized law is the strongest conceptual result; do not hide it behind an optional (b) estimate unless the confirmation panel rejects it.

Expected payoff: high.

## 4. Keep (\kappa_i) parked unless it predicts the confirmation residual

Do not add per-entity (\kappa_i) now. It is real, but adding it before confirmation would reopen the forking-path critique. Pre-register it as a secondary diagnostic on the extension: if the residual concentrates among high/low (\hat\kappa_i) entities and improves OOS under train-only shrinkage, it becomes the next model layer.

Expected payoff: medium, but only after confirmation.

## 5. Use (Q) as a residual map, not a pass/fail gate

Report the Q diagnostics, but do not let them define the narrative. With panels this large, exact equality is not the right scientific standard. The threshold card, OOS gate, and residual-localization story together are more persuasive.

Expected payoff: high for econometric credibility.

## 6. Drop population-matched scoring from the priority list

P6 killed it cleanly. Mention the negative result in SI because it shows discipline, but do not spend more complexity there.

Expected payoff: low.

---

# 6. Revised mock-referee risk after P0–P6

A tough referee can no longer fairly say:

* "Your σ_obs floor is algebraically unidentified."
* "You never ran the full stack through OOS."
* "Your head-collision result is just MC noise and you ignored it."
* "Your (b=1) claim is cleanly rejected."
* "Your lifecycle interpretation is overclaimed and untested."
* "Your comments residual is probably just missingness."

Those have been adjudicated.

A tough referee can still say:

* "This is model discovery unless the frozen extension passes."
* "Two platforms / three metrics is not enough for universality."
* "The exact-fit (Q) statistics are huge."
* "You use different stacks for structural reproduction and OOS movement."
* "A measured (\kappa_i) axis means the one-amplitude law is not literally complete."

That is a much better fight. The first two are now the only truly large ones.

---

# 7. Revised publication verdict

For **PNAS**: credible if the registered comments extension is run and at least two additional systems show the same amplitude collapse / OOS-at-par pattern. Without those, I would expect the paper to be criticized as too narrow despite exceptional internal validation.

For **Nature Communications**: the current hardened package may already be close, especially if framed as an identification-and-validation advance over broader but shallower ranking-dynamics work. The research notes correctly position the project as stronger on measurement identification and OOS validation, weaker on breadth.

For a computational social science flagship journal: strong now, pending only the confirmation extension.

---

# 8. The revised one-sentence spine

> Digital attention rankings obey an approximately factorized rank-diffusion law: a shared rank-conditional stochastic process with independently bounded measurement noise and boundary rebirth, multiplied to first order by one persistent endpoint amplitude, predicts held-out rank movement and reproduces the main churn structure across Facebook tracked activity and Reddit census attention, with residual deviations localized to low-frequency structure and bounded reversion heterogeneity.
