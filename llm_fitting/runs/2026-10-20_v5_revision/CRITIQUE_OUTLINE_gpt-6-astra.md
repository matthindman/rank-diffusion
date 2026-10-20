<!-- Provenance: critique of OUTLINE_v5.md (draft 1), GPT-6 Astra via Codex CLI 0.163.0, read-only, effort high, 2026-10-20; brief BRIEF_OUTLINE_REVIEW.md. Saved verbatim. -->

The outline has the right overall argument, but its strongest contribution still arrives too late within Results 2, and several headings promise more than the planned comparisons establish. I would retain the five Results subsections, shorten the technical setup, and make the distinction between a durable benchmark and its empirical departures explicit throughout.

I read the brief, all five named documents, and the relevant repository instructions and record entries. This is a structural critique; I did not reproduce results, inspect raw or sealed data, or write files.

1. **Make durability the opening conclusion of Results 2, and distinguish the time scales.**

   Durability deserves prominent treatment as a substantive implication of an established model. Its value does not depend on being a new theorem. But placing it in paragraph 4, after accounting, estimation, and balance ratios, makes readers work through the machinery before learning why it matters.

   Open with the intuition: communities can move substantially while the distribution changes slowly, because redistribution and relative decline largely offset each other. Then give the measured ingredients, the comparison, and its limits.

   Three distinctions need explicit space:

   - \(S^2/\sigma^2\), the time for lasting movement to span one slope unit, is a **movement scale**, not automatically a membership lifetime or retention time. The factor of eight follows from the two benchmark formulas; it is not an independently measured ratio of hierarchy durability to member survival.
   - The reflecting-barrier relaxation calculation needs an applicability statement before being transferred to a finite, rank-dependent system with entry and exit.
   - The comments’ linearized relaxation estimate and the theoretical relaxation estimate are different calculations. Calling them “agreement” requires a common estimand and uncertainty, beyond both being slow. See MODEL_STATUS §2z-dl.

   Give readers a five-year conditional trajectory or change distribution as the practical implication. Label it a scenario under maintained parameters. “At most X” requires a specified bound; an ensemble interval is not a maximum.

2. **Keep Results 4 after Results 3, but preview the departure earlier.**

   The strongest sequence is: observed puzzle → durable benchmark → calibrated model and its limits → unexpectedly fast change → competing explanations for persistence.

   Moving the full fast-fall comparison before the model would require introducing its calibration and forecast there, then explaining them again in Results 3. Keep Results 4 as a short, approximately 300-word bridge. Mention the submissions exception at the end of Results 1 and Results 2 so the reader knows the benchmark has something consequential to explain.

   Results 1 currently answers three questions: coexistence, individual advantage, and turnover geometry. Keep its governing question descriptive. Retain one compact geometry comparison and move the turnover equation to the SI. Separate data definitions from the observed puzzle.

   Shorten Introduction paragraph 2: it still combines several stabilizing mechanisms, a theoretical identity, fast transitions, and the precursor. Move equation-specific attribution beside Results 2. The precursor belongs in the introduction in both submission and final versions; only personal attribution should change.

3. **Make the fast-transition and heterogeneous-growth conclusions conditional on their actual tests.**

   “Fell faster than homogeneous random growth allows” sounds like exclusion of a model class. The proposed evidence establishes a discrepancy from particular calibrated benchmarks. A safer heading is **“The submissions head fell faster than the calibrated homogeneous models predicted.”** A model mean alone cannot establish that discrepancy; show the relevant predictive distribution.

   Likewise, do not organize Results 5 around an assured explanation of the fast fall. R12 is recorded as not run in §2z-dl. Its structural role should remain valid if heterogeneous growth reproduces persistence but misses the head’s path.

   Give Results 5 separate paragraphs for the statistic, calibrated alternatives, their discriminating results, and the prior-decliner analysis. End with what remains unidentified. R10’s failures belong beside its interpretation, not solely in an SI failures table.

   The proposed Discussion opening—concentration moved fast “through persistent decline”—reintroduces the causal bridge the synthesis explicitly rejects. The evidence connecting prior decline to excess slide does not yet establish that it explains the particular head-share fall.

4. **Organize figures around inferential tasks, rather than enforcing one figure per subsection.**

   Five subsections and four figures cannot literally satisfy the stated rule. Sharing Figure 4 is reasonable if its two arguments are visibly separated.

   | Figure | Recommended change |
   |---|---|
   | **1: What persists?** | Keep annual curves and turnover. Merge the annotated starting ladder with the curves where legible. Give population-level one- and two-year retention a visible place if individual lock-in remains central; named leaders should illustrate that comparison. |
   | **2: Why is the benchmark durable?** | Keep crossing validation and the corrected balance. Distinguish gross crossing gain from its contribution relative to the universe. Label theoretical movement and relaxation scales separately; do not place an arithmetic-share change on their axis as though it were the same quantity. |
   | **3: What do measured ingredients generate?** | Show equality and observed-ladder starts over five years, with uncertainty and removal scenarios. “First 5–10 years” conflicts with the stated horizon. Put the asymptotic slope comparison in the SI, retaining its failure clearly in the text. |
   | **4: What does the benchmark miss?** | A: observed head share against calibrated predictions. B: posting versus engagement accounting. C: persistence across gaps against heterogeneous alternatives. D: prior-change classes, only if that comparison adds information beyond C. |

   Figure 4’s subsection references currently promise panels C–F, but the figure lists only A–D.

   Use consistent comments/submissions colors and calibration/prediction markings throughout. Four figures with fewer legible panels would improve the argument more than four compressed dashboards.

5. **Make the uses paragraph operational and bounded.**

   The five-paragraph Discussion is a sound design. Its uses paragraph should name concrete scholarly decisions:

   - Compare apparent entrenchment across communities while accounting for rank spacing, shares, and retention.
   - Assess whether observed concentration change exceeds a calibrated stochastic benchmark.
   - Explore how specified changes in growth, variance, entry, or exit would alter concentration over five years.

   Immediately distinguish those uses from identifying algorithmic effects, inferring welfare or platform power, and predicting which named community will succeed. The pooled movement gate does not establish endpoint forecasting ability.

   Removal experiments establish consequences inside the specified model; they do not identify effects of feasible platform policies. Explicitly distinguish such scenarios from refitted simpler alternatives.

   Keep the rival-readings paragraph, but make it a synthesis of unresolved alternatives rather than a second Results summary. End on the scientific question a future period can distinguish. Protocol logistics belong in Methods.

6. **Reserve main-text space for the referee’s most consequential questions.**

   Three gaps should be addressed before polishing prose.

   **What is being compared?** Mean-log shape, local slope, arithmetic head share, and membership retention are related but distinct. Define them where introduced. The observed and predicted head-share paths must use identical populations and denominators; v4 distinguishes its tracked-universe forecast from the broader descriptive series.

   **What information did each model receive?** State whether drift was measured independently or solved to hold the observed ladder, which years supplied calibration, and whether a panel shows the lasting core or full observation model. Starting from equality does not independently explain historical genesis when its ingredients were estimated from the existing hierarchy. Results 3’s heading should acknowledge its incomplete reproduction of the observed head, documented in §2z-dl.

   **Does heterogeneity alter the measurement itself?** Recovery of lasting variance under heterogeneous growth is essential to interpreting R12. If it fails, Results 2 also needs qualification. This cannot be treated as an optional SI refinement.

7. **Correct the budget and increase paragraph counts.**

   The listed section budgets total **5,100 words**, excluding front matter and captions—not 3,900–4,300. Several paragraph allocations also recreate v4’s density: 850 words in four paragraphs and 550 in three.

   A workable allocation is:

   | Section | Words | Paragraphs |
   |---|---:|---:|
   | Introduction | 450 | 4 |
   | Results 1 | 400 | 4 |
   | Results 2 | 650 | 6 |
   | Results 3 | 450 | 4 |
   | Results 4 | 300 | 3 |
   | Results 5 | 700 | 6 |
   | Discussion | 600 | 5 |
   | Methods and availability | 650 | About 6 |
   | **Total** | **4,200** | |

   With front matter and captions, this approaches 5,000 words before references. Seven pages remains a typesetting target, not a consequence of the word count.

   Treat the exemplar sentence-length statistics as diagnostics. Their strongest shared lesson is that each paragraph has a recognizable job and each comparison supports a bounded inference. That principle should govern the human rewrite.