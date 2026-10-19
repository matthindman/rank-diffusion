<!-- Provenance: Task 3 (PNAS exemplars) report by GPT-6 Astra (gpt-6-astra) (Codex CLI 0.163.0, codex --search exec, read-only sandbox, reasoning effort high), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

**Access check.** I read both assignment briefs completely; the v4 manuscript source, SI source, and bibliography at commit `42955ac`; `PROGRAM_STATE.md`; the relevant current results record (§2z-dk of `MODEL_STATUS.md`); and `SYNTHESIS_R5.md`. I inspected the five supplied manuscript figures. I surveyed all 31 exemplar texts, including front matter, introductions, results openings, and discussions or concluding sections, and read six substantially throughout: George and O’Dwyer, Kawakatsu et al., Juul and Ugander, Mallmin et al., Hendrick et al., and Sireci et al. I also inspected all five earlier template/report files. The quantitative report contains **ten paper entries, not twelve**.

Local text and PDF extraction, image inspection, and live web search were available. No exemplar was inaccessible. Some PDF extractions interleave columns; I checked passages used as quotations against the supplied text and alternate extraction. The PNAS submission-guidelines webpage returned a cookie page; its official NAS-authored Overleaf template supplied the relevant formatting guidance. I used the v4 source rather than assuming the newer working-tree manuscript represented v4. I did not independently render the v4 PDFs, open research data, or write files.

**Model:** GPT‑6; assignment designation GPT‑6 Astra, exact build unavailable. **Interface:** Codex CLI. **Tools:** local read-only shell, Poppler PDF extraction, image viewing, web search/full-text retrieval. **Date:** 19 October 2026.

# T3 — What the PNAS exemplars imply for the manuscript

## 1. Principal recommendation

The manuscript should become a shorter paper about **what random growth explains about an attention hierarchy, and what remains unexplained once geometry and sorting are accounted for**.

That formulation preserves the generative model and the substantive puzzle. It also gives the persistence result a clear role: a measured feature that the homogeneous models miss, whose mechanism remains unresolved. The present draft contains these distinctions, but its title, abstract, section order, and figure allocation sometimes obscure them.

The closest exemplars do not share a rigid paragraph formula. They share a more useful discipline: readers can identify the explanatory target, understand the model’s essential ingredients, and see which comparison supports which inference. The manuscript already has unusually substantial comparisons. Its revision problem is largely **selection and ordering**, rather than adding another literature survey or another analysis.

I recommend four main figures and approximately 3,600 words of narrative and Methods, excluding abstract, significance statement, captions, and references. Aim for seven readable pages. Retain eight only if the essential comparisons and uncertainty displays cannot survive that compression.

## 2. Corpus, counting, and limits of the comparison

### What the collection actually represents

The 31 papers span applied mathematics, statistics, ecology, evolution, neuroscience, computational biology, chemistry, urban systems, and online information diffusion. They include empirical model comparisons, mathematical methodology, simulation studies, and experimental biology. They are not 31 interchangeable examples of an empirical stochastic-model paper.

For example, Brown et al. primarily develop mathematical results and numerical illustrations; Ghio et al. analyze sampling algorithms through statistical-physics models; Mallmin et al. study a simulated ecological community. These are useful precedents for mathematical exposition, but weaker precedents for how much empirical identification this manuscript must supply. Conversely, Cramer et al. provide a strong model-evaluation precedent without being a generative explanation of a size distribution. [Brown et al.](https://doi.org/10.1073/pnas.2108031118), [Ghio et al.](https://doi.org/10.1073/pnas.2311810121), [Cramer et al.](https://doi.org/10.1073/pnas.2113561119) **[full text: relevant sections]**.

The following counts describe the supplied collection, not PNAS as a whole.

| Feature | Observed count or range | Interpretation |
|---|---:|---|
| Submission track | 24 Direct Submissions; 7 Contributed | Direct Submission is the relevant comparison group |
| Guest editors explicitly identified | 4 | Interdisciplinary handling is visible, but not exceptional permission to bypass disciplinary scrutiny |
| PDF article length | 5–12 pages; median 9, across 29 PDFs | Long articles occur frequently in this selected collection |
| PDFs of seven pages or fewer | 7/29 | Compact examples exist, but are a minority here |
| Main figures | 2–12; median 4, across 31 papers | Four figures are a practical center, not a requirement |
| Papers with exactly four figures | 14/31 | The proposed four-figure plan has substantial precedent |
| Papers with at most six figures | 27/31 | Eleven- and twelve-figure papers are outliers |
| Numbered equations | 0–52 | Equation count alone is a poor guide to accessibility |
| Papers without numbered equations | 7/31 | Some still contain mathematics or formal results |
| Significance statement | 31/31 | A format convention, not a discriminating editorial signal |
| Printed submission-to-acceptance interval | 35–356 days; median 173 | A description of accepted papers’ histories, not a forecast |

For timing, I used the printed received/sent-for-review and accepted/approved dates. Older Contributed papers use somewhat different labels. These intervals include author revision time and cannot identify referee difficulty, editorial enthusiasm, or the relative ease of submission routes.

For headings, a conservative manual screen identifies **13/31 papers with at least one assertion-bearing results/body heading**, counting modal assertions but excluding Discussion headings: Cramer, Gagler, George, Ghio, Gupta, Heckmann, Jin, Maddu, Mallmin, Minasandra, Morales, Pyo, and Sireci. The remaining papers use topical headings, mathematical organization, or comparatively undivided Results. This classification concerns headings, not whether their paragraphs begin with findings.

### Six full-read exemplars

| Exemplar | Why it is close | Architecture worth studying |
|---|---|---|
| [George and O’Dwyer 2023](https://doi.org/10.1073/pnas.2215832120) **[full text]** | Stochastic abundance model, temporal fluctuations, multiple empirical systems | Model and data → several observables → competing stochastic model |
| [Kawakatsu et al. 2021](https://doi.org/10.1073/pnas.2015188118) **[full text]** | A generative account of hierarchy, mathematical analysis, empirical fitting | Intuitive mechanism → formal model → limiting result → empirical comparisons |
| [Juul and Ugander 2021](https://doi.org/10.1073/pnas.2100786118) **[full text]** | Online behavior; apparent mechanism differences altered by conditioning | Empirical contrast → controlled comparison → model comparison → bounded interpretation |
| [Mallmin et al. 2024](https://doi.org/10.1073/pnas.2312822121) **[full text]** | Stable aggregate distributions with turnover of dominant members | Define model → demonstrate turnover → reduced stochastic explanation → scope |
| [Hendrick et al. 2025](https://doi.org/10.1073/pnas.2501224122) **[full text]** | Stochastic account of macroscopic scaling, empirical distributions | Define observational construction → fluctuation regularity → derived scaling |
| [Sireci et al. 2023](https://doi.org/10.1073/pnas.2217144120) **[full text]** | Models sharing marginal regularities but differing on informative correlations | Empirical target → competing explanations → discriminating prediction → temporal extension |

Hendrick is useful principally for the progression from measured fluctuations to a macroscopic relation. Mallmin is useful principally for the hierarchy-versus-members puzzle. Neither should substitute for the more demanding empirical comparison architecture in Juul or Sireci.

## 3. Structure: what to borrow

### Section architecture and word budgets

There are at least three viable architectures in the close-read set:

1. **Model before empirical tests:** Kawakatsu and George.
2. **Empirical discrepancy before model discrimination:** Juul and Sireci.
3. **Model system before emergent regularities:** Mallmin.

The manuscript needs a combination of the first two. Start with the observed puzzle, give the balance and model enough space to become intelligible, then distinguish reproduction of aggregate structure from explanation of persistent individual changes.

The earlier quantitative report is useful but its section labels require checking. For instance, it assigns much of Juul’s body to “Online Information Diffusion Data,” although that body also contains model comparisons. Dick’s nominal 187-word “Introduction” excludes three introductory subsections; including those produces about 1,249 words. Automated labels should not become editorial prescriptions.

Approximate prose counts from that report illustrate the range:

| Paper | Introduction | Model/theoretical setup | Results or empirical body | Discussion/conclusion | Standalone Methods |
|---|---:|---:|---:|---:|---:|
| George | 635 | 1,607 | 2,729 | 1,081 | 591 |
| Kawakatsu | 493 | 1,905, including limiting analysis | 1,767 | 615 | None |
| Juul | 972 | Integrated | 2,588, including models | 764 | 60 |
| Mallmin | 839 | 667 | 3,560 | 1,272 | 1,338 |
| Maddu | 921 | 2,419 | 2,376 | 758 | 1,084 |
| Cramer | 665 | Integrated | 1,966 | 1,396 | 2,206 |

These are extraction-based counts, not publisher word counts; displayed mathematics and section nesting complicate comparison. [Maddu et al.](https://doi.org/10.1073/pnas.2420621122) **[full text: relevant sections]**.

The useful conclusion is not that an introduction should contain a particular number of paragraphs. It is that **the paper’s intellectual work determines its allocation**. Cramer spends heavily on evaluation Methods. Kawakatsu embeds its methodological work in the model narrative. Juul keeps a substantial conceptual introduction but almost no standalone Methods.

For this manuscript, the present five Results subsections contain approximately **3,350 prose words**, excluding figure environments and displayed equations under a simple source-based count. The final persistence subsection alone is approximately 1,080 words. The largest opportunity is to remove duplication and competing subsidiary stories from Results.

### Equations and model introduction

The six close reads range from no numbered equations in Juul to 42 across Sireci’s main article, including Methods. George and Kawakatsu each carry six; Hendrick carries twelve, only two before the Discussion; Mallmin carries 35, fourteen before the Discussion.

This distribution argues against removing the manuscript’s model equation merely to appear accessible. It also argues against keeping every useful derivation in Results.

The best transferable practice is to establish:

- what changes;
- what drives that change;
- what is observed;
- which quantity will be predicted or compared.

Kawakatsu explains endorsement and forgetting before presenting the machinery. George separates the meaning of abundance fluctuations from the formula that describes them. Hendrick establishes the spatial aggregation procedure before interpreting scaling.

For the draft, keep three mathematical objects prominent:

1. the lasting-level evolution;
2. the crossing-gain relation;
3. the balance governing hierarchy shape.

The exact turnover integral can move to the SI. Its main-text role can be served by the intuition that a flatter ladder places more communities within reach of a boundary crossing. The distinction between this geometric consequence and a distinctive growth mechanism matters more than the full integral.

Also state plainly that the model has measured rank-dependent profiles and transitory components. A compact equation does not make it a three-parameter model.

### Figures and the role of Figure 1

The first figure need not be a schematic. Within the six close reads:

- Kawakatsu uses a model schematic.
- Hendrick establishes the observational construction.
- George introduces datasets and fluctuation behavior.
- Juul makes the decisive empirical comparison immediately.
- Mallmin displays the turnover phenomenon.
- Sireci defines and displays the correlation pattern to be explained.

These are six different implementations of the same useful function: **Figure 1 teaches the reader what the paper needs to explain**.

The draft’s first figure already does much of this. The problem is that its trajectory panel and the separate second figure consume considerable space before the central balance appears. A single opening figure can establish the hierarchy, membership turnover, and the limited geometric explanation.

The extreme figure counts elsewhere should not guide this manuscript. Bartolucci’s eleven figures support a sequence of analytical and numerical constructions; Jin’s twelve support a long methodological argument and repeated generalization comparisons. Their page economies are different. [Bartolucci et al.](https://doi.org/10.1073/pnas.2403682121), [Jin et al.](https://doi.org/10.1073/pnas.2427181122) **[full text: relevant sections]**.

### Results headings

The heading evidence does not support a mandate that every subsection state a conclusion. In the close-read group, George, Mallmin, and Sireci use assertion-bearing headings; Kawakatsu, Juul, and Hendrick largely use topical headings.

George’s four assertion-bearing headings out of five Results subsections work because each introduces a distinct test. Mallmin’s five out of six work because each describes another property of the model community. Sireci’s headings make the comparison among explanatory alternatives explicit.

The manuscript can retain finding-oriented headings, but they must express the actual scope:

- “The top turns over fastest…” needs a defined top set and the geometric comparison.
- “Why the hierarchy is durable” currently sounds broader than the comments-versus-submissions result permits.
- “What homogeneous random growth lacks…” is valuable because “homogeneous” specifies the rejected class.

A heading should compress a supported inference, not convert a qualified paragraph into an unqualified claim.

### Null models, limitations, and Methods

The closest exemplars use rivals as part of the argument. Juul shows what changes after matching. Sireci asks which observation distinguishes models that reproduce other regularities. George compares stochastic models on more than their stationary distribution. Kawakatsu allows different model specifications to fit different empirical systems best.

The manuscript should therefore organize null models **by the inference they test**, rather than present a procession of named worlds. Geometry controls belong beside the turnover claim. Homogeneous and heterogeneous growth belong beside persistence. Forecast baselines belong beside forecast performance.

Limitations should appear twice, for different purposes:

- **Locally**, when they determine how a result can be interpreted.
- **In Discussion**, when they constrain the overall account and its scope.

The failure to distinguish persistence from fixed growth heterogeneity belongs immediately after the persistence comparison. The model’s flatter equilibrium and lack of forecast improvement belong in its Results subsection. Neither should depend on readers reaching the final limitations paragraph.

Main-text Methods should retain the information needed to judge the evidence: observational units, measurement, universe selection, disjoint cohort construction, estimation principles, uncertainty units, evaluation windows, and simulation replication. Implementation details and full derivations can move. Juul’s 60-word standalone Methods is not permission to omit essential design information; much of its design is already in the body.

## 4. Tone and style

### Sentence length: manage conceptual load

The papers do not establish a universal short-sentence style. In the earlier report, George’s introduction has 635 words in 24 sentences—about 26 words per sentence—and Mallmin’s has 839 in 31, about 27. Both contain longer technical sentences.

Their more useful practice is to give sentences distinct jobs. A sentence defines a quantity, states an observation, or explains a comparison. The draft frequently combines all three with a qualification and a literature connection.

The second introductory paragraph is the clearest repair point. It currently moves through firm fluctuations, cumulative advantage, online ranks, cities, random growth, stochastic portfolio theory, and concentration accounting. Divide that material by function, then cut it:

- one short paragraph on the mechanism problem and relevant random-growth tradition;
- a later, local attribution for the balance and arithmetic-share decomposition.

This reduces the burden without pretending the prior literature is smaller.

### First person and hedging

First person is normal in the exemplars. All ten introductions in the earlier quantitative report contain first-person usage. Their rates vary substantially: roughly 3 to 19 occurrences of “we” per thousand words in the principal Introduction rows. There is no evidence for either suppressing “we” or forcing it into every paragraph.

Use “we” for actions that distinguish evidence types: we estimate, compare, simulate, decompose, and test. Prefer the phenomenon as subject when stating an observation.

Hedging should identify the uncertainty. “Suggests” is useful when moving from a pattern to a possible mechanism. It is less useful than saying directly that heterogeneous mean growth also reproduces the statistic. Broad words such as “consistent with” need the named alternative or assumption that makes them informative.

### Generality and universality

The corpus does not speak with one voice. George’s cross-system comparison, Gagler’s biochemical regularities, and Meng’s information-spreading equation use expansive language. Minasandra explicitly leaves the source of shared behavioral patterns unresolved. Jin limits its interpretation to settings resembling the studied replication projects. These are materially different claims. [Gagler et al.](https://doi.org/10.1073/pnas.2106655119), [Meng et al.](https://doi.org/10.1073/pnas.2410227122), [Minasandra et al.](https://doi.org/10.1073/pnas.2503962122) **[full text: relevant sections]**.

For this manuscript, adopt the scope discipline of Jin and the model-comparison discipline of Sireci. Do not borrow “universal” from the collection’s most expansive abstracts. The present evidence concerns particular activity measures, populations, periods, and observation processes. Facebook is useful comparative evidence, but its shorter horizon does not turn every Reddit finding into a cross-platform regularity.

Likewise, “durable” should describe measured relative timescales, not imply stationarity everywhere.

### Novelty, questions, and specialist vocabulary

The strongest novelty signal is a newly informative contrast: observations that look similar under one statistic differ under another, or an apparently diagnostic pattern survives several rival models. That can be communicated without “first,” “unprecedented,” or a declaration that the paper resolves a field.

Question openers are optional. Juul’s significance statement and Lee’s significance statement use them effectively; the corresponding introductions do not all follow the same pattern. Lee’s opening is especially close to the draft’s enduring-structure/transient-members puzzle, but its experimental identification is different. [Lee et al.](https://doi.org/10.1073/pnas.2211572119) **[full text: relevant sections]**.

Keep the manuscript’s opening question. Reduce the number of subsequent rhetorical questions and use declarative transitions to advance the answer.

For vocabulary, introduce ordinary meaning before notation:

- “communities” and “pages” before “endpoints”;
- “temporary fluctuations” before the component labels;
- “the average level of the largest communities relative to the whole population” before \(U_k-U_K\);
- “members’ relative decline” when introducing “slide.”

Retain “attention” as the motivating domain only with a clear early statement that the observations are activity and engagement measures, not direct exposure or time spent.

### Verified passages worth emulating

These are rhetorical examples, not proposed additions to the manuscript.

> “Do different types of information spread differently online?”

Juul and Ugander, p. 1, Significance. The surrounding statement explains why comparisons of cascade statistics may mislead. The question opens an identifiable comparison rather than promising a complete theory. **[verified: supplied file `juul-ugander-2021-comparing-information-diffusion-mechanisms-by-matching-on-cascade-size.pages.txt`, PDF p. 1]**.

> “not all statistical patterns are equally informative.”

Sireci et al., p. 2. The authors explain that different ecological mechanisms can reproduce the same abundance distribution, motivating a more discriminating observation. This is an especially apt transition between the draft’s balance relation and persistence result. **[verified: supplied file `sireci-et-al-2023-environmental-fluctuations-explain-the-universal-decay-of-species-abundance-correlations-with.pages.txt`, PDF p. 2]**.

> “we do not attribute this pattern in the parameter estimates to a universal mechanism”

Kawakatsu et al., p. 5. The surrounding passage identifies an empirical similarity while explicitly withholding a common-mechanism interpretation. The manuscript needs this distinction when discussing persistence across communities. **[verified: supplied file `kawakatsu-et-al-2021-emergence-of-hierarchy-in-networked-endorsement-dynamics.pages.txt`, PDF p. 5]**.

> “Despite its imposing appearance, the equation depends only on two parameters”

George and O’Dwyer, p. 4. The surrounding passage explains the distribution’s parameter meanings. The transferable move is interpreting the equation immediately; the manuscript must not inherit the parameter count. **[verified: supplied file `george-o-dwyer-2023-universal-abundance-fluctuations-across-microbial-communities-tropical-forests-and-urban-populations.pages.txt`, PDF p. 4]**.

Failures are also written directly. George reports cases in which model comparisons or parameter consistency are unfavorable. Kawakatsu reports different empirical winners. Gupta states that current measurement technology does not yet support the proposed inference at full scale. These papers preserve a contribution by stating what remains demonstrated, not by making every comparison favorable. [Gupta et al.](https://doi.org/10.1073/pnas.2207392119) **[full text: relevant sections]**.

## 5. What can be inferred about editors and referees

### External evidence

PNAS’s current policy describes editorial-board assessment, member or guest-editor assessment, and independent peer review. Research papers across routes receive review by at least two independent experts; Direct Submission reviewers are normally anonymous, whereas Contributed papers identify reviewers. These are explicit process facts, not inferred preferences. [PNAS editorial policies](https://www.pnas.org/author-center/editorial-and-journal-policies) **[full text]**.

The official template recommends approximately 4,000 words, 50 references, and four medium graphical elements for a standard six-page article, while allowing longer articles. It also permits different section orders. That supports a compact revision without requiring an artificial IMRaD arrangement. [Official NAS PNAS template](https://www.overleaf.com/latex/templates/template-for-preparing-your-research-report-submission-to-pnas-using-overleaf/fzcbzjvpvnxn) **[full text]**.

A historical editorial explains why the significance statement replaced longer author summaries: those summaries often failed to communicate the important point to nonspecialists. Its durable lesson is separation of purpose:

> “The Significance Statement is distinct in purpose from the abstract”

Verma, *PNAS Plus: Refining a successful experiment*, main editorial text. **[verified: https://pmc.ncbi.nlm.nih.gov/articles/PMC3427102/]**. The editorial’s historical length rules should not be substituted for current guidance. [DOI](https://doi.org/10.1073/pnas.1212313109) **[full text]**.

### Evidence from the accepted papers

The handling lines show multiple disciplinary routes. Alan Hastings handled George, Kawakatsu, and Mallmin; Terrence Sejnowski handled Morales, Mori and Kori, and Pospisil and Pillow. Yuhai Tu appears as guest editor for Bahri and Ghio. Juul had Duncan Watts as guest editor with Adrian Raftery’s editorial-board oversight.

These clusters show that closely related modeling ideas can be evaluated through ecology, neuroscience, applied mathematics, or social-network expertise. They do **not** identify a permissive editor or imply that choosing a field label changes the evidential burden.

The seven Contributed papers are Brown, Dick, Gupta, Hadad, Hendrick, Lee, and Xue. Their presence matters when interpreting the collection, but it does not justify discounting their scientific or stylistic lessons.

### My inferences, with their limits

**First, a sharply specified explanatory target is more transferable than a complete explanation.** Juul makes a focused correction to inference from diffusion statistics. Gupta offers a theoretically supported route whose full empirical implementation remains technologically constrained. This suggests that the draft can be compelling without claiming that its model explains every aspect of hierarchy and movement.

**Second, a discriminating comparison carries more explanatory weight than visual agreement alone.** This is especially clear in Juul and Sireci. For the draft, the controlled geometry comparison and the persistence comparison should receive more narrative emphasis than the raw exponent plot.

**Third, mathematical detail is tolerated when its role is clear.** The equation range is enormous, yet the closest examples provide interpretive scaffolding. Removing the model from the main text would weaken the paper’s identity.

**Fourth, limitations can sharpen a contribution.** The useful pattern is to state what a model accomplishes and which observation remains outside it. The draft’s flatter simulated hierarchy and forecast failures can establish the boundary of its account.

These are inferences from published products, not observations of editorial decisions. We do not have the rejected papers, initial submissions, or referee reports. The collection cannot show that a particular rhetorical move caused acceptance.

Nor does it support universal requirements such as multiple empirical systems, a theorem, a schematic first figure, or a baseline victory. Counterexamples exist within the collection. The observed commonality is broad scientific framing, but that is partly imposed by journal format.

## 6. Comparison with v4 and ranked concrete edits

### What already works

The draft has a recognizable scientific puzzle; connects population structure with named-member movement; introduces a generative process; runs informative counterfactuals; and reports unsuccessful predictions. It also contains selection-aware cohort construction, replicated simulations, and distinctions among geometric, accounting, and dynamical results.

Those are substantive assets. Preserve them. The main weakness is that the paper sometimes offers a smoother, more unified explanation in its framing than its Results establish.

### 1. Align the title, significance statement, and abstract with the actual scope

**Locations:** title; final sentences of significance statement and abstract; final introductory paragraph; opening Discussion paragraph.

The title “How a hierarchy of online attention outlasts its members” is memorable. However, the timescale result supports that phrasing for comments, not uniformly for the submissions head. The current front matter also connects persistence to the head’s participation decline more tightly than the body demonstrates.

**Smallest revision:** retain the question but remove the implied completed causal chain. A broader title option is:

**Turnover and persistence in the hierarchy of online attention**

The significance statement should communicate three things: substantial membership replacement can coexist with a steep hierarchy; measured growth and decline help explain that coexistence; persistent participation changes remain outside the homogeneous model. It does not need the head’s full numerical decomposition.

In the abstract, distinguish these sentences explicitly: the model generates a hierarchy but misses its observed steepness and some dynamics; independently, the submissions head loses share as its posting declines. Do not join them with language implying that the measured persistence has been shown to cause that particular decline.

### 2. Make the balance the organizing result

**Location:** third Results subsection and its transition to the model.

The present heading promises an explanation of durability before specifying where durability holds. Lead with the measured balance, then distinguish gross crossing gains from their net contribution relative to the universe.

**Smallest revision:** rename the subsection along the lines of **Crossings offset decline, with different consequences at the head**. Explain the comments/submissions contrast in its first substantive paragraph.

Keep one direct attribution to the existing rank-based diffusion tradition. The contribution is the measured decomposition and its consequences in these data, not the existence of a previously unknown identity. Fernholz and Gomez are already cited; the task is to position those citations more effectively, not add them as missing references.

### 3. Compress the first two Results subsections into one opening movement

**Locations:** “A fast turnover…” and “The top turns over fastest…”

The current first subsection combines data definition, turnover, shape change, concentration, named examples, and bans in one long paragraph. The second adds an extended exponent discussion.

**Smallest revision:** use three short paragraphs:

1. define the observed populations and puzzle;
2. distinguish top-set replacement from changes at a particular rank;
3. show what the controlled geometry comparison explains.

Retain one named-community example if it makes the rank-versus-share distinction immediately clear. Move the remaining named histories and ban discussion to the SI.

### 4. Move the raw size-exponent dispute out of the main figure sequence

**Locations:** second Results subsection; Figure 2A.

The raw exponent receives a prominent visual comparison despite the draft’s own calibration showing that it cannot distinguish the planted alternatives reliably.

**Smallest revision:** move the raw exponent panel and estimator catalogue to the SI, retaining a main-text sentence that the raw statistic is not sufficiently diagnostic. Promote the paired geometry comparison into the opening figure.

This is a reduction in emphasis, not deletion of an unfavorable result. It makes the main text match the conclusion of the analysis.

### 5. Give the generative model a clear target and an equally clear boundary

**Location:** fourth Results subsection; Figure 4.

Begin with what is measured and what is simulated. Separate the lasting process from the observation/transitory components in one compact diagram or two explanatory sentences.

Then distinguish three tests: generation from equality, evolution from the observed hierarchy, and removal of ingredients. These answer different questions and should not blur into one “model fit” claim.

Keep the model’s flatter equilibrium and lack of held-out movement improvement in the main text. Those findings, documented in `MODEL_STATUS.md` §2z-dk, constrain the explanation. Move the full variant table, implementation-error history, and shorter-panel history to the SI. The earlier failed version must remain documented there.

### 6. Divide the last Results subsection into two explicit arguments

**Location:** “What homogeneous random growth lacks: persistent decline.”

This subsection currently carries persistence, period sensitivity, participation versus engagement, shared shocks, Facebook comparison, prior decliners, and head concentration accounting.

**Smallest revision:** create two visible paragraph groups:

- **Persistent changes beyond homogeneous growth.** Present the gapped statistic, heterogeneous-growth alternative, and common-factor qualification together.
- **Participation and the changing submissions head.** Present the accounting decomposition without claiming that it identifies the mechanism behind persistence.

The factor-adjusted comments result is the cleaner persistence finding. The submissions result’s sensitivity to platform-wide adjustment belongs beside it, not several qualifications later.

The prior-decliner analysis can be one sentence with an SI pointer. Preserve its exploratory status and failed expectations. It should not become the bridge that rhetorically joins two analyses that have not established the same causal chain.

### 7. Rebuild the figures around comparisons

**Locations:** all five main figures.

- **Figure 1:** retain hierarchy and membership turnover; add the compact controlled geometry comparison. Replace much of the faint trajectory spaghetti with a small number of clearly explained trajectories or an uncertainty-bearing summary.
- **Figure 2:** show the boundary relation and the measured net balance. Remove the current Figure 3 turnover-ingredients panel from this location.
- **Figure 3:** show the generative process, hierarchy from equality, and ingredient removals. Include observed benchmarks and seed uncertainty. Keep the forecast verdict in the caption/text even if the complete forecast comparison moves to SI.
- **Figure 4:** give persistence and participation accounting visibly distinct panel groups. Make the heterogeneous-growth explanation and common-factor adjustment legible.

The supplied figures also need local layout repair: long headings and annotations crowd adjacent panels in Figures 3 and 4, and Figure 5’s labels and legends compete for space. These are reasons to simplify panel content before reducing figure size.

### 8. Shorten the introduction and Discussion by removing repeated promises

**Locations:** introductory paragraphs 2–3; Discussion paragraphs 1–2.

The introduction should end with the paper’s actual sequence: measure the puzzle, account for shape change, test a generative model, locate a persistent residual. It does not need to preview every statistic.

The Discussion should state the resulting explanatory boundary once. Remove broad platform-design implications that sound like intervention conclusions. A defensible implication is that interventions or shocks may alter participation and growth, while this analysis does not identify their effects.

Retain the distinction between measured activity and attention. Avoid reintroducing the abandoned restricted-access measurement application.

### 9. Keep essential Methods; move the audit trail without erasing it

**Locations:** Methods and SI.

Keep in main text:

- what each activity measure records;
- panel and analysis-window definitions;
- permanent-rank and absence treatment;
- disjoint cohort selection;
- moment-based estimation;
- simulation replication and uncertainty;
- rolling-origin evaluation;
- exploratory/confirmatory status.

Move detailed estimator variants, all cutoffs, the full model-variant comparison, and the expectation-by-expectation history to SI tables.

The manuscript and brief refer to different comments-panel durations in different contexts. The authors should reconcile those descriptions into one explicit analysis-window table. This is a documentation check, not a request to reopen or reanalyze data.

Replace availability placeholders with concrete destinations and restrictions. Keep the AI disclosure in main Methods, consistent with current PNAS policy, while placing the extended review history in the SI. [PNAS editorial policies](https://www.pnas.org/author-center/editorial-and-journal-policies) **[full text]**.

## 7. Recommended templates and a compact skeleton

Use five exemplars for distinct purposes:

1. **Juul and Ugander:** the overall logic of a controlled comparison.
2. **Sireci et al.:** separating broadly reproducible patterns from discriminating evidence.
3. **Kawakatsu et al.:** introducing a generative hierarchy model intelligibly.
4. **George and O’Dwyer:** linking temporal behavior and population distributions.
5. **Mallmin et al.:** presenting enduring aggregate structure with changing dominant members.

Do not combine their section lengths. Borrow their functions.

| Section | Target words | Function and exemplar connection |
|---|---:|---|
| Significance | 100–110 | Explain the puzzle and consequence for a broad reader |
| Abstract | 200–220 | State findings and model boundary without a causal bridge |
| Introduction | 400 | Puzzle → existing random-growth account → unresolved empirical question |
| Results 1: Turnover and geometry | 500 | Establish phenomenon; distinguish automatic geometric effects, following Juul |
| Results 2: Measured balance | 500 | Connect lasting fluctuations, decline, and shape change, following George |
| Results 3: Generative model | 600 | Introduce process, counterfactuals, successes and failures, following Kawakatsu/Mallmin |
| Results 4: Persistence and participation | 650 | Identify the residual and competing interpretations, following Sireci |
| Discussion | 450 | State explanatory scope and remaining alternatives |
| Methods | 500 | Preserve the design information needed to judge validity |
| Four captions | About 450 total | Takeaway, panel definitions, essential uncertainty and design |

This totals approximately **3,600 words of narrative and Methods**, plus about 760 words of front matter and captions. References, equations, and figure footprints still determine pagination. It is a seven-page design target, not a guaranteed typeset length.

Three numbered equation groups should be sufficient. Retain the stationary relation beside the balance if it aids interpretation; do not add mathematics to meet a numerical target.

The SI would carry the exact turnover derivation, raw-exponent recovery and sensitivity, complete model specification, full forecast comparisons, all simulation variants, period/factor/universe sensitivities, named histories, prior-decliner analysis, and the complete exploratory-analysis record.

There is a reasonable case for eight pages if four figures become illegible or the main text loses the qualifications necessary to interpret persistence. There is no equally strong case for eight pages merely to preserve the present order and every diagnostic.

## 8. What the earlier templates got right and wrong

### Retain their useful practices

The earlier templates correctly emphasize an early empirical object, intuition before equations, comparisons with simpler explanations, informative captions, and a deliberate main-text/SI split. Their distinction between public activity and exposure is still essential. Their recommendation to state assumptions for quantities inferred from incomplete observations reflects good general practice, even though that application is no longer central.

The quantitative report is particularly useful as a descriptive resource. Its paragraph maps show that introductions and discussions perform different citation work from Results, and that first-person usage is ordinary.

### Retire the old scientific storyline

The January–April templates repeatedly prescribe a sequence built around restricted platform access, an invariant rank-share curve, a permanent–transitory model, and a measurement payoff from partial collection. That is a different paper.

Several instructions now conflict with the evidence or question:

- opening with the post-API measurement problem;
- treating rank-share invariance as established;
- requiring the model to reproduce both aggregate structure and individual movement;
- requiring a win over simpler models;
- reserving a main figure for partial-observation estimation;
- ending with independent platform measurement.

These are not harmless placeholders. They can cause a draft to overstate stability, conceal model failures, or add an application that interrupts the present argument.

The templates also overprescribe paragraph mechanics. A claim–evidence–mechanism–implication sequence is useful when the paragraph actually establishes all four. Here, many paragraphs establish an accounting relation or eliminate one explanation without identifying a mechanism. Forcing a mechanism sentence into each would weaken the evidential distinctions.

Similarly, numerical anchors are useful when they identify an effect or comparison. A number in every paragraph is not a demonstrated PNAS preference. Nor is “we find/show for the first time” a useful phrase bank for this manuscript.

Finally, revise the templates’ source claim: the supplied quantitative report contains ten entries, numbered 01–09 and 11. Its counts should be retained with that denominator and with corrected section boundaries where necessary.

The replacement drafting rule should be simple: **each main-text subsection answers one question, identifies the comparison that answers it, and states what that comparison leaves unresolved.**

## 9. Search and verification log

**Date:** 19 October 2026.

**Local materials:** the two briefs; v4 TeX sources and bibliography retrieved with `git show 42955ac:…`; supplied figures; program state, relevant canonical results section, and round-five synthesis; 31 supplied exemplar texts and their named PDFs; five prior template/report files. Lightweight text processing was used for page, figure, equation, timing, and approximate section counts. No research data were opened.

**Web searches:** PNAS editorial preferences and significance statements; “PNAS Plus: Refining a successful experiment”; PNAS submission length, abstract, and significance requirements; current PNAS editorial and peer-review policies.

**External evidence used:** the PNAS editorial-policy page, the official NAS-authored Overleaf template, and Verma’s 2012 editorial. Secondary formatting and submission-advice pages were not used to infer editorial preferences. The direct submission-guidelines page was inaccessible beyond its cookie response.

**Verification boundary:** the report evaluates presentation and comparative architecture. It does not independently reproduce the manuscript’s scientific results, infer acceptance probabilities, or establish priority claims.