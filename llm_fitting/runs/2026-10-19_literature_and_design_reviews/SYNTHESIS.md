# Synthesis of the literature and design reviews (round 6)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-19, lead agent._
- **Manuscript reviewed:** v4 (commit 42955ac).
- **Briefs:** `COMMON_BRIEF.md` and `T1`–`T4`, committed 94695b0 before any review ran.
- **Practices adopted from the owner-supplied GPT-6 Pro report:** `00_PRACTICES_ADOPTED.md`.
- **Record:** MODEL_STATUS §2z-dl.

The owner asked for eight reports: four tasks, each done by GPT-6 Astra (Codex) and by Claude Opus 5.5 (sub-agent). This file records my judgment of what in them is useful and important. A six-page paper plus SI cannot absorb all of it, and should not try.

## Bottom line

1. **The paper's question and its qualitative answer are already in the lead author's book.** *The Internet Trap* (2018), ch. 5, argues that churn produces and maintains the power law of web traffic, using finance-derived stochastic models. The draft must say so and state what it adds (§2). That is a fix of a few sentences, not a threat, if done before submission.
2. **The durability the title celebrates is what homogeneous random growth predicts.** At the measured volatility and slope, the shape should outlast lasting membership by a factor of about eight, on both ladders. Comments behave that way. The submissions head lost a third of its share in three years, an order of magnitude faster than such growth allows, and persistent decline is the candidate channel (C2). This is GLLM's fast-transition problem in attention. Organizing the paper around it turns the generic part into measured theory and the exception into the discovery.
3. **The one new analysis worth running is a calibrated heterogeneous-growth world**, with an estimate of the spread of mean growth and a recovery check (C3). It tests the discovery directly. Everything else proposed is framing, or belongs in the SI.
4. **Write the paper less densely; length is not the problem** (C5). 36.5 words per sentence against the exemplars' 27; "we" at a fifth of their rate; hedges carried by numbers. Target 7 pages with 4 figures. Organize around the balance, fold the geometry result in, and put the raw-β, rank-one and forecasting detail in the SI.
5. **Present every model as a column, in a limited form** (C6). An SI atlas with a stated selection rule and unrun cells shown; the hero model presented as "the calibrated random-growth model"; for the sealed test, forecasts from every eligible model. No main-text ablation ladder and no ensemble.
6. **About 12 new main-text references, chosen to change claims, not to decorate them** (§2).
7. **Several small corrections**:
   - the causal "through that channel" link goes (C1);
   - "rank is a poor measure of entrenchment" and the platform-design sentence become conditional;
   - the l. 93 genealogy is split by mechanism;
   - activity is distinguished from attention;
   - the figure defects are fixed;
   - the AI statement becomes precise about figures.

## 0. What was run, and how far to trust it

| report | model | words | quotations | notes |
|---|---|---|---|---|
| T1 platform literature | GPT-6 Astra | 8,600 | 12 from sources | 24 entries, 10 tier-1 |
| T1 platform literature | Opus 5.5 | 10,300 | about 20, each under 15 words, self-verified against full text or Google Books snippets | 7 groups, 9 tier-1; found the lead author's own precedent |
| T2 stochastic methods | GPT-6 Astra | 6,200 | 4 | 18 entries |
| T2 stochastic methods | Opus 5.5 | 10,400 | about 25, PDF-checked or page-fetched | 9 tier-1; the most consequential report |
| T3 PNAS exemplars | GPT-6 Astra | 5,900 | 4 exemplar + 1 PNAS editorial | |
| T3 PNAS exemplars | Opus 5.5 | 11,000 | 1 (copyright rule), the rest located paraphrases | full counts over 31 papers |
| T4 one model or many | GPT-6 Astra | 5,000 | 3 | |
| T4 one model or many | Opus 5.5 | 5,900 | 1 (copyright rule) | |

### Verification

**Quotations and references in the GPT-6 Astra reports.** An independent Claude sub-agent checked every quotation in the T1, T2 and T4 reports, and the PNAS editorial in T3, against full text (`VERIFICATION_GPT_QUOTES_AND_REFS.md`):
- **Quotations:** 20 of 20 match exactly at the claimed location. 13 were checked in the PDF text with page headers.
- **References:** 14 of 14 key new ones exist with correct details, and none of the 13 claims checked is contradicted.

**Quotations from the exemplars.** These were checked here with a local audit script, `quote_audit.py`, against reading-order text of the source PDFs:
- T3 GPT: 4 of 4 found, at the stated pages.
- T3 Opus: its 5 exemplar quotations found.

**Why the Opus reports carry few quotations.** The Opus sub-agents run under a rule that allows at most one short verbatim quotation per response. They therefore give located paraphrases instead, labelled as such. The owner's request for exact quotations with page numbers is met mainly by the GPT reports; their quotations are verified.

### Process notes
- **Opus reports came back as text.** Sub-agents may not write report files, so the four Opus reports arrived as text and I saved them verbatim from the agent transcripts. The provenance headers say so.
- **Two errors in my brief, not in the manuscript:**
  - The brief gave the comments panel as 136 weeks. The manuscript, correctly, uses December 2018 to December 2022, about 213 weeks.
  - The brief listed an "Atlas" world. The code key `atlas` is the firm-like, reflected-floor Gabaix world, which the SI rightly calls "firm-like". No Banner–Fernholz–Karatzas Atlas comparator exists.

  T3 GPT, T4 GPT and T4 Opus each caught one of these.
- **R11 was run before this round and scored during it.** It tested the double counting of entry and exit in the model's drift (RESULTS R11). It answers the open accounting check that T4 GPT ranked first. None of the reviewers saw the result.

## 1. Where the reports converge (the strongest signal)

Independent reports reaching the same point from different evidence is a lead, not proof. I checked each point below against the manuscript or the data outputs.

### C1. "Through that channel" must go
- **Who:** T1 GPT, T3 GPT, T3 Opus.
- **The problem:** the significance statement (l. 74) and the abstract's last sentence (l. 79) link persistent decline causally to the head's loss of share. The Results disclaim that link at l. 182: we have not shown that the head's decliners are the persistent decliners.
- **Check:** confirmed by reading.
- **Action:** adopt. Present the two findings side by side, without a causal link.

### C2. The separation of time scales is what homogeneous random growth predicts
- **Who:** T2 Opus (derivation), T2 GPT and T4 Opus (the GLLM arc), T3 both (the claim holds on comments only).
- **Check, computed here** from the saved balance-law cells (`runs/2026-10-14_balance_law/cells/law__*.json`, 2019–21 pairs; a calculation, not a test):
  - Take GLLM's reflecting-barrier relaxation time 1/λ = 8S²/σ² at the measured lasting σ_k² and slope S_k.
  - **Comments:** 18–24 years at K/64–K/8.
  - **Submissions:** 13–31 years at K/64–K/8.
  - Over the same ranks, the time for lasting moves to span one slope unit, S²/σ², is 2–4 years.
  - So homogeneous random growth at the measured parameters predicts that the hierarchy outlasts its members on both ladders, by a factor of about eight.
- **The data:**
  - **Comments** behave that way: linearized relaxation of about 9 years at the head, §2z-cp.
  - **The submissions head does not.** Its shape relaxes in 3–4 years, and the head lost a third of its share in three years. That is roughly an order of magnitude faster than homogeneous random growth allows.
- **Why it matters:**
  - This is GLLM's fast-transition problem, transposed to attention with a falling rather than rising concentration.
  - The paper already holds the candidate answer: persistent decline carries 40–60% of the slide's excess over random growth (R10).
  - It already holds the evidence: the homogeneous model "does not forecast the head's fall".
- **Judgment.** This is the most valuable reframing in the round. It turns:
  - the generic part of "outlasts its members" into theory, measured;
  - the submissions head from an exception into the paper's discovery: when the hierarchy moved fast, it moved through persistence.

  It needs no new data. The formula gives the anchor, and the model's 18–27-year relaxation at the head (G3) agrees with it. The title and claim language change, so the **owner decides** (§4).

### C3. The persistence result needs its rivals inside it, and one calibrated heterogeneous-growth world
- **Who:** all four T2/T4 reports; T1 GPT (relational and institutional sources).
- **What they agree on:**
  - Every simulated world has homogeneous mean growth. The only heterogeneous check is a 5-seed synthetic one on one statistic.
  - Three readings of persistence are distinguishable in principle by the lag profile of the gapped correlation:
    - **fixed heterogeneity** (HIP in income dynamics; GLLM's "types"; the second-order model of Fernholz, Ichiba & Karatzas 2013);
    - **temporally correlated, environmentally varying growth** (Kalyuzhny et al. 2015; Mallmin, Traulsen & De Monte 2026);
    - **heterogeneous exposure to common shocks.**
  - The SI's lag profile already points somewhere:

    | gap (weeks) | 0 | 26 | 52 | shape |
    |---|---|---|---|---|
    | submissions | +0.194 | +0.150 | +0.046 | decays |
    | comments | +0.135 | +0.136 | +0.108 | flat |

  - Under heterogeneous growth, variance grows as σ²h + σ_β²h² (Gomez Appendix C.1; the HIP/RIP literature). This would bias the lasting-variance estimate, and with it c_k and β. This is round-5 issue G15.
- **Judgment.** This is the single most valuable new analysis. It tests the paper's discovery directly, which nothing else on the list does. Three parts, all post hoc on development data, with expectations written first:
  1. **Estimate the spread of mean growth σ_β, and its decay, from the autocovariance of window changes** at several gaps, by minimum distance as in the income-dynamics literature. Report it with an interval.
  2. **Run a calibrated heterogeneous-growth world from the 2019 ladder** at 20 seeds, in fixed and decaying variants, plus a common-factor variant on submissions. Score it on:
     - the gapped statistic;
     - R10;
     - the head's 2020–22 share path;
     - the 30-year slope.
  3. **Test recovery of σ_k and β under that world.** This is G15.

  State in advance Fernholz's point: the stationary shape depends only on rank-specific volatility and reversion. So heterogeneity should change speed, not the stationary slope.

### C4. Engage the rank-model tradition precisely, and claim measurement, not the identity
- **Who:** T2 both, T4 Opus, round 5.
- **What to engage:**
  - **Fernholz & Fernholz 2020 (eq. 2.17)** gives S* = σ²/(2g) for Atlas models. Cite it by equation number.
  - **Fernholz, Ichiba & Karatzas 2013** is the second-order, name-specific extension that persistence calls for. It is missing.
  - **Baker & Smyth 2026** (already cited in one clause):
    - same puzzle in equities;
    - a higher evidentiary standard: the curve predicted without fitting, time scales tested;
    - an apparently opposite conclusion: turnover in their sense, meaning listings and delistings, not drift, holds the curve.

    After R11 we can say precisely what their "turnover" maps to: our e_k, the effect of entry and exit. That effect is not small at the comments head (−0.0018 against a slide of 0.0036 a week). So the contrast must be stated with care, not as a simple "the opposite here".
  - **Precision fix (l. 93):**
    - Gibrat's process has no stationary law.
    - In Gabaix 1999 the stabilizer is a lower barrier.
    - "A drift that holds back the larger ones" is the rank-based tradition.

    Split the citation list by mechanism.
- **Judgment.** Adopt all of it. It is cheap and pre-empts the theorist referee who sank enthusiasm in rounds 4 and 5.

### C5. Restructure for density, not just length
**Who:** T3 both, T2 GPT item 8, T4 both.

**The evidence (T3 Opus, counted over all 31 exemplars with one pipeline):**

| measure | draft | exemplar median |
|---|---|---|
| words per sentence | 36.5 | 27 |
| share of sentences over 40 words | 30% | 14% |
| "we" per 1,000 words | 3.1 | 14 |
| modal hedges per 1,000 words | 0.5 | 3.6 |

- The draft hedges with numbers and parentheticals instead of words.
- Eight pages is within the corpus norm (median 9 pages; the closest papers run 6–12). The problem is density.

**Agreed moves:**
- **Organize Results around the balance.** Move geometry to one paragraph inside the opening movement.
- **Move to the SI:**
  - the raw-β panel and estimator catalogue (Fig. 2A, l. 116);
  - the rank-one paragraph (l. 118);
  - the forecasting paragraph and Fig. 4D, leaving two sentences;
  - the persistence robustness numbers.
- **Split the 1,067-word persistence subsection in two:** persistence with its rivals; then participation and the head.
- **Figures:** four, not five. Fig. 1 should teach what must be explained; 18 of 31 exemplars have a schematic there.
- **Discussion:** add a rival-readings paragraph, (a)–(d), as Sireci does. Exemplars run 600–1,300 words; the draft has 539.

**Judgment.**
- Adopt the moves and the 7-page target. A 6-page target would cost the qualifications the paper needs.
- Split sentences. Use "we" for the authors' choices. Hedge with words ("may", "we cannot"), and put the numbers in the next sentence.

### C6. A limited "all of the models" approach
- **Who:** T4 both, independently.
- **Recommendation of both:** neither recommends presenting every plausible model as a new genre. Both recommend:
  - an SI model atlas: a manifest of each model, its inputs, calibration and start, and a diagnostic table with unrun cells marked;
  - at most a compact main-text matrix of facts against models, if it replaces material;
  - one new column, the calibrated heterogeneous-growth world (C3);
  - for the sealed test, forecasts registered from every eligible frozen model, not only the hero. This is the owner's protocol decision.
  - The ablation ladder of 2026-10-12 stays in the SI or the linked explainer. It belongs to an earlier model generation and has its own composite index.
- **Their objections, which I share:**
  - **False comprehensiveness.** Ten homogeneous variants do not address the heterogeneous rival.
  - **Unequal information inputs.** A model given the ladder, one calibrated to hold it, and one started from equality face different tasks.
  - **Dependence among worlds.** The worlds share the estimator, the passing parts and the seeds.
- **Judgment.** Adopt the SI atlas and the column. Decide the main-text matrix after the restructure, on space.

  The owner's instinct is right in one specific way, which T4 Opus states best: the paper's strongest results are already comparative. They are geometry under identical dynamics, persistence against homogeneous worlds, and jumps and copying excluded. The hero model's distinct role is the counterfactuals.

  Presenting the hero as "the calibrated random-growth model", one column among others, costs nothing and answers the cherry-picking worry.

  Most of the benefit of the "all models" idea comes from making the selection rule and the failures visible, not from adding models.

### C7. Platform-literature boundaries (T1 GPT and T1 Opus)
These are interpretive boundaries the literature imposes. None weakens a result; all sharpen claims.
1. **Stable concentration with changing winners is documented at the household level** (Boik, Greenstein & Prince 2019). The paper must not present the coexistence as unrecognized. Its question is the aggregate hierarchy and its mechanism.
2. **Fast incumbent turnover is not universal online.** On Twitch, top streamer status is stable (Houssard et al. 2023). Present the geometry result as a measurement lesson: rank spacing changes apparent mobility. It is a reason cross-platform comparisons need harmonized estimands.
3. **Activity is not attention, success, income or power.** Karma and interactions measure engaged activity. Within-platform concentration is not platform power (Prat & Valletti 2022). "Rank is a poor measure of entrenchment" (l. 188) is too categorical: rank alone is insufficient to measure persistence of advantage.
4. **Leaving a top set, leaving the measured universe, removal by ban and migration are different events** (Ribeiro et al. 2021; Chandrasekharan et al. 2017). This matters because the draft names banned communities.
5. **2019–2022 contains a historical transition** (Veselovsky & Anderson 2023, Reddit through June 2022). It is a substantive rival to reading the window as a stationary regime: rival (c), named and cited.
6. **Persistence may summarize relations among communities** (TeBlunthuis 2025; TeBlunthuis & Hill 2022) or institutional development (TeBlunthuis, Shaw & Hill 2018), not only individual growth types.
7. **The platform-design paragraph (l. 188) must be conditional.** A platform change can act on volatility, entry, exit and dependence, not only on the slide (Nieborg & Poell 2018; Huszár et al. 2022; Bailo et al. 2021).
8. **The item-count decomposition locates variation in posting and commenting volume.** It does not identify a supply-side cause, because production responds to attention (Ciampaglia et al. 2015).
9. **Turnover rates are not platform constants.** Turnover of items is accelerating (Lorenz-Spreen 2019), and submissions fell below trend after 2020 (Veselovsky & Anderson 2023). The relative decline of submissions on the platform must be stated beside the head's fall in posting, or a referee will supply it.
10. **The neutral-copying world is one regime** (Evans & Giometto 2011), not "the neutral model".
11. **The question, and its qualitative answer, are not new** (Hindman 2018; Batty 2006; Iñiguez 2022). The contribution is to measure the answer, to separate what is automatic from what is not, and to find what moves the hierarchy.

## 2. The platform literature (T1): judgment

The two T1 reports are complementary.
- **T1 GPT** is strongest on governance, institutions and what the measures mean.
- **T1 Opus** is strongest on precedent and on the platform's own history in 2019–22.

### The paper's own precedent

**The single most consequential finding of the round** (T1 Opus; secondary hints in T1 GPT and T2 Opus) concerns Hindman 2018, *The Internet Trap*, ch. 5, "The Dynamics of Web Traffic" (with Bruce Rogers), pp. 82–101.

**What the chapter already does**, by T1 Opus's checks of Google Books snippets against page numbers:
- poses the paper's paradox: a stable structure with enormous instability for individual sites;
- proposes that "the funnel of churn actually produces and maintains the power law patterns" (p. 93);
- models daily Hitwise traffic for the top 300 sites (2005–08) with stochastic dynamical systems from finance, citing Fernholz (p. 199, n. 15);
- simulates from several initial conditions, including a flat one (pp. 95–99).

**It differs from the Reddit results in two ways:**
- smaller sites were much more volatile (pp. 9, 91–93);
- the top ten rarely changed (pp. 89, 99).

**The draft does not cite it.** Under single-blind review that is the most damaging omission a referee could find, and it is also the easiest to fix.

**Judgment.**
- Cite it in the introduction as the earlier answer to the same question.
- Say what this paper adds:
  - a rank-by-rank measurement of the balance with a tested crossing identity;
  - lasting movement separated from passing movement;
  - entry and exit;
  - null worlds with identical code;
  - persistence;
  - named communities.
- Turn the two differences into contrast sentences. They are informative:
  - **Size.** Web sites' volatility fell steeply with size, but Reddit communities' lasting volatility hardly does.
  - **The top.** On the web the top ten rarely changed, but on Reddit a quarter to a third of it is replaced each year.
- Only the author can say what the chapter's model was, and whether its size dependence was of lasting or of total variance. The calibration here shows passing swings barely move the raw exponent: planted 0 reads as 0.06. **Owner item 1.**

### Where the two T1 reports converge

Both reports independently name:
- Webster & Ksiazek 2012 (concentration and fragmentation);
- Lorenz-Spreen et al. 2019 (turnover of items is accelerating, including on Reddit);
- Veselovsky & Anderson 2023 (Reddit through COVID: the largest shock in its history; submissions fell below trend after 2020);
- TeBlunthuis, Shaw & Hill 2018 (rise and decline);
- Huszár et al. 2022, PNAS (amplification requires a counterfactual exposure comparison);
- Prat & Valletti 2022 (shares, not ranks, are the policy object);
- Nieborg & Poell 2018;
- the Reddit ban and migration studies (Chandrasekharan 2017; Horta Ribeiro 2021).

T1 Opus adds:
- **The turnover-profile literature:** Bentley 2007, which we already cite, plus Evans & Giometto 2011 and Acerbi & Bentley 2014. This converges with T2 Opus and T2 GPT, three reports in all. Cultural evolution reads a concave profile as anti-conformist copying, and R9 shows that geometry alone produces one.
- **The June 2020 ban wave:** Cima et al. 2024, nearly 2,000 communities banned.
- **Platform effects as the named rival (c):** Lazer et al. 2014; Malik & Pfeffer 2016; Wagner et al. 2021.
- **A change of instrument can reverse measured concentration:** Yang et al. 2020, PNAS.
- **The life-cycle literature on self-reinforcing decline:** Halfaker et al. 2013; Ribeiro 2014; Zhang et al. 2017; Xu & Peng 2024.
- **The distinction between Gibrat growth and strict cumulative advantage:** DiPrete & Eirich 2006, content unverified.

### Proposed references

**For the main text, about 12, replacing material, not adding a review:**

| reference | where it goes |
|---|---|
| Hindman 2018 | introduction, contrast sentences |
| Boik, Greenstein & Prince 2019 | introduction: the coexistence of stable allocation and changing destinations is documented at the household level |
| Webster & Ksiazek 2012 | introduction ¶1, with Yang et al. 2020 |
| Lorenz-Spreen et al. 2019 | introduction ¶1, beside Wu & Huberman: items speed up while their containers form a slow hierarchy |
| Fernholz & Fernholz 2020 | the balance, by equation number |
| Fernholz, Ichiba & Karatzas 2013 | persistence, as the name-specific extension |
| Acerbi & Bentley 2014 | turnover profiles |
| Veselovsky & Anderson 2023 | the surge, and the relative decline of submissions |
| Cima et al. 2024 | bans |
| Halfaker et al. 2013 or TeBlunthuis, Shaw & Hill 2018 | the life cycle |
| Lazer et al. 2014 | platform effects |
| Huszár et al. 2022 | the conditional design sentence |

**In the SI, about 15:**
- Houssard 2023 (Twitch, a stable top);
- Matias 2019 (PNAS);
- Ribeiro 2014;
- Zhang 2017;
- Xu & Peng 2024;
- Malik & Pfeffer 2016;
- Wagner 2021;
- Evans & Giometto 2011;
- Kalyuzhny 2015;
- Mallmin et al. 2024 and 2026;
- Hryshko 2012 and Baker 1997;
- Luttmer 2011;
- Prat & Valletti 2022;
- Nieborg & Poell 2018;
- Bailo 2021;
- Chandrasekharan 2017;
- Horta Ribeiro 2021;
- Ciampaglia 2015;
- Hofman 2021.

**Leave as background:** Strauss 2026, Cotter, Weng, Ciampaglia 2018, Rosen, the long-tail papers, Nelson, Pecile and Machado.

**Two signals of fit with PNAS:** the PNAS citations (Matias, Huszár, Yang, Gleeson) and Hindman's own books.

**Verification status.**
- The GPT quotations and key references were checked independently.
- The T1 Opus quotations (about 20, each under 15 words) were self-verified, not independently audited. The Hindman claims rest on Google Books snippets, so the author should check them.
- Four T1 Opus entries are labelled [memory, unverified] for content, and are not to be cited until read: DiPrete & Eirich, Brynjolfsson, Fleder & Hosanagar, Chandrasekharan 2022.

**The questions T1 GPT finds the literature asking:**
- what community success is;
- how governance shapes participation;
- who benefits from algorithmic distribution;
- at what level concentration is measured;
- whether regularities are autonomous dynamics or institutional outcomes;
- what exit means.

**The questions it proposes the literature should ask.** These are where the paper is strongest:
- when replacement of winners actually redistributes attention;
- how much apparent incumbent protection comes from the spacing of the hierarchy;
- when declining production initiates or responds to attention loss.

T1 GPT suggests making the first of these the introduction's governing question: when does changing membership replenish concentration, and when does concentration erode. **I agree.** Joined with C2, it gives the paper a question platform scholars recognize and an answer random-growth theorists can check.

**T1 Opus adds two questions the literature should ask, both of which the draft answers:**
- **What sets the steepness of an attention hierarchy, as distinct from who occupies it?** The concentration literature measures shares; the turnover literature measures churn. Only Hindman 2018 connects them.
- **Does a change in concentration come from production or from reception?** The draft's split of items against engagement per item answers this exactly, and the answer is undersold.

T1 Opus also names a strength the draft undersells: it studies populations of communities, not cases, which the online-community literature has asked for (TeBlunthuis 2018; Zhang 2017).

**On the deconcentration result.** T1 Opus argues the facts most striking to social-science readers are buried:
- the head lost a third of its share;
- the leaders kept rank one while their shares halved;
- the fall was in posting.

Put them in the fragmentation-versus-concentration debate in one sentence. I agree, provided C1 holds: no causal link to persistence.

## 3. Task by task: what I take and what I leave

### T2: methods (Opus more useful than GPT; complementary)

**Take:**
- C2–C4.
- **The Acerbi & Bentley sentence.** Cultural evolution reads a concave turnover profile as anti-conformist copying; our R9 shows the ladder's geometry produces it under identical dynamics. One sentence, and a contribution to another field.
- **A parameter-provenance table in the SI.** Every input with its moment and window; every output marked calibration or prediction, failures included. Both T2 reports ask for it.
- **A Taylor's-law reading of β, in one sentence.** β ≈ 0 means fluctuations proportional to size: attention to a community moves as a unit.
- **An explicit bridge from continuous theory to the measured panel** (T2 GPT item 4), now partly done by R11.

**Defer.** Future work, or the bridge data:
- the dimensionless phase-plane figure (it adds a figure);
- survival collapse;
- mobility indices;
- luck versus lasting position;
- platform interventions as drift shocks. These risk post hoc date choice; register them for 2023–24 instead.
- first passage of newcomers (too few events).

**Reject:** none outright. Both reports were disciplined.

### T3: exemplars (both strong; Opus's counts are the evidence base)

**Take:**
- C1, C5.
- Stakes sentences in the significance statement and introduction ¶1.
- Fewer numerals in the abstract: about four quantitative statements.
- Claim headings instead of "What/Where" headings.
- Define terms once ("ladder", "top set", "slide", "crossing", "world", which becomes "simulation").
- Name Gomez once.
- Fix the figure defects:
  - clipped titles in Figs. 3–5;
  - Fig. 5B's "gapped momentum";
  - Fig. 1A's axis label.
- Make the AI statement precise about figures: plotted by code written with AI coding assistants, not AI-generated images.

**Owner items:**
- **Title.** No "How…" title in 31 exemplars. That is weak evidence on its own, but the scope problem (C2) is real.
- **Classification.** A dual classification, for example Social Sciences with Applied Mathematics as in Juul & Ugander.
- **Suggested editors.** Population dynamics, network science, economics of random growth.

**Leave:**
- No-numeral abstracts entirely. Social-science readers expect magnitudes, and PNAS has no such rule.
- Lengthening the Discussion to 1,000 words. A 180-word rival-readings paragraph is enough.

**The prior templates (January–April 2026).** Both T3 reports independently say to retire their storyline: the post-API measurement problem, an invariant rank–share curve, the requirement to beat simpler models, a numeric anchor in every paragraph, and "for the first time". Some of the draft's density may come from following them. I agree.

### T4: one model or many

**Take:** C6.
- **Admissibility rules, from both reports:**
  - a stated rule for selecting models;
  - one estimand per comparison;
  - cells coded by the data's position in each world's 5–95% band;
  - unrun cells shown;
  - no composite score, ranking, averaging or "ensemble".
- **The diagnostic blocks of T4 GPT,** a good organizing scheme for the SI atlas: measurement and accounting; structure and geometry; growth process; prediction; long-run construction.
- **R10 into the SI's list of failures and post hoc analyses,** with its two failed expectations. T4 GPT asks for this; it is already in RESULTS and partly in the SI.

**Defer:**
- Iñiguez displacement–replacement and fitness–aging columns. Mark them "not run".
- A specification curve over analyst choices, limited to equivalent choices, for persistence only, in the SI if time allows.

**Leave:** Bayesian averaging and stacking. Both reports advise against it.

## 4. Decisions for the owner

0. ***The Internet Trap*, ch. 5 (with Bruce Rogers)** (§2). It must be cited as the earlier answer to the paper's question. What did its model assume? Was its size dependence of volatility measured on total or on lasting movement? Its two contrasts with Reddit go in the text: steep size dependence, and a stable top ten. Bruce Rogers's role may also bear on acknowledgements or authorship; that is the owner's call. The page numbers come from Google Books snippets of the 2020 paperback and should be checked against the edition cited.
1. **Title and framing (C2).** All options keep the owner's question. My preference is (a). It is accurate on both ladders and states the question and its two-part answer: random growth explains the durability, persistence explains the exception.
   - **(a)** "When the hierarchy of online attention outlasts its members".
   - **(b)** "Random growth and persistent decline in the hierarchy of online attention" (T3 Opus).
   - **(c)** "Turnover and persistence in the hierarchy of online attention" (T3 GPT).
   - **(d)** Keep v4's title, scoped in the significance statement's first sentence.
2. **The GLLM arc as the organizing story** (C2):
   - homogeneous random growth predicts a hierarchy that outlasts its members by about eight times;
   - comments behave that way;
   - the submissions head moved an order of magnitude faster, and persistent decline is the candidate channel.

   This changes claim language in the abstract, the significance statement and the Discussion.
3. **Main-text references.** About 12 new ones (§2). The SI takes the rest.
4. **Sealed-test protocol.** Register forecasts from every eligible frozen model (C6). This adds to the amendments already pending: P6 cannot fail under firm-like β; P5 spans the 2023 source switch and the blackout.
5. **Classification and suggested editors** (T3).
6. **Before any push:** the reports quote published articles at fair-use length, with attribution. The repository is public, so decide whether the eight reports should be pushed or kept local.

## 5. Plan for v5, in order

**Does not depend on owner decisions:**
1. **R11 into the manuscript.** Results 4, Fig. 4 and SI. "Flatter at every rank by 8–46%" becomes "flatter at the head of both ladders (37–41% at K/64) and through the submissions ladder; the comments body matches"; the comments collapse becomes −31%.
2. **C1, C4, C7 and the T3 style fixes:**
   - the causal bridge out;
   - precision at l. 93;
   - entrenchment and platform-design sentences made conditional;
   - activity versus attention made explicit;
   - terms defined;
   - figure defects fixed;
   - AI statement made precise.
3. **C5 restructure toward 7 pages.**
   - Balance first; geometry folded in.
   - Panels to the SI: 2A, 2C, 3C, 4D.
   - Persistence split in two.
   - A rival-readings paragraph.
   - About 6–8 new main-text references; the rest in the SI.
4. **C3, written in PLAN.md before running (R12),** with figures and text only after scoring:
   - the σ_β estimate;
   - the calibrated heterogeneous-growth world;
   - the recovery check.
5. **The SI atlas and parameter-provenance table** (C6).

**Needs the owner:** title and framing (decision 1–2), *The Internet Trap* sentence (3), and the protocol amendments (4).
