<!-- Provenance: Task 1 (platform literature) report by GPT-6 Astra (gpt-6-astra) (Codex CLI 0.163.0, codex --search exec, read-only sandbox, reasoning effort high), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

## A. Access check and search approach

I read both assignment files completely: `COMMON_BRIEF.md` and `T1_PLATFORM_LITERATURE.md`. I also read `paper/pnas_draft.tex`, `paper/pnas_si.tex`, `paper/references.bib`, the round-five `SYNTHESIS_R5.md`, `llm_fitting/PROGRAM_STATE.md`, and the concluding record section §2z-dk of `llm_fitting/MODEL_STATUS.md`. For governing instructions, I read the repository’s `AGENTS.md`, `CLAUDE.md`, and the relevant stochastic-modeling and model-status-authoring skill instructions.

I evaluated the manuscript through its LaTeX source. I did not inspect the rendered manuscript PDFs or figure images. No raw data or sealed directories were opened, no computations were run, and no files were written.

I used live web search, public full-text retrieval, and read-only local file access. Some publisher requests failed or returned access barriers; author manuscripts, institutional repositories, and PubMed Central supplied alternatives. The annotations distinguish full-text access from abstract-only access. In particular, I did not obtain the full text of Hindman’s book, Webster and Ksiazek’s article, or Guess and colleagues’ article. For Prat and Valletti, I read a working-paper version and checked the published article’s bibliographic record. For Lorenz-Spreen and colleagues, I read indexed full-text passages, although direct PDF retrieval failed.

**Model:** GPT-6 Astra, as designated for this session; exact backend build unavailable. **Interface:** Codex CLI. **Tools used:** live web search, public HTML/PDF retrieval, local shell reads. **Date:** 19 October 2026.

The search emphasized 2018–2026 and deliberately sought contrary evidence: prior coexistence of stable attention allocation and changing winners; durable incumbency on other platforms; and changes associated with platform governance, algorithms, and the pandemic. This is a targeted positioning review, not a systematic review with exhaustive coverage.

**Principal assessment:** The paper fits best as an account connecting the changing trajectories of communities to the persistence and movement of an aggregate hierarchy. Its framing should distinguish three objects throughout: **the distribution of activity, the identities occupying its upper ranks, and the institutions controlling its allocation**. The manuscript directly studies the first two. The third supplies plausible explanations and important limits, but is not identified by the present evidence.

## B. Annotated bibliography

The bibliography follows four substantive connections: attention allocation and concentration; community development and disappearance; platform governance and distribution; and production, popularity, and limited attention. This organization separates the outcomes the manuscript measures from the social mechanisms that could generate them.

There are 24 entries, including ten Tier 1 recommendations. “Must engage” does not mean every work needs a separate main-text sentence: several can be addressed together, and detailed comparisons belong in the SI. Cunha and colleagues is already cited; its inclusion concerns deeper engagement, not a missing reference. The existing citations to Kairam and colleagues, TeBlunthuis and Hill, Blumm and colleagues, and Iñiguez and colleagues should likewise be retained rather than presented as discoveries of this review.

### Theme 1. Attention allocation, concentration, and the persistence of winners

#### 1. Boik, Greenstein, and Prince: a close precedent for the opening puzzle

Boik, Andre, Shane Greenstein, and Jeffrey Prince. 2019. “The Persistence of Broadband User Behavior: Implications for Universal Service and Competition Policy.” *Telecommunications Policy* 43(8): 101820. [DOI](https://doi.org/10.1016/j.telpol.2019.04.004). **[full text: author manuscript] — Tier 1, must engage.**

This is the most consequential precedent found for the manuscript’s opening. Comparing household computer-use data from 2008 and 2013, the authors report substantial changes in leading websites and categories alongside stability in how households concentrate and allocate their attention. Their concentration measure concerns a household’s allocation across websites; it is not the aggregate rank–size distribution of communities. That distinction preserves the manuscript’s particular question, but the broader coexistence of stable attention allocation and changing destinations is established prior work.

**Action:** Cite in Introduction paragraph 1 or 2, immediately before sharpening the paper’s question. Add one sentence distinguishing household allocation from the aggregate hierarchy. Replace some of the opening’s mathematical genealogy with this substantive connection. Do not describe the coexistence itself as an unexplored paradox.

#### 2. Houssard and colleagues: concentrated attention can accompany durable incumbency

Houssard, A., F. Pilati, M. Tartari, P. L. Sacco, and R. Gallotti. 2023. “Monetization in Online Streaming Platforms: An Exploration of Inequalities in Twitch.tv.” *Scientific Reports* 13: 1103. [DOI](https://doi.org/10.1038/s41598-022-26727-5). **[full text] — Tier 1, must engage.**

Using Twitch revenue and audience information, this study reports pronounced inequality and substantial stability among leading streamers. Its ranking analysis explicitly engages the rank-dynamics literature already represented in the manuscript. It therefore supplies a useful counterexample to any general proposition that online leaders necessarily turn over rapidly.

The comparison is not numerically direct: creator selection, revenue versus audience measures, thresholds, and observation horizons differ. The authors’ explanation in terms of adaptation is also stronger than their observational design alone establishes.

**Action:** Qualify the Introduction’s platform-wide language and add a short Discussion comparison: concentrated systems can exhibit different degrees of incumbent persistence. In the SI, identify the unit, metric, selection rule, and horizon before comparing turnover. This is a boundary case to explain, not an inconsistency to dismiss.

#### 3. Lorenz-Spreen and colleagues: distinguish topic acceleration from community decline

Lorenz-Spreen, Philipp, Bjarke Mørch Mønsted, Philipp Hövel, and Sune Lehmann. 2019. “Accelerating Dynamics of Collective Attention.” *Nature Communications* 10: 1759. [DOI](https://doi.org/10.1038/s41467-019-09311-w). **[full text: indexed Results and Discussion passages; direct PDF access unsuccessful] — Tier 1, must engage.**

This article studies changing attention trajectories across cultural and online domains. In several settings, popularity rises and falls more rapidly over historical time. A model of competing topics interprets those changes through production, consumption, and saturation. The relevant object is the trajectory of content or topics, not the continuing activity of an organization or community.

It precedes a broad claim that online attention has rapid turnover, while helping articulate what the current paper adds: a community can continually replace its content while maintaining, gaining, or losing its aggregate position.

**Action:** Pair this citation with the existing Wu–Huberman citation in Introduction paragraph 1. State explicitly that short content lifetimes do not determine the lifetime of the communities producing or circulating that content. Avoid implying acceleration is universal across every domain examined.

#### 4. Webster and Ksiazek: concentration does not reveal audience separation

Webster, James G., and Thomas B. Ksiazek. 2012. “The Dynamics of Audience Fragmentation: Public Attention in an Age of Digital Media.” *Journal of Communication* 62(1): 39–56. [DOI](https://doi.org/10.1111/j.1460-2466.2011.01616.x). **[abstract only] — Tier 2, should cite.**

The article’s abstract reports extensive audience duplication across television and internet outlets and argues for audience-centered analysis of fragmentation. It is foundational here because an outlet-level distribution does not establish whether people occupy separate informational worlds. The same people can distribute activity across numerous communities, while different people can generate similar aggregate rankings.

This qualifies the manuscript’s broad language about attention and its possible implications for public discourse. Neither a steep hierarchy nor rapid community replacement measures audience isolation, diversity of individual exposure, or polarization.

**Action:** Add a compact limitation in Discussion paragraph 3: community-level activity does not identify audience overlap or individual exposure. A longer discussion belongs in the SI. The full article should be checked before attributing details beyond the abstract.

#### 5. Hindman: connect concentration to the economics of attracting and retaining audiences

Hindman, Matthew. 2018. *The Internet Trap: How the Digital Economy Builds Monopolies and Undermines Democracy*. Princeton University Press. [Stable book DOI](https://doi.org/10.2307/j.ctv36zrf8). **[abstract only: book description and chapter abstracts] — Tier 2, should cite.**

The accessible descriptions place audience concentration, accumulated advantages, and the economics of attracting and retaining users at the center of the book. That makes it directly relevant to the paper’s opening question about durable inequality. It also highlights a distinction the manuscript should preserve: instability among particular communities need not negate durable advantages in audience acquisition or concentration among the firms hosting them.

My access does not support detailed claims about the book’s empirical estimates or individual mechanisms.

**Action:** Consider one Introduction citation linking the observed hierarchy to substantive attention-economy scholarship. Explain which level the paper studies rather than implying that community turnover refutes concentration arguments. Treat this as ordinary prior scholarship: the author’s identity neither establishes its relevance nor excuses omitting a substantive connection.

#### 6. Prat and Valletti: platform concentration is a different economic object

Prat, Andrea, and Tommaso Valletti. 2022. “Attention Oligopoly.” *American Economic Journal: Microeconomics* 14(3): 530–557. [DOI](https://doi.org/10.1257/mic.20200134). **[full text: December 2019 working-paper version; published abstract checked] — Tier 2, should cite.**

The model treats platforms as intermediaries selling access to consumer attention. Its welfare implications depend on the organization of those intermediaries and on patterns of user overlap, not simply aggregate usage shares. This is especially useful for preventing a slide from inequality among communities to market power among platforms.

The manuscript’s concentration measure could change substantially while the ownership and control of the attention infrastructure remain unchanged. Conversely, platform consolidation could matter without visibly changing the internal rank–size curve.

**Action:** Cite in the Discussion’s entrenchment/platform-design paragraph if the authors retain implications for platform power. Add a sentence separating within-platform activity concentration from concentration among attention intermediaries. The current data cannot adjudicate merger, advertising-price, or consumer-welfare questions.

#### 7. Strauss, Yang, and Mazzucato: distinguish activity inequality from creator earnings

Strauss, Ilan, Jangho Yang, and Mariana Mazzucato. 2026. “‘Rich-get-richer’? Platform Attention and Earnings Inequality Using Patreon Earnings Data.” *Industrial and Corporate Change*, advance publication, dtag044. Published 23 July 2026. [DOI](https://doi.org/10.1093/icc/dtag044). **[full text: Introduction and interpretation sections] — Tier 2, should cite.**

This recent article examines Patreon earnings distributions across creators affiliated with different platforms. Its relevance is the connection—and imperfect mapping—between attention, monetization, and inequality. The authors explicitly acknowledge that their cross-sectional comparisons cannot separate algorithmic allocation from creator selection, audience composition, or conversion practices.

It provides a contemporary creator-economy counterpart to the manuscript, while also warning against treating one outcome as a transparent proxy for another. Falling activity concentration need not imply falling income inequality.

**Action:** Use in an SI positioning paragraph or a brief Discussion boundary statement. Describe the present outcome as concentration in recorded engagement or activity, not economic rewards. Do not import the article’s causal language about algorithms without its accompanying qualifications.

### Theme 2. Community development, persistence, governance, and disappearance

#### 8. TeBlunthuis: community dynamics occur within an interacting ecosystem

TeBlunthuis, Nathan. 2025. “Niche Dynamics in Complex Online Community Ecosystems.” *Proceedings of the International AAAI Conference on Web and Social Media* 19(1): 1880–1892. [DOI](https://doi.org/10.1609/icwsm.v19i1.35907). **[full text] — Tier 1, must engage.**

This study examines changing relationships among thousands of Reddit communities using a nonlinear dynamical approach. It finds patterns interpreted as competition and mutualism and investigates their relationship to community overlap. Its central contribution to the manuscript’s framing is relational: a community’s trajectory may depend on the activities and audiences of other communities, not just its own rank or an enduring individual growth tendency.

The inference is observational; the paper does not make ecological labels equivalent to experimentally identified causal effects.

**Action:** Engage this alongside the already-cited TeBlunthuis–Hill article in the persistent-decline subsection. One sentence should identify changing intercommunity relations as an unresolved source of growth dependence. Present the rank model as a description of aggregate trajectories that could result from such relations, rather than as a competing social explanation that excludes them.

#### 9. Veselovsky and Anderson: the observation period contains a substantive historical transition

Veselovsky, Veniamin, and Ashton Anderson. 2023. “Reddit in the Time of COVID.” *Proceedings of the International AAAI Conference on Web and Social Media* 17(1): 878–889. [DOI](https://doi.org/10.1609/icwsm.v17i1.22196). **[full text] — Tier 1, must engage.**

This is unusually well matched to the manuscript’s platform and period. It separates structural from content change and platform-level from user-level change, examining Reddit through June 2022. The study reports a mixture of lasting and temporary departures from prior trajectories, with differences among submissions, comments, communities, and user cohorts.

Its counterfactual forecasts depend on extrapolated pre-pandemic trends; they do not by themselves identify every pandemic effect. Nevertheless, the article makes historically specific change a substantive rival to interpreting 2019–2022 as an ordinary stationary regime.

**Action:** Cite in the persistent-decline subsection and beside the SI discussion of the 2020 surge. Explain what the manuscript’s existing adjustments address and what they leave unresolved. Do not claim that this study proves the pandemic caused the observed head decline.

#### 10. Cunha and colleagues: community success has several non-equivalent meanings

Cunha, Tiago, David Jurgens, Chenhao Tan, and Daniel M. Romero. 2019. “Are All Successful Communities Alike? Characterizing and Predicting the Success of Online Communities.” *Proceedings of The Web Conference 2019*: 318–328. [DOI](https://doi.org/10.1145/3308558.3313689). **[full text] — Tier 2, should engage more deeply; already cited.**

The authors distinguish community growth, retention, survival, and activity, showing that these measures and their predictors do not coincide. This directly qualifies any interpretation of lower posting or karma as community failure. It also distinguishes predicting future community outcomes from reproducing a cross-sectional hierarchy.

The current manuscript cites this literature in passing but could use it to define its outcome more carefully. Its item-count decomposition measures one dimension of community activity; it does not establish a reduction in distinct contributors or the loss of community value.

**Action:** Revise the persistent-decline subsection’s terminology and retain this citation there. Use “posting and commenting activity” when the evidence is counts. Reserve “participation” for a clearly defined operational meaning, and avoid “failure” or “death” unless an actual survival criterion is supplied.

#### 11. TeBlunthuis, Shaw, and Hill: institutional development can accompany decline

TeBlunthuis, Nathan, Aaron Shaw, and Benjamin Mako Hill. 2018. “Revisiting ‘The Rise and Decline’ in a Population of Peer Production Projects.” *Proceedings of the 2018 CHI Conference on Human Factors in Computing Systems*, Paper 355: 1–7. [DOI](https://doi.org/10.1145/3173574.3173929). **[full text] — Tier 2, should cite.**

Studying Wikia communities, the authors find patterns linking community maturation, newcomer retention, rejection, and the increasing difficulty of changing established norms. They explicitly caution against causal conclusions from the observational design.

This supplies a substantive interpretation of persistent activity differences that is absent from a purely stochastic description: communities may develop institutions that affect recruitment and retention. It also shows why fixed differences between communities and changes within communities are not merely statistical alternatives; they correspond to different social histories.

**Action:** Add this to a short SI paragraph listing plausible sources of persistent growth differences. In the main text, one grouped citation can establish that organizational development is a candidate explanation. The current aggregate panels cannot test newcomer rejection or institutional rigidity without additional measures.

#### 12. Matias: governance can increase participation

Matias, J. Nathan. 2019. “Preventing Harassment and Increasing Group Participation through Social Norms in 2,190 Online Science Discussions.” *Proceedings of the National Academy of Sciences* 116(20): 9785–9789. [DOI](https://doi.org/10.1073/pnas.1813486116). **[full text] — Tier 1, must engage.**

A randomized field experiment in Reddit’s science community finds that displaying community rules improved newcomer compliance and increased newcomer participation. This is stronger causal evidence about a governance intervention than the manuscript’s aggregate trajectories can provide. It establishes that participation is responsive to institutional conditions, without implying that the same intervention explains long-run community decline.

The direction matters: governance is not simply a source of suppression, bans, or reduced activity. It can make participation more attractive.

**Action:** Cite in the Discussion’s account of possible mechanisms behind activity trajectories, alongside evidence about platform distribution. Replace any implicit contrast between intrinsic stochastic dynamics and externally imposed decline with a statement that local rules and platform arrangements can influence the measured process.

#### 13. Ribeiro and colleagues: exit from Reddit need not mean disappearance

Ribeiro, Manoel Horta, Shagun Jhaver, Savvas Zannettou, Jeremy Blackburn, Gianluca Stringhini, Emiliano De Cristofaro, and Robert West. 2021. “Do Platform Migrations Compromise Content Moderation? Evidence from r/The_Donald and r/Incels.” *Proceedings of the ACM on Human-Computer Interaction* 5(CSCW2), Article 316: 1–24. [DOI](https://doi.org/10.1145/3476057). **[full text] — Tier 1, must engage.**

This article follows communities that moved from Reddit to independent sites. It examines changes in activity, recruitment, and harmful behavior, finding that these outcomes need not move together. Its direct relevance is the meaning of disappearance: an endpoint can leave a platform’s measured universe while some community activity continues elsewhere.

This matters especially because the manuscript names communities affected by moderation. Those cases should not silently become examples of ordinary competitive decline.

**Action:** Add a brief qualification beside the named-community examples in the first Results subsection. In the SI, distinguish leaving a top set, leaving the measured universe, platform removal, and migration. The existing aggregate panels can describe the first two; they cannot follow people or activity across sites.

#### 14. Chandrasekharan and colleagues: bans alter behavior as well as endpoint availability

Chandrasekharan, Eshwar, Umashanthi Pavalanathan, Anirudh Srinivasan, Adam Glynn, Jacob Eisenstein, and Eric Gilbert. 2017. “You Can’t Stay Here: The Efficacy of Reddit’s 2015 Ban Examined Through Hate Speech.” *Proceedings of the ACM on Human-Computer Interaction* 1(CSCW), Article 31: 1–22. [DOI](https://doi.org/10.1145/3134666). **[full text] — Tier 2, should cite.**

This foundational moderation study uses observational causal-inference methods to examine Reddit bans and subsequent user behavior. It finds reductions in hateful activity among remaining users and does not find the feared corresponding increase in hate speech in recipient subreddits.

Its relevance is not to attribute the manuscript’s aggregate results to bans. Rather, it demonstrates that removal is a substantive intervention with several possible consequences, not a generic draw from an undifferentiated exit process.

**Action:** Cite with Ribeiro and colleagues in the SI’s exit terminology or beside the banned-community examples. Explain that the entry/exit accounting term records consequences within the observation system; it does not identify their institutional causes or evaluate moderation success.

### Theme 3. Platform governance, algorithmic distribution, and producer adaptation

#### 15. Nieborg and Poell: platform arrangements are changing institutions

Nieborg, David B., and Thomas Poell. 2018. “The Platformization of Cultural Production: Theorizing the Contingent Cultural Commodity.” *New Media & Society* 20(11): 4275–4292. [DOI](https://doi.org/10.1177/1461444818769694). **[full text] — Tier 1, must engage.**

This conceptual article connects cultural production to platform markets, infrastructures, and governance. Its examples from news and games emphasize how producers adapt to changing distribution and monetization arrangements. It supplies the strongest general correction to treating the observed environment as an institutionally fixed container.

A rank-based description can remain useful under this perspective. The issue is interpretation: its drift, volatility, and entry processes summarize activity generated under particular arrangements, rather than revealing an autonomous mechanism insulated from them.

**Action:** Use one sentence in the Discussion’s platform-design paragraph to make institutional contingency explicit. Platform changes can affect several measured ingredients, not only relative decline. Move any extended platform-theory discussion to the SI; the main text needs a boundary, not a new theoretical section.

#### 16. Bailo, Meese, and Hurcombe: platform change, production, and engagement do not move in lockstep

Bailo, Francesco, James Meese, and Edward Hurcombe. 2021. “The Institutional Impacts of Algorithmic Distribution: Facebook and the Australian News Media.” *Social Media + Society* 7(2). [DOI](https://doi.org/10.1177/20563051211024963). **[full text] — Tier 1, must engage.**

Using Facebook engagement, posting, and referral information, this study traces heterogeneous changes across Australian news organizations. Important declines preceded the prominent 2018 algorithm change; some categories behaved differently, and posting responses varied. The authors explicitly state that their study does not identify causal effects of algorithm changes.

This is a particularly useful challenge to both an intrinsic-dynamics narrative and an overly simple platform-shock narrative. Slow decline can involve changing institutional relationships and producer strategies without a single clean discontinuity. The article also recognizes that CrowdTangle engagement does not directly measure reach.

**Action:** Cite in the Discussion and the Facebook measurement limitations. Use it to motivate the distinction among content supply, engagement per item, exposure, and referral traffic. Do not treat post volume as an exogenous supply variable.

#### 17. Huszár and colleagues: algorithmic amplification requires a counterfactual exposure comparison

Huszár, Ferenc, Sofia Ira Ktena, Conor O’Brien, Luca Belli, Andrew Schlaikjer, and Moritz Hardt. 2022. “Algorithmic Amplification of Politics on Twitter.” *Proceedings of the National Academy of Sciences* 119(1): e2025334119. [DOI](https://doi.org/10.1073/pnas.2025334119). **[full text] — Tier 1, must engage.**

The study compares algorithmically ranked and chronological timelines using a long-running randomized holdback. It measures differential exposure to political content and shows why amplification depends on the level of aggregation. The authors also discuss limits arising from the experimental environment and the interpretation of causal mechanisms.

For this manuscript, the key lesson is evidentiary. A ranking of communities by realized engagement is not a measure of the recommender system’s ranking decisions. Observed inequality therefore cannot establish algorithmic favoritism or its absence.

**Action:** Cite where the Discussion moves from measured dynamics to platform design. Replace causal-sounding statements about distribution changes with conditional implications. Explicitly separate statistical rank in the paper from algorithmic placement in a user’s feed.

#### 18. Bandy and Diakopoulos: the apparent impact of platform interventions depends on the outcome

Bandy, Jack, and Nicholas Diakopoulos. 2023. “Facebook’s News Feed Algorithm and the 2020 US Election.” *Social Media + Society* 9(3). [DOI](https://doi.org/10.1177/20563051231196898). **[full text] — Tier 2, should cite.**

This article studies Facebook’s contribution to news audiences using desktop traffic and platform URL-view data. It reports mixed changes associated with the post-election intervention and substantial differences between short-run referrals, audience reach, and longer-run visibility. Its observational comparisons do not furnish a general experimental estimate of Facebook’s influence.

The manuscript studies overlapping calendar years but a different outcome. This paper helps prevent treating aggregate engagement as interchangeable with reach or the incremental audience supplied by a platform.

**Action:** Use in the SI’s measurement discussion, paired with Bailo and colleagues. If discussing policy changes, specify the outcome and horizon the model actually describes. The smallest useful addition is a sentence stating that engagement concentration does not directly measure platform-contributed exposure.

#### 19. Guess and colleagues: exposure and activity changes do not automatically imply attitudinal change

Guess, Andrew M., et al. 2023. “How Do Social Media Feed Algorithms Affect Attitudes and Behavior in an Election Campaign?” *Science* 381(6656): 398–404. [DOI](https://doi.org/10.1126/science.abp9364). **[abstract only: institutional publication record] — Tier 2, should cite if democratic implications remain.**

The abstract reports an experiment replacing default Facebook and Instagram feeds with reverse-chronological feeds. Activity, time spent, and content exposure changed, while the measured political attitudes did not significantly change over the study period. The result should not be generalized to all algorithms, outcomes, or longer horizons.

It is relevant because the manuscript’s broad audience may connect attention concentration to political consequences. That connection needs an additional chain of evidence.

**Action:** A Discussion boundary sentence is sufficient: the paper measures the distribution and persistence of engaged activity, not downstream changes in beliefs or political behavior. If the manuscript makes no such broader implications, this citation can remain background rather than consume main-text space.

#### 20. Cotter: producers respond strategically to perceived algorithms

Cotter, Kelley. 2019. “Playing the Visibility Game: How Digital Influencers and Algorithms Negotiate Influence on Instagram.” *New Media & Society* 21(4): 895–913. [DOI](https://doi.org/10.1177/1461444818815684). **[full text] — Tier 2, should cite.**

Through qualitative analysis of influencer discussions, Cotter examines how creators interpret algorithmic visibility and adjust their practices. The evidence concerns creators’ understandings and strategies; it does not reveal the platform’s underlying code or estimate a causal effect of a specific ranking rule.

Its relevance is the feedback between visibility and production. A reduction in posting can be a response to expected returns or changing distribution conditions, rather than an independent reduction in willingness to participate.

**Action:** Cite with Bailo and colleagues when interpreting the item-count decomposition. Describe it as locating the variation in content production, while leaving the causes of production change unresolved. This use does not require modeling Instagram or expanding its role beyond the manuscript’s negative control.

### Theme 4. Production, popularity, and competition for attention

#### 21. Ciampaglia, Flammini, and Menczer: production can follow attention demand

Ciampaglia, Giovanni Luca, Alessandro Flammini, and Filippo Menczer. 2015. “The Production of Information in the Attention Economy.” *Scientific Reports* 5: 9452. [DOI](https://doi.org/10.1038/srep09452). **[full text] — Tier 2, should cite.**

Using Wikipedia article creation and traffic to related pages, the authors find a temporal association between demand for information and subsequent production. They explicitly leave causal identification open. This is foundational for the manuscript’s interpretation of activity counts: observed content production can respond to prior attention, common events, or anticipated audience demand.

The item-count/per-item decomposition remains informative, but it does not by itself separate supply from demand in a causal economic sense.

**Action:** Cite in the final Results subsection or an SI interpretation note. Replace any suggestion that the decomposition identifies a supply-side cause with the narrower claim that persistent variation is concentrated in posting and commenting volume. Keep the more ambitious causal question open.

#### 22. Candia and colleagues: decline depends on the object and its age

Candia, Cristian, C. Jara-Figueroa, Carlos Rodriguez-Sickert, Albert-László Barabási, and César A. Hidalgo. 2019. “The Universal Decay of Collective Memory and Attention.” *Nature Human Behaviour* 3: 82–91. [DOI](https://doi.org/10.1038/s41562-018-0474-5). **[full text: relevant modeling and interpretation sections] — Tier 2, should cite.**

This article studies temporal decline in attention to cultural objects and distinguishes components of collective memory. Its treatment of preferential attachment and aging underscores that popularity growth and temporal decay can coexist.

For the present paper, this is primarily a distinction to make, not a ready-made explanation. A community continuously produces new items; its aggregate decline is not equivalent to the aging of one item. Community age, content age, and persistent differences in community growth should remain separate concepts.

**Action:** Place beside Lorenz-Spreen and the existing Wu–Huberman citation, preferably in a compact SI positioning paragraph. State that the manuscript studies the continuing producers or venues of content, not just decay after publication. Do not claim that positive gapped correlations identify a universal aging process.

#### 23. Ciampaglia and colleagues: popularity and quality are different outcomes

Ciampaglia, Giovanni Luca, Azadeh Nematzadeh, Filippo Menczer, and Alessandro Flammini. 2018. “How Algorithmic Popularity Bias Hinders or Promotes Quality.” *Scientific Reports* 8: 15951. [DOI](https://doi.org/10.1038/s41598-018-34203-2). **[full text] — Tier 3, useful background.**

This model examines conditions under which popularity-based selection can improve or impair the relationship between visibility and quality. It is useful because neither concentration nor equality supplies its own welfare interpretation. A flatter distribution could spread attention across valuable alternatives, low-quality material, or both.

The manuscript has no independent quality measure, so this literature should discipline interpretation rather than generate another empirical task.

**Action:** If the Discussion retains normative language about concentration, add a brief acknowledgment that concentration and content quality are distinct. Otherwise, keep this as background for the authors rather than adding another main-text citation.

#### 24. Weng and colleagues: a neutral comparator is not the entire limited-attention tradition

Weng, Li, Alessandro Flammini, Alessandro Vespignani, and Filippo Menczer. 2012. “Competition among Memes in a World with Limited Attention.” *Scientific Reports* 2: 335. [DOI](https://doi.org/10.1038/srep00335). **[full text] — Tier 3, useful background.**

The authors show that network structure and finite attention can generate heterogeneous meme popularity and persistence without differences in intrinsic meme quality. This foundational model belongs near the manuscript’s neutral-copying comparison, but its state variables and mechanisms differ from independent community growth.

The manuscript can reject or distinguish the particular simulated comparator it implements. It cannot thereby reject every model of neutral competition, network-mediated diffusion, or scarce attention.

**Action:** Add an SI sentence defining the scope of the neutral-copying comparison. Preserve the current model-specific language and avoid converting a failed comparator into a rejection of an entire substantive tradition. No additional simulation is necessary merely to make that boundary clear.

## C. The questions the literature asks

The following questions are an interpretive synthesis, not a frequency-ranked survey. For the “should ask” and “implied” categories, the quotations establish the neighboring scholarly problem; they do not prove that the proposed question has never been asked elsewhere.

Quotation locations refer to the retrieved version. Author manuscripts are identified explicitly when their pagination differs from the published article.

### C1. Common questions

#### 1. What makes an online community successful, and which outcomes persist?

> “success is multi-faceted and cannot be measured nor predicted by a single measurement.”

Cunha et al., abstract, author PDF p. 1. **[verified: [source PDF](https://arxiv.org/pdf/1903.07724)]**

The authors compare growth, retention, survival, and activity rather than treating these as interchangeable manifestations of success. Their question includes prediction: which early community characteristics anticipate which later outcomes?

**Relation to the draft:** The manuscript answers a narrower question about the persistence and distribution of recorded activity. It does not measure whether members are satisfied, newcomers are retained, or communities fulfill their purposes. Its present panels can support comparisons among item volume, per-item engagement, relative share, and rank, but not reconstruct distinct-person retention from aggregate counts.

**Framing action:** In “What homogeneous random growth lacks,” define the outcome before interpreting it. “Persistent changes in posting and commenting activity” is more precise than an unqualified “persistent decline in participation.” State that activity is one dimension of community development. This makes the connection to HCI/CSCW substantive while avoiding an unsupported equation between losing share and failing as a community.

#### 2. How do community rules influence who participates?

> “How do a community’s rules about behavior influence people’s decisions to participate in an online conversation”

Matias, opening paragraph, author PDF p. 1; excerpt from the opening question. **[verified: [source PDF](https://natematias.com/media/JNM-Preventing-Harassment-PNAS-2019.pdf)]**

The surrounding discussion asks how norms affect both selection into conversations and subsequent behavior. The experiment demonstrates that participation can respond to a concrete governance intervention.

**Relation to the draft:** The manuscript cannot answer this causal question. Counts reveal how much posting or commenting occurred, not why people entered or avoided a discussion. Nor does a long-lived decline establish worsening moderation, harassment, or exclusion. Those explanations require measures absent from the aggregate panels.

**Framing action:** Treat governance as a plausible input into the measured trajectories. In Discussion paragraph 2, add a short grouped sentence recognizing that rules and distribution arrangements may change participation and its persistence. This prevents the stochastic vocabulary from making social decisions disappear. It also avoids presenting moderation only through bans: governance can support activity as well as constrain it.

#### 3. Which groups benefit from algorithmic distribution, and why?

> “understanding the precise causal mechanism that drives amplification invites further study”

Huszár et al., Discussion. **[verified: [full text](https://pmc.ncbi.nlm.nih.gov/articles/PMC8740571/)]**

The study compares exposure under algorithmic and chronological timelines. Even with that intervention, the authors distinguish measured amplification from the reasons different political groups receive different amplification.

**Relation to the draft:** The manuscript cannot answer which groups benefit from the recommender system. It observes realized activity, with no randomized distribution rule and no feed-level exposure counterfactual. Its “rank” is an analyst’s ordering of communities, not the sequence presented to a user.

The existing data can describe changes associated with calendar periods, but such associations would remain vulnerable to common shocks and changing measurement.

**Framing action:** Keep platform design as a conditional application. State that the framework could describe how an independently identified intervention changes the hierarchy, but the present analysis does not identify such an intervention. This is a stronger connection to algorithmic research than suggesting that a descriptive balance already explains platform allocation decisions.

### C2. Meta-questions and implicit questions

#### 4. At what level is concentration being measured, and whose power does it represent?

> “A merger assessment that relies on aggregate platform usage alone can be highly biased.”

Prat and Valletti, abstract, December 2019 working paper p. 1, preceding the 2022 published article. **[verified: [working-paper PDF](https://www.cresse.info/wp-content/uploads/2020/08/2020_AttentionOligopoly.pdf)]**

The model’s economic implications depend on how individual users allocate attention across intermediaries. Aggregate platform size alone omits information important to the modeled bottleneck.

**Relation to the draft:** The paper answers a within-platform distribution question. It does not measure concentration of platform ownership, the ability to charge advertisers, audience overlap, or dependence on a particular intermediary. A subreddit can lose share while its host remains equally powerful.

**Framing action:** Define three distinct meanings of durability: durable inequality, durable incumbent position, and durable control over allocation. The first two belong in the main empirical argument. The third should appear as a limitation or application requiring additional evidence.

This is particularly important for the word “entrenchment.” The manuscript’s rank/share comparison can challenge one measure of incumbent persistence; it cannot establish the absence of institutional or economic entrenchment.

#### 5. Are platform regularities autonomous dynamics or outcomes of changing institutions?

> “Their holding companies constantly adjust platform technologies, business models, and governance structures.”

Nieborg and Poell, p. 4278. **[verified: [source PDF](https://gamespace.nl/content/NIEBORG_POELL_2018_NMS.pdf)]**

The authors argue that platforms themselves evolve, while cultural producers adapt to the resulting arrangements. The institutional environment is part of the process being explained.

**Relation to the draft:** The draft can estimate and compare regularities within its observed environment. It cannot determine which institutional arrangements generated them or establish that the measured coefficients would survive a policy change. The model’s success from equality is a conditional generative result, not evidence that institutions are irrelevant.

**Framing action:** Describe the model as an aggregate account under observed conditions. In the Discussion, replace the proposition that a distribution change acts through relative decline alone with a broader statement: governance can alter drift, variability, entry, and dependence among communities.

This does not require abandoning the stochastic model. It clarifies that mathematical regularity and institutional explanation address different levels of the same phenomenon.

#### 6. What constitutes the system boundary, and what does exit mean?

> “Have the communities retained their activity levels and their capacity to attract new members following the migration to a new platform?”

Ribeiro et al., RQ1, p. 316:3. **[verified: [source PDF](https://shagunjhaver.com/research/articles/ribeiro-2021-migrations/ribeiro-2021-migrations.pdf)]**

The question follows communities beyond Reddit and distinguishes continued activity from recruitment. It makes platform departure an event to investigate rather than an automatic endpoint of community existence.

**Relation to the draft:** The paper can identify movement relative to its measured universe and account for entry and exit within that universe. It cannot establish that activity disappeared from the internet or that the same people stopped participating. The named banned communities make this limitation concrete.

**Framing action:** In the first Results subsection, distinguish loss of rank from loss of platform availability. In the SI, use separate terms for top-set exit, measured-universe exit, removal, and migration.

The distinction also improves the interpretation of replenishment: a community newly entering a top set is not necessarily a newly created community, and its contributors are not necessarily new users.

### C3. Questions the literature should ask more explicitly

These are proposed comparative questions, not claims of an empty literature.

#### 7. Under what conditions does replacement of winners actually redistribute attention?

> “we observe drastic turnover among the top 20 web sites”

Boik et al., Introduction, author manuscript p. 3. **[verified: [source PDF](https://www.hbs.edu/ris/Publication%20Files/The%20Persistence%20of%20Broadband%20User%20Behavior_793969c4-0505-4d04-9e81-06b8990249ec.pdf)]**

The surrounding analysis contrasts changing destinations with stable household allocation patterns. It motivates asking when replacement changes the distribution itself.

**Why it matters:** Turnover can look like democratization even when the same concentration structure is reproduced. Conversely, a familiar leader can retain first place while losing a large share.

**Relation to the draft:** This is one of the questions the manuscript addresses most directly. Its distinction among membership turnover, rank movement, share loss, and shape change is substantive, not merely methodological. The comments/submissions difference is especially valuable because replacement does not have the same relation to hierarchy change in both.

**Framing action:** Make this the Introduction’s governing question. The smallest change is to replace a broad novelty-sounding puzzle with a sharper conditional one: when does changing membership replenish concentration, and when does concentration erode? The model and accounting then answer a recognizable attention-economy question.

#### 8. How much apparent incumbent protection comes from the spacing of the hierarchy?

> “A top streamer status, in addition, is very stable.”

Houssard et al., Results, “Platform trends and viewership.” **[verified: [full text](https://www.nature.com/articles/s41598-022-26727-5)]**

The authors interpret Twitch’s upper ranks as relatively durable. Their measurements motivate comparisons with systems where the head appears more fluid.

**Why it matters:** A rank change has different implications when neighboring actors are close together than when they are separated by large activity gaps. Cross-platform comparisons of mobility can therefore mix dynamics with geometry.

**Relation to the draft:** The manuscript directly addresses this issue within its observed ladders and simulated comparisons. It could explain the comparison more clearly using existing results. It cannot establish that geometry explains the Twitch findings without harmonized data and estimands.

**Framing action:** Present the geometry result as a caution for measuring entrenchment across settings. Add a compact SI comparison specifying rank spacing, threshold, horizon, and activity metric. Do not equate faster top-set replacement with a universal failure of size to protect incumbents.

#### 9. When does declining content production initiate attention loss, and when does it respond to it?

> “Whether there is a hard causal link between demand and supply remains an open question.”

Ciampaglia, Flammini, and Menczer, Discussion, “Supply and demand of information.” **[verified: [full text](https://pmc.ncbi.nlm.nih.gov/articles/PMC4437024/)]**

Their temporal evidence connects attention around existing Wikipedia pages to the creation of related information, while explicitly allowing common causes and alternative directions.

**Why it matters:** Aggregate engagement can decline because fewer items are produced, because items attract less engagement, or through feedback between the two. Locating variation in item counts is informative without identifying the initiating cause.

**Relation to the draft:** The paper answers the accounting part with existing data. It cannot identify whether production changes originate in contributor motivation, audience demand, moderation, algorithmic distribution, or their interaction. Lagged aggregate comparisons would not automatically solve that problem.

**Framing action:** Highlight the item-count decomposition as a substantive result, but call it a decomposition of activity. In the abstract, significance statement, and concluding Results paragraph, avoid language that turns this location of variation into a demonstrated causal channel linking persistent decline to the head’s loss.

### C4. Important questions implied by the literature

#### 10. Does measured persistence describe an enduring process or a historical transition?

> “Did the processes governing online platforms before COVID-19 continue throughout the pandemic, or did a new organization emerge?”

Veselovsky and Anderson, Introduction, author PDF p. 1. **[verified: [source PDF](https://www.cs.utoronto.ca/~ashton/pubs/reddit-covid-icwsm23.pdf)]**

Their framework separates temporary increases from changes that remain after the initial disruption, and distinguishes platform-wide outcomes from user-cohort differences.

**Relation to the draft:** The paper partially addresses this with existing factor adjustments, alternative windows, and the SI’s surge/capture discussion. It does not isolate a period unaffected by all relevant historical and measurement changes. Nor does positive dependence across nonoverlapping windows uniquely distinguish fixed community differences from a changing latent process.

The current data permit careful characterization of those alternatives; they do not automatically permit decisive causal separation.

**Framing action:** Describe the persistence result as a restriction on homogeneous simulated worlds in this observation window. Put the common-shock and fixed-heterogeneity readings together in the Discussion, rather than treating one as a minor robustness issue. Any future out-of-period test should remain explicitly proposed, not represented as already confirmatory or authorized.

#### 11. Can a hierarchy be understood without modeling relations among its communities?

> “Why are there so many overlapping online communities?”

TeBlunthuis, Introduction, p. 1880. **[verified: [source PDF](https://ojs.aaai.org/index.php/ICWSM/article/download/35907/38061/39975)]**

The article studies changing competition and mutualism among communities that overlap in users and topics. The implied question is whether apparently individual growth tendencies partly reflect an ecosystem.

**Relation to the draft:** The rank-based accounting can describe aggregate outcomes without fully modeling those relations. That is a useful abstraction. But it does not establish that endpoint trajectories are socially independent.

Existing aggregate time series can describe co-movement and test the sensitivity of summaries to shared factors. They cannot identify actual user migration, resource competition, or mutual support without further information. The existing co-movement adjustments should not be renamed tests of ecological mechanisms.

**Framing action:** Add one sentence in the persistence subsection explaining that individual growth differences can summarize relational processes. This gives a substantive interpretation to unexplained dependence while preserving the model’s deliberate simplicity. A detailed network extension is unnecessary for the present paper.

#### 12. Would a less concentrated hierarchy be better, and for whom?

> “quality is not necessarily correlated with popularity.”

Ciampaglia et al., Introduction. **[verified: [full text](https://www.nature.com/articles/s41598-018-34203-2)]**

The model separates an item’s quality from its popularity and examines how selection procedures affect their relationship. It makes the evaluation criterion explicit.

**Relation to the draft:** The paper cannot answer whether flattening improves information quality, representation, user welfare, or community health. It measures neither an independent quality criterion nor the distribution of benefits among people. More turnover might expand opportunity, destroy useful institutions, or change neither.

**Framing action:** Present concentration and mobility as descriptive outcomes with potential consequences, not intrinsic welfare scores. The Discussion can say that the framework supplies quantities needed for evaluating interventions, while a normative assessment requires specifying beneficiaries and outcomes.

This boundary strengthens the paper for a broad audience: it explains why measuring the dynamics matters without asserting that one distribution is socially preferable on evidence the paper does not contain.

## D. Synthesis for the draft

### The five most valuable changes

#### 1. Rebuild the Introduction around redistribution, persistence, and established precedents

**Location:** Introduction paragraphs 1–3, especially the transition from item decay to the firm-growth literature.

**Problem:** The opening moves rapidly from a broad platform observation into mathematical ancestry. Readers from communication, HCI, or platform studies encounter their substantive question only indirectly. It also risks overstating how surprising stable allocation alongside turnover should be.

**Smallest useful change:** Replace part of the second paragraph with three distinctions:

- short-lived content versus continuing communities;
- replacement of winners versus redistribution of activity;
- within-platform inequality versus platform power.

Cite Boik, Lorenz-Spreen, and one substantive concentration reference. Retain the essential rank-growth lineage in a shorter sentence; move extended genealogy to the SI.

**Proposed framing, for author consideration:** Earlier work documents both concentrated attention and changing popular destinations. The question here is how the trajectories of continuing communities combine to reproduce—or erode—the distribution of activity.

That wording makes no priority claim. It also creates a clear reason for every subsequent result: geometry explains apparent mobility, the balance describes distributional persistence, and the residual dependence identifies where the chosen homogeneous models are insufficient.

#### 2. Treat entrenchment as a multidimensional outcome

**Location:** “The top turns over fastest because the hierarchy is flattest there,” and Discussion paragraph 2.

**Problem:** The statement that rank is a poor measure of entrenchment is useful but too categorical unless entrenchment is defined. Persistent rank, persistent share, persistent membership in a top set, and durable control over distribution answer different questions.

**Smallest useful change:** Replace “rank is a poor measure” with the narrower proposition that rank alone is insufficient to measure persistence of activity advantage. Keep the named example of a leader losing share while retaining position. Add the Twitch comparison as a boundary case.

A small SI table could distinguish:

| Object | What persistence means | What the draft observes |
|---|---|---|
| Ranked position | Remaining near the same ordinal place | Yes |
| Activity advantage | Retaining level or share | Yes, within the measured universe |
| Top-set membership | Remaining above a threshold | Yes |
| Audience relationship | Retaining particular people | No |
| Platform power | Retaining control over access or monetization | No |

The table would clarify the contribution without another empirical exercise. It would also prevent rapid community turnover from being read as evidence that online power is broadly contestable.

#### 3. Give persistence a substantive interpretation while preserving rival explanations

**Location:** “What homogeneous random growth lacks: persistent decline,” particularly the paragraph citing community-development research.

**Problem:** The subsection contains the paper’s strongest connection to community scholarship, but its current literature engagement is brief. The word “persistence” can sound like an identified mechanism even though fixed growth differences and common historical forces remain viable explanations.

**Smallest useful change:** Replace the passing literature sentence with a compact account of three possibilities: enduring differences among communities, changes in community institutions or relationships, and shared platform-era shocks. Cite Cunha and colleagues, TeBlunthuis’s ecosystem work, and Veselovsky and Anderson.

Then state what the evidence contributes: the observed dependence is inconsistent with the specified homogeneous worlds, while heterogeneous mean growth can reproduce its sign. This formulation keeps the successful diagnostic separate from claims about its cause.

The paper should not adopt a new mechanism or add a research program here. Its present achievement is to locate a behavior that adequate models must represent. That is a meaningful connection to the literature even when the social source remains unresolved.

#### 4. Elevate the production decomposition and remove the unsupported causal bridge

**Location:** Abstract’s final sentence, significance statement’s final sentences, Introduction’s findings paragraph, and the end of the persistent-decline subsection.

**Problem:** “Through that channel” links persistent decline to the submissions head’s posting and share losses more strongly than the manuscript’s own qualification supports. The literature makes the missing link especially consequential: production can respond to visibility, institutional change, audience demand, or strategic adaptation.

**Smallest useful change:** Present two findings consecutively, without claiming the first caused the second:

1. persistence is concentrated in item counts rather than per-item engagement;
2. the submissions head experienced falling posting volume and share during the observation period.

Then state that the analysis does not establish whether the same process explains both. This should agree with the qualification already present in the detailed Results.

Cite Ciampaglia’s production study and, if space permits, Bailo or Cotter in the interpretation paragraph. The result becomes more interesting when framed as locating the empirical variation: continuing communities change partly through how much they produce, not just how individual items perform. It does not need a stronger causal claim to matter.

#### 5. Rewrite the platform-design implication as conditional and institutionally complete

**Location:** Discussion paragraph 2, beginning with the sentence about a change in how a platform distributes attention.

**Problem:** The current sentence implies that distribution changes act through relative decline and that the estimated relaxation times describe their subsequent effects. A policy could also alter volatility, entry and exit, relationships among communities, measurement, and the target distribution itself.

**Smallest useful change:** Replace the two policy sentences with a conditional statement:

*Changes in platform rules or distribution can alter several ingredients of the measured balance. The estimated relaxation times describe adjustment under held-fixed dynamics; evaluating an actual intervention requires identifying which ingredients changed and whether those changes persisted.*

This is proposed wording, not an adopted paper claim.

Use Nieborg–Poell and Huszár as the compact conceptual and empirical references. Bailo belongs nearby or in the SI because it demonstrates why posting, engagement, and distribution changes need not align.

The paper can still motivate long-run consequences of interventions. It should present the framework as a way to organize their measurement, rather than treating its current calibration as an identified policy response.

### What the literature prevents the paper from claiming

The following boundaries should govern both the manuscript and any accompanying summary:

1. **Stable concentration alongside changing winners is not an unrecognized phenomenon.** Boik and colleagues provide a close substantive precedent, albeit at a different level of measurement. The paper should distinguish its estimand and explanatory task.

2. **Fast incumbent turnover is not a universal property of online platforms.** Twitch supplies a relevant contrasting setting. Differences in units and horizons require care, but they do not justify a universal opening claim.

3. **Community activity is not equivalent to audience attention, distinct-person participation, community success, or economic income.** The manuscript’s measures are useful indicators of engaged activity. Their interpretation should remain explicit.

4. **Item-count variation does not identify an exogenous supply mechanism.** Production and attention can influence one another. An accounting decomposition is not a causal separation of supply and demand.

5. **Persistence is not, by itself, evidence of a unique dynamic mechanism.** Fixed differences, institutional development, ecological relations, and historically shared shocks remain different substantive possibilities.

6. **The observed hierarchy does not identify algorithmic amplification.** That requires a counterfactual exposure comparison. Statistical rank and feed placement should never be conflated.

7. **Platform departure is not necessarily community disappearance.** Moderation, migration, changing availability, and falling below a selection threshold are distinct events.

8. **A flatter hierarchy does not establish improved welfare or reduced platform power.** Neither quality nor control over the infrastructure follows directly from community activity shares.

9. **Failure of an implemented neutral or homogeneous comparator does not reject an entire modeling tradition.** The comparison should remain tied to the simulated assumptions and measured outcomes.

These are restrictions on interpretation, not reasons to weaken the empirical presentation. They make the paper’s actual evidence easier to see.

### Where the paper’s distinctive contribution is clearer than its current framing

I would emphasize three connected contributions, without claiming priority.

**First, the paper connects the distribution to the trajectories of its occupants.** Attention research often emphasizes either the aggregate inequality or the careers of particular items, creators, and communities. Here, the useful object is their relationship: the distribution may remain relatively durable while its occupants change, and that relationship can differ across activity streams.

**Second, the paper separates geometry from mobility.** The finding that rank spacing changes apparent turnover is relevant beyond stochastic modeling. It gives researchers studying incumbency a concrete reason to distinguish rank changes from changes in activity advantage. This should be presented as a measurement lesson with substantive implications.

**Third, the manuscript does not stop at reproducing a hierarchy.** Its evidence that a model can generate a broadly plausible distribution while failing to improve movement forecasts or reproduce important historical changes is valuable. The substantive lesson is that explaining a distribution and explaining individual trajectories are different tasks. The persistent item-count variation identifies where those tasks diverge in the observed data.

Together, these support a clearer positioning: **a study of how unequal activity is reproduced through changing communities, and of the limits of homogeneous random growth as an account of those communities’ trajectories**.

The literature does not require an expanded main-text review. A practical revision would add a small number of substantive anchors, strengthen engagement with the already-cited community literature, and put the cross-study comparisons and mechanism taxonomy in the SI. The objective should be a shorter, better connected argument—not a larger bibliography standing in for one.

## E. Search log

**Date:** 19 October 2026.

**Local grounding:** Read the two assignment briefs, manuscript and SI source, all 38 existing bibliography entries, round-five synthesis, program state, and the relevant final results-record section. Used those materials to distinguish missing works from existing references engaged only briefly.

**Search tools and source locations:** General live scholarly web search; publisher records and full text from Nature, SAGE, PNAS/PMC, Science records, Oxford Academic, AEA, and AAAI; ACM article records; author and institutional repositories including Harvard Business School, University of Toronto, university-hosted PDFs, and arXiv author manuscripts. Citation trails were used to locate primary works, not as substitutes for reading them.

**Main query families:**

- Stable attention concentration with website turnover; persistence of broadband attention allocation.
- Collective attention acceleration, cultural-item decay, popularity persistence, finite attention.
- Twitch incumbent stability, creator earnings inequality, Patreon and platform affiliation.
- Online community success, growth, decline, newcomer retention, Reddit competition and mutualism.
- Reddit during COVID; persistent versus temporary platform change.
- Bans, deplatforming, community migration, and post-removal activity.
- Facebook algorithm changes, news distribution, CrowdTangle engagement, referral traffic.
- Algorithmic amplification, chronological-feed experiments, platform power, audience duplication.
- Content production responding to visibility or demand.

**Selection rule:** Prioritized works that change a specific claim, establish a close precedent, provide a substantive rival explanation, or clarify what an observed outcome means. Included older works only where they supply a foundational distinction or a directly relevant moderation or limited-attention precedent.

**Remaining verification needs:** Full-text checks are still needed before detailed use of Webster–Ksiazek, Hindman, and Guess et al.; quotations from Prat–Valletti should be checked against the final published version if used verbatim in the manuscript. No proposed framing change, new analysis, protocol, or causal interpretation was adopted or implemented in this review.