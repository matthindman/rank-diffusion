<!-- Provenance: Task 1 (platform literature) report by Claude Opus 5.5 (claude-opus-5-5) (Claude Code sub-agent, default effort, WebSearch/WebFetch; returned as text because sub-agents may not write report files), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

**I could not write the report to the file.** The tool refused it: "Subagents should return findings as text, not write report files." So `REPORT_T1_opus-5.5.md` does not exist, and the full report follows below the summary, ready to be saved to that path. No repository file was changed.

**The five most important findings (summary):**
1. **The lead author's own book anticipates the paper.** Hindman (2018), *The Internet Trap*, ch. 5 (with Bruce Rogers), already asks the paper's question and gives its qualitative answer: churn sustains the power law. It models web traffic with finance-derived stochastic models, cites Fernholz, and simulates from a flat start. The draft does not cite it.
2. **That chapter contradicts two Reddit results.** Small web sites were far more volatile, and the web's top ten rarely changed.
3. **The geometry result reframes a literature.** Turnover profiles have been read as evidence of copying bias (Bentley; Evans & Giometto; Acerbi & Bentley).
4. **The 2019–22 shocks have literatures to cite.** Veselovsky & Anderson (2023): COVID was Reddit's largest shock and submissions fell below trend. Cima et al. (2024): the June 2020 ban of nearly 2,000 communities. Lazer et al. (2014) and Malik & Pfeffer (2016): platform effects.
5. **Persistent decline is a documented life-cycle pattern.** Halfaker et al. (2013), TeBlunthuis et al. (2018), Ribeiro (2014). The deconcentration finding is undersold.

=====

# Report T1: the substantive literature on digital platforms

**Reviewer:** Claude Opus 5.5 (model id `claude-opus-5-5`, Anthropic), running as a subagent in **Claude Code**. The reasoning setting is not shown to me.
**Tools used:** WebSearch; WebFetch (PDFs it retrieved were saved locally and converted with `pdftotext`, so quotations were checked against the extracted text and not against a model's summary); the Claude desktop app's built-in browser, used read-only for Google Books "search inside this book" snippets; the Crossref API (through WebFetch) for DOIs; Read/Bash for local files.
**Date:** 2026-10-19.
**Manuscript reviewed:** v4 at commit 42955ac (`paper/pnas_draft.tex`, `paper/pnas_si.tex`).

---

## A. Access check and search approach

**Local files read in full:** `COMMON_BRIEF.md`, `T1_PLATFORM_LITERATURE.md`, `paper/pnas_draft.tex`, `paper/pnas_si.tex`, `paper/references.bib`, and `llm_fitting/runs/2026-10-19_external_critiques_round5/SYNTHESIS_R5.md`.

**Local files read in part:**
- `llm_fitting/PROGRAM_STATE.md`: about the first 200 lines.
- `00_PRACTICES_ADOPTED.md`: the first 30 lines.
- `fig2_size_blind.png`: viewed.

**Local files not opened:**
- The compiled PDFs (I read the `.tex` sources).
- Figures 1, 3, 4 and 5.
- Every data file, and everything in `SEALED_PATHS.json`. I did not open that file either, and touched no data folder.
- The other reviewers' reports in this folder, including `REPORT_T1_gpt-6-astra.md`, and `VERIFY_LIST.md`.

**Disclosure.** This session's WebFetch cache is shared with agents running at the same time. Early on, a bulk text-conversion command showed me the first lines of several PDFs those agents had fetched (papers on stochastic portfolio theory, Reddit's news feed and the r/popular feed). I used none of those files. The one Reddit-feed item I cite (Chan et al.) I found and checked separately through its AAAI page.

**Full text could not be retrieved for:**
- Webster & Ksiazek 2012, Hofman et al. 2021, Ordanini & Nunes 2016 and Rosen 1981: paywalled.
- Candia et al. 2019: the repository refused the connection.
- Malik & Pfeffer 2016: the publisher returned 503.
- The PNAS PDF of Huszár et al.: 403, so I used the arXiv version.
- PMC pages: blocked by a bot check.

**Books.** Neither Hindman (2018), Hindman (2009) nor Webster (2014) could be read in full. For them I used Google Books search-inside snippets. A snippet is a short run of the printed page's text with its page number, so it counts as full-text verification for the words shown, and for nothing else.

**Quotation policy in this report.** I keep to **one quotation per source**, each **under 15 words**, because I operate under a standing constraint on reproducing copyrighted text. This falls short of the brief's "1–3 quotations of up to 40 words". To make up for it, I give page numbers for further passages and paraphrase them, so the authors can read them in full. Every quotation is in Section C and is labelled.

**Edition caveats for page numbers:**
- *Hindman 2018:* pages come from the Google Books preview of the 2020 paperback (`id=3PHkDwAAQBAJ`).
- *Webster 2014:* pages come from the 2016 edition preview (`id=oL34DwAAQBAJ`).
- *Iñiguez, Huszár, Veselovsky, Cima, Martin, Ribeiro, Zhang:* quotations were checked against arXiv versions.

The authors should confirm the pagination of the edition they cite, and the wording of the published versions.

**Search approach.**
- I searched with the draft's own vocabulary and with that of neighbouring fields: attention concentration and fragmentation; audience duplication; winner-take-all, superstars and the long tail; turnover of popularity charts and neutral models; rank dynamics; online community rise, decline and life cycles; Reddit bans, quarantines and deplatforming; platform effects and algorithm dynamics; algorithmic amplification; COVID-era platform change; creator economy; participation inequality.
- I looked as hard for work that would weaken the paper as for work that supports it.
- The single most consequential finding of the search is in the lead author's own earlier book. I report it first because a referee will see it.

---

## B. Annotated bibliography

**How it is organized.** The entries are grouped by the claim in the draft they bear on, not by discipline, so that each group answers "what does this change in the paper?". The groups are:
1. Earlier answers to the paper's own question.
2. The turnover of ranked lists.
3. Growth and decline of online communities, which bears on the persistence result.
4. Platform shocks and governance in 2019–2022, rival reading (c).
5. Platform traces as instruments, rival readings (c) and (d).
6. Theories of concentration that the framing touches.
7. Explanation against prediction.

**Tier 1, must engage (9 entries, some grouped):** 1.1, 1.2, 1.3, 2.1, 3.1, 4.1, 4.2, 5.1, plus the cited-but-underused 2.2.

### 1. Earlier answers to the paper's own question

**1.1 [Tier 1] Hindman, M. (2018). *The Internet Trap: How the Digital Economy Builds Monopolies and Undermines Democracy*. Princeton University Press. Chapter 5, "The Dynamics of Web Traffic" (with Bruce Rogers), pp. 82–101.**
- ISBN 978-0-691-15926-3.
- Google Books: https://books.google.com/books?id=3PHkDwAAQBAJ
- Access: [full text, snippets only] for pp. 8–9, 82–101 and 199. [secondary] for the data description, from C. Guthro's review in *Journal of Intellectual Freedom and Privacy* 4(2), 2019, https://journals.ala.org/index.php/jifp/article/download/6992/9815.

*Core insight and evidence.*
- The chapter states the draft's puzzle and the draft's answer for web traffic: structural stability alongside enormous instability for individual sites (p. 84). It proposes that churn itself produces and maintains the power law (p. 93; quotation in C2.1).
- Data: daily Hitwise traffic for the top 300 sites, July 2005 to June 2008 (Guthro's review). The leakage of sites near the rank-300 cutoff is discussed on p. 89.
- Model: stochastic dynamical systems taken from finance (pp. 85–88). The notes cite Fernholz (2002) on the stock market's departure from a power law at the extreme head (p. 199, n. 15).
- Simulation: growth rates are drawn, converted to market shares, re-sorted, and the cycle repeats (p. 95). It is run from several initial conditions, one of them a flat distribution (pp. 96–99).
- Caveat: the chapter warns that over very long runs models of this type eventually fail, because small misspecifications compound (p. 99).
- Two empirical statements differ from the draft's Reddit results:
  - *Size dependence:* smaller sites are much more volatile (p. 9; "Volatility by Traffic", pp. 91–93).
  - *The very top:* the top sites rarely trade places, sites below about rank 25 switch ranks almost daily (p. 89), and it is rare for a site to enter or leave the top ten (p. 99).

*Relevance.* This **precedes** the draft's central question and its qualitative answer:
- Introduction, paragraphs 1–3 (`pnas_draft.tex` ll. 90–95).
- Discussion, first sentence (l. 186).
- The abstract's "We ask where the hierarchy comes from and why it outlasts its members".

It is the lead author's own prior work, which matters under single-blind review: a referee who knows the book will ask why it is not cited. It also **challenges** two Reddit results, and these contrasts are informative, not embarrassing:
- On Reddit, lasting volatility depends only weakly on size (raw β 0.045–0.082; lasting β 0.05–0.13).
- On Reddit, a quarter to a third of the top ten is replaced each year (0.27–0.33).

*Actionable.*
1. Cite it in Introduction paragraph 2 next to `huberman1999`. Say in paragraph 3 what this paper adds:
   - a rank-by-rank measurement of the balance with a tested crossing identity;
   - lasting variance separated from passing variance, by permanent rank;
   - entry and exit;
   - null worlds computed with the same estimators;
   - persistence;
   - named communities.
2. In the size-dependence paragraph (l. 116), add one sentence contrasting web sites' strong size dependence of volatility with Reddit's weak lasting dependence.
3. In the turnover section (l. 114), note that top-ten membership was nearly fixed for web sites but turns over yearly on Reddit.
4. Add an SI paragraph, "Relation to earlier work on web traffic".

Smallest change: one reference and about three sentences. See D1.

**1.2 [Tier 1] Webster, J. G., & Ksiazek, T. B. (2012). The dynamics of audience fragmentation: Public attention in an age of digital media. *Journal of Communication*, 62(1), 39–56.** https://doi.org/10.1111/j.1460-2466.2011.01616.x
Access: [abstract only, through a library catalogue summary]. **Together with** Webster, J. G. (2014). *The Marketplace of Attention: How Audiences Take Shape in a Digital Age*. MIT Press. https://books.google.com/books?id=oL34DwAAQBAJ. Access: [full text, snippets only] for pp. 101 and 163.

*Core insight.*
- This is communication research's central frame for the object the draft studies: how public attention distributes across outlets as choice multiplies.
- The article argues for audience-centric measures of fragmentation, applying network metrics to Nielsen television and internet data (abstract).
- The book treats concentration as a durable structure made jointly by media and users. Its Fig. 5.3 shows the long-tail distribution of internet brands by monthly reach (p. 101). It argues that popularity is valued by media and users together (p. 163; quotation in C2.1).

*Relevance.*
- A communication or CSS referee will expect the draft's first sentence ("Attention online is concentrated…", l. 90) to engage this frame.
- The draft has evidence that bears on it directly and does not say so: the head's arithmetic share fell from 50.2% to 34.0% (submissions, 2019–22), and every top set lost share every year (l. 100).
- Status: **qualifies and extends**. The draft measures production and engagement (karma, posts), not exposure.

*Actionable.* Cite both in Introduction paragraph 1, and in Discussion paragraph 2 with one sentence placing the decline of concentration in the fragmentation debate. Add the caveat that karma is engaged activity, not exposure (already in Limits). See D5.

**1.3 [Tier 1] Lorenz-Spreen, P., Mønsted, B. M., Hövel, P., & Lehmann, S. (2019). Accelerating dynamics of collective attention. *Nature Communications*, 10, 1759.** https://doi.org/10.1038/s41467-019-09311-w
Access: [full text], CC BY, via https://cora.ucc.ie/bitstreams/f4ae8a47-a98a-4a3e-942b-34dbb4a46b9a/download

*Core insight and evidence.*
- Across Twitter hashtags (2013–16), Google Books n-grams, box office, Google Trends, Reddit (2010–15), APS citations and Wikipedia, attention to items rises and falls more steeply and lasts less long over time.
- On Twitter, a hashtag's average stay in the hourly top 50 fell from 17.5 hours (2013) to 11.9 hours (2016) (p. 2; Methods p. 7).
- On Reddit, the authors use the top 1,000 most-commented submissions each month, and their counts "confirm the increasing turnover rates" in their sense (Methods).
- Wikipedia's hourly top 100 shows no marked acceleration (Methods).
- A competition model in which production and consumption rates rise reproduces the pattern (pp. 4–6).

*Relevance.*
- The draft's opening contrasts fast decay of items (`wu2007`) with community-level churn, and calls turnover "constant".
- Lorenz-Spreen et al. is the main recent result on how fast collective attention turns over, it includes Reddit, and it finds turnover among top items **rising**.
- Status: **qualifies**. The draft should not imply that a turnover rate is a stable platform constant. It can add a point of its own: containers (communities) form a slow hierarchy even when attention to items accelerates.

*Actionable.*
- Cite next to `wu2007` in Introduction paragraph 1.
- In "A fast turnover and a durable hierarchy" (l. 100), say whether top-set turnover trends across the three Reddit year pairs. The data on hand answer this: the draft already reports ranges "by cut-off and year".
- One clause in the Discussion on the separation of levels.

**1.4 [Tier 2] Hindman, M. (2009). *The Myth of Digital Democracy*. Princeton University Press.** https://books.google.com/books?id=qE3F3PnYAr8C
Access: [full text, snippets only] for pp. 15, 45, 51, 55–57, 91 and 136.

*Core insight.*
- Ch. 3 ("googlearchy") shows power-law inlink distributions among political sites (pp. 51–55, Figs. 3.1–3.2).
- It argues that winners-take-all patterns recur at every level of the web (p. 57; quotation in C1.1).
- Hitwise data show political traffic concentrated like media traffic (p. 91).
- The whole web's power law is the sum of power laws within categories (p. 136).

*Relevance.* It **precedes** the first sentence of the Introduction and the claim "on every large platform".

*Actionable.* Add to the citation at l. 90 alongside `huberman1999`.

**1.5 [Tier 3] Nelson, J. L. (2020). The enduring popularity of legacy journalism: An analysis of online audience data. *Media and Communication*, 8(2), 40–50.** https://doi.org/10.17645/mac.v8i2.2736
Access: [secondary] (search summary of the abstract).

In a year of US online news audience data, legacy brands made up most of the most-visited news sites despite many digital-native entrants. This **supports** a durable head amid entry. It is optional background for Discussion paragraph 2.

**1.6 [Tier 3] Pecile, G., Di Martino, E., Loru, E., Zollo, S., Di Marco, N., & Cinelli, M. (2026). Persistent structural inequality of online interactions across platforms. arXiv:2605.30996.** https://arxiv.org/abs/2605.30996
Access: [abstract only].

In user–post networks from several platforms, inequality of interactions is stable over time within each platform. This **supports**, at the level of users, the premise of a stable shape. It is a preprint; cite only if a sentence on the generality of stable inequality is wanted.

### 2. Turnover of ranked lists

**2.1 [Tier 1] Acerbi, A., & Bentley, R. A. (2014). Biases in cultural transmission shape the turnover of popular traits. *Evolution and Human Behavior*, 35(3), 228–236.** https://doi.org/10.1016/j.evolhumbehav.2014.02.003
Access: [full text], accepted manuscript at https://bura.brunel.ac.uk/bitstream/2438/20332/2/FullText.pdf. **With** Evans, T. S., & Giometto, A. (2011). Turnover rate of popularity charts in neutral models. arXiv:1105.4044, https://arxiv.org/abs/1105.4044 (access: [full text], abstract and introduction read). **And with** Bentley et al. (2007), already cited as `bentley2007`.

*Core insight.*
- Neutral (unbiased) copying makes a specific prediction for the "turnover profile": how the number of new entries z in a top-y list grows with y.
- Bentley et al. proposed z ≈ y√μ. Evans & Giometto showed this approximates the Wright–Fisher model but fails for the Moran model.
- Acerbi & Bentley summarize the profile as z = a·y^b with neutral b = 0.86 (Introduction). They read b > 0.86 (a convex profile) as bias toward popular traits and b < 0.86 as bias toward unpopular ones (Discussion; quotation in C4.1).
- They apply this to colour terms, US baby names and Last.fm.

*Relevance.*
- Figs. 1C and 2B of the draft are turnover profiles. The draft's result that "the top turns over fastest because the hierarchy is flattest" (Eq. 1; R9) implies that a profile's shape follows from the ladder's local slope under identical dynamics.
- That **qualifies** a decade of inference in cultural evolution that reads profile shape as evidence of transmission bias. It is a contribution of this paper that the draft does not claim.
- The draft's neutral-copying world (top-ten turnover 0.04, share replaced rising with k; SI Null worlds) is one regime of a model family whose profile depends on the regime (Evans & Giometto). It does not stand for "the neutral model" in general.
- My rough reading of Fig. 2B is that the data's share replaced falls from about 0.33 (k = 10) to about 0.16 (k ≈ 6,000). That suggests b near 0.9. This is illustrative, not a measurement.

*Actionable.*
- In "The top turns over fastest…" paragraph 1 (l. 109), add one sentence: turnover profiles are used to infer copying biases [Bentley 2007; Acerbi & Bentley 2014]; Eq. 1 shows that their shape also depends on the ladder's slope, so such inferences need a ladder control.
- In SI "Null worlds", fit b for the data and every world (data on hand).
- Name the regime of the neutral world (Wright–Fisher-like, μ = 0.003) and cite Evans & Giometto.

**2.2 [Cited, engaged in passing; Tier 1 to engage properly] Iñiguez, G., Pineda, C., Gershenson, C., & Barabási, A.-L. (2022). Dynamics of ranking. *Nature Communications*, 13, 1646 (`iniguez2022`).**
Access: [full text], arXiv:2104.13439v2, abstract and Fig. 1 read.

*Core insight.*
- The draft uses only the rank-change statistic C(R).
- The paper's main results: the flux of new elements sets a ranking's stability, and in open (high-flux) systems only the top is stable (abstract; Fig. 1).
- A displacement-and-replacement model reproduces 30 rankings and has two regimes: fast, large jumps or slow diffusion.

*Relevance.*
- Reddit lies in their open regime (high flux, stable rank one).
- Their model is a direct rival account of Figs. 2B–C. This overlaps with Task 4.

*Actionable.* Add one clause in the rank-change paragraph (l. 118): in Iñiguez et al.'s terms Reddit is an open ranking, and Eq. 1 gives a level-based account of the same pattern. The draft's jump-to-random-rank rival (boundary ratio 1.56) is close to their replacement mechanism; say so.

**2.3 [Tier 3] Ordanini, A., & Nunes, J. C. (2016). From fewer blockbusters by more superstars to more blockbusters by fewer superstars: How technological innovation has impacted convergence on the music chart. *International Journal of Research in Marketing*, 33(2), 297–313.** https://doi.org/10.1016/j.ijresmar.2015.07.006
Access: [abstract only] (RePEc summary).

The study counts distinct songs and artists on the Billboard Hot 100 from 1974 to 2013. It reports growing winner-take-all convergence until about 1998, then dispersion. Studies like this read **turnover counts as concentration**, which conflates the two quantities the draft separates.

*Actionable.* Optional SI sentence. If Discussion paragraph 2 says that churn and concentration can move independently, this is the literature that conflates them.

### 3. Growth and decline of online communities (the persistence result)

**3.1 [Tier 1] Halfaker, A., Geiger, R. S., Morgan, J. T., & Riedl, J. (2013). The rise and decline of an open collaboration system: How Wikipedia's reaction to popularity is causing its decline. *American Behavioral Scientist*, 57(5), 664–688.** https://doi.org/10.1177/0002764212469365
Access: [secondary]. **With** TeBlunthuis, N., Shaw, A., & Hill, B. M. (2018). Revisiting "The Rise and Decline" in a population of peer production projects. *CHI '18*, paper 355. https://doi.org/10.1145/3173574.3173929 (access: [full text]).

*Core insight.*
- Wikipedia's editor decline followed growing rejection of newcomers and entrenched norms.
- TeBlunthuis et al. replicated this across hundreds of Wikia wikis: the average community rises and then declines, newcomers' survival falls over time, and norms entrench. They also note that most research had studied a few of the largest communities (quotation in C2.3).

*Relevance.*
- The draft's distinctive empirical result is persistent decline in participation. It is specific to communities on the comments ladder, and decliners carry 40–60% of the excess slide (ll. 168–174).
- The online-community literature has a well-known **life-cycle mechanism for self-reinforcing decline**. The draft cites firm and income analogies first (l. 171) and the community literature only through `kairam2012`, `cunha2019` and `teblunthuis2022`.
- Status: **precedes and qualifies**. Self-reinforcing decline is documented. What is new is measuring its population-level consequence for concentration with a statistic robust to averaging artifacts.
- It also gives rival (b) a dynamic version: persistence by **life-cycle stage**, not a fixed type.

*Actionable.*
- In Persistence paragraph 2, lead with the community literature instead of firms: "Self-reinforcing decline is a documented phase of online communities' life cycles [Halfaker 2013; TeBlunthuis 2018; Ribeiro 2014]."
- Proposed check (partly feasible with the data on hand): do persistent decliners skew older, using first appearance in the 2018–19 panel as a crude age?

**3.2 [Tier 2] Ribeiro, B. (2014). Modeling and predicting the growth and death of membership-based websites. *Proceedings of WWW '14*, 653–664.** https://doi.org/10.1145/2566486.2567984
Access: [full text], arXiv:1307.1354v3.

*Core insight.*
- Six years of daily active users on 22 membership websites.
- A reaction–diffusion–decay model in which active members draw inactive members back into activity.
- Whether a site is self-sustaining depends on a ratio of rates. Above the threshold, activity decays to zero whatever its starting level (Sections 2–3).

*Relevance.* A concrete mechanism for "decliners keep declining": activity feeds on activity, so decline below a threshold compounds. It sits between rival (b), a fixed parameter class, and a dynamic. The quotation is in C1.4.

*Actionable.* Cite in the persistence paragraph with 3.1.

**3.3 [Tier 2] Zhang, J., Hamilton, W. L., Danescu-Niculescu-Mizil, C., Jurafsky, D., & Leskovec, J. (2017). Community identity and user engagement in a multi-community landscape. *ICWSM*, 11, 377–386.** https://doi.org/10.1609/icwsm.v11i1.14904
Access: [full text], arXiv:1705.09665, abstract read.

Across about 300 subreddits, communities with distinctive and temporally dynamic identities retain users better, and niche communities show larger acculturation gaps for newcomers. This is an **observable community trait linked to retention**: a candidate source of the heterogeneous growth that the gapped statistic detects on comments. The quotation is in C2.3.

*Actionable.* Cite in Persistence paragraph 2 as one documented source of heterogeneity between communities.

**3.4 [Tier 2] Xu, Y., & Peng, T.-Q. (2024). Ecological constraints on audience size in the digital media system: Evidence from the longitudinal tracking data from 2019 to 2022. *Human Communication Research*, 50(1).** https://doi.org/10.1093/hcr/hqad028
Access: [abstract only] (index record).

Comscore data for 64 news, communication and social platforms, 2019–2022 (the draft's window). Niche overlap shrinks audiences. The authors report strong short-term audience inertia and distinct long-term growth trends by platform category. At the platform level this is the pairing the draft finds at the community level: passing inertia plus persistent heterogeneous trends. **Supports** (in parallel), and comes from the communication tradition.

*Actionable.* Cite in Persistence paragraph 2 beside `teblunthuis2022`.

**3.5 [Tier 3] Lin, Z., Salehi, N., Yao, B., Chen, Y., & Bernstein, M. S. (2017). Better when it was smaller? Community content and behavior after massive growth. *ICWSM*, 11, 132–141.** https://doi.org/10.1609/icwsm.v11i1.14884
Access: [secondary].

Subreddits made "default" (exposed to every new user) had very different paths after the surge: some kept growing, others stalled. This is evidence that a platform-made shock can sort communities into different trajectories. It is relevant to reading persistence as heterogeneous exposure to platform-wide changes, which the draft finds on submissions.

**3.6 [Tier 3] TeBlunthuis, N. (2025). Niche dynamics in complex online community ecosystems. arXiv:2504.02153 (conditionally accepted, ICWSM 2025).** https://arxiv.org/abs/2504.02153
Access: [abstract only].

8,806 subreddits in 1,919 overlap clusters over five years. Competition and mutualism come in short bursts, and competition is followed by falling overlap. This bears on the draft's co-movement cluster adjustment (SI Persistence, "net of co-movement cluster").

**3.7 [Tier 3] Kraut, R. E., & Resnick, P. (2012). *Building Successful Online Communities: Evidence-Based Social Design*. MIT Press.** ISBN 9780262016575.
Access: [secondary].

The standard synthesis on recruiting, socializing and retaining members. Background for 3.1, if a single book citation is preferred.

### 4. Platform shocks and governance, 2019–2022 (rival reading c)

**4.1 [Tier 1] Veselovsky, V., & Anderson, A. (2023). Reddit in the time of COVID. *ICWSM*, 17, 878–889.** https://doi.org/10.1609/icwsm.v17i1.22196
Access: [full text], arXiv:2304.10777v1.

*Core insight and evidence.*
- COVID-19 ranks among the five largest shocks in Reddit's history on every metric, and is the largest for comments, active users and new users ("Macro-level structure").
- Unlike earlier surges, growth spread across disparate communities rather than a few (abstract).
- Two years on, only comments and active subreddits stayed above the pre-COVID trend. **Submissions fell about 3.6% below trend**, and the authors suggest other structural changes on Reddit may explain it (footnote 2). User counts returned to trend.
- The cohort that joined during COVID was less likely to stay active (abstract).

*Relevance.* This is direct, platform-wide documentation of rival reading (c) in the draft's window. It bears on three places:
- the persistence section's discussion of the 2020 surge (l. 168);
- the Limits paragraph (l. 190);
- the fall in posting at the submissions head (head submissions 33M to 17M while the top 5,000 grew, l. 182). The relative decline of submissions on the platform is a fact a referee will raise.

The short-lived COVID cohort predicts that communities which surged in 2020 later **reversed**. That is the opposite sign to the draft's persistence, so it is a check rather than an explanation. Status: **qualifies**.

*Actionable.*
- Cite at the surge sentence (l. 168) and in Limits.
- In the head-decline paragraph, add one clause noting the platform-wide relative decline of submissions after 2020.
- Proposed check (data on hand): is the positive gapped statistic present among communities with no unusual 2020 growth?

**4.2 [Tier 1] Cima, L., Trujillo, A., Avvenuti, M., & Cresci, S. (2024). The Great Ban: Efficacy and unintended consequences of a massive deplatforming operation on Reddit. *Companion Proceedings of the 16th ACM Web Science Conference*, 85–93.** https://doi.org/10.1145/3630744.3663608
Access: [full text], arXiv:2401.11254v6, abstract and introduction read.

**With:**
- Chandrasekharan, E., et al. (2017). "You can't stay here": The efficacy of Reddit's 2015 ban examined through hate speech. *PACM HCI*, 1(CSCW), 1–22. https://doi.org/10.1145/3134666. Access: [secondary].
- Horta Ribeiro, M., et al. (2021). Do platform migrations compromise content moderation? Evidence from r/The_Donald and r/Incels. *PACM HCI*, 5(CSCW2). https://doi.org/10.1145/3476057. Access: [abstract only].
- Chandrasekharan, E., Jhaver, S., Bruckman, A., & Gilbert, E. (2022). Quarantined! Examining the effects of a community-wide moderation intervention on Reddit. *ACM TOCHI*, 29(4). https://doi.org/10.1145/3490499. Access: [metadata only; content from memory, unverified].

*Core insight.*
- In June 2020 Reddit banned nearly 2,000 communities, including the very large r/The_Donald and r/ChapoTrapHouse.
- Of affected users, 15.6% left Reddit; those who stayed reduced toxicity by 6.6% on average, while 5% increased it by more than 70% (Cima et al., abstract).
- The 2015 bans drove accounts off the site, and the communities that received migrants showed no rise in hate speech (Chandrasekharan et al. 2017).
- Communities that migrated off Reddit lost posting, users and newcomers (Horta Ribeiro et al.).

*Relevance.*
- The draft says three of the 2019 submissions head left the top 5,000, "two of them banned by Reddit in June 2020" (l. 100). It also says bans act at the top ten, which lies outside every world's range (l. 114).
- This literature documents the event, its scale and its redistributive effect: users displaced into other communities.
- Status: **supports and qualifies**. Bans are discrete removals that add to the vanishing share v_k and redistribute activity, so they are part of the "event-driven" top-ten turnover.

*Actionable.*
- Cite Cima et al. at l. 100 and l. 114.
- Proposed check (data on hand, if the ban list is assembled): add one SI sentence on how much of the head's v_k in 2020–21 the June 2020 bans account for.

**4.3 [Tier 2] Huszár, F., Ktena, S. I., O'Brien, C., Belli, L., Schlaikjer, A., & Hardt, M. (2022). Algorithmic amplification of politics on Twitter. *PNAS*, 119(1), e2025334119.** https://doi.org/10.1073/pnas.2025334119
Access: [full text], arXiv:2110.11010, abstract and introduction read.

*Core insight.* A randomized holdout of nearly 2 million daily active accounts kept on a reverse-chronological feed measures how much personalized ranking amplifies reach. Right-leaning sources and politicians were amplified in 6 of 7 countries.

*Relevance.*
- Discussion paragraph 2 says a change in how a platform distributes attention "acts on the balance through the relative decline of those above each rank".
- The amplification literature identifies ranking effects at the level of items and actors with randomized holdouts. The draft has no such identification.
- Status: the draft's statement **extends** the item-level result to a slow, population-level variable, but it is a model implication. The quotation is in C1.5.

*Actionable.* Cite, and write the sentence as conditional: "If a ranking change alters the relative decline above a rank, our accounting implies its effect on concentration unfolds over years."

**4.4 [Tier 3] Chan, J., Choi, F., Saha, K., & Chandrasekharan, E. (2026). Examining algorithmic curation on social media: An empirical audit of Reddit's r/popular feed. *ICWSM*, 20, 391–406.** https://doi.org/10.1609/icwsm.v20i1.42644
Access: [abstract only]. **With** Chan et al. (2025), *The ranking effect: How algorithmic rank influences attention on social media*, arXiv:2509.18440 (access: [abstract only]). In that randomized experiment on a simulated r/popular feed, lower-ranked posts received about 40% less engagement.

Reddit's cross-community trending feed is a concrete platform lever by which communities gain or lose exposure. If Discussion paragraph 2 keeps its design implication, it can name this mechanism.

### 5. Platform traces as instruments (rival readings c and d)

**5.1 [Tier 1] Lazer, D., Kennedy, R., King, G., & Vespignani, A. (2014). The parable of Google Flu: Traps in big data analysis. *Science*, 343, 1203–1205.** https://doi.org/10.1126/science.1248506
Access: [full text], via https://cdanfort.w3.uvm.edu/csc-reading-group/lazer-flu-science-2014.pdf. **With** Malik, M. M., & Pfeffer, J. (2016). Identifying platform effects in social media data. *ICWSM*, 10, 241–249. https://doi.org/10.1609/icwsm.v10i1.14756 (access: [abstract only]).

*Core insight.*
- "Algorithm dynamics" are the changes engineers make to a service and the changes in how users use it (p. 1204). They made Google Flu Trends an unstable measure. "Blue team" dynamics occur when the provider changes the algorithm that produces the data (p. 1204).
- Malik & Pfeffer measure "platform effects" around feature launches. On Facebook, "People You May Know" roughly doubled the daily rate of new ties; on Netflix, a change shifted ratings by about 3%.

*Relevance.*
- Rival (c) has a name and a literature in CSS. The draft's karma (votes) and Facebook interactions are outputs of ranking systems that changed within the window: the June 2021 step in votes per post (SI), the archive's capture change, and CrowdTangle eras.
- Status: **qualifies**.

*Actionable.*
- In Limits (l. 190), name the rival as platform effects or algorithm dynamics, with these citations.
- Say which results rest on counts of posts and comments (less exposed to ranking) and which on karma (more exposed).
- The SI already has the June 2021 dating. Add the citation there. The quotation is in C2.2.

**5.2 [Tier 2] Wagner, C., Strohmaier, M., Olteanu, A., Kıcıman, E., Contractor, N., & Eliassi-Rad, T. (2021). Measuring algorithmically infused societies. *Nature*, 595, 197–204.** https://doi.org/10.1038/s41586-021-03666-1
Access: [full text], via https://dlab.epfl.ch/teaching/spring2022/cs727/papers/wagner2021measuring.pdf

*Core insight.*
- The constructs computational social scientists measure are unstable and shaped by the act of measuring (p. 198).
- Data are "collected under the algorithm" (p. 198).
- The paper calls for integrated theories that bridge micro and macro scales (p. 202).

*Relevance.* This is the framing for why a sealed out-of-period test matters, and why platform shocks belong to the system rather than to nuisance variation. The quotation is in C4.3.

*Actionable.* Cite in the Limits sentence on the 2023–24 protocol.

**5.3 [Tier 2] Yang, T., Majó-Vázquez, S., Nielsen, R. K., & González-Bailón, S. (2020). Exposure to news grows less fragmented with an increase in mobile access. *PNAS*, 117(46), 28678–28683.** https://doi.org/10.1073/pnas.2006089117
Access: [full text], via the ORA copy at https://ora.ox.ac.uk/objects/uuid:28009285-d663-4c9c-a7d2-3cfaf06d13ca

*Core insight.*
- A five-year US panel covering desktop and mobile shows networks of news co-exposure growing denser, so exposure is less fragmented.
- Desktop-only data give the opposite conclusion (Fig. 2 legend, p. 28680).
- Mainstream outlets are the common ground. More than half of the US online population consumes no online news (p. 28678).

*Relevance.*
1. It states the concentration/fragmentation question in PNAS (quotation in C1.1).
2. It shows that **a change of instrument can reverse a trend in measured concentration**. This is the risk the draft carries with Pushshift's capture change and CrowdTangle's eras.

*Actionable.* Cite in Introduction paragraph 1, and add one clause in Limits.

### 6. Theories of concentration that the framing touches

**6.1 [Tier 2] DiPrete, T. A., & Eirich, G. M. (2006). Cumulative advantage as a mechanism for inequality: A review of theoretical and empirical developments. *Annual Review of Sociology*, 32, 271–297.** https://doi.org/10.1146/annurev.soc.32.061604.123127
Access: [memory, unverified] for content; metadata verified through Crossref.

*Core insight (from memory, to be checked).* The review separates "strict" Mertonian cumulative advantage, in which growth depends on current level and divergence follows, from path dependence and from multiplicative, Gibrat-type growth.

*Relevance.* Introduction paragraph 2 (l. 93) says "cumulative advantage lets early leaders accumulate more", citing Merton, Salganik and van de Rijt. The draft's own model has weak size dependence: Gibrat-like, not strict cumulative advantage. A sociologist referee will want the distinction stated.

*Actionable.* Replace that clause with: "multiplicative growth alone produces steep hierarchies; stricter forms of cumulative advantage, in which advantage itself compounds [DiPrete & Eirich 2006], are not needed for the balance we measure."

**6.2 [Tier 3] Rosen, S. (1981). The economics of superstars. *American Economic Review*, 71(5), 845–858.** https://www.jstor.org/stable/1803469
Access: [secondary].

Small differences in quality become large differences in reward under scale economies in consumption. This is economics' main **static** explanation of concentration: heterogeneity in quality, the stationary cousin of rival (b). One clause in Introduction paragraph 2 if space allows; otherwise SI.

**6.3 [Tier 3] Brynjolfsson, E., Hu, Y. J., & Simester, D. (2011). Goodbye Pareto principle, hello long tail: The effect of search costs on the concentration of product sales. *Management Science*, 57(8), 1373–1386.** https://doi.org/10.1287/mnsc.1110.1371
**With** Fleder, D., & Hosanagar, K. (2009). Blockbuster culture's next rise or fall: The impact of recommender systems on sales diversity. *Management Science*, 55(5), 697–712. https://doi.org/10.1287/mnsc.1080.0974
Access: [memory, unverified] for content; metadata verified.

These are the long-tail and recommender arguments: search and recommendation are design levers that raise or lower concentration. Background for Discussion paragraph 2.

**6.4 [Tier 3] Prat, A., & Valletti, T. (2022). Attention oligopoly. *American Economic Journal: Microeconomics*, 14(3), 530–557.** https://doi.org/10.1257/mic.20200134
Access: [abstract only].

Platforms broker attention. Merger review based only on aggregate usage can mislead. In competition economics, **shares of attention, not ranks**, are the policy object. Use it to aim the Discussion's "rank is a poor measure of entrenchment" (l. 188) at rank-based accounts (popular rankings; rank-stability statistics), not at the economics practice, which already uses shares.

**6.5 [Tier 3] Nieborg, D. B., & Poell, T. (2018). The platformization of cultural production: Theorizing the contingent cultural commodity. *New Media & Society*, 20(11), 4275–4292.** https://doi.org/10.1177/1461444818769694
Access: [memory, unverified] for content; metadata verified.

Producers' fortunes depend on platform governance and changes to algorithms. This is the cultural-industries framing of rival (c). Optional.

**6.6 [Tier 3] Machado, G., Pacheco, D., Menezes, R., & Baxter, G. (2025). Super-linear growth and rising inequality in online social communities: Insights from Reddit. arXiv:2503.02661.** https://arxiv.org/abs/2503.02661
Access: [abstract only].

Within subreddits, participation is more concentrated in larger communities (Gini rises with size; comments scale super-linearly with users). This bears on reading item counts at the head (SI R8) and on the draft's caveat that "item counts measure posting and commenting, not participants" (l. 190).

### 7. Explanation against prediction

**7.1 [Tier 2] Hofman, J. M., Watts, D. J., Athey, S., et al. (2021). Integrating explanation and prediction in computational social science. *Nature*, 595, 181–188.** https://doi.org/10.1038/s41586-021-03659-0
Access: [abstract only, through secondary summaries].

*Core insight.* A schema that places work by how explanatory it is and how much it tests predictions, and a case for integrative modelling.

*Relevance.* The draft's model "forecasts held-out rank movement no better than simpler models" and claims explanation and counterfactual (ll. 157–160). This is the accepted vocabulary for that position.

*Actionable.* One clause in the paragraph on held-out movement (l. 160) or in the Discussion.

**7.2 [Tier 3] Martin, T., Hofman, J. M., Sharma, A., Anderson, A., & Watts, D. J. (2016). Exploring limits to prediction in complex social systems. *Proceedings of WWW '16*, 683–694.** https://doi.org/10.1145/2872427.2883001
Access: [full text], arXiv:1602.01013, abstract read.

The best models explain less than half the variance in cascade sizes even with rich data, which suggests bounds on predictability. This **supports** reporting the null forecasting result plainly. The quotation is in C1.6.

**7.3 [Tier 3; data rather than literature] Horta Ribeiro, M., & West, R. (2021). YouNiverse: Large-scale channel and video metadata from English-speaking YouTube. *ICWSM* (arXiv:2012.10378).** https://arxiv.org/abs/2012.10378
Access: [abstract only].

136,000 channels with weekly subscriber and view counts. It could serve as an open third platform for the boundary relation, given that "two platforms and one window" is a stated limit. Future work only.

### Cited works engaged only in passing (not "missing")

- **`bentley2007`:** used only as a null world. Its main result, the turnover profile, is the object of Fig. 2B. See 2.1.
- **`iniguez2022`:** used only for C(R). Its result that flux sets stability, and its displacement–replacement model, are rivals for Fig. 2B. See 2.2.
- **`salganik2006`:** cited for cumulative advantage. Its finding is that social influence raises both inequality and unpredictability, which bears more on the null forecasting result (l. 160) than on cumulative advantage. Consider moving the citation.
- **`kairam2012`, `cunha2019`, `teblunthuis2022`:** one sentence. Fine, if 3.1 and 3.2 are added.

---

## C. The questions the literature asks

Each question below gives:
- one or two short exact quotations, at most one per source in the whole report, each under 15 words and labelled;
- a paraphrase of their context;
- where the draft stands;
- how the draft should frame its relation to the question.

"Verified" means I checked the words against full text, or against a Google Books page snippet, in this session.

### C1. The most common questions

**C1.1 Does digital abundance concentrate or disperse attention?**
- Yang et al. (2020), p. 28678: "the impact that those choices have on overall patterns of news consumption" **[verified: https://ora.ox.ac.uk/objects/uuid:28009285-d663-4c9c-a7d2-3cfaf06d13ca/files/rxk81jk611]**. *Context:* the opening sentences say audiences have more choice, but the effect of that choice on overall consumption is unclear, and past research expected fragmentation.
- Hindman (2009), p. 57: "winners-take-all patterns are repeated on every level of the Web" **[verified: Google Books snippet, https://books.google.com/books?id=qE3F3PnYAr8C]**. *Context:* summing up the chapter on link structure, he argues the concentration is too general to be explained by features peculiar to politics.
- Webster & Ksiazek (2012) frame the same question as audience fragmentation against duplication (paraphrase from the abstract; no quotation).

*Status:* **partly answered.** The draft shows concentration within Reddit falling steadily in 2019–22 and decomposes the fall (participation against per-item engagement). It does not measure exposure.
*Framing:* say this is evidence on concentration of **activity** within one platform. It is not about exposure across outlets, and it does not settle the fragmentation debate. That the head's share fell by a third while the leaders kept rank one is the result this literature will find most interesting.

**C1.2 How stable are rankings, and who stays on top?**
- Iñiguez et al. (2022), abstract: "Far less is known, however, about how rankings change in time." **[verified: https://arxiv.org/abs/2104.13439 (v2 PDF)]**. *Context:* a century of work on aggregated rank–size regularities is contrasted with little work on how rankings move.
- Hindman (2018) asks how stable winners-take-all patterns are and reports that it is rare for a site to enter or leave the top ten (pp. 83–84, 99; paraphrase, pages for checking).

*Status:* **answered**, with a contrast the draft has not drawn. Reddit's rank one is stable, but its top ten turns over yearly, unlike the web sites of 2005–08.
*Framing:* present Eq. 1 as one account of set turnover and rank change on a single ladder (it already says so, l. 118), and add the cross-system contrast (D1).

**C1.3 How fast does collective attention turn over, and is it speeding up?**
- Lorenz-Spreen et al. (2019), p. 1: "Is this the inevitable conclusion of the way information is disseminated and consumed?" **[verified: https://cora.ucc.ie/bitstreams/f4ae8a47-a98a-4a3e-942b-34dbb4a46b9a/download]**. *Context:* after reporting steeper rises and shorter periods of attention across decades and domains, they ask whether this follows from how information spreads. Their model answers yes.
- Candia et al. (2019) ask how attention to cultural products decays, and find a two-timescale (biexponential) decay (paraphrase from the abstract).

*Status:* **could answer with the data on hand**, for communities over three year pairs.
*Framing:* the draft's object is the turnover of containers, not items. Say whether that turnover has a trend, and point out that a slow hierarchy of communities can coexist with fast-moving items.

**C1.4 Why do online communities grow, stabilize, decline and die?**
- Ribeiro (2014), abstract: "general mechanisms that can describe both successes and failures" **[verified: https://arxiv.org/abs/1307.1354 (v3 PDF)]**. *Context:* he argues that studies of website popularity have been biased toward successes, and that a model must explain failures too.
- TeBlunthuis et al. (2018) ask whether Wikipedia's pattern of growth and stabilization holds in other communities, and find that it does on average (abstract; paraphrase). Kairam et al. (2012) and Cunha et al. (2019), already cited, ask which features predict growth and survival.

*Status:* **partly answered.** The draft shows that decline persists and carries the excess slide. It does not say why, and it rightly makes no causal claim.
*Framing:* place the persistence result as the population-level consequence of a documented life-cycle regularity (3.1, 3.2). That makes it more credible and keeps it distinctive: the field has not connected community decline to the shape of the hierarchy.

**C1.5 Do platform interventions (bans, quarantines, ranking) change where activity goes?**
- Cima et al. (2024), abstract: "the effectiveness of many moderation interventions is still unclear" **[verified: https://arxiv.org/abs/2401.11254 (v6 PDF)]**. *Context:* their motivation for studying the 2020 ban wave.
- Huszár et al. (2022), abstract: "these algorithms may amplify some messages while reducing the visibility of others" **[verified: https://arxiv.org/abs/2110.11010]**. *Context:* it sets up the randomized holdout that measures amplification.

*Status:* **cannot answer causally.** The draft can describe what bans did to the head's membership (data on hand).
*Framing:* intervention studies measure effects over weeks to months on users or items. The draft adds that structural effects on concentration may take years (relaxation times). This is a reason the two literatures should meet, not a causal finding.

**C1.6 How predictable is success?**
- Martin et al. (2016), abstract: "How predictable is success in complex social systems?" **[verified: https://arxiv.org/abs/1602.01013]**. *Context:* they argue the question had not been specified well enough, then bound the predictability of cascades.

*Status:* **answered for movement.** The model does no better than simple rules at 1–13 weeks.
*Framing:* this literature makes the draft's null forecasting result unsurprising and respectable. Cite it, and keep the result in the main text.

### C2. Meta-questions and implicit questions

**C2.1 Is the internet a levelling force or a concentrating one, and is concentration temporary?** This question organizes communication and political-communication work on attention, often without being stated.
- Hindman (2018), p. 93: "the funnel of churn actually produces and maintains the power law patterns" **[verified: Google Books snippet, https://books.google.com/books?id=3PHkDwAAQBAJ]**. *Context:* after documenting high churn and a stable power law, he proposes that the churn sustains the structure. On p. 100 he adds that claims of outward diffusion of audiences assume the pattern of churn will change (paraphrase).
- Webster (2014), p. 163: "the cultural ballast that pulls society together" **[verified: Google Books snippet, https://books.google.com/books?id=oL34DwAAQBAJ]**. *Context:* he argues that popularity, valued by media and users together, keeps the shape of attention distributions durable.

*Status:* the draft **answers** a sharpened version: whether concentration is durable, and through what. Its answer has two parts:
- the shape is automatic in random growth;
- the change in concentration came through persistent decline in participation at the head.

*Framing:* acknowledge that the qualitative answer exists (Hindman 2018). Present the contribution as measuring it, and separating what is automatic from what is not.

**C2.2 Are patterns in platform traces facts about people, or about the platform and the instrument?**
- Lazer et al. (2014), p. 1204: "Is measurement stable and comparable across cases and over time?" **[verified: https://cdanfort.w3.uvm.edu/csc-reading-group/lazer-flu-science-2014.pdf]**. *Context:* it opens their discussion of algorithm dynamics.
- Yang et al. (2020) show that desktop-only data reverse the measured trend in fragmentation (Fig. 2 legend, p. 28680; paraphrase).

*Status:* **partly addressed.** The draft dates the June 2021 step, separates counts from karma, and audits two confounders. It does not audit all capture effects, as Limits says.
*Framing:* name the rival as platform effects. Say which results rest on counts (the persistence) and which on karma (the slope and the balance).

**C2.3 What is the right unit and population: a few large communities or populations of them; items or containers?**
- TeBlunthuis et al. (2018), p. 355:1: "studied a small number of the largest communities" **[verified: https://Www.Gwern.net/doc/wikipedia/2018-teblunthuis.pdf (mirror of the CHI paper)]**. *Context:* they criticize peer-production research for generalizing from Wikipedia.
- Zhang et al. (2017), abstract: "mostly limited to glimpses gathered from isolated studies of individual communities" **[verified: https://arxiv.org/abs/1705.09665]**. *Context:* this is why they build a typology across about 300 subreddits.

*Status:* **answered by design.** The draft studies whole top-K populations (5,000 and 12,500) with absence-penalized membership.
*Framing:* this is a strength the draft undersells. One sentence in the Introduction or Methods can say it studies populations of communities rather than cases, which is what this literature has asked for.

**C2.4 Did the 2020 shock reorganize platforms or only perturb them?**
- Veselovsky & Anderson (2023), abstract: "whether they were temporary or long-lasting" **[verified: https://arxiv.org/abs/2304.10777]**. *Context:* they say researchers lack a rigorous account of the pandemic's effects on platforms and of whether those effects lasted.

*Status:* **could partly answer with the data on hand.** The comparisons after the surge exist. The draft's persistence holds when windows begin after July 2020.
*Framing:* cite it. Treat the window as unusual, not typical. Turn the surge into an asset: it is a natural shock against which the relaxation of the hierarchy could be read.

### C3. Questions the literature is not asking, but should

**C3.1 What sets the steepness of an attention hierarchy, as distinct from who occupies it?**
The concentration literature measures shares, Gini coefficients and HHI. The turnover literature measures churn. Only Hindman (2018) connects them. Nobody measures the two ingredients of steepness (volatility and relative decline) rank by rank. This matters because every claim that a platform "concentrates" attention is a claim about steepness. Without the balance, a reader cannot tell whether a change in concentration reflects changed dynamics or a ladder relaxing toward its existing balance.
*Status:* the draft **answers** this. It is the paper's strongest framing for a PNAS audience.

**C3.2 How long does concentration take to respond to a platform change?**
Studies of interventions (C1.5) and of the pandemic (C2.4) measure effects over weeks to two years. None asks about the relaxation time of the hierarchy itself. This matters because if the time scale is years to decades (the draft's 9 years at the comments head, 3–4 at the submissions head, linearized), short-window evaluations of design changes understate or misdate their structural effects.
*Status:* the draft **answers** this in a linearized, post hoc form.
*Framing:* keep it conditional. Hindman (2018, p. 99) warns that long runs of such models compound misspecification, which matches the draft's own caution (l. 190).

**C3.3 Does a change in concentration come from production (posting) or from reception (engagement per item)?**
Exposure studies (Yang; Webster) and engagement studies (Sangiorgio; Huszár) rarely split the two. This matters for causal stories: a head that posts less is a different phenomenon from a head whose posts earn less.
*Status:* the draft **answers** this exactly: its split of shares into items and per-item engagement (SI R8) shows the submissions head's fall was in posting.
*Framing:* this result is undersold. Give it one sentence in the abstract or Significance, or keep it where it is but name the question.

**C3.4 Is rank stability a measure of dominance?**
Ranking studies (Iñiguez; Blumm) and popular accounts of platform power often treat a stable top rank as entrenchment. Competition economics uses shares (Prat & Valletti). Nobody shows that the two diverge for the same units. This matters because, on Reddit, the leaders kept rank one while losing more than half their share (r/memes 7.0% to 1.8%; r/AskReddit 11.3% to 4.9%).
*Status:* the draft **answers** this with named examples (l. 100, l. 188).
*Framing:* aim the claim at rank-based readings and point out that share-based measures already exist (D5).

### C4. Implied questions

**C4.1 Against what null should claims of bias, amplification or "winner-take-all" be judged?**
This follows from C1.1 and C1.5. If random growth produces steep, stable hierarchies automatically, then a steep hierarchy is not by itself evidence of algorithmic favouritism or cumulative advantage.
- Acerbi & Bentley (2014), Discussion: "biases that select popular traits produce 'convex' turnover profiles" **[verified: https://bura.brunel.ac.uk/bitstream/2438/20332/2/FullText.pdf]**. *Context:* they summarize how departures from the neutral model's turnover profile reveal transmission biases.

*Status:* the draft **answers** this through its null worlds and the geometry result.
*Framing:* say that profile-based inferences of bias need a control for the ladder (2.1).

**C4.2 Is community decline self-reinforcing, and through which channel?**
This follows from C1.4: if success breeds participation, decline may breed decline. Ribeiro (2014) and Halfaker et al. (2013) give mechanisms (no further quotation, to keep one per source).
*Status:* **cannot be answered causally with community-level data**. The decliners' split (R10) is consistent with self-reinforcement but post hoc. Testing it would need user-level data, which the data statement excludes.

**C4.3 If platform measurement changes with the platform, what can confirm a dynamic?**
This follows from C2.2.
- Wagner et al. (2021), p. 198: "the theoretical constructs we aim to measure, predict and explain are often unstable" **[verified: https://dlab.epfl.ch/teaching/spring2022/cs727/papers/wagner2021measuring.pdf; reference number omitted]**. *Context:* they argue that algorithmic platforms change the very constructs being measured.

*Status:* the draft's answer is the sealed 2023–24 test written in advance. As SI notes, its second change spans the 2023 source switch and the API blackout, which is exactly this problem.
*Framing:* cite Wagner et al. where the protocol is mentioned (l. 190), and say the amendment is needed for this reason.

---

## D. Synthesis for the draft

### D1. The five most valuable changes to framing and positioning

**1. Position the paper against Hindman (2018, ch. 5) explicitly** (Introduction paragraphs 2–3; Discussion paragraph 1; size and turnover paragraphs).

*The issue.* The chapter (with Bruce Rogers) already:
- poses the paradox of a stable structure with churning members;
- proposes that churn sustains the power law;
- models web traffic as a stochastic dynamical system from finance, citing Fernholz;
- simulates from several initial conditions, one of them flat.

Under single-blind review, the omission will read as either an oversight or overclaiming.

*Smallest change.*
- After `huberman1999` in paragraph 2, add: "and web traffic keeps a stable rank–share structure through daily churn, which has been modelled with stochastic dynamical systems from finance [Hindman 2018]".
- In paragraph 3, replace "We take these ideas to online attention" with wording that says what is added: measurement rank by rank with a tested crossing identity; lasting movement separated from passing; entry and exit; null worlds; persistence.
- Add two contrast sentences, which make the paper stronger:
  - "Web sites' volatility fell steeply with size [Hindman 2018]; Reddit communities' lasting volatility hardly does" (l. 116).
  - "On the web of 2005–08 the top ten rarely changed [Hindman 2018]; on Reddit a quarter to a third of it is replaced each year" (l. 114).

*Proposed check.* Is the web's "funnel" partly passing variance? The draft's calibration (planted 0 gives raw 0.06) suggests passing swings cannot produce a large funnel, so the contrast is probably real. Compute or quote the web's implied β to make it quantitative.

**2. Recast "the top turns over fastest because the ladder is flattest" as a contribution to the turnover-profile literature** ("The top turns over fastest…", paragraph 1; SI "Null worlds").

*The issue.* Cultural evolution has used turnover profiles for 15 years to infer copying biases (Bentley 2007; Evans & Giometto 2011; Acerbi & Bentley 2014). Ranking dynamics reads them as stability (Iñiguez 2022). The draft's R9 shows that, with dynamics held fixed, the ladder's shape alone changes the profile.

*Smallest change.*
- One sentence and two citations in the text.
- In the SI, a fitted profile exponent b for the data and each world, with the neutral benchmark of 0.86.
- A note on the regime of the neutral-copying world.

This turns a defensive result (it is not size-blindness) into a positive one (profile shape is confounded with geometry).

**3. Treat the 2019–22 platform shocks with the platform literature, not only with the draft's own audits** (persistence paragraph 1, l. 168; head-decline paragraph, l. 182; Limits, l. 190).

*Smallest change.*
- Cite Veselovsky & Anderson (2023) for the surge: growth spread across communities; a lasting rise of comments and active subreddits; a relative decline of submissions after 2020.
- Cite Cima et al. (2024) for the June 2020 ban wave.
- Cite Lazer et al. (2014) and Malik & Pfeffer (2016) to name rival (c) as platform effects.
- In Limits, sort the results into those that rest on counts and those that rest on karma.

The fall of the submissions head's posting (33M to 17M) should be stated alongside the platform-wide relative decline of submissions. Otherwise a referee will supply it.

**4. Anchor the persistence result in the life-cycle literature on online communities** (Persistence paragraph 2, l. 171).

*Smallest change.*
- Replace the sentence that leads with firm growth ("It is familiar elsewhere: the serial correlation of firm growth…") with one that leads with online communities: rise and decline (Halfaker 2013; TeBlunthuis 2018), activity that sustains itself or decays (Ribeiro 2014), identity and retention (Zhang 2017), and platform-level inertia with long-run trends in the same years (Xu & Peng 2024).
- Keep the GLLM analogy in the Discussion, where it works.

This answers rival (b) with a known dynamic alternative (life-cycle stage). It makes the result read as a measured consequence of a documented regularity, not as an anomaly against homogeneous worlds.

**5. Bring forward the deconcentration result and its participation channel, and connect them to the concentration and fragmentation debate** (Introduction paragraph 1; Discussion paragraph 2).

*The issue.* For a PNAS social-science reader, the arresting facts are:
- Reddit's head lost a third of its share in three years;
- the leaders kept rank one while their shares halved;
- the fall was in posting, not in engagement per post.

These speak to the field's most common question (C1.1) and to the rank-versus-share question it has not asked (C3.4).

*Smallest change.*
- One sentence in Introduction paragraph 1 citing Webster & Ksiazek (2012) and Yang et al. (2020).
- In Discussion paragraph 2, rewrite "Rank is a poor measure of entrenchment" as: "Stable rank is a poor measure of entrenchment: shares, the measure competition economics uses [Prat & Valletti 2022], fell by more than half for leaders that kept rank one."
- Make the platform-design sentence conditional and cite Huszár et al. (2022).

*Page budget.* The additions come to about 150–200 words. To pay for them:
- shorten Introduction paragraph 2's firm-growth sentence and its stochastic-portfolio-theory list (keep the citations, cut a clause each);
- move the Facebook β details at l. 116 to the SI.

### D2. What the literature prevents the paper from claiming

1. **That the question, or its qualitative answer, is new.** That churn can sustain a stable attention hierarchy was argued and modelled for web traffic (Hindman 2018). Rank–size stability with churning members is documented for cities (`batty2006`) and for rankings in general (`iniguez2022`).
2. **That weak size dependence of volatility is a general property of online attention.** Web sites showed strong size dependence (Hindman 2018), and web traffic shows fluctuation scaling (`menezes2004`, `eisler2008`). The claim is about Reddit communities' lasting component, and is estimator-dependent, as the draft says.
3. **That turnover rates are platform constants.** Turnover of attention to items has risen across a decade (Lorenz-Spreen 2019), and the window contains Reddit's largest shock (Veselovsky & Anderson 2023).
4. **That persistent decline is "the clearest departure from random growth" in a sense the field would find new.** Self-reinforcing decline is a documented phase of online communities' life cycles. The new part is its measured weight in the change of concentration. Keep "clearest departure in these data", which is accurate, and cite the life-cycle work.
5. **That the neutral-copying world represents neutral models generally.** Turnover profiles depend on the model and its regime (Evans & Giometto 2011).
6. **That the platform-design implications are findings.** Amplification and moderation effects are identified with holdouts or quasi-experiments (Huszár 2022; Chandrasekharan 2017; Cima 2024). The draft's implications follow from its accounting and should stay conditional.
7. **That karma measures attention.** The draft already says it measures engaged activity. Exposure-based work (Yang 2020) shows that measured concentration can reverse with the instrument.

### D3. Where the paper is more distinctive than its framing shows

These are not priority claims. They are places where the draft offers something the literatures above lack.

1. **Turnover profiles separated from geometry.** Eq. 1 and R9 show, with dynamics held fixed, that the shape of a ranked list's turnover profile follows its local slope. This bears directly on a literature (2.1) that reads profile shape as a behavioural signature.
2. **An exact production-against-reception split of a fall in concentration within one platform**, for named communities, with the persistence that carries it. Exposure research rarely sees production; engagement research rarely decomposes concentration.
3. **Rank stability with collapsing share for the same leaders.** This is concrete evidence on a measurement question that competition and platform-power debates have not addressed directly.
4. **Every statistic computed in the data and in null worlds with identical code.** This practice is largely absent from platform studies of concentration and turnover. Say so once, in Methods or the Introduction, as part of what the paper offers the field.
5. **Populations of communities rather than cases** (C2.3). This answers a methodological demand the online-community literature has made.

### D4. Checked against proposed

**Checked in this session:**
- the existence and bibliographic details of every work listed (Crossref, publisher or repository pages);
- every quotation in Section C, against full text or a Google Books page snippet;
- Hindman (2018): the chapter title and co-author, pages 82–101, the notes citing Fernholz (p. 199), and the statements paraphrased in 1.1 (pp. 9, 84, 89, 91–93, 95–100);
- the quantitative statements I attribute to Lorenz-Spreen, Veselovsky & Anderson, Cima et al., Yang et al., Lazer et al., Wagner et al., Acerbi & Bentley, Ribeiro and TeBlunthuis et al., in their full texts.

**Proposed for the authors to check:**
- Hindman's pagination in the 2018 hardback.
- The content of DiPrete & Eirich (2006), Brynjolfsson et al. (2011), Fleder & Hosanagar (2009), Nieborg & Poell (2018) and Chandrasekharan et al. (2022), all [memory, unverified].
- The profile exponent b for the data and the worlds; my figure of about 0.9 is a rough reading of Fig. 2B.
- How much of the head's v_k in 2020–21 the June 2020 bans account for.
- Whether the persistence statistic holds in communities without unusual growth in 2020.
- Whether persistent decliners skew older.
- The web's implied β, for the contrast in D1.1.
- The published wording of the arXiv-verified quotations.

---

## E. Search log

All searches and retrievals were made on 2026-10-19. Main queries, in order:

1. Hindman's *Internet Trap*: "dynamics of web traffic", churn and stable distribution; "musical chairs"; Fernholz and stochastic portfolio theory. Also Google Books search-inside on `id=3PHkDwAAQBAJ` for: "musical chairs", "in spite of the constant", "volatility", "smaller sites", "initial condition", "run forever", "stock market", "sustains the inequality", "unacknowledged", "constant market churn", "Dynamics of Web Traffic", "how stable are these", "stochastic".
2. Hindman's *Myth of Digital Democracy* (`id=qE3F3PnYAr8C`): "winners-take-all", googlearchy and "power law".
3. Webster's *Marketplace of Attention* (`id=oL34DwAAQBAJ`): "power law", "basic shape", "turnover", "massive concentrations". Webster & Ksiazek 2012 (Crossref).
4. Lorenz-Spreen et al. 2019; Candia et al. 2019; Yang et al. 2020 (ORA PDF).
5. Turnover profiles: Evans & Giometto (arXiv); Acerbi & Bentley 2014 (Brunel PDF); Ordanini & Nunes 2016; Billboard turnover (Pessa et al. 2026, arXiv:2607.11446, seen but not listed).
6. Reddit community life cycles: TeBlunthuis 2018 (PDF); Halfaker 2013; Ribeiro 2014 (arXiv); Zhang et al. 2017 (arXiv); Lin et al. 2017; TeBlunthuis 2025; Kraut & Resnick; Panek et al. 2018 (seen, not listed).
7. Platform shocks: COVID and Reddit (Veselovsky & Anderson, arXiv); the Great Ban (Cima et al., arXiv); Chandrasekharan 2017 and 2022; Horta Ribeiro 2021; Reddit feeds (Chan et al. 2025 and 2026; Moehring, Management Science, seen through a search summary and not listed).
8. Measurement: Lazer et al. 2014 (UVM PDF); Wagner et al. 2021 (EPFL PDF); Malik & Pfeffer 2016 (abstract; PDF returned 503); Pushshift score capture (non-scholarly results, not used).
9. Economics and sociology: Prat & Valletti 2022; Boik, Greenstein & Prince 2016 (NBER w22427, abstract only, not listed); Rosen 1981; Brynjolfsson et al. 2011; Fleder & Hosanagar 2009; DiPrete & Eirich 2006; Nieborg & Poell 2018 (Crossref).
10. Attention inequality: Zhu & Lerman 2016 (seen, not listed); Pecile et al. 2026; Machado et al. 2025; Nelson 2020; Xu & Peng 2024.
11. Searches with no result useful here: "stochastic portfolio theory OR Atlas model applied to web traffic or social media", which found nothing beyond Hindman; "subreddit rank–size stability", which found no direct study; Facebook's 2021 reduction of political content, which found only news reports.
12. Explanation and prediction: Hofman et al. 2021 (abstract); Martin et al. 2016 (arXiv).

Databases and tools: WebSearch (general web), arXiv, publisher and repository pages (ORA, DASH/UVM, Brunel BURA, UCC CORA, EPFL), Crossref API, Google Books search-inside (built-in browser, read-only).