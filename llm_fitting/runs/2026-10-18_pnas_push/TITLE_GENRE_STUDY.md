_Title study by a Claude Code subagent (general-purpose; no prior context), 2026-10-18, at the owner's request ("send a subagent to review the top 10-ish most-cited PNAS articles each from 2020-2024 … pick a headline"). Data: OpenAlex API and Crossref, accessed 2026-10-18; article types checked against PMC JATS headings. Saved verbatim by the orchestrating session, which spot-checked the 2023 OpenAlex ranking (top three titles and counts match). Model output, not a record of the owner's choice._

# PNAS title study and a title for the rank-diffusion paper

## 1. Method and sources

**Source and date.** All data were pulled on **2026-10-18 (about 08:00 UTC)**. I called the OpenAlex REST API directly with `curl`, not through WebFetch; it is the same API and needs no key. I confirmed the PNAS source id at `api.openalex.org/sources?filter=issn:0027-8424`, which returns S125754415, "Proceedings of the National Academy of Sciences", ISSNs 0027-8424 and 1091-6490. The query for each year was:
`works?filter=primary_location.source.id:S125754415,publication_year:YYYY,type:article&sort=cited_by_count:desc&per-page=40`.
I ran the same query with `type:!article` to catch research papers that OpenAlex mis-types. For every candidate I also took Crossref's `is-referenced-by-count` on the same day as a second count. I did not write anything to the repository; scratch files are in the session scratchpad only.

**Checking article type.** OpenAlex "article" includes PNAS Perspectives and Special Feature introductions, and PNAS's JATS `article-type` is "research-article" for all of them. Judging by title alone would have been unreliable. So for every candidate I pulled the PMC full-text XML (Europe PMC REST, and NCBI E-utilities `efetch db=pmc` as a fallback) and read the `<article-categories>` headings. These carry "Perspective", "Introduction" and the PNAS section, for example "Social Sciences / Psychological and Cognitive Sciences". pnas.org itself returns HTTP 403 to scripted requests, so I did not use landing pages.

**Problems and decisions.**
- **13 non-research items removed from the top-10 windows.** They are 11 Perspectives, 1 Special Feature Introduction and 1 Special Feature Perspective, listed below. One of them, "The changing risk and burden of wildfire in the United States", reads like research but is headed "Perspective" in its JATS.
- **One research article OpenAlex hides.** OpenAlex types the nudging meta-analysis (2021) as "review", but its PNAS heading is a research section, so I included it. The asymptomatic SARS-CoV-2 meta-analysis (488 citations) was also mis-typed, but it falls below the 2021 cut-off.
- **One OpenAlex count error.** "IFI16-dependent STING signaling is a crucial regulator of anti-HER2 immune response in HER2+ breast cancer" (2022) has 572 citations in OpenAlex, against 46 in Crossref and 45 in Europe PMC. I treated this as a citation-linking error and excluded the paper.
- **Ranks near the cut-off depend on the source.** The two counts agree within about 15% for most papers. The exceptions are 2024 #4, the theory-of-mind paper (OpenAlex 335, Crossref 210), and several COVID papers from 2020 (OpenAlex higher). Using Crossref would reorder ranks 8–12 in some years.
- **Inaugural Articles and Brief Reports.** No Inaugural Article reached a top-10 window. Brief Reports are research and are kept (flagged †).
- **Titles cleaned up.** OpenAlex breaks subscripts and superscripts ("Co 3 O 4", "CD8 +"); I restored "Co3O4" and "CD8+". Otherwise titles are exactly as OpenAlex gives them.
- **Field labels.** "PNAS section" is the first major class in the article's own JATS headings (Bio, Phys or Soc). "Group" is my topic coding: BIO = biomedical and molecular; CHEM = chemistry, physics and engineering; ENV = environment, earth and ecology; SOC = social, behavioral and computational social science. PNAS files several environmental papers under Social Sciences / Sustainability Science, so the two labels sometimes differ.

**Excluded from the top-10 windows** (OpenAlex citations in brackets):
- 2021: *Insect decline in the Anthropocene: Death by a thousand cuts* (1943, Special Feature Introduction); *An evidence review of face masks against COVID-19* (1530, Perspective); *The persistent threat of emerging plant disease pandemics to global food security* (822, Perspective); *Agricultural intensification and climate change are rapidly decreasing insect biodiversity* (802, Special Feature Perspective); *The changing risk and burden of wildfire in the United States* (694, Perspective); *The crucial role of genome-wide genetic variation in conservation* (608, Perspective); *Defining and quantifying the core microbiome: Challenges and prospects* (605, Perspective).
- 2022: *Climate Endgame: Exploring catastrophic climate change scenarios* (792, Perspective); the IFI16 paper (count error); *Sequence locally, think globally: The Darwin Tree of Life Project* (558, Perspective); *Ten facts about land systems for sustainability* (459, Perspective).
- 2023: *The debate over understanding in AI's large language models* (339, Perspective).
- 2024: *Endosomal escape: A bottleneck for LNP-mediated therapeutics* (580, Perspective); *Can Generative AI improve social science?* (366, Perspective).
- 2020: none. The first non-research item is at OpenAlex position 14 (*The frontier of simulation-based inference*, a Colloquium review).

## 2. The corpus (50 research articles)

Rank is among research articles within the year, by OpenAlex count. "OA" is the OpenAlex `cited_by_count` and "CR" the Crossref `is-referenced-by-count`, both read on 2026-10-18. DOIs all begin `10.1073/pnas.`. † = Brief Report; MA = meta-analysis.

**2020** (10th place = 1235 OA citations)

| # | Title | OA | CR | PNAS section | Group | DOI |
|---|---|---|---|---|---|---|
| 1 | RepeatModeler2 for automated genomic discovery of transposable element families | 3813 | 4147 | Bio | BIO | 1921046117 |
| 2 | Cell entry mechanisms of SARS-CoV-2 | 3131 | 3022 | Bio | BIO·COVID | 2003138117 |
| 3 | Effective treatment of severe COVID-19 patients with tocilizumab | 2648 | 1945 | Bio | BIO·COVID | 2005615117 |
| 4 | Effectiveness of convalescent plasma therapy in severe COVID-19 patients | 1961 | 1478 | Bio | BIO·COVID | 2004168117 |
| 5 | The impact of COVID-19 on small business outcomes and expectations | 1607 | 1186 | Soc (Econ) | SOC·COVID | 2006991117 |
| 6 | Land use and climate change impacts on global soil erosion by water (2015-2070) | 1468 | 1357 | Soc (Sust.) | ENV | 2001403117 |
| 7 | Improved protein structure prediction using predicted interresidue orientations | 1413 | 1455 | Bio | BIO | 1914677117 |
| 8 | Enhanced isolation of SARS-CoV-2 by TMPRSS2-expressing cells † | 1412 | 1435 | Bio | BIO·COVID | 2002589117 |
| 9 | Active learning narrows achievement gaps for underrepresented students in undergraduate science, technology, engineering, and math (MA) | 1374 | 1172 | Soc (Psych) | SOC | 1916903117 |
| 10 | Identifying airborne transmission as the dominant route for the spread of COVID-19 ‡ | 1235 | 1028 | Soc/Phys | ENV·COVID | 2009637117 |

‡ PNAS published a correction for this paper (OpenAlex lists "Correction for Zhang et al."), and it was publicly contested. It is not retracted, as far as I know.

**2021** (10th place = 578)

| # | Title | OA | CR | PNAS section | Group | DOI |
|---|---|---|---|---|---|---|
| 1 | Biological structure and function emerge from scaling unsupervised learning to 250 million protein sequences | 3260 | 3328 | Bio+Phys | BIO | 2016239118 |
| 2 | The echo chamber effect on social media | 2060 | 1792 | Phys (CS)+Soc | SOC | 2023301118 |
| 3 | Learning loss due to school closures during the COVID-19 pandemic | 1733 | 1102 | Soc | SOC·COVID | 2022376118 |
| 4 | Machine learning–accelerated computational fluid dynamics | 1045 | 1016 | Phys | CHEM | 2101784118 |
| 5 | On the mechanism of tissue-specific mRNA delivery by selective organ targeting nanoparticles | 1007 | 1023 | Phys (Eng)+Bio | BIO·LNP | 2109256118 |
| 6 | People have shaped most of terrestrial nature for at least 12,000 years | 838 | 758 | Soc (Sust.) | ENV | 2023483118 |
| 7 | Global urban population exposure to extreme heat | 756 | 718 | Soc (Sust.) | ENV | 2024792118 |
| 8 | The effectiveness of nudging: A meta-analysis of choice architecture interventions across behavioral domains (MA; OpenAlex type "review") | 740 | 655 | Soc (Psych) | SOC | 2107346118 |
| 9 | Lifestyle and mental health disruptions during COVID-19 | 589 | 488 | Soc (Econ) | SOC·COVID | 2016632118 |
| 10 | Constraining the atmospheric limb of the plastic cycle | 578 | 545 | Phys | ENV | 2020719118 |

**2022** (10th place = 355)

| # | Title | OA | CR | PNAS section | Group | DOI |
|---|---|---|---|---|---|---|
| 1 | Pharmaceutical pollution of the world's rivers | 1660 | 1540 | Phys | ENV | 2113947119 |
| 2 | Biodiversity impacts and conservation implications of urban land expansion projected to 2050 | 827 | 765 | Soc (Sust.)+Bio | ENV | 2117297119 |
| 3 | Lung-selective mRNA delivery of synthetic lipid nanoparticles for the treatment of pulmonary lymphangioleiomyomatosis | 505 | 515 | Bio+Phys | BIO·LNP | 2116271119 |
| 4 | Identification of Fenton-like active Cu sites by heteroatom modulation of electronic density | 452 | 342 | Phys (Chem) | CHEM·cat | 2119492119 |
| 5 | Lipid nanoparticle-mediated lymph node–targeting delivery of mRNA cancer vaccine elicits robust CD8+ T cell response | 423 | 404 | Bio+Phys | BIO·LNP | 2207841119 |
| 6 | AI-synthesized faces are indistinguishable from real faces and more trustworthy † | 389 | 365 | Soc (Psych) | SOC·AI | 2120481119 |
| 7 | Full adoption of the most effective strategies to mitigate methane emissions by ruminants can help meet the 1.5 °C target by 2030 but not 2050 | 387 | 389 | Bio (Sust.) | ENV | 2111294119 |
| 8 | Electron delocalization triggers nonradical Fenton-like catalysis over spinel oxides | 372 | 370 | Phys (Chem) | CHEM·cat | 2201607119 |
| 9 | Gender-diverse teams produce more novel and higher-impact scientific ideas | 360 | 336 | Soc | SOC | 2200841119 |
| 10 | Calibration-free, high-precision, and robust terahertz ultrafast metasurfaces for monitoring gastric cancers | 355 | 354 | Phys | CHEM | 2209218119 |

**2023** (10th place = 295)

| # | Title | OA | CR | PNAS section | Group | DOI |
|---|---|---|---|---|---|---|
| 1 | ChatGPT outperforms crowd workers for text-annotation tasks † | 1171 | 1062 | Soc (Pol. Sci.) | SOC·AI | 2305016120 |
| 2 | Using cognitive psychology to understand GPT-3 | 552 | 470 | Soc+Phys (CS) | SOC·AI | 2218523120 |
| 3 | Enumerating soil biodiversity | 506 | 520 | Bio (Ecol.) | ENV | 2304663120 |
| 4 | Farmland practices are driving bird population decline across Europe | 497 | 513 | Bio (Ecol.) | ENV | 2216573120 |
| 5 | Oxygen doping of cobalt-single-atom coordination enhances peroxymonosulfate activation and high-valent cobalt–oxo species formation | 442 | 437 | Phys | CHEM·cat | 2219923120 |
| 6 | Generating dual-active species by triple-atom sites through peroxymonosulfate activation for treating micropollutants in complex water | 431 | 430 | Phys | CHEM·cat | 2300085120 |
| 7 | Climate change will accelerate the high-end risk of compound drought and heatwave events | 399 | 390 | Phys (Stat.) | ENV | 2219825120 |
| 8 | Human heuristics for AI-generated language are flawed | 334 | 324 | Soc+Phys (CS) | SOC·AI | 2208839120 |
| 9 | The optimized Fenton-like activity of Fe single-atom sites by Fe atomic clusters–mediated electronic configuration modulation | 313 | 306 | Phys | CHEM·cat | 2300281120 |
| 10 | The total mass, number, and distribution of immune cells in the human body | 295 | 317 | Bio | BIO | 2308511120 |

**2024** (10th place = 209; these papers have had only about 1–2.5 years to collect citations)

| # | Title | OA | CR | PNAS section | Group | DOI |
|---|---|---|---|---|---|---|
| 1 | Rapid single-particle chemical imaging of nanoplastics by SRS microscopy | 460 | 448 | Phys+Bio | CHEM | 2300582121 |
| 2 | Global trends in antibiotic consumption during 2016–2023 and future projections through 2030 | 440 | 456 | Bio | BIO | 2411919121 |
| 3 | Fenton-like activity and pathway modulation via single-atom sites and pollutants comediates the electron transfer process | 389 | 393 | Phys | CHEM·cat | 2313387121 |
| 4 | Evaluating large language models in theory of mind tasks (counts disagree) | 335 | 210 | Soc+Phys | SOC·AI | 2405460121 |
| 5 | GPT is an effective tool for multilingual psychological text analysis | 292 | 307 | Soc (Psych) | SOC·AI | 2308950121 |
| 6 | KAT8-catalyzed lactylation promotes eEF1A2-mediated protein synthesis and colorectal carcinogenesis | 279 | 300 | Bio | BIO | 2314128121 |
| 7 | A Turing test of whether AI chatbots are behaviorally similar to humans | 252 | 226 | Soc (Econ) | SOC·AI | 2313925121 |
| 8 | AI can help people feel heard, but an AI label diminishes this impact | 242 | 241 | Soc (Psych) | SOC·AI | 2319112121 |
| 9 | Utilizing the oxygen-atom trapping effect of Co3O4 with oxygen vacancies to promote chlorite activation for water decontamination | 226 | 232 | Phys | CHEM·cat | 2319427121 |
| 10 | Reassessing the role of urban green space in air pollution control | 209 | 211 | Bio+Phys | ENV | 2306200121 |

**Topic clusters.** Four topics account for 26 of the 50 papers (52%):
- **COVID-19: 8.** Six of 2020's top 10, plus two in 2021.
- **AI and large language models: 8.** One in 2022, then 7 of the 20 papers from 2023–24.
- **Fenton-like / peroxymonosulfate water-treatment catalysis: 7**, all from 2022–24. This is one highly productive subfield whose papers cite each other densely.
- **Lipid-nanoparticle mRNA delivery: 3.**

By PNAS section: Soc 19, Bio 16, Phys 15. By my topic groups: SOC 15, BIO 13, ENV 12, CHEM 10.

## 3. Genre analysis

### Length
- **Words** (split on spaces; hyphenated compounds count as one word): mean 10.6, median 10, SD 3.8, interquartile range 8–13, range 3–25. 42 of 50 titles have 13 words or fewer.
- **Characters**: mean 81, median 74, interquartile range 60–101, range 29–142. Only one title is over 135 characters (the methane title, 142).
- **Shortest:** *Enumerating soil biodiversity* (3 words); *Cell entry mechanisms of SARS-CoV-2* and *Machine learning–accelerated computational fluid dynamics* (5 each).
- **Longest:** the methane title (25); the Co3O4 title (17).
- **By group**, mean (median) words: SOC 9.7 (10), BIO 10.3 (9), ENV 10.9 (11.5), CHEM 12.1 (12.5).
- **By year**, mean words: 2020 9.6; 2021 9.5; 2022 12.2; 2023 10.1; 2024 11.7.
- **By form:** declarative titles run longer (mean 12.1) than noun-phrase titles (9.8).

### Grammatical form

| Form | n | % | Examples |
|---|---|---|---|
| Declarative sentence (finite verb states a finding) | 17 | 34% | *Gender-diverse teams produce more novel and higher-impact scientific ideas*; *ChatGPT outperforms crowd workers…* |
| Noun phrase | 25 | 50% | *The echo chamber effect on social media*; *Pharmaceutical pollution of the world's rivers* |
| Gerund-led phrase | 8 | 16% | *Enumerating soil biodiversity*; *Reassessing the role of urban green space…* |
| Question | 0 | 0% | Only an embedded one: *A Turing test **of whether** AI chatbots…* |
| Colon or two-part | 1 | 2% | *The effectiveness of nudging: A meta-analysis…* |
| Two clauses joined by "but" | 2 | 4% | *AI can help people feel heard, but an AI label diminishes this impact*; methane title ("by 2030 but not 2050") |

- **Half the titles state or imply a result.** That is the 17 declaratives plus 8 noun phrases that carry a claim, such as *Effective treatment…*, *Improved…*, *Enhanced isolation…*, *Learning loss due to school closures…* and *Identifying airborne transmission as the dominant route…*.
- **Declaratives are rising.** By year they number 1, 2, 5, 5 and 4: 3 of 20 in 2020–21 against 14 of 30 in 2022–24. The 2020–21 figure is pulled down by COVID clinical titles and tool papers, which are conventionally noun phrases.
- **Colons and questions belong to Perspectives.** Among the 13 excluded non-research items, 5 have colons (38%) and 1 is a question. Among the 50 research titles, 1 has a colon and none is a question. In PNAS, "Topic: subtitle" and question titles mark a Perspective, not a research paper.

### Verbs
- **Tense.** All 17 declaratives are in the present-time frame: simple present 9 (*narrows, emerge, elicits, triggers, produce, outperforms, enhances, comediates, promotes*), present copula 3 (*are indistinguishable, are flawed, is an effective tool*), modal *can* 2, present progressive 1 (*are driving*), present perfect 1 (*have shaped*), future 1 (*will accelerate*). **None is in the past tense.**
- **Meaning.** 13 of the 17 use causal or effect verbs. The other 4 are evaluative or comparative (*outperforms, indistinguishable/more trustworthy, are flawed, is an effective tool*), and all four are AI-behavior papers in social science.
- **No stock verb.** Apart from *can help* (twice), no verb repeats. Of the verbs suggested in the brief, *reveals*, *predicts* and *increases* never occur; *drives* occurs once (*are driving*) and *shapes* once (*have shaped*). Authors choose the most specific causal verb rather than a formula.
- **"Emerge from".** *Biological structure and function emerge from scaling…* is the corpus's one "emergence" title: a property of a system arising from a simple process. That is close to our claim.

### Wording

| Feature | n | % | Notes |
|---|---|---|---|
| Hedge words | 2 | 4% | Both are "can help". No *may*, *suggest*, *potential* or *associated with* |
| Mechanism words (mechanism, -mediated, via, modulation, pathway, triggers) | 8 | 16% | All BIO or CHEM |
| First person ("we") | 0 | 0% | |
| Any acronym, abbreviation or element symbol | 24 | 48% | Mostly everyday ones: COVID-19, AI, mRNA, (Chat)GPT |
| Specialist-only abbreviations (TMPRSS2, CD8+, KAT8, SRS, Co3O4…) | 8 | 16% | None in SOC or ENV |
| Specialist jargon (my judgment) | 16 | 32% | CHEM 9/10, BIO 7/13, **ENV 0/12, SOC 0/15** |
| Numbers or dates | 6 | 12% | *250 million*, *12,000 years*, *1.5 °C … 2030 … 2050*, *2016–2023* |
| Named place | 1 | 2% | *across Europe* |
| Scope words (global, world's, across, most of, total) | 8 | 16% | Always tied to a measured domain; never *universal* or *law* |

**Specificity.** Specific titles name the object, not the dataset. Named systems are frequent: SARS-CoV-2/COVID-19 (8), ChatGPT/GPT (3), tocilizumab, Turing test, KAT8. Social-science titles name the domain at platform level without naming the platform. *The echo chamber effect on social media* compared, to my recollection, Facebook, Twitter, Reddit and Gab, but its title says "social media". The near-miss *Out-group animosity drives engagement on social media* (2021, research rank about 13; Facebook and Twitter data) does the same.

### By field
- **Social and computational social science (15)** has the shortest titles, the highest declarative share (7/15, 47%), no specialist jargon, and objects every reader knows (social media, ChatGPT, AI, teams, school closures). The two sub-styles are causal claims (*Gender-diverse teams produce…*, *Active learning narrows…*) and named concepts (*The echo chamber effect on social media*).
- **Economics** keeps the noun-phrase "impact of X on Y" convention: *The impact of COVID-19 on small business outcomes and expectations*, *Lifestyle and mental health disruptions during COVID-19*, *A Turing test of whether AI chatbots…*. The near-miss *Economic and social consequences of human mobility restrictions under COVID-19* (2020, 1002) is the same.
- **Complex systems and quantitative social science** are thin in the top 50. The closest near-misses (OpenAlex positions 11–40, not part of the corpus) are worth noting:
  - *The Diversity–Innovation Paradox in Science* (2020, 980 citations; research section) names a paradox outright.
  - *How digital media drive affective polarization through partisan sorting* (2022, 288) uses a "how X drives Y through Z" mechanism form.
  - *No evidence for nudging after adjusting for publication bias* (2022, 286).
  - The last two were not individually type-checked.
- **Biomedical (13)** titles are mostly noun phrases (10/13). Where molecular titles are declarative they are full of mechanism words (*KAT8-catalyzed lactylation promotes eEF1A2-mediated…*). Clinical COVID titles use an evaluative adjective in a noun phrase (*Effective treatment of…*).
- **Chemistry and engineering (10)** titles are the longest and most technical (9/10 contain specialist jargon), in a subfield register no general PNAS reader is expected to parse.
- **Environmental (12)** titles combine plain words with scope and quantities (*world's rivers*, *global*, *12,000 years*, *by 2030 but not 2050*). They also have the corpus's only future-tense claim and its most memorable long declaratives.

### Topic versus style: what this sample can and cannot show
- **Topic drives the citations.** 52% of the corpus sits in four topic clusters. COVID titles were cited because of the pandemic, catalysis titles because of a prolific inter-citing subfield, LLM titles because of 2023–24 interest. None of that is evidence about wording.
- **Field size drives the citations.** Biomedicine and chemistry produce more citing papers. The 10th-place count falls from 1235 (2020) to 209 (2024) partly because of time and partly because of field mix.
- **The data cannot test whether title style raises citations.** Within-year ranks show no advantage for declaratives: mean rank 5.8 for declaratives, 4.9 for noun phrases, 6.8 for gerund titles, where lower is better. The noun-phrase figure is inflated by the COVID titles of 2020, and the groups are 17, 25 and 8 titles. There is no comparison group of low-cited titles, so the sample cannot tell whether any feature *causes* citations.
- **What it can show** is what PNAS editors and readers accept in their most visible research titles: the range of length and form, the absence of questions, colons, "we" and wordplay, and which features recur. "Most cited" is not "best titled". PNAS copy-edits titles, and ranks 8–12 are source-sensitive and inside the noise.

**Published work on titles and citations.** I am confident of the direction of these findings, less so of exact details, and none is PNAS-specific. Check before citing any of them.
- Letchford, Moat & Preis (2015, *Royal Society Open Science*) found that shorter titles were associated with more citations across a large multi-journal sample. Fairly confident.
- Jamali & Nikzad (2011, *Scientometrics*, PLOS articles) found that question titles were downloaded more but cited less, and that colon titles did slightly worse. Moderately confident.
- Paiva et al. (2012, *Clinics*) reported that short titles stating results were cited more. Moderately confident.
- Habibzadeh & Yadollahie (2010, *Croatian Medical Journal*) and Jacques & Sebire (2010, *JRSM Short Reports*) found the opposite for length, or a positive effect of colons, in some medical journals. Moderately confident.
- A randomized trial by Wager and colleagues (around 2016, *Research Integrity and Peer Review*) found that declarative titles changed readers' perception of findings only slightly. Moderately confident.
- Overall the literature is mixed, the effects are small and confounded by journal and field, and I know of no well-identified causal estimate.
- I believe PNAS's author instructions ask for short, plain-language titles (my recollection is a limit of about 135 characters), but check the current Information for Authors.

## 4. Distilled norms

1. **Aim for about 10 words and about 75 characters; stay within 8–13 words.** Corpus median 10 words and 74 characters. Examples: *Out-group animosity drives engagement on social media*, the near-miss (8); *Farmland practices are driving bird population decline across Europe* (9); *Gender-diverse teams produce more novel and higher-impact scientific ideas* (9).
2. **When the finding is a relationship, state it as a present-tense sentence with a specific causal verb.** 34% of all titles and 47% in social science; never past tense. Examples: *Active learning narrows achievement gaps…*; *Electron delocalization triggers…*; *Biological structure and function emerge from scaling…*.
3. **Avoid questions and colons in a research title; in PNAS they signal a Perspective.** 0 questions and 1 colon in 50 research titles, against 6 of 13 Perspectives and Introductions. Contrast *Can Generative AI improve social science?* and *Climate Endgame: …*, both Perspectives.
4. **Social and environmental titles use plain words a general reader can parse.** No specialist jargon in 27 of 27 SOC and ENV titles. Examples: *The echo chamber effect on social media*; *Pharmaceutical pollution of the world's rivers*.
5. **Name the domain, not the dataset.** Use specific nouns, but at the level of the phenomenon. Examples: *…on social media*, not the platforms; *…of the world's rivers*; *…across Europe*. Scope words appear in 16% of titles and always describe the measured domain; no title claims a universal law.
6. **Hedge rarely, but honestly, with a modal.** 2 of 50 titles hedge, both with "can". Examples: *…can help meet the 1.5 °C target by 2030 but not 2050*; *AI can help people feel heard, but…*. A conditional or model-based claim gets "can", not "may suggest".
7. **A built-in contrast is allowed, and memorable, if the sentence stays literal.** Examples: *…but an AI label diminishes this impact*; *…by 2030 but not 2050*; the near-miss *The Diversity–Innovation Paradox in Science*. No research title uses wordplay or metaphor. The only literary title in the windows, *Death by a thousand cuts*, belongs to a Special Feature Introduction.
8. **No "we", and no method in the title unless the method is the finding.** 0 first-person titles. Method-led titles (*RepeatModeler2…*, *Using cognitive psychology to understand GPT-3*) are tool or approach papers.

## 5. Candidates, new candidates, recommendation

**A note on the paper's own terms.** In the paper, *churn* means lasting volatility (σ²) and *turnover* means replacement of top-set members. In those terms, the claim that *churn creates* the hierarchy is theory plus model (slope = churn/slide; the model starting from equality). The claim that *turnover sustains* it is measured: risers cross the boundary at the predicted rate (0.96), and this inflow balances the slide. A referee will read every verb against that division.

### The three current candidates

| | 1. *The churn that unseats online leaders creates the hierarchy they lead* | 2. *Turnover creates and sustains the hierarchy of online attention* | 3. *Online attention keeps its hierarchy by replacing its leaders* |
|---|---|---|---|
| Length | 11 words / 69 characters: fits (N1) | 9 / 63: fits | 9 / 61: fits |
| Form | Declarative, present tense (N2) | Declarative, two verbs (N2) | Declarative with a "by" mechanism (N2) |
| Contradiction readable? | Yes, in the "leaders … lead" echo | Implicit: the reader must know turnover should erode hierarchy | Yes: "keeps" against "replacing" |
| Honesty of the verb | **"creates" is model and theory only**; the measured result ("sustains") is missing | **"creates"** has the same problem; "sustains" is measured | "keeps" = sustains, measured; drops creation |
| Plain words (N4) | "churn" is business jargon, and the paper uses it for volatility, not turnover; "unseats" is a political metaphor | Good; "turnover" is the standard term in ecology, economics and rank dynamics | Good, but personifies attention, which neither "keeps" nor "replaces" anything; "its … its" |
| Ambiguity | "Online leaders" reads as people (influencers, politicians), not communities and pages | Low | "Replacing its leaders" suggests only the top turns over, whereas turnover is 16–33% at every scale |
| Genre fit | Clever; relative-clause subject plus pronoun echo; no research title in the corpus is built this way (against N7) | Closest to the corpus pattern "X verb Y" | Good fit; slightly informal |

**Verdicts.**
- **#1** is the most memorable and the least genre-typical. Its main verb is the claim most exposed to a referee, and it omits the measured result.
- **#2** is a sound PNAS title, but "creates" overclaims, and the paradox is left for the reader to supply.
- **#3** is the most honest of the three, but its personification and the "leaders only" reading weaken it.

### New candidates

| | Title | Words / chars | Why | Risk |
|---|---|---|---|---|
| A | **Continual turnover sustains a slowly changing hierarchy of online attention** | 10 / 75 | Puts the contradiction in the noun phrases ("continual turnover" against "slowly changing hierarchy"); measured verb; plain words; domain-level noun | Drops "creates" (it moves to the abstract) |
| B | Turnover sustains the hierarchy of online attention and can build it from equality | 13 / 82 | Carries the owner's "creates and maintains", using the corpus's own hedge ("can", N6); "from equality" is a concrete, surprising specific (Finding 3) | Long end of the range; "build it from equality" is model-only, and some referees will still want "in a model" |
| C | The turnover of online leaders sustains the hierarchy they lead | 10 / 63 | Keeps #1's echo, but with the standard term and the measured verb | "Online leaders" still reads as people |
| D | Size does not protect online leaders, yet turnover sustains the hierarchy | 11 / 73 | Two-clause contrast like *AI can help people feel heard, but…*; carries Findings 1 and 2 | "The hierarchy" is left without "of what"; leaders ambiguity |
| E | Turnover builds and sustains the hierarchy of online attention | 9 / 62 | Fullest match to the owner's wording | "builds" carries the same overclaim as "creates" |

### Recommendation: **"Continual turnover sustains a slowly changing hierarchy of online attention"**

Why this title, with the corpus evidence for each point:
- **It follows the genre's dominant pattern for social-science findings.** It is a present-tense declarative with a causal verb (norm 2; 7 of 15 social-science titles), like *Gender-diverse teams produce…* and the near-miss *Out-group animosity drives engagement on social media*.
- **Its length is exactly typical:** 10 words and 75 characters, against a corpus median of 10 and 74 (norm 1).
- **It names the owner's "foundational contradiction" literally rather than through wordplay.** Continual turnover sits against a slowly changing hierarchy, the same contrast-in-one-sentence device as *…by 2030 but not 2050* and *…but an AI label diminishes this impact*, without the metaphor that no corpus research title uses (norm 7).
- **"Slowly changing" is the honest form of "stable-ish".** It matches the paper's own puzzle sentence, and it stays true given Finding 4 (Reddit's head share fell from 50% to 34%). "Stable", "enduring" or "persistent" would not.
- **"Continual" is safer than "constant".** "Constant" could be read as "the same at every scale", which is false: the top turns over fastest.
- **The verb is the measured result.** No hedge is needed (norm 6), and creation can lead the abstract's model sentence.
- **"Online attention" names the domain, not the dataset (norm 5).** This is the same move as *The echo chamber effect on social media*, which pooled four platforms under one label.
- **There is no colon and no question (norm 3), and no word a general PNAS reader cannot parse (norm 4).**
- **If the owner wants creation in the title**, B is the honest form ("can build it from equality"). Avoid an unhedged "creates" or "builds" (#1, #2, E).

### Words a referee could call overclaiming
- **"creates" / "builds" — high risk.** The 2019–2022 ladders are already hierarchical, so no creation is observed. It rests on theory (Gabaix, Fernholz, Gomez) and the model started from equality. Use "can build", or keep it out of the title.
- **"online attention" — moderate risk.** The data are two platforms, and activity counts stand in for attention. The corpus precedent for platform-level generalization is strong, but the first sentence of the abstract should name Reddit and Facebook.
- **"sustains" — low risk.** It is measured (0.96). Where the balance tipped, the hierarchy changed, which "slowly changing" already allows for.
- **"stable", "enduring", "persistent" — avoid.** They contradict Finding 4, and "persistent" collides with the paper's own word for the decline in participation.
- **"leaders" — wording risk, not evidence risk.** It reads as people unless "of online attention" anchors it.
- **"universal", "law", "explains" — avoid.** No corpus title claims a universal law, and the mathematics is not new to this paper.

I can publish this report as a private web page if that would help in sharing it with the team.
