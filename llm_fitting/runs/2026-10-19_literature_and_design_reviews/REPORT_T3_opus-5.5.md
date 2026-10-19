<!-- Provenance: Task 3 (PNAS exemplars) report by Claude Opus 5.5 (claude-opus-5-5) (Claude Code sub-agent, default effort, local files + WebSearch/WebFetch; returned as text because sub-agents may not write report files), 2026-10-19, on manuscript v4 (commit 42955ac). Brief: COMMON_BRIEF.md + the task file, committed 94695b0 before the review. Saved verbatim by Claude Opus 5.5 (the session's lead agent) from the model's final message; nothing in the body below is edited. Claims in it are the reviewer's and are checked in SYNTHESIS.md before use. -->

I could not write the report file: the Write tool refused it ("Subagents should return findings as text, not write report files"). The complete report is below. Please save it to /Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-10-19_literature_and_design_reviews/REPORT_T3_opus-5.5.md. No repository file was changed.

**Summary (≤150 words)**
1. The draft's prose is denser than every exemplar's: 36.5 words per sentence against a median of 27, and 3 uses of "we" per 1,000 words against 14.
2. The first page claims more than the Results deliver. "Outlasts its members" holds only on Reddit comments. "Through that channel" (significance statement and abstract) contradicts l. 182, which disclaims the link.
3. Claim-sentence Results headings appear in only 4 of 31 exemplars, all of them close in kind to ours. None uses "What/Where" headings, and four of the draft's five headings use that form.
4. Fig. 1 has no schematic (18 of 31 exemplars have one), and Figs. 3–5 have clipped panel titles.
5. Eight pages is within the norm: the exemplar median is 9. Reach about 7 by cutting roughly 1,000 words and moving four panels to the SI, and use the extra room for a Discussion paragraph on the rival readings, as Sireci does.

---

# Report T3: what 31 PNAS model papers teach about structure, tone and editorial preferences, compared with draft v4

**Provenance.** Model: Claude Opus 5.5 (`claude-opus-5-5`), running as a Claude Code sub-agent at the default reasoning setting. Tools: local files (Read; Bash with python3, pdftotext, pdftohtml, grep and awk, all read-only), WebSearch and WebFetch. Date: 2026-10-19.

## 0. Access check

**Read in this session**

- **Instructions:** `COMMON_BRIEF.md`, `T3_PNAS_EXEMPLARS.md` and `00_PRACTICES_ADOPTED.md` (in the round's run folder).
- **Manuscript v4:** `paper/pnas_draft.tex` (read in full), `paper/pnas_draft.pdf` (as extracted text; 8 pages, references on pp. 7–8) and `paper/pnas_si.tex` (in full).
  - I confirmed that `paper/` is unchanged between 42955ac and HEAD.
  - I read the titles in `paper/references.bib`.
- **Figures:** all five main-text figures, viewed as images.
- **Records:** `SYNTHESIS_R5.md` in full. Of `PROGRAM_STATE.md` I read **only the opening section**; it is not needed for this task.
- **Exemplars, all 31 packet text files:**
  - For every paper I read the front matter: title, editor line, dates, significance statement and abstract.
  - For every paper I read the introduction or its last paragraph, the Results headings with their first sentences, and the opening and closing of the Discussion.
  - **Eight read in full** (main text, not Methods details): George & O'Dwyer 2023, Mallmin 2024, Kawakatsu 2021, Sireci 2023, Juul & Ugander 2021, Mori 2020 and Lee 2022, plus Hendrick 2025 except its Methods.
  - **Read in large part:** Xue 2020 (introduction, the ablation section, Discussion) and Meng 2025 (introduction, Discussion, data statement).
- **Prior templates:**
  - Read in full: `PNAS_template_Claude4.5_Jan2026.md`, `PNAS_template_GPT5.2_Jan2026.md` and `PNAS_template_rank_share_v5_Apr2026.md`.
  - Read in part: `revised_pnas_template_platform_measurement_Apr2026.md` (first 140 lines and its outline).
  - `pnas_exemplar_structure_report_noquotes_Jan2026.txt`: section tables for its 10 papers. It covers papers 01–09 and 11, not the 12 that the task file mentions. I spot-checked two of its papers against my own counts.

**Source PDFs.** The packet's layout text interleaves the columns of two-column pages. To read and count reliably, I ran `pdftotext` (reading order) and `pdftohtml -xml` (fonts, used to find headings) on the 29 source PDFs named on the first line of each packet file, and on nothing else in that folder. I did not open other files there.

**Could not open**

- pnas.org returned HTTP 403 on every page I tried.
- PMC showed a bot check, and Europe PMC returned HTTP 500.
- In their place I used:
  - archived PNAS author pages from April 2020, through WebFetch;
  - a public rendering of the PNAS LaTeX template (rticles gallery; the text is PNAS's own template text, compiled March 2021);
  - Crossref.

  The archived pages may be out of date.

**Quotation constraint**

- **The rule.** A binding rule in my operating instructions limits verbatim quotation of third-party text to one passage of under 15 words per response.
- **What that means here:**
  - The report contains **one** verbatim quotation from an exemplar (§3.8).
  - Every other exemplar passage I cite is given as a location I checked, with its content paraphrased in my own words. The authors can read it at the page given.
  - I quote the draft and the prior templates, which are the owner's own documents, where a recommendation needs their exact words.
- **Why it matters.** This falls short of the brief's request for several exact quotations. I would rather report that than break the rule or present a reconstruction from memory.

**Counting caveats**

- Automatic counts from PDF text carry known errors: interleaved columns, captions, and symbols lost from equations.
- I checked by hand every count that the argument depends on (headings, tracks, dates, figures, limitations passages).
- Where a regex can only give a lower bound, I say so.

## 1. The six findings that matter most

1. **The draft's prose is denser than every exemplar's.** I measured all 31 exemplars and the draft with the same pipeline.

   | measure | exemplars: median (range) | draft |
   |---|---|---|
   | mean sentence length, words | 27 (19.5–34.5) | 36.5 |
   | share of sentences over 40 words | about 14% | 30% |
   | "we/our" per 1,000 words | 14.0 (2.9–27.8) | 3.1 |
   | modal hedges ("may", "suggest", "likely"…) per 1,000 words | 3.6 (1.0–9.6) | 0.5 |

   Only Morales (31.5%) comes near the draft's share of long sentences. Only the 100-author Cramer consortium paper uses less first person than the draft. The draft qualifies its claims with numbers and clauses packed inside each sentence, where the exemplars use short sentences and plain words like "may" or "we cannot".

2. **The first page claims more than the Results deliver, in two places.**
   - **"Outlasts its members" is stated generally, but the Results establish it only on Reddit comments.** At the submissions head, the draft says the shape relaxes in three to four years, "no longer than its membership takes to turn over." The claim appears in the title, the significance statement and the abstract.
   - **"Through that channel" gives a causal link the Results disclaim.** It appears in the significance statement (l. 74) and the last sentence of the abstract (l. 79), linking persistent decline to the head's loss of share. Yet the Results (l. 182) say "we have not shown that the communities whose declines persist are the head's decliners." The brief also says the paper makes no causal claim.
   - **Why this matters for PNAS.** On the archived PNAS pages, more than half of Direct Submissions are declined by the Board without review, and a single negative review that the editor agrees with is enough to reject (§4). An inconsistency between the first page and the Results is the kind of finding that settles such a decision.

3. **Claim-sentence Results headings are a feature of the closest exemplars, not of PNAS in general.**
   - Only 4 of 31 papers use full-sentence claims for most of their Results headings: George, Mallmin, Sireci and Gagler. All four are stochastic or scaling models of abundance distributions, the kind closest to ours.
   - Ten of 31 have at least one claim heading. None uses a "What…/Where…/How…" heading.
   - Four of the draft's five Results headings are of that "wh-" form.

4. **Fig. 1 has no schematic, and the figure set is heavier than the norm.**
   - 18 of 31 exemplars put a schematic of the model, the approach or the data organization in Fig. 1.
   - The median exemplar has 4 figures. The draft has 5 figures with 17 panels.
   - Several panel titles in Figs. 3–5 are visibly clipped or overlapping.
   - The central idea, a top-*k* boundary across which risers cross in while members slide down, is never drawn.

5. **Eight pages are within the norm. The problem is density, not length.**
   - Of 29 exemplar PDFs, the median runs 9 pages including references. Only 4 are 6 pages or shorter.
   - The closest papers run 6–12 pages: Juul 6, Kawakatsu 7, Mori 7, Hendrick 8, Sireci 11, George 11, Mallmin 12.
   - PNAS allows 12 pages.
   - A target of about 7 pages, reached by thinning numbers and moving four panels to the SI, is better justified than forcing 6.

6. **The exemplars model how to present rivals, and the draft scatters them.**
   - The closest papers set rival mechanisms side by side, each with its own prediction:
     - Sireci compares three models with three different predictions (pp. 3–6) and states two interpretations of its own result, a pessimistic and an optimistic one (p. 8).
     - George ends its Results with a direct comparison against an environmental-noise model, reported as counts per system (p. 9).
     - Gagler gives a whole Results heading to ruling out the most obvious alternative (p. 5).
     - Xue removes an ingredient to show that it is necessary (p. 8).
   - The draft has all the material for this. But rival (a), that the balance is automatic, and rivals (b)–(d) are handled clause by clause across four subsections.
   - The Discussion (540 words, short against the exemplars' 600–1,300) never assembles them.

## 2. Structure: what the 31 papers do

A per-paper table is in the Appendix. All counts below are out of 31 unless stated otherwise.

### 2.1 Section architecture

- **Introduction heading.** Only 2 papers have one (Bartolucci and Maddu, both numbered mathematical papers). The other 29 open with an unheaded introduction. The draft follows the majority.
- **"Results" heading.** 19 have a "Results" section. The other 12 use topic-named sections instead, as in Juul's data section and its section on models, Kawakatsu's modelling and data sections, Mori's sequence of tests, and the numbered sections of the mathematical papers.
- **Where the model goes.** Both orders are common.
  - **Model first** (8): George's "Model and Data" section, Mallmin's numbered Model section, Kawakatsu, Xue, Marchi, Agrawal, Pyo, Morales.
  - **Phenomenon first** (6), meaning the empirical regularity comes first and the models follow: Sireci, Juul, Mori, Hendrick, Meng, Minasandra.
  - Papers that set out to explain an empirical regularity tend to come from the second group. That is the draft's order too.
- **Discussion.** 30 have a Discussion or Conclusion. Brown has neither. Three have both a Discussion and a separate Conclusion: Minasandra, Morales and Pospisil.
- **Methods.** 18 have a Materials and Methods section after the Discussion. 13 have no main-text Methods: the mathematical and physics papers, and Kawakatsu, Mori and Xue. Dick puts it first.
- **Roadmap paragraph.** Rare: a regex finds three (Gagler, Morales, Mori), and Kawakatsu has a one-line version.

### 2.2 Word budgets of the closest papers and of the draft

The exemplar figures are section counts from the prior structure report. I spot-checked George: my count of its Model and Data section is 1,596 words against the report's 1,607, and of its Results 2,846 against 2,729.

| paper (pages) | introduction | model/theory | Results | Discussion | Methods |
|---|---|---|---|---|---|
| Juul & Ugander (6) | 972 | n/a | 2,588 | 764 | 60 |
| Kawakatsu (7) | 493 | 1,412 + 493 | 1,767 | 615 | none |
| Marchi (8) | 525 | in Results | 3,822 | 1,078 | 796 |
| George & O'Dwyer (11) | 635 | 1,607 | 2,729 | 1,081 | 591 |
| Mallmin (12) | 839 | 667 | 3,560 | 1,272 | 1,338 |
| **draft v4 (8)** | **483** | in Results | **3,273** | **539** | **~780** |

The draft's Results subsections run 312, 576, 728, 590 and 1,067 words. Its captions total about 680 words, the significance statement 120 and the abstract 240.

Two departures stand out:

- **The Discussion is proportionally the smallest.** Results words per Discussion word are 6.1 in the draft, against 3.3–4.0 for George, Mallmin, Marchi and Juul. Only Kawakatsu (6.0) is similar.
- **The introduction is at the short end, like Kawakatsu and Marchi.** That is fine, but almost all of it is one literature paragraph (213 words, 11 `\cite` commands).

Across all exemplars, the significance statement has a median of 116 words (range 84–138) and the abstract a median of 211 (126–252). The draft's 120 and 240 sit at the top of both ranges.

### 2.3 Main text and SI

All 31 cite an SI Appendix. The exemplars that place detail there keep three things in the main text: the model in words plus its key equation, the empirical figures, and one comparison with a rival. Derivations, robustness checks and extended comparisons go to the SI.

- George puts its goodness-of-fit alternatives in its SI Fig. S2 and keeps the comparison with a rival noise model in main Fig. 7.
- Mallmin moves numerical detail to the SI and keeps the phase diagram in the main text.

PNAS's own template says the same: if detailed methods go to the SI, the main-text methods must still let a reader follow the logic of the procedures [verified: pdftotext of the PNAS LaTeX template rendering, https://eddelbuettel.github.io/rticles-gallery/pnas_demo.pdf; paraphrased].

### 2.4 Equations, and how a model is introduced

Display equations, counting the main-text Methods, sort into four groups:

- **None:** 7 papers (Cramer, Gagler, Gupta, Heckmann, Juul, Lee, Minasandra).
- **1–6:** 7 papers (Agrawal, Dick, George, Kawakatsu, Mori, Bahri, Meng).
- **7–14:** 9 papers.
- **19 or more:** 8 papers (Bartolucci, Brown, Ghio, Hadad, Mallmin, Marchi, Pospisil, Sireci).

Several heavy papers push their equations into Methods. Before Methods, Hendrick has 2 of 12, Morales 4 of 10, Sireci 12 of 42 and Mallmin 14 of 35.

The draft's four equations are at the light end, next to George and Kawakatsu, and need no cutting.

In the closest papers, a model is introduced in three steps:

1. assumptions in words;
2. one stochastic differential equation or update rule;
3. what it predicts, in a sentence aimed at a non-specialist.

Two passages show the third step well:

- George follows its fold-change distribution (Eq. 4, p. 4) with a sentence telling readers that, despite its imposing look, the expression has only two parameters.
- Meng says which of its parameters are measured directly and which one alone is fitted (introduction).

The draft does the parameter accounting (l. 155: "none fitted to the statistics we test it on"). But it introduces Eq. 1 (turnover) before the reader has seen why turnover matters to the question.

### 2.5 Figures, and the role of Fig. 1

- **How many.** The median is 4 figures (range 2–12); 13 papers have exactly 4.
- **What Fig. 1 does:**
  - In 18 papers it includes a schematic: the model (Kawakatsu, Marchi, Xue), the approach (Gagler, Gupta, Jin, Pospisil) or the data organization (Sireci, Hendrick).
  - In 13 it shows data or the phenomenon only. George shows its three systems' time series; Mori shows maps; Juul reproduces the published result and then its size-matched version.
- **Caption first lines.** Most state the panels' subject. Some state the result: Marchi says a simple model predicts the wave, Minasandra that the hazard functions decrease, Wang that the rescaled relationship gives a metric.

The draft's Fig. 1 is a phenomenon figure, the George or Mori type. It shows the puzzle (A, B, C) but not the mechanism, and its panel A, "log activity relative to rank K (offset)", needs decoding.

### 2.6 Results headings

Each paper's Results headings were classified by hand.

- **Full-sentence claims for most headings: 4 papers.**
  - George: 4 of 5 subsections.
  - Mallmin: 5 of 6.
  - Sireci: 4 of 4.
  - Gagler: 3 of 3. Its second heading rules out the obvious alternative explanation (p. 5).
- **At least one claim heading:** 10 papers. In addition to those four:
  - Cramer, 3 of 7: one says model accuracy rankings are highly variable, another that performance degrades with horizon.
  - Heckmann, 2 of 5.
  - Gupta, Pyo, Jin and Morales, 1 each.
- **Evaluative noun phrases,** which state a finding without a verb: Hendrick's opening heading, which asserts universality; Minasandra's Results headings; Marchi's heading on antigenic waves; Morales's heading on quasiuniversal scaling.
- **Neutral topic labels** for all headings: the remaining roughly 17 papers, including Juul, Kawakatsu, Mori, Marchi, Meng and Xue.
- **Interrogative "What/Where/How" headings: none of the 31.**

The draft's headings:

| heading | type |
|---|---|
| "A fast turnover and a durable hierarchy" | noun phrase |
| "The top turns over fastest because the hierarchy is flattest there" | claim |
| "What turnover returns, and why the hierarchy is durable" | wh- |
| "Where the hierarchy comes from: a generative model" | wh- |
| "What homogeneous random growth lacks: persistent decline" | wh- |

### 2.7 How null models and baselines appear

Almost every exemplar has an explicit comparator in the main text. The forms differ by kind of paper:

- **Randomized counterfactuals drawn from the data,** shown against the observed value: Sireci reshuffles operational taxonomic units (OTUs) on the phylogenetic tree (p. 2); Mori builds 1,000 random partitions of space (pp. 2–6).
- **Rival mechanistic models with distinct predictions:** Sireci's three models; George's environmental-noise model; Kawakatsu's three score functions, compared by likelihood (pp. 4–6).
- **Matched comparisons that remove an artifact:** Juul's size matching.
- **Ablations:** Xue removes the eco-evolutionary feedback and shows the scaling disappears (p. 8).
- **Simple baselines:** Cramer's naive forecast and Dick's hierarchy of baselines.

Two more patterns:

- **Comparators share the panel with the data** (Sireci Fig. 1B and 3, Juul Figs. 1 and 3, George Fig. 7). The draft does the same in Figs. 2B, 3A–B and 5A.
- **Held-out forecasting is unusual** outside forecasting papers. Cramer is the clear case; Heckmann and Bourdais use cross-validation. The closest model papers validate their models by fitting distributions, by goodness-of-fit simulation (George pp. 4 and 6) and by comparing likelihoods.

### 2.8 Where limitations go

- **Explicit passages: about 13 of 31** have a passage that names limitations or caveats of the authors' own work, nearly always in the second half of the Discussion:
  - Kawakatsu, a paragraph with first and second limitations (p. 6);
  - Sireci, a paragraph that enumerates assumptions (pp. 7–8);
  - Juul (p. 6), Gupta, Dick, Meng, Wang and Mori–Kori;
  - Marchi, whose Discussion refers back to its caveats;
  - Cramer;
  - Bahri, a numbered "Limitations" subsection;
  - Bourdais, whose Discussion opens with "Limitations".
- **The other 18** present them as extensions or future work (George pp. 9–10, Mallmin pp. 8–9).
- **How they end.** Most close the Discussion on an outlook or a broad statement after the limitations, not on the limitations themselves. Juul is a clean example: its limitations sentence is followed by a two-sentence closing statement (p. 6).

The draft's last Discussion paragraph begins "The limits are those of the data" and ends on the procedural point that a protocol "is to be adopted before those data are opened."

### 2.9 What main-text Methods contain

The range runs from about 60 words to over 2,000:

- **Juul (about 60 words):** a statement that the main text contains everything needed to replicate.
- **George (about 590):** data processing per system and fitting procedures.
- **Mallmin (about 1,340) and Sireci:** derivations and simulation details, with many equations.
- **Cramer (about 2,200):** eligibility rules and evaluation design. It is also **the only exemplar that labels an analysis post hoc**, with a single phrase in its Methods (p. 9).

The draft's Methods, about 780 words including the registration statement and the data statement, sit in the normal range.

## 3. Tone and style

### 3.1 Sentence length and hedging

The numbers are in §1, item 1. Three things stand behind them.

- **Long sentences in the exemplars are Results sentences that describe a figure.** Their framing sentences, in the introduction, the significance statement and the opening of the Discussion, are short.
- **The draft's long sentences sit in the framing places.** For example, the abstract's sentence about the model building a hierarchy from equality runs to 40 words, and the significance statement's sentence on random growth to 31.
- **Hedging differs in kind.** The exemplars hedge with modal verbs and scope clauses:
  - Kawakatsu: "suggests" (p. 4); estimates to be treated with caution (pp. 4–5).
  - Sireci calls its stretched exponential a merely phenomenological fit (p. 2).

  The draft hedges with numbers and parentheticals ("(post hoc; each set rests on one to four overlapping starts)"). Those are honest, but they lengthen sentences and push the claim to the end. Splitting such a sentence into a claim sentence and a caveat sentence fixes both.

### 3.2 First person

- **Frequency.** 30 of 31 exemplars use "we show/find/demonstrate". 23 use "Here, we" or "Here we". The median is 14 "we/our" per 1,000 words.
- **Discussion openings.** About 25 of the 30 papers with a Discussion or Conclusion say, within its first three sentences, what the authors did, usually in the first person. Kawakatsu, Xue, Sireci and Marchi open their very first sentence this way.
- **The draft** uses "we" about three times per 1,000 words. Its Discussion opens with an impersonal claim, and many Results sentences start with a participle ("Measured rank by rank, crossings…", "Started from complete equality, it builds…").

Active first person would shorten the sentences and make clear which steps were the authors' choices, such as cohorts fixed by permanent rank or the two-spacing difference.

### 3.3 How generality is claimed

- **Use of "universal":** 18 papers use the word; 4 put it in the title (George, Sireci, Gagler, and Morales's "quasiuniversal").
- **It is always backed by many systems:** three systems (George), eight biomes (Sireci), 137 cities on four continents (Hendrick), many domains of life (Gagler).
- **Papers with fewer systems scope their claims.**
  - Kawakatsu declines to attribute its persistent parameter pattern across four datasets to a universal mechanism and calls it an observation worth future study (p. 5).
  - Mori reports that its common power law fails for India and then shows it holds under a stricter definition of a city (p. 6).
- **Counterexamples exist:** Meng and Hendrick write expansively ("universal phenomenon", "profound self-similarity"). Both papers were accepted, so restraint is not a requirement. But neither shows a result that holds on one platform and fails on the other, as the draft must.

The draft avoids "universal", which is right. Its generality lives in the title instead ("a hierarchy of online attention"), and the evidence supports it on one of three ladders (finding 2).

### 3.4 How novelty is signalled without claiming it

- **Gap statement, then "Here, we".** A regex finds explicit gap phrases ("is lacking", "remains unclear", "not well understood", "is still missing") in the opening of at least 11 papers; that is a lower bound. Kawakatsu's abstract says the mechanisms behind the creation and endurance of hierarchies are not well understood (p. 1), which is close to our question.
- **A hedged statement about prior work.** George writes that it knows of no comparison across these data types with a single model (p. 2), and later that it goes beyond earlier studies by applying one model to three systems (p. 3).
- **A puzzle.** Bartolucci's significance statement names an open question and then resolves it.
- **Outright priority claims are rare.** Two papers make one: Morales says "for the first time" in its Discussion, and Meng uses "unique perspective" and "surprisingly". The PNAS guidance I could open does not mention novelty claims. The archived review-policy page says appeals based on novelty or general interest are unlikely to succeed [verified via WebFetch of the archived 2020 page; paraphrased].

The draft signals novelty by stating what is automatic and what is not (l. 186). That move is stronger than any of the above, and it should be kept.

### 3.5 Questions as openers

- **In the title:** 1 paper (Agrawal).
- **In the significance statement:** 3 papers; it opens with one in Juul and in Lee, and Pyo has one in the middle.
- **Opening the abstract:** 1 paper (Juul).
- **In the front matter or the first roughly 1,500 words:** 10 papers. Juul's first introduction paragraph asks four questions in a row; Bahri asks four; Pyo asks four.

The draft has questions in two places: the significance statement ("Where does it come from?") and the end of the first introduction paragraph. That matches Juul and Lee, and both questions earn their place because they are answered at once. A title beginning "How…" appears in none of the 31.

### 3.6 How specialist terms are introduced

- **Each term is defined once, at first use,** in an appositive or with quotation marks:
  - Kawakatsu defines a hierarchy between dashes in its first sentence (p. 1).
  - Sireci defines macroecology the same way (p. 1).
  - Juul introduces the term "cascade" in quotation marks (p. 1).
  - Mallmin gives a one-sentence mathematical definition of chaos in its introduction (p. 1).
- **Coined terms are few.**

The draft relies on many coined or informal terms. Counts in the body, introduction to Discussion:

| term | uses | defined? |
|---|---|---|
| ladder | 25 | never in the main text; first used as "We study three ladders" |
| head | 30 | |
| slide | 22 | |
| crossing(s) | 17 | |
| world(s), for simulated null models | 18 | |
| lasting | 12 | |
| top set | 12 | |
| universe | 11 | |
| passing (swings) | 7 | |

Gomez is named nine times. "Structure function", "gap identity" and "Atlas slope" appear without explanation. One definitions paragraph or a labelled schematic would serve a broad reader better (§5.3, edits 5 and 9).

### 3.7 How failures and rival models are written about

- **Failures are stated briefly, as counts, at the point where they occur:**
  - George: 28 of 85 microbial species give inconsistent parameter estimates (p. 6); the rival model wins in 2 of 4 forest clusters (p. 9).
  - Mori: India fails the test, followed by an explanation and a robustness check (p. 6).
  - Sireci: the model reproduces mean correlations, not their distribution (p. 7).
- **None of the 31** reports expectations written in advance, scored failures, sealed data or AI use.
- **Rivals get a sentence each that says what the evidence does and does not exclude:**
  - Sireci says that rejecting competition as the source of the pattern does not mean competition is absent (p. 6).
  - Lee says its result is agnostic about which of two mechanisms carries the memory (p. 3).
  - Juul separates what a randomized experiment would be needed for from the more answerable question of sufficiency (p. 2).

The draft's handling of failures (forecasting no better than simpler models, a model hierarchy 8–46% flatter than observed) matches George's candour. It is longer, though, and some of it can go to the SI.

### 3.8 Passages to learn from

**One verbatim quotation** (all I am allowed; see §0):

> Lee et al. 2022, significance statement, first sentence: "How does the brain maintain memories that long outlast the proteins that encoded them?" [verified: `lee-et-al-2022-synaptic-memory-survives-molecular-turnover.pages.txt`, p. 1, Significance; also in the source PDF's text]

Lee's significance statement uses the frame of our title, a structure that outlasts its constituents, and opens with it as a question. The paper's own title is a declarative sentence: "Synaptic memory survives molecular turnover".

**Paraphrased passages.** Each was located and checked in this session against the source PDF's text; most were also found in the packet file. Word for word, they are at the pages given.

| paper, location | what it does | lesson for our draft |
|---|---|---|
| Kawakatsu 2021, abstract, p. 1 | Says plainly that the mechanisms behind the creation and endurance of hierarchies are not well understood. | Our question can be stated the same way, as a gap about endurance. |
| Juul & Ugander 2021, p. 2 | Concedes that whether two kinds of content follow the same rules would need a randomized experiment. Reframes the question as whether one set of rules is sufficient to produce the observed differences. | This is exactly the logic of "what random growth implies automatically". |
| Sireci 2023, p. 2 | Calls its stretched-exponential fit phenomenological, since the mechanism behind its form is not understood. | A model for how to label the persistence statistic or the composition term. |
| Sireci 2023, p. 6 | Excluding competition as the source of the pattern does not mean competition is absent. Competition simply leaves no detectable phylogenetic signal at the resolution available. | |
| Sireci 2023, p. 8 | Lays out a pessimistic and an optimistic reading of its own result. | |
| Lee 2022, p. 3 | Stresses that its results take no position on which mechanism propagates the activity. | A template for the draft's statement on persistence and the head's fall. |
| Mallmin 2024, p. 4 | Says that the rank–abundance curve and each species' rank change over time while the community's statistical structure stays conserved. | The phenomenon statement closest to ours. |
| Mallmin 2024, p. 8 | Dismisses neutral theory because it predicts turnover times vastly longer than those observed. | A comparison of time scales used to reject a rival, as the draft could do for neutral copying. |
| Marchi 2021, abstract and significance, p. 1 | Presents an emergent time scale, longer than the time scales of the system's parts, as the headline result. | The draft's separation of time scales on comments can be stated the same way. |
| Mori 2020, significance, p. 1 | States that the observed similarity could not arise from a random-growth process. | A plain, one-sentence way to state a result against random growth. |
| Gagler 2022, p. 5 | A Results heading states that the universality is not explained by components shared across life. | A heading that rules out the most obvious rival. |
| George 2023, p. 10 | Closes with a flourish adapted from Box's aphorism, calling its model useful and unreasonably effective. | A counterexample: it works only when the model's success is unqualified, which ours is not. |
| Meng 2025, data statement | Explains that platform agreements prevent sharing one platform's raw data, says what is shared, and tells readers how to apply for access. | A 2025 precedent for the CrowdTangle restrictions. |

## 4. What editors' and referees' choices reveal

### 4.1 Evidence from the 31 papers

**Tracks**

- **24 Direct Submissions** and **7 Contributed** papers: Brown, Dick, Gupta, Hadad, Hendrick, Lee and Xue.
- Four Direct Submissions were handled by **guest editors** invited by the Board:
  - Watts for Juul & Ugander, accepted by Board member Raftery;
  - Tu for Bahri and for Ghio;
  - Cornish-Bowden for Heckmann, accepted by Board member Fersht.

**Editors who appear more than once**

- Hastings: George, Kawakatsu, Mallmin.
- Sejnowski: Morales, Mori–Kori, Pospisil.
- Tu: Bahri, Ghio.

Papers on social or platform data were edited by:

| editor | paper | field of the paper |
|---|---|---|
| Watts (guest) | Juul & Ugander | online diffusion |
| Barabási | Meng | online social networks |
| Scheinkman | Mori | city sizes, Economic Sciences |
| Wachter | Cramer | demography/forecasting |
| Bickel | Jin | statistics with social-science data |
| Athey (contributing NAS member) | Hadad | economics/statistics |

**Classification**

- Ten papers carry a **dual classification**.
- Only 2 carry "Social Sciences": Juul (paired with Applied Mathematics) and Jin (paired with Statistics).
- 3 carry "Economic Sciences": Brown, Hadad and Mori.
- 8 carry "Applied Mathematics", including Kawakatsu, Meng and Juul.

**Time from receipt to acceptance**

- Direct Submissions: median 191 days (range 35–356; n = 24).
- Contributed papers: median 153 days (n = 7).
- The seven closest Direct Submissions (George, Sireci, Juul, Kawakatsu, Meng, Mori, Mallmin): median 218 days.
- George took 356 days and thanks three reviewers.

**What every paper has:** a significance statement, an abstract of at most 252 words, an SI Appendix, a data statement and "we show/find" language (30 of 31). Almost all also have an explicit comparator.

**What none has:**

- written expectations that are later scored (one paper, Cramer, labels an analysis post hoc);
- sealed confirmation data;
- an AI-use statement;
- a "How…" title or "What/Where" headings.

These papers date from 2019 to 2025. The missing AI statement may reflect when they were published, not editors' preference.

**What sets the closest papers apart** (George, Sireci, Mallmin, Kawakatsu, Mori, Hendrick, Xue, Meng, Juul), as against the corpus as a whole:

- several systems or datasets;
- one minimal model with few parameters and an analytic prediction;
- a rival mechanism tested inside the Results;
- claims of generality scaled to the number of systems;
- abstracts with no numerals: George, Mallmin, Sireci, Kawakatsu, Mori, Hendrick, Marchi and Xue have none, while 13 of the 29 PDF abstracts have at least one;
- Discussions of 600–1,300 words.

### 4.2 Outside evidence on editorial process

Archived PNAS pages from April 2020, fetched with WebFetch and paraphrased here:

- More than half of Direct Submissions are declined by the Editorial Board without further review.
- Submissions are assigned to a Board member in one of 31 disciplines.
- Authors recommend three Board members and five reviewers.
- Editors obtain at least two reviews.
- A single negative review, if the editor agrees with it, is enough to recommend rejection.
- Appeals on the basis of novelty or general interest are unlikely to succeed.
- Research reports should present original research of exceptional importance, and all papers should be intelligible to a broad scientific audience.
- The preferred length is 6 pages, with a maximum of 12. A standard 6-page article is about 4,000 words and 50 references.

Sources (archived 2020; check current wording):

- https://bac-lac.wayback.archive-it.org/web/20200424185956/https://www.pnas.org/page/authors/journal-policies
- https://bac-lac.wayback.archive-it.org/web/20200424185956/https://www.pnas.org/page/authors/purpose-scope
- https://bac-lac.wayback.archive-it.org/web/20200424185956/https://www.pnas.org/page/authors/submission

PNAS's LaTeX template, as rendered in the rticles gallery, says three things about the front matter. The significance statement is limited to 120 words, must be understandable to an undergraduate-educated scientist outside the field, and should explain the work's relevance in a broad context. The abstract should explain the article's major contributions to a general reader. [verified: pdftotext of https://eddelbuettel.github.io/rticles-gallery/pnas_demo.pdf; paraphrased]

Commercial pre-submission blogs (manusights.com) describe a desk screen of breadth and readiness, and say PNAS requires AI disclosure and forbids AI-generated images [secondary; not verified with PNAS]. I would not rely on them except as a prompt to check PNAS's current AI policy (§5.3, edit 11).

### 4.3 Inference

These are inferences, not findings. Survivorship applies throughout: the features shared by all 31 may be no more than the house format.

1. **The decision is made on the first page.** The Board screens before any referee reads the paper, and it reads the significance statement, the abstract and Fig. 1. The draft's first page is mechanism-first and makes no case for importance, and it carries the two overstatements in finding 2. Editors of these papers came from network science, statistics and economics, which suggests the significance statement has to make the importance legible to someone outside both platform studies and stochastic portfolio theory.
2. **One referee can sink the paper, so internal consistency matters more than breadth.** A referee who finds the first page claiming more than the Results is enough.
3. **Classification steers who edits the paper.** The two social-data papers with a mathematical model (Juul, Meng) were classified with Applied Mathematics and edited by network scientists. The economics precedent in the random-growth tradition (Mori) was edited by an economist. Round 5's worry about desk rejection under a social-science editor is consistent with this pattern.
4. **Expect at least one round of major revision,** with about 7 months to acceptance for the closest papers.
5. **The registration apparatus is a differentiator, not a convention.** Its main-text footprint should be small: Cramer's one-phrase label is the precedent. The scored expectations belong in the SI, as v4 already has them.

## 5. Comparison with draft v4

### 5.1 Where the draft already matches the best exemplars

- **Architecture.** An unheaded introduction ends in a "here we" paragraph that lists three findings (l. 95). The phenomenon comes first, followed by a measured mechanism, then the model. This is Sireci's and Juul's order.
- **A light model.** Four equations; the model in words before its equation; derivations in the SI; parameters declared, not tuned to the test statistics. This is like George and Kawakatsu.
- **Cross-system design.** Two Reddit ladders and Facebook pages, with Instagram as a control.
- **An analytic relation checked by simulation.** The boundary ratio is 0.94 in the data against 1.04–1.12 in the simulated worlds (l. 139). This is the most common validation in the corpus.
- **Null models and the data side by side,** computed by the same code and drawn on the same panels. This is Sireci, Mori and Juul.
- **Ablations.** Removing volatility, or removing the slide, over 30 years (l. 157). This is Xue's test of necessity.
- **Heterogeneity as the key departure** (persistence, l. 168–171), parallel to Mallmin's subsection on species with persistent competitive differences (p. 5).
- **Candour about failure:**
  - the model forecasts no better than simpler models;
  - its hierarchy is flatter than the observed one;
  - the submissions shortfall in crossings is unexplained.
- **The one claim heading** ("The top turns over fastest because the hierarchy is flattest there") is in the closest exemplars' style.
- **The data statement** handles platform restrictions in the way Meng's does.
- **Section lengths:** the significance statement and abstract are within limits, and the Methods are of normal length.

### 5.2 Where it departs, section by section

**Title, l. 64**

- "How…" titles: none of 31. Declarative-sentence titles: 8 of 31, including Lee's "survives". Noun-phrase titles: about 15.
- The claim "outlasts its members" holds on comments only.
- Options for the owner, all shorter than a scope clause:
  - (i) a noun phrase: "Random growth and persistent decline in the hierarchy of online attention";
  - (ii) a sentence title that the evidence supports everywhere: "Volatility and decline set the shape of online attention";
  - (iii) keep the "outlasts" frame and scope it in the significance statement's first sentence.

**Significance statement, l. 74**

- Exemplars open with stakes or context in about 27 of 31 cases.
- The draft opens with the phenomenon and numbers. It never says why a reader outside the field should care, for example that durable concentration bears on entrenchment and on whether platform changes can redistribute attention. The draft makes that point only in Discussion ¶2.
- It ends with a causal link that the draft itself disclaims.

**Abstract, l. 79**

- The closest exemplars carry no numerals in their abstracts. The draft carries 4 numerals and about 13 quantitative statements in 240 words, uses "we" once, and has 40-word sentences.
- The last sentence has the "Through that channel" problem.
- The second-to-last sentence packs two persistence results into one.

**Introduction, l. 90–95**

- ¶1 is good: the puzzle, then a question. It lacks a stakes sentence.
- ¶2 crowds four literatures into one paragraph of 213 words with 11 `\cite` commands, ending with Baker & Smyth. The closest exemplars spread prior work over two or three paragraphs and end on a gap sentence (Kawakatsu ¶2–4, Sireci ¶3–6).
- The draft never says plainly, before the Results, the thing a referee will say: that random-growth theory already predicts the balance (rival a), so the open questions are whether it holds rank by rank in attention data and what moves the hierarchy. Paragraph 3 implies it. Saying it in one sentence pre-empts the strongest objection, as Juul's sufficiency framing does.

**Results subsection 1, l. 100**

- Good content. It uses "ladder" without defining it.
- Its last sentences, on named leaders, are vivid and should stay.

**Results subsection 2, l. 108–118**

- This answers a secondary question (why the top turns over fastest) before the main one (why the shape is durable).
- ¶2 on β (193 words) is calibration detail; the SI already carries the table.
- ¶3 on the occupant of rank one (100 words) is a side issue that one sentence and a pointer to the SI can carry.

**Results subsection 3, l. 126–147**

- This is the paper's core and should come second.
- ¶2 mixes the estimator's construction with results. In George, the estimator goes to Methods.
- ¶3 (335 words) is the densest paragraph in the paper.

**Results subsection 4, l. 150–160**

- ¶2 (the ablations) is the paper's most memorable result and is well placed.
- ¶3, the held-out forecasting (176 words), is unusual for the genre and works against the subsection's message. It needs two sentences in the main text and the table in the SI.
- Panel 4D moves with it.

**Results subsection 5, l. 167–182**

- At 1,067 words this is the longest subsection. It carries the discovery but also many robustness numbers in ¶1–2.
- ¶4 (292 words) on the head's share is important but can be shortened by half.
- The sentence admitting that the head's decliners have not been shown to be the persistent decliners (l. 182) is exactly Lee's style and should stay.

**Figures**

- No schematic.
- 17 panels.
- Rendering defects visible in the PNGs:
  - Fig. 3: panel A's title runs into the "B" label.
  - Fig. 4: panel C's title runs into "D".
  - Fig. 5: in panel D the "D" overlaps the title, which reads as "Dead lost participation".
  - Fig. 5B: x-axis labels collide ("later", "net of trend"), and its y-axis says "gapped momentum", a term not used in the text.
  - Fig. 1A: the y-axis "log activity relative to rank K (offset)" needs decoding.
- The file name `fig5_tipping` reflects an earlier framing (it is not visible in the PDF).

**Discussion, l. 186–190**

- 539 words in three paragraphs: summary with interpretation, then implications, then limits plus the protocol.
- Exemplars run 600–1,300 words and end on an outlook.
- Missing: one paragraph that assembles rival readings (a)–(d) and says which result answers each, in the manner of Sireci's two interpretations.

**Methods, l. 192–210**

- Fine in length.
- "Registration and AI use" is unique in the corpus. Keep it to about three sentences.

### 5.3 Ranked list of concrete edits

Each edit gives its location, what it changes, and the smallest change that would do the job.

1. **Remove the causal link and scope the frame on the first page.** (Significance l. 74, abstract l. 79, title l. 64.)
   - *What it changes:* the first page then says no more than the Results.
   - *Smallest change:*
     - Replace "Through that channel" with "Over the same three years" in both places.
     - In the significance statement, change "the shape of the hierarchy takes years to decades to change" to "on Reddit comments its shape takes about a decade to change".
     - The owner chooses among the title options in §5.2.
2. **Add stakes, one sentence each.** (Significance, a new second sentence; introduction ¶1 after its first sentence, l. 90.)
   - *What it changes:* it supplies the importance that the Board screens for.
   - *Smallest change:* one sentence on why durable concentration matters, for instance for entrenchment of leading communities and for whether a platform's changes can redistribute attention. Lift it from Discussion ¶2, l. 188.
3. **Thin the abstract and write it in the active voice.** (l. 79, target about 200 words.)
   - Keep at most four quantitative statements: the share of each top set replaced in a year; c/(s̄ − e) ≈ 0.64 or "two thirds"; persistence of +0.13 to +0.20 against at most −0.02 in every homogeneous world; the head's loss of a third of its share.
   - Use "we measure / we find / we build" for the authors' steps.
   - Split every sentence over 30 words.
4. **Convert the four headings that are not full claims into declarative claims.** (l. 99, 126, 150, 167.) For example:
   - "The hierarchy's shape changes far more slowly than its members turn over";
   - "Crossings return two thirds of what members lose";
   - "Measured volatility and decline build the hierarchy and hold it";
   - "Unlike homogeneous random growth, decline persists on Reddit".

   This follows the convention of George, Mallmin and Sireci.
5. **Add a schematic and define the terms once.** (New panel 1A, a definitions sentence at l. 100.)
   - *What it changes:* a broad reader learns ladder, top-*k* boundary, slide, crossings and entry and exit from one picture.
   - *Smallest change:* one panel in which members slide down at rate s̄, risers cross in at c = σ²/2S and entrants and exits add e. One sentence: "We call the rank–size curve of a top-*K* universe a ladder".
   - Replace "worlds" with "simulated null models" or "simulations" throughout.
6. **Put the main result second.** (Move l. 126–147 before l. 108–118.)
   - Shrink the geometry subsection to one paragraph of about 250 words: Eq. 1 and the controlled comparison.
   - Move the β paragraph (l. 116) and the rank-one paragraph (l. 118) to the SI, leaving one sentence each.
7. **Add a rival-readings paragraph to the Discussion.** (New ¶2 after l. 186, about 180 words.)
   - Walk through each rival in turn:
     - (a) the balance is automatic: so stated, and that is why the paper measures it, together with the null-model ratios;
     - (b) fixed heterogeneity: persistence fades within a year on submissions and holds on comments, which is consistent with growth types and does not separate a dynamic from fixed differences;
     - (c) platform shocks: comparisons that exclude the surge and the capture change, one at a time;
     - (d) selection on noise: cohorts fixed by permanent rank, and the size-blind baseline for Gomez's within-term.
   - End with what the sealed data will test.
8. **Move the forecasting result out of the main text.** (l. 160 and Fig. 4D.)
   - Two sentences: the model forecasts movement and one-year retention no better than simpler rules, and does not forecast the head's fall. Point to the SI table.
9. **Reduce named specialist constructs.** (Throughout.)
   - Name Gomez once, then write "the displacement term".
   - Explain "structure function" in Methods.
   - Move "gap identity" and "Atlas slope" to a parenthetical with citations.
10. **Consider a dual classification.** (l. 66; owner decision.)
    - "Social Sciences" together with "Applied Mathematics", as in Juul & Ugander; or "Economic Sciences" with "Applied Mathematics", from the random-growth tradition (Mori).
    - When suggesting three Board members, look to the fields that edited the closest papers: population dynamics, network science, economics of random growth. I could not verify current Board membership.
11. **Make the AI statement precise and check current policy.** (l. 206; SI §AI.)
    - Say that the figures were plotted by code written with AI coding assistants. The SI's "analyses, code, figures and drafts were produced with AI coding agents" could be misread as AI-generated images.
    - Check PNAS's current AI rules for where the disclosure goes. My only source for them is secondary.
12. **Fix the figure defects** listed in §5.2, and shorten the captions from about 680 words to about 450.
13. **Discussion order.** Summary; what is automatic and what is not (edit 7); implications (l. 188); limitations (l. 190, without the protocol detail); one closing outlook sentence that names the 2023–24 test.

### 5.4 Page budget

**The case for staying at 8.** Of 29 exemplar PDFs, 19 run 9 pages or more, and the closest papers run 6–12. PNAS allows 12. Eight pages, references included, is not by itself a mark against the draft.

**The case for about 7.** The density problem (finding 1) is real, and cutting words is the easiest way to lower it. Cuts that lose nothing a referee needs:

| item | words saved |
|---|---|
| β paragraph → SI (keep one sentence) | ~160 |
| occupant of rank one → one sentence | ~70 |
| geometry ¶1 trimmed, with controlled-comparison detail moved to SI | ~90 |
| estimator construction in subsection 3, ¶2 → Methods/SI | ~110 |
| subsection 3, ¶3 tightened | ~85 |
| forecasting → two sentences | ~125 |
| persistence robustness (surge, capture, membership) → SI | ~80 + ~115 |
| head-share paragraph halved | ~110 |
| subsection 1 trimmed | ~60 |
| captions | ~200 |
| **total** | **~1,200** |

- **Panels to the SI:** 2A (raw β), 2C (rank change), 3C (ingredients of turnover) and 4D (held-out error). That leaves 13 panels.
- **Additions:** the stakes sentences (about 40 words), the rival-readings paragraph (about 180) and the schematic panel.
- **Net:** about 1,000 fewer words, which should give about 7 pages including references. Shortening sentences without cutting content (edit 3 applied throughout) also helps, because shorter sentences use fewer words for the same claims.

## 6. Templates to follow

### 6.1 Structural models

1. **Sireci et al. 2023, "Environmental fluctuations explain the universal decay of species-abundance correlations with phylogenetic distance"** (Ecology; 11 pages; doi:10.1073/pnas.2217144120).
   - The best model for the logic of the argument: an empirical law first, then three rival mechanisms with three distinct predictions, then the one that reproduces the law, then an extension to time-series data.
   - Its Discussion lists the paper's limitations one by one and gives two readings of its own result, which is what our rival readings (a)–(d) need.
2. **George & O'Dwyer 2023, "Universal abundance fluctuations across microbial communities, tropical forests, and urban populations"** (Ecology and Physics; 11 pages; doi:10.1073/pnas.2215832120).
   - One minimal stochastic model across systems, with analytic predictions.
   - Claim headings.
   - A final Results subsection that pits the model against a rival noise model, with counts per system.
   - Explains its equations for a broad reader.
3. **Kawakatsu et al. 2021, "Emergence of hierarchy in networked endorsement dynamics"** (Applied Mathematics; 7 pages; doi:10.1073/pnas.2015188118).
   - The closest topic: how hierarchies form and endure.
   - The closest length: 7 pages and about 5,250 words.
   - Four figures and a model schematic in Fig. 1.
   - A plain limitations paragraph and restrained claims of generality.
4. **Juul & Ugander 2021, "Comparing information diffusion mechanisms by matching on cascade size"** (Social Sciences and Applied Mathematics; 6 pages; doi:10.1073/pnas.2100786118).
   - Platform data.
   - Frames its question as sufficiency, to separate an artifact from a mechanism.
   - The dual-classification precedent.
   - A 60-word Methods section, and a closing that follows its limitations.

**Sources for particular moves** (not for overall structure):

- Lee et al. 2022 (doi:10.1073/pnas.2211572119): the "outlast" frame, and taking no position on mechanism.
- Mallmin et al. 2024 (doi:10.1073/pnas.2312822121): a phenomenon statement about turnover under a conserved distribution, and rejecting a rival by comparing time scales.
- Xue et al. 2020 (doi:10.1073/pnas.1915088117): a heading about necessity, for the ablations.
- Marchi et al. 2021 (doi:10.1073/pnas.2103398118): an emergent time scale as the headline.

### 6.2 Skeleton of our paper

Target about 4,600 words of main text, plus about 450 of captions, about 115 for the significance statement and about 200 for the abstract.

**Significance (≤120 words, no numerals)**

1. Stakes: concentration of attention, and its durability.
2. The puzzle.
3. The answer: volatility against decline.
4. What random growth does not supply: persistence.
5. What that implies.

**Abstract (about 200 words, at most four numbers, active voice).** Context; data; the balance and its measured size; the model built from the measured ingredients and its ablations; persistence; the head's fall, with no causal wording.

**Introduction (about 600 words, 4 paragraphs, no heading)**

1. Stakes and puzzle, ending in a question.
2. What random growth and stochastic portfolio theory already imply. Rival (a) stated outright.
3. What is known online, and the gap: whether the balance holds rank by rank in attention data, and what moves it.
4. Here we… (three findings).

**Results (about 2,700 words)**

| subsection | words | claim heading (example) | content | figure |
|---|---|---|---|---|
| R1 | ~400 | The hierarchy's shape changes far more slowly than its members turn over | data, turnover, named leaders | Fig. 1: A schematic of the top-*k* bookkeeping; B rank–size curves by year; C rank trajectories; D share replaced by *k* |
| R2 | ~800 | Crossings return two thirds of what members lose | Eqs. 2–3; measured against predicted; balance and relaxation times; null models; one paragraph on geometry (Eq. 1) | Fig. 2: A measured against predicted crossings with null models; B crossings against slide; C turnover profile, data ladder against constant slope |
| R3 | ~550 | Measured volatility and decline build the hierarchy and hold it | model in words + Eq. 4; from equality; ablations; flatter than observed; forecasting in one sentence | Fig. 3: A from equality; B check of the stationary balance; C 30-year ablations |
| R4 | ~950 | Unlike homogeneous random growth, decline persists on Reddit | the persistence statistic against null models; items against engagement per item; the factor split; prior decliners carry the excess slide; the head's share falling through participation | Fig. 4: A gapped correlations against null models; B items, per item, net of trend; C the head's share decomposed; D the participation split |

**Discussion (about 750 words, 5 paragraphs)**

1. What we did and found, in the first person.
2. What is automatic and what is not, rivals (a)–(d) (edit 7).
3. Implications: entrenchment and platform design.
4. Limitations.
5. Outlook: the sealed 2023–24 test in one sentence.

**Materials and Methods (about 650 words).** Data; universe and permanent rank; estimators, including the structure function and the two-spacing difference; the model; simulated null models; registration and AI use (2–3 sentences).

**Data, materials, and software availability.** As in v4, with Meng as the precedent.

**SI additions.** The β section with its calibration; rank change; the ingredients of turnover; the held-out movement table; the persistence robustness checks.

## 7. What the earlier template attempts got right and wrong

**Right, and confirmed by the counts**

- **An unheaded opening.** The v3 platform template recommended no Introduction heading; 29 of 31 papers have none. The v5 template's advice to draft with an explicit Introduction heading goes against the corpus.
- **Length limits.** The significance statement around 90–120 words and the abstract around 190–245 match the medians (116 and 211).
- **The model's place.** The model in the main text, intuition before formalism, derivations in the SI.
- **Restraint on generality.** Restraint about "universal" is right for a paper with two platforms, though the closest exemplars do claim universality when they have enough systems (§3.3).
- **Figures.** The v5 template's plan of four strong figures matches the median.
- **First person.** The structure report's "we" counts of 3–59 per 1,000 words were correct. The templates said to use "we", and the draft did not.

**Wrong, or wrong for the current question**

1. **The question itself.** All four templates centre on measuring platforms under restricted data access, with a rank–share invariant and estimators from partial collection. The paper now asks an explanatory question, and that apparatus (a "post-API" contribution, a third Results subsection on estimation) should not carry over.
2. **Claim-first headings were overgeneralized, and the examples were misattributed.** v5 names Marchi as a model of "concise, claim-first subsections". Marchi's headings, like Kawakatsu's and Juul's, are topic labels. The convention belongs to a subset of four papers (§2.6).
3. **A priority-claim template.** The Claude 4.5 template's sentence pattern "We find/show for the first time…" invites the priority claims that the brief says PNAS discourages. Only 2 of 31 exemplars make one.
4. **Rules that pushed numbers into every paragraph.** The GPT-5.2 and v5 templates require "at least one numeric anchor" in each Results paragraph, and the Claude 4.5 template a fixed claim → evidence → mechanism → implication arc. The exemplars do neither uniformly, and following the rules literally may explain much of the draft's density.
5. **Introductions that were too long.** The Claude 4.5 template asks for 900–1,200 words in 7–10 paragraphs. The closest papers run about 490–970.
6. **The schematic in the wrong place.** The GPT-5.2 and Claude 4.5 plans put the model schematic in Fig. 3. Exemplars that use a schematic put it in Fig. 1.
7. **The structure report's coverage.** It covers 10 papers, not 12. Its section counts agree with mine within about 5% where I checked.

## 8. What I checked and what I propose

**Checked in this session against full text or primary pages**

- All counts in §§2–4 and the Appendix, with the caveats in §0.
- The quotation in §3.8.
- The locations of every paraphrased passage.
- The editor lines, tracks and dates of all 31 papers.
- The draft's word counts, from the TeX.
- The draft's style metrics, from the PDF text, by the same pipeline as the exemplars.
- The figure defects, seen in the PNG files.
- The archived PNAS policy statements (WebFetch; paraphrased).
- The template's guidance on the significance statement and the abstract (pdftotext).

**Proposed, for the authors to check or decide**

- Every edit in §5.3.
- The title options.
- The classification and the choice of Board members to suggest; I could not verify current membership.
- PNAS's current AI-use and fee policies; my sources are secondary or archived.
- Whether the 2020 archived review policy, including the share declined without review, still holds.
- My claim that the brief's statement that PNAS discourages novelty claims is not on the pages I could open. It may be on current pages I could not reach.

## 9. Works cited

**Exemplars** [full text: packet file and source-PDF text]

| paper | doi:10.1073/ |
|---|---|
| Agrawal et al. 2019 | pnas.1918484117 |
| Bahri et al. 2024 | pnas.2311878121 |
| Bartolucci et al. 2024 | pnas.2403682121 |
| Bourdais et al. 2024 | pnas.2403449121 |
| Brown et al. 2021 | pnas.2108031118 |
| Cramer et al. 2022 | pnas.2113561119 |
| Dick et al. 2023 | pnas.2218605120 |
| Gagler et al. 2022 | pnas.2106655119 |
| George & O'Dwyer 2023 | pnas.2215832120 |
| Ghio et al. 2024 | pnas.2311810121 |
| Gupta et al. 2022 | pnas.2207392119 |
| Hadad et al. 2021 | pnas.2014602118 |
| Heckmann et al. 2020 | pnas.2001562117 |
| Hendrick et al. 2025 | pnas.2501224122 |
| Jin et al. 2025 | pnas.2427181122 |
| Juul & Ugander 2021 | pnas.2100786118 |
| Kawakatsu et al. 2021 | pnas.2015188118 |
| Lee et al. 2022 | pnas.2211572119 |
| Maddu et al. 2025 | pnas.2420621122 |
| Mallmin et al. 2024 | pnas.2312822121 |
| Marchi et al. 2021 | pnas.2103398118 |
| Meng et al. 2025 | pnas.2410227122 |
| Minasandra et al. 2025 | pnas.2503962122 |
| Morales et al. 2023 | pnas.2208998120 |
| Mori et al. 2020 | pnas.1913014117 |
| Mori & Kori 2022 | pnas.2113620119 |
| Pospisil & Pillow 2025 | pnas.2506535122 |
| Pyo et al. 2023 | pnas.2220014120 |
| Sireci et al. 2023 | pnas.2217144120 |
| Wang et al. 2024 | pnas.2318380121 |
| Xue et al. 2020 | pnas.1915088117 |

**PNAS guidance**

- **Archived author pages** (journal-policies, purpose-scope, submission), April 2020 snapshot, URLs in §4.2 [full text via WebFetch; paraphrased].
- **PNAS LaTeX template, rticles gallery rendering:** https://eddelbuettel.github.io/rticles-gallery/pnas_demo.pdf [full text; paraphrased].
- **Commercial guides,** used only as prompts to check (§4.2) [secondary; not relied on]:
  - https://manusights.com/blog/pnas-ai-policy
  - https://manusights.com/blog/how-to-avoid-desk-rejection-at-pnas

## 10. Search log

All searches on 2026-10-19.

- **Local analysis** (python3, pdftotext, pdftohtml; read-only) of the packet files and the 29 named source PDFs:
  - headings, from fonts and run-in patterns;
  - counts of figures, equation labels, words, sentences, first person, hedges, questions, digits, "universal", "novel" and limitation sentences;
  - editor lines and dates;
  - quotation verification by whitespace-normalized matching.
- **WebSearch:**
  - PNAS author guidelines (length, significance statement, priority);
  - PNAS editorial expectations, desk rejection and Editorial Board;
  - PNAS Information for Authors (significance, Materials and Methods);
  - PNAS page length and fees;
  - PNAS AI policy;
  - Verma's PNAS editorials;
  - Rand & Pfeiffer 2009 on publication tracks (not used).
- **WebFetch:**
  - pnas.org author-center pages (HTTP 403);
  - PMC and Europe PMC (blocked or HTTP 500);
  - Crossref for 10.1073/pnas.1602165113 (exists; content not read; not used);
  - the archive-it snapshots of three PNAS author pages (read);
  - the rticles PNAS template PDF (read, then extracted locally).

## Appendix: the 31 exemplars

Abbreviations: Dir. = Direct Submission; Contr. = Contributed; days = from receipt to acceptance; pages = PDF pages including references (n/a for the EPUB conversions); eqs = numbered display equations in the main text including Methods; heads = type of Results headings (claim = majority full-sentence claims; NP = evaluative noun phrases; topic = neutral labels); lim. = an explicit passage on the authors' own limitations or caveats.

| paper | classification | track; editor | days | pages | figs | eqs | heads | lim. |
|---|---|---|---|---|---|---|---|---|
| Agrawal 2019 | Chemistry | Dir.; Bell | 35 | 6 | 3 | 4 | topic | no |
| Bahri 2024 | Physics | Dir.; Tu (guest) | 193 | n/a | 3 | 6 | topic | yes |
| Bartolucci 2024 | Applied Math | Dir.; Parisi | 173 | 12 | 11 | 52 | topic | no |
| Bourdais 2024 | Applied Math | Dir.; Petzold | 125 | 9 | 5 | 9 | topic | yes |
| Brown 2021 | Applied Math; Econ. Sci. | Contr.; Cohen | 153 | 9 | 2 | 20 | topic | no |
| Cramer 2022 | Statistics; Pop. Biol. | Dir.; Wachter | 184 | 12 | 5 | 0 | mixed (3/7) | yes |
| Dick 2023 | Computer Sci. | Contr.; Kearns | 60 | 9 | 4 | 4 | topic | yes |
| Gagler 2022 | Biophys. & Comp. Biol. | Dir.; Koonin | 255 | 12 | 6 | 0 | claim (3/3) | no |
| George 2023 | Ecology; Physics | Dir.; Hastings | 356 | 11 | 7 | 6 | claim (4/5) | no |
| Ghio 2024 | Physics | Dir.; Tu (guest) | 226 | 10 | 2 | 23 | topic | no |
| Gupta 2022 | Systems Biol. | Contr.; Lander | 72 | 10 | 4 | 0 | topic (1/6) | yes |
| Hadad 2021 | Statistics; Econ. Sci. | Contr.; Athey | 221 | 10 | 4 | 25 | topic | no |
| Heckmann 2020 | Systems Biol. | Dir.; Cornish-Bowden (guest) | 168 | 9 | 5 | 0 | mixed (2/5) | no |
| Hendrick 2025 | Environ. Sci. | Contr.; Rinaldo | 166 | 8 | 4 | 12 | NP | no |
| Jin 2025 | Statistics; Social Sci. | Dir.; Bickel | 241 | 12 | 12 | 9 | topic (1) | no |
| Juul 2021 | Social Sci.; Applied Math | Dir.; Watts (guest) | 228 | 6 | 4 | 0 | topic | yes |
| Kawakatsu 2021 | Applied Math | Dir.; Hastings | 218 | 7 | 4 | 6 | topic | yes |
| Lee 2022 | Neuroscience | Contr.; Nicoll | 67 | 5 | 4 | 0 | none | no |
| Maddu 2025 | Biophys. & Comp. Biol.; Applied Math | Dir.; Nieminen | 287 | 12 | 5 | 10 | topic | no |
| Mallmin 2024 | Ecology | Dir.; Hastings | 189 | 12 | 7 | 35 | claim (5/6) | no |
| Marchi 2021 | Physics; Evolution | Dir.; Fisher | 94 | 8 | 4 | 20 | topic/NP | caveats |
| Meng 2025 | Applied Math | Dir.; Barabási | 208 | n/a | 4 | 5 | topic | yes |
| Minasandra 2025 | Ecology; Biophys. & Comp. Biol. | Dir.; Robinson | 56 | 10 | 3 | 0 | NP | no |
| Morales 2023 | Biophys. & Comp. Biol. | Dir.; Sejnowski | 208 | 12 | 5 | 10 | topic (1) | partial |
| Mori 2020 | Econ. Sci. | Dir.; Scheinkman | 195 | 7 | 6 | 5 | topic | no |
| Mori & Kori 2022 | Applied Math | Dir.; Sejnowski | 146 | 5 | 4 | 14 | topic | yes |
| Pospisil 2025 | Neuroscience | Dir.; Sejnowski | 169 | 11 | 4 | 19 | topic | no |
| Pyo 2023 | Biophys. & Comp. Biol. | Dir.; Olvera de la Cruz | 132 | 7 | 4 | 10 | topic (1) | no |
| Sireci 2023 | Ecology | Dir.; Marquet | 285 | 11 | 4 | 42 | claim (4/4) | yes |
| Wang 2024 | Microbiology | Dir.; Nielsen | 158 | 8 | 4 | 8 | none | yes |
| Xue 2020 | Evolution; Physics | Contr.; Goldenfeld | 160 | 9 | 6 | 10 | topic/NP | no |