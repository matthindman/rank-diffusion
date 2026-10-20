<!-- Provenance: research memo on PNAS review anonymity, self-citation and AI-figure policy, by a Claude Opus 5.5 sub-agent (Claude Code; WebSearch/WebFetch; Wayback captures via curl), 2026-10-20. Saved verbatim by the lead agent. -->

## Memo: PNAS review anonymity, self-citation of the 2018 book chapter, and AI-use rules (as of 20 Oct 2026)

**Access.** pnas.org returned HTTP 403 throughout. WebFetch cannot reach web.archive.org, so I read the Wayback captures with curl. The newest captures are 31 Jul 2026 (policies) and 17 Jun 2026 (submission). Nothing in the repository was modified.

### Sources and how I reached them
- **[A] PNAS, "Editorial and Journal Policies."** Wayback capture of 31 Jul 2026: https://web.archive.org/web/20260731061433/https://www.pnas.org/author-center/editorial-and-journal-policies. Primary source. No later capture exists.
- **[B] PNAS, "Submitting your manuscript."** Wayback capture of 17 Jun 2026: https://web.archive.org/web/20260617040225/https://www.pnas.org/author-center/submitting-your-manuscript. Primary source.
- **[C] PNAS LaTeX class `pnas-new.cls` v1.47 (2025/11/01).** Local copy at `/Users/hindman/Documents/GitHub/rank-diffusion/paper/pnas-new.cls`. Primary source.
- **[D] PNAS journal-policies page, 2020.** Archive-It capture of 21 May 2020 (bac-lac.wayback.archive-it.org). Historical.
- **[E] Berenbaum, "On peer review—then, now, and soon to be?"** PNAS 120:e2302593120 (2023). Full text via Europe PMC (PMC10242717).
- **[F] Blau et al., "Protecting scientific integrity in an age of generative AI."** PNAS 121:e2407886121 (2024). Full text via Europe PMC (PMC11145223). PNAS's policy cites this paper.
- **[G] Manusights, "PNAS AI Policy."** Secondary source, updated 8 Oct 2026: https://manusights.com/blog/pnas-ai-policy
- **[H] Elsevier, "What are the requirements for double-anonymized peer review?"** Updated 28 Aug 2026: https://www.elsevier.support/publishing/answer/what-are-the-requirements-for-doubleanonymized-peer-review
- **[I] Taylor & Francis, "Anonymous peer review."** Undated (©2026): https://authorservices.taylorandfrancis.com/publishing-your-research/peer-review/anonymous-peer-review/
- **[J] Springer Nature, "Double-anonymous peer review."** Undated (©2026): https://www.springernature.com/kr/snapp/submitting/how-to-submit/double-anonymous
- **[K] Cambridge, APSR "Preparing your materials."** Fetched 20 Oct 2026: https://www.cambridge.org/core/journals/american-political-science-review/information/author-instructions/preparing-your-materials
- **[L] ACM SIGPLAN, "Policies Regarding Review of Conference Papers."** Page generated 18 Oct 2026: https://sigplan.org/Resources/Policies/Review. Plus the PLDI 2014 FAQ on double-blind review (last modified 13 Sep 2013).
- **[M] IEEE.** Communications Letters double-anonymous track (comsoc.org) and the RFIC 2026 double-blind policy. Read only through search summaries.
- **[N] AAPT / American Journal of Physics, anonymous-review guidance.** Undated (©2026): https://aapt.org/Publications/AJP/anonymous_review.cfm
- **[O] AEA.** Moved all its journals to single-blind review on 1 Jul 2011; the AER editorial policy still says single-blind. Read only through search summaries (AEA statement as reproduced by Organizations and Markets; aeaweb.org).

### 1. PNAS review anonymity
- **Single-anonymous only.** [A] says Direct Submissions get single-anonymous review: reviewers' names stay confidential unless the reviewers agree otherwise. Contributed submissions have open review with named reviewers.
- **No double-anonymous option.** I found none in [A], [B], [D] or [E], and no request mechanism.
- **The manuscript identifies the author by design.** [B] requires a title page with author names, affiliations and corresponding-author contacts. The corresponding author must supply an ORCID at submission, and the contributions statement and competing-interest declaration identify authors too.
- **The template has no way to anonymize.** The class file [C] has no anonymous or blind option; its only review option is `lineno`, which is commented out.
- **This has held since at least 2020.** [D] says Direct Submissions have anonymous peer review, and the editor's name is hidden from authors until acceptance.
- **Practical consequence.** Reviewers will see the author's name. Masking a self-citation hides nothing at PNAS. One could write to the editorial office, but no stated provision exists.

### 2. Self-citation under anonymized review: standard practice elsewhere
Where review is double-anonymous, the major publishers and societies largely agree on four points:
- **Cite your own work in the third person.** For example, "Smith and Black (2007) showed," not "we previously showed." This is the rule at Springer Nature [J], Taylor & Francis [I], Elsevier [H], APSR [K], ACM SIGPLAN [L] and IEEE double-anonymous venues [M] (Communications Letters forbids "my" or "our" for prior work).
- **Do not delete or redact essential self-references.** Elsevier says not to eliminate them [H]. APSR says not to redact self-citations [K]. The PLDI FAQ [L] warns that leaving out your own precursor makes it look as if you are replicating earlier work.
- **Placeholders are a fallback, not the default.** Elsevier allows "[Anonymous, 2007]" in the text with "Details omitted for double-anonymized reviewing" in the references [H]. Taylor & Francis allows an "[author(s)]" placeholder when first-person reference is unavoidable [I]. AJP prefers an anonymized copy of an essential work uploaded as reviewer-only material, and rules out a note saying a citation was removed [N]. Practice varies: some APA journals forbid "author" placeholders.
- **Remove acknowledgments and funding.** They are supplied separately and restored in the published version [H, I, J].

In final versions, the full reference, names and acknowledgments come back, and first-person framing becomes acceptable. Not everyone uses this system: AEA journals dropped double-blind review in 2011, citing search engines and the cost of conflict-of-interest checks [O].

### 3. PNAS on AI-made figures and AI disclosure
- **Disclosure location and content [A].** Use of AI during research must be disclosed in Materials and Methods, or in the Acknowledgments if there is no Methods section. The tool's name and specific model or version should be given. AI cannot be an author, and the authors are solely accountable for, and must fact-check, AI-assisted output.
- **Images.** The key clause in [A] reads "AI tools for creating images or graphics may not be used," with an exception only when the software is itself the subject of the work. AI-generated cover art is not accepted ([A], [B]).
- **Screening.** PNAS says it screens submissions with Pangram to detect AI-generated content [A].
- **Figures plotted by AI-written code.** No PNAS text addresses this case. My reading, which is an interpretation and not a ruling: the ban targets AI tools that create the graphic itself. A figure drawn deterministically by plotting code (matplotlib, for example) from deposited outputs is made by the plotting library, not by an AI image tool. Writing that code with an AI assistant is "AI use during the research process" and is covered by the Methods disclosure. Neither [F] nor [G] resolves this either:
  - [F] asks that AI-generated imagery carry provenance marking so it is not taken for real observations.
  - [G] says AI-assisted analysis code needs disclosure plus validation, and says nothing on code-plotted figures.
  - [G] also mentions an "editorial office permission" exception that the 31 Jul 2026 policy capture does not contain. Treat it as unverified.
- **To be safe:** say explicitly in Methods that the figures were rendered by deterministic plotting code (name the library) from deposited outputs, with no generative image tool. Ask the editorial office only if a figure contains AI-generated graphics, such as a schematic.
- **The current draft** (`paper/pnas_draft.tex`, "Registration and AI use" subsection) already puts the disclosure in Methods and names Claude Opus 5.5 and Codex gpt-6-astra, as PNAS requires. To fully meet [A]:
  - Name the review models and their versions as well; the draft only mentions "four rounds of review by further models."
  - Change "the authors' direction" to the singular.
  - Add the figure-provenance sentence above.
  - Add one sentence accepting accountability.

### Other PNAS rules that bear on the chapter
- **Prior publication [A].** The submission must not have been previously published, judged case by case. Figures published elsewhere must be identified, with permission from the copyright holder (here, Princeton University Press).
- **Text overlap.** [B] encourages following the Text Recycling Research Project's best practices, and [A] says iThenticate screens for similarity to published works.
- **No priority claims.** [B] says not to include statements of novelty or priority.
- **Reference format.** [B] requires page ranges for book chapters.

### Recommendation
**Submitted version.** Cite the chapter openly. Do not mask it.
- **Why not mask.** Under single-anonymous review the author's name is on the title page, so an "Anonymous (2018)" placeholder hides nothing. It would also look like concealing a related prior publication, which is exactly what the prior-publication policy and iThenticate screening look for.
- **The reference.** Use PNAS numbered style: M. Hindman, *The Internet Trap: How the Digital Economy Builds Monopolies and Undermines Democracy* (Princeton University Press, Princeton, NJ, 2018), chap. 5, pp. [verify]. The chapter title and pages need checking; I did not confirm them.
- **The wording.** Mention it in one or two places, in neutral numbered form, for example: "The question of why the top of the ranking stays stable while its members change, and a stochastic-growth approach to it, were set out in ref. N (chap. 5); here…" PNAS's numbered style makes this read naturally, avoids "my earlier work" phrasing and avoids sounding like a priority claim.
- **The cover letter.** State the relationship plainly: the paper continues the chapter's question and method, and is new in its data, model and held-out tests. Confirm that no chapter text or figures are reused, or list any that are, with permission. This lets the editor judge novelty openly.

**Final version.** Draw the through-line explicitly. Name the chapter as the origin of the question and method in the Introduction (first person is fine for a single author), and return to it in the Discussion to say what the new work confirms, corrects or extends. Frame it as lineage, not priority ("first to show"). The reference entry stays the same. If the paper ever goes to a double-anonymous venue instead, use the standard approach: third-person citation kept in the reference list; a placeholder or reviewer-only copy only if the third-person form still gives you away; acknowledgments removed; all of it restored at acceptance.