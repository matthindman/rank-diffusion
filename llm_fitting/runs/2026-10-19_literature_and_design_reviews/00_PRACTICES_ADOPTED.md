# What this round takes from the GPT-6 Pro report on AI review, and what it leaves

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-19._

**Source.** The owner supplied a report produced by GPT-6 Pro on 19 October 2026: "Using AI to strengthen a PNAS manuscript", 15 pages, with a chat summary. It is not committed here because it is the owner's document. The owner's instruction is to treat it as guidelines, not gospel, and to use judgment.

## Adopted, and why

1. **One authoritative packet and a short, neutral scientific brief.** The brief states the question, the evidence, what is post hoc, the limits and the strongest rival explanation. It does not coach the reviewer to think the work is exceptional. Five referee rounds showed that a model told only what the paper claims reviews the claim, not the evidence.
2. **An access check before analysis.** Each report opens with what it actually read, what it could not open, and whether each source was full text or abstract only. Earlier rounds sometimes reviewed a figure from its caption.
3. **Claim-level verification, with quotation honesty.** Every quotation must be exact, short, in quotation marks, located (page or section), and labelled as checked against the full text or not. A real DOI does not show that a paper says what the sentence says. This matters most for the owner's request for exact quotes with page numbers: a fabricated quote is worse than none, so an unverified quote must be labelled as such.
4. **Neighbouring-field vocabulary, and searching both ways.** The draft sits between stochastic portfolio theory, firm and city growth, ecology, cultural evolution and the study of platforms. Each uses its own words for the same objects (rank turnover, churn, Zipf dynamics, cumulative advantage, neutral drift). The reviewer is asked to search for work that would weaken the contribution as hard as for work that supports it.
5. **Priority over volume, with no quotas.** Ask for a ranked short list first and allow zero. The report's evidence agrees with five rounds of experience: long lists cost adjudication time and bury the two items that matter.
6. **Separate first assessments.** The two models do not see each other's reports. Agreement is a lead, not verification, and every consequential claim is rechecked here before it changes the paper.
7. **Concise outcome briefs over elaborate role scaffolding.** These briefs specify the objective, the evidence, what the reviewer may do and the deliverable, and drop theatrical personas. One reason is the report's reading of current Astra guidance. The other is that the round-5 panel personas produced overlapping text.
8. **A light search log.** Dates, main queries and tools, enough for a later pass to extend the search. No "systematic review" label.
9. **Fresh wording and local, inspectable recommendations.** Each recommendation says where in the draft it goes and the smallest change that would do the job.
10. **A record of models and settings.** Model, interface, reasoning setting, tools and date go in each report's provenance header and in MODEL_STATUS. This is needed for the PNAS AI-use statement.

## Not adopted, or adapted

- **A preliminary four-task calibration of the models.** Not needed now. The owner chose the two models, and five rounds have already shown how each behaves on this paper.
- **Fable 5.1 as the second reviewer.** The owner asked for Opus 5.5. The report's Fable-specific cautions (unmarked source reuse, dense prose) are folded into rule 9 for both models anyway.
- **Google Deep Review, Elicit and other external services.** Not used. They would add confidentiality questions, and these tasks do not need them.
- **PRISMA-S/PRESS formality.** Only the search-log habit is borrowed.
- **Prose-revision prompts 6–7 (controlled editing and meaning comparison).** Kept for the editing stage, after the synthesis. They are not part of this round. The meaning-comparison discipline already runs in every manuscript revision here: protected numbers and claim language are owner-gated.
- **The report's statements about vendor documentation** (effort defaults, retention). Taken as operational hints, not verified facts. Neither these briefs nor the synthesis relies on them.

## Settings used

| task | GPT-6 Astra (Codex CLI 0.163.0, `--search`, read-only sandbox) | Claude Opus 5.5 (Claude Code sub-agent, WebSearch/WebFetch) |
|---|---|---|
| T1 platform literature | reasoning `high` (retrieval-heavy) | default |
| T2 stochastic-model methods | `xhigh` | default |
| T3 PNAS exemplars | `high` (reading-heavy) | default |
| T4 many models | `xhigh` | default |
