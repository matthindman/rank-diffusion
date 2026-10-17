# Higher altitude, then deeper: the prompt

_Written by Claude Opus 5.5 (Anthropic), 2026-10-18, at the owner's request
("write — and then follow — your own 'higher altitude' and 'deeper' prompt,
looking at the current status of the paper, and what would improve it to the
point it would be enthusiastically endorsed by editors and reviewers at
PNAS"). It is written to be followed by the session that wrote it, so it
also names the habits that session falls into. The answer is
`MEMO.md` in this folder, and the record is MODEL_STATUS §2z-dc._

---

I don't want another review of this paper, and I don't want a longer list of
fixes. I want a higher-altitude version of the question first, then a deeper
version of the one answer that matters. Three critiques and a day of
corrections have made the paper honest. They have not made it exciting, and
honesty without a reason to care is a referee's "correct but incremental".

## Altitude: what is this paper for?

Put the repository's vocabulary away: balance law, B, boundary ratio, spacing
differences, §-letters. Then answer, in the order given:

1. **Who is the reader?** Who opens a PNAS paper on online attention: a
   network scientist, an inequality economist, a computational social
   scientist, a platform-policy researcher? What question do they already
   carry that this paper answers? Name the debate, not the method.
2. **What one sentence would a science journalist write?** If none can be
   written from what is now true, say so. That is the finding.
3. **What does the reader believe now that the data contradict, or settle?**
   "Rich get richer" (preferential attachment, cumulative advantage). "The
   platforms are locked in" against "the platforms say anyone can rise".
   "Zipf is a stable law". What is the cross-domain comparison that turns a
   platform paper into a paper about inequality?
4. **Why does it matter now?** Platform governance, concentration of
   attention, the precedent in wealth that Gomez set. Be specific.
5. **What is the unreasonable target?** State the most ambitious claim these
   data could carry. Then argue for it as an advocate would, and find the
   version of it that is solvable now.

## Range: three headlines, not one

Write three candidate headline results, each one sentence long. For each,
give:
- the evidence it needs;
- what is already in hand;
- what is missing;
- how a hostile expert would attack it first.

Score each on expected enthusiasm × defensibility with the data on hand.
Choose one. The other two become supporting results or are dropped.

## Depth: make the chosen headline unassailable, by doing the work

- **The attack.** For the chosen headline, find what a hostile expert attacks
  first. In this program that is usually one of three things: regression to
  the mean, measurement artefact, or "mechanical". Find it, then run the
  analysis that answers it. Do not describe the analysis; run it.
- **The method check.** Before trusting a statistic on data, show in a
  planted world that it recovers the right answer *and* that its naive
  version gives the wrong one.
- **The numbers.** Every number comes from a script in this folder, with
  intervals, across every ladder where it can be measured (Reddit comments,
  Reddit submissions, Facebook pages). Where it cannot be measured (the
  Instagram levels; the submissions head), say so.
- **Failure.** If the headline fails, report the failure and fall back to
  the next headline. Do not soften the result to keep the story.

## Then make it a paper

Rewrite what an editor sees first:
- title;
- significance;
- abstract;
- Figure 1;
- the first paragraph.

Do all of it around the chosen headline, in a new draft that does not
overwrite the current one. Say what would still stop an enthusiastic
endorsement after this, and what would fix it.

## Rules (binding)

- AGENTS.md holds. Sealed folders are untouched. Development panels only,
  each contact logged in `EXPOSURE_LEDGER.md`.
- Instagram levels are out of scope (§2z-c, §2z-cx).
- Claim language stays a draft for the owner. Protocol v3 is set aside, at
  the owner's request.
- Do not judge the work by its own documents. Measure.
- **The habits to guard against, from this session's own record:**
  - claiming before checking (§2z-da: "at par", "stable form", "at every
    rank");
  - narrating instead of running;
  - letting an exciting headline survive a planted world that says it is
    mechanical.
