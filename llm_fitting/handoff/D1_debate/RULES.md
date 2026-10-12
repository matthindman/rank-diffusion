# How this debate runs

Two participants: Claude (writes on `main`) and Codex (writes on branch
`codex/d1-debate`, in its own working copy). Files live in this folder.

1. **Round 1, independent.** Each writes a first position without seeing
   the other's: `CLAUDE_1.md`, `CODEX_1.md`. Claude's is written first and
   held back; only its SHA-256 is committed (`CLAUDE_1.sha256`). Claude
   publishes the file after `CODEX_1.md` is committed, so either side can
   check that neither position was written with the other in view.
2. **Round 2.** Each reads the other's first position and answers it point
   by point: where it agrees, where it does not and why, and a proposed
   text for the consensus. `CLAUDE_2.md`, `CODEX_2.md`.
3. **Round 3, only if needed.** One more exchange.
4. **Close.** Claude drafts `CONSENSUS.md`; Codex writes `CODEX_SIGNOFF.md`
   saying AGREE, or listing what it does not agree with. If three rounds do
   not produce agreement, the consensus file states the disagreement and
   the measurement that would settle it. The owner decides either way.

What a position must contain: an answer to each of the four questions in
`QUESTION.md`; the literature it rests on, each citation marked as read in
this session, known from training and not re-read, or seen only in a search
summary; what would change the author's mind; a concrete recommendation.

Ground rules.

- Agreement between two models is not evidence (stochastic-modeling skill,
  rule 12). For every empirical claim the consensus names the measurement
  that would test it. Nobody concedes a point to end the debate.
- No model code, no fits, no simulations, no gate evaluations in this
  debate. Reading the record, the code and the literature is the work.
- Codex: this session is separate from the one running V2. Do not use the
  V2 working copy, and do not write anywhere but this folder on your
  branch. The V2 session must not read this folder until its part 4 is
  committed.
- Budget: three rounds each, at most.

Waiting. Codex commits and then checks for Claude's reply with
`git show main:llm_fitting/handoff/D1_debate/<file>` every few minutes for
up to 45 minutes; if nothing has arrived, it stops with a line saying so,
and the owner can tell it to continue. Claude is woken by a watcher on
Codex's branch.
