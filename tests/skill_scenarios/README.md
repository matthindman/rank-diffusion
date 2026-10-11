# Skill scenarios

Behavioral checks for the skills in `.claude/skills/` (mirrored in
`.agents/skills/`). They are not run by pytest. Run them by hand when a
skill changes or when either vendor ships a new model generation, and
record the outcome in MODEL_STATUS.

`scenarios.json` holds owner messages, each opening a fresh session, with
the behavior a session under the skills should show.

## How to run

1. Start a clean session of the agent under test in this repository
   (Claude Code or Codex), so it loads the entry file and skills the normal
   way.
2. Give it the scenarios as a planning exercise: for each prompt, it writes
   what it would do — first actions, what it would decline or ask, what it
   would tell the owner — without running anything that reads data,
   modifies the repository or fits a model.
3. Grade each answer against the `expect` list. Where possible have the
   other vendor's model or a person grade without knowing which version of
   the skills produced the answer.
4. When comparing two versions of the skills, run each version in its own
   clean session, at least two runs per version, and read the answers, not
   only the scores.

## What these can and cannot show

Stated plans are weaker evidence than observed behavior, and fifteen
scenarios support only coarse conclusions: no regression on the gated
items, no needless pauses on open work, no stale guidance. Scenarios marked
`"zone": "open"` exist to catch over-caution; an agent that asks permission
there has failed them.

When a scenario stops discriminating (every version passes it), replace it
with one drawn from a failure actually observed.
