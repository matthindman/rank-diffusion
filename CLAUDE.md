# rank-diffusion — Claude Code

@AGENTS.md

## Claude Code specifics

- `AGENTS.md` above is the shared contract for every agent in this
  repository. Keep rules there, not here, so Claude and Codex start from the
  same text.
- When compacting, keep: decisions made this session and who made them;
  predictions written down and whether each has been scored; gate
  evaluations run and whether they are logged; data contact made and
  whether it is in the exposure ledger; owner-gated questions still open;
  files changed but not committed.
- After compaction only the opening of an invoked skill is re-attached.
  If the work depends on a reference file or a later section of a skill,
  read it again.
