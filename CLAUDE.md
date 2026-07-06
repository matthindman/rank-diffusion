# rank-diffusion — Claude Instructions

Mature, externally reviewed research program (PNAS-track) modeling rank
dynamics of digital attention. Strict methodology; committed results must be
protected.

**Before any modeling, fitting, evaluation, data-intake, or paper work, read
`.claude/skills/stochastic-modeling/SKILL.md` in full** — it is the core
skill (method contract, frozen specs, pitfall catalogue, current agenda).
Then read the newest dated §2 subsections of `llm_fitting/MODEL_STATUS.md`
(the canonical record — all numbers live there) and, if touching new data,
`llm_fitting/CONFIRMATION_PROTOCOL.md` (frozen; binding).

Quick rules (full contract in the skill):
- OOS movement gate = only acceptance criterion; never single-split numbers.
- No spec-fishing; pre-register predictions; report failures as findings.
- Defaults byte-identical; legacy guard (facebook 14/15 / churn 0.013) +
  `python -m pytest tests/ -q` (repo root) green at every commit.
- Estimate by permanent rank, never current rank.
- FB: cutdown parquet or SSD Era A only; IG = negative control.
- Head-churn metrics need bands (seed SD ±0.15); reps ≥ 20 for head claims.
- Append dated sections to MODEL_STATUS.md; never rewrite history.
- Frozen specs/thresholds/protocols are owner-gated — ask before changing.
