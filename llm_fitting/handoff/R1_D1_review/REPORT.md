# R1 handoff — completed artifact review
OpenAI Codex, 2026-10-12. Branch codex/r-d1-review; base main 191422962f3ba22fe6db11d19d860fd779cc42f0. Design/check implementation committed before execution at 15ef4ced45a200cc535a0d39081b875dc116e15c.

## Work completed
Read the current D1 dossier and cited A-task source/record/results, §2z-as–bd, V1/V2 evidence, the standing review brief, and the subsequent signed candidate-2 consensus/queue on main. REVIEW.md is the one review round, ranked by consequence. No file under review was changed.

Executable checks use deterministic fixtures and existing result archives only. Three check predictions passed:
1. A4 half-panel N halves; unclipped coordinates shift log(2); half the fixture clips at zero. PASS.
2. Common activity growth produces nonzero ladder drift with stable normalized shape and identities. PASS.
3. The complete strong random-walk/current-rank-attraction pair table contains separated endpoint rows beyond the failed declared middle contrast. PASS as an unblinded verification only: the correction's counts were read and disclosed before this check.

DESIGN items 4–5 were static provenance/document checks, not numerical predictions: they verified the long-submissions 20-seed reference and superseded operative recommendations. No predeclared check failed. These are review checks, not a model validation or adoption verdict.

## Commands
From this working-copy root, registered Python:
```sh
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 llm_fitting/validation/review_d1_checks.py --out llm_fitting/handoff/R1_D1_review/CHECKS.json
bash scripts/health_check.sh
```
The helper retrieves the complete V2 pair archive from committed blob 6772c1d8c8d7b94159052e9b051649bf6797b318; main deliberately excludes this large archive. CHECKS.json records SHA256 and source hashes. It is not a substitute data source.

The additional descriptive volatility audit selects exactly `statistic == "volatility/group=middle/ratio"` from the existing `llm_fitting/runs/2026-10-12_V2_planted_mechanisms/diagnostic_rates.csv.gz`, preserving every panel and condition (54 rows). Reproduce with:
```sh
/Library/Frameworks/Python.framework/Versions/3.11/bin/python3 - <<'PY'
import pandas as pd
d = pd.read_csv("llm_fitting/runs/2026-10-12_V2_planted_mechanisms/diagnostic_rates.csv.gz")
q = d[d.statistic.eq("volatility/group=middle/ratio")]
assert len(q) == 54
q.to_csv("llm_fitting/handoff/R1_D1_review/V2_MIDDLE_VOLATILITY_ALL.csv", index=False)
PY
```
This auxiliary extraction was added after reading §2z-bd and is not a predeclared mechanism test. The first exploratory name lookup used the nonexistent prefix “volatility_persistence,” returned zero rows and wrote an empty CSV; that lookup was corrected to the existing key above. No model run failed or was killed.

## Validation and handoff
Health checks before the design commit passed: 479 research tests plus 13 subtests, eight package tests, legacy 14/15 and churn 0.013. HEALTH_DESIGN.txt retains the log. Final health checks also passed: 479 research tests plus 13 subtests, eight package tests, sealed-guard self-check and legacy 14/15 with churn 0.013; HEALTH_FINAL.txt retains the log. Protected model/record/ledger/default files were not edited. There were zero gate calls in R1; GATE_ROWS.md says so.

The actionable implementation finding is in A4's profile-reliability coordinate, with an empirical rerun proposed for Claude. The review itself has no missing input and needs no QUESTIONS.md. V3's separate timing question remains in its own handoff. Candidate 2 is withdrawn under the separate closed consensus; nothing here reopens it. Nothing adopted, no new agenda or paper wording, no push.

Not run: a corrected empirical A4 fit, nuisance-refitted mechanism classification, candidate 3/4 fitting, any 40-seed candidate comparison, or a new reference/adoption evaluation.
