# Raw run logs — IG censoring rescue (MODEL_STATUS §2z-c / §2z-d)

Verbatim outputs behind every number in §2z-c (rescue) and §2z-d
(operationalization + Tier-2 comparison). Commands are recorded in those
sections; pre-registration in `llm_fitting/ig_censoring_prereg.md`.

| file | what |
|---|---|
| `ig_censoring_forensics.log` | P1 (naive) + P2 on the top-50k cut file |
| `ig_censoring_forensics2.log` | P1 refined — per-post decomposition, c² = 0.253 |
| `ig_censoring_forensics3.log` | full-panel instrument health + P2 re-test (L2 dropout) |
| `ig_censoring_forensics4.log` | cohort dropout week-correlation, K concentration, P3 |
| `ig_sigma_obs_check.log` | fitted σ_obs vs n_posts thinning envelope |
| `card_hm_5rep.log` / `card_hm_20rep.log` | instagram_hm in-sample card (9/15) |
| `card_pp_5rep.log` | instagram_pp scored FAILURE (φ→0 degeneracy, 7/15) |
| `oos_hm.log` | Tier-0 OOS movement gate (at par, scale 1.0×5) |
| `oos_hm_dist_scores.log` | gate + CRPS/PIT/W1-ref proper scores |
| `community_hm_10rep.log` | Tier-2 community layer (S(1) overshoot ~2.9×, kernels) |

Note: forensics logs were regenerated 2026-07-06 from the committed scripts
(deterministic); model-run logs are the session originals.
