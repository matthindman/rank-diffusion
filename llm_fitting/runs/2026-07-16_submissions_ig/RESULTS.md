# Submissions backtest + IG 2023 — results (2026-07-16/17)

PREREG_2026-07-16, one pass per item, outcome-unseen cross-metric replication
(NOT confirmation). Canonical writeup: MODEL_STATUS §2z-ae. K = 5,000.

| # | test | gating | verdict | key number |
|---|------|--------|---------|------------|
| P0/P1 | intake gate | hard | **PASS** | 142.4M→53.7M exact, 212 wks, guard 0 |
| P2 | census | hard | **PASS** | new-ids/wk ~55k, CV 0.206 |
| P3 | universe K | rule | **K=5,000** | share 0.918 (2,500→0.856) |
| P4a | s-trend (headline) | hard | **FAIL** | s 1.113→1.082→1.130 (non-monotone) |
| P4b | era vs composition | hard | **PASS** | era +0.274 > comp −0.257 |
| P5 | amplitude collapse | hard | **PASS** | b(4)=0.977, b(8)=0.965 |
| P6 | movement gate | hard | **PASS** | 0.194 ≤ 0.163+0.05; cov 0.80 (at-par, no edge) |
| P7 | departure deficit (hardened) | hard | **PASS** | K/2 ratio 2.74; composition ≥90% crossings |
| P8 | head law | non-gating | **not fired** | S(1) sim 0.054 < emp 0.072 (opp. sign) |
| P9a | VR13 excess | hard | **PASS** | excess +0.0469 |
| P9b | functional share | hard | **FAIL** | F=0.260 outside [0.5,0.75] |
| P10 | Spec-B for IG | hard | **NOT ADJUDICATED** | build fail-closed; wrong unit |
| P11 | instrument dropout | hard | **NOT ADJUDICATED** | needs the IG daily panel |

Headline: the hardened §2z-ac established-departure deficit (P7) and the b≈1
amplitude law (P5) TRANSPORT to submissions; the monotone s-trend (P4a) and
functional VR share (P9b) do not. IG deferred to a corrected endpoint-level
build (owner decision). See PHASE1_IG_BUILD_FINDING.md,
PHASE0_EXECUTION_CORRECTION.md.
