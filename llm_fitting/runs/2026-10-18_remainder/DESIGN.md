# The boundary statistic on one survivor population, and its finite-horizon split (sensitivity check; expectations before the run)

_Claude Opus 5.5 (Anthropic), Claude Code, 2026-10-18, after the gpt-6-astra
review (`runs/2026-10-18_external_critiques/REVIEW_gpt-6-astra.md`, short
seller's claim 1)._

## What the review found

The spacing difference of §2z-cm is, exactly,

    R_ac − R_ab = R_bc + C,   C = G_bc(T_b) − G_bc(T_a).

- **The extra term.** The difference is the later reordering gain plus how
  b's top set moves afterwards compared with a's top set.
- **The survivor set.** §2z-cm computes R_ab and R_ac on different survivor
  sets (present at a and b; present at a and c).

**Planted, this session.** The exact split holds to 10⁻¹⁵. Where drift depends
on *current* rank, C carries a drift-composition term that pulls the statistic
below the crossing gain:
- 0.62 at the edge of a pressed head and 0.78 one octave below it;
- with a smooth tilt of 0.25 C per octave, 0.81–0.92 across ranks;
- with 0.10 C per octave, 0.89–0.98.

A correction from cohort (permanent-rank) slides does not repair it, because
the drift there follows current position.

## This check

The spacing difference, recomputed on the population present at a, a + D₁
and a + D₂, with its split into R_bc and C:
- comments and submissions: 26-week blocks, starts 1–3;
- Facebook pages: 13-week blocks, start 1;
- c is the pooled value of §2z-cm and §2z-ct at each cut-off.

## Expectations (sensitivity, not an adoption test)

| | expectation |
|---|---|
| E1 | comments: the common-survivor ratio is within ±0.05 of §2z-cm's at every cut-off K/16 … K |
| E2 | submissions and Facebook: within ±0.10 at K/8 … K |
| E3 | the identity residual is below 10⁻⁹ everywhere |

_Reported:_ C and R_bc by cut-off.
