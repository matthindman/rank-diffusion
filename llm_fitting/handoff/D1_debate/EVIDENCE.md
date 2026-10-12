# Evidence both sides start from

Facts only, each with its place in `llm_fitting/MODEL_STATUS.md`. No
interpretation is intended; if a line reads as one, say so.

## The model as it stands (§1)

- Log activity = lasting level + passing AR(1) swing (+ a medium one on
  long panels) + measurement noise, times a persistent amplitude per
  endpoint. Parameters are step profiles over about 55 bands of permanent
  rank.
- The lasting level reverts to the endpoint's own home at a band-specific
  rate. Homes are seeded from the sorted observed values of the first week.
- Binding language, settled with reviewers: "entity-home OU with
  rank-dependent variances"; the cross-section of homes "is the MEASURED
  stationary ladder ... the ONE taken-as-given input"; structure claims are
  "conditional reproduction + MAINTENANCE of the ladder"; "Ladder GENESIS is
  out of scope".
- The paper outline (`paper/pnas_outline.tex`) positions the contribution
  against the random-growth tradition (which "explains stationary ladders
  but is silent on who occupies the rungs") and the ranking-dynamics
  literature, and plans a Discussion claim that "positions are structural
  and, to first order, occupants are exchangeable up to one persistent
  amplitude ... durable concentration without durable incumbents".

## What Phase 1 measured (all exploratory, development data)

- The reversion-rate profile does not reproduce between halves of the
  endpoints and sits on its slowest grid value in 31–52% of bands (§2z-at).
- Slope of a later year-over-year change in an endpoint's level on an
  earlier one, two four-year Reddit panels: adjacent +0.28 and +0.31, one
  apart +0.01 and +0.07 (a random walk gives +0.25 and 0). The fitted
  model: −0.01 to −0.12 and −0.21 to −0.23. In the head group of long
  submissions the data's values are +0.18 and −0.08 (§2z-aw).
- Variance of h-week changes, with an estimated annual cycle's share
  removed, is close to linear in h from 13 to 78 weeks on three Reddit
  panels; the fitted model falls 0.05–0.12 below the line at 78 weeks
  (§2z-aw addendum). The Reddit panels have an endpoint-specific annual
  cycle the model lacks (§2z-aw).
- Endpoints with train-window permanent rank in the top half relocate to
  the boundary zone 2.5 and 3.8 times as often as simulated; their exits
  from the top-K are under-produced 2.3 and 2.7 times (§2z-au, §2z-aw).
- With reversion switched off in the fitted model (parameters otherwise
  unchanged): the year-over-year slopes become +0.21 and +0.02; exits of
  the established cohort rise from 0.44 to 0.60 and from 0.37 to 0.53 of
  the data's rate; ladder drift (movement of the rank-size curve between
  the first and last third of the panel) becomes 0.907, 1.145 and 0.370 on
  FB, long submissions and comments, against 0.267, 0.501 and 0.258 in the
  data and 0.115, 0.136 and 0.172 as fitted; the simulated top-1 share goes
  to 14.7, 4.0 and 0.99 times the data's (§2z-ay). That arm keeps the
  model's exit-and-rebirth at the bottom and its fitted lasting volatility,
  which was estimated jointly with the reversion.
- Volatility depends weakly on size: scaling exponents −0.03 to −0.12 for
  the lasting and passing parts (§2z-at).
- Long-run and short-run volatility correlate 0.83–0.85 across endpoints
  where one amplitude gives 1.00; an endpoint's volatility is not fixed
  over time (§2z-aw).

## The proposal under question (decision document, candidate 2)

"Replace the pull toward an endpoint's own starting level by a pull toward
the level the ladder assigns to the rank the endpoint holds now. ... One
strength for the pull, per panel, in place of a reversion rate per band."
Identifying moment proposed: ladder drift.

## Not measured

- Whether an endpoint's expected growth depends on its size or position,
  once selection on noise, censoring at the bottom of the recorded list and
  universe selection are controlled.
- Entry: how many endpoints enter the top-K from below or from outside, at
  what sizes, and what happens to them.
- Whether the rank-size curve can be predicted from measured movement
  parameters (drift and variance by rank, crossing rates) without being
  given.
- Anything beyond 78 weeks. Facebook has 86 weeks and cannot speak to a
  year.

## Reading in the repository

`llm_fitting/lit_review/research_notes/Rank dynamics modeling best
practices/rank_dynamics_model_families.md` (rank-based, first-order and
hybrid Atlas models; Fernholz's gap identity; a proposed "closure ratio";
random-growth models), with a verification status at its head. Citations
there carry tags; re-check before relying on one.
