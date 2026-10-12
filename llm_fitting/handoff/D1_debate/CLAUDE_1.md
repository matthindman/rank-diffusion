# Claude — first position

_Claude (Opus 5.5), 2026-10-12. Written before any position from Codex
existed. Citation tags: [repo] = in the repository's literature note, with
the status given there; [search] = confirmed to exist by a web search in
this session, text not re-read; [memory] = known from training, not
re-read._

## Short answers

1. **Yes, the owner is right.** A pull toward "the level the ladder
   assigns to the rank you hold now" takes the ladder as an input and
   builds the dynamics to restore it. The rank-size structure is assumed.
   I withdraw candidate 2 in the form I wrote it.
2. **The model as it stands assumes it too.** Homes are seeded from the
   measured ladder and each endpoint reverts to its home. The binding
   language already says so: maintenance of the ladder, not its genesis.
   So the choice is not between an organic model and an imposed one. Both
   impose it. Mine would have imposed it more nakedly.
3. **The right move for the paper is to stop choosing among attractors
   and make the ladder an output, or at least an overidentified test.**
   What has to be measured first is listed below.
4. **Next: three measurements and one design rule, no model code.**

## Why a pull toward the current rank explains nothing

In the rank-based literature the growth rate "of whoever holds rank k" is
negative at the top of any stationary ranked system. Fernholz is explicit
that these rank-indexed rates are time-averaged functionals of the whole
system, not structural coefficients, and that stability is the condition
that their partial sums are negative, which he calls cross-sectional mean
reversion [repo: Fernholz 2016/2017, arXiv 1602.00159, proofs read there].
The repository's note adds that in a noisy panel part of this is
regression to the mean of whoever is transiently on top. So "endpoints are
pulled toward the ladder at their current rank" is the statement that the
cross-section is stationary, written as if it were a force. It is true of
every stationary system, including one driven entirely by persistent
identities. It cannot be a cause.

There is a second problem. First-order models "can be built to have any
stationary distribution" [repo: Fernholz & Fernholz 2020, arXiv
1707.04285]. Fitting one to reproduce the ladder is therefore no test.

## Why the personal home is not the organic alternative either

The home has a substantive reading, which is to its credit: endpoints
differ durably in appeal, and the hierarchy is the distribution of appeal.
That is the fitness tradition. But the model does not derive the
distribution of homes; it copies it from the data. And Phase 1 removed the
evidence for the mechanism at the horizons we can see: no reversion year
over year, relocation 2.5–3.8 times too rare, a reversion rate that is not
identified. A home whose pull is too slow to see in four years is not a
tested mechanism.

## What "emerging from micro-level behaviour" would mean here

If endpoints' lasting levels are not anchored to themselves, something
else keeps the cross-section from spreading. Independent random walks
spread; A8 shows it (ladder drift 1.4–3.4 times the data's). The
literature offers a small number of mechanism classes for a stable
distribution with churning members, and they differ in what they predict
about individual movement, which is what this project's data can see and
cross-sectional studies cannot:

- **Durable identity** (fitness, the present home). Signature: an
  endpoint's level reverts to its own long-run level. Contradicted at up to
  a year and a half.
- **Frictional random growth** (Gibrat's law with a floor or with entry
  and exit: Champernowne, Simon, Gabaix 1999 [search], Luttmer 2007
  [memory]). Growth does not depend on size or identity; incumbents drift
  down relative to the total; entrants arrive at the bottom. The
  distribution emerges, and its exponent is a function of the drift, the
  variance and the entry rate. Signatures: random-walk levels (seen);
  growth independent of size (not measured); enough entry to balance the
  books (not measured).
- **Size- or position-dependent growth** (cumulative advantage with
  diminishing returns; in the rank-based literature, the Atlas family:
  Banner, Fernholz & Karatzas 2005 [search]; Ichiba et al. 2011 for the
  hybrid of rank and name effects [search]). Signature: expected growth
  falls with size. Not measured. The head of long submissions gives a hint
  (+0.18 and −0.08 where the rest of the panel has +0.31 and +0.01).

In the second and third classes the ladder is derived from measured
movement. In first-order models the mean gap between adjacent ranks is
(σ_k² + σ_{k+1}²) / (−4 Σ_{j≤k} g_j) [repo, from Fernholz & Fernholz]; in
the random-growth family the tail exponent is set by drift over variance.
Either gives a prediction of the rank-size curve from quantities estimated
on changes, which can then be compared with the curve itself. That is the
test the present model cannot offer, and it is the one the owner is asking
for.

## What I recommend

**Design rule.** In whatever replaces the home, the ladder is never the
attractor. It is predicted from measured movement and compared with the
data, or the model is not making a structural claim.

**Three measurements before any model code**, each on data and on both
simulation arms (with the home, without it) through one code path, each
with a planted-recovery check from V2:

- M1. *The growth law.* Mean and variance of year-scale change in the
  lasting level as a function of starting size, with the three known traps
  handled explicitly: selection on noise (position measured on a block
  that shares no weeks with the growth), censoring at the bottom of the
  recorded list (restrict to the top half, where A2 already works), and
  universe selection (train-only membership). This decides between the
  second and third class.
- M2. *The flows.* Entry into the top-K from below and from outside, by
  size and age, and the fate of entrants; Iñiguez et al.'s flux statistic
  for comparability with thirty other ranked systems (Nature
  Communications 2022 [search]). Frictional growth needs entry to balance
  incumbents' decline; A7 found the established cohort losing about a
  tenth in log rank between windows.
- M3. *The overidentification.* The closure ratio proposed in the
  repository's note, mean gap at rank k over gap variance divided by twice
  the crossing rate: near 1 where occupants are exchangeable, well below 1
  where ranks are pinned to names. It is a direct measurement of "how
  rank-based against how name-based" each platform is at each rank.

**One demonstration for the paper, if a derived-ladder model survives.**
Start the simulation from a deliberately wrong ladder and show that it
converges to the observed one. The home model cannot do this by
construction: it remembers its seed forever.

## Is this the right decision for PNAS

Not as I proposed it. The stronger paper is the one that says which
quantities are assumed, which are derived and which are tested, and moves
the ladder from the first list to the second. The outline's own Discussion
claim, "durable concentration without durable incumbents", is better
supported by Phase 1 than by the model that was built to make it, and it
is a claim about mechanism class that identity-resolved panels with a
measurement model can adjudicate and cross-sections cannot. Perspectives in
this field ask for exactly the combination of explanation and tested
prediction (Hofman et al., Nature 2021 [search]).

Two cautions. Matching the rank-size curve is weak evidence by itself,
because many mechanisms produce the same curve; the discriminating
evidence is the movement signatures, so M1 and M2 matter more than any
fit. And none of this identifies *why* growth is or is not independent of
size (ranking algorithms, habit, a finite attention budget). The data can
identify the class of process and its parameters. Causes at that level
need the natural experiments the panels contain, such as the Facebook feed
eras, and that is a separate analysis.

## What would change my mind

- M1 shows reversion to an endpoint's own level at horizons beyond what
  A7 could see, or V2 shows that the year-over-year statistic cannot tell
  a slow home from none at these panel lengths. Then the home stays, with
  a slow rate, and the claim stays conditional.
- The ladder predicted from measured movement misses the observed one
  badly on every panel. Then a derived ladder is not available with these
  data, and the honest paper keeps the ladder as a declared input.
- A derived-ladder model cannot match the present one on the held-out
  movement gate within the margin. Movement prediction is the adoption
  criterion, and it does not get traded for elegance.

## Risks I see in my own recommendation

Stationary-law predictions are noisy on 86 to 212 weeks and a truncated
list, and the top share is a functional that small parameter changes move
a lot [repo: Chatterjee & Pal]. Rank-based theory assumes no observation
noise, and these panels are dominated by it at short horizons; the
project's separation of lasting from passing movement is what makes the
measurement possible, and also where it can go wrong. And this is a larger
change of course than candidate 2 was, late in the development phase.
