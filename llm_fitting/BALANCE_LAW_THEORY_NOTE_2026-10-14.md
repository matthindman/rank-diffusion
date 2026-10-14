# The balance law of an attention ladder: derivation, relation to prior work, and what is new

_DRAFT for the paper's theory and methods sections. Claude Opus 5.5
(Anthropic), Claude Code, 2026-10-14. Changes no claim language: the binding
claim set is §6's addendum of 2026-07-12 and is the owner's. Results are
quoted from MODEL_STATUS by section._

## 1. Setting

- **Names and the universe.** A population of named endpoints (communities,
  pages, channels) with log levels x_i(t) of an attention metric. The universe
  is the top K by level, with names entering and leaving it.
- **The ladder.** x_(1) ≥ x_(2) ≥ … are the order statistics; U_k(t) is the
  mean of the top k, and the ladder's shape is shape_k = U_k − U_K.
- **Its slope.** S_k is minus the slope of level on log rank at rank k: the
  ladder is steep where S is large. The level gap between neighbouring ranks
  is about S_k/k, and the density of names per unit level at the rank-k level
  is n(x_k) = k/S_k.

## 2. The law

**Exact accounting (discrete).** Let P be the names present at both ends of
an interval and T_a the top k of P at the start. Then

    U_k(b) − U_k(a) = G_k + R_k

- G_k, the mean change of T_a's members.
- R_k ≥ 0, the reordering gain: the top k at b minus T_a's members at b.

The full universe's shape change equals P's plus an entry–exit term computed
from the two ladders with and without entrants and leavers (§2z-cc, §2z-cd).
No assumption is involved.

**The rate law (continuous).** Let names move by Itô processes with local
drift μ and local variance σ². Then the density of levels obeys the forward
equation ∂n/∂t = −∂(μn)/∂x + ½∂²(σ²n)/∂x². Write Σ_k for the sum of the
top k, with x_k defined by ∫_{x_k}^∞ n = k. Integrating by parts:

    dΣ_k/dt = Σ_{top k} μ_i + ½ σ²(x_k) n(x_k)

So per member:

    dU_k/dt = (mean drift of the top k) + c_k,   c_k = σ_k² / (2 S_k)

For the shape, with s̄_k the mean slide of the names above k against U_K:

    d shape_k / dt = c_k − s̄_k      (+ entry term)

In words: the top k gains from crossings at its boundary, at a rate set by
how much names there move (σ²) and how crowded the ladder is there (1/S),
and loses what its members lose against the pack. The law needs only that
the density is smooth on the scale of a step at the boundary; it does not
need stationarity.

**Stationary form.** With d shape_k/dt = 0, the slope the movement sustains is

    S_k* = σ_k² / (2 (s̄_k − entry term))

With a single slide g this is S = σ²/(2g): Zipf's law (S = 1) holds exactly
when the variance is twice the slide.

## 3. Relation to stochastic portfolio theory and random growth

- **Fernholz's gap identity** (R. Fernholz 2017, J. Applied Econometrics;
  arXiv 1602.00159) is E[gap_k] = σ_gap² / (2κ_k), with κ_k = −2(α_1 + … + α_k)
  the local-time rate and α_j the relative growth of rank j's holder.
  - Map it with gap_k ≈ S_k/k, σ_gap² ≈ 2σ² and Σ_{j≤k} α_j = −k s̄_k.
  - Then κ_k = 2k s̄_k and S_k = σ²/(2 s̄_k): **our stationary form is
    Fernholz's identity in continuum notation.**
  - The Atlas-model slope σ²/(2g) (Fernholz & Fernholz 2020) is its
    one-slide case.
- **Changes in concentration.** Fernholz & Koch (Dallas Fed WP 1604; AER
  P&P 2017; EER 2021) compare two stationary regimes of US bank concentration
  around a dated policy change and attribute the change to volatility against
  reversion by rank. That is comparative statics, with no dynamic law; local
  time appears only inside the reversion estimator. Throughout SPT the
  local-time term is *measured* from rank switching, and the rank-based
  coefficients are those of *whoever holds* a rank.
- **The forward equation for ordered levels** is established theory: porous-
  medium limits (Shkolnikov 2012; Jourdain & Reygner 2013, 2015), out-of-
  equilibrium profiles for rank-dependent drifts (Cabezas, Dembo, Sarantsev &
  Sidoravicius, CPAM 2019), and local times of ranked semimartingales (Banner
  & Ghomrasni 2008). The rate law of §2 is derivable from it.
- **Entry and exit, closest competitor.** Baker & Smyth ("Traveling waves in
  equity markets with rank-based entry and exit", arXiv 2608.27156, August
  2026) write the CDF's evolution with rank-dependent entry and exit. Their
  flux ½σ²g is our boundary term, and their slope without turnover is
  σ²/(2(B(s) − sB(1))). They infer drift from the wave identity rather than
  measure it, and compare predicted relaxation times with the persistence of
  shape deviations on CRSP.
- **Rank-density estimators.** Itkin & Larsson (arXiv 2403.04674, Remark 3.5)
  consider and reject an occupation-density estimator of local time as data
  inefficient. Our σ²/(2S) is a prediction tested against measured crossings,
  not an estimator of them.
- **Random growth** (Gabaix 1999, 2009; Gabaix, Lasry, Lions & Moll 2016)
  gives the stationary tail and its transition speed, with entry and death.
- **Estimation.** SPT calibrations (Fernholz 2017; Fernholz & Koch; Itkin
  & Larsson) all condition on current rank. Itkin & Larsson's appendix B.1
  is a discretisation bias in a *volatility* estimator, called open there.
  No calibration found treats the drift analogue: regression of whoever is on
  top, and the constants passing swings and block averaging leave.
- **Online attention.** Blumm et al. (PRL 2012) link noise to rank stability
  with item-level Langevin dynamics. Iñiguez et al. (Nat. Commun. 2022)
  model rank flux in rank space only, and note that linking score and rank is
  an open extension. Neutral-copying models predict turnover across a chart
  boundary by another mechanism (Bentley et al. 2007; Evans & Giometto 2011).
  None links individual movement to the slope of the rank-size curve
  quantitatively. No rank-based or transition-speed analysis of a platform
  algorithm change was found.

The mathematics of §2 is therefore not new. What can be new is what
follows. The novelty search behind this section is in
`runs/2026-10-14_boundary_law/NOVELTY_SEARCH.md`: broad, not exhaustive, with
reading level tagged per item.

## 4. What this program adds

1. **The boundary term is predicted, not measured.** c_k = σ_k²/(2S_k) uses
   the lasting variance of named movement and the ladder's local slope, both
   measured independently of the crossings. Comparing it with the measured
   reordering gain rank by rank is a falsifiable test of the law's only
   physics (§2z-cm).
   - *Comments:* the measured lasting crossing gain over c_k is 0.98–1.22 at
     every rank from K/64 to K. In the body the 5–95% intervals are about ±10%.
   - *Submissions:* the crossings are smooth across ranks, but the independent
     variance estimates in the head strata (31–145 communities) are too erratic
     to test them.
2. **Lagrangian estimation.** Drifts and variances are measured on cohorts
   fixed by permanent rank with absence-penalised membership, not on whoever
   holds a rank, which selects on noise (§2z-a onwards).
3. **Lasting against passing movement.**
   - Block means and the variance route remove passing swings from σ²
     (§2z-cc).
   - Differences between two spacings remove the one-time constants that
     passing swings and block averaging leave in comparisons from one start.
     The two have opposite signs: whoever is on top through a swing falls
     back, and whoever is on top by a block average carries momentum (§2z-bw,
     §2z-by, §2z-cm).
   - No SPT calibration found treats this drift bias; Itkin & Larsson's
     open question is the analogous one for volatility.
4. **An open universe.** Entry and exit are exact terms for a finite top-K
   universe. Rank-based entry and exit in continuum form are Baker & Smyth's,
   so this is accounting, not a new idea. On Reddit, entry is
   29–41% of the incumbents' slide on comments and 8–19% on submissions
   (§2z-cc). On comments entry is what holds the ladder flat (§2z-cd, §2z-ck).
5. **Out of equilibrium, through dated platform changes.**
   - The law accounts for the ladder's change by rank through Reddit's June
     2021 Home-feed change and three reversals of the comments head (§2z-cd,
     §2z-cg).
   - Forecasts of structure work where drift by rank persists (submissions)
     and fail where it cycles (comments) (§2z-ce, §2z-cg, §2z-ci).
6. **Consequences that can be tested.**
   - *Detection.* A change in drift by rank shows within weeks in large strata
     and within months at the head (§2z-ci).
   - *Long run.* Falling entry steepens a ladder unless drift by rank works
     against the head (§2z-ck).
   - *Climbing.* A typical newcomer's climb to the head takes decades at
     measured rates. In a stationary ladder the chance of ever climbing from
     rank m to rank k is k/m, and the climb lengthens as 1/slide² (§2z-ck).

## 5a. Positioning, in one paragraph

The paper cannot claim the law's mathematics. It can claim:
- the first test of a rank-based balance out of equilibrium, with the crossing
  term predicted from measured lasting volatility and crowding and checked
  rank by rank and year by year against measured crossings;
- named-cohort estimation that removes the drift bias of ranking by current
  position, which rank-based calibrations leave untreated;
- the first application of this framework to online attention, through a
  dated platform algorithm change.

It must cite and position against Fernholz (2017), Fernholz & Koch, Itkin &
Larsson (2024), Baker & Smyth (2026) and Gabaix, Lasry, Lions & Moll (2016),
and for the domain Blumm et al. (2012) and Iñiguez et al. (2022). It must
disclose that n(x_k) = k/S_k is a continuum density standing in for the
gap density at zero, exact only for exponential gaps.

## 5. What the paper still needs

- **The boundary theory by rank** (§2z-cm).
- **A second system, and an outcome-blind test.** Both are owner-gated: the
  sealed Reddit bridge 2023–24, with forecasts fixed in the draft protocol
  (§2z-cf), and the sealed Wikipedia panels.
- **Codex's review** (R5) and the planted worlds with moving drift (V5).
- **New claim language** for the rate law, which is the owner's.

## 7. A proposed argument for the paper (for the owner; no claim language changed)

1. **The puzzle.** Attention ladders keep their shape while the names on them
   move a great deal: entrants, exits and reshuffling every year, against a
   rank-size curve that barely changes.
2. **The answer is a balance at every rank.** A top set holds its share when
   crossings at its boundary return what its members lose against the pack,
   plus what entrants take. The crossings are predicted from two measurable
   things, how much names at that rank move (lasting variance) and how crowded
   the ladder is there (slope). That prediction holds rank by rank on comments
   with two independent variance estimators, and over time; on submissions it
   holds roughly in the body and cannot be measured at the head (§2z-cm,
   §2z-co).
3. **Out of equilibrium, the shape moves at the rate of the imbalance.**
   - A platform change acts on the ladder through the drift of names by rank,
     and through their mobility. Reddit's June 2021 Home feed lowered the drift
     of the very top within days, and the crossings at the head of submissions
     roughly tripled over the following year (§2z-cm, §2z-co).
   - The ladder then changes at the rate the law gives, and keeps changing
     while the imbalance lasts (§2z-cd, §2z-cg, §2z-ca).
4. **Forecasting structure is forecasting drift by rank, and on this record
   that is no better than trend.**
   - The law forecasts where drift by rank persists (submissions), but no
     better than repeating the ladder's own last change.
   - Comments' head moved once, sharply, in mid-2021, and no forecast saw it
     (§2z-ce, §2z-cg, §2z-co).
   - A change in drift by rank shows within weeks across most of the ladder
     and within months at the head (§2z-ci).
   - So the paper should claim explanation and counterfactual (what slope the
     measured movement sustains, how fast the ladder must move), not superior
     forecasts.
5. **Consequences.**
   - Falling entry, as platforms mature, steepens ladders unless drift by rank
     works against the head.
   - A newcomer's climb to the head takes decades at measured rates (§2z-ck).
6. **Method.** Named cohorts fixed by permanent rank, block means and
   differences of spacings remove the biases that ranking by current position,
   passing swings and sampling interval put into rank-based estimates (§2z-cm).

Evidence so far: two Reddit metrics, 2019–2022, DEVELOPMENT. Figures in
`runs/2026-10-15_figures/`. Missing:
- an outcome-blind test and a second system (sealed data, owner-gated; a
  revised primary test centred on the boundary theory is proposed in the 2023
  draft protocol);
- Codex's review.
