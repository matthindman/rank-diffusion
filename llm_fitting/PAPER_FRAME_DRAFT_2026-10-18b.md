# Paper frame "b": size does not protect the leaders of online attention (DRAFT, for the owner)

_Claude Opus 5.5 (Anthropic), 2026-10-18. The output of the altitude prompt
(`runs/2026-10-18_altitude/PROMPT.md`, `MEMO.md`; record §2z-dc). It sits
beside `PAPER_FRAME_DRAFT_2026-10-18.md` and does not replace it; the owner
chooses. NOT ADOPTED: claim language is the owner's. Every number cites its
record section._

## Title options

1. **Size does not protect the leaders of online attention** (recommended)
2. Contestable at every scale: why the leaders of online attention keep changing
3. Fast names, slow ladders: how measured mobility sets the inequality of online attention (the 10-18 frame)

## Significance (draft; PNAS limit 120 words)

Large firms are steadier than small ones, and that steadiness makes market
leaders hard to dislodge. We find that the leaders of online attention have
almost no such protection. On Reddit and Facebook, the year-to-year
fluctuations of a community's activity barely shrink with its size. About a
fifth of every Reddit top set, from the top hundred to the top ten thousand,
is replaced each year. The rate at which leaders are displaced at each rank
follows from a balance known from the dynamics of top wealth: how much
communities there move, over how crowded the ranks are. Concentration
therefore moves with how growth differs across ranks, which shifted on Reddit
around a dated change to its feed.

## Abstract (draft; PNAS limit 250 words)

Online attention is concentrated, yet its leaders change. We ask
whether size protects them, as it protects large firms, whose growth
fluctuates far less than small firms'. We use weekly records of
Reddit communities (2019–2022) and Facebook pages. The
spread of a community's one-year change in activity barely falls with size:
it scales as size to the power −0.01 to −0.08, against −0.15 to −0.2 for
firms. A community's activity moves as a unit, not as a portfolio of
independent posts: on Reddit neither the volume of posts and comments nor
the engagement each earns steadies with size. Leaders are therefore displaced at
similar rates at every scale; 16–21%
of each Reddit top set, from the top 78 to the top 12,500, is replaced each
year. Displacement at each rank follows a balance known from top wealth
shares: communities crossing a rank's boundary return lasting variance over
twice the local slope of the rank-size curve. On cohorts fixed in advance,
the relation holds across ranks on Reddit, Facebook and
censoring-corrected Instagram (pooled ratio 0.96; 0.89–1.05). On Reddit
comments it predicts displacement a year ahead from earlier volatility
(0.94). Models in which communities jump to random ranks fail: movement is
growth in levels. Because mobility is nearly scale-free, concentration moves
with how drift differs by rank. The 78 leading submissions communities'
share of activity fell from 50% to 34% in three years, as attention shifted
to the next tier around a 2021 feed change.

## Figure 1 (`runs/2026-10-18_altitude/fig_contestable.png`)

**Figure 1. Size does not protect the leaders of online attention.**
- *(A)* The standard deviation of a community's one-year change in log
  activity against its size, by permanent-rank stratum, from the top K/64 to
  K, on three ladders. Dotted lines show the slope that firms' size–variance
  scaling (β = 0.2) would give from the same bottom stratum.
- *(B)* The share of each top set still in it a year later, against the top
  set's size as a share of the universe.
- *(C)* Displacement at each rank in 2020–2022 against its prediction from
  2019–2020 volatility (lasting variance over twice the slope), 13-week
  blocks, with no overlap in time. Grey: the margin [0.85, 1.18]. Comments
  pool to 0.94 and submissions to 0.62.
- *(D)* The same test applied to rank-model rivals calibrated to the data's
  turnover: jumps to random ranks fail in the body of the ladder; rank
  diffusion with a fixed ladder passes.

Sources: §2z-dc (A, C, D), §2z-cu (B).

## First paragraph (draft prose)

Attention online is concentrated in a few places. Of the five thousand
most active communities on Reddit, the 78 largest drew half of all their
submission activity in 2019 (§2z-da). Whether such leaders are entrenched is
disputed:
- critics of platforms describe a rich-get-richer system, as in models of
  preferential attachment and cumulative advantage;
- platforms answer that anyone can rise, and point to leaders that
  collapsed.

In markets for goods the question has a classic answer: size protects. The
growth rates of large firms fluctuate far less than those of small ones,
with a standard deviation falling roughly as size to the power −0.15 to
−0.2 (Stanley et al. 1996; Riccaboni et al. 2008). Lower volatility makes a
large firm's position harder to dislodge. Here we ask whether size protects the leaders of online
attention, and what follows for how concentrated attention becomes. We use
four-year weekly records of tens of thousands of Reddit communities and 86
weeks of Facebook pages.

## What carries each claim

| claim | evidence | record | standing |
|---|---|---|---|
| Size barely reduces volatility (β 0.01–0.08 against 0.15–0.2 for firms) | raw and lasting β with intervals on three ladders | §2z-dc Z | post hoc design, committed before the run |
| A community's attention moves as a unit, not as a portfolio | engagement per item: γ 0.04–0.12 against 0.5 for independent items | §2z-dc P | post hoc design, committed before the run; heavy-tailed items argued, not measured |
| Leaders displaced at similar rates at every scale | retention 0.79/0.82/0.84 (comments), 0.79/0.80/0.82 (submissions) | §2z-cu | descriptive |
| Displacement at each rank = lasting variance over twice the slope | four ladders, in period, pooled 0.955 | §2z-cm, §2z-ct, §2z-cx, §2z-da | registered (comments, Facebook, Instagram); partial on submissions |
| …and a year ahead | comments 0.944; submissions 0.617 (fails) | §2z-dc O | post hoc design, committed before the run |
| Movement is growth in levels | calibrated jumps to random ranks 1.56; fixed-ladder rank diffusion 1.05 (not excluded) | §2z-dc V | planted |
| Concentration falls with drift by rank | arithmetic share 50.2% → 34.0%; sustained level 15–16% (index) | §2z-da, §2z-cw | in sample |
| A shift to the next tier around June 2021 | event study against placebo years (X1 FAIL for the head; the next tier and comments distinctive) | §2z-da | registered design; post hoc sensitivity |

## Does not claim

- that the displacement law is new. It is Gomez's (2023) for top wealth
  shares;
- that leaders "lose ground", which is generic in random growth (§2z-dc,
  test A);
- that rank exchange of every kind is ruled out. Rank diffusion with a fixed
  ladder passes the same test (§2z-dc, test V);
- prediction ahead of time on Reddit submissions, where it fails (§2z-dc,
  test O);
- the cause of drift by rank, or the size of the June 2021 effect;
- anything about Instagram levels.
