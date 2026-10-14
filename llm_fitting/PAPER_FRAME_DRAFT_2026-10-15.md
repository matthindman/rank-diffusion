# Paper frame: title, significance, abstract, figure captions (DRAFT, for the owner)

_Claude Opus 5.5 (Anthropic), 2026-10-15. NOT ADOPTED. The binding claim set
is the owner's (MODEL_STATUS §6 addendum, 2026-07-12) and is not changed
here. Every statement cites the record section it rests on. Companion
documents: `REVIEWER_QUESTIONS_2026-10-15.md` and
`BALANCE_LAW_THEORY_NOTE_2026-10-14.md`._

## Title options

1. How individual mobility holds the shape of online attention
2. A balance at every rank: crossings, drift and the stability of attention inequality
3. Stable inequality from moving parts: a rank-by-rank law of online attention

## Significance (PNAS limit 120 words)

Online attention is highly unequal, and the shape of that inequality is
stable even though the communities holding attention change places
constantly. We show that a balance at every rank reconciles the two.
A top set keeps its share when the crossings at its boundary, predicted from
how much communities there move and how crowded the ranks are, return what
its members lose against the field. The prediction holds rank by rank on
Reddit and Facebook. The ladder changes only when communities' drift by rank
changes, as after a platform's feed change. This makes the stability of
attention inequality measurable, and shows how platform design reshapes it
and that the full effect takes years.

## Abstract (draft; PNAS limit 250 words)

The distribution of attention across online communities is extremely unequal
and remarkably stable in form. Yet a fifth of every top set is replaced each
year, and a typical community's rank moves by a fifth to a quarter (Reddit,
2019–2022). We show how individual mobility and aggregate stability fit
together.

For any top set, the change in its share is exactly its members' mean
movement against the field plus a gain from communities crossing its
boundary. Rank-based diffusion theory predicts that gain from two measurable
quantities: the lasting volatility of communities at that rank, and the
local slope of the rank-size curve.

We measure both on cohorts fixed in advance, removing the
regression-to-the-mean and averaging biases of ranking by current position. Measured crossings match the prediction at every rank from the
top 200 to the top 12,500 on Reddit comments, over time and with two
estimators, and on Facebook pages. The crossing gain is nearly constant from
one half-year to the next. Changes in the curve's shape therefore come from
changes in drift by rank. After Reddit's 2021 Home-feed change, the drift of
the very top fell within days and its mobility tripled.

The same balance gives the slope current movement sustains, and how long a
ladder takes to reach it: years on Reddit submissions, a decade on comments.
It shows that falling entry, as platforms mature, steepens inequality. The
law explains and constrains structure; it does not forecast it better than
the curve's own recent trend.

_Sources:_ §2z-cu (turnover, mobility), §2z-cm, §2z-co, §2z-ct (the boundary
theory), §2z-cr (steady crossings), §2z-ca, §2z-cm (June 2021), §2z-cp
(relaxation), §2z-ck (entry), §2z-co, §2z-cs (forecasting).

_Before submission:_ the abstract claims nothing out of sample. An
outcome-blind test on the sealed 2023–24 Reddit data (§2z-cp) or Wikipedia
would let it.

## Figure captions (draft)

**Figure 1. Moving names, a slowly changing ladder** (`runs/2026-10-15_figures/fig_puzzle.png`).
- *(A)* The rank-size curve of activity (log level above rank K against rank
  over K): one curve per year for Reddit comments (K = 12,500) and
  submissions (K = 5,000), per half-year for Facebook pages (K = 3,500).
- *(B)* Weekly ranks of 120 communities drawn at random from the 2019 top
  12,500 of Reddit comments.
- *(C)* The share of each top set still in it one to three years later.

**Figure 2. The balance at every rank** (`…/2026-10-15_comments_2021_head/fig_shape_decomposition.png`).
Each half-year's change in the shape of the top 781 (Reddit comments) and top
312 (submissions), split exactly into members' movement against the field
(blue) and the gain from crossings at the boundary (orange). The crossing
gain is nearly constant. The shape moves when members' movement does,
notably the broad gain of the comments head in April–September 2021.

**Figure 3. Crossings follow lasting volatility over twice the slope, on three ladders** (`…/fig_three_ladders.png`).
- *(A)* Measured lasting crossing gain against the prediction σ²/(2S), from
  independently measured volatility and slope, at ranks K/64 … K. Grey: the
  registered band [0.75, 1.33]. Open symbols: ranks where fewer than 150
  communities sit at the boundary.
- *(B)* The ratio by rank, with 5–95% bootstrap intervals.

**Figure 4. Structure from movement** (`…/fig_sustained_ladder.png`, `…/fig_why_it_curves.png`).
The ladder each year's measured movement would sustain, S*_k = σ_k²/(2 ×
net slide_k) integrated over log rank, against the ladder observed.
- On comments the observed ladder lies between the two years' sustained
  ladders.
- On submissions both years sustain a much flatter head, and the head is
  sinking toward it.
- Inset or SI: the ladder steepens down the ranks because lasting volatility
  rises and the net slide falls toward the bottom.

**Figure 5. A platform change** (`…/fig_rate_law_and_head_mobility.png`).
- *(A)* The rate law in its same-year form over 60 rank-by-half-year cells.
- *(B)* Crossing gain at the head of Reddit submissions over three yearly
  windows around the June 2021 Home-feed change, with intervals.

## What the paper claims and does not claim (to keep the frame honest)

**Claims (DEVELOPMENT data, in sample):**
- the exact decomposition;
- the boundary theory rank by rank on three ladders;
- steady crossings;
- the sustained slope and relaxation times;
- the debiased cohort method.

**Does not claim:**
- the law's mathematics, which is stochastic portfolio theory's;
- causes of drift by rank (the owner's framing, §2z-cj);
- better forecasts than trend;
- anything out of sample until a sealed test is run;
- the head of Reddit submissions, where the variance cannot be measured;
- an explanation of submissions' shortfall of crossings at ranks of about
  300–600 (§2z-cu, §2z-cv).
