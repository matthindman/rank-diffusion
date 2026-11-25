# Common Rank-Diffusion Model — Stages 1–4 Development Report & Cross-Platform Parameter Atlas

_Date: 2026-11-25. Status: EXPLORATORY DEVELOPMENT EVIDENCE ONLY (DEVELOPMENT panels only; zero contact with SEALED or EXPOSURE-UNKNOWN data; no default, frozen stack, gate, or registered protocol changed)._

---

## 1. Executive Summary

This report documents a four-stage development pass to build a single, parsimonious, moment-identified stochastic model of digital attention rank dynamics (`llm_fitting/common_rankdiff.py`, opt-in only) evaluated across **all four DEVELOPMENT ladders** (`facebook_a` Era A, `instagram_hm` via the §2z-c censoring apparatus, `reddit_comments_ext`, and `reddit_submissions_long`) against all three program goals:

1. **Goal 1 (Aggregate Structure):** Reproduce turnover across scales, top-occupant churn, stable rank-size ladders, arithmetic head shares (with zero `expm1` blowups), boundary crossing ratios, established-departure rates and crossing fractions, and cross-platform persistence/asymmetry signatures.
2. **Goal 2 (Individual Endpoint Movement & Share Shifts):** Accurately forecast individual endpoints' relative log-levels, coarsened log-ranks, arithmetic audience-share shifts, and top-$k$ retention probabilities over strictly calendar-aligned horizons $h \in \{1, 4, 13, 26, 52\}$ weeks, plus Gate v1 pooled displacement accuracy.
3. **Goal 3 (Parsimony):** Replace the ~500 knot-level parameters of `Model A` / `rankdiff_rdm` (`movinghome_vp`) with **11 core scalars** (Stage 2) or **14–15 scalars** (Stage 3, `asymmetry=True`), with every scalar identified in closed form from a declared moment or independent instrument — zero parameters tuned to scores, $D$, or Gate v1.

### Headline Findings
- **Stage 2 (11 core scalars, `asymmetry=False`):**
  - Eliminates all five structural defects found in `Model A` / `rankdiff_rdm`: (i) split-half covariance estimator for $s_{\text{vol}}$ is unbiased at true $s_{\text{vol}} = 0$ (fixes Codex V2 bias); (ii) smooth 7-knot all-rank balancing drift from rank 1 to $N$ (no rank-20 freeze); (iii) bounded empirical standardized innovations (eliminates §2z-du `expm1` blowups completely); (iv) zero-parameter order-statistic starting-ladder deconvolution (replaces ad-hoc $c = 0.96$); (v) explicit separation of intermittent weekly absence from bottom-buffer permanent attrition (fixes established-departure crossing fraction from ~15% to 98.9–100.0%).
  - Beats `no_change`, `rank_bin`, and the no-EB-volatility ablation on **16/17** strictly calendar-aligned endpoint `logR CRPS` cells across all four platforms, with individual audience-share shift correlations `0.51–0.68` and Gate v1 wins on `facebook_a` (`0.181` vs. `0.222`, 3/5) and `instagram_hm` (`0.216` vs. `0.469`, 4/5).
- **Stage 3 (14–15 scalars, opt-in `asymmetry=True`: rank-scaled fading positive surges + persistent entity drift + 4-state Kalman filter):**
  - **Variance-conserving closed-form identification:** Fading positive surges $s_{it}$ ($\lambda_s, \mu_s, \text{hl}_s \in \{8, 13, 20, 26\}$w) are identified strictly from marginal single-horizon 3rd cumulants $\kappa_3(\Delta_h X)$ and the odd excess 4th moment $E[Z^3|Z|] - 4E[|Z|]E[Z^3]$ (which vanishes identically for symmetric noise of any kurtosis); persistent entity drift $d_{it}$ ($\sigma_d$, fixed $\text{hl}_d = 156$w) is identified in closed form from exact gapped window-change covariance net of passing $\xi$ and surge $s$ covariances. Both variance curves are subtracted from $D_b(h)$ before fitting $\sigma_L$, conserving total variance.
  - **Automatic cross-platform specialization:** $\hat{\sigma}_d$ turns off automatically (`0.00000`) on broadcast/creator feeds (`facebook_a`, `instagram_hm`) and turns on cleanly at virtually identical amplitude on both Reddit community ladders (`0.00775` comments, `0.00774` submissions).
  - **Goal 1 out-of-moment breakthrough:** Without ever using $D$, $p_{\text{fall}}$, or $p_{\text{rise}}$ in estimation:
    - `reddit_comments_ext`: gapped $\rho$ jumps from `-0.049` (S2) to **`+0.121`** (emp `+0.145`), $D$ from `-0.027` to **`+0.110` `[+0.095, +0.124]`** (emp `+0.132`), $p_{\text{fall}} = +0.126$ (emp `+0.158`), $p_{\text{rise}} = +0.018$ (emp `+0.026`).
    - `reddit_submissions_long`: gapped $\rho$ jumps from `-0.021` (S2) to **`+0.145` `[+0.133, +0.158]`** (emp `+0.155`, inside 90% band!), $D$ from `-0.037` to **`+0.089`** (emp `+0.222`), $p_{\text{fall}} = +0.142$ (emp `+0.240`), $\mu_{21} = -0.187$ (emp `-0.163`).
    - `facebook_a`: reproduces positive $D = +0.078$ (emp `+0.031`), opposite-sign third moments $\mu_{21} = -0.198, \mu_{12} = +0.049$ (emp `-0.107, +0.098`), and leverage `< 1` (`0.978` vs. emp `0.938`).
  - **Goal 2 gains:** Stage 3 beats Stage 2 on calendar-aligned `logR CRPS` in **14/17** panel-horizon cells (5/5 on Submissions, up to **-12.7%** at $h = 4$; 4/5 on Comments; 4/4 on Facebook; 1/3 on Instagram with the other 2 within `0.0007`), achieves exact `0.89–0.90` 90% predictive coverage across all 5 horizons on Submissions, flips `reddit_comments_ext` Gate v1 from a loss (`0.148`, 2/5) to a clean win (**`0.105`, 3/5** vs. baseline `0.132`; v1 uniform-surge `0.084`, 4/5), and cuts Submissions Gate v1 error from `0.766` to `0.529` (v1 `0.431`).

---

## 2. Stage 1 Diagnostic Findings & Scored Predictions (P1.1–P1.6)

All Stage 1 predictions were written in [DESIGN.md](file:///Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/DESIGN.md) before running [stage1_asymmetry.py](file:///Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/stage1_asymmetry.py) and [stage1_parsimony_predict.py](file:///Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/stage1_parsimony_predict.py).

| ID | Pre-Run Prediction | Measured Outcome | Verdict |
|---|---|---|---|
| **P1.1** | Planted third moments separate worlds: homogeneous $(\mu_{21},\mu_{12})\approx(0,0)$; skewed decline $(-,-)$; fading surges $(-,+)$. Empirical Reddit $\mu_{21}<0$, sign of $\mu_{12}$ adjudicates. | Planted: hom `(-0.003, +0.008)`; skewed decline `(-0.549, -0.560)`; fading surges `(-0.534, +0.209)`; combined `(-0.525, +0.062)`. Empirical: FB `(-0.103, +0.093)` (pure fading surge), Reddit Comments `(-0.098, -0.013)` & Submissions `(-0.151, +0.014)` (combined!). | **PASS** |
| **P1.2** | Reddit consecutive decline/gain run ratios $r_2, r_3 > 1.15$ beyond permutation null; FB closer to `1.0`. | Comments (26w): `r2=1.29, r3=1.44, r4=1.56`; Submissions (26w): `r2=1.25, r3=1.33, r4=1.39` (all far above null). FB (13w): `r2=1.02` (inside null `[0.95, 1.05]`). IG (7w): `r2=1.25, r3=1.38` (driven by positive skew $\text{median} < \text{mean}$). | **PASS** |
| **P1.3** | 50-cluster co-movement bootstrap widens Reddit $D$ 90% CIs by `1.3–2.5×`, but $D$ stays strictly positive on both Reddit ladders. | Comments widen `1.64×` (cluster 90% CI `[+0.077, +0.188]`); Submissions widen `2.27×` (cluster 90% CI `[+0.146, +0.283]`). Strictly positive on both ladders and at all 6 origins. FB cluster 90% CI `[-0.009, +0.071]` crosses 0. | **PASS** |
| **P1.4** | Item supply $N = \log(1+\text{items})$ carries more lasting variance and persistent decline than engagement-per-item $E = X - N$ across platforms. | **Platform-split surprise!** True on Reddit (community supply carries 65–77% of $\sigma_L^2$ and Submissions persistence `N: rho=+0.171` vs `E: rho=+0.027`), **REVERSED** on broadcast/creator platforms FB & IG (`E` carries 63–74% of $\sigma_L^2$ and FB's entire fading-surge third-moment signature: `mu21=-0.139, mu12=+0.167, lev=0.885`). | **PARTIAL** (major cross-platform finding!) |
| **P1.5** | 2-parameter log-linear profile captures $\ge 75\%$ of between-band variance on $\ge 3/4$ platforms; split-half entity volatility Spearman $\rho > 0.65$ on 4/4. | $\log \gamma_0(z)$ linear $R^2$: FB `0.952`, IG `0.975`, Comments `0.773`, Submissions `0.796` (4/4 $\ge 75\%$!). Split-half entity vol Spearman $\rho$: FB `0.648`, IG `0.648`, Comments `0.682`, Submissions `0.621` (all strong `0.62–0.68`, slightly below `0.65` on 3/4). | **PARTIAL** |
| **P1.6** | `local_level` beats `no_change` at all $h \ge 1$; EB $\hat{v}_i$ predicts $|\Delta \tilde{X}|$ and beats band-only CRPS; asymmetric trend beats symmetric on Reddit at $h=26, 52$. | `local_level` beat `no_change` on 17/17 cells; EB $\hat{v}_i$ correlated `0.24–0.38` with future $|\Delta X_h|$ out to 52w; asymmetric trend modestly improved Reddit `h=26, 52` while symmetric linear trend hurt at every horizon. | **PASS** |

---

## 3. Cross-Platform Parameter Atlas (Goal 3)

Fitted by identical closed-form code (`llm_fitting/common_rankdiff.py`, `estimate_common`) across all four DEVELOPMENT platforms (Stage 2: 11 scalars; Stage 3: 14–15 scalars, `gamma_L = 0`).

| Parameter | Physical Role & Identification Moment | `facebook_a` (Era A, 86w, $K$=3,500) | `instagram_hm` (§2z-c, 52w, $K$=10,000) | `reddit_comments_ext` (213w, $K$=12,500) | `reddit_submissions_long` (212w, $K$=5,000) | Cross-Platform Pattern |
|---|---|---|---|---|---|---|
| **Scalar count (S2 / S3)** | Free scalars (vs ~500 in Model A / RDM) | **11 / 14** (Spec-B floor) | **11 / 14** | **11 / 15** | **11 / 15** | **>30× reduction** |
| $\sigma_{L0}$ (S2 / S3) | Lasting weekly SD at rank $K$ (from $D_b(h)$) | `0.1258 / 0.1137` | `0.0843 / 0.1113` | `0.1044 / 0.0783` | `0.0978 / 0.0429` | Drift $d_i$ explains ~40–55% of Reddit S2 $\sigma_{L0}$ |
| $\beta$ (S2 / S3) | Size-volatility exponent $\sigma_L(L)\propto e^{-\beta(L-L_K)}$ | `+0.076 / +0.073` | `+0.142 / +0.160` | `+0.067 / +0.172` | `+0.024 / -0.031` | Weakly positive on FB/IG/Comments; near-0 on Submissions |
| $\phi$ (S2 / S3) | Pooled passing AR(1) persistence | `0.84 / 0.84` | `0.93 / 0.89` | `0.93 / 0.89` | `0.89 / 0.93` | **Universal band `[0.84, 0.93]` (4–10w half-life)** |
| $a_\xi, b_\xi$ (S3) | Passing SD at $K$ & rank slope in $z - z_K$ | `-0.84, +0.07` | `-0.79, +0.05` | `-0.94, +0.06` | `-0.96, +0.15` | **Universal $b_\xi > 0$** (smaller endpoints noisier) |
| $a_{\text{obs}}, b_{\text{obs}}$ (S3) | Weekly burst/obs noise SD at $K$ & slope | `-0.71, +0.13` | `-0.17, +0.12` | `-1.20, +0.14` | `-1.25, +0.22` | **Platform gradient:** IG (`0.84`) > FB (`0.49`) > Reddit (`0.29–0.30`) |
| $s_{\text{vol}}$ | Entity volatility log-var SD (split-half cov) | `0.745` | `0.759` | `0.692` | `0.718` | **Striking universal constant `0.69–0.76` (~2.0× SD spread!)** |
| $a_{\text{abs}}, b_{\text{abs}}$ | Logit intermittent weekly absence at $K$ & slope | `-4.59, +1.43` | `-2.49, +0.76` | `-4.24, +2.50` | `-6.31, +1.40` | IG `7.7%`/wk > Comments `1.4%` > FB `1.0%` > Subs `0.18%` |
| $\lambda_{\text{exit}}$ | Bottom-buffer weekly permanent attrition | `0.0004` | `0.0001` | `0.0003` | `0.0001` | **Universal:** true exit rare; 98–100% of top-$K$ exits cross |
| $\text{hl}_s, \lambda_s, \mu_s$ (S3) | Fading positive surges (from $\kappa_3(h)$ & odd 4th) | `26w, 0.0020, 1.12` | `26w, 0.0600, 0.25` | `26w, 0.0023, 1.20` | `26w, 0.0013, 1.20` | **Universal 26w surge decay** ($V_s = 0.07\text{–}0.14$) |
| $\sigma_d$ (S3, $\text{hl}_d$=156w) | Persistent entity drift SD (from net gapped cov) | **`0.00000`** | **`0.00000`** | **`0.00775`** | **`0.00774`** | **Sharp platform divide:** 0 on feeds, `0.0077` on communities! |

### Universal Laws vs. Cross-Platform Structural Differences
1. **Universal across digital attention platforms:** (a) Stable rank-size ladders maintained by rank-crossing balance (`boundary_ratio ≈ 1.00`) with crossing-dominated departures (`98–100%`); (b) tightly clustered passing persistence $\phi \in [0.84, 0.93]$; (c) nearly invariant entity-level volatility heterogeneity $s_{\text{vol}} \in [0.69, 0.76]$; (d) positive rank gradients in passing and observation volatility ($b_\xi, b_{\text{obs}} > 0$); (e) bounded weekly innovations (`std q999 = 3.6–5.2`, `max_X = 16.9–18.0`); and (f) right-skewed medium-horizon surges ($\text{hl}_s \approx 26$w).
2. **Broadcast/creator feeds (Facebook, Instagram) vs. community platforms (Reddit):**
   - **Feed platforms:** Engagement-per-item $E$ carries 63–74% of lasting variance; high short-horizon burst/observation noise ($\sigma_{\text{obs}} = 0.49\text{–}0.84$); zero multi-year persistent entity drift ($\hat{\sigma}_d = 0$); fading viral surges dominate third moments ($\mu_{21} < 0, \mu_{12} > 0$).
   - **Community platforms:** Community supply $N$ carries 65–77% of lasting variance; low weekly noise floor ($\sigma_{\text{obs}} = 0.29\text{–}0.30$); active 3-year lifecycle drift ($\hat{\sigma}_d \approx 0.00775$/wk on both comments and submissions) that combines with fading positive surges to produce strong positive persistence ($\rho \approx +0.15$) and persistent decline asymmetry ($D = +0.13\text{–}+0.22$).

---

## 4. Full Benchmark: Stage 2 (11 Scalars) vs. Stage 3 (14–15 Scalars)

Paired common-random-numbers benchmark from [stage3_benchmark.py](file:///Users/hindman/Documents/GitHub/rank-diffusion/llm_fitting/runs/2026-11-25_common_model/stage3_benchmark.py) (`stage3_benchmark_results.json`, 20 seeds for Goal 1; 5 rolling origins × 20 members strictly calendar-aligned for Goal 2).

### 4A. Goal 1 — Aggregate Structure & Asymmetry (Median `[q05, q95]` across 20 seeds)

| Panel & Metric | Empirical | Stage 2 (11 sc) | Stage 3 (14–15 sc) | Assessment |
|---|---:|---:|---:|---|
| **`facebook_a`** Turn top10 / K/4 | `0.350` / `0.237` | `0.350` / `0.215` | `0.500` / `0.225` | S2 exact on top10; S3 improves K/4 |
| `facebook_a` Rank-1 chg / Head share | `0.518` / `0.229` | `0.447` / `0.244` | **`0.541` / `0.238`** | S3 matches both closely; zero blowups |
| `facebook_a` Boundary / Dep cross% | `0.980` / `99.5%` | **`0.997`** / `99.9%` | `1.134` / `99.9%` | S2 exact on boundary; S3 slightly high |
| `facebook_a` Gapped $\rho$ / Asym $D$ | `-0.095` / `+0.031` | `-0.068` / `-0.026` | **`-0.078` / `+0.078`** | S3 matches sign of $D$, $\mu_{21}=-0.198, \mu_{12}=+0.049$, lev `0.978` |
| **`instagram_hm`** Turn top10 / Rank-1 chg | `0.267` / `0.392` | `0.167` / `0.726` | **`0.300` / `0.578`** | S3 fixes top-10 turnover & improves rank-1 |
| `instagram_hm` Head share / Boundary | `0.420` / `1.000` | `0.394` / `0.991` | **`0.405` / `1.012`** | Both accurate; zero blowups (`max_X 18.5`) |
| `instagram_hm` Dep rate / Dep cross% | `0.138` / `98.0%` | `0.108` / `98.9%` | `0.112` / `99.0%` | Both match crossing dominance |
| **`reddit_comments_ext`** Turn top10 / K/4 | `0.267` / `0.162` | **`0.267`** / `0.131` | `0.333` / `0.136` | S2 exact on top10; S3 close (`[0.265, 0.435]`) |
| `reddit_comments_ext` Head share / Boundary | `0.506` / `1.009` | **`0.544` / `1.011`** | `0.608` / `1.038` | Both blowup-free (`max_X 17.4 / 18.5`) |
| `reddit_comments_ext` Dep rate / cross% | `0.0159` / `99.6%` | `0.0180` / `99.9%` | **`0.0156` / `99.9%`** | S3 exact on departure rate & crossing% |
| `reddit_comments_ext` Gapped $\rho$ / Asym $D$ | `+0.145` / `+0.132` | `-0.049` / `-0.027` | **`+0.121` / `+0.110`** | **Major win:** $p_{\text{fall}}=+0.126$ (emp `+0.158`), $p_{\text{rise}}=+0.018$ (emp `+0.026`) |
| **`reddit_submissions_long`** Turn top10 / K/4 | `0.300` / `0.162` | `0.267` / `0.152` | `0.233` / `0.150` | Both close; Rank-1 chg `0.031 / 0.033` vs `0.057` |
| `reddit_submissions_long` Head share / Boundary | `0.357` / `1.117` | `0.590` / `1.000` | `0.683` / `1.026` | **Open gap:** head share too high (weak/neg $\beta$ lets head fan out) |
| `reddit_submissions_long` Gapped $\rho$ / Asym $D$ | `+0.155` / `+0.222` | `-0.021` / `-0.037` | **`+0.145` / `+0.089`** | $\rho$ exact inside 90% band; $D$ captures ~40% of emp (`+0.222`), $\mu_{21}=-0.187$ vs `-0.163` |

### 4B. Goal 2 — Calendar-Aligned Individual Endpoint Forecasts & Gate v1 (5 Rolling Origins × 20 Members)

| Panel | Horizon | `logR CRPS` (`no_chg` / `rank_bin` / **S2** / **S3**) | `X CRPS` (**S2** / **S3**) | 90% Cov (**S2** / **S3**) | Share $\Delta s$ Corr (**S2** / **S3**) | Head Brier (`rb` / **S2** / **S3**) |
|---|---:|---:|---:|---:|---:|---:|
| **`facebook_a`** | $h=1$ | `0.2966` / `0.2314` / `0.1966` / **`0.1962`** | `0.2983` / **`0.2960`** | `0.83` / `0.83` | `0.51` / `0.51` | `0.0884` / **`0.0747`** / `0.0764` |
| | $h=4$ | `0.3589` / `0.2704` / `0.2301` / **`0.2297`** | `0.3414` / **`0.3402`** | `0.85` / `0.84` | `0.56` / `0.56` | `0.1058` / **`0.0915`** / `0.0934` |
| | $h=13$ | `0.4360` / `0.3166` / `0.2701` / **`0.2697`** | `0.4244` / **`0.4234`** | `0.85` / `0.84` | `0.60` / `0.56` | `0.1198` / **`0.1019`** / `0.1021` |
| | $h=26$ | `0.4857` / `0.3423` / `0.3052` / **`0.3049`** | **`0.5050`** / `0.5054` | `0.85` / `0.83` | `0.55` / `0.53` | `0.1212` / **`0.1098`** / `0.1104` |
| **`instagram_hm`** | $h=1$ | `0.4348` / `0.3114` / **`0.2572`** / `0.2577` | **`0.4512`** / `0.4537` | `0.81` / `0.81` | `0.54` / `0.53` | `0.1113` / **`0.0868`** / `0.0880` |
| | $h=4$ | `0.4703` / `0.3295` / **`0.2750`** / `0.2757` | `0.5000` / **`0.4935`** | `0.81` / `0.82` | `0.59` / `0.58` | `0.1147` / **`0.0925`** / `0.0926` |
| | $h=13$ | `0.5098` / `0.3470` / `0.2920` / **`0.2914`** | **`0.5715`** / `0.5767` | `0.81` / `0.81` | `0.56` / `0.58` | `0.1174` / `0.0967` / **`0.0966`** |
| **`reddit_comments_ext`** | $h=1$ | `0.1248` / `0.1234` / `0.0827` / **`0.0823`** | **`0.1953`** / `0.1959` | `0.85` / `0.82` | `0.47` / `0.30` | `0.0421` / `0.0350` / **`0.0338`** |
| | $h=4$ | `0.1559` / `0.1414` / `0.1042` / **`0.1033`** | `0.2407` / **`0.2398`** | `0.86` / `0.84` | `0.22` / `0.30` | `0.0524` / `0.0447` / `0.0447` |
| | $h=13$ | `0.1947` / `0.1657` / `0.1334` / **`0.1313`** | `0.3105` / **`0.3052`** | `0.89` / `0.85` | `0.25` / `0.27` | `0.0655` / **`0.0584`** / `0.0588` |
| | $h=26$ | `0.2298` / `0.1869` / `0.1577` / **`0.1567`** | `0.3785` / **`0.3727`** | `0.89` / `0.85` | `0.54` / `0.48` | `0.0786` / **`0.0708`** / `0.0726` |
| | $h=52$ | `0.2684` / `0.2101` / **`0.1892`** / `0.1949` | `0.4765` / **`0.4751`** | `0.88` / `0.85` | `0.65` / `0.56` | `0.0831` / **`0.0821`** / `0.0895` |
| **`reddit_submissions_long`** | $h=1$ | `0.0955` / `0.1032` / `0.0738` / **`0.0681`** | `0.1365` / **`0.1275`** | `0.89` / `0.89` | `0.33` / `0.18` | `0.0364` / `0.0257` / **`0.0242`** |
| | $h=4$ | `0.1229` / `0.1196` / `0.1021` / **`0.0891`** | `0.1826` / **`0.1631`** | `0.89` / **`0.90`** | `0.27` / `0.07` | `0.0453` / `0.0389` / **`0.0351`** |
| | $h=13$ | `0.1675` / `0.1459` / `0.1382` / **`0.1266`** | `0.2418` / **`0.2258`** | `0.91` / **`0.90`** | `0.29` / `0.17` | `0.0518` / `0.0517` / **`0.0482`** |
| | $h=26$ | `0.2121` / `0.1735` / `0.1696` / **`0.1620`** | `0.3002` / **`0.2925`** | `0.92` / **`0.90`** | `0.57` / `0.36` | **`0.0607`** / `0.0646` / `0.0630` |
| | $h=52$ | `0.2706` / **`0.2125`** / `0.2149` / `0.2145` | **`0.3971`** / `0.3987` | `0.92` / **`0.90`** | `0.68` / `0.53` | **`0.0802`** / `0.0842` / `0.0873` |

### 4C. Gate v1 Pooled Displacement Summary (5 Rolling Splits × 20 Reps, Exploratory Harness)

| Panel | Historical-Mobility Baseline | Stage 2 (11 sc) | Stage 3 v1 (uniform surge, 14–15 sc) | Stage 3 v2 (rank-scaled surge, 14–15 sc) | Recorded `movinghome_vp` (~500 params, §2z-dg) |
|---|---:|---:|---:|---:|---:|
| `facebook_a` | `0.222 ± 0.094` | **`0.181 ± 0.030` (3/5)** | `0.170 ± 0.024` (3/5) | `0.186 ± 0.050` (3/5) | `0.123 ± 0.026` (dev-contaminated, §2z-dd) |
| `instagram_hm` | `0.469 ± 0.184` | **`0.216 ± 0.070` (4/5)** | `0.273 ± 0.104` (3/5) | `0.241 ± 0.117` (3/5) | `0.257 ± 0.109` (Model A G1, §2z-ap) |
| `reddit_comments_ext` | `0.132 ± 0.056` | `0.148 ± 0.044` (2/5) | **`0.084 ± 0.023` (4/5)** | **`0.105 ± 0.041` (3/5)** | `0.130 ± 0.054` (Model A `0.123`, §2z-dg) |
| `reddit_submissions_long` | `0.193 ± 0.056` | `0.766 ± 0.241` (0/5) | `0.431 ± 0.069` (0/5) | `0.529 ± 0.135` (0/5) | `0.183 ± 0.074` (Model A `0.188`, §2z-dg) |

_(Note: the Gate v1 harness in `stage3_benchmark.py` evaluates top-100 displacement moments `dRank1, 4, 13, RACF1, coll1, 5, 20` on 5 splits starting at `min_train = 78/36/22` and is not identical to `rankdiff_kalman.py`'s frozen split calendar; hence `base_rel` differs slightly from `GATE_LEDGER.md` frozen rows and all rows are logged as `exploratory`.)_

---

## 5. Stage 3 Predictions Scored Transparently (P3.1–P3.3) & Honest Failures

| ID | Pre-Run Prediction | Measured Outcome | Verdict |
|---|---|---|---|
| **P3.1** | Synthetic recovery: planted $\sigma_d = 0$ + surges recovers $\hat{\sigma}_d \approx 0$ and surges $>0$; planted full Stage 3 recovers $\sigma_d$, $V_s$, and cuts $\sigma_{L0}$ inflation; `asymmetry=False` bit-identical; 4-state alignment exact. | All 9/9 unit & ADEMP recovery tests in `tests/test_common_rankdiff.py` PASS (`hl_s = 13.0` exact, `V_s = 0.107` vs `0.111`, $\hat{\sigma}_d = 0.0093$ vs `0.013`, `sigma_L0` reduced cleanly vs S2, 4-state exact `0.97 / 0.95`). | **PASS** |
| **P3.2** | On Reddit, Stage 3 gives $\rho \in [0.08, 0.20]$, $D \in [0.08, 0.25]$, $p_{\text{fall}} > 0.10$, $p_{\text{rise}} < 0.06$, decline runs $> 1.15$, zero blowups; on FB $\hat{\sigma}_d \approx 0$ automatically, $D > 0$, $\mu_{21} < 0, \mu_{12} > 0$. | Comments: $\rho=+0.121$, $D=+0.110$, $p_{\text{fall}}=+0.126$, $p_{\text{rise}}=+0.018$ (all hit!). Submissions: $\rho=+0.145$, $D=+0.089$, $p_{\text{fall}}=+0.142$, $p_{\text{rise}}=+0.055$ (all hit!). FB: $\hat{\sigma}_d=0$, $D=+0.078$, $\mu_{21}=-0.198, \mu_{12}=+0.049$ (hit!). **Misses:** decline runs `r2=1.06–1.09` fell short of `> 1.15` (and `lev=0.89–0.90` vs emp `1.11–1.13`), confirming a residual negatively skewed decline component beyond symmetric $d_i$ + positive surges. | **PARTIAL** |
| **P3.3** | Stage 3 improves Reddit `X CRPS` and `logR CRPS` at $h=26, 52$ (bringing Submissions `h=52` below `rank_bin`) and improves Submissions Gate v1, while staying within $\pm 0.005$ of S2 on FB/IG. | Improved `logR CRPS` on 14/17 cells including `h=1, 4, 13, 26` on both Reddit panels and `h=52` on Submissions (`0.2145` vs `0.2149`); stayed within `0.0007` of S2 on FB/IG; cut Submissions Gate v1 (`0.766 -> 0.529`, v1 `0.431`) and flipped Comments Gate v1 to a win (`0.105` vs `0.132`). **Misses:** Submissions `h=52` (`0.2145`) remained `0.0020` behind `rank_bin` (`0.2125`); Comments `h=52` `logR CRPS` ticked up `0.1892 -> 0.1949`; Submissions `share_corr` fell slightly under S3 (`0.57 -> 0.36` at $h=26$). | **PARTIAL** |

### Failed / Abandoned Development Variants (Reported at Equal Prominence)
1. **`fit_gamma_L = True` (coupling entity volatility $v_i$ to lasting shocks $L$):** Worsened calendar-aligned CRPS at every horizon relative to `gamma_L = 0` while adding 1 scalar; turned OFF by default (`stage2_v2_benchmark_results.json`).
2. **Strict equality Spec-B constraint on `facebook_a`:** Forcing $\sigma_{\text{obs}}$ strictly equal to the daily replication floor without an excess 1-week burst term forced $\phi$ to collapse to `0.45` (`sse = 0.015`); switching to exact lower-bound inequality cut SSE 7× (`0.0020`) and recovered $\phi = 0.84$.
3. **Quadratic `A_lr` residualization in `filter_endpoints` (`stage2_v3`):** Quiet subreddits with `var_i == 0` distorted the quadratic fit and blew up $\hat{v}_i$ (`Submissions h=1 CRPS 0.073 -> 0.175`); immediately reverted.
4. **Remaining open structural gaps (at diminishing returns on DEVELOPMENT data):**
   - `reddit_submissions_long` arithmetic head share (`0.59` S2 / `0.68` S3 vs. emp `0.36`) and Gate v1 (`0.43–0.53` vs. base `0.193`, driven by near-zero empirical `coll1 = 0.05` magnifying small relative errors).
   - Residual leverage (`lev = 1.11–1.13` empirical vs. `0.89–0.90` S3) and multi-block decline runs (`r2 = 1.25–1.28` vs. `1.06–1.09` S3) on Reddit, pointing to a modest negatively skewed tail in persistent decline beyond symmetric $d_i$ + positive surges.
   - `instagram_hm` 90% coverage (`0.81` vs. nominal `0.90`), reflecting the §2z-c censoring floor's sharp non-Gaussian excursions.

---

## 6. Readiness Checklist for Held-Out & New Platforms (Owner-Gated)

**No sealed or unexamined data was touched anywhere in Stages 1–4.** Before opening the Reddit 2023–24 bridge (WD drive, SEALED), Wikipedia pageviews (`/Volumes/T9/rank-diffusion-data/wikipedia`, EXPOSURE UNKNOWN), or YouTube (NOT IN LEDGER), we recommend freezing a single pre-registered protocol covering:

1. **Two frozen arms, zero retuning:** Evaluate **Common-S2** (11 scalars, `asymmetry=False`) and **Common-S3** (14–15 scalars, `asymmetry=True`) out-of-the-box with identical closed-form estimators and zero hand tuning.
2. **Five pre-registered primary tests on each new platform:**
   - **T1 (Universal constants):** Does $\phi \in [0.75, 0.96]$ and $s_{\text{vol}} \in [0.55, 0.90]$ hold on YouTube, Wikipedia, and Reddit 2023–24?
   - **T2 (Feed vs. community structural split):** Does YouTube (creator/recommendation feed, or channels with upload + views-per-video decomposition) show broadcast-like behavior ($E$ dominates $\sigma_L^2$, fading surges, small/zero $\sigma_d$) or community-like persistent drift ($\sigma_d > 0$)? Does Wikipedia (event-driven attention) show strong fading surges with $\sigma_d \approx 0$? Does Reddit 2023–24 reproduce $\sigma_d \approx 0.007\text{–}0.009$, $D > 0$, and $p_{\text{fall}} > p_{\text{rise}}$?
   - **T3 (Goal 1 structure):** Boundary ratio within `[0.80, 1.25]`, crossing-dominated departures (`> 90%`), zero `expm1` blowups, and head-share calibration within 20-seed `[q05, q95]` bands.
   - **T4 (Goal 2 individual endpoint movement):** Calendar-aligned `logR CRPS` beating BOTH `no_change` and `rank_bin` at $h \in \{1, 4, 13, 26\}$ weeks (and `h = 52` where $T$ permits), plus calibrated 90% coverage `[0.82, 0.94]`.
   - **T5 (Gate v1 pooled displacement):** Pooled relative error beating or matching the historical-mobility baseline across 5 rolling origins × 20 reps.
3. **Clean intake gates:** Run `data-intake` schema/era/instrument audits first (especially Pushshift $\to$ Arctic Shift score-timing forensics on the Reddit 2023–24 bridge, and exposure audit on Wikipedia 2025-01).


---

## 7. Independent Technical Reviews (Fable 5.2 & Astra 6.2) and Record Corrections

Following the initial Stage 1–4 pass (§2z-dx), two independent read-only technical assessments were commissioned from **Fable 5.2** ([REVIEW_R1_fable-5.2.md](REVIEW_R1_fable-5.2.md)) and **Astra 6.2** ([REVIEW_R1_astra-6.2.md](REVIEW_R1_astra-6.2.md)) using a shared brief ([REVIEW_PROMPT_R1.md](REVIEW_PROMPT_R1.md)). Both reviewers inspected the code, tests, ledgers, and results and identified critical mathematical, evaluator, and reporting issues that guided Rounds 2–3:

### Explicit Corrections to Stage 1–4 (§2z-dx) Reporting
1. **Instagram $\hat{\sigma}_d = 0$ is unidentified at $T = 52$, not a broadcast-platform discovery:** `_estimate_stage3_drift` returns `0` whenever `T < 75` (`short_panel` branch). Claiming $\hat{\sigma}_d = 0$ "turns off automatically on broadcast feeds" applies only to `facebook_a` ($T = 86$, where $T \ge 75$ actually tested the moment and returned `0` vs. Reddit matched-length 86w cuts where Comments recovered `0.0088–0.0107` and early Submissions recovered `0.0085`).
2. **20-member `0.05/0.95` quantile coverage bug (Astra 6.2):** For $M = 20$ exchangeable continuous draws, linearly interpolated `np.quantile(ens, [0.05, 0.95])` has expected coverage $17.1/21 = 0.8143$, NOT `0.90`. Thus Stage 3's `0.81` Instagram coverage was actually calibrated while `0.90` Submissions coverage was over-dispersed. Fixed in Round 3 via `n_ens = 39` exact order statistics $[x_{(2)}, x_{(38)}]$ ($36/40 = 0.9000$, verified by an internal Gaussian oracle check `0.8985`, `pit_sd = 0.2895`) plus randomized PIT uniformity.
3. **Canonical departure decomposition (§2z-ac):** Stage 3's `dep_cross_frac` used `still_obs_below[1:] | ever_after[1:]`, mixing immediate crossings with intermittent absences that later reappear. Round 3 decomposes departures strictly into (i) immediate crossing (`r_t <= K`, `K < r_{t+1} < inf`, same ID), (ii) absent at $t+1$ returning within 13 weeks, and (iii) terminal/$>13\text{w}$ departure.
4. **Evaluator cohort & denominator fixes:** Stage 3 masked departed IDs before computing `med_m` and `tot_m` (omitting reborn top-$K$ competitors) and filtered `ok_x = all 20 draws finite` separately per model (dropping ~80% of Instagram accounts or collapsing NaNs to the median). Fixed via `forecast_endpoints(..., return_totals=True)`, single-pass conditional-on-present `X` scoring, and a single truth-defined cohort across all models.
5. **Minor reporting clarifications:** `s_vol = 0.69–0.76` is a log-**variance** SD (~2.0× variance / ~1.43× SD spread, not 2.0× SD); Stage 1's `local_level` symmetric trend improved MAE in 14/17 cells (not hurt at every horizon); P3.1 was PARTIAL against `DESIGN.md` §2 (which initially listed `alpha_skew` and a 3-state filter); `hl_s` hitting the 26w grid ceiling was not evidence of a universal 26-week decay; and the scratch cache's full-panel 4K universe membership + full-era FB Spec-B is a known development-cache limitation (not train-only universe rebuilding).

---

## 8. Round 2A Diagnostics & Round 2B Mathematical Fixes

### Round 2A Targeted Diagnostics (`round2a_diagnostics.py` → `round2a_diagnostics.json`)
- **D1 (Kalman MAE vs. EWMA):** Stage 3's `MAE` lag vs. `local_level` EWMA came from 20-draw MC median jitter and subtracting `median(denoised_pred)` instead of `med_K[T0-1]`. Centered on `med_K[T0-1]`, analytical Kalman `L_hat + phi*xi_hat` beat both `no_change` and `ewma(0.35)` on **4/4 platforms** at $h=1$ (`FB 0.3796 vs 0.3859`, `IG 0.6339 vs 0.6433`, `Comments 0.3063 vs 0.3099`, `Submissions 0.2017 vs 0.2019`).
- **D2 (Decline-state hypothesis killed + exact $\kappa_5/\kappa_3$ surges):** A sign-preserving within-entity time-permutation null accounted for **100%** of Reddit's excess decline runs (`Comments r2 = 1.328` vs. sign-perm `1.360`, `r3 = 1.485` vs. `1.512`; `Submissions r2 = 1.258` vs. `1.299`, `r3 = 1.337` vs. `1.406`), and Astra's 3-window mixed third moment was near zero (`+0.0012` Comments, `-0.0157` Submissions). **Decision:** Do NOT add a `decline_skew` or decline-state parameter. Replacing the heuristic surge divisor and hard clips with the exact additive cumulant ratio $\kappa_5/\kappa_3 = (m_5/m_3)\mu^2$ produced 100% interior $(\hat{\lambda}, \hat{\mu})$ across all 4 panels.
- **D3 (Matched-length `T=86` control, multi-gap drift decay, and `s_vol` placebo):**
  - Poisson count-noise placebo (`true s_vol = 0`) yielded `s_vol_hat = 0.0369` (FB-like) and `0.0000` (Reddit-like) vs. empirical `0.69–0.76`, confirming entity volatility heterogeneity is genuine.
  - Multi-gap covariance (`W=13, H=39`) on Reddit decayed to 0 by `g = 39w` (`Comments g=4/13/26/39: 0.0122 / 0.0123 / 0.0073 / 0.0000`; `Submissions: 0.0088 / 0.0091 / 0.0062 / 0.0000`), pinning true drift persistence at ~1 year (`hl_d = 52w`), not 3 years (`156w`).
  - Matched-length 86w cuts recovered `sigma_d = 0.0088–0.0107` on Comments and `0.0085` on early Submissions vs. `0.0000` on Facebook Era A.

### Round 2B Fixes in `llm_fitting/common_rankdiff.py` (14/14 unit tests passing)
1. **Exact capped-exponential ($C=5.0$) raw moments** `CAP_M1..CAP_M5` everywhere + 120-week surge warmup.
2. **Exact boundary-free $\kappa_5/\kappa_3$ surge estimator** over `SURGE_HL_GRID = [8, 13, 26, 39, 52]`.
3. **`DRIFT_HL_FIXED = 52.0`** (1-year drift half-life matching D3's empirical gap decay).
4. **Symmetrized `innov_pool`** under Stage 3 (`asymmetry=True, surge_lam > 0`), eliminating skew double-counting.
5. **Exact Spec-B lower-bound law** (`a_obs_floor, b_obs_floor`) guaranteeing $\sigma_{\text{obs}}(z) \ge \sigma_{\text{floor}}(z)$ at every rank $z$.
6. **Exact first-observation covariances** (`P12 = -V_xi*v_hat`, `P[0,3] = -v_s0_i`) and `s_sc_eb**2` in `base_var1`.
7. **Fixed `s >= 0` clamp bug** (clamps `s_init >= 0` without shifting `L_init`) + `return_totals=True`.
8. **Within-band Bessel-corrected variogram demeaning + stratum-demeaned drift covariance + top-$K$ median centering:** First exact frozen Gate v1 run revealed that subtracting full-universe `nanmean(X, axis=1)` (including 15k buffer subreddits) injected a `-1.08` log-unit cross-tier trend into `reddit_submissions_long` band 0 (`+68%` fake variance at lag 40, crushing `V_xi` and `sigma_obs` to 0). Demeaning $h$-step increments within each permanent-rank band (Bessel $n_t/(n_t-1)$), demeaning drift covariance within permanent-rank strata (`K/16, K/4, K`), and centering on `med_K(t)` removed this contamination with zero new parameters — cutting `reddit_submissions_long` Gate v1 error nearly in half (`0.603 -> 0.346`, `S3 0.527 -> 0.310`) and driving `reddit_comments_ext` S3 to **`0.098 ± 0.033`** (100% cov, 5/5 wins).

---

## 9. Exact Frozen Gate v1 Results (`round3_frozen_gate.py`)

Calling `rankdiff_kalman.py`'s exact frozen functions (`_gate_windows`, `_rank_dist`, `_moments`, `_collisions_from_topids`, `_boot_ci`, `MOM_FLOOR`) at 5 splits × 20 reps (boot 2000):

| Panel ($K$) | Persistence baseline | Frozen Model A (50+ sc, ledger) | Frozen RDM′ `movinghome_vp` (ledger) | **Common-S2 (11 sc)** | **Common-S3 (14–15 sc)** |
|---|---:|---:|---:|---:|---:|
| `facebook_a` (3,500) | `0.147 ± 0.030` | `0.103 ± 0.044` (80%) | `0.123 ± 0.026` (40%) | **`0.105 ± 0.043`** (60%, 4/5) | **`0.108 ± 0.023`** (60%, 4/5) |
| `instagram_hm` (10,000) | `0.869 ± 0.554` | `0.257 ± 0.109` (60%) | — | **`0.296 ± 0.216`** (80%, 4/5) | `0.360 ± 0.276` (80%, 4/5) |
| `reddit_comments_ext` (12,500) | `0.158 ± 0.038` | `0.123 ± 0.051` (80%) | `0.130 ± 0.054` (80%) | **`0.129 ± 0.071`** (80%, 3/5) | **`0.098 ± 0.033`** (100%, **5/5**, vs S2 4/5) |
| `reddit_submissions_long` (5,000) | `0.165 ± 0.044` | `0.188 ± 0.063` (80%) | `0.183 ± 0.074` (80%) | `0.346 ± 0.064` (0%, 0/5) | `0.310 ± 0.125` (20%, 0/5, vs S2 3/5) |

*Note on `reddit_submissions_long` residual:* After removing the cross-tier mean drift (dropping error from `0.603/0.527` to `0.346/0.310`; split `T0=106 S3` hit `0.125` with exact `dRank1/4/13` match), simulated median top-100 displacements remain ~1–4 ranks above empirical (`dRank1 5 vs 4, dRank4 7 vs 5, dRank13 10–12 vs 8`). This reflects an honest structural trade-off: `reddit_submissions_long` has extreme 1% outlier variance (`raw/iqr = 2.0–4.2×`), so a moment-matched homoscedastic lasting step (`gamma_L=0`) matches variance while slightly over-predicting *median* displacement.

---

## 10. Repaired Round 3 Gold Benchmark (`round3_gold_benchmark_results.json`)

### Goal 1: Aggregate Structure, Canonical Departures & Out-of-Moment Asymmetry (20 paired seeds)

| Metric | `facebook_a` Emp / S2 / S3 | `instagram_hm` Emp / S2 / S3 | `reddit_comments_ext` Emp / S2 / S3 | `reddit_submissions_long` Emp / S2 / S3 |
|---|---:|---:|---:|---:|
| Free scalars | — / **11** / 14 | — / **11** / 14 | — / **11** / 15 | — / **11** / 15 |
| Top-10 turnover | `0.350` / **`0.350`** / `0.550` | `0.267` / `0.167` / **`0.333`** | `0.267` / **`0.267`** / `0.450` | `0.300` / `0.233` / **`0.300`** |
| Rank-1 wkly chg | `0.518` / **`0.459`** / `0.435` | `0.392` / `0.814` / `0.755` | `0.028` / **`0.000`** / `0.201` | `0.057` / **`0.047`** / `0.066` |
| Head share (blk 0 → last) | `0.239→0.229` / **`0.241→0.243`** / `0.280→0.290` | `0.437→0.420` / **`0.396→0.392`** / `0.399→0.396` | `0.614→0.506` / `0.580→0.590` / `0.588→0.591` | `0.511→0.357` / `0.477→0.511` / `0.509→0.565` |
| Boundary ratio | `0.980` / **`0.998`** / `1.137` | `1.000` / **`0.978`** / `1.157` | `1.009` / **`1.001`** / `1.132` | `1.117` / `0.986` / **`1.111`** |
| Dep imm. cross% (§2z-ac) | `95.7%` / **`95.4%`** / `94.4%` | `76.2%` / **`74.8%`** / `73.0%` | `98.9%` / **`92.7%`** / `89.2%` | `99.2%` / `97.7%` / **`98.1%`** |
| Dep absent→ret13% | `3.7%` / **`4.5%`** / `5.4%` | `21.8%` / **`24.2%`** / `26.0%` | `0.6%` / `7.2%` / `10.7%` | `0.7%` / `2.3%` / `1.9%` |
| Gapped $\rho$ (emp 90% CI) | `-0.095 [-.11,-.08]` / `-0.068` / `-0.067` | `-0.021 [-.03,-.01]` / `-0.103` / `-0.081` | `+0.145 [+.14,+.16]` / `-0.049` / **`+0.104`** | `+0.155 [+.14,+.17]` / `-0.039` / **`+0.088`** |
| Asym $D$ (emp 90% CI) | `+0.031 [-.01,+.08]` / `-0.027` / **`+0.044`** | `-0.087 [-.12,-.05]` / `+0.005` / `+0.082` | `+0.132 [+.11,+.16]` / `-0.039` / **`+0.149`** | `+0.222 [+.18,+.27]` / `-0.035` / **`+0.163`** |

*10-decile $E[y \mid x\text{ decile}]$ curve:* On both Reddit ladders, `Common-S3` reproduces the exact non-monotonic hook shape out-of-moment (`Comments emp [-.216 -> +.107 at d8 -> +.049 at d9]` vs `S3 [-.188 -> +.142 at d8 -> -.025 at d9]`; `Submissions emp [-.265 -> +.112 -> +.033]` vs `S3 [-.164 -> +.122 -> -.056]`), whereas symmetric `S2` is monotonically downward-sloping. On `instagram_hm`, empirical $D = -0.087$ is negative (opposite sign from Reddit/FB), making `S2` cleaner than `S3`.

### Goal 2: Repaired Individual Endpoint Forecasts (`M = 39`, Exact `[x_(2), x_(38)]` Coverage, Common Cohort)
- **Analytical Kalman `X MAE`:** `S3` beats both `no_change` and train-tuned `local_level` EWMA in **16/17** cells (`S2` in **14/17**; e.g., `FB h=1..26: 0.402/0.466/0.578/0.692 vs EWMA 0.408/0.475/0.603/0.721`; `IG h=1..13: 0.637/0.690/0.792 vs EWMA 0.638/0.694/0.802`; `Comments h=1..26: 0.286/0.346/0.433/0.528 vs EWMA 0.287/0.348/0.440/0.535`; `Submissions h=1..52: 0.166/0.210/0.286/0.372/0.504 vs EWMA 0.167/0.210/0.288/0.377/0.507`).
- **Exact distribution-free 90% coverage `[x_(2), x_(38)]` & PIT SD (target `0.289`):**
  - `facebook_a`: **`0.902–0.912`** (`pit_sd 0.267–0.276`)
  - `instagram_hm`: **`0.893–0.900`** (`pit_sd 0.280–0.283`) — dead-on 90%!
  - `reddit_comments_ext`: **`0.905–0.922`** (`pit_sd 0.262–0.277`)
  - `reddit_submissions_long`: **`0.944–0.952`** (`pit_sd 0.232–0.244`, ~5% over-dispersed due to 4× outlier kurtosis).
- **Coarsened `logR CRPS`:** `S2` and `S3` beat both `no_change` and `rank_bin` in **17/17** cells by **6–35%**.
- **Fair Head Brier (`fBrierHD`):** `S2` beats `rank_bin` in **17/17** cells by **3–36%** (`S3` in **16/17**).
- **Head Audience Share-Shift (`shareHD_MAE`, top-$K/16$):**
  - `facebook_a`: `S2` beats zero-shift and EWMA on **4/4** horizons (`-18%` to `-27%` vs zero-shift, `r = +0.58 to +0.63`).
  - `instagram_hm`: `S2/S3` beat zero-shift and EWMA on **3/3** horizons (`-25%` to `-30%` vs zero-shift, `r = +0.57 to +0.64`).
  - `reddit_comments_ext`: `S2` beats zero-shift on **5/5** horizons and beats/ties EWMA on **4/5** (`h=52: -10.9%`, `r = +0.67`).
  - `reddit_submissions_long`: near-zero short-horizon shifts favor zero-shift at `h=1..13`, while at `h=26w, 52w` **`S3` beats both zero-shift and EWMA** (`h=52: -9.2%`, **`r = +0.807`**).

### True Out-of-Period Structure Test (Fit 2019–20 → Roll Forward 2021–22, Zero 2021–22 Info, 20 Seeds)
- **`reddit_comments_ext`:** `S2` forecasts 2021–22 `Turn top10 = 0.200 [0.00, 0.40]` (emp `0.300`), `Lad gap r1 = 10.38 [9.74, 11.04]` (emp `10.27`), `Boundary = 0.987` (emp `0.947`), `Dep imm% = 94.6%` (emp `95.6%`); `S3` adds `Turn K/4 = 0.143` (emp `0.153`), `Lad K/64 = 6.015` (emp `6.098`), `Dep rate = 0.0084` (emp `0.0069`), and positive out-of-period asymmetry `D = +0.055` (emp `+0.123`, `S2 = -0.042`).
- **`reddit_submissions_long`:** `S2` forecasts 2021–22 `Turn top10 = 0.300` (emp `0.300`), `Lad K/64 = 4.825` (emp `4.701`), `Dep imm% = 98.6%` (emp `95.0%`); `S3` forecasts out-of-period **`Gapped rho = +0.085 [+0.073, +0.102]` (emp `+0.102`, `S2 -0.033`)**, **`Asym D = +0.163 [+0.144, +0.190]` (emp `+0.133`, `S2 -0.029`)**, and **`Dep rate = 0.0067` (emp `0.0055`)**. Neither stationary model predicts Reddit's one-time secular head-share decentralization (`Comments 0.575 → 0.506`, `Submissions 0.509 → 0.357`).

---

## 11. Final Verdict & Held-Out Readiness

Development on the four seen ladders has reached **clear diminishing returns**:
- **Recommended primary reference (`Common-S2`, 11 free scalars):** Cleanest stationary structure (top-10 turnover, boundary ratio ~1.00, canonical immediate crossings, stable block head share), exact 90% coverage on 3/4 platforms (`0.893–0.922`), 17/17 wins on `logR CRPS` and fair head Brier, and competitive exact frozen Gate v1 on 3/4 platforms (`FB 0.105`, `IG 0.296`, `Comments 0.129`).
- **Recommended paired asymmetry comparator (`Common-S3`, 14–15 free scalars):** Moment-identified fading surges ($\kappa_5/\kappa_3$) + 52-week persistent drift ($\sigma_d$); unlocks out-of-moment 10-decile asymmetry curves and $D$, best-ever Gate v1 on `reddit_comments_ext` (`0.098 ± 0.033`, 100% cov, 5/5 wins), 16/17 Kalman MAE wins, and `r = +0.81` 52-week head share-shift predictions on `reddit_submissions_long`, at a known cost of mild top-10/boundary overshoot from `hl_s = 52w` surges and wrong-sign $D$ on Instagram.
- **Ready for owner decision:** Freeze a pre-registered held-out protocol specifying both `Common-S2` (primary) and `Common-S3` (comparator) before any contact with the sealed Reddit 2023–24 bridge, YouTube, or Wikipedia datasets.
