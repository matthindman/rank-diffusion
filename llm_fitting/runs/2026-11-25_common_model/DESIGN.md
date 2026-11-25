# Common Stochastic Model — Stage 1 Diagnostic Design

_Date: 2026-11-25. Status: EXPLORATORY (DEVELOPMENT data only). Nothing here modifies a default, a frozen stack, a registered protocol, or any sealed dataset._

## 0. Scope & Data Hygiene

**Allowed DEVELOPMENT datasets only** (per `llm_fitting/EXPOSURE_LEDGER.md`):
1. `facebook_a`: `/Volumes/T9/rank-diffusion-data/derived/fb_weekly_rebuilt.parquet` (Era A: 2020-11-02..2022-06-27, 86 weeks, K=3,500) + `fb_daily.parquet` (Era A, Spec-B). Item column: `post_count`.
2. `reddit_comments_ext`: `/Volumes/T9/rank-diffusion-data/derived/reddit_comments_2018-12_2022-12_weekly_REGISTERED.parquet` (2018-12..2022-12, 213 weeks, K=12,500). Item column: `comment_count`.
3. `reddit_submissions_long`: `/Volumes/T9/rank-diffusion-data/derived/reddit_weekly_long.parquet` (2018-12..2022-12, 212 weeks, K=5,000). Item column: `submission_count`.
4. `instagram_hm`: `llm_fitting/ig_hm_totals.parquet` (2023 weeks 1–52, K=10,000; strictly through the §2z-c censoring apparatus, estimand "of a-matching activity"). Item column: `n_posts`.

**Strictly untouched (SEALED / EXPOSURE UNKNOWN / NOT IN LEDGER):**
- Reddit 2023–24 bridge (WD drive, SEALED)
- Wikipedia pageviews (`/Volumes/T9/rank-diffusion-data/wikipedia`, EXPOSURE UNKNOWN)
- `tmp_youtube_2022plus_extract.json` and any sibling `youtube-*` directories

**Memory discipline:** Build a lean, exact universe cache (top-4K by absence-penalized permanent rank, matching `mrd.restrict_universe`) outside the repository in the session scratch directory so no diagnostic script holds full 26–54M-row frames in memory.

---

## 1. Stage 1 Measurements & Pre-Run Predictions

Every prediction below is written BEFORE running any Stage 1 diagnostic script.

### 1A. Third-Moment & Leverage Split of the Asymmetry (`stage1_asymmetry.py`)

For non-overlapping gapped changes $(x, y)$ of window means (stratum-demeaned within 3 permanent-rank strata: head $\le K/16$, mid $(K/16, K/2]$, body $(K/2, K]$):
- Reddit: 13-week windows, 52-week span, 13-week gap (origins stepping by 13 weeks).
- Facebook: 4-week windows, 26-week span, 4-week gap (origins stepping by 4 weeks).
- Instagram (`instagram_hm`, presence $\ge 0.5$): 4-week windows, 16-week span, 4-week gap (origins stepping by 4 weeks).

Compute:
- Standard asymmetry $D = p_{\text{fall}} - p_{\text{rise}}$, $p_{\text{fall}}$, $p_{\text{rise}}$, Spearman persistence $\rho(x, y)$.
- Standardized cross third moments:
  $$\mu_{21} = \frac{E[x^2 y]}{\sigma_x^2 \sigma_y}, \qquad \mu_{12} = \frac{E[x y^2]}{\sigma_x \sigma_y^2}$$
- Volatility-direction (leverage) ratio: $\text{SD}(y \mid x \le q_{10}) / \text{SD}(y \mid x \ge q_{90})$.
- First verify $\mu_{21}$ and $\mu_{12}$ on three synthetic planted worlds: (i) homogeneous Gaussian walk, (ii) negatively skewed persistent growth rates, (iii) positively skewed fading surges.

**Predictions:**
- **P1.1 (Planted separation):** In synthetic worlds, homogeneous Gaussian gives $\mu_{21} \approx 0, \mu_{12} \approx 0$; skewed persistent decline gives $\mu_{21} < 0, \mu_{12} < 0$ (same sign); fading positive surges give $\mu_{21} < 0, \mu_{12} > 0$ (opposite signs). On Reddit empirical data, $\mu_{21} < 0$ on both ladders; the sign of $\mu_{12}$ adjudicates whether persistent skewed decline ($\mu_{12} \le 0$) or fading surges ($\mu_{12} > 0$) dominates.

### 1B. Consecutive Run Lengths of Declines vs. Gains
On non-overlapping 26-week blocks (Reddit, 8 blocks $\to$ 7 transitions) and 13-week blocks (Reddit & Facebook), count runs of $m \in \{2, 3, 4\}$ consecutive stratum-demeaned declines vs. $m$ consecutive gains, compared to a within-entity sign-flip / time-permutation null.

**Predictions:**
- **P1.2 (Decline runs):** On Reddit, the ratio of consecutive multi-block decline runs to gain runs at $m \ge 2$ and $m \ge 3$ will exceed 1.15 on both ladders, beyond permutation bands. On Facebook (13-week blocks), the ratio will be closer to 1.0.

### 1C. Honest Uncertainty: Origin-by-Origin & Co-Movement Cluster Bootstrap
Cluster universe entities into $C = 50$ co-movement clusters using standardized weekly residual trajectories (k-means on truncated SVD of weekly demeaned changes). Resample whole clusters (200 draws) vs. independent entities (200 draws) for $D$ and persistence; also report each forecast origin individually and leave-one-origin-block-out.

**Predictions:**
- **P1.3 (Cluster bands):** Co-movement cluster bootstrap 90% intervals for $D$ and persistence on Reddit will be 1.3–2.5× wider than entity-bootstrap intervals, but the 90% cluster interval for $D$ will remain strictly positive on both Reddit ladders.

### 1D. Two-Channel Physical Decomposition: Item Supply vs. Engagement-per-Item
Decompose weekly log activity as:
$$X_{it} = N_{it} + E_{it}, \qquad N_{it} = \log(1 + \text{items}_{it}), \qquad E_{it} = X_{it} - N_{it}$$
Compute lasting variance $\sigma_L^2$ (via structure-function slope), persistence $\rho$, asymmetry $D$, $p_{\text{fall}}$, $p_{\text{rise}}$, $\mu_{21}$, $\mu_{12}$, and cross-channel persistence $\text{Corr}(\Delta N_1, \Delta E_2)$, $\text{Corr}(\Delta E_1, \Delta N_2)$ on all four ladders.

**Predictions:**
- **P1.4 (Supply vs. Virality):** Across ladders, lasting variance and persistent decline ($p_{\text{fall}}$) will be markedly stronger in item supply $N$ than in engagement-per-item $E$, while $E$ will show stronger mean reversion / fading upside ($p_{\text{rise}} \le 0$).

### 1E. Parsimony & Defect Audit (`stage1_parsimony_predict.py`)
Across 8 permanent-rank bands on all four ladders:
1. Measure band profiles of $\gamma_0(z)$, $\gamma_1(z)$, structure-function curvature & slope ($D_b(h)$), positive vs. negative standardized residual tails (95th, 99th, 99.9th percentiles and kurtosis), weekly absence rate vs. 13-week disappearance rate, and split-half entity volatility correlation $\text{Corr}(\log \hat{\sigma}_{i,1}, \log \hat{\sigma}_{i,2})$.
2. Measure $R^2$ of a 2-parameter linear-in-$z$ (or linear-in-level) law for each band profile.

**Predictions:**
- **P1.5 (Low-dimensionality & volatility persistence):** A 2-parameter linear law in log-rank $z$ will capture $\ge 75\%$ of between-band variance for volatility profiles on at least 3 of 4 ladders. Split-half entity volatility correlation will exceed 0.65 on all four ladders (confirming per-entity $\hat{v}_i$ is a strong unused signal for Goal 2).

### 1F. Calendar-Aligned Individual Endpoint Predictability Ceiling (Goal 2)
At 5 rolling origins per panel (train-only universe, zero lookahead, strict calendar alignment $T_0 \to T_0 + h$ with $h \in \{1, 4, 13, 26, 52\}$ as panel length permits), evaluate individual-endpoint out-of-sample accuracy for:
- **Target 1 (Relative level / log-share shift):** $\tilde{X}_{i, T_0+h} = X_{i, T_0+h} - \text{median}_K(X_{T_0+h})$, MAE & fair CRPS.
- **Target 2 (Coarsened log-rank):** $\log(\min(r_{i, T_0+h}, K+1))$, fair CRPS.
- **Target 3 (Audience share shift $\Delta s_{i,h}$):** for top-$K$ entities, MAE and correlation of predicted vs. realized $\Delta s_{i,h}$.
- **Target 4 (Top-$k$ retention / outside-$K$ event):** Brier score.
- **Target 5 (Volatility / move-size prediction):** $R^2$ and Spearman correlation predicting $|\Delta \tilde{X}_{i,h}|$ using empirical-Bayes entity volatility $\hat{v}_i$ vs. band-only volatility.

Compare transparent out-of-sample predictors:
1. `no_change`: last observed state at $T_0$.
2. `local_level`: 1-state EWMA / Kalman filter separating lasting level from passing noise.
3. `local_level_plus_trend`: adding recent gapped/trailing change (symmetric vs. asymmetric: past decline vs. past gain separately).
4. `local_level_plus_items`: adding trailing item-supply level/change vs. engagement-per-item residual.
5. `eb_vol`: predictive scale using per-endpoint shrunk volatility $\hat{v}_i$ vs. band-constant volatility.

**Predictions:**
- **P1.6 (Endpoint predictability ceiling):**
  - `local_level` beats `no_change` on level MAE and log-rank CRPS at all horizons $h \ge 1$ on all four ladders.
  - Per-endpoint empirical-Bayes volatility $\hat{v}_i$ beats band-only volatility on predicting future move magnitude $|\Delta \tilde{X}_{i,h}|$ and improves Gaussian/empirical-residual CRPS at $h = 1, 4, 13, 26$ on all four ladders.
  - Asymmetric past trend (separating decline from gain) improves out-of-sample point/CRPS forecasts on Reddit at $h = 26, 52$, while symmetric linear trend adds little or hurts.

---

## 2. Stage 3 Design & Pre-Run Predictions (2026-11-25, written before coding)

Stage 2 (`common_rankdiff.py`, 11 core scalars) reached diminishing returns as a homogeneous random-growth model:
- Beats `no_change`, `rank_bin`, and `noEB` across 16/17 calendar-aligned endpoint `logR CRPS` cells on all four platforms, wins Gate v1 on FB (3/5), IG (5/5), and Reddit comments (3/5), eliminates `expm1` blowups and ad-hoc $c=0.96$ patches, and fixes the departure crossing composition (~99.9%).
- Remaining structural/long-horizon gap (expected by construction): Reddit gapped persistence ($\rho = +0.145 / +0.155$ vs Stage 2 $-0.042 / -0.026$), asymmetry $D$ ($+0.132 / +0.222$ vs Stage 2 $-0.028 / -0.028$), consecutive decline runs ($1.25\text{–}1.56\times$), Facebook's fading-surge third moments ($\mu_{21} = -0.10, \mu_{12} = +0.09$), and Submissions' head-volatility overstatement from attributing persistent drift variance to $\sigma_L^2$.

### Mechanism (`asymmetry=True`, opt-in; Stage 2 default bit-identical)
Add two moment-identified components (+5 scalars max, 0 when inactive -> 11–16 total):
1. **Fading positive viral surges** $s_{i,t+1} = \rho_s s_{it} + B_{it} E_{it}$ ($\lambda_s, \mu_s, \text{hl}_s \in \{8, 13, 26\}$w): identified ONLY from marginal skewness and Kelly tail asymmetry of single changes at $h \in \{4, 13, 26\}$w (never $D$). Exact variance $2 V_s (1 - \rho_s^h)$ subtracted from $D_b(h)$.
2. **Skewed persistent entity drift** $d_{i,t+1} = \rho_d d_{it} + \sigma_d \sqrt{1 - \rho_d^2} \zeta_{it}$ ($\text{hl}_d = 156$w fixed; 2 free scalars $\sigma_d, \alpha_{\text{skew}}$):
   - $\sigma_d$ identified in closed form from gapped autocovariance $\text{Cov}(x, y)$ net of $\xi$ and $s$ covariances ($\hat{\sigma}_d = 0$ automatically when net covariance $\le 0$, as on FB/IG!). Exact drift structure function $\sigma_d^2 f_{\text{vel}}(h; \rho_d)$ subtracted from $D_b(h)$ before fitting $\sigma_L$.
   - $\alpha_{\text{skew}} \in [0, 0.95]$ identified from the non-overlapping 2-block consecutive decline-vs-gain run ratio $R_2$ (never $D$).
3. **Exact 3-state Kalman filter** $[L_{it}, \xi_{it}, d_{it}]$ when $\sigma_d > 0$ (reduces exactly to 2-state when $\sigma_d = 0$) for individual endpoint forecasts at $h = 1, 4, 13, 26, 52$ weeks.

### Pre-Run Predictions
- **P3.1 (Recovery):** Planted worlds with $\sigma_d = 0$ recover $\hat{\sigma}_d \approx 0$; planted worlds with $(\lambda_s, \mu_s, \sigma_d, \alpha_{\text{skew}})$ recover all parameters cleanly; `asymmetry=False` stays bit-identical.
- **P3.2 (Goal 1 out-of-moment asymmetry & persistence):** On Reddit comments & submissions, Stage 3 achieves $\rho \in [+0.08, +0.20]$, strictly positive $D \in [+0.08, +0.25]$ with $p_{\text{fall}} > +0.10$ and $p_{\text{rise}} < +0.06$, decline run ratio $> 1.15$, and zero `expm1` blowups. On Facebook, $\hat{\sigma}_d \approx 0$ automatically while surges shift $D$ positive and match $\mu_{21} < 0, \mu_{12} > 0$.
- **P3.3 (Goal 2 long-horizon endpoint forecasts & must-not-worsen):** On Reddit, Stage 3 improves `X CRPS` and `logR CRPS` at $h = 26, 52$ weeks (bringing Submissions `h=52` below `rank_bin`) and improves Submissions' Gate v1 error, while staying within $\pm 0.005$ of Stage 2 on Facebook and Instagram.
