#!/usr/bin/env python3
"""Community-canonical + forecast-evaluation metrics (2026-07-06 metrics audit).

ADDITIVE, DESCRIPTIVE-ONLY.  Nothing here enters the 15-card, `_score`, the
churn error, or the OOS gate's frozen relative-error criterion; committed
baselines are untouched.  This module supplies:

(a) The ranking-dynamics literature's canonical observables, so the paper can
    report them by their community names (Iniguez, Pineda, Gershenson &
    Barabasi, Nat. Commun. 2022; Cocho et al., PLoS ONE 2015):
      - rank-change probability C(r) as a FULL curve (generalizes coll{cr})
      - rank flux F and rank turnover o_t / o-dot at the top-K boundary
      - rank diversity d(k): distinct occupants of rank k per unit time
(b) Direct ladder (goal-1 estimand) metrics -- the aggregate rank-size claim,
    previously computed (`_ranksize`) but never scored:
      - D_ladder: RMSE of the time-mean rank-size curve over log-rank bins
      - D_share:  max concentration-curve gap  |S_sim(k) - S_emp(k)|
    (FB shares are "of tracked activity"; Reddit shares are platform-wide --
    the census/censoring language discipline of MODEL_STATUS 2g applies.)
(c) Rolling-origin variants of the two period-0-anchored card rows (R2_h,
    Pers_h).  The committed single-origin rows stay the regression guard;
    these are the stationarity-respecting versions for the paper (the FB Era-A
    election-week initialization sensitivity of 2o is the concrete motivation).
(d) A rank-band transition matrix (mobility kernel) emp-vs-sim with a
    row-occupancy-weighted total-variation distance.
(e) Proper-score primitives for the OOS gate (wired opt-in via
    `rankdiff_kalman --dist-scores`): ensemble CRPS (Gneiting & Raftery 2007),
    pooled PIT values, and predictive quantile coverage.  The pooled sim
    displacement distribution is the predictive law for an exchangeable cohort
    member (declared: unconditional/pooled, not per-entity conditional).

Usage (mirrors scorecard_bands.py):
  python llm_fitting/community_metrics.py facebook_a --top-k 3500 --temperament \
      --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long --stat-factor \
      --two-scale --mix-hetero [--reps 5] [--print-trans]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402


# --------------------------------------------------------------------------- #
# (a) community-canonical observables
# --------------------------------------------------------------------------- #
def log_rank_grid(kmax: int, n: int = 12) -> np.ndarray:
    """Log-spaced unique integer ranks 1..kmax."""
    g = np.unique(np.round(np.logspace(0, np.log10(max(kmax, 1)), n)).astype(int))
    return g[(g >= 1) & (g <= kmax)]


def rank_change_curve(top_ids: np.ndarray, r_grid: np.ndarray | None = None):
    """C(r): probability the occupant of rank r changes between consecutive
    weeks -- the coll{cr} statistic evaluated on a full log-spaced rank grid
    (Iniguez et al. 2022's rank-change probability)."""
    kmax = top_ids.shape[1]
    if r_grid is None:
        r_grid = log_rank_grid(kmax)
    out = np.full(r_grid.size, np.nan)
    for j, r in enumerate(r_grid):
        if r - 1 < kmax:
            prev, cur = top_ids[:-1, r - 1], top_ids[1:, r - 1]
            m = (prev >= 0) & (cur >= 0)
            if m.any():
                out[j] = float(np.mean(prev[m] != cur[m]))
    return r_grid, out


def flux_turnover(top_ids: np.ndarray, K: int) -> dict:
    """Rank flux F (mean weekly out-rate of the top-K list, = outfluxK) and
    rank turnover o_t = |cumulative distinct top-K occupants|/K with mean
    turnover rate o-dot = (o_{T-1} - o_0)/(T-1) (Iniguez et al. 2022)."""
    T = top_ids.shape[0]
    if top_ids.shape[1] < K or T < 2:
        return dict(F=np.nan, o=np.array([]), o_dot=np.nan)
    sets = [set(top_ids[t, :K]) - {-1} for t in range(T)]
    fl = [len(sets[t] - sets[t + 1]) / len(sets[t]) for t in range(T - 1) if sets[t]]
    seen: set = set()
    o = np.empty(T)
    for t in range(T):
        seen |= sets[t]
        o[t] = len(seen) / K
    o_dot = (o[-1] - o[0]) / (T - 1)
    return dict(F=float(np.mean(fl)) if fl else np.nan, o=o, o_dot=float(o_dot))


def rank_diversity(top_ids: np.ndarray, kmax: int | None = None) -> np.ndarray:
    """d(k): number of distinct occupants of rank k across the window,
    normalized by T (Cocho et al. 2015).  Cumulative and identity-based --
    orthogonal information to the per-week-pair collision rate."""
    T, K = top_ids.shape
    kmax = min(kmax or K, K)
    d = np.full(kmax, np.nan)
    for k in range(kmax):
        col = top_ids[:, k]
        valid = col[col >= 0]
        if valid.size:
            d[k] = len(set(valid.tolist())) / T
    return d


# --------------------------------------------------------------------------- #
# (b) ladder / concentration metrics
# --------------------------------------------------------------------------- #
def ladder_rmse(rs_emp: np.ndarray, rs_sim: np.ndarray, n_bins: int = 12) -> float:
    """RMSE between time-mean rank-size curves (log-values by rank), averaged
    within log-spaced rank bins so the head does not dominate mechanically.
    rs_*: (T, M) sorted per-week log-values (diagnostics' `ranksize`).

    LEVEL-SENSITIVE (measured 2026-07-06): the model removes the platform-wide
    level by design, so on panels with a secular level path this statistic is
    dominated by the un-modeled common level, not shape (comments, growing
    census: D_ladder 1.23 with a uniform-in-rank sign; FB Era A, declining:
    part of +0.4).  The level-ROBUST ladder statistics are the share-based
    ones -- share_curve / share_distance / top_share (shares normalize within
    week).  Interpret D_ladder only on level-stable panels or alongside them."""
    ce, cs = np.nanmean(rs_emp, axis=0), np.nanmean(rs_sim, axis=0)
    M = min(ce.size, cs.size)
    ce, cs = ce[:M], cs[:M]
    edges = np.unique(np.round(np.logspace(0, np.log10(M), n_bins + 1)).astype(int))
    diffs = []
    for a, b in zip(edges[:-1], edges[1:]):
        e, s = ce[a - 1:b], cs[a - 1:b]
        m = np.isfinite(e) & np.isfinite(s)
        if m.any():
            diffs.append(np.mean(e[m]) - np.mean(s[m]))
    return float(np.sqrt(np.mean(np.square(diffs)))) if diffs else np.nan


def ladder_drift(ranksize: np.ndarray, n_bins: int = 8) -> float:
    """Eulerian ladder STATIONARITY: RMS across log-rank bins of
    (last-third time-mean ladder - first-third time-mean ladder).  The card's
    15 rows are all Lagrangian, scale-free, or identity-based -- none tests
    whether the rank-size curve itself stays put; this does.  Computed
    identically for emp and sim; a sim initialized at the empirical week-0
    ladder should drift no more than the data does."""
    T, M = ranksize.shape
    if T < 6:
        return np.nan
    a = np.nanmean(ranksize[:T // 3], axis=0)
    b = np.nanmean(ranksize[2 * T // 3:], axis=0)
    edges = np.unique(np.round(np.logspace(0, np.log10(M), n_bins + 1)).astype(int))
    diffs = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        da, db = a[lo - 1:hi], b[lo - 1:hi]
        m = np.isfinite(da) & np.isfinite(db)
        if m.any():
            diffs.append(np.mean(db[m]) - np.mean(da[m]))
    return float(np.sqrt(np.mean(np.square(diffs)))) if diffs else np.nan


def top_share(ranksize: np.ndarray, k: int = 1) -> float:
    """Time-mean share of activity held by the top-k ranks within the
    recorded top-M (activity = expm1(X)).  S(1) is the sharpest single probe
    of the stationary head law: a process whose stationary cross-section is
    too wide at the head intermittently grows a runaway #1 and overshoots
    this badly (and seed-noisily) while every change-based card row stays
    blind to it."""
    act = np.expm1(ranksize)
    tot = np.nansum(act, axis=1)
    ok = tot > 0
    if not ok.any() or k > act.shape[1]:
        return np.nan
    return float(np.mean(np.nansum(act[ok, :k], axis=1) / tot[ok]))


def share_curve(ranksize: np.ndarray, k_grid: np.ndarray) -> np.ndarray:
    """Concentration curve S(k) = mean over weeks of the top-k share of
    activity within the recorded top-M (activity = expm1(X), X = log1p)."""
    act = np.expm1(ranksize)
    tot = np.nansum(act, axis=1)
    out = np.full(k_grid.size, np.nan)
    ok = tot > 0
    for j, k in enumerate(k_grid):
        if k <= act.shape[1]:
            out[j] = float(np.mean(np.nansum(act[ok, :k], axis=1) / tot[ok]))
    return out


def share_distance(rs_emp: np.ndarray, rs_sim: np.ndarray,
                   k_grid: np.ndarray | None = None) -> tuple[float, np.ndarray, np.ndarray, np.ndarray]:
    """D_share = max_k |S_sim(k) - S_emp(k)| over a log-spaced k grid."""
    M = min(rs_emp.shape[1], rs_sim.shape[1])
    if k_grid is None:
        k_grid = log_rank_grid(M, 10)
    se = share_curve(rs_emp[:, :M], k_grid)
    ss = share_curve(rs_sim[:, :M], k_grid)
    m = np.isfinite(se) & np.isfinite(ss)
    d = float(np.max(np.abs(se[m] - ss[m]))) if m.any() else np.nan
    return d, k_grid, se, ss


# --------------------------------------------------------------------------- #
# (c) rolling-origin variants of the period-0-anchored card rows
# --------------------------------------------------------------------------- #
def _scored_complete(values: np.ndarray, ranks: np.ndarray, score_k: int | None):
    """Replicate diagnostics' population filters exactly (score-k mean-rank
    filter, then complete columns) so rolling rows score the SAME population
    as the committed card."""
    if score_k is not None:
        with np.errstate(invalid="ignore"):
            rf = np.where(ranks > 0, ranks.astype(float), np.nan)
            mean_rank = np.nanmean(rf, axis=0)
        in_k = np.isfinite(mean_rank) & (mean_rank <= score_k)
        if in_k.sum() >= 10:
            values = values[:, in_k]
    obs_all = np.all(np.isfinite(values), axis=0)
    return values[:, obs_all] if obs_all.sum() >= 10 else values


def rolling_r2(V: np.ndarray, horizons=(1, 4, 13)) -> dict:
    """Rolling-origin R2(h): mean over origins t of corr(V[t], V[t+h])^2.
    The committed card row is the t=0 slice of this statistic."""
    T = V.shape[0]
    out = {}
    for h in horizons:
        vals = []
        for t in range(T - h):
            a, b = V[t], V[t + h]
            m = np.isfinite(a) & np.isfinite(b)
            if m.sum() > 5 and np.std(a[m]) > 1e-12 and np.std(b[m]) > 1e-12:
                vals.append(np.corrcoef(a[m], b[m])[0, 1] ** 2)
        out[f"R2_{h}"] = (float(np.mean(vals)), float(np.std(vals))) if vals else (np.nan, np.nan)
    return out


def rolling_pers(top_ids: np.ndarray, top_k: int, horizons=(1, 4, 13)) -> dict:
    """Rolling top-set retention: mean over origins t of
    |Top_k(t) & Top_k(t+h)|.  The committed card row is the t=0 slice."""
    T = top_ids.shape[0]
    sets = [set(top_ids[t, :top_k]) - {-1} for t in range(T)]
    out = {}
    for h in horizons:
        vals = [len(sets[t] & sets[t + h]) for t in range(T - h) if sets[t]]
        out[f"Pers{h}"] = (float(np.mean(vals)), float(np.std(vals))) if vals else (np.nan, np.nan)
    return out


# --------------------------------------------------------------------------- #
# (d) rank-band transition matrix (mobility kernel)
# --------------------------------------------------------------------------- #
def band_edges(K: int, base=(10, 50, 200, 1000)) -> list[int]:
    """Ascending band upper bounds ending at K (bands: 1..10, 11..50, ...)."""
    e = [b for b in base if b < K]
    return e + [K]


def transition_matrix(top_ids: np.ndarray, h: int,
                      edges: list[int] | None = None, K: int | None = None):
    """M[a, b] = P(rank band b at t+h | rank band a at t), averaged over all
    origins; final column = 'out' (absent from the recorded top-K at t+h).
    Returns (M row-stochastic, row_mass, labels)."""
    T, Kmax = top_ids.shape
    K = min(K or Kmax, Kmax)
    edges = edges or band_edges(K)
    nb = len(edges)
    labels = ([f"1-{edges[0]}"] +
              [f"{a + 1}-{b}" for a, b in zip(edges[:-1], edges[1:])])
    counts = np.zeros((nb, nb + 1))
    lo = 0
    band_of = np.empty(K, dtype=int)
    for bi, hi in enumerate(edges):
        band_of[lo:hi] = bi
        lo = hi
    for t in range(T - h):
        pos = {int(top_ids[t + h, r]): r for r in range(K - 1, -1, -1)
               if top_ids[t + h, r] >= 0}
        for r in range(K):
            i = int(top_ids[t, r])
            if i < 0:
                continue
            a = band_of[r]
            r2 = pos.get(i)
            counts[a, band_of[r2] if r2 is not None else nb] += 1
    row_mass = counts.sum(axis=1)
    M = np.divide(counts, row_mass[:, None], out=np.zeros_like(counts),
                  where=row_mass[:, None] > 0)
    return M, row_mass, labels + ["out"]


def transition_distance(M_emp: np.ndarray, M_sim: np.ndarray,
                        row_mass_emp: np.ndarray) -> float:
    """Empirical-occupancy-weighted mean total-variation distance between
    the transition-matrix rows."""
    w = row_mass_emp / max(row_mass_emp.sum(), 1.0)
    tv = 0.5 * np.abs(M_emp - M_sim).sum(axis=1)
    return float(np.sum(w * tv))


# --------------------------------------------------------------------------- #
# (e) proper-score primitives (OOS gate, opt-in wiring in rankdiff_kalman)
# --------------------------------------------------------------------------- #
def crps_sample(ens: np.ndarray, obs: np.ndarray) -> np.ndarray:
    """Ensemble CRPS per observation: CRPS(F, y) = E|X-y| - 0.5 E|X-X'|
    (Gneiting & Raftery 2007), O(n log n) via sorted prefix sums."""
    xs = np.sort(np.asarray(ens, dtype=float))
    n = xs.size
    if n == 0:
        return np.full(np.asarray(obs).size, np.nan)
    obs = np.atleast_1d(np.asarray(obs, dtype=float))
    cs = np.cumsum(xs)
    idx = np.searchsorted(xs, obs, side="right")
    below = np.where(idx > 0, cs[np.maximum(idx - 1, 0)], 0.0)
    sum_abs = obs * idx - below + (cs[-1] - below) - obs * (n - idx)
    i = np.arange(1, n + 1)
    half_gmd = float(np.sum((2 * i - n - 1) * xs)) / (n * n)  # 0.5 E|X-X'|
    return sum_abs / n - half_gmd


def crps_mean(ens: np.ndarray, obs: np.ndarray) -> float:
    v = crps_sample(ens, obs)
    return float(np.mean(v)) if np.isfinite(v).any() else np.nan


def pit_values(ens: np.ndarray, obs: np.ndarray) -> np.ndarray:
    """Mid-rank PIT of each observation under the pooled ensemble law:
    (#{x < y} + 0.5 #{x = y}) / n.  Uniform(0,1) under a correct forecast."""
    xs = np.sort(np.asarray(ens, dtype=float))
    n = xs.size
    obs = np.atleast_1d(np.asarray(obs, dtype=float))
    lo = np.searchsorted(xs, obs, side="left")
    hi = np.searchsorted(xs, obs, side="right")
    return (lo + 0.5 * (hi - lo)) / max(n, 1)


def quantile_coverage(ens: np.ndarray, obs: np.ndarray,
                      qs=(0.1, 0.5, 0.9)) -> dict:
    """Share of observations at or below each predictive quantile -- should
    match the nominal level under a calibrated forecast.  This is PREDICTIVE
    coverage, distinct from the gate's model-median-in-empirical-CI check."""
    ens = np.asarray(ens, dtype=float)
    obs = np.atleast_1d(np.asarray(obs, dtype=float))
    if ens.size == 0 or obs.size == 0:
        return {q: np.nan for q in qs}
    return {q: float(np.mean(obs <= np.quantile(ens, q))) for q in qs}


# --------------------------------------------------------------------------- #
# driver (mirrors scorecard_bands.py)
# --------------------------------------------------------------------------- #
def _compute_all(values, ranks, top_ids, ranksize, top_k, score_k):
    """All in-sample community metrics for one (emp or sim) structure set."""
    out = {}
    K = score_k or top_ids.shape[1]
    ft = flux_turnover(top_ids, K)
    out["F"], out["o_dot"] = ft["F"], ft["o_dot"]
    out["_C_grid"], out["_C"] = rank_change_curve(top_ids)
    out["_d"] = rank_diversity(top_ids)
    V = _scored_complete(values, ranks, score_k)
    out["_rollR2"] = rolling_r2(V)
    out["_rollPers"] = rolling_pers(top_ids, top_k)
    out["_trans"] = {h: transition_matrix(top_ids, h, K=K)
                     for h in (1, 4, 13) if h < top_ids.shape[0]}
    out["_ranksize"] = ranksize
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("platform")
    ap.add_argument("--top-k", type=int, default=None)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--temperament", action="store_true")
    ap.add_argument("--min-knot-entities", type=int, default=None)
    ap.add_argument("--md-lags", type=int, default=None)
    ap.add_argument("--t-tails", action="store_true")
    ap.add_argument("--md-vr", action="store_true")
    ap.add_argument("--md-vr-long", action="store_true")
    ap.add_argument("--stat-factor", action="store_true")
    ap.add_argument("--two-scale", action="store_true")
    ap.add_argument("--mix-hetero", action="store_true")
    ap.add_argument("--spec-b", action="store_true")
    ap.add_argument("--print-trans", action="store_true",
                    help="print the h=1 emp/sim transition matrices")
    a = ap.parse_args()

    cfg = mrd.PLATFORMS[a.platform]
    df = mrd.load_panel(cfg)
    if a.top_k:
        df = mrd.restrict_universe(df, a.top_k, buffer_mult=4)
    score_k = df.attrs.get("score_k")
    T = int(df["period"].max()) + 1
    mean_n = df.groupby("period").size().mean()
    top_k = max(10, int(round(0.01 * (score_k if score_k else mean_n))))
    cov_lang = ("of tracked activity" if a.platform.startswith("facebook")
                or a.platform.startswith("instagram") else "platform-wide (census)")
    print(f"=== community metrics: {a.platform} T={T} score_k={score_k} "
          f"reps={a.reps}  [shares {cov_lang}] ===")

    ev, er, et, ers = mrd.empirical_structures(df, top_k, topid_k=score_k)
    emp = _compute_all(ev, er, et, ers, top_k, score_k)
    emp_card = mrd.diagnostics(ev, er, et, ers, top_k, score_k=score_k)

    sigma_obs_fix = None
    if a.spec_b:
        import spec_b_sigma_obs as sb
        daily_path = cfg.get("daily_path",
                             sb.DAILY_PATH if a.platform == "reddit" else None)
        daily = sb.load_daily(set(df["entity_id"].unique()), path=daily_path,
                              day_guard=cfg.get("day_guard", False))
        cur = sb.spec_b_curve(df, daily)
        sigma_obs_fix = (cur["z"], cur["sigma_obs"])
    p = mrd.estimate(df, temper=a.temperament, min_knot_n=a.min_knot_entities,
                     md_lags=a.md_lags, t_tails=a.t_tails,
                     sigma_obs_fix=sigma_obs_fix, md_vr=a.md_vr,
                     md_vr_long=a.md_vr_long, stat_factor=a.stat_factor,
                     two_scale=a.two_scale, mix_hetero=a.mix_hetero)

    sims, lad, shr, drf, ts1 = [], [], [], [], []
    for s in range(a.reps):
        sim = mrd.simulate(p, T, seed=s, kappa=None if a.md_lags else 0.15,
                           top_record=score_k)
        tv, tr, ti, rs = mrd._sim_struct(sim)
        sims.append(_compute_all(tv, tr, ti, rs, top_k, score_k))
        lad.append(ladder_rmse(ers, rs))
        shr.append(share_distance(ers, rs)[0])
        drf.append(ladder_drift(rs))
        ts1.append(top_share(rs, 1))
        print(f"  sim rep {s + 1}/{a.reps} done")

    def sim_ms(key):
        v = np.array([s[key] for s in sims], dtype=float)
        return np.nanmean(v), np.nanstd(v)

    print("\n  FLUX / TURNOVER @ top-K (Iniguez et al. 2022 naming; F = outfluxK)")
    for key, label in (("F", "rank flux F (weekly out-rate)"),
                       ("o_dot", "turnover rate o-dot")):
        m, sd = sim_ms(key)
        print(f"    {label:<34}emp {emp[key]:.4f}   sim {m:.4f} ± {sd:.4f}")

    print("\n  RANK-CHANGE CURVE C(r)  (occupant-change probability by rank)")
    grid = emp["_C_grid"]
    Cs = np.array([np.interp(grid, s["_C_grid"], s["_C"]) for s in sims])
    print(f"    {'r':>6}{'emp':>9}{'sim':>9}{'simSD':>9}")
    for j, r in enumerate(grid):
        print(f"    {r:>6}{emp['_C'][j]:>9.3f}{np.nanmean(Cs[:, j]):>9.3f}"
              f"{np.nanstd(Cs[:, j]):>9.3f}")

    print("\n  RANK DIVERSITY d(k)  (distinct occupants of rank k per week)")
    kmax = min(len(emp["_d"]), min(len(s["_d"]) for s in sims))
    dg = log_rank_grid(kmax, 10)
    Ds = np.array([[s["_d"][k - 1] for k in dg] for s in sims])
    print(f"    {'k':>6}{'emp':>9}{'sim':>9}{'simSD':>9}")
    for j, k in enumerate(dg):
        print(f"    {k:>6}{emp['_d'][k - 1]:>9.3f}{np.nanmean(Ds[:, j]):>9.3f}"
              f"{np.nanstd(Ds[:, j]):>9.3f}")

    print(f"\n  LADDER (rank-size / concentration; shares {cov_lang})")
    print(f"    D_ladder (RMSE of time-mean log-value, log-rank bins) = "
          f"{np.nanmean(lad):.4f} ± {np.nanstd(lad):.4f}")
    print(f"    D_share  (max_k |S_sim(k) - S_emp(k)|)                = "
          f"{np.nanmean(shr):.4f} ± {np.nanstd(shr):.4f}")
    print(f"    ladder drift (last vs first third, RMS over bins)     : "
          f"emp {ladder_drift(ers):.4f}   sim {np.nanmean(drf):.4f} ± {np.nanstd(drf):.4f}")
    print(f"    S(1) top-1 activity share (stationary head law)       : "
          f"emp {top_share(ers, 1):.4f}   sim {np.nanmean(ts1):.4f} ± {np.nanstd(ts1):.4f}")

    print("\n  ROLLING-ORIGIN variants (mean over origins ± SD; committed "
          "single-origin card row in parens)")
    for fam, emp_key in (("_rollR2", "R2_{h}"), ("_rollPers", "Pers{h}")):
        for h in (1, 4, 13):
            key = emp_key.format(h=h)
            if key not in emp[fam]:
                continue
            em, es = emp[fam][key]
            sv = np.array([s[fam][key][0] for s in sims], dtype=float)
            card = emp_card.get(key, np.nan)
            print(f"    {key:<8} emp {em:8.3f} ± {es:5.3f}   "
                  f"sim {np.nanmean(sv):8.3f} ± {np.nanstd(sv):5.3f}   "
                  f"(card emp: {card:.3f})")

    print("\n  TRANSITION MATRIX (rank-band mobility kernel; "
          "occupancy-weighted TV distance)")
    for h in sorted(emp["_trans"]):
        Me, we, lab = emp["_trans"][h]
        ds = [transition_distance(Me, s["_trans"][h][0], we)
              for s in sims if h in s["_trans"]]
        print(f"    h={h:<3} TV(emp, sim) = {np.nanmean(ds):.4f} ± {np.nanstd(ds):.4f}")
    if a.print_trans and 1 in emp["_trans"]:
        Me, we, lab = emp["_trans"][1]
        Ms = np.nanmean(np.stack([s["_trans"][1][0] for s in sims]), axis=0)
        for name, M in (("EMPIRICAL", Me), ("SIMULATED (mean)", Ms)):
            print(f"\n    {name} h=1:  rows = band at t, cols = band at t+1")
            print("      " + "".join(f"{c:>10}" for c in lab))
            for i, r in enumerate(lab[:-1]):
                print(f"      {r:<8}" + "".join(f"{M[i, j]:>10.3f}"
                                                for j in range(M.shape[1])))


if __name__ == "__main__":
    main()
