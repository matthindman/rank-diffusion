#!/usr/bin/env python3
"""P3 + P6 (2026-07-05, review C3/F4/F5/B5/B15/R7): uncertainty-aware scorecard.

Additive diagnostics -- the 15-row card stays the visual; this adds:
  (a) EMPIRICAL bands: entity bootstrap (resample tracked columns jointly, so
      cross-metric covariance is captured) for the median/entity-pooled rows
      (VR/ACF/RACF/R2/dRank); MOVING-BLOCK bootstrap over weeks for the
      time-mean rows (coll*, outfluxK, return4K).  Pers{h} is a single-origin
      set overlap -- no empirical sampling band exists; it gets a sim MC band
      only (declared).
  (b) SIM MC bands: reps >= 20 (seeds 0..reps-1), per-metric mean/SD and the
      full MC covariance.
  (c) Omnibus distance Q = d' Omega^+ d over the 15 card metrics,
      d = m_sim - m_emp, Omega = Cov_boot(emp) + Cov_MC(sim)/reps (shrunk 50%
      toward its diagonal for conditioning; pseudo-inverse).  Q is a
      covariance-weighted DESCRIPTIVE distance -- the chi-square df=15 scale
      is a reference point, not a formal test (moments overlap in windows).

P6 (--censor): population-matched scoring -- rank-based weekly censoring of
the simulated tracked population at the empirical presence fraction: at each
week t, the same share of tracked columns that is unobserved empirically is
NaN'd in the sim, worst simulated rank first (absence = below the observation
floor).  Zero new dynamics; the complete-column scoring filter then applies
the SAME quiet-entity selection to sim as to data.  Pre-registered prediction
(2m/R7): the comments VR4/8 residual narrows.

Usage:
  python llm_fitting/scorecard_bands.py reddit_comments --top-k 12500 \
      --temperament --min-knot-entities 8 --md-lags 6 --t-tails --md-vr-long \
      --stat-factor --two-scale --mix-hetero [--censor] [--reps 20] [--boot 100]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

CARD = (["VR2", "VR4", "VR8", "VR13", "ACF1", "ACF2", "RACF1", "RACF4",
         "RACF13", "R2_1", "R2_4", "R2_13", "Pers1", "Pers4", "Pers13"])
ENT_BOOT_KEYS = ["VR2", "VR4", "VR8", "VR13", "ACF1", "ACF2", "RACF1", "RACF4",
                 "RACF13", "R2_1", "R2_4", "R2_13", "dRank1", "dRank4", "dRank13"]
BLK_KEYS = [f"coll{c}" for c in mrd.COLLISION_RANKS] + ["outfluxK", "return4K"]


def _week_series(top_ids: np.ndarray, K: int, ret_h: int = 4):
    """Per-week-pair series behind the time-mean churn metrics (for the
    moving-block bootstrap): collision indicators per rank, outflux and
    return rates per week."""
    T = top_ids.shape[0]
    out = {}
    for cr in mrd.COLLISION_RANKS:
        if cr - 1 < top_ids.shape[1]:
            prev, cur = top_ids[:-1, cr - 1], top_ids[1:, cr - 1]
            m = (prev >= 0) & (cur >= 0)
            out[f"coll{cr}"] = np.where(m, (prev != cur).astype(float), np.nan)
    if K and top_ids.shape[1] >= K:
        sets = [set(top_ids[t, :K]) - {-1} for t in range(T)]
        outr = np.full(T - 1, np.nan)
        back = np.full(T - 1, np.nan)
        for t in range(T - 1):
            if not sets[t]:
                continue
            dropped = sets[t] - sets[t + 1]
            outr[t] = len(dropped) / len(sets[t])
            if t + ret_h < T and dropped:
                back[t] = len(dropped & sets[t + ret_h]) / len(dropped)
        out["outfluxK"], out["return4K"] = outr, back
    return out


def _block_boot_joint(ws: dict, B: int, L: int, rng) -> pd.DataFrame:
    """Moving-block bootstrap over weeks, SAME block indices across metrics
    per draw -- cross-metric covariance within the churn/boundary family is
    real, which the per-block Q decomposition needs."""
    keys = list(ws.keys())
    n = min(len(v) for v in ws.values())
    if n <= L:
        return pd.DataFrame({k: np.full(B, np.nanmean(v)) for k, v in ws.items()})
    n_blocks = int(np.ceil(n / L))
    out = {k: np.empty(B) for k in keys}
    for b in range(B):
        starts = rng.integers(0, n - L + 1, size=n_blocks)
        idx = np.concatenate([np.arange(s, s + L) for s in starts])[:n]
        for k in keys:
            out[k][b] = np.nanmean(ws[k][:n][idx])
    return pd.DataFrame(out)


def entity_boot_draws(values, ranks, top_ids, ranksize, top_k, score_k,
                      B, rng) -> pd.DataFrame:
    n = values.shape[1]
    rows = []
    for b in range(B):
        idx = rng.integers(0, n, size=n)
        d = mrd.diagnostics(values[:, idx], ranks[:, idx], top_ids, ranksize,
                            top_k, score_k=None)   # score_k pre-applied
        rows.append({k: d.get(k, np.nan) for k in ENT_BOOT_KEYS})
    return pd.DataFrame(rows)


def _apply_score_k(values, ranks, score_k):
    """Pre-apply diagnostics' score_k population filter once, so bootstrap
    draws resample the SCORED population (mirrors diagnostics exactly)."""
    if score_k is None:
        return values, ranks
    with np.errstate(invalid="ignore"):
        rf = np.where(ranks > 0, ranks.astype(float), np.nan)
        mean_rank = np.nanmean(rf, axis=0)
    in_k = np.isfinite(mean_rank) & (mean_rank <= score_k)
    if in_k.sum() >= 10:
        return values[:, in_k], ranks[:, in_k]
    return values, ranks


def censor_to_presence(tvals, tranks, absent_frac):
    """P6: rank-based weekly censoring at the empirical presence fraction.
    absent_frac[t] = share of the empirical scored population unobserved in
    week t; the worst-ranked that share of sim tracked columns is censored."""
    tv, tr = tvals.copy(), tranks.copy()
    T, n = tv.shape
    for t in range(min(T, len(absent_frac))):
        k = int(round(absent_frac[t] * n))
        if k <= 0:
            continue
        r = np.where(tr[t] > 0, tr[t], -np.inf)     # already-absent stay absent
        fin = np.isfinite(tv[t]) & (tr[t] > 0)
        if fin.sum() <= k:
            continue
        order = np.argsort(r)                        # ascending; worst = end
        cut = order[-k:]
        tv[t, cut] = np.nan
        tr[t, cut] = 0
    return tv, tr


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("platform")
    ap.add_argument("--top-k", type=int, default=None)
    ap.add_argument("--reps", type=int, default=20)
    ap.add_argument("--boot", type=int, default=100)
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
    ap.add_argument("--censor", action="store_true",
                    help="P6 population-matched scoring (rank-based weekly "
                         "censoring at the empirical presence fraction)")
    a = ap.parse_args()

    cfg = mrd.PLATFORMS[a.platform]
    df = mrd.load_panel(cfg)
    if a.top_k:
        df = mrd.restrict_universe(df, a.top_k, buffer_mult=4)
    score_k = df.attrs.get("score_k")
    T = int(df["period"].max()) + 1
    mean_n = df.groupby("period").size().mean()
    top_k = max(10, int(round(0.01 * (score_k if score_k else mean_n))))
    print(f"=== scorecard bands: {a.platform} T={T} score_k={score_k} "
          f"reps={a.reps} boot={a.boot} censor={a.censor} ===")

    ev, er, et, ers = mrd.empirical_structures(df, top_k, topid_k=score_k)
    emp = mrd.diagnostics(ev, er, et, ers, top_k, score_k=score_k)

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

    # empirical weekly absence among the scored population (for --censor)
    ev_s, er_s = _apply_score_k(ev, er, score_k)
    absent_frac = 1.0 - np.isfinite(ev_s).mean(axis=1)

    sims = []
    for s in range(a.reps):
        sim = mrd.simulate(p, T, seed=s, kappa=None if a.md_lags else 0.15,
                           top_record=score_k)
        tv, tr, ti, rs = mrd._sim_struct(sim)
        if a.censor:
            tv, tr = censor_to_presence(tv, tr, absent_frac)
        sims.append(mrd.diagnostics(tv, tr, ti, rs, top_k, score_k=score_k))
        print(f"  sim rep {s + 1}/{a.reps} done")
    keys = [k for k in emp if not k.startswith("_")]
    S = pd.DataFrame([{k: d.get(k, np.nan) for k in keys} for d in sims])
    sim_mean, sim_sd = S.mean(), S.std(ddof=1)

    rng = np.random.default_rng(0)
    print("  entity bootstrap ...")
    EB = entity_boot_draws(ev_s, er_s, et, ers, top_k, score_k, a.boot, rng)
    print("  moving-block bootstrap (weeks) ...")
    ws = _week_series(et, score_k or 0)
    L = max(4, min(13, (T - 1) // 5))
    BB = _block_boot_joint(ws, a.boot, L, rng)

    # ---- table -------------------------------------------------------------
    def band(k):
        src = EB if k in EB.columns else (BB if k in BB.columns else None)
        if src is None or src[k].isna().all():
            return (np.nan, np.nan)
        return tuple(np.nanpercentile(src[k], [2.5, 97.5]))

    print(f"\n  {'metric':<10}{'emp':>8}{'emp 95% band':>18}{'sim':>8}"
          f"{'simSD':>8}{'diff':>8}{'z':>7}")
    zrows = {}
    for k in keys:
        lo, hi = band(k)
        e, s, sd_mc = emp.get(k, np.nan), sim_mean.get(k, np.nan), sim_sd.get(k, np.nan)
        emp_sd = (np.nanstd(EB[k]) if k in EB.columns
                  else np.nanstd(BB[k]) if k in BB.columns else 0.0)
        tot_sd = float(np.sqrt(emp_sd ** 2 + (sd_mc ** 2) / a.reps)) \
            if np.isfinite(sd_mc) else emp_sd
        z = (s - e) / tot_sd if tot_sd > 1e-12 else np.nan
        zrows[k] = z
        btxt = f"[{lo:7.3f},{hi:7.3f}]" if np.isfinite(lo) else "     (MC only)   "
        print(f"  {k:<10}{e:>8.3f}{btxt:>18}{s:>8.3f}{sd_mc:>8.3f}"
              f"{s - e:>+8.3f}{z:>7.1f}")

    # ---- omnibus Q over the 15 card metrics ---------------------------------
    d = np.array([sim_mean.get(k, np.nan) - emp.get(k, np.nan) for k in CARD])
    ok = np.isfinite(d)
    # emp covariance: entity-boot rows jointly; Pers rows -> zero emp var
    C_emp = np.zeros((len(CARD), len(CARD)))
    eb_idx = [i for i, k in enumerate(CARD) if k in EB.columns]
    sub = EB[[CARD[i] for i in eb_idx]].to_numpy()
    C_sub = np.cov(sub, rowvar=False)
    for a_, i in enumerate(eb_idx):
        for b_, j in enumerate(eb_idx):
            C_emp[i, j] = C_sub[a_, b_]
    C_mc = np.cov(S[CARD].to_numpy(), rowvar=False) / a.reps
    Om = C_emp + C_mc
    Om = 0.5 * Om + 0.5 * np.diag(np.diag(Om))          # 50% diagonal shrinkage
    Om = Om[np.ix_(ok, ok)]
    dd = d[ok]
    Q = float(dd @ np.linalg.pinv(Om, rcond=1e-10) @ dd)
    print(f"\n  omnibus Q = {Q:.1f} over {ok.sum()} card moments "
          f"(chi2 df={ok.sum()} reference: mean {ok.sum()}, "
          f"p95 ~ {ok.sum() + 1.645 * np.sqrt(2 * ok.sum()):.0f}) -- descriptive")

    # ---- Q block decomposition (residual LOCALIZATION, review B5) ----------
    # Each block gets its own Omega from whichever bootstrap covers it
    # (entity boot: VR/ACF/RACF/R2; joint block boot: churn/boundary; Pers:
    # MC-only, declared harsh). Q_b/df is the comparable per-block scale.
    BLOCKS = [
        ("VR", ["VR2", "VR4", "VR8", "VR13"]),
        ("ACF/RACF", ["ACF1", "ACF2", "RACF1", "RACF4", "RACF13"]),
        ("R2", ["R2_1", "R2_4", "R2_13"]),
        ("Pers (MC-only)", ["Pers1", "Pers4", "Pers13"]),
        ("churn", [f"coll{c}" for c in mrd.COLLISION_RANKS]),
        ("boundary", ["outfluxK", "return4K"]),
    ]
    print(f"\n  Q by block:   {'block':<16}{'df':>4}{'Q_b':>10}{'Q_b/df':>9}")
    for name, keys_b in BLOCKS:
        kb = [k for k in keys_b
              if np.isfinite(emp.get(k, np.nan)) and np.isfinite(sim_mean.get(k, np.nan))]
        if not kb:
            continue
        db = np.array([sim_mean[k] - emp[k] for k in kb])
        src = EB if kb[0] in EB.columns else (BB if kb[0] in BB.columns else None)
        C_e = (np.atleast_2d(np.cov(src[kb].to_numpy(), rowvar=False))
               if src is not None else np.zeros((len(kb), len(kb))))
        C_m = np.atleast_2d(np.cov(S[kb].to_numpy(), rowvar=False)) / a.reps
        Ob = C_e + C_m
        Ob = 0.5 * Ob + 0.5 * np.diag(np.diag(Ob))
        Qb = float(db @ np.linalg.pinv(Ob, rcond=1e-10) @ db)
        print(f"                {name:<16}{len(kb):>4}{Qb:>10.1f}{Qb / len(kb):>9.1f}")
    npass, ntot, churn = mrd._score(emp, sim_mean.to_dict(), top_k)
    print(f"  v4.3-style card: {npass}/{ntot}  churn err {churn:.3f}  "
          f"(threshold card, for continuity)")


if __name__ == "__main__":
    main()
