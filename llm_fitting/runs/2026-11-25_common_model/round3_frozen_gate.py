#!/usr/bin/env python3
"""Exact Frozen Gate v1 Evaluation of Common-S2 (11 scalars) and Common-S3 (14-15 scalars).

Imports and calls the EXACT frozen Gate v1 scoring machinery from llm_fitting/rankdiff_kalman.py:
  - rk._gate_windows(T, n_splits=5) -> identical split calendar & test_len
  - rk._rank_dist(R, hor, cap=100) -> identical |Delta rank| pooled over top-200 cohort starting <=100
    and per-entity median Pearson RACF1
  - rk._collisions_from_topids(topids, (1, 5, 20)) -> identical collision rates
  - rk._moments & rk.MOM_FLOOR & rk._boot_ci(B=2000) -> identical relative-error & CI coverage
  - Historical-mobility baseline = train-window movement (exact match to rk.oos_gate!)

Saves full per-split, per-moment breakdowns for transparent comparison against recorded
Model A and movinghome_vp rows in GATE_LEDGER.md.
Runs 100% sandboxed on DEVELOPMENT caches only. Zero sealed data contact.
"""
from __future__ import annotations

import gc
import json
import math
import sys
from pathlib import Path

import numpy as np
from scipy.stats import wasserstein_distance

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import common_rankdiff as cr  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402

CACHE_DIR = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
OUT_DIR = Path(__file__).resolve().parent

PANELS = {
    "facebook_a": dict(spec_b=True),
    "instagram_hm": dict(spec_b=False),
    "reddit_comments_ext": dict(spec_b=False),
    "reddit_submissions_long": dict(spec_b=False),
}


def load_cached(panel: str):
    d = np.load(CACHE_DIR / f"{panel}.npz", allow_pickle=True)
    data = {k: d[k] for k in d.files}
    sb = None
    sb_path = CACHE_DIR / f"{panel}_spec_b.json"
    if sb_path.exists():
        js = json.loads(sb_path.read_text())
        sb = (np.array(js["z"], float), np.array(js["sigma_obs"], float))
    return data, sb


def emp_dist_from_R(R_win: np.ndarray, horizons: list[int], coll_ranks=(1, 5, 20), cohort_k: int = 200):
    """Exact matrix counterpart of rankdiff_kalman.emp_dist over a 0-based window R_win (T x N)."""
    T, N = R_win.shape
    coh = np.flatnonzero(np.isfinite(R_win[0]) & (R_win[0] <= cohort_k))
    R_coh = R_win[:, coh]
    dist, racf1 = rk._rank_dist(R_coh, horizons, cap=100)
    KT = max(coll_ranks)
    top_ids = np.full((T, KT), -1, dtype=np.int64)
    for t in range(T):
        ok = np.flatnonzero(np.isfinite(R_win[t]))
        if ok.size:
            ord_ok = ok[np.argsort(R_win[t, ok], kind="stable")]
            k_fill = min(KT, ord_ok.size)
            top_ids[t, :k_fill] = ord_ok[:k_fill]
    colls = rk._collisions_from_topids(top_ids, coll_ranks)
    return dist, racf1, {c: colls.get(f"coll{c}", np.nan) for c in coll_ranks}


def sim_dist_common(
    filt: dict,
    p: cr.CommonParams,
    T_sim: int,
    horizons: list[int],
    reps: int = 20,
    coll_ranks=(1, 5, 20),
    cohort_k: int = 200,
    seed_offset: int = 0,
):
    """Exact conditional Gate v1 simulation matching rankdiff_kalman.sim_dist_cond conventions:
    - Step 1 is week T0 (first held-out week), cohort = top-200 at step 1 (matching sim_cohort_conditional)
    - Displacements pooled across all `reps` before taking medians; RACF1 = median across reps;
      collisions = mean across reps using slot entity IDs (tracking rebirths!).
    """
    N = filt["L_hat"].size
    use_4 = "P4" in filt
    if use_4:
        chol4 = np.linalg.cholesky(filt["P4"] + 1e-10 * np.eye(4)[None, :, :])
        has_d = p.decline_var > 0.0
        has_s = (p.surge_lam > 0.0) and (p.surge_mu > 0.0) and (p.surge_rho > 0.0)
        sd_d = math.sqrt(p.decline_var) if has_d else 0.0
        s_mean = cr._surge_mean(p.surge_lam, p.surge_mu, p.surge_rho) if has_s else 0.0
        s_sc = filt.get("s_scale", np.ones(N))
    else:
        c11 = np.sqrt(np.maximum(filt["P11"], 1e-10))
        c21 = filt["P12"] / c11
        c22 = np.sqrt(np.maximum(filt["P22"] - c21 * c21, 1e-10))

    KT = max(coll_ranks)
    pooled = {h: [] for h in horizons}
    racfs = []
    colls = {c: [] for c in coll_ranks}

    for s in range(reps):
        rng = np.random.default_rng(seed_offset + s)
        if use_4:
            draw = np.einsum("nij,nj->ni", chol4, rng.standard_normal((N, 4)))
            L_i = filt["L_hat"] + draw[:, 0]
            xi_i = filt["xi_hat"] + draw[:, 1]
            d_i = np.clip(filt["d_hat"] + draw[:, 2], -2.8 * sd_d, 2.8 * sd_d) if has_d else None
            if has_s:
                s_obs_raw = filt["s_hat"] + draw[:, 3]
                s_i = np.maximum(s_obs_raw / np.maximum(s_sc, 1e-4) + s_mean, 0.0)
            else:
                s_i = None
        else:
            z1, z2 = rng.standard_normal(N), rng.standard_normal(N)
            L_i = filt["L_hat"] + c11 * z1
            xi_i = filt["xi_hat"] + c21 * z1 + c22 * z2
            d_i, s_i = None, None

        if p.s_vol > 0.02:
            lv = np.log(filt["v_hat"]) - 0.5 * (filt["post_s_v"] ** 2) + filt["post_s_v"] * np.clip(rng.standard_normal(N), -2.0, 2.0)
            v_i = np.exp(np.clip(lv, -2.5 * p.s_vol, 2.5 * p.s_vol))
        else:
            v_i = np.ones(N)

        st = cr.CommonState(p, L_i, rng, xi0=xi_i, v0=v_i, d0=d_i, s0=s_i)
        cranks = np.full((T_sim, cohort_k), np.nan, dtype=float)
        topids = np.full((T_sim, KT), -1, dtype=np.int64)
        coh_slots = None
        coh_ids = None

        for t in range(T_sim):
            xt = st.step(apply_absence=True)
            ok = np.flatnonzero(np.isfinite(xt))
            if not ok.size:
                continue
            ord_ok = ok[np.argsort(-xt[ok], kind="stable")]
            rank = np.full(N, np.nan, dtype=float)
            rank[ord_ok] = np.arange(1, ord_ok.size + 1, dtype=float)
            if coh_slots is None:
                k_sel = min(cohort_k, ord_ok.size)
                coh_slots = ord_ok[:k_sel].copy()
                coh_ids = st.ids[coh_slots].copy()
            alive = st.ids[coh_slots] == coh_ids
            cr_t = rank[coh_slots]
            cr_t[~alive] = np.nan
            cranks[t, :coh_slots.size] = cr_t
            k_top = min(KT, ord_ok.size)
            topids[t, :k_top] = st.ids[ord_ok[:k_top]]

        d, rf = rk._rank_dist(cranks, horizons, cap=100)
        for h in horizons:
            pooled[h].append(d[h])
        racfs.append(rf)
        cc = rk._collisions_from_topids(topids, coll_ranks)
        for c in coll_ranks:
            colls[c].append(cc.get(f"coll{c}", np.nan))

    dist = {h: (np.concatenate(pooled[h]) if pooled[h] else np.array([])) for h in horizons}
    return dist, float(np.nanmedian(racfs)), {c: float(np.nanmean(colls[c])) for c in coll_ranks}


def run_panel(pn: str, cfg: dict, n_splits: int = 5, reps: int = 20, boot: int = 2000) -> dict:
    print(f"\n================ EXACT FROZEN GATE v1: {pn} ================", flush=True)
    data, sb = load_cached(pn)
    X = data["X"].astype(np.float64)
    R = data["R"].astype(np.float64)
    floors = data["floors"].astype(np.float64)
    K = int(data["K"])
    T, N = X.shape

    origins, test_len = rk._gate_windows(T, n_splits)
    hor = [h for h in (1, 4, 13) if h < test_len]
    keys = [f"dRank{h}" for h in hor] + ["RACF1"] + [f"coll{c}" for c in (1, 5, 20)]
    print(f"  T={T} K={K} test_len={test_len} origins={origins} hor={hor}", flush=True)

    splits_out = []
    for si, T0 in enumerate(origins):
        X_tr, R_tr, fl_tr = X[:T0], R[:T0], floors[:T0]
        R_te = R[T0:T0 + test_len]

        # Re-rank strictly inside train+test on the same entities (exact match to rk._split_panel)
        ed, erf, ec = emp_dist_from_R(R_te, hor)
        td, trf, tc = emp_dist_from_R(R_tr, hor)
        em = rk._moments(ed, erf, ec, hor)
        bm = rk._moments(td, trf, tc, hor)

        p2 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=sb if cfg["spec_b"] else None, refine_drift=True, asymmetry=False)
        p3 = cr.estimate_common(X_tr, R_tr, K=K, floors=fl_tr, spec_b=sb if cfg["spec_b"] else None, refine_drift=True, asymmetry=True)
        f2 = cr.filter_endpoints(X_tr, R_tr, p2, floors_tr=fl_tr)
        f3 = cr.filter_endpoints(X_tr, R_tr, p3, floors_tr=fl_tr)

        sd2, srf2, sc2 = sim_dist_common(f2, p2, test_len, hor, reps=reps, seed_offset=1000 * si)
        sd3, srf3, sc3 = sim_dist_common(f3, p3, test_len, hor, reps=reps, seed_offset=1000 * si)
        sm2 = rk._moments(sd2, srf2, sc2, hor)
        sm3 = rk._moments(sd3, srf3, sc3, hor)

        kk = [k for k in keys if np.isfinite(em.get(k, np.nan)) and abs(em[k]) >= rk.MOM_FLOOR]
        be_vec = {k: abs(bm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk if np.isfinite(bm.get(k, np.nan))}
        e2_vec = {k: abs(sm2[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk if np.isfinite(sm2.get(k, np.nan))}
        e3_vec = {k: abs(sm3[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk if np.isfinite(sm3.get(k, np.nan))}
        be = float(np.mean(list(be_vec.values())))
        e2 = float(np.mean(list(e2_vec.values())))
        e3 = float(np.mean(list(e3_vec.values())))

        h0 = hor[0]
        lo, hi = rk._boot_ci(ed[h0], B=boot)
        in_ci_2 = bool(np.isfinite(lo) and lo <= sm2[f"dRank{h0}"] <= hi)
        in_ci_3 = bool(np.isfinite(lo) and lo <= sm3[f"dRank{h0}"] <= hi)
        w1_2 = float(wasserstein_distance(ed[h0], sd2[h0])) if ed[h0].size and sd2[h0].size else np.nan
        w1_3 = float(wasserstein_distance(ed[h0], sd3[h0])) if ed[h0].size and sd3[h0].size else np.nan

        print(f"    T0={T0:3d} | base={be:.3f} | S2={e2:.3f} (ci={in_ci_2}, w1={w1_2:.1f}) | S3={e3:.3f} (ci={in_ci_3}, w1={w1_3:.1f})", flush=True)
        print(f"           | S2 moment err: " + " ".join(f"{k}:{v:.2f}" for k, v in e2_vec.items()), flush=True)
        print(f"           | S3 moment err: " + " ".join(f"{k}:{v:.2f}" for k, v in e3_vec.items()), flush=True)

        splits_out.append(dict(
            T0=int(T0), base_rel=be, s2_rel=e2, s3_rel=e3,
            in_ci_s2=in_ci_2, in_ci_s3=in_ci_3, w1_s2=w1_2, w1_s3=w1_3,
            em=em, bm=bm, sm2=sm2, sm3=sm3,
            err_vec_base=be_vec, err_vec_s2=e2_vec, err_vec_s3=e3_vec,
        ))

    res = dict(
        panel=pn, T=T, K=K, test_len=int(test_len), origins=[int(o) for o in origins], hor=hor,
        base_rel=float(np.mean([s["base_rel"] for s in splits_out])),
        base_sd=float(np.std([s["base_rel"] for s in splits_out])),
        s2_rel=float(np.mean([s["s2_rel"] for s in splits_out])),
        s2_sd=float(np.std([s["s2_rel"] for s in splits_out])),
        s2_cov=float(np.mean([s["in_ci_s2"] for s in splits_out])),
        s2_wins_base=int(sum(s["s2_rel"] < s["base_rel"] for s in splits_out)),
        s3_rel=float(np.mean([s["s3_rel"] for s in splits_out])),
        s3_sd=float(np.std([s["s3_rel"] for s in splits_out])),
        s3_cov=float(np.mean([s["in_ci_s3"] for s in splits_out])),
        s3_wins_base=int(sum(s["s3_rel"] < s["base_rel"] for s in splits_out)),
        s3_wins_s2=int(sum(s["s3_rel"] < s["s2_rel"] for s in splits_out)),
        splits=splits_out,
    )
    print(f"  SUMMARY {pn}: base={res['base_rel']:.3f}±{res['base_sd']:.3f} | "
          f"S2={res['s2_rel']:.3f}±{res['s2_sd']:.3f} (cov={res['s2_cov']*100:.0f}%, wins={res['s2_wins_base']}/5) | "
          f"S3={res['s3_rel']:.3f}±{res['s3_sd']:.3f} (cov={res['s3_cov']*100:.0f}%, wins={res['s3_wins_base']}/5, vsS2={res['s3_wins_s2']}/5)", flush=True)
    del X, R, floors, data
    gc.collect()
    return res


def main():
    panels = sys.argv[1:] if len(sys.argv) > 1 else list(PANELS.keys())
    out_path = OUT_DIR / "round3_frozen_gate_results.json"
    all_res = {}
    for pn in panels:
        all_res[pn] = run_panel(pn, PANELS[pn], n_splits=5, reps=20, boot=2000)
        out_path.write_text(json.dumps(all_res, indent=2))
    print(f"\nSaved -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
