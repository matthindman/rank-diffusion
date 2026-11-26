#!/usr/bin/env python3
"""D4b (Round 8): localize the upward bias of the R6 volatility posterior (training window only).
Re-runs the R6 posterior arithmetic for the top-200 cohort and compares, per entity, the measured
median |dX_1| with the posterior's OWN predicted median at (a) the posterior mean v_hat, (b) the
posterior mean of log v, (c) the MAP grid point; and with an independent MC median at v_hat."""
import json, math, sys
from pathlib import Path
import numpy as np
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_opus_r8 as cr
CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")

def posterior_parts(p, dx, z, sL):
    """Verbatim R6 _volatility_posterior arithmetic, returning per-grid predictions too."""
    n = np.isfinite(dx).sum(axis=0); valid = n >= 4
    measured = np.nanmedian(np.abs(dx[:, valid]), axis=0)
    rng = np.random.default_rng(71329); size = 8192
    draw = lambda: rng.choice(p.innov_pool, size=size)
    xi = rng.standard_normal(size)
    nb = max(1, math.ceil(math.log(0.001) / math.log(p.phi)))
    for _ in range(nb):
        xi = p.phi * xi + math.sqrt(1 - p.phi ** 2) * draw()
    xi_delta = (p.phi - 1) * xi + math.sqrt(1 - p.phi ** 2) * draw()
    obs_delta = draw() - draw(); lasting = draw()
    drift = math.sqrt(p.decline_var) * rng.standard_normal(size)
    jump_delta = (rng.random(size) < p.jump_lam) * cr._draw_jump_unit(p, rng, size) if p.jump_lam > 0 else np.zeros(size)
    order = np.argsort(z)
    nodes = np.unique(np.linspace(0, z.size - 1, min(25, z.size)).round().astype(int))
    nodes = np.unique(np.r_[nodes, np.geomspace(1, z.size, min(25, z.size)).round().astype(int) - 1])
    ix = order[nodes]; z_grid = z[ix]; z_eval = z[valid]
    passing_var = 2 * cr.sigma_obs_of(p, z) ** 2 + 2 * (1 - p.phi) * cr.v_xi_of(p, z)
    jump_sd = cr._jump_sd_of(p, z)
    fixed_var = p.decline_var + p.jump_lam * jump_sd ** 2
    grid = np.linspace(-2.5, 2.5, 41) * p.s_vol
    log_prior = -0.5 * ((grid + 0.5 * p.s_vol ** 2) / p.s_vol) ** 2
    pass_draws = np.sqrt(cr.v_xi_of(p, z_grid))[:, None] * xi_delta + cr.sigma_obs_of(p, z_grid)[:, None] * obs_delta
    fixed_draws = drift + jump_sd[ix, None] * jump_delta
    LW = np.empty((grid.size, int(valid.sum()))); PRED = np.empty_like(LW)
    for j, lv in enumerate(grid):
        v = math.exp(lv)
        s = fixed_draws + sL[ix, None] * lasting + math.sqrt(v) * pass_draws
        q45, med, q55 = np.quantile(np.abs(s), [0.45, 0.5, 0.55], axis=1)
        var = np.maximum(sL ** 2 + passing_var * v + fixed_var, 1e-12)
        pred = np.interp(z_eval, z_grid, med / np.sqrt(var[ix])) * np.sqrt(var[valid])
        use = np.interp(z_eval, z_grid, (q55 - q45) / np.maximum(0.2 * med, 1e-12))
        se = np.maximum(use, 1e-4) / np.sqrt(n[valid] / 2)
        r = np.log(np.maximum(measured, 1e-12) / np.maximum(pred, 1e-12))
        LW[j] = log_prior[j] - np.log(se) - 0.5 * (r / se) ** 2
        PRED[j] = pred
    W = np.exp(LW - LW.max(axis=0)); W /= W.sum(axis=0)
    return valid, measured, grid, W, PRED, n

out = {}
for pn, Tf in [("reddit_submissions_long", 54), ("reddit_comments_ext", 54), ("facebook_a", 33)]:
    d = np.load(CACHE / f"{pn}.npz", allow_pickle=True)
    X, R, fl, K = d["X"].astype(float), d["R"].astype(float), d["floors"].astype(float), int(d["K"])
    T0 = Tf + (52 if Tf > 40 else 21)
    sb = None
    if pn == "facebook_a":
        js = json.loads((CACHE / "facebook_a_spec_b.json").read_text()); sb = (np.array(js["z"]), np.array(js["sigma_obs"]))
    p = cr.estimate_common(X[:T0], R[:T0], K=K, floors=fl[:T0], spec_b=sb, refine_drift=False, asymmetry=False)
    Xtr, Rtr, ftr = X[:Tf], R[:Tf], fl[:Tf]
    pm = cr._top_k_center(Xtr, Rtr, K); Xc = Xtr - pm
    pen = np.where(np.isfinite(Rtr), Rtr, ftr[:, None]); N = X.shape[1]
    pr = np.empty(N); pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, N + 1)
    z = np.log(np.clip((pr - 0.5) / N, cr.Z_CLIP, 1))
    dX = np.diff(Xc, axis=0); dX = dX - np.nanmean(dX, axis=1, keepdims=True)
    mx = np.nanmean(Xtr, axis=0); mx = np.where(np.isfinite(mx), mx, p.L_K - 1); sL = cr.sigma_L_of(p, mx)
    valid, measured, grid, W, PRED, n = posterior_parts(p, dX, z, sL)
    vi = np.flatnonzero(valid)
    coh = np.flatnonzero(np.isfinite(Rtr[-1]) & (Rtr[-1] <= 200))
    pos = np.searchsorted(vi, coh); keep = (pos < vi.size) & (vi[np.minimum(pos, vi.size - 1)] == coh)
    c = pos[keep]
    v_mean = np.exp(grid) @ W; lv_mean = grid @ W; j_map = np.argmax(W, axis=0)
    def pred_at(lv):  # interpolate posterior predictions over the grid at arbitrary log v
        return np.array([np.interp(lv[i], grid, PRED[:, i]) for i in range(lv.size)])
    m = measured[c]
    r_vmean = np.log(pred_at(np.log(v_mean))[c] / m)
    r_lvmean = np.log(pred_at(lv_mean)[c] / m)
    r_map = np.log(PRED[j_map, np.arange(PRED.shape[1])][c] / m)
    r_lo = np.log(PRED[0, c] / m)
    post_sd = np.sqrt(np.maximum((grid ** 2) @ W - lv_mean ** 2, 0))[c]
    res = dict(n_coh=int(c.size), med_n_weeks=float(np.median(n[vi][c])),
               logratio_at_vhat=float(np.median(r_vmean)), logratio_at_mean_logv=float(np.median(r_lvmean)),
               logratio_at_map=float(np.median(r_map)), share_floor_above_measured=float(np.mean(r_lo > 0)),
               median_post_sd_logv=float(np.median(post_sd)), median_lv_mean=float(np.median(lv_mean[c])),
               prior_center=float(-0.5 * p.s_vol ** 2), s_vol=p.s_vol,
               all_valid_logratio_at_map=float(np.median(np.log(PRED[j_map, np.arange(PRED.shape[1])] / measured))))
    out[pn] = res
    print(pn, json.dumps(res, indent=1))
(HERE / "diag_vpost2.json").write_text(json.dumps(out, indent=1))
