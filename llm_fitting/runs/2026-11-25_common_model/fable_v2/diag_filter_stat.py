#!/usr/bin/env python3
"""D3: efficiency / robustness of the endpoint-volatility statistic inside the Round 4 posterior machinery.

Planted panels from the module's own simulator (lognormal v_i known). Three statistics of |dX_1| evaluated
through the SAME model-implied inversion (41-point lognormal prior grid, 8192 common draws):
  median (Round 4), upper-10%-trimmed mean, plain mean (L1).
Designs: clean; contaminated (one +/-3 log-unit burst per entity at a random week). T = 52 (IG-like,
7.7%/wk absence) and T = 100. Score: mean |log(v_hat / v_true)| over top-K entities, 3 seeds.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import common_rankdiff_fable as cr  # noqa: E402
from test_common_rankdiff_fable import _make_planted_params  # noqa: E402


def posterior_stat(p, dx, z, sL, stat: str, alpha: float = 0.10):
    n = np.isfinite(dx).sum(axis=0)
    valid = n >= 4
    v_hat = np.ones(z.size)
    if not valid.any():
        return v_hat
    A = np.abs(dx[:, valid])

    def emp_stat(Y):  # Y: (T, m) with NaN
        if stat == "median":
            return np.nanmedian(Y, axis=0)
        out = np.empty(Y.shape[1])
        for j in range(Y.shape[1]):
            y = Y[:, j]
            y = y[np.isfinite(y)]
            if stat == "mean":
                out[j] = y.mean()
            else:
                q = np.quantile(y, 1.0 - alpha)
                out[j] = y[y <= q].mean()
        return out

    measured = emp_stat(A)
    rng = np.random.default_rng(71329)
    size = 8192

    def draw():
        return rng.choice(p.innov_pool, size=size) if p.innov_mode == "empirical" else rng.standard_normal(size)

    xi = rng.standard_normal(size)
    for _ in range(max(1, math.ceil(math.log(0.001) / math.log(p.phi)))):
        xi = p.phi * xi + math.sqrt(1 - p.phi ** 2) * draw()
    xi_delta = (p.phi - 1.0) * xi + math.sqrt(1 - p.phi ** 2) * draw()
    obs_delta = draw() - draw()
    lasting = draw()
    order = np.argsort(z)
    nodes = np.unique(np.linspace(0, z.size - 1, min(25, z.size)).round().astype(int))
    ix = order[nodes]
    z_grid, z_eval = z[ix], z[valid]
    passing_var = 2.0 * cr.sigma_obs_of(p, z) ** 2 + 2.0 * (1.0 - p.phi) * cr.v_xi_of(p, z)
    grid = np.linspace(-2.5, 2.5, 41) * p.s_vol
    log_prior = -0.5 * ((grid + 0.5 * p.s_vol ** 2) / p.s_vol) ** 2
    lw = np.empty((grid.size, int(valid.sum())))
    pass_draws = np.sqrt(cr.v_xi_of(p, z_grid))[:, None] * xi_delta + cr.sigma_obs_of(p, z_grid)[:, None] * obs_delta
    for j, logv in enumerate(grid):
        v = math.exp(logv)
        S = np.abs(sL[ix, None] * lasting + math.sqrt(v) * pass_draws)  # (nodes, size)
        if stat == "median":
            q45, med, q55 = np.quantile(S, [0.45, 0.5, 0.55], axis=1)
            T_stat = med
            unit_se = (q55 - q45) / np.maximum(0.2 * med, 1e-12)  # = 1/(2 m f) relative
        else:
            if stat == "mean":
                T_stat = S.mean(axis=1)
                unit_se = S.std(axis=1) / np.maximum(T_stat, 1e-12)
            else:
                q = np.quantile(S, 1.0 - alpha, axis=1)
                Sw = np.minimum(S, q[:, None])
                T_stat = np.array([S[i][S[i] <= q[i]].mean() for i in range(S.shape[0])])
                unit_se = (Sw.std(axis=1) / (1.0 - alpha)) / np.maximum(T_stat, 1e-12)
        variance = np.maximum(sL ** 2 + passing_var * v, 1e-12)
        pred = np.interp(z_eval, z_grid, T_stat / np.sqrt(variance[ix])) * np.sqrt(variance[valid])
        se = np.maximum(np.interp(z_eval, z_grid, unit_se), 1e-4) / np.sqrt(n[valid] / 2.0)
        resid = np.log(np.maximum(measured, 1e-12) / np.maximum(pred, 1e-12))
        lw[j] = log_prior[j] - np.log(se) - 0.5 * (resid / se) ** 2
    w = np.exp(lw - lw.max(axis=0))
    w /= w.sum(axis=0)
    v_hat[valid] = np.exp(grid) @ w
    return v_hat


def main():
    res = {}
    for T, a_abs in ((52, -2.5), (100, -4.5)):
        p_true, L0 = _make_planted_params(N=3000, K=1500, sigma_L0=0.10, beta=0.05, phi=0.90, a_xi=-0.8, b_xi=0.05,
                                          a_obs=-0.3, b_obs=0.12, s_vol=0.75, gamma_L=0.0, a_abs=a_abs, b_abs=0.7, innov_mode="empirical")
        for design in ("clean", "contaminated"):
            errs = {s: [] for s in ("median", "trim10", "mean")}
            for seed in (1, 2, 3):
                rng = np.random.default_rng(seed)
                st = cr.CommonState(p_true, L0, rng)
                v_true = st.v.copy()
                Xs = np.empty((T, p_true.N))
                Rs = np.full((T, p_true.N), np.nan)
                for _ in range(10):
                    st.step()
                for t in range(T):
                    x = st.step(apply_absence=True)
                    Xs[t] = x
                    ok = np.flatnonzero(np.isfinite(x))
                    Rs[t, ok[np.argsort(-x[ok], kind="stable")]] = np.arange(1, ok.size + 1, dtype=float)
                if design == "contaminated":
                    tt = rng.integers(0, T, size=p_true.N)
                    sgn = rng.choice([-1.0, 1.0], size=p_true.N)
                    Xs[tt, np.arange(p_true.N)] += 3.0 * sgn
                fl = np.sum(np.isfinite(Rs), axis=1) + 1.0
                p_hat = cr.estimate_common(Xs, Rs, K=1500, floors=fl, refine_drift=False)
                Xc = Xs - cr._top_k_center(Xs, Rs, 1500)
                pen = np.where(np.isfinite(Rs), Rs, fl[:, None])
                pr = np.empty(p_true.N)
                pr[np.argsort(pen.mean(axis=0), kind="stable")] = np.arange(1, p_true.N + 1)
                z = np.log(np.clip((pr - 0.5) / p_true.N, 1e-6, 1.0))
                dX = np.diff(Xc, axis=0)
                dX = dX - np.nanmean(dX, axis=1, keepdims=True)
                mean_X = np.nanmean(Xs, axis=0)
                sL = cr.sigma_L_of(p_hat, np.where(np.isfinite(mean_X), mean_X, p_hat.L_K - 1.0))
                top = pr <= 1500
                for s in errs:
                    vh = posterior_stat(p_hat, dX, z, sL, s)
                    errs[s].append(float(np.mean(np.abs(np.log(vh[top] / v_true[top])))))
            line = " | ".join(f"{s}: {np.mean(e):.4f}" for s, e in errs.items())
            print(f"T={T:3d} {design:12s}: {line}", flush=True)
            res[f"T{T}_{design}"] = {s: float(np.mean(e)) for s, e in errs.items()}
    (HERE / "diag_filter_stat.json").write_text(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
