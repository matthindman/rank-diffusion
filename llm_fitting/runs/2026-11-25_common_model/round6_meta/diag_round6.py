"""Round 6 diagnostics (DEVELOPMENT cache only; zero sealed data).

Measures the four bottlenecks standing between Round 4 (variance + Astra filter)
and Round 5 (Fable S2+J) becoming ONE unified Common Meta-Framework:
  D6.1: Band-by-band variance vs robust-IQR split for (sL2, vxi, so2) across all 4 panels,
        including the rank profile of the lasting-variance gap (why FB head overshot in S2+J)
        and the short-lag passing/obs gap (why FB/IG cov90 was under-dispersed in S2+J).
  D6.2: Band-by-band skewness kappa_3(h) and odd-moment surge profile across all 4 panels,
        checking (a) rank attenuation of surges vs b_xi, (b) outlier-resistant surge amplitude
        moments vs kappa_5/kappa_3, and (c) why IG triggers wrong-sign surges.
  D6.3: Band-by-band gapped drift covariance (does Reddit persistent drift sigma_d vary by rank?).
  D6.4: Planted filter-statistic efficiency comparison inside _volatility_posterior:
        median(|dX_1|) vs trim10_mean(|dX_1|) vs mean(|dX_1|) on clean and burst-contaminated data.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
import numpy as np

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "llm_fitting"))

import common_rankdiff as cr

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
OUT = Path(__file__).resolve().parent


def load_panel(name: str):
    d = np.load(CACHE / f"{name}.npz")
    spec_b = None
    if name == "facebook_a":
        with open(CACHE / "facebook_a_spec_b.json") as f:
            sb = json.load(f)
        spec_b = (np.asarray(sb["z"], float), np.asarray(sb["sigma_obs"], float))
    return d["X"], d["R"], int(d["K"]), spec_b


def diag_band_variance_vs_robust(name: str, X: np.ndarray, R: np.ndarray, K: int, spec_b):
    pv = cr.estimate_common(X, R, K, spec_b=spec_b, refine_drift=False, asymmetry=False, scale_moment="variance")
    pr = cr.estimate_common(X, R, K, spec_b=spec_b, refine_drift=False, asymmetry=False, scale_moment="robust")
    dv, dr = pv.diag, pr.diag
    sL_v = np.array(dv["sL2_band"], float)
    sL_r = np.array(dr["sL2_band"], float)
    vxi_v = np.array(dv["vxi_band"], float)
    vxi_r = np.array(dr["vxi_band"], float)
    so_v = np.array(dv["so2_band"], float)
    so_r = np.array(dr["so2_band"], float)
    gap = np.array(dr["jumps"]["gap_band"], float)
    bz = np.array(dv["band_z"], float)
    print(f"\n[D6.1 {name}] phi={pv.phi:.3f} | var sL0={pv.sigma_L0:.4f} beta={pv.beta:+.3f} a_xi={pv.a_xi:+.2f} b_xi={pv.b_xi:+.2f} a_obs={pv.a_obs:+.2f}")
    print(f"          rob sL0={pr.sigma_L0:.4f} beta={pr.beta:+.3f} a_xi={pr.a_xi:+.2f} b_xi={pr.b_xi:+.2f} a_obs={pr.a_obs:+.2f} | jump_lam={pr.jump_lam} jump_c={pr.jump_c:.4f} gap_share={dr['jumps']['gap_share']:.3f}")
    print("  band |   z-zK | sL2_var | sL2_rob |   gap  | gap/var | vxi_r/v | so2_r/v")
    for b in range(len(bz) - 1):
        g_frac = gap[b] / sL_v[b] if (np.isfinite(sL_v[b]) and sL_v[b] > 1e-8) else np.nan
        vr = vxi_r[b] / vxi_v[b] if (np.isfinite(vxi_v[b]) and vxi_v[b] > 1e-8) else np.nan
        sr = so_r[b] / so_v[b] if (np.isfinite(so_v[b]) and so_v[b] > 1e-8) else np.nan
        print(f"    {b:2d} | {bz[b]-pv.z_K:+6.2f} | {sL_v[b]:7.5f} | {sL_r[b]:7.5f} | {gap[b]:6.4f} | {g_frac:7.3f} | {vr:7.3f} | {sr:7.3f}")
    return {
        "band_z": bz.tolist(), "z_K": pv.z_K,
        "sL2_var": sL_v.tolist(), "sL2_rob": sL_r.tolist(), "gap": gap.tolist(),
        "vxi_var": vxi_v.tolist(), "vxi_rob": vxi_r.tolist(),
        "so2_var": so_v.tolist(), "so2_rob": so_r.tolist(),
    }


def _perm_and_bands(R: np.ndarray, K: int):
    T, N = R.shape
    floors = np.sum(np.isfinite(R), axis=1).astype(float) + 1.0
    pen = np.where(np.isfinite(R), R, floors[:, None])
    rperm = np.mean(pen, axis=0)
    pres = np.isfinite(R).mean(axis=0)
    pr_ord = np.empty(N, dtype=float)
    pr_ord[np.argsort(rperm, kind="stable")] = np.arange(1, N + 1)
    edges = cr._band_edges(K, N)
    nb = len(edges) - 1
    band_all = np.clip(np.searchsorted(edges, pr_ord, side="right") - 1, 0, nb - 1)
    band_of = np.where(pres >= 0.5, band_all, -1)
    return pr_ord, pres, band_of, nb


def diag_surge_anatomy(name: str, X: np.ndarray, R: np.ndarray, K: int, spec_b):
    T, N = X.shape
    pr_ord, pres, band_of, nb = _perm_and_bands(R, K)
    in_k_ent = (pr_ord <= K) & (pres >= 0.5)
    # Center by top-K median
    med_k = np.full(T, np.nan)
    for t in range(T):
        sel = (R[t] <= K) & np.isfinite(X[t])
        if sel.any():
            med_k[t] = np.median(X[t, sel])
    Xc = X - med_k[:, None]

    # Check kappa_3(h) and L-skewness / robust skewness by lag and by permanent-rank band
    print(f"\n[D6.2 {name}] Surge anatomy (T={T}, K={K})")
    lags = [h for h in (1, 2, 4, 8, 13, 26, 39) if h < T // 2]
    print("  lag |   k3_all |  skew_all |  q_skew(90/50/10) |   k5/k3_mu | odd4_ratio |  k3_head(b0-1) | k3_body(b>=2)")
    lag_stats = {}
    for h in lags:
        d = Xc[h:] - Xc[:-h]
        # demean within band
        dm = np.full_like(d, np.nan)
        for b in range(nb - 1):
            idx = np.flatnonzero((band_of == b) & in_k_ent)
            if idx.size >= 10:
                sub = d[:, idx]
                with np.errstate(all="ignore"):
                    mu = np.nanmean(sub, axis=1, keepdims=True)
                dm[:, idx] = sub - mu
        v_all = dm[:, in_k_ent]
        v_all = v_all[np.isfinite(v_all)]
        k2 = float(np.mean(v_all ** 2))
        k3 = float(np.mean(v_all ** 3))
        k5 = float(np.mean(v_all ** 5) - 10.0 * k2 * k3)
        skew = k3 / max(k2 ** 1.5, 1e-12)
        q10, q50, q90 = np.quantile(v_all, [0.10, 0.50, 0.90])
        q_skew = (q90 + q10 - 2.0 * q50) / max(q90 - q10, 1e-12)
        # Implied mu from k5/k3 for capped exp
        mu_k5 = math.sqrt(max(k5 / k3, 0.0) * cr.CAP_M3 / cr.CAP_M5) if k3 > 1e-6 else np.nan
        # Odd 4th ratio: E[d^3 |d|] / (E[|d|] * k3) - 4 -> scale-free indicator of jump size
        odd4 = (float(np.mean((v_all ** 3) * np.abs(v_all))) - 4.0 * float(np.mean(np.abs(v_all))) * k3) / max(abs(k3), 1e-12) if abs(k3) > 1e-6 else np.nan

        vh = dm[:, (band_of <= 1) & in_k_ent]
        vh = vh[np.isfinite(vh)]
        vb = dm[:, (band_of >= 2) & (band_of < nb - 1) & in_k_ent]
        vb = vb[np.isfinite(vb)]
        k3_h = float(np.mean(vh ** 3)) if vh.size > 100 else np.nan
        k3_b = float(np.mean(vb ** 3)) if vb.size > 100 else np.nan
        print(f"  {h:3d} | {k3:+8.4f} | {skew:+9.4f} | {q_skew:+16.4f} | {mu_k5:10.3f} | {odd4:+10.3f} | {k3_h:+14.4f} | {k3_b:+13.4f}")
        lag_stats[h] = dict(k2=k2, k3=k3, skew=skew, q_skew=q_skew, mu_k5=mu_k5 if np.isfinite(mu_k5) else None, k3_h=k3_h, k3_b=k3_b)

    # Band-by-band k3 at h=1 and h=4 (check rank scaling of surges!)
    print("  Band k3(h=4) & k3(h=13):")
    for b in range(nb - 1):
        idx = np.flatnonzero((band_of == b) & in_k_ent)
        if idx.size < 10:
            continue
        vals = {}
        for h in (1, 4, 13):
            if h >= T // 2:
                vals[h] = np.nan
                continue
            sub = Xc[h:, idx] - Xc[:-h, idx]
            with np.errstate(all="ignore"):
                sub = sub - np.nanmean(sub, axis=1, keepdims=True)
            vf = sub[np.isfinite(sub)]
            vals[h] = float(np.mean(vf ** 3)) if vf.size > 50 else np.nan
        print(f"    band {b} (n={idx.size:4d}): k3(1)={vals[1]:+.4f}  k3(4)={vals[4]:+.4f}  k3(13)={vals[13]:+.4f}")

    # Check mu21 / mu12 on short (4w) and medium (13w) non-overlapping increments
    for W in (4, 13):
        if 2 * W >= T:
            continue
        d1 = Xc[W:T - W] - Xc[:T - 2 * W]
        d2 = Xc[2 * W:] - Xc[W:T - W]
        # demean within band
        dm1 = np.full_like(d1, np.nan)
        dm2 = np.full_like(d2, np.nan)
        for b in range(nb - 1):
            idx = np.flatnonzero((band_of == b) & in_k_ent)
            if idx.size >= 10:
                with np.errstate(all="ignore"):
                    dm1[:, idx] = d1[:, idx] - np.nanmean(d1[:, idx], axis=1, keepdims=True)
                    dm2[:, idx] = d2[:, idx] - np.nanmean(d2[:, idx], axis=1, keepdims=True)
        ok = np.isfinite(dm1) & np.isfinite(dm2)
        x1, x2 = dm1[ok], dm2[ok]
        sd1 = float(np.std(x1))
        sd2 = float(np.std(x2))
        mu21 = float(np.mean((x1 ** 2) * x2)) / max(sd1 * sd1 * sd2, 1e-12)
        mu12 = float(np.mean(x1 * (x2 ** 2))) / max(sd1 * sd2 * sd2, 1e-12)
        print(f"  Adjacent W={W:2d}w: mu21={mu21:+.4f}, mu12={mu12:+.4f} (fading positive surge => mu21 < 0, mu12 > 0)")
    return lag_stats


def diag_drift_by_band(name: str, X: np.ndarray, R: np.ndarray, K: int):
    T, N = X.shape
    if T < 75:
        print(f"\n[D6.3 {name}] skipped (T={T} < 75)")
        return {}
    pr_ord, pres, band_of, nb = _perm_and_bands(R, K)
    med_k = np.full(T, np.nan)
    for t in range(T):
        sel = (R[t] <= K) & np.isfinite(X[t])
        if sel.any():
            med_k[t] = np.median(X[t, sel])
    Xc = X - med_k[:, None]

    W = 26 if T >= 130 else 13
    G = 13 if T >= 130 else 8
    step = max(W // 2, 4)
    print(f"\n[D6.3 {name}] Gapped window covariance by permanent-rank band (W={W}, G={G}):")
    out = {}
    for b in range(nb - 1):
        idx = np.flatnonzero((band_of == b) & (pr_ord <= K) & (pres >= 0.5))
        if idx.size < 20:
            continue
        covs = []
        for t0 in range(0, T - 2 * W - G + 1, step):
            with np.errstate(all="ignore"):
                m0 = np.nanmean(Xc[t0:t0 + W, idx], axis=0)
                m1 = np.nanmean(Xc[t0 + W:t0 + 2 * W, idx], axis=0)
                # wait: gapped pairs!
                # block 0: [t0, t0+W), block 1: [t0+W, t0+2W) -> no, gapped!
                # d1 = mean(t0+W..t0+2W) - mean(t0..t0+W) is wrong if we want gap G; use block width B:
            break
        # Use exact _estimate_stage3_drift geometry: W=26 (two 26w diffs separated by gap G=13 -> total span 2W+G? No, check!)
        # Actually d1 = X[t+W] - X[t] averaged? No, let's check exact W-week point-to-point diffs separated by gap G!
        # d1 = X[t+W] - X[t], d2 = X[t+W+G+W] - X[t+W+G]
        span = 2 * W + G
        for t0 in range(0, T - span, max(W // 2, 4)):
            d1 = Xc[t0 + W, idx] - Xc[t0, idx]
            d2 = Xc[t0 + 2 * W + G, idx] - Xc[t0 + W + G, idx]
            ok = np.isfinite(d1) & np.isfinite(d2)
            if ok.sum() >= 15:
                a, c = d1[ok] - np.mean(d1[ok]), d2[ok] - np.mean(d2[ok])
                covs.append(float(np.mean(a * c)))
        mc = float(np.mean(covs)) if covs else np.nan
        # implied sigma_d assuming rho_d = 0.5^(1/52)
        rho_d = 0.5 ** (1.0 / 52.0)
        # E[sum_{u=0}^{W-1} d_u * sum_{v=W+G}^{2W+G-1} d_v] / sigma_d^2
        # = rho_d^(G+1) * ((1 - rho_d^W)/(1 - rho_d))^2 * ... exact sum:
        s_fac = 0.0
        for u in range(W):
            for v in range(W + G, 2 * W + G):
                s_fac += rho_d ** abs(v - u)
        sig_d = math.sqrt(max(mc, 0.0) / s_fac) if np.isfinite(mc) else np.nan
        print(f"    band {b} (n={idx.size:4d}): raw_cov={mc:+.5f} -> implied sig_d={sig_d:.5f}")
        out[b] = dict(n=int(idx.size), raw_cov=mc, sig_d=sig_d)
    return out


def diag_filter_efficiency():
    """Compare median(|dX|) vs upper-10%-trimmed mean(|dX|) vs L1 mean(|dX|) inside the 41-point posterior."""
    print("\n[D6.4] Planted filter statistic efficiency (T=52 & T=100, clean & 1-burst-contaminated)")
    rng = np.random.default_rng(12345)
    for T in (52, 100):
        for contam in (False, True):
            N = 2000
            s_vol = 0.7
            z_v = np.clip(rng.standard_normal(N), -2.5, 2.5)
            v_true = np.exp(s_vol * z_v)
            v_true = v_true / np.mean(v_true)
            sL = 0.08
            pass_var = 0.40  # IG-like passing + obs 1-week variance
            sd_tot = np.sqrt(sL * sL + v_true * pass_var)
            dx = rng.standard_normal((T, N)) * sd_tot[None, :]
            if T == 52:
                # 10% intermittent missingness like IG
                mask = rng.random((T, N)) < 0.10
                dx[mask] = np.nan
            if contam:
                # 1 burst of +-3 log units per entity
                rows = rng.integers(0, T, size=N)
                signs = rng.choice([-1.0, 1.0], size=N)
                for i in range(N):
                    if np.isfinite(dx[rows[i], i]):
                        dx[rows[i], i] += 3.0 * signs[i]

            grid = np.linspace(-2.5, 2.5, 41) * s_vol
            log_prior = -0.5 * ((grid + 0.5 * s_vol * s_vol) / s_vol) ** 2
            # Reference MC samples per grid point
            M = 8000
            z0 = rng.standard_normal(M)
            abs_dx = np.abs(dx)
            n_obs = np.sum(np.isfinite(abs_dx), axis=0)

            res = {}
            for mode in ("median", "trim10", "l1_mean"):
                # Compute observed entity statistic
                obs_stat = np.full(N, np.nan)
                for i in range(N):
                    vals = abs_dx[:, i]
                    vals = vals[np.isfinite(vals)]
                    if vals.size < 3:
                        continue
                    if mode == "median":
                        obs_stat[i] = float(np.median(vals))
                    elif mode == "l1_mean":
                        obs_stat[i] = float(np.mean(vals))
                    else:
                        sv = np.sort(vals)
                        k_cut = max(1, int(math.floor(0.10 * sv.size)))
                        obs_stat[i] = float(np.mean(sv[:-k_cut])) if sv.size > k_cut + 1 else float(np.mean(sv))

                # Posterior via exact Gaussian-approx likelihood of the statistic over grid
                logw = np.empty((grid.size, N))
                for j, lg in enumerate(grid):
                    v = math.exp(lg)
                    sd_g = math.sqrt(sL * sL + v * pass_var)
                    samp = np.abs(sd_g * z0)
                    if mode == "median":
                        mu_g = float(np.median(samp))
                        # Asymptotic variance of sample median of |N(0, sd_g^2)|: 1 / (4 n f(m)^2)
                        # Here f(m) = 2 * phi(m/sd_g) / sd_g; for m = 0.6745 sd_g, f(m) = 0.6356/sd_g => var = 0.6188 * sd_g^2 / n
                        c_var = 0.6188 * (sd_g ** 2)
                    elif mode == "l1_mean":
                        mu_g = float(np.mean(samp))
                        c_var = float(np.var(samp))
                    else:
                        sv = np.sort(samp)
                        k_cut = int(math.floor(0.10 * sv.size))
                        mu_g = float(np.mean(sv[:-k_cut]))
                        # Variance of upper-10%-trimmed mean via Winsorized variance / (0.9^2 n)
                        q90 = sv[-k_cut]
                        w_samp = np.minimum(samp, q90)
                        c_var = float(np.var(w_samp)) / (0.90 ** 2)
                    se2 = np.maximum(c_var / np.maximum(n_obs, 1.0), 1e-8)
                    logw[j] = log_prior[j] - 0.5 * ((obs_stat - mu_g) ** 2) / se2 - 0.5 * np.log(se2)
                logw = logw - np.max(logw, axis=0, keepdims=True)
                w = np.exp(logw)
                w /= np.sum(w, axis=0, keepdims=True)
                v_hat = np.exp(np.sum(w * grid[:, None], axis=0))
                v_hat = v_hat / np.mean(v_hat)
                err = float(np.mean(np.abs(np.log(v_hat / v_true))))
                res[mode] = err
            print(f"  T={T:3d} contam={str(contam):5s}: median={res['median']:.4f} | trim10={res['trim10']:.4f} | l1_mean={res['l1_mean']:.4f}")


def main():
    results = {}
    for name in ("facebook_a", "instagram_hm", "reddit_comments_ext", "reddit_submissions_long"):
        X, R, K, spec_b = load_panel(name)
        results[name] = {
            "d61": diag_band_variance_vs_robust(name, X, R, K, spec_b),
            "d62": diag_surge_anatomy(name, X, R, K, spec_b),
            "d63": diag_drift_by_band(name, X, R, K),
        }
    diag_filter_efficiency()
    with open(OUT / "diag_round6.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nSaved diag_round6.json")


if __name__ == "__main__":
    main()
