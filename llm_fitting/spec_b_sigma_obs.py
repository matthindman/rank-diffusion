#!/usr/bin/env python3
"""Spec-B: identify the weekly observation-noise curve sigma_obs(z) from the
DAILY-within-week noise floor (independent of any weekly movement moment).

Model: the weekly metric is the SUM of daily karma (verified exact).  Daily
log-activity within an entity-week is level + day-of-week + non-persistent
noise; whatever averages out within the week cannot carry week-to-week signal,
so its delta-method image on the weekly log-sum is a FLOOR for sigma_obs:

    Var(log sum_j D_j | week level) ~ sigma_d^2 * sum_j p_j^2      (p = daily shares)

Three estimators:
  * toeplitz (PRIMARY): fit sigma_d^2 * Toeplitz(1, rho_1..rho_6) to the
    empirical within-week residual covariance (projected through the
    week-mean centering), then map exactly: floor = mean_w p_w' Sigma p_w.
    Within-week residuals are mildly mean-reverting (rho_1..3 ~ -0.1, as the
    2026-06 handoff warned), which the iid mapping ignores -- the iid floor
    overstates by ~2x.
  * splithalf / residual (cross-checks): iid-mapping variants; report as
    upper bounds.

Per the handoff: daily data is used ONLY as a noise floor -- no daily
dynamics model.  Caveat: multi-day within-week bursts count partly as noise
here (they are transitory at weekly frequency anyway).

Usage:
  python llm_fitting/spec_b_sigma_obs.py            # report both methods, K=5000
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

DAILY_PATH = "data/reddit/reddit_daily.parquet"
HALF_A, HALF_B = [0, 2, 4, 6], [1, 3, 5]


def load_daily(members: set, path: str = DAILY_PATH) -> pd.DataFrame:
    d = pd.read_parquet(path, columns=["date", "endpoint_id", "metric_value"])
    d = d[d["endpoint_id"].isin(members)].copy()
    d["date"] = pd.to_datetime(d["date"])
    d["week"] = d["date"] - pd.to_timedelta(d["date"].dt.weekday, unit="D")
    d["dow"] = d["date"].dt.weekday
    return d


def _week_matrix(d: pd.DataFrame, weeks) -> tuple[np.ndarray, pd.Index]:
    """(entity, week) x 7 daily-karma matrix (absent day = 0), weeks restricted."""
    d = d[d["week"].isin(weeks)]
    mat = d.pivot_table(index=["endpoint_id", "week"], columns="dow",
                        values="metric_value", fill_value=0.0)
    mat = mat.reindex(columns=range(7), fill_value=0.0)
    K = mat.to_numpy(dtype=float)
    ok = K.sum(1) > 0
    return K[ok], mat.index[ok]


def _toeplitz_floor(R: np.ndarray, p_sh: np.ndarray) -> tuple[float, float]:
    """Fit sigma_d^2 * Toeplitz(1, rho) to the centered residual covariance and
    return (sigma_d, corrected weekly floor variance).  R: (n,7) centered log
    residuals (complete-positive weeks); p_sh: (n,7) daily karma shares."""
    C7 = np.eye(7) - np.ones((7, 7)) / 7
    Cemp = R.T @ R / len(R)
    bases = []
    for l in range(7):
        T = np.zeros((7, 7))
        for i in range(7):
            for j in range(7):
                if abs(i - j) == l:
                    T[i, j] = 1.0
        bases.append(C7 @ T @ C7)
    X = np.stack([b.ravel() for b in bases], 1)
    coef, *_ = np.linalg.lstsq(X, Cemp.ravel(), rcond=None)
    s2, rho = coef[0], coef[1:] / max(coef[0], 1e-12)
    Sig = np.empty((7, 7))
    for i in range(7):
        for j in range(7):
            Sig[i, j] = s2 * (1.0 if i == j else rho[abs(i - j) - 1])
    floor = float(np.mean(np.einsum("nj,jk,nk->n", p_sh, Sig, p_sh)))
    return float(np.sqrt(max(s2, 0.0))), max(floor, 0.0)


def spec_b_curve(uni: pd.DataFrame, daily: pd.DataFrame, n_bands: int = 12,
                 max_period: int | None = None, method: str = "toeplitz") -> dict:
    """sigma_obs,B(z) on the universe's rank coordinate.

    uni: weekly universe panel (restrict_universe output); daily: load_daily
    output for (a superset of) its members.  max_period: use only weeks with
    period < max_period (train-only identification for the OOS gate)."""
    wk = uni.drop_duplicates("period").sort_values("period")[["period", "ts"]]
    if max_period is not None:
        wk = wk[wk["period"] < max_period]
        uni = uni[uni["period"] < max_period]
    weeks = set(wk["ts"])
    perm = uni.groupby("entity_id")["rank"].mean()
    N = int(round(uni.groupby("period")["entity_id"].size().mean()))

    K, idx = _week_matrix(daily[daily["endpoint_id"].isin(set(perm.index))], weeks)
    tot = K.sum(1)
    p = K / tot[:, None]
    sum_p2 = (p ** 2).sum(1)
    ent = np.asarray(idx.get_level_values(0))

    if method == "toeplitz":
        full = (K > 0).all(1)
        L = np.log(K[full])
        dow = (L - L.mean(1, keepdims=True)).mean(0)
        R = L - dow[None, :]
        R = R - R.mean(1, keepdims=True)
        p_sh = p[full]
        pr = perm.reindex(ent[full]).to_numpy()
        keep = np.isfinite(pr)
        R, p_sh, pr, ent_f = R[keep], p_sh[keep], pr[keep], ent[full][keep]
        edges = np.quantile(pr, np.linspace(0, 1, n_bands + 1))
        band = np.clip(np.searchsorted(edges, pr, side="right") - 1, 0, n_bands - 1)
        z_out, s_out, rows = [], [], []
        for b in range(n_bands):
            m = band == b
            if m.sum() < 300:
                continue
            sig_d, floor = _toeplitz_floor(R[m], p_sh[m])
            r_med = float(np.median(pr[m]))
            z_out.append(np.log(np.clip((r_med - 0.5) / N, mrd.Z_CLIP, 1.0)))
            s_out.append(float(np.sqrt(floor)))
            rows.append((r_med, sig_d, float((p_sh[m] ** 2).sum(1).mean()),
                         float(np.sqrt(floor)), int(pd.unique(ent_f[m]).size)))
        return dict(z=np.array(z_out), sigma_obs=np.array(s_out), N=N,
                    table=pd.DataFrame(rows, columns=["rank", "sigma_d", "sum_p2",
                                                      "sigma_obsB", "n_ent"]))

    if method == "splithalf":
        Sa, Sb = K[:, HALF_A].sum(1), K[:, HALF_B].sum(1)
        ok = (Sa > 0) & (Sb > 0)
        with np.errstate(divide="ignore", invalid="ignore"):
            Pa, Pb = p[:, HALF_A].sum(1), p[:, HALF_B].sum(1)
            bracket = (p[:, HALF_A] ** 2).sum(1) / Pa ** 2 + (p[:, HALF_B] ** 2).sum(1) / Pb ** 2
        tab = pd.DataFrame({"ent": ent[ok], "y": np.log(Sa[ok]) - np.log(Sb[ok]),
                            "br": bracket[ok], "sp2": sum_p2[ok]})
        tab["y"] = tab["y"] - tab.groupby("ent")["y"].transform("mean")
        nw = tab.groupby("ent")["y"].transform("count")
        tab["y2adj"] = tab["y"] ** 2 * nw / np.maximum(nw - 1, 1)   # demeaning dof
        tab = tab[nw >= 8]
    else:  # residual method, positive days only
        L = np.where(K > 0, np.log(K, out=np.full_like(K, np.nan), where=K > 0), np.nan)
        dow_eff = np.nanmean(L - np.nanmean(L, axis=1, keepdims=True), axis=0)
        R = L - dow_eff[None, :]
        R = R - np.nanmean(R, axis=1, keepdims=True)
        npos = np.isfinite(R).sum(1)
        ok = npos >= 4
        ss = np.nansum(R ** 2, axis=1)
        tab = pd.DataFrame({"ent": ent[ok], "y2adj": ss[ok] / (npos[ok] - 1),
                            "br": 1.0, "sp2": sum_p2[ok]})

    tab["pr"] = perm.reindex(tab["ent"]).to_numpy()
    tab = tab.dropna(subset=["pr"])
    edges = np.quantile(tab["pr"], np.linspace(0, 1, n_bands + 1))
    band = np.clip(np.searchsorted(edges, tab["pr"], side="right") - 1, 0, n_bands - 1)
    z_out, s_out, rows = [], [], []
    for b in range(n_bands):
        s = tab[band == b]
        if len(s) < 50:
            continue
        sig_d2 = float(s["y2adj"].mean() / s["br"].mean())
        sp2 = float(s["sp2"].mean())
        so = float(np.sqrt(max(sig_d2 * sp2, 0.0)))
        r_med = float(s["pr"].median())
        z_out.append(np.log(np.clip((r_med - 0.5) / N, mrd.Z_CLIP, 1.0)))
        s_out.append(so)
        rows.append((r_med, np.sqrt(sig_d2), sp2, so, s["ent"].nunique()))
    return dict(z=np.array(z_out), sigma_obs=np.array(s_out), N=N,
                table=pd.DataFrame(rows, columns=["rank", "sigma_d", "sum_p2",
                                                  "sigma_obsB", "n_ent"]))


if __name__ == "__main__":
    K_TOP = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    uni = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS["reddit"]), K_TOP, buffer_mult=4)
    daily = load_daily(set(uni["entity_id"].unique()))
    pA = mrd.estimate(uni, temper=True, min_knot_n=8, md_lags=6)
    for method in ("toeplitz", "splithalf", "residual"):
        cur = spec_b_curve(uni, daily, method=method)
        tag = " (PRIMARY)" if method == "toeplitz" else " (iid upper bound)"
        print(f"\n=== Spec-B sigma_obs(z) — {method}{tag} (reddit K={K_TOP}) ===")
        t = cur["table"].copy()
        t["specA_md"] = np.interp(cur["z"], pA.z_knots, pA.sigma_obs)
        print(t.round(3).to_string(index=False))
