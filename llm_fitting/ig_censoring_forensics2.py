#!/usr/bin/env python3
"""P1 refinement: separate mechanical dlog M from per-post mean dynamics.

Decomposition: log Y = log M + log Ybar (Ybar = per-post mean engagement).
Under thinning, Var(dlog Ybar | M_t, M_t+1) = c^2 * (1/M_t + 1/M_t+1) + true
per-post dynamics; the q_i selection bias cancels in Ybar entirely.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PATH = "llm_fitting/ig_weekly_ranked_top50k.parquet"


def ols(x, yv):
    x1 = np.column_stack([np.ones_like(x), x])
    beta, *_ = np.linalg.lstsq(x1, yv, rcond=None)
    return beta


def main() -> None:
    df = pd.read_parquet(PATH, columns=["date", "user_name", "metric_value", "n_posts"])
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    df["t"] = df["date"].map({w: k for k, w in enumerate(weeks)})
    df = df[df["metric_value"] > 0]
    df = df.sort_values(["user_name", "t"])
    s = df["user_name"].to_numpy()
    t = df["t"].to_numpy()
    ly = np.log(df["metric_value"].to_numpy(float))
    m = df["n_posts"].to_numpy(float)
    lm = np.log(m)
    lybar = ly - lm

    nxt = (s[1:] == s[:-1]) & (t[1:] == t[:-1] + 1)
    dly = ly[1:][nxt] - ly[:-1][nxt]
    dlm = lm[1:][nxt] - lm[:-1][nxt]
    dlyb = lybar[1:][nxt] - lybar[:-1][nxt]
    hreg = 1.0 / m[1:][nxt] + 1.0 / m[:-1][nxt]

    print(f"pairs: {len(dly):,}")
    print(f"var components: var(dlogY)={dly.var():.3f} = var(dlogM)={dlm.var():.3f} "
          f"+ var(dlogYbar)={dlyb.var():.3f} + 2cov={2*np.cov(dlm,dlyb)[0,1]:.3f}")

    b0, b1 = ols(hreg, dlyb ** 2)
    print(f"\nper-post mean regression: (dlog Ybar)^2 = {b0:.3f} + {b1:.3f} * hreg")
    print("binned:  mean_h   mean_dlogYbar2   n")
    bins = np.quantile(hreg, np.linspace(0, 1, 11))
    ib = np.clip(np.searchsorted(bins, hreg, side="right") - 1, 0, 9)
    for k in range(10):
        z = ib == k
        print(f"   {hreg[z].mean():8.3f}  {(dlyb[z]**2).mean():8.3f}   {z.sum():,}")

    # high-M limit: the residual per-post dynamics (the 'true movement' target)
    for label, z in [("M>=30 both", (m[1:][nxt] >= 30) & (m[:-1][nxt] >= 30)),
                     ("M>=50 both", (m[1:][nxt] >= 50) & (m[:-1][nxt] >= 50)),
                     ("M>=100 both", (m[1:][nxt] >= 100) & (m[:-1][nxt] >= 100))]:
        if z.sum() > 200:
            imp = b1 * hreg[z].mean()
            print(f"{label}: n={z.sum():,}  (dlogYbar)^2={ (dlyb[z]**2).mean():.3f} "
                  f"(implied thinning {imp:.3f})  (dlogY)^2={(dly[z]**2).mean():.3f}")

    # kurtosis sanity of per-post changes at high M (t-tails live here)
    z = (m[1:][nxt] >= 30) & (m[:-1][nxt] >= 30)
    x = dlyb[z]
    print(f"\nhigh-M dlogYbar: sd={x.std():.3f}, excess kurtosis={pd.Series(x).kurt():.2f}")


if __name__ == "__main__":
    main()
