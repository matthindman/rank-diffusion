#!/usr/bin/env python3
"""Spec-B-analogue check: does the CALIBRATED sigma_obs profile on the IG
totals panel match the EXTERNAL thinning-law prediction from n_posts?

Thinning envelope per band (delta method, weekly totals):
  c^2 * E[1/M]  <=  Var_thin(log Y)  <=  (1 + c^2) * E[1/M]
with c^2 ~= 0.25 (per-post P1 slope, ig_censoring_forensics2) — lower edge
if week-to-week M variation is entirely true posting dynamics, upper edge
if M ~ Poisson (pure thinning).  Compare to the fitted sigma_obs^2
head/tail values printed by the card run (0.209 head, 0.732 tail).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

PATH = "llm_fitting/ig_hm_totals.parquet"
C2 = 0.253


def main() -> None:
    df = pd.read_parquet(PATH)
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    T = len(weeks)
    df["t"] = df["date"].map({w: k for k, w in enumerate(weeks)})

    # absence-penalized permanent rank within this panel (same rule as build)
    df["r"] = df.groupby("t")["metric_value"].rank(ascending=False, method="first")
    floor = df.groupby("t")["r"].max() + 1.0
    tot_floor = floor.sum()
    d = df.copy()
    d["rmf"] = d["r"] - d["t"].map(floor)
    perm = (d.groupby("user_name")["rmf"].sum() + tot_floor) / T
    pr = perm.rank(method="first")
    d["perm_rank"] = d["user_name"].map(pr)

    bands = [(1, 100), (100, 300), (300, 1000), (1000, 3000), (3000, 10000)]
    print("band(perm rank)   E[1/M]   thin sigma range      n_acct  mean M")
    for lo, hi in bands:
        z = (d["perm_rank"] > lo - 1) & (d["perm_rank"] <= hi)
        invm = (1.0 / d.loc[z, "n_posts"].clip(lower=1)).mean()
        smin, smax = np.sqrt(C2 * invm), np.sqrt((1 + C2) * invm)
        print(f"  {lo:5d}-{hi:5d}    {invm:6.3f}   [{smin:.3f} .. {smax:.3f}]   "
          f"{d.loc[z,'user_name'].nunique():6,}  {d.loc[z,'n_posts'].mean():7.1f}")
    print("\nfitted sigma_obs (card, calibrated Spec-A): head 0.209 .. tail 0.732")


if __name__ == "__main__":
    main()
