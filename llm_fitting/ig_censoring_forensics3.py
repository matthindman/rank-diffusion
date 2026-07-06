#!/usr/bin/env python3
"""Full-panel IG instrument forensics + P2 re-test without the top-50k cut.

1. Instrument-health series: accounts/week, sum n_posts/week, sum metric/week,
   min metric/week (cap detection), new-ids/week.
2. P2 re-test: absence hazard of high-mean-posts accounts in the FULL panel.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

FULL = "llm_fitting/ig_weekly_ranked.parquet"
CUT = "llm_fitting/ig_weekly_ranked_top50k.parquet"


def main() -> None:
    tbl = pq.read_table(FULL, columns=["date", "user_name", "metric_value", "n_posts"])
    df = tbl.to_pandas()
    del tbl
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    T = len(weeks)
    df["t"] = df["date"].map({w: k for k, w in enumerate(weeks)})
    print(f"FULL panel: {len(df):,} rows, T={T} weeks "
          f"[{pd.Timestamp(weeks[0]).date()} .. {pd.Timestamp(weeks[-1]).date()}]")

    wk = df.groupby("t").agg(
        n_accounts=("user_name", "size"),
        sum_posts=("n_posts", "sum"),
        sum_metric=("metric_value", "sum"),
        min_metric=("metric_value", "min"),
        med_metric=("metric_value", "median"),
    )
    first_seen = df.groupby("user_name")["t"].min()
    new_by_week = first_seen.value_counts().sort_index()
    wk["new_ids"] = new_by_week.reindex(wk.index).fillna(0).astype(int)
    print("\nInstrument-health series (weekly):")
    print(wk.to_string(float_format=lambda v: f"{v:,.0f}"))

    # ---- P2 re-test on the full panel ----
    g = df.groupby("user_name", sort=False)
    acc = pd.DataFrame({
        "mposts": g["n_posts"].mean(),
        "npres": g["t"].size(),
        "mlogy": g["metric_value"].apply(lambda x: np.log(x[x > 0]).mean()),
    })
    acc["presfrac"] = acc["npres"] / T

    d2 = df[["user_name", "t"]].sort_values(["user_name", "t"])
    s2 = d2["user_name"].to_numpy()
    t2 = d2["t"].to_numpy()
    valid = t2 < T - 1
    nxt_same = np.zeros(len(t2), bool)
    nxt_same[:-1] = (s2[1:] == s2[:-1]) & (t2[1:] == t2[:-1] + 1)
    hz = pd.DataFrame({"user_name": s2, "absent": valid & ~nxt_same, "valid": valid})
    hzacc = hz.groupby("user_name").agg(n_valid=("valid", "sum"), n_abs=("absent", "sum"))
    hzacc = hzacc.join(acc[["mposts", "presfrac"]])
    hzacc = hzacc[hzacc["n_valid"] > 0]
    hzacc["hazard"] = hzacc["n_abs"] / hzacc["n_valid"]
    mb = [0, 1, 2, 3, 5, 10, 20, 50, 100, 10**9]
    hzacc["mb"] = pd.cut(hzacc["mposts"], mb, right=False)
    tab = hzacc.groupby("mb", observed=True).agg(
        n=("hazard", "size"), mean_mposts=("mposts", "mean"),
        hazard=("hazard", "mean"), pres=("presfrac", "mean"))
    tab["poisson_pred"] = np.exp(-tab["mean_mposts"])
    print("\nP2 re-test (FULL panel) — absence hazard by mean n_posts:")
    print(tab.to_string(float_format=lambda v: f"{v:.4f}"))
    hi = hzacc[hzacc["mposts"] >= 20]
    print(f"m̄>=20: n={len(hi):,}, mean hazard={hi['hazard'].mean():.4f}, "
          f"mean presence={hi['presfrac'].mean():.3f}")

    # presence-fraction distribution for the head (top 5k by level)
    head = acc.nlargest(5000, "mlogy")
    print("\ntop-5k-by-level presence fraction quantiles:",
          np.percentile(head["presfrac"], [5, 25, 50, 75, 95]).round(3))
    # eras? presence by half
    dfh = df[df["user_name"].isin(head.index)]
    ph = dfh.groupby("t")["user_name"].size()
    print("head accounts present per week (first 10 / last 10):")
    print(" ", ph.head(10).to_list(), "...", ph.tail(10).to_list())


if __name__ == "__main__":
    main()
