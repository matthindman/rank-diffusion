#!/usr/bin/env python3
"""(a) Is high-m̄ account absence week-correlated (instrument) or independent
(behavior)?  (b) Concentration stats on the measurable population to
pre-register K.  (c) P3 aggregation-scaling check directly on the data."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

FULL = "llm_fitting/ig_weekly_ranked.parquet"


def main() -> None:
    df = pq.read_table(FULL, columns=["date", "user_name", "metric_value", "n_posts"]).to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    weeks = np.sort(df["date"].unique())
    df["t"] = df["date"].map({w: k for k, w in enumerate(weeks)})
    T = len(weeks)
    df = df[df["t"] > 0]  # drop partial week 0

    g = df.groupby("user_name", sort=False)
    acc = pd.DataFrame({"mposts": g["n_posts"].mean(), "npres": g["t"].size()})

    # ---- (a) cohort-week absence structure for prolific accounts ----
    coh = acc[(acc["mposts"] >= 10) & (acc["npres"] >= 13)].index
    sub = df[df["user_name"].isin(coh)]
    pres = sub.pivot_table(index="user_name", columns="t",
                           values="n_posts", aggfunc="size").notna()
    pres = pres.reindex(columns=range(1, T), fill_value=False)
    rate = 1 - pres.mean(axis=0)  # cohort absence rate per week
    print(f"cohort: {len(coh):,} accounts (m̄>=10, npres>=13)")
    print("cohort weekly ABSENCE rate: mean={:.3f} sd={:.3f} min={:.3f} max={:.3f}".format(
        rate.mean(), rate.std(), rate.min(), rate.max()))
    print("  worst 8 weeks:", rate.nlargest(8).round(3).to_dict())
    print("  best 4 weeks :", rate.nsmallest(4).round(3).to_dict())
    # binomial-independence benchmark: sd of weekly rate if iid across accounts
    p = rate.mean()
    print(f"  iid benchmark sd = {np.sqrt(p*(1-p)/len(coh)):.4f} "
          f"(observed {rate.std():.4f}; >>1x means week-level common shocks)")

    # ---- (b) concentration stats on the measurable population ----
    meas = acc[acc["npres"] >= 13].index
    subm = df[df["user_name"].isin(meas)]
    print(f"\nmeasurable population (npres>=13/52): {len(meas):,} accounts, "
          f"{len(subm):,} rows")
    shares = {}
    for K in [500, 1000, 2500, 5000, 10000, 20000]:
        s_k = []
        for t, grp in subm.groupby("t"):
            v = grp["metric_value"].to_numpy(float)
            v.sort()
            tot = v.sum()
            s_k.append(v[-K:].sum() / tot if tot > 0 else np.nan)
        shares[K] = np.nanmean(s_k)
    print("mean weekly top-K share of measurable-population activity:")
    for K, s in shares.items():
        print(f"  K={K:6d}: {s:.3f}")

    # ---- (c) P3: aggregation scaling of movement for a fixed prolific cohort ----
    print("\nP3 aggregation check (cohort m̄>=10, consecutive-present pairs):")
    subc = sub[sub["metric_value"] > 0].copy()
    for agg_w in [1, 2, 4]:
        c = subc.copy()
        c["ta"] = (c["t"] - 1) // agg_w
        a = c.groupby(["user_name", "ta"]).agg(
            y=("metric_value", "sum"), m=("n_posts", "sum"), k=("t", "size"))
        a = a[a["k"] == agg_w]  # fully-present periods only
        a = a.reset_index().sort_values(["user_name", "ta"])
        s_ = a["user_name"].to_numpy(); t_ = a["ta"].to_numpy()
        ly = np.log(a["y"].to_numpy(float)); m_ = a["m"].to_numpy(float)
        nxt = (s_[1:] == s_[:-1]) & (t_[1:] == t_[:-1] + 1)
        d2 = (ly[1:][nxt] - ly[:-1][nxt]) ** 2
        h = 1.0 / m_[1:][nxt] + 1.0 / m_[:-1][nxt]
        print(f"  {agg_w}w: pairs={nxt.sum():7,}  mean(dlogY)^2={d2.mean():.3f}  "
              f"mean thinning regressor={h.mean():.4f}")


if __name__ == "__main__":
    main()
