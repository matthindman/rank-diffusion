#!/usr/bin/env python3
"""PREREG_2026-07-16 IG runners (A2.1 build + reconciliation gate,
A2.2 P10 i–iii, A3.1 P11). Run only per the registered phases.

  build      posts -> per-account-DAY panel (frozen conventions: id =
             user_name; post dedup on url keeping max total_interactions;
             engagement = total_interactions; date = post_created_date as
             exported; keeps n_posts; aggregate-safe columns only)
  reconcile  A2.1 gate: daily-derived weekly vs the analyzed weekly panel
             — EXACT bidirectional index + value equality on the modeled
             population; index equality + activity-weighted discrepancy
             <= 0.001 outside. ANY failure STOPS P10.
  p10 --top-k 10000   Spec-B tests i–iii (1/M slope, orientation,
             envelope 12/12 vs the recorded instagram_hm sigma_obs)
  p11        A3.1 synchronization test (frozen top-10k cohort, 52-week
             calendar matrix, whole-week circular-shift null)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

RAW = "data/ssd/raw_small/instagram/full_ig.parquet"
DAILY_OUT = "data/ssd/derived/ig_daily_2023.parquet"
WEEKS_2023 = pd.date_range("2023-01-02", "2023-12-25", freq="7D")  # 52 complete


def build(raw=RAW, out=DAILY_OUT):
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(raw)
    best = {}                     # url-hash -> (interactions, user, date)
    for batch in pf.iter_batches(
            batch_size=2_000_000,
            columns=["user_name", "post_created_date", "total_interactions",
                     "url"]):
        df = batch.to_pandas()
        h = pd.util.hash_array(df["url"].to_numpy(dtype=object))
        ti = pd.to_numeric(df["total_interactions"], errors="coerce").fillna(0)
        for hh, t, u, d in zip(h, ti, df["user_name"],
                               df["post_created_date"]):
            cur = best.get(hh)
            if cur is None or t > cur[0]:
                best[hh] = (float(t), u, d)
    agg = pd.DataFrame(best.values(),
                       columns=["metric_value", "user_name", "date"])
    agg["date"] = pd.to_datetime(agg["date"])
    daily = (agg.groupby(["user_name", agg["date"].dt.normalize()])
             .agg(metric_value=("metric_value", "sum"),
                  n_posts=("metric_value", "size")).reset_index())
    daily["metric_value"] = daily["metric_value"].round().astype("int64")
    daily.to_parquet(out, index=False)
    print(f"wrote {out}: {len(daily):,} account-days, "
          f"{daily['user_name'].nunique():,} accounts, "
          f"{daily['date'].min().date()}..{daily['date'].max().date()}; "
          f"posts kept {len(agg):,} (url-dedup keep-max)")


def dedup_posts(df):
    """Frozen dedup rule as a pure function (tested): unique url keeping
    max total_interactions."""
    return (df.sort_values("total_interactions")
            .drop_duplicates("url", keep="last"))


def weekly_from_daily(daily):
    wk = daily["date"] - pd.to_timedelta(pd.to_datetime(daily["date"]).dt.weekday,
                                         unit="D")
    w = (daily.assign(wk=wk).groupby(["user_name", "wk"], as_index=False)
         .agg(metric_value=("metric_value", "sum"),
              n_posts=("n_posts", "sum")))
    return w[w["wk"].isin(WEEKS_2023)].rename(columns={"wk": "date"})


def reconcile(daily_df, analyzed_weekly, modeled_ids):
    """A2.1 gate (pure function; raises SystemExit on failure).
    analyzed_weekly: DataFrame(user_name, date, metric_value).
    modeled_ids: the registered train-safe universe (every origin's set,
    union)."""
    dw = weekly_from_daily(daily_df).set_index(["user_name", "date"])
    aw = analyzed_weekly.set_index(["user_name", "date"])
    m_dw = dw[dw.index.get_level_values(0).isin(modeled_ids)]
    m_aw = aw[aw.index.get_level_values(0).isin(modeled_ids)]
    only_d = m_dw.index.difference(m_aw.index)
    only_a = m_aw.index.difference(m_dw.index)
    if len(only_d) or len(only_a):
        raise SystemExit(f"RECONCILE FAIL (modeled population): "
                         f"{len(only_d)} daily-only, {len(only_a)} "
                         f"weekly-only (entity, week) cells")
    if not np.array_equal(m_dw["metric_value"].sort_index().to_numpy(),
                          m_aw["metric_value"].sort_index().to_numpy()):
        n = int((m_dw["metric_value"].sort_index().to_numpy()
                 != m_aw["metric_value"].sort_index().to_numpy()).sum())
        raise SystemExit(f"RECONCILE FAIL: {n} modeled metric_value cells "
                         f"differ (integer sums admit no tolerance)")
    if "n_posts" in aw.columns:
        if not np.array_equal(m_dw["n_posts"].sort_index().to_numpy(),
                              m_aw["n_posts"].sort_index().to_numpy()):
            raise SystemExit("RECONCILE FAIL: modeled n_posts cells differ")
    o_dw, o_aw = dw.drop(m_dw.index), aw.drop(m_aw.index, errors="ignore")
    only_d = o_dw.index.difference(o_aw.index)
    only_a = o_aw.index.difference(o_dw.index)
    if len(only_d) or len(only_a):
        raise SystemExit(f"RECONCILE FAIL (outside population): index sets "
                         f"differ ({len(only_d)}/{len(only_a)})")
    both = o_dw.join(o_aw, rsuffix="_a")
    num = float((both["metric_value"] - both["metric_value_a"]).abs().sum())
    den = float(both["metric_value_a"].abs().sum())
    if den and num / den > 0.001:
        raise SystemExit(f"RECONCILE FAIL: activity-weighted discrepancy "
                         f"{num / den:.5f} > 0.001 outside the modeled set")
    print("RECONCILE PASS")


# ---------------- P11 (A3.1) ---------------- #
def cohort_matrix(daily, cohort_n=10_000):
    """(cohort_n x 364) daily n_posts>0 presence matrix over the 52 common
    complete weeks; missing account-day cells = 0 matching posts."""
    days = pd.date_range(WEEKS_2023[0], WEEKS_2023[-1] + pd.Timedelta(days=6),
                         freq="D")
    top = sorted(daily.groupby("user_name")["n_posts"].sum()
                 .nlargest(cohort_n).index)   # deterministic row order
    sub = daily[daily["user_name"].isin(top)]
    piv = (sub.pivot_table(index="user_name", columns="date",
                           values="n_posts", fill_value=0)
           .reindex(index=top, columns=days, fill_value=0))
    return (piv.to_numpy() > 0).astype(np.int8)   # 1 = active day


def p11_stat(active):
    """T_obs = variance across the 52 calendar weeks of the cohort-wide
    mean absent-day fraction."""
    absent = 1 - active
    weekly = absent.reshape(absent.shape[0], 52, 7).mean(axis=2)  # (n, 52)
    return float(np.var(weekly.mean(axis=0), ddof=0))


def p11_null(active, draws=500, seed=0):
    """Whole-week circular shifts per account: preserves each entity's
    total absence, weekday pattern, burstiness, run structure; destroys
    only cross-account synchronization."""
    rng = np.random.default_rng(seed)
    n = active.shape[0]
    byweek = active.reshape(n, 52, 7)
    out = []
    for _ in range(draws):
        shifts = rng.integers(0, 52, n)
        rolled = np.stack([np.roll(byweek[i], shifts[i], axis=0)
                           for i in range(n)])
        out.append(p11_stat(rolled.reshape(n, 364)))
    return np.array(out)


def p11(daily, cohort_n=10_000, draws=500, seed=0):
    m = cohort_matrix(daily, cohort_n)
    assert m.shape == (cohort_n, 364), m.shape
    t_obs = p11_stat(m)
    null = p11_null(m, draws, seed)
    q975 = float(np.percentile(null, 97.5))
    print(f"P11: T_obs {t_obs:.6f} vs null q97.5 {q975:.6f} "
          f"(null median {np.median(null):.6f}; {draws} draws seed {seed}) "
          f"-> {'PASS' if t_obs > q975 else 'FAIL'}")
    print("  claim ceiling on PASS: synchronized missingness CONSISTENT "
          "WITH instrument dropout (behavioral/seasonal shocks not "
          "excludable from presence data)")
    return t_obs > q975


# ---------------- P10 i–iii (A1.8/A2.2) ---------------- #
def p10_specb(top_k=10_000):
    import minimal_rankdiff as mrd
    import spec_b_sigma_obs as sb
    df = mrd.load_panel(mrd.PLATFORMS["instagram_hm"])
    df = mrd.restrict_universe(df, top_k, buffer_mult=4)
    daily_all = pd.read_parquet(DAILY_OUT).rename(
        columns={"user_name": "endpoint_id"})
    ids = set(df["entity_id"].unique())
    daily = daily_all[daily_all["endpoint_id"].isin(ids)]
    # guard convention (A2.2): drop weeks containing flagged days from the
    # floor estimation
    counts = daily.groupby("date")["endpoint_id"].size().sort_index()
    med = counts.shift(1).rolling(28, min_periods=14).median()
    flagged = counts[(med.notna()) & (counts < 0.6 * med)].index
    if len(flagged):
        bad_wk = set(pd.to_datetime(flagged)
                     - pd.to_timedelta(pd.to_datetime(flagged).weekday, unit="D"))
        wk = daily["date"] - pd.to_timedelta(
            pd.to_datetime(daily["date"]).dt.weekday, unit="D")
        daily = daily[~wk.isin(bad_wk)]
        print(f"  guard: {len(flagged)} flagged days -> "
              f"{len(bad_wk)} weeks dropped from floor estimation")
    cur = sb.spec_b_curve(df, daily[["date", "endpoint_id", "metric_value"]])
    z, sig = np.asarray(cur["z"]), np.asarray(cur["sigma_obs"])
    # per-band mean weekly n_posts (M) at matched z coordinates
    wk = weekly_from_daily(daily.rename(columns={"endpoint_id": "user_name"}))
    m_by_e = wk.groupby("user_name")["n_posts"].mean()
    pr = df.groupby("entity_id")[["z"]].mean()
    pr["M"] = pr.index.map(m_by_e)
    pr = pr.dropna()
    bands = np.digitize(pr["z"], np.quantile(z, np.linspace(0, 1, 13)[1:-1]))
    M_band = pr.groupby(bands)["M"].mean().to_numpy()[:12]
    from scipy.stats import spearmanr
    n = min(len(M_band), len(sig))
    rho = spearmanr(sig[:n] ** 2, 1.0 / M_band[:n]).statistic
    slope = np.polyfit(np.log(1.0 / M_band[:n]), np.log(sig[:n] ** 2), 1)[0]
    ok_i = (rho > 0) and (0.5 <= slope <= 1.5)
    print(f"P10(i) 1/M law: Spearman {rho:+.3f}, log-log slope {slope:.3f} "
          f"-> {'PASS' if ok_i else 'FAIL'}")
    third = max(1, len(sig) // 3)
    ok_ii = sig[:third].mean() < sig[-third:].mean()
    print(f"P10(ii) orientation head {sig[:third].mean():.4f} < tail "
          f"{sig[-third:].mean():.4f} -> {'PASS' if ok_ii else 'FAIL'}")
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
                     stat_factor=True)
    rec = np.interp(z, p.z_knots, p.sigma_obs)
    ok_iii = bool(np.all(rec >= sig - 1e-12))
    print(f"P10(iii) envelope: recorded sigma_obs >= centered floor at "
          f"{int((rec >= sig - 1e-12).sum())}/12 bands (12/12 required) "
          f"-> {'PASS' if ok_iii else 'FAIL'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=("build", "reconcile", "p10", "p11"))
    ap.add_argument("--top-k", type=int, default=10_000)
    ap.add_argument("--analyzed-weekly",
                    default="llm_fitting/ig_hm_totals_ts.parquet")
    ap.add_argument("--members",
                    default="llm_fitting/ig_trainsafe_members.parquet")
    a = ap.parse_args()
    if a.what == "build":
        build()
    elif a.what == "reconcile":
        daily = pd.read_parquet(DAILY_OUT)
        aw = pd.read_parquet(a.analyzed_weekly)
        ids = set(pd.read_parquet(a.members)["entity_id"].unique())
        reconcile(daily, aw, ids)
    elif a.what == "p10":
        p10_specb(a.top_k)
    else:
        p11(pd.read_parquet(DAILY_OUT))
