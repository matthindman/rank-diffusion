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


def _merge_uniques(parts):
    """Exact-URL merge (A4.5): concat per-batch unique-url aggregates and
    re-reduce. Keeps max interactions; tracks user/date min & max so
    conflicting duplicates FAIL at the end."""
    cat = pd.concat(parts, ignore_index=True)
    return cat.groupby("url", as_index=False).agg(
        ti=("ti", "max"), u_min=("u_min", "min"), u_max=("u_max", "max"),
        d_min=("d_min", "min"), d_max=("d_max", "max"))


def build(raw=RAW, out=DAILY_OUT):
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(raw)
    parts, n_rows = [], 0
    for batch in pf.iter_batches(
            batch_size=2_000_000,
            columns=["user_name", "post_created_date", "total_interactions",
                     "url"]):
        df = batch.to_pandas()
        n_rows += len(df)
        if df["url"].isna().any() or (df["url"].astype(str) == "").any():
            raise SystemExit("BUILD FAIL: null/empty url (A4.5 anomaly)")
        ti = pd.to_numeric(df["total_interactions"], errors="coerce")
        if ti.isna().any():
            raise SystemExit("BUILD FAIL: null/non-numeric "
                             "total_interactions (A4.5: no silent zero-fill)")
        if df["user_name"].isna().any() or df["post_created_date"].isna().any():
            raise SystemExit("BUILD FAIL: null user_name/post_created_date")
        b = pd.DataFrame({"url": df["url"].astype(str),
                          "ti": ti.astype(float),
                          "u_min": df["user_name"].astype(str),
                          "u_max": df["user_name"].astype(str),
                          "d_min": pd.to_datetime(df["post_created_date"]),
                          "d_max": pd.to_datetime(df["post_created_date"])})
        parts.append(b.groupby("url", as_index=False).agg(
            ti=("ti", "max"), u_min=("u_min", "min"), u_max=("u_max", "max"),
            d_min=("d_min", "min"), d_max=("d_max", "max")))
        if len(parts) >= 8:
            parts = [_merge_uniques(parts)]
    agg = _merge_uniques(parts)
    if (agg["u_min"] != agg["u_max"]).any() or (agg["d_min"] != agg["d_max"]).any():
        n = int(((agg["u_min"] != agg["u_max"])
                 | (agg["d_min"] != agg["d_max"])).sum())
        raise SystemExit(f"BUILD FAIL: {n} duplicate urls with conflicting "
                         f"(user_name, date) (A4.5 anomaly)")
    agg = agg.rename(columns={"u_min": "user_name", "d_min": "date",
                              "ti": "metric_value"})
    daily = (agg.groupby(["user_name", agg["date"].dt.normalize()])
             .agg(metric_value=("metric_value", "sum"),
                  n_posts=("metric_value", "size")).reset_index())
    daily["metric_value"] = daily["metric_value"].round().astype("int64")
    daily.to_parquet(out, index=False)
    print(f"wrote {out}: {len(daily):,} account-days, "
          f"{daily['user_name'].nunique():,} accounts, "
          f"{daily['date'].min().date()}..{daily['date'].max().date()}; "
          f"{n_rows:,} post rows -> {len(agg):,} unique urls "
          f"(exact-url dedup keep-max)")


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
    if "n_posts" not in aw.columns:
        raise SystemExit("RECONCILE FAIL: analyzed weekly panel has no "
                         "n_posts column (A4.6: required; data finding)")
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


# ---------------- P10 i–iii (A1.8/A2.2/A4.7/A4.8) ---------------- #
def p10_verdicts(sig, M_band, recorded_sigma, n_bands=12):
    """PURE P10 logic (testable both directions). sig: centered-floor
    sigma_obs per band; M_band: mean weekly n_posts per band ALIGNED to
    the same band membership; recorded_sigma: the recorded instagram_hm
    sigma_obs interpolated at the same 12 z coordinates.
    STRUCTURAL (A4.8): exactly n_bands bands must exist in all three."""
    from scipy.stats import spearmanr
    out = {}
    if not (len(sig) == len(M_band) == len(recorded_sigma) == n_bands):
        out["structural"] = False
        out["i"] = out["ii"] = out["iii"] = False
        return out
    out["structural"] = True
    rho = float(spearmanr(np.asarray(sig) ** 2, 1.0 / np.asarray(M_band)).statistic)
    slope = float(np.polyfit(np.log(1.0 / np.asarray(M_band)),
                             np.log(np.asarray(sig) ** 2), 1)[0])
    out["rho"], out["slope"] = rho, slope
    out["i"] = (rho > 0) and (0.5 <= slope <= 1.5)
    third = n_bands // 3
    out["ii"] = float(np.mean(sig[:third])) < float(np.mean(sig[-third:]))
    out["iii"] = bool(np.all(np.asarray(recorded_sigma) >= np.asarray(sig) - 1e-12))
    return out


def p10_specb(top_k=10_000):
    import minimal_rankdiff as mrd
    import spec_b_sigma_obs as sb
    df = mrd.load_panel(mrd.PLATFORMS["instagram_hm"])
    df = mrd.restrict_universe(df, top_k, buffer_mult=4)
    daily_all = pd.read_parquet(DAILY_OUT).rename(
        columns={"user_name": "endpoint_id"})
    # A4.7: day guard from PLATFORM-WIDE daily row counts, BEFORE any
    # modeled-account restriction
    counts = daily_all.groupby("date")["endpoint_id"].size().sort_index()
    med = counts.shift(1).rolling(28, min_periods=14).median()
    flagged = counts[(med.notna()) & (counts < 0.6 * med)].index
    ids = set(df["entity_id"].unique())
    daily = daily_all[daily_all["endpoint_id"].isin(ids)]
    if len(flagged):
        bad_wk = set(pd.to_datetime(flagged)
                     - pd.to_timedelta(pd.to_datetime(flagged).weekday,
                                       unit="D"))
        wk = daily["date"] - pd.to_timedelta(
            pd.to_datetime(daily["date"]).dt.weekday, unit="D")
        daily = daily[~wk.isin(bad_wk)]
        print(f"  guard (platform-wide): {len(flagged)} flagged days -> "
              f"{len(bad_wk)} weeks dropped from floor estimation")
    cur = sb.spec_b_curve(df, daily[["date", "endpoint_id", "metric_value"]],
                          return_members=True)
    z, sig = np.asarray(cur["z"]), np.asarray(cur["sigma_obs"])
    # A4.8: per-band M over spec_b_curve's OWN band membership
    npost = daily.set_index(["endpoint_id", "date"])["n_posts"]
    M_band = []
    for mem in cur["members"]:
        # mem = (entity, week) MultiIndex of the band's entity-weeks; M =
        # mean weekly posts over exactly those cells
        ents = mem.get_level_values(0)
        wks = pd.to_datetime(mem.get_level_values(1))
        vals = []
        for e, w in zip(ents, wks):
            days = pd.date_range(w, periods=7, freq="D")
            v = npost.reindex([(e, d) for d in days]).sum()
            vals.append(float(v))
        M_band.append(float(np.mean(vals)))
    p = mrd.estimate(df, temper=True, min_knot_n=8, md_lags=6, t_tails=True,
                     stat_factor=True)
    rec = np.interp(z, p.z_knots, p.sigma_obs)
    v = p10_verdicts(sig, np.asarray(M_band), rec)
    print(f"P10 structural 12 bands: "
          f"{'PASS' if v['structural'] else 'FAIL (skipped/short bands)'}")
    if v["structural"]:
        print(f"P10(i) 1/M: Spearman {v['rho']:+.3f}, slope {v['slope']:.3f} "
              f"-> {'PASS' if v['i'] else 'FAIL'}")
        print(f"P10(ii) orientation -> {'PASS' if v['ii'] else 'FAIL'}")
        print(f"P10(iii) envelope 12/12 -> {'PASS' if v['iii'] else 'FAIL'}")
    return v


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
        import hashlib
        want = ("130726eb194597fcbba67ca3eced29a5f8e5e20d34dcc8e3ae186e455"
                "ac75aac")
        got = hashlib.sha256(Path(a.members).read_bytes()).hexdigest()
        if got != want:
            raise SystemExit(f"RECONCILE FAIL: member file sha256 {got[:12]}"
                             f"... != registered {want[:12]}... (A4.6)")
        daily = pd.read_parquet(DAILY_OUT)
        aw = pd.read_parquet(a.analyzed_weekly)
        ids = set(pd.read_parquet(a.members)["entity_id"].unique())
        reconcile(daily, aw, ids)
    elif a.what == "p10":
        p10_specb(a.top_k)
    else:
        p11(pd.read_parquet(DAILY_OUT))
