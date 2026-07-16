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


N_PARTS = 16     # A5.3: each url lives in exactly one partition


def _validate_batch(df):
    """A4.5/A5.3 strict validation: FAIL on null/empty urls, null ids or
    dates, and nonfinite/negative/fractional interaction counts."""
    if df["url"].isna().any() or (df["url"].astype(str) == "").any():
        raise SystemExit("BUILD FAIL: null/empty url (A4.5 anomaly)")
    ti = pd.to_numeric(df["total_interactions"], errors="coerce")
    a = ti.to_numpy(dtype=float)
    if ti.isna().any() or not np.isfinite(a).all():
        raise SystemExit("BUILD FAIL: null/non-numeric/non-finite "
                         "total_interactions (A4.5: no silent zero-fill)")
    if (a < 0).any():
        raise SystemExit("BUILD FAIL: negative total_interactions")
    if (a != np.floor(a)).any():
        raise SystemExit("BUILD FAIL: fractional total_interactions")
    if df["user_name"].isna().any() or df["post_created_date"].isna().any():
        raise SystemExit("BUILD FAIL: null user_name/post_created_date")
    return ti


BUILD_TMP = str(Path(RAW).parent / "_build_tmp")   # A6.2': url-bearing
# scratch stays under raw_small (the registered scope), never system temp


def build(raw=RAW, out=DAILY_OUT, tmp_dir=None):
    """A6.2' THREE-PASS truly-bounded external aggregation:
      pass 1  stream batches -> url-hash partitions (disk)
      pass 2  per url-partition: exact-url dedup keep-max + conflict FAIL,
              aggregate to account-day partials, REPARTITION by user-hash
              to disk (an account-day may span url-partitions)
      pass 3  per user-partition: merge partials, append through a
              streaming Parquet writer
    Peak memory = one partition at every stage. Failure-safe cleanup."""
    import shutil
    import pyarrow as pa
    import pyarrow.parquet as pq
    tmp = Path(tmp_dir or BUILD_TMP)
    try:
        tmp.mkdir(parents=True, exist_ok=True)
        pf = pq.ParquetFile(raw)
        counters = {i: 0 for i in range(N_PARTS)}
        n_rows = 0
        for batch in pf.iter_batches(
                batch_size=2_000_000,
                columns=["user_name", "post_created_date",
                         "total_interactions", "url"]):
            df = batch.to_pandas()
            n_rows += len(df)
            ti = _validate_batch(df)
            b = pd.DataFrame({"url": df["url"].astype(str),
                              "ti": ti.astype("int64"),
                              "user_name": df["user_name"].astype(str),
                              "date": pd.to_datetime(df["post_created_date"])})
            part = pd.util.hash_array(b["url"].to_numpy(dtype=object)) % N_PARTS
            for i, chunk in b.groupby(part):
                chunk.to_parquet(tmp / f"u{i:02d}_{counters[i]:04d}.parquet",
                                 index=False)
                counters[i] += 1
        n_urls = 0
        ctr2 = {j: 0 for j in range(N_PARTS)}
        for i in range(N_PARTS):
            files = sorted(tmp.glob(f"u{i:02d}_*.parquet"))
            if not files:
                continue
            part = pd.concat([pd.read_parquet(f) for f in files],
                             ignore_index=True)
            agg = part.groupby("url", as_index=False).agg(
                ti=("ti", "max"), u_min=("user_name", "min"),
                u_max=("user_name", "max"),
                d_min=("date", "min"), d_max=("date", "max"))
            bad = ((agg["u_min"] != agg["u_max"])
                   | (agg["d_min"] != agg["d_max"]))
            if bad.any():
                raise SystemExit(f"BUILD FAIL: {int(bad.sum())} duplicate "
                                 f"urls with conflicting (user_name, date) "
                                 f"(A4.5 anomaly)")
            n_urls += len(agg)
            d = (agg.rename(columns={"u_min": "user_name",
                                     "ti": "metric_value"})
                 .assign(date=agg["d_min"].dt.normalize())
                 .groupby(["user_name", "date"], as_index=False)
                 .agg(metric_value=("metric_value", "sum"),
                      n_posts=("metric_value", "size")))
            upart = (pd.util.hash_array(d["user_name"].to_numpy(dtype=object))
                     % N_PARTS)
            for j, chunk in d.groupby(upart):
                chunk.drop(columns=[]).to_parquet(
                    tmp / f"d{j:02d}_{ctr2[j]:04d}.parquet", index=False)
                ctr2[j] += 1
            for f in files:
                f.unlink()
            del part, agg, d
        writer = None
        n_days = n_accounts = 0
        dmin = dmax = None
        for j in range(N_PARTS):
            files = sorted(tmp.glob(f"d{j:02d}_*.parquet"))
            if not files:
                continue
            d = (pd.concat([pd.read_parquet(f) for f in files],
                           ignore_index=True)
                 .groupby(["user_name", "date"], as_index=False).sum())
            d["metric_value"] = d["metric_value"].astype("int64")
            d["n_posts"] = d["n_posts"].astype("int64")
            tbl = pa.Table.from_pandas(d, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out, tbl.schema)
            writer.write_table(tbl)
            n_days += len(d)
            n_accounts += d["user_name"].nunique()
            dmin = d["date"].min() if dmin is None else min(dmin, d["date"].min())
            dmax = d["date"].max() if dmax is None else max(dmax, d["date"].max())
            for f in files:
                f.unlink()
            del d
        if writer is not None:
            writer.close()
        print(f"wrote {out}: {n_days:,} account-days, {n_accounts:,} "
              f"accounts (partition-disjoint), "
              f"{dmin.date()}..{dmax.date()}; {n_rows:,} post rows -> "
              f"{n_urls:,} unique urls (exact-url, {N_PARTS}x{N_PARTS} "
              f"three-pass external)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)   # failure-safe: url-bearing
        # scratch never outlives the build


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


GUARDED_OUT = "data/ssd/derived/ig_daily_2023_guarded.parquet"


def apply_guard(daily):
    """A4.7 PLATFORM-WIDE day guard as a pure function: flag days with row
    count < 60% of the trailing prior-28-day median; drop every week
    containing a flagged day. Returns (filtered, flagged_days)."""
    counts = daily.groupby("date")["user_name"].size().sort_index()
    med = counts.shift(1).rolling(28, min_periods=14).median()
    flagged = counts[(med.notna()) & (counts < 0.6 * med)].index
    if not len(flagged):
        return daily, flagged
    fl = pd.to_datetime(flagged)
    bad_wk = set(fl - pd.to_timedelta(fl.weekday, unit="D"))
    wk = (pd.to_datetime(daily["date"])
          - pd.to_timedelta(pd.to_datetime(daily["date"]).dt.weekday,
                            unit="D"))
    return daily[~wk.isin(bad_wk)], flagged


def guard(inp=DAILY_OUT, out=GUARDED_OUT):
    daily = pd.read_parquet(inp)
    filtered, flagged = apply_guard(daily)
    filtered = filtered.rename(columns={"user_name": "endpoint_id"})
    filtered.to_parquet(out, index=False)
    print(f"guard: {len(flagged)} flagged days; wrote {out} "
          f"({len(filtered):,} of {len(daily):,} account-days)")


def band_M(members, weekly_nposts):
    """A5.3 vectorized per-band mean weekly n_posts over spec_b_curve's
    OWN band membership. weekly_nposts: Series indexed by
    (user_name, week-Monday)."""
    out = []
    for mem in members:
        vals = weekly_nposts.reindex(mem)
        out.append(float(vals.mean()))
    return np.array(out)


def p6_verdict(model_rel, base_rel, coverage):
    """A5.2 pure P6 rule."""
    return bool(model_rel <= base_rel + 0.05 and coverage >= 0.60)


def p10iv_verdict(model_rel, base_rel, coverage):
    """A5.2 pure P10(iv) rule (A2.2: P6 conditions + the restored
    |model - 0.320| <= 0.15 prediction)."""
    return bool(p6_verdict(model_rel, base_rel, coverage)
                and abs(model_rel - 0.320) <= 0.15)


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
    daily_all = pd.read_parquet(GUARDED_OUT)   # A5.2: pre-guarded
    # (platform-wide, ig_daily_2023.py guard; A4.7)
    ids = set(df["entity_id"].unique())
    daily = daily_all[daily_all["endpoint_id"].isin(ids)]
    cur = sb.spec_b_curve(df, daily[["date", "endpoint_id", "metric_value"]],
                          return_members=True)
    z, sig = np.asarray(cur["z"]), np.asarray(cur["sigma_obs"])
    # A4.8: per-band M over spec_b_curve's OWN band membership
    wknp = weekly_from_daily(daily.rename(columns={"endpoint_id":
                                                   "user_name"}))
    weekly_nposts = wknp.set_index(["user_name", "date"])["n_posts"]
    M_band = band_M(cur["members"], weekly_nposts)   # A5.3 vectorized
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
    ap.add_argument("what", choices=("build", "guard", "reconcile",
                                     "p10", "p11"))
    ap.add_argument("--top-k", type=int, default=10_000)
    ap.add_argument("--analyzed-weekly",
                    default="llm_fitting/ig_hm_totals_ts.parquet")
    ap.add_argument("--members",
                    default="llm_fitting/ig_trainsafe_members.parquet")
    a = ap.parse_args()
    if a.what == "build":
        build()
    elif a.what == "guard":
        guard()
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
