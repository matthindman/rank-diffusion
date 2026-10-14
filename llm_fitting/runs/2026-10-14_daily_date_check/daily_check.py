"""Daily check of the 28 June 2021 step and of the archive's short-delay pockets.  See DESIGN.md.

    caffeinate -dims python scripts/heavy_lock.py -- python -u llm_fitting/runs/2026-10-14_daily_date_check/daily_check.py
"""
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

DER = Path("data/ssd/derived")
OUT = Path(__file__).resolve().parent
FILES = {"submissions": ("reddit_daily_long.parquet", "submission_count"),
         "comments": ("reddit_comments_2018-12_2022-12_daily.parquet", "comment_count")}
PREDICTED = {"submissions": ["2019-08-25", "2019-08-26", "2019-08-27", "2019-08-28", "2019-08-29", "2019-08-30", "2019-08-31",
                             "2020-04-28", "2020-04-29", "2020-04-30", "2021-06-12", "2022-05-28", "2022-05-29", "2022-05-30"],
             "comments": []}
W0, W1 = pd.Timestamp("2021-05-03"), pd.Timestamp("2021-08-01")
SEL0, SEL1 = pd.Timestamp("2021-05-31"), pd.Timestamp("2021-06-27")
POCKET = [pd.Timestamp("2021-06-12")]
D = lambda s: pd.Timestamp(s)  # noqa: E731


def daily_totals(name):
    f, cnt = FILES[name]
    pf = pq.ParquetFile(DER / f)
    parts = []
    for i in range(pf.metadata.num_row_groups):
        t = pf.read_row_group(i, columns=["date", "metric_value", cnt]).to_pandas()
        parts.append(t.groupby("date").agg(karma=("metric_value", "sum"), items=(cnt, "sum"), rows=("metric_value", "size")))
    return pd.concat(parts).groupby(level=0).sum().sort_index()


def window(name):
    f, cnt = FILES[name]
    pf = pq.ParquetFile(DER / f)
    parts = []
    for i in range(pf.metadata.num_row_groups):
        st = pf.metadata.row_group(i).column(0).statistics
        if st.max < W0 or st.min > W1:
            continue
        t = pf.read_row_group(i, columns=["date", "endpoint_id", "metric_value", cnt]).to_pandas()
        parts.append(t[(t.date >= W0) & (t.date <= W1)])
    df = pd.concat(parts, ignore_index=True).rename(columns={cnt: "items", "metric_value": "karma"})
    df["endpoint_id"] = df["endpoint_id"].astype("category")
    return df


def pocket_screen(tot, name):
    kpi = (tot.karma / tot["items"]).rename("kpi")
    ratio = pd.Series(np.nan, index=kpi.index)
    v = kpi.reindex(pd.date_range(kpi.index.min(), kpi.index.max()))
    for t in v.index:
        nb = [v.get(t + pd.Timedelta(days=7 * j)) for j in (-4, -3, -2, -1, 1, 2, 3, 4)]
        nb = [x for x in nb if x is not None and np.isfinite(x)]
        if len(nb) >= 4 and np.isfinite(v[t]):
            ratio[t] = v[t] / np.median(nb)
    low = ratio[ratio < 0.7]
    print(f"\n[{name}] pocket screen: {tot.shape[0]} days {tot.index.min().date()} to {tot.index.max().date()}; "
          f"days with karma per item below 0.7 of the local same-weekday median: {len(low)}")
    for t, r in low.items():
        print(f"    {t.date()} {t.day_name()[:3]}  ratio {r:.3f}   karma per item {kpi[t]:.2f}   items {int(tot.loc[t, 'items'])}")
    if PREDICTED[name]:
        print("  predicted dates, whatever the ratio:")
        for s in PREDICTED[name]:
            t = D(s)
            print(f"    {t.date()} {t.day_name()[:3]}  ratio {ratio.get(t, np.nan):.3f}")
    print(f"  ratio quantiles over the panel: 1% {ratio.quantile(.01):.3f}, 5% {ratio.quantile(.05):.3f}, median {ratio.median():.3f}")
    return ratio


def series(df):
    sel = df[(df.date >= SEL0) & (df.date <= SEL1) & ~df.date.isin(POCKET)]
    order = sel.groupby("endpoint_id", observed=True).karma.sum().sort_values(ascending=False)
    top5, middle = set(order.index[:5]), set(order.index[200:1000])
    g = df.groupby("date")
    tot_k, tot_n = g.karma.sum(), g["items"].sum()
    out = pd.DataFrame(index=tot_k.index)
    for label, s in (("top5", top5), ("middle", middle)):
        sub = df[df.endpoint_id.isin(s)].groupby("date")
        k, n = sub.karma.sum(), sub["items"].sum()
        out[f"{label}_karma_share"] = np.log(k / tot_k)
        out[f"{label}_item_share"] = np.log(n / tot_n)
        if label == "top5":
            out["top5_karma_per_item_vs_rest"] = np.log(k / n) - np.log((tot_k - k) / (tot_n - n))
    pos1, posmid = {}, {}
    for t, x in g:
        v = np.sort(x.karma.to_numpy())[::-1]
        pos1[t] = np.log(v[0] / v.sum())
        posmid[t] = np.log(v[200:1000].sum() / v.sum())
    out["position1_karma_share"] = pd.Series(pos1)
    out["positions201_1000_karma_share"] = pd.Series(posmid)
    out["platform_karma_per_item"] = np.log(tot_k / tot_n)
    return out.sort_index()


def analyse(out, name):
    d7 = out - out.shift(7, freq="D")
    quiet = d7[(d7.index >= D("2021-05-10")) & (d7.index <= D("2021-06-20"))]
    bad = [t for t in quiet.index if t in POCKET or (t - pd.Timedelta(days=7)) in POCKET]
    quiet = quiet.drop(index=bad)
    noise = 1.4826 * (quiet - quiet.median()).abs().median()
    res = {}
    print(f"\n[{name}] noise scale of d7 (10 May to 20 June), onset, and share in place before 1 July:")
    for c in out.columns:
        z = d7[c] / noise[c]
        onset = None
        for t in pd.date_range("2021-06-21", "2021-07-14"):
            w = [z.get(t + pd.Timedelta(days=j), np.nan) for j in range(3)]
            if all(np.isfinite(w)) and all(abs(x) > 3 for x in w) and len({np.sign(x) for x in w}) == 1:
                onset = t
                break
        m = lambda a, b: out.loc[D(a):D(b), c].mean()  # noqa: E731
        base, mid, late = m("2021-06-21", "2021-06-23"), m("2021-06-28", "2021-06-30"), m("2021-07-05", "2021-07-07")
        Dstat = (mid - base) / (late - base) if abs(late - base) > 1e-12 else np.nan
        res[c] = dict(noise=float(noise[c]), onset=None if onset is None else str(onset.date()), D=float(Dstat),
                      change_to_5_7_july=float(late - base), change_in_noise_units=float((late - base) / noise[c]))
        print(f"  {c:<32} noise {noise[c]:.4f}  onset {res[c]['onset']}  change 21-23 Jun to 5-7 Jul {late - base:+.3f} "
              f"({(late - base) / noise[c]:+.1f} noise)  D {Dstat:+.2f}")
    cols = list(out.columns)
    print(f"\n[{name}] d7 in noise units, 14 June to 20 July (columns: {', '.join(cols)})")
    for t in pd.date_range("2021-06-14", "2021-07-20"):
        mark = "  <- 28 June" if t == D("2021-06-28") else ("  <- 1 July" if t == D("2021-07-01") else "")
        print(f"  {t.date()} {t.day_name()[:3]}  " + "  ".join(f"{d7.loc[t, c] / noise[c]:+6.1f}" for c in cols) + mark)
    print(f"\n[{name}] levels, 21 June to 7 July (log; same columns)")
    for t in pd.date_range("2021-06-21", "2021-07-07"):
        print(f"  {t.date()} {t.day_name()[:3]}  " + "  ".join(f"{out.loc[t, c]:+7.3f}" for c in cols))
    return res


if __name__ == "__main__":
    allout = []
    for name in ("submissions", "comments"):
        tot = daily_totals(name)
        pocket_screen(tot, name)
        df = window(name)
        print(f"\n[{name}] window {W0.date()} to {W1.date()}: {len(df)} rows, {df.endpoint_id.nunique()} subreddits")
        out = series(df)
        analyse(out, name)
        allout.append(out.add_prefix(name + "__"))
    pd.concat(allout, axis=1).round(5).to_csv(OUT / "daily_series.csv")
