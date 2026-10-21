"""R17 of PLAN.md (after referee round 8; owner, 2026-10-21: "do all 3 tiers"): the asymmetry as one banded statistic,
the remaining checks, and the heavier model runs.

The primary asymmetry statistic (fixed in PLAN.md addendum R17 before any run), computed by the same code on data and
worlds: for origins every 13 weeks, x and y are the year-apart changes of 13-week window means sharing no weeks (gap
13; r12_hetgrowth.window_changes), demeaned within the three strata of permanent rank.  In the bottom and top deciles of
x, the persistence of a fall and of a rise are the ratios
        p_fall = mean(y | x <= q10) / mean(x | x <= q10),     p_rise = mean(y | x >= q90) / mean(x | x >= q90),
which are equal under any persistence that is linear in x, whatever the skew of x.  The asymmetry is D = p_fall - p_rise,
averaged over origins.  Bands: 200 bootstrap draws over communities within strata.  The decile curve (mean y against
mean x by decile of x) is reported beside it.

    python r17_round8.py asym_data <panel>                         D, curves, bands, robustness splits, by stratum
    python r17_round8.py asym_worlds <panel> <variant> <s0> <s1> <json params>
    python r17_round8.py asym_summ <panel> <tag>
    python r17_round8.py table_data <panel>                        null_worlds.stats on the data (Table 1, H9c)
    python r17_round8.py events <panel>                            the head communities behind the 2021 mid-month steps
    python r17_round8.py durability2 <panel> <control_num|joint>   R16-F extended (lasting levels, 520 weeks, sigma step)
    python r17_round8.py jgrid2 <panel>                            the joint grid past its edges
    python r17_round8.py jcalib <panel>                            24-moment objective under correct specification;
                                                                   slow-swing false positive
    python r17_round8.py jdgrid2 <panel>                           R16-G redone: spreads traded, km free
"""
import gc
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))
import mts_step1 as m1  # noqa: E402
import r12_hetgrowth as h  # noqa: E402
import r16_tier3 as t3  # noqa: E402

NB = 200
JOINT = {"reddit_submissions_long": dict(s=0.8, head=0.25, km=0.5, m=0.8), "reddit_comments_ext": dict(s=0.6, head=0.25, km=0.5, m=0.7)}
DECL = {"reddit_submissions_long": dict(pi=0.25, s=0.7, hl=208, m=1.0), "reddit_comments_ext": dict(pi=0.25, s=0.7, hl=208, m=0.7)}


# --------------------------------------------------------------------------- the primary statistic
def _pairs(X, lab, g=13, origins=None):
    """Stratum-demeaned (x, y) per origin, with the origin's first week."""
    T = X.shape[0]
    out = []
    a = 0
    while a + 2 * (h.SPAN + h.W) + g <= T:
        if origins is None or a in origins:
            x, y = h.window_changes(X, a), h.window_changes(X, a + h.SPAN + h.W + g)
            ok = (lab >= 0) & np.isfinite(x) & np.isfinite(y)
            xd, yd = x.copy(), y.copy()
            for j in range(3):
                s = ok & (lab == j)
                if s.sum() > 30:
                    xd[s] -= x[s].mean()
                    yd[s] -= y[s].mean()
            out.append((a, xd, yd, ok))
        a += 13
    return out


def tail_ratios(xd, yd, ok, q=0.1):
    xo, yo = xd[ok], yd[ok]
    if xo.size < 100:
        return np.nan, np.nan
    ql, qh = np.quantile(xo, [q, 1 - q])
    lo, hi = xo <= ql, xo >= qh
    return float(yo[lo].mean() / xo[lo].mean()), float(yo[hi].mean() / xo[hi].mean())


def deciles(xd, yd, ok):
    xo, yo = xd[ok], yd[ok]
    qs = np.quantile(xo, np.linspace(0, 1, 11))
    bins = np.clip(np.searchsorted(qs, xo, side="right") - 1, 0, 9)
    return [[float(xo[bins == b].mean()), float(yo[bins == b].mean())] for b in range(10)]


def asym_stat(X, lab, g=13, origins=None, strata=None):
    """D, p_fall, p_rise (origin means), the decile curve (origin mean) and origin-level D."""
    labx = lab if strata is None else np.where(np.isin(lab, strata), lab, -1)
    P = _pairs(X, labx, g, origins)
    pf, pr, cur, per = [], [], [], []
    for a, xd, yd, ok in P:
        f, r = tail_ratios(xd, yd, ok)
        if np.isfinite(f) and np.isfinite(r):
            pf.append(f)
            pr.append(r)
            per.append(dict(origin=int(a), D=f - r))
            cur.append(deciles(xd, yd, ok))
    if not pf:
        return None
    return dict(D=float(np.mean(pf) - np.mean(pr)), p_fall=float(np.mean(pf)), p_rise=float(np.mean(pr)),
                curve=np.mean(np.array(cur), 0).tolist(), by_origin=per, n_origins=len(pf))


def boot(X, lab, fn, n=NB, seed=17):
    rng = np.random.default_rng(seed)
    vals = []
    for _ in range(n):
        cols = np.concatenate([rng.choice(np.flatnonzero(lab == j), (lab == j).sum(), replace=True) for j in range(3)])
        r = fn(X[:, cols], lab[cols])
        if r is not None:
            vals.append([r["D"], r["p_fall"], r["p_rise"]])
    v = np.array(vals)
    return dict(D=np.quantile(v[:, 0], [0.05, 0.5, 0.95]).tolist(), p_fall=np.quantile(v[:, 1], [0.05, 0.95]).tolist(),
                p_rise=np.quantile(v[:, 2], [0.05, 0.95]).tolist(), n=int(len(v)))


def _dump(path, obj):
    path.write_text(json.dumps(obj, indent=1, default=float))


# --------------------------------------------------------------------------- data
def mode_asym_data(pn):
    df, K = h.load(pn)
    X, R, floors, outside, ids = m1.matrices(df, K)
    dates = df.groupby("period")["ts"].first().sort_index()
    T = X.shape[0]
    lab = h.strata(R, floors, K)
    out = dict(panel=pn, K=K)
    out["primary"] = asym_stat(X, lab)
    out["primary_band"] = boot(X, lab, lambda Xb, lb: asym_stat(Xb, lb))
    # robustness splits (registered in R17)
    wk = lambda d: int(np.searchsorted(dates.to_numpy(), np.datetime64(d)))
    jul21 = wk("2021-07-01")
    allo = [p[0] for p in _pairs(X, lab)]
    early, late = allo[: len(allo) // 2], allo[len(allo) // 2:]
    out["early_origins"] = dict(origins=early, **(asym_stat(X, lab, origins=set(early)) or {}))
    out["late_origins"] = dict(origins=late, **(asym_stat(X, lab, origins=set(late)) or {}))
    pre4 = [p[0] for p in _pairs(X, lab, g=4) if p[0] + 2 * (h.SPAN + h.W) + 4 <= jul21]      # gap 4: both changes end before July 2021
    out["before_july2021_gap4"] = dict(origins=pre4, **(asym_stat(X, lab, g=4, origins=set(pre4)) or {}))
    import r16_capture as cap
    col = {c[0] for c in cap.terminal_collapses(df)}
    keep = np.array([str(e) not in col for e in ids])
    out["without_collapses"] = dict(n_dropped=int((~keep).sum()), **asym_stat(X[:, keep], h.strata(R[:, keep], floors, K)))
    Xf = np.where(np.isfinite(X), X, np.nanmin(X, axis=1)[:, None])       # absent weeks at that week's universe floor
    out["exits_at_floor"] = asym_stat(Xf, lab)
    out["by_stratum"] = {str(j): asym_stat(X, lab, strata=[j]) for j in range(3)}
    out["by_stratum_band"] = {str(j): boot(X, lab, lambda Xb, lb, j=j: asym_stat(Xb, lb, strata=[j]), n=100) for j in range(3)}
    out["spearman_split"] = h.asymmetry(X, lab)
    _dump(HERE / f"r17_asym_data__{pn}.json", out)
    pb = out["primary_band"]
    print(f"{pn}: D {out['primary']['D']:+.3f} [{pb['D'][0]:+.3f}, {pb['D'][2]:+.3f}] (p_fall {out['primary']['p_fall']:+.3f}, "
          f"p_rise {out['primary']['p_rise']:+.3f}); early {out['early_origins'].get('D', np.nan):+.3f}, late {out['late_origins'].get('D', np.nan):+.3f}; "
          f"before Jul 2021 (gap 4) {out['before_july2021_gap4'].get('D', np.nan):+.3f} ({len(pre4)} origins); no collapses {out['without_collapses']['D']:+.3f}; "
          f"exits at floor {out['exits_at_floor']['D']:+.3f}; by stratum {[round(out['by_stratum'][str(j)]['D'], 3) for j in range(3)]}", flush=True)


def _item_matrix(pn, df, K):
    """Weekly item counts (submissions or comments) for the universe's members, from the T9 daily panel, streamed;
    log(1 + count), aligned to the universe frame's periods and entity order (m1.matrices)."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    import r16_capture as cap
    X, R, floors, _, ids = m1.matrices(df, K)
    ids = np.asarray(ids).astype(str)
    weeks = df.groupby("period")["ts"].first().sort_index()
    path, ccol, _ = cap.DAILY[pn]
    pf = pq.ParquetFile(h.ROOT / path)
    parts = []
    for i in range(pf.num_row_groups):
        tb = pf.read_row_group(i, columns=["date", "endpoint_id", ccol])
        tb = tb.filter(pc.is_in(tb["endpoint_id"], value_set=pa.array(sorted(set(ids)))))
        if tb.num_rows:
            g = tb.to_pandas()
            g["date"] = pd.to_datetime(g["date"])
            g = g[g["date"] < weeks.iloc[-1] + pd.Timedelta(days=7)]        # no day after the panel's last week
            g["period"] = np.searchsorted(weeks.to_numpy(), g["date"].to_numpy(), side="right") - 1
            parts.append(g.groupby(["period", "endpoint_id"])[ccol].sum().reset_index())
        del tb
    G = pd.concat(parts).groupby(["period", "endpoint_id"])[ccol].sum().reset_index()
    G = G[(G["period"] >= 0) & (G["period"] < X.shape[0])]
    piv = G.pivot(index="period", columns="endpoint_id", values=ccol).reindex(index=range(X.shape[0]), columns=ids)
    Xi = np.log1p(piv.to_numpy(float))
    Xi = np.where(np.isfinite(X), np.nan_to_num(Xi, nan=0.0), np.nan)      # present weeks as in the karma panel
    return Xi, X, R, floors, ids, weeks


def mode_asym_items(pn):
    """R17-B3: D on item counts, in R17-A's construction and in the bridge protocol's construction translated back two
    years onto development weeks (change 1 = 2019 Q3 -> 2020 Q3, 13-week windows; change 2 = 9-week windows
    2021-04-05 -> 2022-04-04, 27 weeks after change 1's last week; cohort the top K by permanent rank over 2019 H1)."""
    df, K = h.load(pn)
    Xi, X, R, floors, ids, weeks = _item_matrix(pn, df, K)
    del df
    gc.collect()
    lab = h.strata(R, floors, K)
    out = dict(panel=pn, K=K)
    out["items_r17a"] = asym_stat(Xi, lab)
    out["items_r17a_band"] = boot(Xi, lab, lambda Xb, lb: asym_stat(Xb, lb))
    out["items_by_stratum"] = {str(j): asym_stat(Xi, lab, strata=[j]) for j in range(3)}
    wk = lambda d: int(np.searchsorted(weeks.to_numpy(), np.datetime64(d)))
    import balance_accounting as ba

    def proto(Xm):
        c0, c1 = wk("2019-01-07"), wk("2019-07-01")
        pr, _ = ba.perm_rank(R, c0, c1, floors)
        o = np.argsort(pr, kind="stable")
        labp = np.full(R.shape[1], -1)
        for j, (lo, hi) in enumerate(((0, K // 16), (K // 16, K // 4), (K // 4, K))):
            labp[o[lo:hi]] = j
        w = lambda a, n: h.wmean(Xm, a, a + n)
        x = w(wk("2020-07-06"), 13) - w(wk("2019-07-01"), 13)
        y = w(wk("2022-04-04"), 9) - w(wk("2021-04-05"), 9)
        ok = (labp >= 0) & np.isfinite(x) & np.isfinite(y)
        xd, yd = x.copy(), y.copy()
        for j in range(3):
            s = ok & (labp == j)
            xd[s] -= x[s].mean()
            yd[s] -= y[s].mean()
        f, r = tail_ratios(xd, yd, ok)
        return dict(D=f - r, p_fall=f, p_rise=r, n=int(ok.sum())), labp, xd, yd, ok

    for name, Xm in (("items", Xi), ("karma", X)):
        res, labp, xd, yd, ok = proto(Xm)
        rng = np.random.default_rng(18)
        vals = []
        for _ in range(NB):
            cols = np.concatenate([rng.choice(np.flatnonzero(ok & (labp == j)), (ok & (labp == j)).sum(), replace=True) for j in range(3)])
            f, r = tail_ratios(xd[cols], yd[cols], np.ones(cols.size, bool))
            vals.append(f - r)
        res["band"] = np.quantile(vals, [0.05, 0.5, 0.95]).tolist()
        out[f"protocol_{name}"] = res
    _dump(HERE / f"r17_asym_items__{pn}.json", out)
    print(f"{pn}: items D (R17-A) {out['items_r17a']['D']:+.3f} {np.round(out['items_r17a_band']['D'], 3).tolist()}; "
          f"protocol construction: items {out['protocol_items']['D']:+.3f} {np.round(out['protocol_items']['band'], 3).tolist()}, "
          f"karma {out['protocol_karma']['D']:+.3f} {np.round(out['protocol_karma']['band'], 3).tolist()}", flush=True)


def mode_asym_worlds(pn, variant, s0, s1, prm):
    df, K, T, p, base, ladder, L0 = t3._setup(pn)
    del df
    gc.collect()
    q = t3.build(variant, p, ladder, K, prm)
    tag = f"{variant}_{'_'.join(f'{k}{v}' for k, v in prm.items())}"
    for seed in range(s0, s1 + 1):
        fr, _, _ = h.sim_frame(q, L0, T, seed, K)
        X, R, floors, _, _ = m1.matrices(fr, K)
        lab = h.strata(R, floors, K)
        r = asym_stat(X, lab)
        r["by_stratum"] = {str(j): asym_stat(X, lab, strata=[j]) for j in range(3)}
        r["spearman_split"] = h.asymmetry(X, lab)
        r.update(seed=seed, prm=prm)
        _dump(HERE / f"r17_asym_world__{pn}__{tag}__s{seed}.json", r)
        print(f"{pn} {tag} seed {seed}: D {r['D']:+.3f} (p_fall {r['p_fall']:+.3f}, p_rise {r['p_rise']:+.3f})", flush=True)
        del fr, X, R
        gc.collect()


def mode_asym_summ(pn, tag):
    W = [json.loads(f.read_text()) for f in sorted(HERE.glob(f"r17_asym_world__{pn}__{tag}__s*.json"))]
    band = lambda v: dict(mean=float(np.mean(v)), q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)), n=len(v))
    out = dict(panel=pn, tag=tag, n=len(W), D=band([w["D"] for w in W]), p_fall=band([w["p_fall"] for w in W]),
               p_rise=band([w["p_rise"] for w in W]), curve=np.mean([w["curve"] for w in W], 0).tolist(),
               by_stratum={str(j): band([w["by_stratum"][str(j)]["D"] for w in W if w["by_stratum"][str(j)]]) for j in range(3)})
    _dump(HERE / f"r17_asym_worlds__{pn}__{tag}.json", out)
    print(json.dumps({k: out[k] for k in ("n", "D", "p_fall", "p_rise", "by_stratum")}, default=float), flush=True)


def mode_table_data(pn):
    """Table 1's statistics computed on the data by the worlds' code (null_worlds.stats), for H9c."""
    df, K = h.load(pn)
    T = int(df["period"].max()) + 1
    res = h._nw(K, T).stats(df, "data")
    law = res.get("law", {})
    out = dict(panel=pn, turnover=res.get("turnover"), beta=res.get("beta"), gapped=res.get("gapped"), boundary_pooled=res.get("boundary_pooled"),
               law=law, c_over_s=(law["mean_c"] / law["mean_sbar"]) if law else None)
    _dump(HERE / f"r17_table_data__{pn}.json", out)
    print(json.dumps(out, default=float)[:1500], flush=True)


def mode_events(pn):
    """The head communities behind the two largest mid-month steps of 2021 in the submissions head's item share
    (15 February and 15 May): each community's change in its share of the top K's items, 14 days after against 14
    days before, and the block's head share without the largest contributors."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    import r16_capture as cap
    df, K, T, nb, start, dates, A, tops, heads, edges = cap.blocks_and_rosters(pn)
    del df
    gc.collect()
    path, ccol, _ = cap.DAILY[pn]
    b = next(i for i, (lo, hi) in enumerate(edges) if lo <= pd.Timestamp("2021-03-01") <= hi)
    lo, hi = edges[b]
    top = set(map(str, tops[b]))
    head = set(map(str, heads[b]))
    pf = pq.ParquetFile(h.ROOT / path)
    parts = []
    for i in range(pf.num_row_groups):
        t = pf.read_row_group(i, columns=["date", "endpoint_id", ccol])
        t = t.filter(pc.is_in(t["endpoint_id"], value_set=pa.array(sorted(top))))
        if t.num_rows:
            g = t.to_pandas()
            g["date"] = pd.to_datetime(g["date"])
            parts.append(g[(g["date"] >= lo) & (g["date"] <= hi)])
        del t
    D = pd.concat(parts)
    tot = D.groupby("date")[ccol].sum()
    out = dict(panel=pn, block=[str(lo.date()), str(hi.date())], events={})
    for day in ("2021-02-15", "2021-05-15"):
        d0 = pd.Timestamp(day)
        bef = (D["date"] >= d0 - pd.Timedelta(days=14)) & (D["date"] < d0)
        aft = (D["date"] >= d0) & (D["date"] < d0 + pd.Timedelta(days=14))
        sb = D[bef].groupby("endpoint_id")[ccol].sum() / tot[(tot.index >= d0 - pd.Timedelta(days=14)) & (tot.index < d0)].sum()
        sa = D[aft].groupby("endpoint_id")[ccol].sum() / tot[(tot.index >= d0) & (tot.index < d0 + pd.Timedelta(days=14))].sum()
        ch = (sa.reindex(sorted(head)).fillna(0) - sb.reindex(sorted(head)).fillna(0)).sort_values()
        hb, ha = float(sb.reindex(sorted(head)).fillna(0).sum()), float(sa.reindex(sorted(head)).fillna(0).sum())
        top3 = ch.index[:3].tolist()
        out["events"][day] = dict(head_share_before=hb, head_share_after=ha, log_step=float(np.log(ha / hb)),
                                  largest_falls=[dict(community=c, share_change=float(ch[c]), share_before=float(sb.get(c, 0.0))) for c in ch.index[:8]],
                                  share_of_step_top3=float(ch[top3].sum() / (ha - hb)))
    # the block's head share of items with and without the six largest contributors
    named = sorted({c["community"] for e in out["events"].values() for c in e["largest_falls"][:3]})
    hs = lambda excl: float(D[D["endpoint_id"].isin(head - set(excl))][ccol].sum() / D[~D["endpoint_id"].isin(excl)][ccol].sum())
    out["named"] = named
    out["block_head_item_share"] = hs([])
    out["block_head_item_share_without_named"] = hs(named)
    _dump(HERE / f"r17_events__{pn}.json", out)
    print(json.dumps(out, default=float, indent=1)[:3000], flush=True)


# --------------------------------------------------------------------------- R16-F extended
def _lasting_shape(H, ids, K, ks, nblocks):
    """Shape of the LASTING levels (the moving home), 52-week block means by identity present all block."""
    T = H.shape[0]
    uid, inv = np.unique(ids, return_inverse=True)
    M = np.full((T, uid.size), np.nan)
    M[np.repeat(np.arange(T), ids.shape[1]), inv.ravel()] = H.ravel()
    out = []
    for b in range(nblocks):
        seg = M[52 * b:52 * (b + 1)]
        ok = np.isfinite(seg).all(0)
        lad = np.sort(seg[:, ok].mean(0))[::-1]
        out.append([float(lad[:k].mean() - lad[:K].mean()) for k in ks])
    return np.array(out)


def _simulate_with_home(q, L0, T, seed):
    import rankdiff_rdm as rdm
    rng = np.random.default_rng(seed)
    st = rdm._State(q, np.asarray(L0, float), rng, lasting="movinghome_vp")
    Xs = np.empty((T, st.N))
    Hs = np.empty((T, st.N))
    ids = np.empty((T, st.N), np.int64)
    for t in range(T):
        Xs[t] = st.step()
        Hs[t] = st.home
        ids[t] = st.ids
    return Xs, Hs, ids


def mode_durability2(pn, world):
    """Step in the head's decline (0.2 a year, current top K/64) and, GLLM-style, a step of x1.2 in lasting volatility at
    every rank (drift not re-solved); 520 weeks, 10 seeds, m 1.0; observed and lasting shapes at K/64 and K/16;
    membership retention from the control."""
    DELTA = 0.2
    df, K, T, p, base, ladder, L0 = t3._setup(pn)
    del df
    gc.collect()
    q = h.het_params(p, ladder, "control_num") if world == "control_num" else t3.build("joint", p, ladder, K, JOINT[pn])
    ks = (K // 64, K // 16)
    N = L0.size
    zk = np.log((ks[0] - 0.5) / N)
    z = np.unique(np.concatenate([q.drift_z, [zk - 1e-6, zk + 1e-6]]))
    g0 = np.interp(z, q.drift_z, q.drift_g)
    arms = {"control": replace(q, drift_z=z, drift_g=g0), "head_step": replace(q, drift_z=z, drift_g=g0 - (DELTA / 52.0) * (z < zk)),
            "sigma_step": replace(q, drift_z=z, drift_g=g0, sigma0=q.sigma0 * 1.2)}
    rows = []
    for seed in range(10):
        r = {}
        for name, qq in arms.items():
            Xs, Hs, ids = _simulate_with_home(qq, L0, 520, seed)
            sh, rt = t3._block_shape_and_retention(Xs, ids, K, ks, 10)
            r[name] = dict(shape=sh.tolist(), retention=rt.tolist(), lasting=_lasting_shape(Hs, ids, K, ks, 10).tolist())
            del Xs, Hs, ids
            gc.collect()
        rows.append(dict(seed=seed, **r))
        d_obs = np.array(r["head_step"]["shape"])[:, 0] - np.array(r["control"]["shape"])[:, 0]
        d_las = np.array(r["head_step"]["lasting"])[:, 0] - np.array(r["control"]["lasting"])[:, 0]
        print(f"{pn} {world} seed {seed}: head step, observed {np.round(d_obs, 2).tolist()}; lasting {np.round(d_las, 2).tolist()}", flush=True)
        _dump(HERE / f"r17_durability__{pn}__{world}.json", dict(panel=pn, world=world, delta=DELTA, ks=list(ks), rows=rows))


# --------------------------------------------------------------------------- Tier 3: the joint grid past its edges, its calibration, R16-G redone
def mode_jgrid2(pn):
    if pn == "reddit_submissions_long":
        pts = [dict(s=s, head=hm, km=km, m=m) for s in (0.8, 1.0) for hm in (0.0, 0.1, 0.25) for km in (0.25, 0.35, 0.5) for m in (0.6, 0.8)]
        pts += [dict(s=0.0, head=1.0, km=km, m=m) for km in (0.25, 0.35, 0.5) for m in (0.6, 0.8)]
    else:
        pts = [dict(s=0.6, head=hm, km=km, m=0.7) for hm in (0.0, 0.1, 0.25) for km in (0.25, 0.35, 0.5)]
        pts += [dict(s=0.0, head=1.0, km=km, m=0.7) for km in (0.25, 0.35, 0.5)]
    out = t3._grid(pn, "joint", pts, "joint")
    old = json.loads((HERE / f"r16_joint_grid__{pn}.json").read_text())
    allrows = old["rows"] + out["rows"]
    out["best_all"] = min(allrows, key=lambda r: r["obj"])
    out["best_new"] = min(out["rows"], key=lambda r: r["obj"])
    _dump(HERE / f"r17_jgrid2__{pn}.json", out)
    print("best overall", out["best_all"]["prm"], round(out["best_all"]["obj"], 1), flush=True)


def mode_jcalib(pn):
    """20 pseudo-data panels (seeds 2000-2019) from (i) the joint world at the overall argmin (correct specification) and
    (ii) a homogeneous world with the slower swing (km 0.5, s 0), each calibrated by the union of the joint grids' saved
    simulated moments with the data's bootstrap sd; minimum objective and chosen parameters."""
    df, K, T, p, base, ladder, L0 = t3._setup(pn)
    del df
    gc.collect()
    g1 = json.loads((HERE / f"r16_joint_grid__{pn}.json").read_text())
    g2 = json.loads((HERE / f"r17_jgrid2__{pn}.json").read_text())
    rows = g1["rows"] + g2["rows"]
    G = np.array([r["aux"] for r in rows])
    sd = np.array(g1["sd"])
    best = g2["best_all"]["prm"]
    worlds = {"joint_at_argmin": t3.build("joint", p, ladder, K, best),
              "homogeneous_slow_swing": t3.build("joint", p, ladder, K, dict(s=0.0, head=1.0, km=0.5, m=best["m"]))}
    out = dict(panel=pn, data_obj=g2["best_all"]["obj"], argmin=best, n_grid=len(rows), worlds={})
    for name, q in worlds.items():
        reps = []
        for seed in range(2000, 2020):
            a = t3._world_aux(q, L0, T, seed, K)[0]
            v = t3.vec(a, "joint")
            obj = np.nansum(((G - v) / sd) ** 2, axis=1)
            j = int(np.argmin(obj))
            reps.append(dict(seed=seed, prm=rows[j]["prm"], obj=float(obj[j])))
            gc.collect()
        out["worlds"][name] = reps
        objs = [r["obj"] for r in reps]
        print(f"{pn} {name}: min objective median {np.median(objs):.1f} [{np.quantile(objs, 0.05):.1f}, {np.quantile(objs, 0.95):.1f}] "
              f"(data {out['data_obj']:.1f}); chosen s {[r['prm']['s'] for r in reps]}", flush=True)
        _dump(HERE / f"r17_jcalib__{pn}.json", out)


def mode_jdgrid2(pn):
    base = json.loads((HERE / f"r17_jgrid2__{pn}.json").read_text())["best_all"]["prm"]
    pts = [dict(s=s, head=base["head"], km=km, m=base["m"], pi=0.25, hl=208, sd=sd)
           for sd in (0.3, 0.5, 0.7) for s in (0.4, 0.6, 0.8) for km in (0.35, 0.5)]
    out = t3._grid(pn, "joint_decline", pts, "joint")
    out["best"] = min(out["rows"], key=lambda r: r["obj"])
    out["joint_best_obj"] = json.loads((HERE / f"r17_jgrid2__{pn}.json").read_text())["best_all"]["obj"]
    _dump(HERE / f"r17_jdgrid2__{pn}.json", out)
    print("best", out["best"]["prm"], round(out["best"]["obj"], 1), "against the joint", round(out["joint_best_obj"], 1), flush=True)


def main():
    mode, pn = sys.argv[1], sys.argv[2]
    if mode == "asym_data":
        return mode_asym_data(pn)
    if mode == "asym_items":
        return mode_asym_items(pn)
    if mode == "asym_worlds":
        return mode_asym_worlds(pn, sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), json.loads(sys.argv[6]))
    if mode == "asym_summ":
        return mode_asym_summ(pn, sys.argv[3])
    if mode == "table_data":
        return mode_table_data(pn)
    if mode == "events":
        return mode_events(pn)
    if mode == "durability2":
        return mode_durability2(pn, sys.argv[3])
    if mode == "jgrid2":
        return mode_jgrid2(pn)
    if mode == "jcalib":
        return mode_jcalib(pn)
    if mode == "jdgrid2":
        return mode_jdgrid2(pn)
    raise SystemExit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
