#!/usr/bin/env python3
"""M2: who the entrants are, and where and when the ladder's shape moves.

Design and predictions: llm_fitting/runs/2026-10-13_M2_entrants_shape/DESIGN.md.
Follow-up to M1 (mts_step1.py) under the movement-to-structure agenda.  Data
only: no fit, no simulation, no gate evaluation.

    python llm_fitting/mts_step1b.py run reddit_submissions_long
    python llm_fitting/mts_step1b.py report

analyse() takes a long panel (entity_id, period, rank, X, N, and optionally
metric and ts) and is the one code path for real and synthetic panels.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
from mts_step1 import NA, PANELS, PRES, PRIMARY, TRACK_MULT, _ks, matrices  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-13_M2_entrants_shape")
BOOT = 400
QUARTER = 13
# Registered before any run, from the owner's chart of submission-karma share by rank
# (received 2026-10-13): Reddit's recommendation change of 28 June 2021.
BREAKS = {"reddit_submissions_long": "2021-06-28", "reddit_comments_ext": "2021-06-28"}
CHART_RANKS = (1, 3, 5, 10, 25, 50, 100, 250, 500, 1000, 5000)
TOP_N = (1, 5, 20)
ENTRANT_CLASSES = ("new", "silent", "intermittent", "nearly continuous")


# --------------------------------------------------------------------------- #
# Histories and block ladders
# --------------------------------------------------------------------------- #
def history(X, block, start, nb):
    """First and last observed week of each endpoint, and its presence (share of
    weeks observed) in each block."""
    fin = np.isfinite(X)
    T = X.shape[0]
    any_ = fin.any(axis=0)
    first = np.where(any_, fin.argmax(axis=0), T)
    last = np.where(any_, T - 1 - fin[::-1].argmax(axis=0), -1)
    pres = np.stack([fin[start + b * block:start + (b + 1) * block].mean(axis=0) for b in range(nb)])
    return first, last, pres


def block_ladders(X, outside, block):
    """Block means and, per block, how far down the ladder is certified exact."""
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=PRES)
    n_cert = []
    for b in range(nb):
        seg = outside[start + b * block:start + (b + 1) * block]
        bd = float(np.nanmax(seg)) if np.isfinite(seg).any() else -np.inf
        lv = M[b][np.isfinite(M[b])]
        n_cert.append(int((lv > bd).sum()))
    return M, start, nb, n_cert


# --------------------------------------------------------------------------- #
# Part E: entrants, the top K's origins, exits
# --------------------------------------------------------------------------- #
def entrant_origins(M, first, pres, b, k, block, start):
    """Entrants to the top k of block b+1 (no block level in block b), by what the
    record shows up to the end of block b."""
    o1 = ba._order(M[b + 1])[:k]
    ell = float(M[b + 1][o1[-1]])
    is_ent = ~np.isfinite(M[b][o1])
    ent = o1[is_ent]
    rank = np.flatnonzero(is_ent) + 1
    t0 = start + b * block
    p = pres[b][ent]
    cls = np.where(first[ent] >= t0, 0, np.where(p == 0, 1, np.where(p < 0.5, 2, 3)))
    gain = M[b + 1][ent] - ell
    out = dict(k=int(k), n_entrants=int(ent.size), entrant_gain=float(gain.sum()), classes={})
    for j, name in enumerate(ENTRANT_CLASSES):
        m = cls == j
        row = dict(n=int(m.sum()), share=float(m.mean()) if ent.size else None,
                   gain_share=float(gain[m].sum() / gain.sum()) if ent.size and gain.sum() > 0 else None,
                   median_rank=float(np.median(rank[m])) if m.any() else None)
        if name == "new":
            row["median_weeks_since_first_observed"] = (
                float(np.median(t0 + block - first[ent][m])) if m.any() else None)
        out["classes"][name] = row
    out["median_rank_all"] = float(np.median(rank)) if ent.size else None
    return out


def topk_origins(M, pr0, pres0, first, b, K, end_block0):
    """Occupants of the top K of block b by their status in the first block."""
    o = ba._order(M[b])[:K]
    full = pres0[o] >= PRES
    cats = {
        "in the first block's cohort": full & (pr0[o] <= K),
        "permanent rank in (K, 2K]": full & (pr0[o] > K) & (pr0[o] <= 2 * K),
        "beyond 2K": full & (pr0[o] > 2 * K),
        "partly present in the first block": (pres0[o] > 0) & ~full,
        "not observed through the first block": (pres0[o] == 0) & (first[o] >= end_block0),
        "seen only before the first block": (pres0[o] == 0) & (first[o] < end_block0),
    }
    q = K // 4
    return {name: dict(n=int(m.sum()), share=float(m.mean()), share_of_top_quarter=float(m[:q].mean()))
            for name, m in cats.items()}


def exit_fates(M, last, pres, b, k, block, start, nb):
    """Exits from the top k of block b (no block level in block b+1)."""
    o0 = ba._order(M[b])[:k]
    ex = o0[~np.isfinite(M[b + 1][o0])]
    t1 = start + (b + 1) * block
    gone = last[ex] < t1
    partly = ~gone & (pres[b + 1][ex] > 0)
    silent_back = ~gone & (pres[b + 1][ex] == 0)
    return dict(k=int(k), n_exits=int(ex.size), right_censored=bool(b + 1 == nb - 1),
                gone=int(gone.sum()), partly_present_next_block=int(partly.sum()),
                silent_then_back=int(silent_back.sum()),
                gone_share=float(gone.mean()) if ex.size else None)


# --------------------------------------------------------------------------- #
# Part S: shape, by rank and by date
# --------------------------------------------------------------------------- #
def band_edges(K):
    e = [float(K)]
    while e[-1] / 2 >= K / 128 - 1e-9:
        e.append(e[-1] / 2)
    return [0.0] + e[::-1] + [2.0 * K, 4.0 * K]


def weekly_ladder_series(X, K):
    """Per week: the mean level in each band of current rank, the cumulative
    top-k means, and the centre (top-K mean).  Exact: the weekly top 4K is
    tracked.  NaN for a week with fewer than 4K endpoints observed."""
    edges = band_edges(K)
    ks = _ks(K, K)
    T = X.shape[0]
    n4 = int(TRACK_MULT * K)
    bands = np.full((T, len(edges) - 1), np.nan)
    cum = np.full((T, len(ks)), np.nan)
    ranks = [r for r in CHART_RANKS if r <= n4]
    at_rank = np.full((T, len(ranks)), np.nan)          # level at single ranks
    topn = np.full((T, len(TOP_N)), np.nan)             # raw metric summed over the top n positions
    for t in range(T):
        v = X[t][np.isfinite(X[t])]
        if v.size < n4:
            continue
        top = np.sort(np.partition(v, v.size - n4)[v.size - n4:])[::-1]
        cs = np.cumsum(top)
        for j in range(len(edges) - 1):
            lo, hi = int(np.floor(edges[j])), int(np.floor(edges[j + 1]))
            if hi > lo:
                bands[t, j] = (cs[hi - 1] - (cs[lo - 1] if lo > 0 else 0.0)) / (hi - lo)
        cum[t] = cs[np.array(ks) - 1] / np.array(ks)
        at_rank[t] = top[np.array(ranks) - 1]
        raw = np.cumsum(np.expm1(top[:max(TOP_N)]))
        topn[t] = raw[np.array(TOP_N) - 1]
    centre = cum[:, ks.index(K)]
    return dict(edges=edges, ks=ks, bands=bands, cum=cum, centre=centre, ranks=ranks, at_rank=at_rank, topn=topn)


def trend(y, n_boot=BOOT, block=QUARTER, seed=0, drop=None):
    """Least-squares slope of y on the week index, per year (x 52), with a 5-95%
    interval from a circular moving-block bootstrap of the residuals."""
    y = np.asarray(y, dtype=float)
    t = np.arange(y.size, dtype=float)
    ok = np.isfinite(y)
    if drop is not None:
        ok &= ~np.asarray(drop, dtype=bool)
    if ok.sum() < 3 * block:
        return dict(n=int(ok.sum()))
    tt, yy = t[ok], y[ok]
    A = np.column_stack([np.ones(tt.size), tt])
    beta = np.linalg.lstsq(A, yy, rcond=None)[0]
    res = yy - A @ beta
    rng = np.random.default_rng(seed)
    n = res.size
    nblk = int(np.ceil(n / block))
    slopes = np.empty(n_boot)
    for i in range(n_boot):
        s0 = rng.integers(0, n, nblk)
        idx = ((s0[:, None] + np.arange(block)[None, :]) % n).ravel()[:n]
        slopes[i] = np.linalg.lstsq(A, A @ beta + res[idx], rcond=None)[0][1]
    est, lo, hi = 52 * beta[1], 52 * np.quantile(slopes, 0.05), 52 * np.quantile(slopes, 0.95)
    # the nominal interval runs short on persistent series (about 75-80% coverage for AR(0.5-0.8)
    # residuals, 55-60% for AR(0.95), in synthetic checks); "wide" doubles it about the estimate
    return dict(n=int(n), per_year=float(est), q05=float(lo), q95=float(hi),
                wide05=float(est - 2 * (est - lo)), wide95=float(est + 2 * (hi - est)))


def split_trend(y, tb, n_boot=BOOT, drop=None):
    """Trends before and after week tb, and their difference with the doubled interval."""
    y = np.asarray(y, dtype=float)
    b = trend(y[:tb], n_boot, drop=None if drop is None else drop[:tb])
    a = trend(y[tb:], n_boot, seed=1, drop=None if drop is None else drop[tb:])
    out = dict(before=b, after=a)
    if "per_year" in b and "per_year" in a:
        half = np.hypot((b["q95"] - b["q05"]) / 2, (a["q95"] - a["q05"]) / 2)
        ch = a["per_year"] - b["per_year"]
        out.update(change=float(ch), change_wide05=float(ch - 2 * half), change_wide95=float(ch + 2 * half),
                   step_26_weeks=float(np.nanmean(y[tb:tb + 26]) - np.nanmean(y[max(0, tb - 26):tb])))
    return out


def half_change_weeks(y, tb, window=QUARTER):
    """Weeks after tb at which the 13-week mean has covered half of the distance from
    the 13 weeks before tb to the last 13 weeks of the series (centre of the window)."""
    y = np.asarray(y, dtype=float)
    base, final = np.nanmean(y[tb - window:tb]), np.nanmean(y[-window:])
    if not np.isfinite(base + final) or abs(final - base) < 1e-12:
        return None
    for w in range(0, y.size - tb - window + 1):
        if (np.nanmean(y[tb + w:tb + w + window]) - base) / (final - base) >= 0.5:
            return float(w + window / 2)
    return None


def period_means(y, length, start):
    nb = (y.size - start) // length
    return [float(np.nanmean(y[start + b * length:start + (b + 1) * length])) for b in range(nb)]


def analyse(df, K, block, boot=BOOT, flagged_weeks=None, break_date=None):
    """Parts E and S for one panel.  Returns a JSON-ready dict.  break_date: a date
    registered beforehand; trends are then also given before and after it."""
    X, R, floors, outside, ids = matrices(df, K)
    T = X.shape[0]
    M, start, nb, n_cert = block_ladders(X, outside, block)
    first, last, pres = history(X, block, start, nb)
    dates = (df.groupby("period")["ts"].first().reindex(range(T)).astype(str).tolist()
             if "ts" in df.columns else [str(t) for t in range(T)])
    out = dict(K=int(K), T=int(T), n_tracked=int(X.shape[1]), block=int(block), presence=PRES,
               blocks=dict(n=int(nb), start=int(start), certified_top=n_cert,
                           first_week_of_each=[dates[start + b * block] for b in range(nb)]))
    ks_e = [K // 4, K // 2, K, 2 * K]

    # ---- Part E ---------------------------------------------------------------------------
    ent, exits, acct = [], [], []
    for b in range(nb - 1):
        for k in ks_e:
            ok = min(n_cert[b], n_cert[b + 1]) >= k
            head = dict(start=b, end=b + 1, k=int(k), left_censored=bool(b == 0))
            if not ok:
                ent.append(dict(head, not_adjudicated="rank not certified exact in both blocks"))
                exits.append(dict(head, not_adjudicated="rank not certified exact in both blocks"))
                continue
            ent.append(dict(head, **entrant_origins(M, first, pres, b, k, block, start)))
            exits.append(dict(head, **exit_fates(M, last, pres, b, k, block, start, nb)))
        ks_a = _ks(K, K)
        if min(n_cert[b], n_cert[b + 1]) >= K:
            rows = ba.topk_accounting(M[b], M[b + 1], ks_a, K)
            acct.append(dict(start=b, end=b + 1, weeks=int(block), rows=[
                dict(k=r["k"], g=r["G"] / (r["k"] * block), r=r["R"] / (r["k"] * block),
                     f=r["F"] / (r["k"] * block), shape=r["shape"] / (r["k"] * block)) for r in rows]))
        else:
            acct.append(dict(start=b, end=b + 1, not_adjudicated="rank K not certified exact in both blocks"))
    pr0, pres0 = ba.perm_rank(R, start, start + block, floors)
    origins = []
    for b in range(1, nb):
        if n_cert[b] >= K:
            origins.append(dict(block=b, classes=topk_origins(M, pr0, pres0, first, b, K, start + block)))
        else:
            origins.append(dict(block=b, not_adjudicated="rank K not certified exact"))
    out["entrants"], out["exits"], out["origins"], out["accounting"] = ent, exits, origins, acct

    # ---- Part S ---------------------------------------------------------------------------
    w = weekly_ladder_series(X, K)
    logtot = (np.log(df.groupby("period")["metric"].sum().reindex(range(T)).to_numpy(dtype=float))
              if "metric" in df.columns else np.full(T, np.nan))
    drop = None
    if flagged_weeks is not None:
        drop = np.zeros(T, dtype=bool)
        drop[[t for t in flagged_weeks if 0 <= t < T]] = True
    edges = w["edges"]
    bands = []
    for j in range(len(edges) - 1):
        lvl = w["bands"][:, j]
        row = dict(lo=edges[j], hi=edges[j + 1],
                   level=trend(lvl, boot), vs_centre=trend(lvl - w["centre"], boot),
                   vs_total=trend(lvl - logtot, boot) if np.isfinite(logtot).any() else None,
                   block_means_vs_centre=period_means(lvl - w["centre"], block, start))
        if drop is not None:
            row["vs_centre_without_flagged_weeks"] = trend(lvl - w["centre"], boot, drop=drop)
            row["level_without_flagged_weeks"] = trend(lvl, boot, drop=drop)
        bands.append(row)
    cum = [dict(k=int(k), vs_centre=trend(w["cum"][:, i] - w["centre"], boot),
                block_means_vs_centre=period_means(w["cum"][:, i] - w["centre"], block, start))
           for i, k in enumerate(w["ks"])]
    centre = dict(level=trend(w["centre"], boot), vs_total=trend(w["centre"] - logtot, boot)
                  if np.isfinite(logtot).any() else None, block_means=period_means(w["centre"], block, start))
    if drop is not None:
        centre["level_without_flagged_weeks"] = trend(w["centre"], boot, drop=drop)
    total = dict(level=trend(logtot, boot)) if np.isfinite(logtot).any() else None
    # when: 13-week means against the centre for three bands, counted back from the end
    qstart = T - (T // QUARTER) * QUARTER
    when = []
    pick = [0, next(j for j in range(len(edges) - 1) if abs(edges[j + 1] - K / 16) < 1e-6),
            next(j for j in range(len(edges) - 1) if abs(edges[j + 1] - K / 4) < 1e-6)]
    for j in pick:
        qm = period_means(w["bands"][:, j] - w["centre"], QUARTER, qstart)
        steps = np.diff(qm)
        tot = float(np.nansum(np.abs(steps)))
        when.append(dict(lo=edges[j], hi=edges[j + 1], quarter_means=qm, steps=steps.tolist(),
                         step_dates=[dates[qstart + (i + 1) * QUARTER] for i in range(len(steps))],
                         largest_step_share=float(np.nanmax(np.abs(steps)) / tot) if tot > 0 else None,
                         net_change=float(qm[-1] - qm[0])))
    out["shape"] = dict(bands=bands, cumulative=cum, centre=centre, total=total, when=when,
                        n_flagged_weeks=int(drop.sum()) if drop is not None else 0)

    # ---- shares of the platform total at single ranks (the owner's chart), and the break ------
    have_tot = bool(np.isfinite(logtot).any())
    tb = None
    if break_date is not None and "ts" in df.columns:
        import pandas as pd
        ts = pd.to_datetime(df.groupby("period")["ts"].first().reindex(range(T)))
        after = np.flatnonzero((ts >= pd.Timestamp(break_date)).to_numpy())
        tb = int(after[0]) if after.size and 3 * QUARTER <= after[0] <= T - 3 * QUARTER else None
    brk = dict(date=break_date, week=tb, block_containing=None if tb is None else int((tb - start) // block))
    if have_tot:
        with np.errstate(divide="ignore", invalid="ignore"):
            log_share = np.log(np.expm1(w["at_rank"])) - logtot[:, None]
            log_top = np.log(w["topn"]) - logtot[:, None]
        ref = (slice(tb - 26, tb) if tb is not None else slice(start, start + block))
        seg = X[ref]
        okn = np.isfinite(seg).mean(axis=0) >= PRES
        seg_mean = np.nansum(seg, axis=0) / np.maximum(np.isfinite(seg).sum(axis=0), 1)
        named = np.argsort(-np.where(okn, seg_mean, -np.inf))[:5]
        named_share = np.log(np.nansum(np.expm1(X[:, named]), axis=1)) - logtot
        qs = T - (T // QUARTER) * QUARTER
        brk["rank_shares"] = [dict(rank=int(r), trend=trend(log_share[:, i], boot),
                                   quarter_means=period_means(log_share[:, i], QUARTER, qs),
                                   split=split_trend(log_share[:, i], tb, boot) if tb is not None else None)
                              for i, r in enumerate(w["ranks"])]
        brk["top_n_shares"] = [dict(n=int(n), trend=trend(log_top[:, i], boot),
                                    quarter_means=period_means(log_top[:, i], QUARTER, qs),
                                    split=split_trend(log_top[:, i], tb, boot) if tb is not None else None)
                               for i, n in enumerate(TOP_N)]
        brk["named_top5"] = dict(basis="the 26 weeks before the registered date" if tb is not None else "the first block",
                                 trend=trend(named_share, boot), quarter_means=period_means(named_share, QUARTER, qs),
                                 split=split_trend(named_share, tb, boot) if tb is not None else None)
        brk["quarter_start_dates"] = [dates[qs + i * QUARTER] for i in range((T - qs) // QUARTER)]
    if tb is not None:
        brk["bands_vs_centre"] = [dict(lo=edges[j], hi=edges[j + 1],
                                       split=split_trend(w["bands"][:, j] - w["centre"], tb, boot),
                                       half_change_weeks=half_change_weeks(w["bands"][:, j] - w["centre"], tb))
                                  for j in range(len(edges) - 1)]
        brk["centre_level"] = split_trend(w["centre"], tb, boot)
    out["break"] = brk
    return out


# --------------------------------------------------------------------------- #
# Scoring (DESIGN.md, "Predictions")
# --------------------------------------------------------------------------- #
def _ent(res, start, k):
    return next((e for e in res["entrants"] if e["start"] == start and e["k"] == k), None)


def _band(res, hi):
    return next(b for b in res["shape"]["bands"] if abs(b["hi"] - hi) < 1e-6)


def score(res, system):
    K, nb = res["K"], res["blocks"]["n"]
    S = {}

    def put(name, status, **detail):
        S[name] = dict(status=status, detail=detail)

    if system == "reddit":
        pairs = [_ent(res, b, K) for b in (1, 2)]
        if any(p is None or "not_adjudicated" in p or not p["n_entrants"] for p in pairs):
            for nm in ("E1", "E2", "E3"):
                put(nm, NA, reason="pair not certified or no entrants")
        else:
            new = [p["classes"]["new"]["share"] for p in pairs]
            sil = [p["classes"]["silent"]["share"] for p in pairs]
            med = [p["median_rank_all"] for p in pairs]
            put("E1", "PASS" if all(0.25 <= v <= 0.60 for v in new) else "FAIL", new_share=new)
            put("E2", "PASS" if all(v < 0.10 for v in sil) else "FAIL", silent_share=sil)
            put("E3", "PASS" if all(v > K / 2 for v in med) else "FAIL", median_rank=med, half_K=K / 2)
        last = next((o for o in res["origins"] if o["block"] == nb - 1), None)
        if last is None or "not_adjudicated" in last:
            put("E4", NA, reason="not certified")
        else:
            v = last["classes"]["not observed through the first block"]["share"]
            put("E4", "PASS" if v >= 0.10 else "FAIL", share_of_top_K=v)
        ex = next((e for e in res["exits"] if e["start"] == 1 and e["k"] == K), None)
        if ex is None or "not_adjudicated" in ex or not ex["n_exits"]:
            put("E5", NA, reason="not certified or no exits")
        else:
            put("E5", "PASS" if ex["gone_share"] < 0.30 else "FAIL", gone_share=ex["gone_share"], n_exits=ex["n_exits"])
        put("E6", NA, reason="stated for Facebook")
        # shape
        above = [b for b in res["shape"]["bands"] if b["hi"] <= K / 4 + 1e-6]
        neg = all("wide95" in b["vs_centre"] and b["vs_centre"]["wide95"] < 0 for b in above)
        t_head, t_mid = _band(res, K / 64)["vs_centre"], _band(res, K / 4)["vs_centre"]
        ratio = (t_head["per_year"] / t_mid["per_year"]) if t_mid.get("per_year") else float("nan")
        put("S1", "PASS" if neg and t_mid["per_year"] < 0 and ratio >= 1.5 else "FAIL",
            all_bands_above_K4_negative=neg, head_over_mid=ratio,
            trends={f"{b['hi']:.0f}": b["vs_centre"].get("per_year") for b in above})
        c, top = res["shape"]["centre"]["level"], res["shape"]["bands"][0]["level"]
        put("S2", "PASS" if c["wide05"] > 0 and top["per_year"] > -c["per_year"] else "FAIL",
            centre=c, top_band=top)
        cum = next(x for x in res["shape"]["cumulative"] if x["k"] == min(res["shape"]["cumulative"],
                   key=lambda z: abs(z["k"] - K / 16))["k"])
        bm = cum["block_means_vs_centre"]
        if len(bm) >= 4:
            put("S3", "PASS" if abs(bm[1] - bm[0]) > abs(bm[3] - bm[2]) else "FAIL",
                first_step=bm[1] - bm[0], last_step=bm[3] - bm[2], k=cum["k"])
        else:
            put("S3", NA, reason="fewer than four blocks")
        put("S5", NA, reason="stated for Facebook")
    else:
        for nm in ("E1", "E2", "E3", "E4", "E5", "S1", "S2", "S3"):
            put(nm, NA, reason="stated for the Reddit panels")
        p = _ent(res, 1, K)
        if p is None or "not_adjudicated" in p or not p["n_entrants"]:
            put("E6", NA, reason="pair not certified or no entrants")
        else:
            put("E6", "PASS" if p["classes"]["new"]["share"] < 0.30 else "FAIL", new_share=p["classes"]["new"]["share"])
        c, top = res["shape"]["centre"]["level"], res["shape"]["bands"][0]["level"]
        put("S5", "PASS" if c["wide95"] < 0 and top["per_year"] > c["per_year"] else "FAIL", centre=c, top_band=top)
    brk = res.get("break", {})

    def clearly(d, sign):
        return d is not None and "wide05" in d and (d["wide05"] > 0 if sign > 0 else d["wide95"] < 0)

    shares = {r["rank"]: r for r in brk.get("rank_shares", [])}
    if res.get("panel", "").startswith("reddit_submissions") and brk.get("week") is not None and 1 in shares:
        r1, deep = shares[1]["split"], shares[max(shares)]["split"]
        put("S6", "PASS" if clearly(r1["before"], +1) and clearly(r1["after"], -1)
            and clearly(deep["before"], +1) and clearly(deep["after"], +1) else "FAIL",
            rank1_before=r1["before"], rank1_after=r1["after"], deepest_rank=max(shares),
            deepest_before=deep["before"], deepest_after=deep["after"])
    else:
        put("S6", NA, reason="a reproduction of the owner's chart on the submissions panel only")
    bv = brk.get("bands_vs_centre")
    if res.get("panel", "").startswith("reddit_comments") and bv:
        topb = bv[0]["split"]
        put("S7", "PASS" if topb.get("change_wide95", 1) < 0 else "FAIL", change=topb.get("change"),
            wide=[topb.get("change_wide05"), topb.get("change_wide95")])
    else:
        put("S7", NA, reason="stated for the comments panel")
    if system == "reddit" and bv:
        h_top = bv[0]["half_change_weeks"]
        h_mid = next(b for b in bv if abs(b["hi"] - K / 4) < 1e-6)["half_change_weeks"]
        put("S8", NA if h_top is None or h_mid is None else "PASS" if h_top < h_mid else "FAIL",
            top_band_weeks=h_top, K8_to_K4_weeks=h_mid,
            all_bands=[b["half_change_weeks"] for b in bv])
    else:
        put("S8", NA, reason="needs a registered date")
    w16 = next(x for x in res["shape"]["when"] if abs(x["hi"] - K / 16) < 1e-6)
    put("S4", "PASS" if w16["largest_step_share"] is not None and w16["largest_step_share"] <= 0.30 else "FAIL",
        largest_step_share=w16["largest_step_share"],
        largest_step_date=w16["step_dates"][int(np.nanargmax(np.abs(w16["steps"])))])
    return S


def run(panel):
    import minimal_rankdiff as mrd
    cfg = PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    flagged = None
    if cfg["system"] == "facebook":
        import instrument_eras as ie
        import pandas as pd
        days = ie.flag_days(ie.day_count_series(ie.DAILY_PATHS["facebook"]))
        weeks = {pd.Timestamp(x) for x in ie.weeks_containing(days)}
        ts = df.groupby("period")["ts"].first()
        flagged = [int(p) for p, d in ts.items() if pd.Timestamp(d) in weeks]
    elif panel == "reddit_submissions_long":
        import instrument_eras as ie
        import pandas as pd
        weeks = {pd.Timestamp(x) for x in ie.reddit_flagged_weeks("submissions")}
        ts = df.groupby("period")["ts"].first()
        flagged = [int(p) for p, d in ts.items() if pd.Timestamp(d) in weeks]
    res = analyse(df, cfg["K"], cfg["block"], flagged_weeks=flagged, break_date=BREAKS.get(panel))
    res.update(panel=panel, system=cfg["system"], finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"m2__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] T={res['T']} tracked={res['n_tracked']} blocks={res['blocks']['n']} "
          f"certified_top={res['blocks']['certified_top']} flagged_weeks={res['shape']['n_flagged_weeks']} written",
          flush=True)


def _t(d):
    return "n/a" if not d or "per_year" not in d else f"{d['per_year']:+.3f} ({d['q05']:+.3f} to {d['q95']:+.3f})"


def report():
    lines, scores = [], {}
    for f in sorted((OUT / "cells").glob("m2__*.json")):
        d = json.loads(f.read_text())
        tag, K = d["panel"], d["K"]
        lines += [f"\n## {tag}  (K = {K}, T = {d['T']}, tracked = {d['n_tracked']}, {d['blocks']['n']} blocks of "
                  f"{d['block']} weeks starting {', '.join(x[:10] for x in d['blocks']['first_week_of_each'])})",
                  f"certified exact top of each block ladder: {d['blocks']['certified_top']}", "",
                  "Entrants to the top k of the later block (no block level in the earlier one):", "",
                  "| blocks | k | entrants | new | silent | intermittent | nearly continuous | new: share of gain | "
                  "median rank | new: median weeks since first seen |", "|---|---|---|---|---|---|---|---|---|---|"]
        for e in d["entrants"]:
            lab = f"{e['start']}→{e['end']}" + (" (left-censored)" if e["left_censored"] else "")
            if "not_adjudicated" in e:
                lines.append(f"| {lab} | {e['k']} | NOT ADJUDICATED | | | | | | | |")
                continue
            c = e["classes"]
            sh = lambda n: "n/a" if c[n]["share"] is None else f"{c[n]['share']:.0%}"
            gs = c["new"]["gain_share"]
            lines.append(f"| {lab} | {e['k']} | {e['n_entrants']} | {sh('new')} | {sh('silent')} | {sh('intermittent')} | "
                         f"{sh('nearly continuous')} | {'n/a' if gs is None else f'{gs:.0%}'} | {e['median_rank_all']} | "
                         f"{c['new'].get('median_weeks_since_first_observed')} |")
        names = ["in the first block's cohort", "permanent rank in (K, 2K]", "beyond 2K",
                 "partly present in the first block", "not observed through the first block",
                 "seen only before the first block"]
        lines += ["", "Where the top K of each later block came from (status in the first block):", "",
                  "| block | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
        for o in d["origins"]:
            if "classes" in o:
                lines.append(f"| {o['block']} | " + " | ".join(
                    f"{o['classes'][nm]['share']:.1%} (top quarter {o['classes'][nm]['share_of_top_quarter']:.1%})"
                    for nm in names) + " |")
        lines += ["", "Exits from the top k of the earlier block:", "",
                  "| blocks | k | exits | gone from the record | partly present next block | silent, then back |",
                  "|---|---|---|---|---|---|"]
        for e in d["exits"]:
            if "not_adjudicated" in e:
                continue
            lab = f"{e['start']}→{e['end']}" + (" (right-censored)" if e["right_censored"] else "")
            lines.append(f"| {lab} | {e['k']} | {e['n_exits']} | {e['gone']} | {e['partly_present_next_block']} | "
                         f"{e['silent_then_back']} |")
        s = d["shape"]
        lines += ["", f"Trends per year, least squares over the panel (5-95% moving-block bootstrap); "
                      f"centre = top-K mean. Flagged weeks: {s['n_flagged_weeks']}.", "",
                  f"centre level: {_t(s['centre']['level'])}; centre against the platform total: {_t(s['centre']['vs_total'])}; "
                  f"platform total: {_t(s['total']['level']) if s['total'] else 'n/a'}", "",
                  "| band of current rank | level | against the centre | against the platform total | block means against the centre |",
                  "|---|---|---|---|---|"]
        for b in s["bands"]:
            lines.append(f"| ({b['lo']:.0f}, {b['hi']:.0f}] | {_t(b['level'])} | {_t(b['vs_centre'])} | {_t(b['vs_total'])} | "
                         + ", ".join(f"{x:+.3f}" for x in b["block_means_vs_centre"]) + " |")
            if "vs_centre_without_flagged_weeks" in b:
                lines.append(f"| … without flagged weeks | {_t(b['level_without_flagged_weeks'])} | "
                             f"{_t(b['vs_centre_without_flagged_weeks'])} | | |")
        lines += ["", "Accounting between consecutive blocks, per endpoint per week (centre K):", "",
                  "| blocks | k | occupant loss | reordering gain | flow | shape |", "|---|---|---|---|---|---|"]
        for a in d["accounting"]:
            if "rows" not in a:
                continue
            for r in a["rows"]:
                lines.append(f"| {a['start']}→{a['end']} | {r['k']} | {r['g']:.5f} | {r['r']:.5f} | {r['f']:+.5f} | {r['shape']:+.5f} |")
        lines += ["", "When: 13-week means against the centre, step by step:", ""]
        for wn in s["when"]:
            big = int(np.nanargmax(np.abs(wn["steps"])))
            lines.append(f"- ({wn['lo']:.0f}, {wn['hi']:.0f}]: net change {wn['net_change']:+.3f}; largest step "
                         f"{wn['steps'][big]:+.3f} beginning {wn['step_dates'][big][:10]}, "
                         f"{wn['largest_step_share']:.0%} of the summed absolute steps; steps: "
                         + ", ".join(f"{x:+.3f}" for x in wn["steps"]))
        bk = d.get("break", {})
        if bk.get("rank_shares"):
            lines += ["", f"Log share of the platform's weekly total at single ranks (registered date: {bk['date']}, "
                          f"week {bk['week']}, inside block {bk['block_containing']}). Trends per year; "
                          "wide = the doubled interval.", "",
                      "| rank | whole panel | before the date (wide) | after the date (wide) | change (wide) | 26 weeks after minus 26 before |",
                      "|---|---|---|---|---|---|"]
            def tw(x):
                return "n/a" if not x or "per_year" not in x else f"{x['per_year']:+.3f} ({x['wide05']:+.3f} to {x['wide95']:+.3f})"
            def row(label, r):
                sp = r.get("split") or {}
                ch = "n/a" if "change" not in sp else f"{sp['change']:+.3f} ({sp['change_wide05']:+.3f} to {sp['change_wide95']:+.3f})"
                st = "n/a" if "step_26_weeks" not in sp else f"{sp['step_26_weeks']:+.3f}"
                return f"| {label} | {tw(r['trend'])} | {tw(sp.get('before'))} | {tw(sp.get('after'))} | {ch} | {st} |"
            for r in bk["rank_shares"]:
                lines.append(row(r["rank"], r))
            for r in bk["top_n_shares"]:
                lines.append(row(f"top {r['n']} positions together", r))
            lines.append(row(f"the 5 names at the top in {bk['named_top5']['basis']}", bk["named_top5"]))
            lines += ["", "Rank 1, 13-week means of the log share, from " + bk["quarter_start_dates"][0][:10] + ": "
                      + ", ".join(f"{x:+.2f}" for x in bk["rank_shares"][0]["quarter_means"])]
        if bk.get("bands_vs_centre"):
            lines += ["", "Bands against the centre, before and after the registered date:", "",
                      "| band | before (wide) | after (wide) | change (wide) | weeks after the date to half of the change |",
                      "|---|---|---|---|---|"]
            for b in bk["bands_vs_centre"]:
                sp = b["split"]
                ch = "n/a" if "change" not in sp else f"{sp['change']:+.3f} ({sp['change_wide05']:+.3f} to {sp['change_wide95']:+.3f})"
                lines.append(f"| ({b['lo']:.0f}, {b['hi']:.0f}] | {tw(sp.get('before'))} | {tw(sp.get('after'))} | {ch} | "
                             f"{b['half_change_weeks']} |")
            lines.append("")
            lines.append("centre level before and after: " + tw(bk["centre_level"].get("before")) + "; "
                         + tw(bk["centre_level"].get("after")))
        if tag in PRIMARY:
            scores[tag] = score(d, d["system"])
    lines += ["", "## Scores (DESIGN.md)", "", "| | " + " | ".join(scores) + " |", "|---|" + "---|" * len(scores)]
    for nm in ("E1", "E2", "E3", "E4", "E5", "E6", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"):
        lines.append(f"| {nm} | " + " | ".join(s[nm]["status"] for s in scores.values()) + " |")
    lines += ["", "Details of every score:", "", "```", json.dumps(scores, indent=1, default=float), "```"]
    text = "\n".join(lines) + "\n"
    (OUT / "report_tables.md").write_text(text)
    print(text)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("panel", choices=sorted(PRIMARY))
    sub.add_parser("report")
    a = ap.parse_args()
    run(a.panel) if a.cmd == "run" else report()


if __name__ == "__main__":
    main()
