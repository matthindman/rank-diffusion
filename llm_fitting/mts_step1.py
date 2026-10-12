#!/usr/bin/env python3
"""M1: the balance accounting on a panel (movement-to-structure agenda, step 1).

Protocol, estimands, predictions, scoring rules and the feasibility screen:
llm_fitting/MTS_PROTOCOL_STEPS_1_2_2026-10-13.md (v2, after Codex's review R4).
Statistics: balance_accounting.py.  Data only: no fit, no simulation, no gate
evaluation.

    python llm_fitting/mts_step1.py run reddit_submissions_long --reviewed
    python llm_fitting/mts_step1.py report

analyse() is the one code path for real and planted panels; score() applies
the protocol's fixed scoring rules to its output.  A cell whose tracked
population is not certified exact is returned as NOT ADJUDICATED, never as a
number.
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

OUT = Path("llm_fitting/runs/2026-10-13_M1_balance")
PANELS = {                      # platform key in minimal_rankdiff.PLATFORMS, K, block length in weeks
    "facebook_a": dict(platform="facebook_a", K=3500, block=26, system="facebook"),
    "reddit_comments_ext": dict(platform="reddit_comments_ext", K=12500, block=52, system="reddit"),
    "reddit_submissions_long": dict(platform="reddit_submissions_long", K=5000, block=52, system="reddit"),
    "reddit_comments": dict(platform="reddit_comments", K=12500, block=26, system="reddit"),  # secondary
}
PRIMARY = ("facebook_a", "reddit_comments_ext", "reddit_submissions_long")
HORIZONS = (1, 4, 13, 26, 52)
BOUNDARIES = (0.5, 1.0, 2.0)    # centre and cohort cutoff, as multiples of K
TRACK_MULT = 4                  # endpoints ever inside the weekly top 4K are tracked
PRES = 0.9                      # a block level needs presence in 90% of the block's weeks
BOOT = 400
NA = "NOT ADJUDICATED"
B1_RANGE = dict(reddit=(0.0018, 0.0045), facebook=(0.0045, 0.010))


def matrices(df, K):
    """Level and weekly-rank matrices for every endpoint that is ever inside the
    weekly top TRACK_MULT * K, from a long panel with columns entity_id, period,
    rank (among everything observed that week), X and N.  Also the weekly
    floors N_t + 1 and, per week, the level at rank TRACK_MULT * K + 1: the most
    an untracked endpoint can have that week (NaN when nothing is ranked there,
    in which case no untracked endpoint is present that week)."""
    T = int(df["period"].max()) + 1
    cut = TRACK_MULT * K
    keep = df.loc[df["rank"] <= cut, "entity_id"].unique()
    sub = df[df["entity_id"].isin(keep)]
    x = sub.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    r = sub.pivot(index="period", columns="entity_id", values="rank").reindex(range(T))
    X = x.to_numpy(dtype=float)
    R = np.nan_to_num(r.to_numpy(dtype=float), nan=0.0).astype(np.int64)
    floors = df.groupby("period")["N"].first().reindex(range(T)).to_numpy(dtype=float) + 1.0
    outside = (df[df["rank"] == cut + 1].set_index("period")["X"].reindex(range(T)).to_numpy(dtype=float))
    return X, R, floors, outside, np.asarray(x.columns)


def _ks(K, top):
    ks, k = [], float(top)
    while k >= 12:
        ks.append(int(round(k)))
        k /= 2
    return sorted(set(ks))


def _summ(res, ks, n_seg=4):
    """Mean over origins and the range over n_seg consecutive calendar segments."""
    out = {}
    seg = [s for s in np.array_split(np.arange(res["origins"].size), n_seg) if s.size]
    for q in ("g", "r", "f", "shape", "r_diff", "step", "swap"):
        a = res[q]
        out[q] = dict(mean=np.nanmean(a, axis=0).tolist(),
                      seg_min=np.nanmin([np.nanmean(a[s], axis=0) for s in seg], axis=0).tolist(),
                      seg_max=np.nanmax([np.nanmean(a[s], axis=0) for s in seg], axis=0).tolist())
    out["max_abs_resid"] = float(np.nanmax(np.abs(res["resid"])))
    out["n_origins"] = int(res["origins"].size)
    out["ks"] = list(ks)
    return out


def _role(sel, b0, b1, nb):
    """Which comparisons count, fixed before any run (protocol section 5)."""
    if sel > 0:
        return "replicate cohort (same outcome blocks as a pair above; not counted again)"
    if nb >= 4:
        return {(2, 3): "primary (one block between selection and start)",
                (1, 2): "no gap", (1, 3): "long, no gap"}.get((b0, b1), "other")
    return "primary (no gap available)" if (b0, b1) == (1, 2) else "other"


def analyse(df, K, block, horizons=HORIZONS, boundaries=BOUNDARIES, boot=BOOT, pair_step=1):
    """Parts A and B of the protocol for one panel.  Returns a JSON-ready dict.
    pair_step: keep only block pairs whose separation is a multiple of it (2 for
    the declared 26-week secondary on Reddit, read a whole year apart)."""
    X, R, floors, outside, ids = matrices(df, K)
    T = X.shape[0]
    out = dict(K=int(K), T=int(T), n_tracked=int(X.shape[1]), block=int(block), presence=PRES,
               pair_step=int(pair_step), boundaries={})
    nb = T // block
    start = T - nb * block                             # drop the earliest weeks, keep whole blocks
    M = ba.block_means(X, block, start=start, min_frac=PRES)
    bound, n_cert = [], []
    for b in range(nb):
        seg = outside[start + b * block:start + (b + 1) * block]
        bd = float(np.nanmax(seg)) if np.isfinite(seg).any() else -np.inf
        lv = M[b][np.isfinite(M[b])]
        bound.append(bd)
        n_cert.append(int((lv > bd).sum()))            # the top n_cert of block b's ladder is exact
    out["blocks"] = dict(n=int(nb), start=int(start), untracked_bound=bound, certified_top=n_cert)
    for mult in boundaries:
        kc = int(round(mult * K))
        ks = _ks(K, kc)
        cell = dict(k_centre=kc, ks=ks)
        # ---- Part A: weekly positions.  Exact: the weekly top 4K is tracked by construction. ----
        cell["weekly"] = {}
        for h in horizons:
            if h >= T // 2 or np.sqrt(2) * kc > TRACK_MULT * K:
                continue
            origins = np.arange(0, T - h, max(1, h // 2))
            cell["weekly"][str(h)] = _summ(ba.accounting_series(X, h, ks, kc, origins), ks)
        # ---- Part B: block means, cohorts fixed on an earlier block ---------------------------
        c = ba.centre_series(M, kc)
        blocks = dict(centre=c.tolist(), pairs=[])
        for sel in range(nb - 2):
            lo, hi = start + sel * block, start + (sel + 1) * block
            pr, pres = ba.perm_rank(R, lo, hi, floors)
            labels, which = ba.cohort_bins(pr, kc, pres, pres_floor=PRES)
            outside_bin = len(labels) - 1
            for b0 in range(sel + 1, nb - 1):
                for b1 in range(b0 + 1, nb):
                    if (b1 - b0) % pair_step:
                        continue
                    pair = dict(select=sel, start=b0, end=b1, weeks=int((b1 - b0) * block),
                                role=_role(sel, b0, b1, nb),
                                bins=[list(map(float, lb)) if lb != "outside" else lb for lb in labels])
                    centre_ok = min(n_cert[b0], n_cert[b1]) >= kc
                    if not centre_ok:
                        pair["not_adjudicated"] = "the centre's ranks are not certified exact in both blocks"
                        blocks["pairs"].append(pair)
                        continue
                    drift, draws = ba.cohort_drift(M, c, which, len(labels), b0, b1, n_boot=boot, return_draws=True)
                    acc = ba.topk_accounting(M[b0], M[b1], ks, kc)
                    pred = ba.predictable_loss(M, c, which, drift, b0, b1, ks, draws=draws, outside_bin=outside_bin)
                    diff = []
                    for j, k in enumerate(ks):
                        if min(n_cert[b0], n_cert[b1]) < k:
                            acc[j] = dict(k=int(k), not_adjudicated="rank not certified exact in both blocks")
                            pred[j] = dict(k=int(k), not_adjudicated="rank not certified exact in both blocks")
                        if n_cert[b0] >= np.ceil(np.sqrt(2) * k):
                            diff.append(ba.diffusion_gain(M[b0], M[b1], k, kc))
                        else:
                            diff.append(dict(k=int(k), not_adjudicated="diffusion band not certified exact"))
                    drift[outside_bin]["note"] = ("retrospectively selected: tracked because it reached the weekly "
                                                  "top 4K at some time, including later; not a cohort")
                    pair.update(drift=drift, accounting=acc, diffusion=diff, predictable=pred,
                                occupancy_at_start=ba.occupancy(which, len(labels), M[b0], ks))
                    if b1 == b0 + 1:
                        pair["carriers"] = ba.carrier_regression(
                            M, c, b_pos=sel if b0 == sel + 1 else b0 - 1, b_start=b0, b_end=b1,
                            mask=which != outside_bin, b_home=sel if b0 - 1 > sel else None, n_boot=boot)
                    blocks["pairs"].append(pair)
        cell["blocks"] = blocks
        out["boundaries"][str(mult)] = cell
    return out


# --------------------------------------------------------------------------- #
# Scoring: the protocol's fixed rules (section 6) and the feasibility screen (8)
# --------------------------------------------------------------------------- #
def _near(ks, target):
    return min(ks, key=lambda k: abs(k - target))


def _pair(cell, role_prefix, sel=0):
    for p in cell["blocks"]["pairs"]:
        if p["select"] == sel and p["role"].startswith(role_prefix):
            return p
    return None


def _octaves(p, K):
    """The seven octave bins of permanent rank from (K/128, K/64] to (K/2, K]."""
    return [r for lb, r in zip(p["bins"], p["drift"]) if lb != "outside" and lb[0] > 0 and lb[1] >= K / 64 - 1e-9]


def _slide(p, K):
    """Common slide per week: minus the pooled mean change of those bins, per week."""
    rows = [r for r in _octaves(p, K) if "mean" in r]
    if not rows:
        return None, None
    w = np.array([r["n"] for r in rows], dtype=float)
    pooled = float((w * np.array([r["mean"] for r in rows])).sum() / w.sum())
    return -pooled / p["weeks"], pooled


def _by_k(rows, k):
    return next((r for r in rows if r["k"] == k), None)


def _ok(row):
    return row is not None and "not_adjudicated" not in row


def score(res, system):
    """Apply the protocol's scoring rules to one panel's analyse() output.
    Returns {prediction: dict(status=PASS | FAIL | INCONCLUSIVE | NOT ADJUDICATED, detail=...)},
    plus 'screen' and the quantities that cross-panel predictions need."""
    K = res["K"]
    cell = res["boundaries"]["1.0"]
    ks, kc = cell["ks"], cell["k_centre"]
    KR = [k for k in ks if K / 16 - 1 <= k <= K]
    head = [k for k in ks if k < K / 16 - 1]
    wk = cell["weekly"]
    S = {}

    def put(name, status, **detail):
        S[name] = dict(status=status, detail=detail)

    # A1: the identity closes everywhere it was computed
    worst = max([w["max_abs_resid"] for c in res["boundaries"].values() for w in c["weekly"].values()] or [0.0])
    for c in res["boundaries"].values():
        for p in c["blocks"]["pairs"]:
            for a in p.get("accounting", []):
                if _ok(a):
                    worst = max(worst, abs(a["resid"]) / (a["k"] * p["weeks"]))
    put("A1", "PASS" if worst < 1e-9 else "FAIL", max_abs_resid=worst)

    # A2: one-week reordering against its small-step value
    w1 = wk.get("1")
    if w1:
        ratio = {k: w1["r"]["mean"][ks.index(k)] / w1["r_diff"]["mean"][ks.index(k)] for k in ks}
        put("A2", "PASS" if all(0.6 <= ratio[k] <= 1.2 for k in KR) else "FAIL", ratio={k: ratio[k] for k in KR})
        put("A2b", (NA if not head else "PASS" if all(ratio[k] < 1 for k in head) else "FAIL"),
            ratio={k: ratio[k] for k in head})
        k4 = _near(ks, K / 4)
        hmax = str(max(int(h) for h in wk))
        g1, gl = w1["g"]["mean"][ks.index(k4)], wk[hmax]["g"]["mean"][ks.index(k4)]
        put("A3", "INCONCLUSIVE" if gl <= 0 else "PASS" if g1 / gl >= 5 else "FAIL",
            k=k4, g_week1=g1, g_longest=gl, longest=int(hmax))
        worst4 = max(abs(wk[h]["shape"]["mean"][ks.index(k)]) / wk[h]["r"]["mean"][ks.index(k)]
                     for h in wk if int(h) <= 13 for k in KR if k != kc)
        put("A4", "PASS" if worst4 < 0.1 else "FAIL", worst_abs_shape_over_r=worst4)
    else:
        for nm in ("A2", "A2b", "A3", "A4"):
            put(nm, NA, reason="no weekly cells")

    prim = _pair(cell, "primary")
    nogap = _pair(cell, "no gap") or (prim if prim and "no gap available" in prim["role"] else None)
    names_b = ("B1", "B2", "B3", "B4", "B5", "B6", "B7", "B9", "B10")
    if prim is None or "not_adjudicated" in prim:
        for nm in names_b:
            put(nm, NA, reason="no certified primary pair")
        S["screen"] = dict(status=NA, detail=dict(reason="no certified primary pair"))
        S["_slide_no_gap"] = None
        return S
    acc, pred, dif, wks = prim["accounting"], prim["predictable"], prim["diffusion"], prim["weeks"]

    # B1: block-level reordering gain per endpoint per week against the constants of 2z-bf
    lo, hi = B1_RANGE[system]
    kk = [_near(ks, K / d) for d in (16, 8, 4, 2)]
    if all(_ok(_by_k(acc, k)) for k in kk):
        rb = {k: _by_k(acc, k)["R"] / (k * wks) for k in kk}
        put("B1", "PASS" if all(lo <= v <= hi for v in rb.values()) else "FAIL", r_block=rb, range=[lo, hi])
    else:
        put("B1", NA, reason="rank not certified")

    # B2, B3, B10: the common slide of cohorts fixed beforehand
    slide, pooled = _slide(prim, K)
    octs = _octaves(prim, K)
    full = len(octs) == 7 and all("mean" in r for r in octs) and slide is not None
    aK = _by_k(acc, kc)
    if system != "reddit":
        put("B2", NA, reason="stated for the Reddit panels; no pair with a gap here")
    elif not full or not _ok(aK):
        put("B2", NA, reason="a bin has too few survivors, or rank K is not certified")
    else:
        ratio = slide / (aK["R"] / (kc * wks)) if aK["R"] > 0 else float("nan")
        bad = any(r["q05"] > 0 for r in octs) or not (0.5 <= ratio <= 2)
        good = all(r["q95"] < 0 for r in octs) and 0.5 <= ratio <= 2
        put("B2", "FAIL" if bad else "PASS" if good else "INCONCLUSIVE", slide_per_week=slide,
            slide_over_r_at_K=ratio, bin_means=[r["mean"] for r in octs])
    if full and pooled != 0:
        m = 0.3 * abs(pooled)
        st = ["PASS" if (-m <= r["q05"] - pooled and r["q95"] - pooled <= m) else
              "FAIL" if (r["q05"] - pooled > m or r["q95"] - pooled < -m) else "INC" for r in octs]
        put("B3", "PASS" if st.count("PASS") >= 5 else "FAIL" if st.count("FAIL") >= 3 else "INCONCLUSIVE",
            bins=st, margin=m, pooled=pooled)
    else:
        put("B3", NA, reason="a bin has too few survivors")
    s12 = _slide(nogap, K)[0] if (nogap and nogap is not prim and "drift" in nogap) else None
    if system != "reddit" or s12 is None or slide is None:
        put("B10", NA, reason="needs two pairs from one cohort")
    elif s12 <= 0 or slide <= 0:
        put("B10", "INCONCLUSIVE", slide_no_gap=s12, slide_gap=slide)
    else:
        put("B10", "PASS" if 0.7 <= s12 / slide <= 1.5 else "FAIL", slide_no_gap=s12, slide_gap=slide, ratio=s12 / slide)

    # B4: association with the still earlier level
    car = prim.get("carriers", {})
    if system != "reddit" or "home" not in car:
        put("B4", NA, reason="no still earlier block in the primary pair")
    else:
        a, b = car["home_q05"], car["home_q95"]
        put("B4", "PASS" if (-0.10 <= a and b <= 0.10) else "FAIL" if (a > 0.10 or b < -0.10) else "INCONCLUSIVE",
            home=car["home"], q05=a, q95=b, pos=car["pos"])

    # B5: the predictable share, with its joint interval
    k5 = [_near(ks, K / 4), _near(ks, K / 2), kc]
    share = {}
    for k in k5:
        a, p = _by_k(acc, k), _by_k(pred, k)
        if _ok(a) and _ok(p) and p["G_pred"] is not None and a["R"] > 0:
            share[k] = (p["G_pred"] / a["R"], p["G_pred_q05"] / a["R"], p["G_pred_q95"] / a["R"])
    if system != "reddit" or len(share) < 3:
        put("B5", NA, reason="stated for the Reddit panels" if system != "reddit" else "share not computable", share=share)
    else:
        pts = all(0.5 <= v[0] <= 1.2 for v in share.values())
        out_ = any(v[2] < 0.5 or v[1] > 1.2 for v in share.values())
        put("B5", "PASS" if pts else "FAIL" if out_ else "INCONCLUSIVE", share=share)

    # B6: flow
    low = [k for k in ks if k <= K / 2 + 1]
    if all(_ok(_by_k(acc, k)) for k in low):
        fl = {k: abs(_by_k(acc, k)["F"]) / _by_k(acc, k)["R"] if _by_k(acc, k)["R"] > 0 else float("inf") for k in low}
        ent = {k: _by_k(acc, k)["n_enter"] / k for k in low}
        if system == "reddit":
            put("B6", "PASS" if all(v < 0.15 for v in fl.values()) else "FAIL", flow_over_r=fl)
        else:
            put("B6", "PASS" if all(v < 0.01 for v in ent.values()) else "FAIL", entrants_per_k=ent, flow_over_r=fl)
    else:
        fl = None
        put("B6", NA, reason="rank not certified")

    # B7 and B9: shape and the small-step bridge over one block step
    k7 = [k for k in KR if k != kc]
    if all(_ok(_by_k(acc, k)) for k in k7):
        sh = {k: abs(_by_k(acc, k)["shape"]) / _by_k(acc, k)["R"] for k in k7}
        put("B7", "PASS" if all(v < 0.3 for v in sh.values()) else "FAIL", abs_shape_over_r=sh)
    else:
        sh = None
        put("B7", NA, reason="rank not certified")
    if all(_ok(_by_k(acc, k)) and _ok(_by_k(dif, k)) for k in KR):
        br = {k: _by_k(acc, k)["R"] / _by_k(dif, k)["R_diff"] for k in KR}
        put("B9", "PASS" if all(0.5 <= v <= 1.1 for v in br.values()) else "FAIL", r_over_small_step=br)
    else:
        br = None
        put("B9", NA, reason="rank or diffusion band not certified")

    # ---- the feasibility screen (section 8): every part must hold -----------------------------
    parts = {}
    parts["identity closes"] = S["A1"]["status"]
    two = [share.get(_near(ks, K / 4)), share.get(_near(ks, K / 2))]
    if any(v is None for v in two):
        parts["predictable share determined and material"] = NA
    else:
        parts["predictable share determined and material"] = (
            "PASS" if all(0.5 <= v[1] and v[2] <= 1.5 and (v[2] - v[1]) / 2 <= 0.25 for v in two) else "FAIL")
    parts["shape stable"] = S["B7"]["status"]
    parts["flow small"] = NA if fl is None else ("PASS" if all(v < 0.15 for v in fl.values()) else "FAIL")
    parts["small-step bridge within 0.5 to 1.5"] = NA if br is None else (
        "PASS" if all(0.5 <= v <= 1.5 for v in br.values()) else "FAIL")
    parts["two pairs of one cohort agree"] = S["B10"]["status"]
    vals = list(parts.values())
    overall = "FAIL" if "FAIL" in vals else "PASS" if all(v == "PASS" for v in vals) else "INCONCLUSIVE"
    S["screen"] = dict(status=overall, detail=parts)
    S["_slide_no_gap"] = s12 if s12 is not None else (slide if nogap is prim else None)
    return S


def run(panel, reviewed, block=None):
    if not reviewed:
        sys.exit("refusing to run on a registered panel without --reviewed: the protocol "
                 "(MTS_PROTOCOL_STEPS_1_2_2026-10-13.md) is reviewed before anything runs")
    import minimal_rankdiff as mrd
    cfg = PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    step = 2 if (cfg["system"] == "reddit" and (block or cfg["block"]) == 26) else 1   # whole years apart
    res = analyse(df, cfg["K"], block or cfg["block"], pair_step=step)
    res.update(panel=panel, system=cfg["system"], finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    name = f"m1__{panel}.json" if block is None else f"m1__{panel}__block{block}.json"
    (OUT / "cells" / name).write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] T={res['T']} tracked={res['n_tracked']} blocks={res['blocks']['n']} "
          f"certified_top={res['blocks']['certified_top']} written", flush=True)


def _fmt(v):
    return "n/a" if v is None else f"{v:.5f}" if isinstance(v, float) else str(v)


def report():
    """Tables and scores for every cell on disk, to stdout and report_tables.md."""
    lines, scores, slides = [], {}, {}
    for f in sorted((OUT / "cells").glob("m1__*.json")):
        d = json.loads(f.read_text())
        tag = f.stem.replace("m1__", "")
        cell = d["boundaries"]["1.0"]
        lines += [f"\n## {tag}  (K = {d['K']}, T = {d['T']}, tracked = {d['n_tracked']}, block = {d['block']} weeks, "
                  f"presence >= {d['presence']})",
                  f"certified exact top of each block ladder: {d['blocks']['certified_top']}", "",
                  "Weekly positions, rates per endpoint per week (centre K):", "",
                  "| horizon | k | occupant loss g | reordering r | small-step k v / 2S | flow f | shape | replaced |",
                  "|---|---|---|---|---|---|---|---|"]
        for h, w in cell["weekly"].items():
            for j, k in enumerate(w["ks"]):
                lines.append(f"| {h} | {k} | {w['g']['mean'][j]:.5f} | {w['r']['mean'][j]:.5f} | "
                             f"{w['r_diff']['mean'][j]:.5f} | {w['f']['mean'][j]:+.5f} | {w['shape']['mean'][j]:+.5f} | "
                             f"{w['swap']['mean'][j]:.3f} |")
        for p in cell["blocks"]["pairs"]:
            lines += ["", f"Blocks, cohort on block {p['select']}, change {p['start']} -> {p['end']} "
                          f"({p['weeks']} weeks): **{p['role']}**"]
            if "not_adjudicated" in p:
                lines.append(f"NOT ADJUDICATED: {p['not_adjudicated']}")
                continue
            lines += ["", "| k | g | r | small-step | f | shape | predictable share of R (5-95%) | of cohort survivors' loss | exits | from outside |",
                      "|---|---|---|---|---|---|---|---|---|---|"]
            for a, pl, dg in zip(p["accounting"], p["predictable"], p["diffusion"]):
                if "not_adjudicated" in a:
                    lines.append(f"| {a['k']} | NOT ADJUDICATED: {a['not_adjudicated']} | | | | | | | | |")
                    continue
                kw = a["k"] * p["weeks"]
                sh = ("n/a" if pl["G_pred"] is None or a["R"] <= 0 else
                      f"{pl['G_pred'] / a['R']:.2f} ({pl['G_pred_q05'] / a['R']:.2f} to {pl['G_pred_q95'] / a['R']:.2f})")
                sc = "n/a" if pl["G_pred"] is None or pl["G_cohort"] == 0 else f"{pl['G_pred'] / pl['G_cohort']:.2f}"
                rd = "n/a" if "not_adjudicated" in dg else f"{dg['R_diff'] / kw:.5f}"
                lines.append(f"| {a['k']} | {a['G'] / kw:.5f} | {a['R'] / kw:.5f} | {rd} | {a['F'] / kw:+.5f} | "
                             f"{a['shape'] / kw:+.5f} | {sh} | {sc} | {pl['n_exit']} | {pl['n_outside']} |")
            lines += ["", "| cohort bin (permanent rank) | survivors | attrition | mean change vs centre (5-95%) | per week |",
                      "|---|---|---|---|---|"]
            for lb, r in zip(p["bins"], p["drift"]):
                name = "outside (retrospective; not a cohort)" if lb == "outside" else f"({lb[0]:.0f}, {lb[1]:.0f}]"
                if "mean" in r:
                    lines.append(f"| {name} | {r['n']} | {r['attrition']:.3f} | {r['mean']:+.4f} "
                                 f"({r['q05']:+.4f} to {r['q95']:+.4f}) | {r['mean'] / p['weeks']:+.5f} |")
                else:
                    lines.append(f"| {name} | {r['n']} | {r['attrition']:.3f} | too few | |")
            if "carriers" in p:
                c = p["carriers"]
                lines.append("")
                lines.append("Predictive associations (not a mechanism reading): " + ", ".join(
                    f"{nm} {c[nm]:+.3f} ({c[nm + '_q05']:+.3f} to {c[nm + '_q95']:+.3f})"
                    for nm in ("pos", "home") if nm in c) + f"; n = {c.get('n')}")
        if tag in PRIMARY:
            scores[tag] = score(d, d["system"])
            slides[tag] = (d["system"], scores[tag].pop("_slide_no_gap"))
    lines += ["", "## Scores (protocol section 6) and the feasibility screen (section 8)", ""]
    preds = ["A1", "A2", "A2b", "A3", "A4", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B9", "B10", "screen"]
    lines += ["| | " + " | ".join(scores) + " |", "|---|" + "---|" * len(scores)]
    for nm in preds:
        lines.append(f"| {nm} | " + " | ".join(s[nm]["status"] for s in scores.values()) + " |")
    fb = [v for s, v in slides.values() if s == "facebook" and v]
    rd = [v for s, v in slides.values() if s == "reddit" and v]
    if fb and rd and min(rd) > 0:
        ratio = fb[0] / float(np.mean(rd))
        b8 = "PASS" if 1.3 <= ratio <= 3 else "FAIL"
        lines += ["", f"B8 (cross-platform; no-gap pairs, block lengths differ, so weak): Facebook slide over the Reddit "
                      f"mean = {ratio:.2f}: {b8}"]
    else:
        lines += ["", f"B8: {NA} (needs a positive no-gap slide on both platforms)"]
    lines += ["", "Details of every score:", "", "```", json.dumps(scores, indent=1, default=float), "```"]
    text = "\n".join(lines) + "\n"
    (OUT / "report_tables.md").write_text(text)
    print(text)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("panel", choices=sorted(PANELS))
    r.add_argument("--reviewed", action="store_true")
    r.add_argument("--block", type=int, default=None,
                   help="declared secondary runs only (protocol section 7)")
    sub.add_parser("report")
    a = ap.parse_args()
    run(a.panel, a.reviewed, a.block) if a.cmd == "run" else report()


if __name__ == "__main__":
    main()
