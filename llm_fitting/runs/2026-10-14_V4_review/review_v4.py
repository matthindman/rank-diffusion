"""Claude's review of Codex's V4 (planted rivals), 2026-10-14.  Reads result files only.

1. Recomputes V4's headline numbers from its seed-level tables.
2. Scores the prediction registered in MODEL_STATUS 2z-bw before the grid was complete.
3. Slide, reordering gain and occupants' loss in the planted worlds (cross-check of 2z-bw).
4. Applies V4's fixed nearest-centroid rule to the empirical step 1 cells.
5. The lasting reordering rate from how the total gain grows with spacing, step 1 cells.

Needs seed_evidence.jsonl.gz, which is on branch codex/v4-planted-rivals and ignored on main.

    PYTHONPATH=. python llm_fitting/runs/2026-10-14_V4_review/review_v4.py
"""
import gzip
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
from llm_fitting import mts_step1 as m1  # noqa: E402
from llm_fitting.validation import balance_rivals_sim as sim  # noqa: E402
from llm_fitting.validation import balance_rivals_study as st  # noqa: E402

V4 = Path("llm_fitting/runs/2026-10-13_V4_planted_rivals")
M1 = Path("llm_fitting/runs/2026-10-13_M1_balance/cells")
REDDIT = ("reddit_comments_ext", "reddit_submissions_long")
PANELS = REDDIT + ("facebook_a",)
num = lambda x: np.nan if x is None else float(x)  # noqa: E731


def wilson(k, n, z=1.959964):
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    s = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (c - s) / d, (c + s) / d


def part1(h, ev):
    print("PART 1. V4's headline numbers, recomputed.")
    ok = (h.assign(good=(h.status == "VALID") & (h.relative_error.abs() <= 0.25))
           .groupby(["panel", "rival", "seed"]).good.all().groupby(level=[0, 1]).sum())
    best = ok.idxmax()
    print(f"  slope success: rows with Wilson lower bound >= 0.80: {sum(wilson(int(k), 20)[0] >= 0.8 for k in ok)} of {len(ok)}; "
          f"largest {best[0]} {best[1]} {int(ok.max())}/20 [{wilson(int(ok.max()), 20)[0]:.3f},{wilson(int(ok.max()), 20)[1]:.3f}]; "
          f"home {[int(ok[(p, 'home')]) for p in PANELS]}")
    print(f"  forecasts valid {int((h.status == 'VALID').sum())}, failed {int((h.status != 'VALID').sum())}: "
          f"{h[h.status != 'VALID'].reason.value_counts().to_dict()}")
    c = pd.read_csv(V4 / "classifications.csv.gz")
    t = c.groupby(["rival", "panel"]).correct.sum().unstack()
    print("  class recovery, correct of 20 (comments, submissions, Facebook):")
    for rv in sim.RIVALS:
        print(f"    {rv:<17} {[int(t.loc[rv, p]) for p in PANELS]}")

    def joint(r):
        p = m1._pair(r["analysis"]["boundaries"]["1.0"], "primary")
        if p is None or "carriers" not in p:
            return None
        return bool(num(p["carriers"].get("pos_q95")) < 0 and num(p["carriers"].get("home_q05")) > 0)
    for panel in REDDIT:
        for rv in ("fixed_walk", "rw_persistent"):
            v = [joint(r) for r in ev if r["provenance"]["panel"] == panel and r["provenance"]["rival"] == rv]
            print(f"  joint restoring sign, {panel} {rv}: {sum(bool(x) for x in v)}/{len(v)}")


def part2(h):
    print("\nPART 2. Prediction registered in 2z-bw: for fixed_walk and unstabilized on the two Reddit profiles the median")
    print("signed slope error is between -20% and -40%, and within +-15% when predictions are multiplied by 1.5.")
    h = h.assign(e15=1.5 * (1 + h.relative_error) - 1)
    hit1 = hit2 = 0
    for panel in REDDIT:
        for rv in ("fixed_walk", "unstabilized"):
            g = h[(h.panel == panel) & (h.rival == rv) & (h.status == "VALID")]
            a, b = g.relative_error.median(), g.e15.median()
            hit1 += -0.40 <= a <= -0.20
            hit2 += abs(b) <= 0.15
            ok15 = int(g.groupby("seed").apply(lambda s: len(s) == 5 and bool((s.e15.abs() <= 0.25).all())).sum())
            byk = "  ".join(f"k={k}: {x.relative_error.median():+.2f}|{x.e15.median():+.2f}" for k, x in g.groupby("k"))
            print(f"  {panel:<24} {rv:<13} as designed {a:+.3f}   times 1.5 {b:+.3f}   all five within 25% with the factor: {ok15}/20")
            print(f"      by cut-off (as designed | times 1.5): {byk}")
    print(f"  first part holds in {hit1} of 4, second part in {hit2} of 4.")
    ok = (h.assign(good=(h.status == "VALID") & (h.e15.abs() <= 0.25)).groupby(["panel", "rival", "seed"]).good.all()
           .groupby(level=[0, 1]).sum())
    print(f"  with the factor, rows with Wilson lower bound >= 0.80: {sum(wilson(int(k), 20)[0] >= 0.8 for k in ok)} of {len(ok)}; "
          f"largest {ok.idxmax()} {int(ok.max())}/20")


def part3(ev):
    print("\nPART 3. Block quantities in the planted worlds, medians over 20 seeds, and the same in the data.")
    print("  panel                    world            slide    block r  block g  r/slide  share")
    for panel in PANELS:
        for rv in ("fixed_walk", "unstabilized", "rw_medium", "rw_persistent", "rank_drift", "position_ou"):
            F = [r["features"] for r in ev if r["provenance"]["panel"] == panel and r["provenance"]["rival"] == rv]
            md = lambda k: float(np.nanmedian([num(f.get(k)) for f in F]))  # noqa: E731
            print(f"  {panel:<24} {rv:<15} {md('slide'):8.5f} {md('block/r'):8.5f} {md('block/g'):8.5f} "
                  f"{md('block/r') / md('slide'):7.2f} {md('share'):6.2f}")
        fe = st.feature_vector(json.loads((M1 / f"m1__{panel}.json").read_text()))
        print(f"  {panel:<24} {'DATA':<15} {fe['slide']:8.5f} {fe['block/r']:8.5f} {fe['block/g']:8.5f} "
              f"{fe['block/r'] / fe['slide']:7.2f} {fe['share']:6.2f}\n")


def part4(ev):
    print("PART 4. V4's fixed nearest-centroid rule applied to the empirical step 1 cells (all 20 seeds per rival as training).")
    for panel in PANELS:
        tr = [r for r in ev if r["provenance"]["panel"] == panel]
        feats = sorted({k for r in tr for k in r["features"]})
        A = np.array([[num(r["features"].get(k)) for k in feats] for r in tr])
        labels = np.array([r["provenance"]["rival"] for r in tr])
        fe = st.feature_vector(json.loads((M1 / f"m1__{panel}.json").read_text()))
        q = np.array([[num(fe.get(k)) for k in feats]])
        cnt = np.isfinite(A).sum(0)
        mu = np.nansum(A, axis=0) / np.maximum(cnt, 1)
        sd = np.sqrt(np.nansum((A - mu) ** 2, axis=0) / np.maximum(cnt - 1, 1))
        keep = (cnt >= 2) & (sd > 1e-12)
        tf = lambda B: np.column_stack([np.where(np.isfinite(B[:, keep]), (B[:, keep] - mu[keep]) / sd[keep], 0.0),  # noqa: E731
                                        (~np.isfinite(B[:, keep])).astype(float)])
        Z, zq = tf(A), tf(q)[0]
        kept = [f for f, k in zip(feats, keep) if k]
        res = []
        for rv in sim.RIVALS:
            C = Z[labels == rv]
            own = np.sum((C - C.mean(0)) ** 2, axis=1)
            res.append((float(np.sum((C.mean(0) - zq) ** 2)), rv, float(np.median(own)), float(own.max())))
        res.sort()
        allown = np.concatenate([np.sum((Z[labels == rv] - Z[labels == rv].mean(0)) ** 2, axis=1) for rv in sim.RIVALS])
        print(f"  {panel}: {len(kept)} features.  Squared distance of the data to each centroid (planted seeds to their own: "
              f"median {np.median(allown):.1f}, 95th percentile {np.percentile(allown, 95):.1f})")
        print("     " + "; ".join(f"{rv} {d:.0f}" for d, rv, _, _ in res))
        d0, rv0 = res[0][0], res[0][1]
        cen0 = Z[labels == rv0].mean(0)
        contrib = (zq - cen0) ** 2
        names = kept + [k + " (missing in one, present in the other)" for k in kept]
        top = [f"{names[j]} {contrib[j] / d0:.0%}" for j in np.argsort(-contrib)[:5]]
        print(f"     nearest {rv0} ({sim.CLASSES[rv0]}); the distance comes mostly from: {', '.join(top)}")


def part5():
    print("\nPART 5. Lasting reordering rate from the growth of the total gain with spacing: (R(D2) D2 - R(D1) D1) / (D2 - D1)")
    print("per endpoint, median over cut-offs K/16..K.  A constant left in block means by passing swings, and the L/3")
    print("offset of averaging, both cancel.  Approximate: the reordering gain is not exactly additive.")
    for panel in REDDIT:
        for f in (panel, panel + "__block26"):
            c = json.loads((M1 / f"m1__{f}.json").read_text())
            K, L = c["K"], c["block"]
            tot, seen = {}, set()
            for p in c["boundaries"]["1.0"]["blocks"]["pairs"]:
                if (p["start"], p["end"]) in seen:
                    continue
                seen.add((p["start"], p["end"]))
                rows = [x for x in p["accounting"] if K / 16.5 <= x["k"] <= K]
                tot.setdefault(p["weeks"], []).append(float(np.median([x["R"] / x["k"] for x in rows])))
            Ds = sorted(tot)
            mean = {D: float(np.mean(tot[D])) for D in Ds}
            est = [f"D {a}->{b}: {(mean[b] - mean[a]) / (b - a):.4f}" for a, b in zip(Ds[:-1], Ds[1:])]
            if len(Ds) > 2:
                est.append(f"D {Ds[0]}->{Ds[-1]}: {(mean[Ds[-1]] - mean[Ds[0]]) / (Ds[-1] - Ds[0]):.4f}")
            print(f"  {panel:<24} L={L:<3} total gain by spacing {', '.join(f'{D}: {mean[D]:.3f}' for D in Ds)}   rate  {';  '.join(est)}")
        fe = st.feature_vector(json.loads((M1 / f"m1__{panel}.json").read_text()))
        print(f"  {panel:<24} cohort slide, primary pair: {fe['slide']:.4f}")


if __name__ == "__main__":
    h = pd.read_csv(V4 / "heldout_slopes.csv.gz")
    rows = [json.loads(line) for line in gzip.open(V4 / "seed_evidence.jsonl.gz", "rt")]
    ev = [r for r in rows if r["provenance"]["stage"] == "evaluation" and r["status"] == "RAN-OK"]
    part1(h, ev)
    part2(h)
    part3(ev)
    part4(ev)
    part5()
