"""D1-D5 of DESIGN.md: is Reddit's momentum a mechanism or an artefact?

    python llm_fitting/runs/2026-10-18_momentum/momentum_diagnostics.py
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402

PRES = 0.9
CUT = pd.Timestamp("2021-07-01")


def load(panel):
    K = m1.PANELS[panel]["K"]
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]]), K, buffer_mult=4)
    T = int(df["period"].max()) + 1
    dates = pd.to_datetime(df.groupby("period")["ts"].first().reindex(range(T)).to_numpy())
    X = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T)).to_numpy(float)
    first = np.argmax(np.isfinite(X), axis=0)
    return K, X, dates, first


def blocks(X, L):
    T = X.shape[0]
    nb = T // L
    st = T - nb * L
    out = []
    for b in range(nb):
        seg = X[st + b * L:st + (b + 1) * L]
        ok = np.isfinite(seg).mean(0) >= PRES
        with np.errstate(all="ignore"):
            out.append(np.where(ok, np.nanmean(seg, 0), np.nan))
    return np.array(out), st, nb


def mom(B, a, K, sel=None, floor=False):
    top = np.argsort(np.where(np.isfinite(B[a]), -B[a], np.inf))[:K]
    if sel is not None:
        top = top[sel[top]]
    b0, b1, b2 = B[a][top], B[a + 1][top], B[a + 2][top]
    if floor:
        fl = [np.sort(B[c][np.isfinite(B[c])])[::-1][min(K, np.isfinite(B[c]).sum()) - 1] for c in (a + 1, a + 2)]
        b1 = np.where(np.isfinite(b1), b1, fl[0])
        b2 = np.where(np.isfinite(b2), np.where(np.isfinite(b1), b2, fl[1]), fl[1])
    ok = np.isfinite(b0) & np.isfinite(b1) & np.isfinite(b2)
    if ok.sum() < 30:
        return np.nan, int(ok.sum())
    return float(spearmanr(b1[ok] - b0[ok], b2[ok] - b1[ok]).correlation), int(ok.sum())


out = {}
for panel, half in (("reddit_submissions_long", 26), ("reddit_comments_ext", 26), ("facebook_a", 13)):
    K, X, dates, first = load(panel)
    res = {}
    # D1 / D5: consecutive half-year (or quarter) changes, by triple, before and after the July 2021 capture change
    B, st, nb = blocks(X, half)
    tri = []
    for a in range(nb - 2):
        end = dates[min(st + (a + 3) * half - 1, len(dates) - 1)]
        r, n = mom(B, a, K)
        tri.append(dict(start=str(dates[st + a * half].date()), end=str(end.date()), pre=bool(end < CUT), rho=r, n=n))
    res["by_triple"] = tri
    pre = [t["rho"] for t in tri if t["pre"] and np.isfinite(t["rho"])]
    post = [t["rho"] for t in tri if not t["pre"] and np.isfinite(t["rho"])]
    res["pre_mean"] = float(np.mean(pre)) if pre else None
    res["post_mean"] = float(np.mean(post)) if post else None
    print(f"{panel}: {half}-week blocks, momentum by triple " + "; ".join(f"{t['start']}..{t['end']} {t['rho']:+.3f}" for t in tri), flush=True)
    print(f"   mean before July 2021 {res['pre_mean']}, after {res['post_mean']}", flush=True)
    if panel.startswith("reddit"):
        BA, sta, nba = blocks(X, 52)
        # D2: age, annual triple 2020 -> 2021 -> 2022 (blocks 1, 2, 3); first observed relative to the panel start
        young = first >= 26
        old = first <= 3
        res["age"] = dict(young=mom(BA, 1, K, young), old=mom(BA, 1, K, old), all=mom(BA, 1, K))
        print(f"   D2 age (annual 2020->2021->2022): young {res['age']['young']}, old {res['age']['old']}, all {res['age']['all']}", flush=True)
        # D3: size strata, annual triple 0 -> 1 -> 2
        order = np.argsort(np.where(np.isfinite(BA[0]), -BA[0], np.inf))
        rank0 = np.empty(X.shape[1], int)
        rank0[order] = np.arange(1, X.shape[1] + 1)
        strata = {"top K/16": rank0 <= K // 16, "K/16-K/4": (rank0 > K // 16) & (rank0 <= K // 4), "K/4-K": (rank0 > K // 4) & (rank0 <= K)}
        res["size"] = {k: mom(BA, 0, K, v) for k, v in strata.items()}
        print(f"   D3 size (annual 2019->2020->2021): " + ", ".join(f"{k} {v}" for k, v in res["size"].items()), flush=True)
        # D4: survivorship -- absent blocks kept at the floor
        res["floor"] = dict(dropped=mom(BA, 0, K), floored=mom(BA, 0, K, floor=True))
        print(f"   D4 survivorship (annual 2019->2020->2021): dropped {res['floor']['dropped']}, absent at floor {res['floor']['floored']}", flush=True)
    out[panel] = res
Path(__file__).with_name("momentum_diagnostics.json").write_text(json.dumps(out, indent=1))
