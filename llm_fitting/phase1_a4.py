#!/usr/bin/env python3
"""A4 of the 2026-10-12 work plan: how much of the band profiles is structure.

Design and predictions: llm_fitting/runs/2026-10-12_A4_profiles/DESIGN.md.

    python llm_fitting/phase1_a4.py
    python llm_fitting/phase1_a4.py --rerank-halves   # 2026-10-12 correction, see below

The first command reproduces the recorded run (MODEL_STATUS 2z-at). Codex's
review R1 found that its half fits are not in the full fit's rank coordinate:
the halves keep their full-universe ranks while the estimator recomputes N
from each half, so every half coordinate is shifted by log 2 and the lower
half of each half's endpoints is clipped at z = 0. `--rerank-halves` ranks
each half within itself (a quantile coordinate survives thinning), compares
the halves only where both have knots, and writes
`summary_reranked_halves.json`. Design: `CORRECTION_DESIGN.md`.
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ladder_explainer as le  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-12_A4_profiles")
PANELS = ("facebook_a", "reddit_submissions_long", "reddit_comments")


def profiles(p):
    kap = np.asarray(p.kappa_z, dtype=float)
    sp, st, so = (np.asarray(a, dtype=float) for a in (p.sigma_perm, p.sigma_trans, p.sigma_obs))
    with np.errstate(divide="ignore", invalid="ignore"):
        return dict(z=np.asarray(p.z_knots, dtype=float),
                    log_sigma_perm=np.log(sp), log_sigma_trans=np.log(st),
                    log_sigma_obs=np.log(so), log_kappa=np.log(kap),
                    log_V=np.log(sp ** 2 / (2.0 * kap)), kappa=kap)


def r2(y, X):
    ok = np.isfinite(y)
    if ok.sum() < X.shape[1] + 2 or np.var(y[ok]) == 0:
        return np.nan, None
    beta, *_ = np.linalg.lstsq(X[ok], y[ok], rcond=None)
    res = y[ok] - X[ok] @ beta
    return float(1 - res.var() / y[ok].var()), beta


def main(rerank=False):
    out = {}
    for panel in PANELS:
        cfg = le.PANELS[panel]
        spec = le.spec_for(panel, "struct", cfg["struct_path"][-1])
        df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[cfg["struct"]]),
                                   cfg["K"], buffer_mult=4)
        order = df.groupby("entity_id")["rank"].mean().sort_values(kind="mergesort").index
        halves = [set(order[0::2]), set(order[1::2])]
        attrs = dict(df.attrs)
        fits = [le._fit(df, spec)]
        for h in halves:
            sub = df[df["entity_id"].isin(h)].copy()
            if rerank:
                sub = mrd._rank_within(sub)
            sub.attrs.update(attrs)
            fits.append(le._fit(sub, spec))
        full, a, b = (profiles(f) for f in fits)
        z = full["z"]
        N = fits[0].N
        w0 = np.asarray(fits[0].w0, dtype=float)
        rank = np.clip(np.exp(z) * N, 1, len(w0))
        home = np.interp(rank, np.arange(1, len(w0) + 1), w0)
        res = dict(knots=int(len(z)), knots_half=[int(len(a["z"])), int(len(b["z"]))],
                   spec=spec, profiles={})
        # where both halves have knots; the recorded run used every full knot
        sup = (z >= max(a["z"].min(), b["z"].min())) & (z <= min(a["z"].max(), b["z"].max())) \
            if rerank else np.ones(len(z), dtype=bool)
        res["knots_compared"] = int(sup.sum())
        res["N_half"] = [int(f.N) for f in fits[1:]]
        for name in ("log_sigma_perm", "log_sigma_trans", "log_sigma_obs",
                     "log_kappa", "log_V"):
            ya, yb = np.interp(z, a["z"], a[name]), np.interp(z, b["z"], b[name])
            ok = np.isfinite(ya) & np.isfinite(yb) & sup
            r = float(np.corrcoef(ya[ok], yb[ok])[0, 1]) if ok.sum() > 3 and \
                np.std(ya[ok]) > 0 and np.std(yb[ok]) > 0 else np.nan
            rel = 2 * r / (1 + r) if np.isfinite(r) and r > -1 else np.nan
            y = full[name]
            one = np.ones_like(z)
            lin, _ = r2(y, np.c_[one, z])
            quad, _ = r2(y, np.c_[one, z, z * z])
            _, beta = r2(y, np.c_[one, home])
            if rerank:      # the line on the knots the halves are compared on
                lin_s, _ = r2(np.where(sup, y, np.nan), np.c_[one, z])
            else:
                lin_s = lin
            res["profiles"][name] = dict(
                half_corr=r, reliability=rel, r2_line=lin, r2_quad=quad,
                r2_line_compared=lin_s,
                line_share=lin_s / rel if rel and rel > 0 else np.nan,
                scaling_exponent=None if beta is None else float(beta[1]),
                sd_across_knots=float(np.nanstd(y)))
        kap = full["kappa"]
        top = slice(0, max(1, len(kap) // 3))
        grid = np.unique(np.round(kap, 6))
        res["kappa"] = dict(values_used=[float(v) for v in grid],
                            share_slowest=float(np.mean(np.isclose(kap, kap.min()))),
                            share_fastest=float(np.mean(np.isclose(kap, kap.max()))),
                            slowest=float(kap.min()), fastest=float(kap.max()),
                            top_third_share_slowest=float(np.mean(np.isclose(kap[top], kap.min()))),
                            grid_slowest=float(1 - mrd.A_GRID_VR.max()),
                            grid_fastest=float(1 - mrd.A_GRID_VR.min()))
        out[panel] = res
        print(f"\n## {panel}: {res['knots']} knots (halves {res['knots_half']}, N {res['N_half']}; "
              f"{res['knots_compared']} knots compared)")
        print(f"{'profile':16s} half-corr reliab  R2 line  R2 quad  line/reliab  exponent  SD")
        for name, v in res["profiles"].items():
            print(f"{name:16s} {v['half_corr']:8.2f} {v['reliability']:6.2f} {v['r2_line']:8.2f} "
                  f"{v['r2_quad']:8.2f} {v['line_share']:11.2f} "
                  f"{v['scaling_exponent'] if v['scaling_exponent'] is not None else float('nan'):9.2f} "
                  f"{v['sd_across_knots']:5.2f}")
        k = res["kappa"]
        print(f"kappa values used: {k['values_used']}; at slowest {k['share_slowest']:.0%} "
              f"(top third {k['top_third_share_slowest']:.0%}), at fastest {k['share_fastest']:.0%}; "
              f"grid {k['grid_slowest']:.3f}..{k['grid_fastest']:.3f}")
    name = "summary_reranked_halves.json" if rerank else "summary.json"
    (OUT / name).write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rerank-halves", action="store_true")
    main(rerank=ap.parse_args().rerank_halves)
