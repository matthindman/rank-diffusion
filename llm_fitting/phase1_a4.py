#!/usr/bin/env python3
"""A4 of the 2026-10-12 work plan: how much of the band profiles is structure.

Design and predictions: llm_fitting/runs/2026-10-12_A4_profiles/DESIGN.md.

    python llm_fitting/phase1_a4.py
"""
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


def main():
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
        for name in ("log_sigma_perm", "log_sigma_trans", "log_sigma_obs",
                     "log_kappa", "log_V"):
            ya, yb = np.interp(z, a["z"], a[name]), np.interp(z, b["z"], b[name])
            ok = np.isfinite(ya) & np.isfinite(yb)
            r = float(np.corrcoef(ya[ok], yb[ok])[0, 1]) if ok.sum() > 3 and \
                np.std(ya[ok]) > 0 and np.std(yb[ok]) > 0 else np.nan
            rel = 2 * r / (1 + r) if np.isfinite(r) and r > -1 else np.nan
            y = full[name]
            one = np.ones_like(z)
            lin, _ = r2(y, np.c_[one, z])
            quad, _ = r2(y, np.c_[one, z, z * z])
            _, beta = r2(y, np.c_[one, home])
            res["profiles"][name] = dict(
                half_corr=r, reliability=rel, r2_line=lin, r2_quad=quad,
                line_share=lin / rel if rel and rel > 0 else np.nan,
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
        print(f"\n## {panel}: {res['knots']} knots (halves {res['knots_half']})")
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
    (OUT / "summary.json").write_text(json.dumps(out, indent=1, default=float))


if __name__ == "__main__":
    main()
