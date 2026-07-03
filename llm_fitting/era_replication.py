#!/usr/bin/env python3
"""P4 replication (2026-07-03): do the Era-A parameter estimates reproduce on
the post-recovery CrowdTangle segments?

Era discipline (instrument_eras.py / MODEL_STATUS 2g): C (12 complete wks) and
D (2 complete wks -- P0 amendment) are REPLICATION segments only.  NO OOS gates
(segments far too short for rolling-origin splits) and NO entry/boundary/
coverage claims (fixed censored panel).  Each estimate is limited to what the
segment supports, with every adaptation DECLARED here:

  * temperament s: min_changes=8 on C (T=12 gives max 11 changes; the default
    12 would silently force s=0 -- the 2f short-window wrinkle).  For an
    apples-to-apples comparison the Era-A reference s is recomputed at
    min_changes=8 alongside the canonical (min_changes=12) value.
  * MD covariance partition on C: md_lags=4 (gamma_0..4; A's canonical fit
    uses 0..6 -- lag-6 autocovariances from 11-change series are too noisy).
    The Era-A reference row is recomputed at md_lags=4 for comparability.
  * Spec-B daily noise floor: day-guarded, fewer bands (8 on C, 4 on D) to
    keep >=300 entity-weeks per band.
  * era C in-sample scorecard: reps=5; horizon-13 metrics (VR13, RACF13,
    R2_13, Pers13) are undefined at T=12 and count as fails in the x/15
    print -- max achievable is 11/15.  t_df is inf by construction (the
    kurtosis moment needs >=12 within-entity changes; T=12 gives 11).
  * era D: WEEKLY estimation infeasible (T=2) -- daily noise floor only.

Usage:
  python llm_fitting/era_replication.py            # full A/C/D comparison
"""
from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402
import spec_b_sigma_obs as sb  # noqa: E402

K_UNIVERSE = 3500  # pre-registered FB coverage K (COVERAGE_K["facebook_a"][90])


def _curve_summary(z, vals, p):
    """head / mid / tail values of a knot curve (top..tail by permanent rank)."""
    i_mid = len(vals) // 2
    return f"{vals[0]:.3f}/{vals[i_mid]:.3f}/{vals[-1]:.3f}"


def era_params(platform: str, md_lags: int, min_changes: int,
               specb_bands: int, weekly: bool = True) -> dict:
    cfg = mrd.PLATFORMS[platform]
    df = mrd.load_panel(cfg)
    T = int(df["period"].max()) + 1
    uni = mrd.restrict_universe(df, K_UNIVERSE, buffer_mult=4)
    out = dict(platform=platform, T=T, mean_N=float(df.groupby("period").size().mean()))

    if weekly:
        t = mrd.estimate_temperament(uni, min_changes=min_changes)
        out["s"] = t["s"]; out["s_nent"] = t["n_entities"]
        p = mrd.estimate(uni, min_knot_n=8, md_lags=md_lags, t_tails=True)
        p = replace(p, temper_s=t["s"])
        out["params"] = p
        out["kappa"] = _curve_summary(p.z_knots, p.kappa_z, p)
        out["sigma_obs"] = _curve_summary(p.z_knots, p.sigma_obs, p)
        out["sigma_perm"] = _curve_summary(p.z_knots, p.sigma_perm, p)
        out["sigma_trans"] = _curve_summary(p.z_knots, p.sigma_trans, p)
        out["phi"] = _curve_summary(p.z_knots, p.phi, p)
        out["t_df"] = p.t_df

    daily = sb.load_daily(set(uni["entity_id"].unique()),
                          path=cfg["daily_path"], day_guard=cfg.get("day_guard", False))
    cur = sb.spec_b_curve(uni, daily, n_bands=specb_bands)
    out["specB"] = cur
    return out


def scorecard(platform: str, p, reps: int = 5) -> tuple:
    """In-sample scorecard on a short segment (mirrors run_platform's scoring)."""
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[platform]),
                               K_UNIVERSE, buffer_mult=4)
    score_k = df.attrs["score_k"]
    T = int(df["period"].max()) + 1
    top_k = max(10, int(round(0.01 * score_k)))
    ev, er, et, ers = mrd.empirical_structures(df, top_k, topid_k=score_k)
    emp = mrd.diagnostics(ev, er, et, ers, top_k, score_k=score_k)
    sims = [mrd.diagnostics(*mrd._sim_struct(mrd.simulate(p, T, seed=s, kappa=None,
                                                          top_record=score_k)),
                            top_k, score_k=score_k)
            for s in range(reps)]
    sim = {k: np.nanmean([s[k] for s in sims]) for k in emp if not k.startswith("_")}
    mrd._print_compare(emp, sim, p, top_k, score_k=score_k)
    return emp, sim


if __name__ == "__main__":
    print("=" * 72)
    print("P4 — CrowdTangle era replication: A (reference) vs C vs D")
    print("=" * 72)

    print("\n--- Era A reference (md_lags=4 + min_changes=8 for comparability; "
          "canonical A uses md6/min12 -- see MODEL_STATUS P1) ---")
    A = era_params("facebook_a", md_lags=4, min_changes=8, specb_bands=12)
    uniA = None  # A's canonical s at default min_changes, for the wrinkle report
    sA12 = mrd.estimate_temperament(
        mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS["facebook_a"]),
                              K_UNIVERSE, buffer_mult=4))["s"]
    print(f"  A: s(min8)={A['s']:.3f}  s(min12, canonical)={sA12:.3f}")

    print("\n--- Era C (T=12): temperament + MD(4) + Spec-B ---")
    C = era_params("facebook_c", md_lags=4, min_changes=8, specb_bands=8)

    print("\n--- Era D (T=2): weekly estimation INFEASIBLE -- Spec-B floor only ---")
    D = era_params("facebook_d", md_lags=0, min_changes=8, specb_bands=4, weekly=False)

    print("\n" + "=" * 72)
    print("PARAMETER REPLICATION TABLE (head/mid/tail by permanent rank)")
    print("=" * 72)
    hdr = f"{'quantity':<22}{'Era A':>20}{'Era C':>20}{'Era D':>16}"
    print(hdr)
    rows = [
        ("T (complete wks)", A["T"], C["T"], D["T"]),
        ("temperament s", f"{A['s']:.3f} (n={A['s_nent']})",
         f"{C['s']:.3f} (n={C['s_nent']})", "n/a (T=2)"),
        ("kappa(z)", A["kappa"], C["kappa"], "n/a"),
        ("sigma_obs MD", A["sigma_obs"], C["sigma_obs"], "n/a"),
        ("sigma_perm", A["sigma_perm"], C["sigma_perm"], "n/a"),
        ("sigma_trans", A["sigma_trans"], C["sigma_trans"], "n/a"),
        ("phi", A["phi"], C["phi"], "n/a"),
        ("t_df", f"{A['t_df']:.1f}", f"{C['t_df']:.1f}", "n/a"),
    ]
    for name, a, c, d in rows:
        print(f"{name:<22}{str(a):>20}{str(c):>20}{str(d):>16}")

    print("\nSpec-B sigma_obs floor (daily, guarded), per era:")
    for tag, E in (("A", A), ("C", C), ("D", D)):
        t = E["specB"]["table"]
        print(f"  era {tag}: " + "  ".join(
            f"r~{int(r.rank)}:{r.sigma_obsB:.3f}(n={int(r.n_ent)})"
            for r in t.itertuples()))

    print("\n--- Era C in-sample scorecard (reps=5; h=13 metrics NaN at T=12) ---")
    scorecard("facebook_c", C["params"])
