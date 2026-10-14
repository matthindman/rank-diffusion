"""Forecasts of the 2023 block of the Reddit ladders, from development data only (2018-12..2022-12).
Written and committed before any contact with the sealed 2023-2024 archive.  See the draft protocol
llm_fitting/BRIDGE_FORECAST_PROTOCOL_DRAFT_2026-10-14.md.

Two forecasters, both fixed here:
  A. forward simulation (mts_forward): the 2021->2022 transition applied to the 2022 block; the
     2020->2021 transition as a sensitivity.  Target: change of shape of the full ladder.
  B. rate law (mts_balance_law): slides of the 2021 cohort over 2021->2022, lasting variance from
     2020-2022, slope of the 2022 ladder.  Target: rate of change of shape of the persisting set;
     plus the 2021->2022 entry term for the full ladder.
Baselines, also fixed here: no change; repeat the 2021->2022 change.

    PYTHONPATH=. python llm_fitting/runs/2026-10-14_bridge_forecast/make_forecast.py
"""
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, "llm_fitting")
import balance_accounting as ba  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_forward as fw  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path(__file__).resolve().parent
out = {}
for panel in ("reddit_submissions_long", "reddit_comments_ext"):
    cfg = m1.PANELS[panel]
    K, block = cfg["K"], cfg["block"]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    M, n_cert = fw.block_ladders(df, K, block)
    ks = fw.cut_positions(K)
    W = 2 * K
    obs = {b: fw.observed(M, b, K, ks, W) for b in range(4)}
    trs = {a: fw.transition(M, a, K, W) for a in (1, 2)}
    A = {}
    for a in (2, 1):
        f = fw.forecast(M, 3, [trs[a]], K, ks, n_sim=200, mode="empirical", seed=2023)
        A[f"transition_{2019 + a}_{2020 + a}"] = dict(
            change={k: f["shape"][k] - obs[3]["shape"][k] for k in ks}, sd=f["shape_sd"], slope_2023=f["slope"])
    # rate law: slides of the cohort fixed on 2021 (block 2) over 2021->2022, variance from blocks 1-3
    st = bl.strata(K)
    R_ = m1.matrices(df, K)
    X, R, floors, outside, ids = R_
    T = X.shape[0]
    start = T - (T // block) * block
    pr, pres = ba.perm_rank(R, start + 2 * block, start + 3 * block, floors)
    which2 = bl._assign(pr, pres, st)
    pr0, pres0 = ba.perm_rank(R, start, start + block, floors)
    which0 = bl._assign(pr0, pres0, st)
    rng = np.random.default_rng(2023)
    var_rows, var_bs = bl.lasting_variance(M, which0, st, (1, 2, 3), block, rng, 300)
    vn = [v["n"] for v in var_rows]
    cuts = bl.cutoffs(K)
    t23 = bl.pair_terms(M, which2, st, 2, 3, K, cuts, block, rng, 300)   # slides over 2021->2022, cohort 2021
    lad22 = np.sort(M[3][np.isfinite(M[3])])[::-1]
    B = {}
    for (j, k), r in zip(cuts, t23["rows"]):
        if k >= K:
            continue
        s2 = bl._at_boundary([v["sigma2"] for v in var_rows], vn, j)
        s2b = np.array([bl._at_boundary(var_bs[:, t], vn, j) for t in range(300)])
        S = bl._slope(lad22, k)
        pred = s2 / (2 * S) - r["sbar"]
        band = s2b / (2 * S) - r["sbar_bs"]
        B[k] = dict(rate_persisting=float(pred), q05=float(np.nanquantile(band, 0.05)), q95=float(np.nanquantile(band, 0.95)),
                    entry_2021_2022=float(r["entry"]), rate_full=float(pred + r["entry"]),
                    change_full_over_52_weeks=float(52 * (pred + r["entry"])), slope_2022=float(S))
    out[panel] = dict(K=K, cutoffs=ks, shape_2022={k: obs[3]["shape"][k] for k in ks}, slope_2022=obs[3]["slope"],
                      forward_simulation=A, rate_law=B,
                      baselines=dict(no_change={k: 0.0 for k in ks},
                                     repeat_last={k: obs[3]["shape"][k] - obs[2]["shape"][k] for k in ks}))
(OUT / "forecast.json").write_text(json.dumps(out, indent=1, default=float))
lines = []
for panel, o in out.items():
    ks = o["cutoffs"]
    A = o["forward_simulation"]["transition_2021_2022"]
    A1 = o["forward_simulation"]["transition_2020_2021"]
    lines += [f"\n## {panel} (K = {o['K']}): change of shape from the 2022 block to the 2023 block, log units", "",
              "| cut-off | shape in 2022 | forward simulation (sd) | same, 2020→2021 transition | rate law, full ladder over 52 weeks (persisting-set 5–95%, per week) | repeat last change | no change |",
              "|---|---|---|---|---|---|---|"]
    for k in ks:
        b = o["rate_law"][k]
        lines.append(f"| {k} | {o['shape_2022'][k]:+.3f} | {A['change'][k]:+.3f} ({A['sd'][k]:.3f}) | {A1['change'][k]:+.3f} | "
                     f"{b['change_full_over_52_weeks']:+.3f} ({b['q05']:+.5f} to {b['q95']:+.5f}) | {o['baselines']['repeat_last'][k]:+.3f} | 0 |")
    lines += ["", "Local slope in 2023, forward simulation: " + ", ".join(f"{k}: {A['slope_2023'][k]:.3f} (2022: {o['slope_2022'][k]:.3f})" for k in ks)]
(OUT / "forecast.md").write_text("\n".join(lines) + "\n")
print("\n".join(lines))
