"""R4 adversarial checks. Synthetic inputs only; no empirical panel loader."""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from balance_accounting import (block_means, carrier_regression, centre_series,
    cohort_bins, cohort_drift, perm_rank, predictable_loss, topk_accounting)
import mts_step1


def ranks(x):
    out = np.zeros(x.shape, dtype=int)
    for t in range(len(x)):
        order = np.flatnonzero(np.isfinite(x[t]))
        order = order[np.argsort(-x[t, order], kind="stable")]
        out[t, order] = np.arange(1, len(order) + 1)
    return out


def frame(x):
    r = ranks(x)
    t, i = np.nonzero(np.isfinite(x))
    d = pd.DataFrame(dict(entity_id=i, period=t, X=x[t, i], rank=r[t, i]))
    d["N"] = d.groupby("period")["entity_id"].transform("size")
    return d


def main():
    results = {}
    path = np.array([[1., -1.], [-1., 1.], [1., -1.]])
    gain = lambda a, b: topk_accounting(a, b, [1], 2)[0]["R"]
    results["reordering_not_additive"] = dict(
        whole=gain(path[0], path[2]),
        subdivided=gain(path[0], path[1]) + gain(path[1], path[2]))

    # Exact covariance of adjacent 52-week means of a unit stationary OU
    # sampled weekly: Cov(X_s, X_t) = exp(-kappa * abs(s-t)). No name effects.
    kappa, block = .01, 52
    t = np.arange(block)
    cov = np.array([np.exp(-kappa * np.abs(t[:, None] - (t[None, :] + j * block))).mean()
                    for j in range(4)])
    beta = np.linalg.solve([[cov[0], cov[1]], [cov[1], cov[0]]],
                           [cov[2] - cov[1], cov[3] - cov[2]])
    results["position_only_ou_block_population"] = dict(
        kappa=kappa, block=block, cov=cov.tolist(), pos=beta[0], home=beta[1])

    # Actual adjacent block configuration of the runner. A random walk lasting
    # level plus AR noise. No restoring force acts on the lasting level.
    rng = np.random.default_rng(1000)
    n, T, K, phi = 6000, 208, 1000, .98
    base = rng.normal(0, 2., n)
    e = rng.normal(0, .6, n)
    latent = np.zeros(n)
    x = np.empty((T, n))
    for t in range(T):
        latent += rng.normal(0, .005, n)
        e = phi * e + .6 * np.sqrt(1 - phi * phi) * rng.normal(size=n)
        x[t] = base + latent + e
    m = block_means(x, 52)
    c = centre_series(m, K)
    pr, pres = perm_rank(ranks(x), 0, 52)
    labels, which = cohort_bins(pr, K, pres)
    results["rw_plus_ar_adjacent_blocks"] = carrier_regression(
        m, c, 1, 2, 3, which < len(labels) - 1, b_home=0, seed=1000)
    dr = cohort_drift(m, np.zeros(4), which, len(labels), 1, 2, seed=1000)
    inside = [r for r in dr[:-1] if "mean" in r]
    results["rw_plus_ar_selected_raw_drift"] = dict(
        pooled_mean=sum(r["n"] * r["mean"] for r in inside) / sum(r["n"] for r in inside),
        bins=inside)

    # The precise sum of independent standard normal variables has SD sqrt(2).
    z = 1.6448536269514722
    bracket = predictable_loss(np.array([[2., 1.]]), np.zeros(1), np.array([0, 1]),
                               [dict(mean=0., q05=-z, q95=z)] * 2, 0, [2])[0]
    results["marginal_interval_sum"] = dict(reported=[bracket["G_pred_lo"], bracket["G_pred_hi"]],
                                           correct_normal_90=[-z * np.sqrt(2), z * np.sqrt(2)])

    # Fixed cohort; all negative changes disappear. The statistic estimates
    # survivor change, not the original cohort's unconditional mean (zero).
    m = np.array([np.zeros(100), np.zeros(100), np.r_[np.ones(50), np.full(50, np.nan)]])
    results["attrition_selects_sign"] = cohort_drift(m, np.zeros(3), np.zeros(100, dtype=int),
                                                    1, 1, 2, seed=1000)[0]
    # Same issue in the accounting share: count only one surviving occupant
    # in G, but multiply its survivor mean by two initial occupants in G_pred.
    m = np.array([[10., 9., 8., 7.], [9., np.nan, 7., 6.]])
    c = centre_series(m, 3)
    dr = cohort_drift(np.tile(m, (1, 10)), c, np.zeros(40, dtype=int), 1, 0, 1, seed=1000)
    # With identical -1 changes among survivors the bin mean is exact; use
    # its value to avoid minimum sample-size effects in the four-name example.
    row = dict(mean=float(-1 - (c[1] - c[0])), q05=float(-1 - (c[1] - c[0])),
               q95=float(-1 - (c[1] - c[0])))
    acc = topk_accounting(m[0], m[1], [2], 3)[0]
    pred = predictable_loss(m, c, np.zeros(4, dtype=int), [row], 0, [2])[0]
    results["attrition_weight_mismatch"] = dict(G=acc["G"], G_pred=pred["G_pred"],
                                               n_exit=acc["n_exit"])

    # A steady rank-49 endpoint is never tracked with K=12, but is the
    # largest block mean. Each of 480 tracked endpoints spikes one week in ten.
    K, block, T = 12, 10, 30
    x = np.zeros((T, 481))
    for t in range(T):
        x[t, (t % 10) * 48:(t % 10 + 1) * 48] = 10 + np.arange(48) * .001
    x[:, -1] = 9
    r = mts_step1.analyse(frame(x), K, block, horizons=(), boundaries=(1.,), boot=20)
    bounds = r["blocks"]["untracked_bound"]
    at2k = r["blocks"]["level_at_2K"]
    results["invalid_tracking_still_scored"] = dict(
        tracked=r["n_tracked"], true_block_top=float(block_means(x, block)[0].max()),
        untracked_bound=bounds, level_at_2K=at2k,
        returned_pairs=len(r["boundaries"]["1.0"]["blocks"]["pairs"]),
        margin_positive=bool(np.all(np.array(at2k) > np.array(bounds))))

    print(json.dumps(results, indent=2, default=float))


if __name__ == "__main__":
    main()
