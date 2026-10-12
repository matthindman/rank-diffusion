#!/usr/bin/env python3
"""Balance accounting: who leaves the top k, who replaces them, and what holds the ladder.

Movement-to-structure agenda, step 1.  Protocol, estimands and predictions:
llm_fitting/MTS_PROTOCOL_STEPS_1_2_2026-10-13.md.  The statistics are model-free
and are computed by the same code on real and planted panels.  Nothing here
fits or simulates a model, and nothing is tuned.

Input everywhere: a level matrix L[P, n] with one row per period (a week, or a
block of weeks averaged by block_means) and one column per endpoint; NaN means
the endpoint has no level in that period.

The accounting identity (exact, any horizon, any process).  Let A be the k
endpoints with the highest level at the first period and A' at the second, and
let ell be the k-th highest level at the second period.  Then

    k * (mean level of A' at the second period - mean level of A at the first)
        =   N                 named change of the occupants A still observed
          + R  (>= 0)         reordering: climbers above ell, fallers below it
          + F                 flow: entrants (no level at the first period) and
                              exits (no level at the second)

Measured against a centre c (the mean level of the top k_c positions), the
left side is the change in the SHAPE of the ladder, and

    shape change  =  -G + R + F,      G = occupant loss relative to the centre.

A stationary shape with no flow has G = R: what the occupants of the top k lose
is what reordering puts back.

R is the ENDPOINT reordering gain over the interval.  It keeps the names that
held the top k at the first period, so it nets out everything that crossed
and crossed back, and it is not additive over sub-intervals: the path
(1, -1) -> (-1, 1) -> (1, -1) has R = 0 overall and R = 4 in two steps.  It is
not the accumulated local time of the gap between ranks k and k+1; only for a
short interval in a diffusion does its expectation approach half of that, and
then its small-step value is k v / (2 S), with v the variance of the changes
near rank k and S the local slope.  Whether k v / (2 S) tracks R at a given
horizon is something to measure and to validate on planted panels, not a
theorem (Codex's review R4, finding 2).
"""
from __future__ import annotations

import numpy as np

Z90 = 1.6448536269514722


# --------------------------------------------------------------------------- #
# Building blocks
# --------------------------------------------------------------------------- #
def block_means(X, block, start=0, min_frac=0.7, n_blocks=None):
    """Means of consecutive blocks of `block` rows; NaN where an endpoint is
    present in fewer than min_frac of the block's rows."""
    nb = (X.shape[0] - start) // block if n_blocks is None else int(n_blocks)
    M = np.full((nb, X.shape[1]), np.nan)
    for b in range(nb):
        seg = X[start + b * block:start + (b + 1) * block]
        cnt = np.isfinite(seg).sum(axis=0)
        M[b] = np.where(cnt >= min_frac * block, np.nansum(seg, axis=0) / np.maximum(cnt, 1), np.nan)
    return M


def perm_rank(R, lo, hi, floors=None):
    """Absence-penalized permanent rank over rows [lo, hi): the mean rank, with
    an absent row (rank 0) counted at that row's floor.  floors: one value per
    row of R (N_t + 1); default R.max() + 1."""
    r = R[lo:hi].astype(float)
    fl = (np.full(r.shape[0], float(R.max() + 1)) if floors is None
          else np.asarray(floors, dtype=float)[lo:hi])
    return np.where(r > 0, r, fl[:, None]).mean(axis=0), (r > 0).mean(axis=0)


def _order(L):
    """Indices of present endpoints, highest level first (stable)."""
    idx = np.flatnonzero(np.isfinite(L))
    return idx[np.argsort(-L[idx], kind="mergesort")]


def ladder(L, n=None):
    v = np.sort(L[np.isfinite(L)])[::-1]
    return v if n is None else v[:n]


def certified(level_row, bound, rank):
    """True if the top `rank` of a block ladder built from tracked endpoints is
    exact: the level at that rank is strictly above the most an untracked
    endpoint could have (bound; -inf when no untracked endpoint can exist)."""
    v = ladder(level_row)
    rank = int(np.ceil(rank))
    return bool(v.size >= rank and v[rank - 1] > bound)


def local_slope(lad, lo, hi):
    """Minus the regression slope of the ladder on log rank over ranks (lo, hi]."""
    r = np.arange(max(1, int(np.floor(lo)) + 1), min(int(np.floor(hi)), lad.size) + 1)
    if r.size < 3:
        return np.nan
    return float(-np.polyfit(np.log(r), lad[r - 1], 1)[0])


# --------------------------------------------------------------------------- #
# The exact accounting between two periods
# --------------------------------------------------------------------------- #
def topk_accounting(L0, L1, ks, k_centre):
    """Exact decomposition of the change in the top-k ladder between two
    periods, for each k in ks, relative to the mean level of the top k_centre
    positions.  Sums are totals over the k positions (divide by k and by the
    horizon for a rate per endpoint per week).

    Returns one dict per k: G (occupant loss), R (reordering gain), F (flow),
    shape (change in k * (top-k mean - centre)), resid (shape + G - R - F,
    zero up to rounding), and the counts of climbers, fallers, entrants, exits.
    """
    o0, o1 = _order(L0), _order(L1)
    if min(o0.size, o1.size) < max(max(ks), k_centre):
        raise ValueError("fewer present endpoints than the largest k")
    dc = float(L1[o1[:k_centre]].mean() - L0[o0[:k_centre]].mean())
    p0, p1 = np.isfinite(L0), np.isfinite(L1)
    in0 = np.zeros(L0.size, dtype=bool)
    in1 = np.zeros(L0.size, dtype=bool)
    out = []
    for k in ks:
        in0[:] = False
        in1[:] = False
        in0[o0[:k]] = True
        in1[o1[:k]] = True
        ell = float(L1[o1[k - 1]])
        named = in0 & p1
        exits = in0 & ~p1
        fall = in0 & p1 & ~in1
        climb = in1 & ~in0 & p0
        enter = in1 & ~p0
        N = float((L1[named] - L0[named]).sum()) - named.sum() * dc
        R = float((L1[climb] - ell).sum() + (ell - L1[fall]).sum())
        F = float((L1[enter] - ell).sum() - (L0[exits] - ell).sum()) - exits.sum() * dc
        shape = k * (float(L1[o1[:k]].mean() - L0[o0[:k]].mean()) - dc)
        out.append(dict(k=int(k), G=float(-N), R=R, F=float(F), shape=float(shape),
                        resid=float(shape - (N + R + F)),
                        n_climb=int(climb.sum()), n_fall=int(fall.sum()),
                        n_enter=int(enter.sum()), n_exit=int(exits.sum()), dc=dc))
    return out


def diffusion_gain(L0, L1, k, k_centre, width=np.sqrt(2.0)):
    """Small-step value of the reordering gain at rank k: k v / (2 S).  The
    endpoints are chosen by their rank at the first period, so v is affected by
    selection on that period's noise: this is a diagnostic of the observed
    positions, not a structural variance by permanent rank.

    v: variance of the changes (about their own mean) of the endpoints ranked in
    (k / width, k * width] at the first period and observed at the second;
    S: local slope of the first period's ladder over the same ranks.  Also
    returns step = sd of the change / S: the approximation needs step << 1.
    """
    o0 = _order(L0)
    lo, hi = k / width, min(k * width, o0.size)
    band = o0[int(np.floor(lo)):int(np.floor(hi))]
    band = band[np.isfinite(L1[band])]
    d = L1[band] - L0[band]
    v = float(d.var()) if d.size > 2 else np.nan
    S = local_slope(L0[o0], lo, hi)
    ok = np.isfinite(v) and np.isfinite(S) and S > 0
    return dict(k=int(k), v=v, S=S, n=int(d.size),
                R_diff=float(k * v / (2 * S)) if ok else np.nan,
                step=float(np.sqrt(v) / S) if ok else np.nan)


def accounting_series(L, h, ks, k_centre, origins=None):
    """topk_accounting and diffusion_gain for every origin t with t + h in
    range, as rates per endpoint per period: g = G/(k h), r = R/(k h),
    f = F/(k h), shape/(k h), r_diff = R_diff/(k h).  Returns arrays
    [n_origins, len(ks)] in a dict, plus the origins."""
    P = L.shape[0]
    origins = np.arange(0, P - h) if origins is None else np.asarray(origins)
    keys = ("g", "r", "f", "shape", "resid", "r_diff", "step", "swap")
    res = {q: np.full((origins.size, len(ks)), np.nan) for q in keys}
    for a, t in enumerate(origins):
        acc = topk_accounting(L[t], L[t + h], ks, k_centre)
        for j, c in enumerate(acc):
            kh = c["k"] * h
            res["g"][a, j], res["r"][a, j], res["f"][a, j] = c["G"] / kh, c["R"] / kh, c["F"] / kh
            res["shape"][a, j], res["resid"][a, j] = c["shape"] / kh, c["resid"] / kh
            res["swap"][a, j] = (c["n_fall"] + c["n_exit"]) / c["k"]
            dg = diffusion_gain(L[t], L[t + h], c["k"], k_centre)
            res["r_diff"][a, j], res["step"][a, j] = dg["R_diff"] / kh, dg["step"]
    res["origins"] = origins
    return res


# --------------------------------------------------------------------------- #
# Drift measured from cohorts fixed on an earlier window
# --------------------------------------------------------------------------- #
def centre_series(M, k_centre):
    """Mean level of the top k_centre positions in each period."""
    return np.array([ladder(M[b], k_centre).mean() if np.isfinite(M[b]).sum() >= k_centre else np.nan
                     for b in range(M.shape[0])])


def cohort_bins(pr, K, pres=None, pres_floor=0.7, min_edge=12):
    """Octave bins of permanent rank, head first: (0, e], (e, 2e], ..., (K/2, K],
    then one bin "outside" for everything else (below the cutoff or absent on
    the selection window).  Returns (labels, assignment array of bin index)."""
    edges = [float(K)]
    while edges[-1] / 2 >= min_edge:
        edges.append(edges[-1] / 2)
    edges = [0.0] + edges[::-1]
    ok = np.isfinite(pr) if pres is None else (pres >= pres_floor)
    which = np.full(pr.size, len(edges) - 1, dtype=int)
    for j in range(len(edges) - 1):
        which[ok & (pr > edges[j]) & (pr <= edges[j + 1])] = j
    labels = [(edges[j], edges[j + 1]) for j in range(len(edges) - 1)] + ["outside"]
    return labels, which


def cohort_drift(M, c, which, n_bins, b_start, b_end, n_boot=400, seed=0, return_draws=False):
    """Mean and mean squared change, between two later periods, of cohorts fixed
    beforehand.  Change = (M[b_end] - c[b_end]) - (M[b_start] - c[b_start]).

    `which` must be built from periods strictly before b_start.

    The estimand is SURVIVOR-CONDITIONAL: the mean over cohort members with a
    level at both periods.  Members with a level at b_start and none at b_end
    are counted as attrition; their changes are unobserved and nothing here
    corrects for selection on survival.  Selection on an earlier period does
    not by itself make the mean free of that period's noise: a component that
    is still correlated with the selection period when the change is measured
    decays over it (review R4, finding 1).

    5-95% endpoint bootstrap per bin.  With return_draws, also the bootstrap
    means [n_boot, n_bins] (NaN for a bin with fewer than five survivors), so
    that a weighted sum over bins can be given a joint interval.
    """
    rng = np.random.default_rng(seed)
    d = (M[b_end] - c[b_end]) - (M[b_start] - c[b_start])
    start = np.isfinite(M[b_start])
    rows = []
    draws = np.full((n_boot, n_bins), np.nan)
    for j in range(n_bins):
        m = (which == j) & start
        x = d[m & np.isfinite(d)]
        row = dict(bin=j, n_start=int(m.sum()), n=int(x.size),
                   attrition=float(1 - x.size / max(m.sum(), 1)))
        if x.size >= 5:
            bs = x[rng.integers(0, x.size, (n_boot, x.size))].mean(axis=1)
            draws[:, j] = bs
            row.update(mean=float(x.mean()), q05=float(np.quantile(bs, 0.05)),
                       q95=float(np.quantile(bs, 0.95)), msq=float((x * x).mean()),
                       var=float(x.var()))
        rows.append(row)
    return (rows, draws) if return_draws else rows


def carrier_regression(M, c, b_pos, b_start, b_end, mask, b_home=None, n_boot=400, seed=0):
    """Predictive associations of the later change with two earlier levels.

    OLS of the change from b_start to b_end (relative to the centre) on the
    level at b_pos and, when b_home is given, the level at the still earlier
    period b_home, both relative to the centre.

    The slopes are descriptive.  Their SIGNS DO NOT IDENTIFY A MECHANISM
    (review R4, finding 1): a level with drift that depends on position only
    gives a negative slope on b_pos and a positive one on b_home once it is
    averaged into blocks (-0.34 and +0.07 for an OU at 0.01 per week in
    52-week blocks), and a random walk plus a slowly decaying component gives
    the same pattern as a personal home.  Placing the predictors before the
    interval removes selection on the interval's own noise and nothing more.
    What a pattern of slopes means is decided against planted rivals at the
    same block lengths, never from this table.
    """
    rng = np.random.default_rng(seed)
    y = (M[b_end] - c[b_end]) - (M[b_start] - c[b_start])
    cols = [M[b_pos] - c[b_pos]] + ([M[b_home] - c[b_home]] if b_home is not None else [])
    ok = mask & np.isfinite(y) & np.all([np.isfinite(v) for v in cols], axis=0)
    Xd = np.column_stack([np.ones(ok.sum())] + [v[ok] for v in cols])
    yy = y[ok]
    if yy.size < 30:
        return dict(n=int(yy.size))
    beta = np.linalg.lstsq(Xd, yy, rcond=None)[0]
    bs = np.empty((n_boot, beta.size))
    for b in range(n_boot):
        i = rng.integers(0, yy.size, yy.size)
        bs[b] = np.linalg.lstsq(Xd[i], yy[i], rcond=None)[0]
    names = ["intercept", "pos"] + (["home"] if b_home is not None else [])
    return dict(n=int(yy.size), **{nm: float(beta[i]) for i, nm in enumerate(names)},
                **{nm + "_q05": float(np.quantile(bs[:, i], 0.05)) for i, nm in enumerate(names)},
                **{nm + "_q95": float(np.quantile(bs[:, i], 0.95)) for i, nm in enumerate(names)})


def predictable_loss(M, c, which, drift_rows, b_start, b_end, ks, draws=None, outside_bin=None):
    """How much of the occupants' loss was predictable from the earlier window?

    For each k, over S_k = the endpoints holding the top k at b_start that have
    a level at b_end and belong to a cohort bin (not `outside_bin`):

        G_pred    minus the sum over S_k of the mean change of each one's bin
        G_cohort  minus the sum over S_k of their actual changes

    Both cover the same endpoints, the survivors that topk_accounting's G also
    covers; exits and occupants from outside the cohort are counted, not
    weighted (review R4, finding 4).  The outside bin is left out because the
    tracked set admits it with later information.

    With `draws` from cohort_drift(return_draws=True), q05 and q95 are the
    quantiles of the weighted sum over the same bootstrap draws: a joint
    interval, conditional on the realized centre, on who occupies the top k
    and on R (finding 5).  Without draws no interval is given.
    """
    mean = np.array([r.get("mean", np.nan) for r in drift_rows])
    d = (M[b_end] - c[b_end]) - (M[b_start] - c[b_start])
    o = _order(M[b_start])
    survive = np.isfinite(M[b_end])
    member = np.ones(which.size, dtype=bool) if outside_bin is None else (which != outside_bin)
    out = []
    for k in ks:
        occ = o[:k]
        S = occ[survive[occ] & member[occ]]
        w = np.bincount(which[S], minlength=mean.size).astype(float)
        use = w > 0
        row = dict(k=int(k), n_cohort_survivors=int(S.size),
                   n_outside=int((survive[occ] & ~member[occ]).sum()),
                   n_exit=int((~survive[occ]).sum()), bin_counts=w.astype(int).tolist(),
                   G_cohort=float(-d[S].sum()), G_pred=None, G_pred_q05=None, G_pred_q95=None)
        if S.size and not np.any(use & ~np.isfinite(mean)):
            row["G_pred"] = float(-(w[use] * mean[use]).sum())
            if draws is not None:
                dist = -(draws[:, use] @ w[use])
                row["G_pred_q05"] = float(np.quantile(dist, 0.05))
                row["G_pred_q95"] = float(np.quantile(dist, 0.95))
        out.append(row)
    return out


def occupancy(which, n_bins, M_row, ks):
    """Where the cohorts sit later: share of each cohort bin's endpoints (with a
    level in M_row) found inside the top k of M_row, for each k."""
    o = _order(M_row)
    rank = np.full(M_row.size, np.inf)
    rank[o] = np.arange(1, o.size + 1)
    present = np.isfinite(M_row)
    return [[float((rank[(which == j) & present] <= k).mean()) if ((which == j) & present).any() else np.nan
             for k in ks] for j in range(n_bins)]
