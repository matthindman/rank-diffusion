#!/usr/bin/env python3
"""Residual anatomy: model-free statistics of where large moves live.

Work plan of 2026-10-12, Phase 1, measurements A1 and A2. The statistics
answer two questions about a ranked panel, and are computed by the same
code on real and simulated panels:

  A1  Where are the fat tails: in changes that pass, or in changes that
      last?  tails_by_horizon(), retention().
  A2  What are the departures of established endpoints: slow drift to the
      boundary, a short excursion, or a lasting drop?  departures(),
      departure_summary(), long_dispersion().

Input format (the only one): two identity matrices,

    R[T, n]  weekly rank within the universe, 0 = absent that week
    X[T, n]  log activity, NaN = absent that week

Builders:
    from_long(df)   any panel in the registered long schema (columns
                    entity_id, period, rank, X), normally the output of
                    minimal_rankdiff.restrict_universe
    from_sim(sim)   minimal_rankdiff.simulate(..., track=N, track_ids=True):
                    tracked slots are split into identities, so a slot that
                    dies and is reborn becomes two columns

Conventions shared with exit_audit.py (the P7 estimand of PREREG_2026-07-16):
permanent rank is the mean weekly rank over a window with absent weeks
counted at the floor; a cohort is defined on a train window only; an event
is an exit from the top-K at t+1 from an in-K week t.

Nothing here fits or simulates a model, and nothing is tuned.
"""
from __future__ import annotations

import numpy as np

MAD_TO_SD = 1.4826
GROUPS = (("head", 0.0, 0.1), ("middle", 0.1, 0.5), ("shell", 0.5, 1.0))
HORIZONS = (1, 2, 4, 8, 13, 26, 52)
RETENTION_LAGS = (1, 2, 4, 8, 13, 26)


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #
def from_long(df):
    """(R, X, ids) from a long panel with columns entity_id, period, rank, X."""
    T = int(df["period"].max()) + 1
    r = df.pivot(index="period", columns="entity_id", values="rank").reindex(range(T))
    x = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    R = np.nan_to_num(r.to_numpy(dtype=float), nan=0.0).astype(np.int32)
    X = x.to_numpy(dtype=float)
    X[R <= 0] = np.nan
    return R, X, np.asarray(r.columns)


def from_sim(sim):
    """(R, X) from a simulation run with track_ids=True: one column per
    identity, present during its lifetime and absent outside it."""
    tr, tv, ti = (np.asarray(sim[k]) for k in ("tranks", "tvals", "tids"))
    T, n = tr.shape
    rcols, xcols = [], []
    for j in range(n):
        ids = ti[:, j]
        cuts = np.flatnonzero(ids[1:] != ids[:-1]) + 1
        for a, b in zip(np.r_[0, cuts], np.r_[cuts, T]):
            r = np.zeros(T, dtype=np.int32)
            x = np.full(T, np.nan)
            r[a:b], x[a:b] = tr[a:b, j], tv[a:b, j]
            rcols.append(r)
            xcols.append(x)
    R, X = np.stack(rcols, axis=1), np.stack(xcols, axis=1)
    X[R <= 0] = np.nan
    return R, X


# --------------------------------------------------------------------------- #
# Shared pieces
# --------------------------------------------------------------------------- #
def perm_rank(R, lo=0, hi=None, floor=None):
    """Absence-penalized permanent rank over weeks [lo, hi), and presence."""
    r = R[lo:hi].astype(float)
    floor = float(R.max() + 1) if floor is None else float(floor)
    pen = np.where(r > 0, r, floor)
    return pen.mean(axis=0), (r > 0).mean(axis=0)


def cohort(R, lo, hi, cut, pres_floor=0.7, floor=None):
    pr, pres = perm_rank(R, lo, hi, floor)
    return (pr <= cut) & (pres >= pres_floor)


def defactor(X, member=None):
    """Remove the common weekly factor: the mean change among members present
    in both weeks.  Returns Y with Y[t] - Y[t-1] = change net of the factor."""
    use = X if member is None else X[:, member]
    d = np.diff(use, axis=0)
    cnt = np.isfinite(d).sum(axis=1)
    f = np.where(cnt > 0, np.nansum(d, axis=1) / np.maximum(cnt, 1), 0.0)
    return X - np.r_[0.0, np.cumsum(f)][:, None]


def _groups(pr, K, groups=GROUPS):
    return {name: (pr > lo * K) & (pr <= hi * K) for name, lo, hi in groups}


def _mad_scale(a, axis=0):
    with np.errstate(invalid="ignore", all="ignore"):
        med = np.nanmedian(a, axis=axis, keepdims=True)
        return MAD_TO_SD * np.nanmedian(np.abs(a - med), axis=axis), np.squeeze(med, axis)


def _tail_row(z):
    """Shape statistics of standardized changes z (1-D, finite)."""
    az = np.abs(z)
    q50, q90, q99 = np.quantile(az, [0.5, 0.9, 0.99])
    up, dn = z[z > 0], -z[z < 0]
    return dict(n=int(z.size), q50=float(q50), q90=float(q90), q99=float(q99),
                r9050=float(q90 / q50), r9990=float(q99 / q90),
                p3=float(np.mean(az > 3.0)),
                q99_up=float(np.quantile(up, 0.99)) if up.size > 100 else np.nan,
                q99_down=float(np.quantile(dn, 0.99)) if dn.size > 100 else np.nan)


# --------------------------------------------------------------------------- #
# A1a. Tail shape by horizon
# --------------------------------------------------------------------------- #
def tails_by_horizon(R, X, K, horizons=HORIZONS, groups=GROUPS, pres_floor=0.7,
                     min_changes=12, floor=None):
    """Shape of h-week changes, by permanent-rank group and horizon.

    Population: identities whose full-window permanent rank is <= K with
    presence >= pres_floor.  Changes are net of the common factor and use
    every start week (overlapping).  Two standardizations:

      entity  each identity's changes divided by its own robust scale at
              that horizon (needs >= min_changes changes).  A persistent
              endpoint amplitude cancels, so what is left is the shape of
              that endpoint's own shocks.
      group   changes divided by the robust scale of the whole rank group.
              The spread of amplitudes across endpoints stays in.

    Reported per (group, horizon, standardization): quantiles of |z|, the
    ratios q90/q50 and q99/q90 (Gaussian: 2.44 and 1.57), the share beyond
    3 (Gaussian: 0.0027), and the 99th percentile of up and down moves.
    A horizon is skipped when the panel is shorter than 2.5 h.
    """
    T = R.shape[0]
    pr, pres = perm_rank(R, 0, T, floor)
    base = (pr <= K) & (pres >= pres_floor)
    Y = defactor(X, base)
    gmask = _groups(pr, K, groups)
    rows = []
    for h in horizons:
        if T < 2.5 * h:
            continue
        d = Y[h:] - Y[:-h]
        for g, m in gmask.items():
            dg = d[:, m & base]
            if dg.shape[1] < 20:
                continue
            ok = np.isfinite(dg).sum(axis=0) >= min_changes
            s_i, med_i = _mad_scale(dg[:, ok])
            good = s_i > 0
            ze = ((dg[:, ok][:, good] - med_i[good]) / s_i[good]).ravel()
            ze = ze[np.isfinite(ze)]
            flat = dg[np.isfinite(dg)]
            s_g = MAD_TO_SD * np.median(np.abs(flat - np.median(flat)))
            zg = (flat - np.median(flat)) / s_g
            for mode, z in (("entity", ze), ("group", zg)):
                if z.size >= 500:
                    rows.append(dict(group=g, h=int(h), mode=mode,
                                     entities=int(ok.sum() if mode == "entity"
                                                  else dg.shape[1]),
                                     **_tail_row(z)))
    return rows


# --------------------------------------------------------------------------- #
# A1b. How much of a move is kept, by size and sign
# --------------------------------------------------------------------------- #
def retention(R, X, K, lags=RETENTION_LAGS, big=2.0, small=1.0, groups=GROUPS,
              pres_floor=0.7, min_changes=12, floor=None):
    """Fraction of a one-week move still in place k weeks later.

    A move is the factor-removed change d_t = Y_t - Y_{t-1}, standardized by
    the identity's own robust scale of one-week changes.  Classes: big_up
    (z > big), big_down (z < -big), small_up (0 < z < small), small_down
    (-small < z < 0).  Retention at lag k is

        sum over events of (Y_{t+k} - Y_{t-1})  /  sum of (Y_t - Y_{t-1})

    over the events whose week t+k is observed.  Benchmarks: 1 for a random
    walk; 0.5 for white noise around a fixed level (half of a change is the
    departure of last week's noise, which does not come back).
    """
    T = R.shape[0]
    pr, pres = perm_rank(R, 0, T, floor)
    base = (pr <= K) & (pres >= pres_floor)
    Y = defactor(X, base)
    d1 = Y[1:] - Y[:-1]                       # d1[t-1] = Y_t - Y_{t-1}
    ok = np.isfinite(d1).sum(axis=0) >= min_changes
    s_i, med_i = _mad_scale(d1)
    with np.errstate(invalid="ignore", divide="ignore"):
        z = (d1 - med_i) / s_i
    classes = {"big_up": z > big, "big_down": z < -big,
               "small_up": (z > 0) & (z < small), "small_down": (z < 0) & (z > -small)}
    rows = []
    for g, m in _groups(pr, K, groups).items():
        cols = m & base & ok & (s_i > 0)
        if cols.sum() < 20:
            continue
        for cname, cmask in classes.items():
            ev = cmask[:, cols] & np.isfinite(d1[:, cols])
            for k in lags:
                if T - 1 - k < 1:
                    continue
                # event at index i means weeks t-1 = i, t = i+1; later week i+1+k
                e = ev[: T - 1 - k]
                later = Y[1 + k:, cols] - Y[: T - 1 - k, cols]
                first = d1[: T - 1 - k, cols]
                use = e & np.isfinite(later)
                n = int(use.sum())
                if n < 50:
                    continue
                rows.append(dict(group=g, cls=cname, k=int(k), n=n,
                                 retention=float(later[use].sum() / first[use].sum())))
    return rows


# --------------------------------------------------------------------------- #
# A2. Departures of established endpoints
# --------------------------------------------------------------------------- #
def departures(R, K, t0, t1=None, cut_frac=0.5, pres_floor=0.7, lookback=13,
               near=0.75, follow=13, floor=None, train_lo=0):
    """Every exit from the top-K by the established cohort, with its anatomy.

    Cohort: permanent rank over the train window [train_lo, t0) <= cut_frac*K
    and presence there >= pres_floor (the P7 cohort for cut_frac = 0.5).
    Event: in the top-K at week t, not in it at t+1, for t in [t0, t1-2].
    Returns (events, exposure) where exposure is the number of in-K
    cohort-weeks at risk and events is a list of dicts:

      kind      'crossing' (observed below K next week) or 'absence'
      margin    (K - rank_t) / K: how far inside the cut the week before
      pre_med   median rank over the `lookback` weeks up to t, over K
                (absent weeks at the floor)
      slid      pre_med > near: the endpoint had already slid to the
                boundary zone before it left
      back      weeks from t until it is next inside the top-K (np.inf if
                never within the panel)
      followed  weeks of panel after week t
      nth       1 for the identity's first exit in the scoring window, 2...
      cls       one of
                  'slide'      slid, whatever happens next
                  'excursion'  not slid, back within `follow` weeks
                  'lasting'    not slid, not back within `follow` weeks,
                               with at least `follow` weeks of panel left
                  'censored'   not slid, not back, panel ends too soon
    """
    T = R.shape[0] if t1 is None else int(t1)
    floor = float(R.max() + 1) if floor is None else float(floor)
    coh = cohort(R, train_lo, t0, cut_frac * K, pres_floor, floor)
    rr = R[:T, coh].astype(float)
    inK = (rr > 0) & (rr <= K)
    pen = np.where(rr > 0, rr, floor)
    events, counts = [], np.zeros(rr.shape[1], dtype=int)
    for t in range(t0, T - 1):
        leave = np.flatnonzero(inK[t] & ~inK[t + 1])
        for e in leave:
            counts[e] += 1
            later = np.flatnonzero(inK[t + 2:, e])
            back = float(later[0] + 2) if later.size else np.inf   # weeks after t
            followed = T - 1 - t                                    # weeks after t
            pre_med = float(np.median(pen[max(train_lo, t - lookback + 1): t + 1, e]) / K)
            slid = pre_med > near
            if slid:
                cls = "slide"
            elif back <= follow:
                cls = "excursion"
            elif followed >= follow:
                cls = "lasting"
            else:
                cls = "censored"
            events.append(dict(
                t=int(t), kind="crossing" if rr[t + 1, e] > 0 else "absence",
                margin=float((K - rr[t, e]) / K), pre_med=pre_med, slid=bool(slid),
                back=back, followed=int(followed), nth=int(counts[e]), cls=cls))
    exposure = int(inK[t0: T - 1].sum())
    return events, exposure


def departure_summary(events, exposure):
    """Rates per cohort-week at risk, in total and by class, kind and order."""
    out = dict(exposure=int(exposure), n=len(events),
               rate=len(events) / max(exposure, 1))
    for key, values in (("cls", ("slide", "excursion", "lasting", "censored")),
                        ("kind", ("crossing", "absence"))):
        for v in values:
            out[f"rate_{v}"] = sum(e[key] == v for e in events) / max(exposure, 1)
    out["rate_first"] = sum(e["nth"] == 1 for e in events) / max(exposure, 1)
    out["rate_repeat"] = out["rate"] - out["rate_first"]
    if events:
        out["median_margin"] = float(np.median([e["margin"] for e in events]))
        back = np.array([e["back"] for e in events])
        fol = np.array([e["followed"] for e in events])
        for w in (4, 13, 26):
            elig = fol >= w
            out[f"back_within_{w}"] = (float(np.mean(back[elig] <= w))
                                       if elig.any() else np.nan)
    return out


def long_dispersion(R, X, K, horizons=(13, 26, 52, 78, 104), groups=GROUPS,
                    pres_floor=0.7, floor=None):
    """Spread of h-week changes at long horizons, by rank group: the variance
    and a robust version (squared MAD scale), with entity-pooled changes net
    of the common factor.  A lasting layer that keeps wandering shows a
    spread still growing where an OU level has levelled off."""
    T = R.shape[0]
    pr, pres = perm_rank(R, 0, T, floor)
    base = (pr <= K) & (pres >= pres_floor)
    Y = defactor(X, base)
    rows = []
    for h in horizons:
        if T < 2.5 * h:
            continue
        d = Y[h:] - Y[:-h]
        for g, m in _groups(pr, K, groups).items():
            flat = d[:, m & base]
            flat = flat[np.isfinite(flat)]
            if flat.size < 500:
                continue
            mad = MAD_TO_SD * np.median(np.abs(flat - np.median(flat)))
            rows.append(dict(group=g, h=int(h), n=int(flat.size),
                             var=float(np.var(flat)), robust_var=float(mad ** 2)))
    return rows
