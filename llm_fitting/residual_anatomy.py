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
      anchor  (added for A7) each identity's h-week changes divided by its
              own robust scale of ONE-week changes, then by the group's
              robust scale of the result.  The "entity" ratio is higher for
              a series that wanders than for one that reverts, even with
              Gaussian shocks, because the scale is estimated from the same
              overlapping changes; this one is not (Gaussian: 1.57 at every
              horizon, a little more from the noise in the one-week scale).
              It keeps in any difference between endpoints in how long-run
              and short-run volatility relate.

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
    d1 = Y[1:] - Y[:-1]
    s1, _ = _mad_scale(d1)
    s1 = np.where(np.isfinite(d1).sum(axis=0) >= min_changes, s1, np.nan)
    rows = []
    for h in horizons:
        if T < 2.5 * h:
            continue
        d = Y[h:] - Y[:-h]
        for g, m in gmask.items():
            dg = d[:, m & base]
            if dg.shape[1] < 20:
                continue
            sa = s1[m & base]
            use = np.isfinite(sa) & (sa > 0)
            za = (dg[:, use] / sa[use]).ravel()
            za = za[np.isfinite(za)]
            if za.size >= 500:
                ca = np.median(za)
                za = (za - ca) / (MAD_TO_SD * np.median(np.abs(za - ca)))
                rows.append(dict(group=g, h=int(h), mode="anchor",
                                 entities=int(use.sum()), **_tail_row(za)))
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
                t=int(t), e=int(e), kind="crossing" if rr[t + 1, e] > 0 else "absence",
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



# --------------------------------------------------------------------------- #
# A7. Follow-ups: relocation by endpoint, direction, volatility over time
# --------------------------------------------------------------------------- #
def cohort_fate(R, K, t0, t1=None, cut_frac=0.5, pres_floor=0.7, near=0.75,
                lookback=13, follow=13, floor=None, train_lo=0):
    """The cohort of `departures`, counted by endpoint instead of by event.

    One relocation to the boundary produces many exit events, so event counts
    cannot say whether endpoints drift there or step there.  Per endpoint:

      shift            log(permanent rank in the scoring window / permanent
                       rank in the train window); quantiles over endpoints
                       present in at least pres_floor of the scoring weeks
      share_relocated  scoring-window permanent rank beyond near*K, and
                       present (so not a death)
      share_gone       present in fewer than pres_floor of the scoring weeks
      share_time_outside   cohort-weeks of the scoring window not in the top-K
      share_ever_exit, exits_per_exiting
      first_<class>    class of each endpoint's FIRST exit, as shares
    """
    T = R.shape[0] if t1 is None else int(t1)
    fl = float(R.max() + 1) if floor is None else float(floor)
    coh = cohort(R, train_lo, t0, cut_frac * K, pres_floor, fl)
    Rc = R[:, coh]
    pr0, _ = perm_rank(Rc, train_lo, t0, fl)
    pr1, pres1 = perm_rank(Rc, t0, T, fl)
    here = pres1 >= pres_floor
    shift = np.log(pr1[here] / pr0[here])
    rr = Rc[t0:T].astype(float)
    events, _ = departures(R, K, t0, t1=t1, cut_frac=cut_frac, pres_floor=pres_floor,
                           lookback=lookback, near=near, follow=follow, floor=floor,
                           train_lo=train_lo)
    first = [e for e in events if e["nth"] == 1]
    n = int(coh.sum())
    out = dict(n=n, n_present=int(here.sum()),
               share_relocated=float(np.mean((pr1 > near * K) & here)),
               share_relocated_out=float(np.mean((pr1 > K) & here)),
               share_gone=float(np.mean(~here)),
               share_time_outside=float(1.0 - ((rr > 0) & (rr <= K)).mean()),
               share_ever_exit=len(first) / max(n, 1),
               exits_per_exiting=len(events) / max(len(first), 1))
    for q in (50, 75, 90, 95, 99):
        out[f"shift_q{q}"] = float(np.quantile(shift, q / 100)) if shift.size else np.nan
    for c in ("slide", "excursion", "lasting", "censored"):
        out[f"first_{c}"] = (float(np.mean([e["cls"] == c for e in first]))
                             if first else np.nan)
    return out


def _block_means(Y, block, min_frac=0.5):
    nb = Y.shape[0] // block
    M = np.full((nb, Y.shape[1]), np.nan)
    for b in range(nb):
        seg = Y[b * block:(b + 1) * block]
        cnt = np.isfinite(seg).sum(axis=0)
        M[b] = np.where(cnt >= min_frac * block,
                        np.nansum(seg, axis=0) / np.maximum(cnt, 1), np.nan)
    return M


def momentum(R, X, K, block=26, groups=GROUPS, pres_floor=0.7, floor=None, gaps=None):
    """Does the direction an endpoint moved predict how it moves later?

    Levels net of the common factor are averaged over consecutive blocks of
    `block` weeks; c_b is the change between block b and block b+1, with the
    rank group's mean change removed block by block.  Slope of a later change
    on an earlier one, pooled over endpoints and blocks:

      adjacent  c_{b+1} on c_b.  They share block b+1.  Random walk: +0.25;
                white noise around a fixed level: -0.5.
      skip      c_{b+2} on c_b.  No block in common.  Zero for a random walk
                and for white noise around a fixed level; negative for a
                level that reverts on the scale of the blocks; positive when
                direction persists (endpoint-specific trends).

    gaps: other spacings, in blocks; rows are then named "gap<g>".  An
    endpoint-specific annual cycle gives a positive slope where the two
    changes are a whole number of years apart and a negative one at half
    years, so 26-week blocks with "skip" (one year apart) cannot tell a cycle
    from a trend.  Whole-year blocks average the cycle out.
    """
    T = R.shape[0]
    pr, pres = perm_rank(R, 0, T, floor)
    base = (pr <= K) & (pres >= pres_floor)
    M = _block_means(defactor(X, base), block)
    C = M[1:] - M[:-1]
    rows = []
    for g, m in _groups(pr, K, groups).items():
        c = C[:, m & base]
        cnt = np.isfinite(c).sum(axis=1, keepdims=True)
        c = c - np.nansum(c, axis=1, keepdims=True) / np.maximum(cnt, 1)
        pairs = ((("adjacent", 1), ("skip", 2)) if gaps is None
                 else tuple((f"gap{g}", int(g)) for g in gaps))
        for name, gap in pairs:
            if c.shape[0] <= gap:
                continue
            a, b = c[:-gap].ravel(), c[gap:].ravel()
            ok = np.isfinite(a) & np.isfinite(b)
            if ok.sum() < 200:
                continue
            a, b = a[ok], b[ok]
            rows.append(dict(group=g, block=int(block), pair=name, n=int(ok.sum()),
                             slope=float((a * b).sum() / (a * a).sum()),
                             corr=float(np.corrcoef(a, b)[0, 1])))
    return rows


def volatility_persistence(R, X, K, groups=GROUPS, pres_floor=0.7, min_changes=12,
                           floor=None):
    """Is an endpoint's volatility a fixed property or does it change over time?

    Log robust scale of each endpoint's one-week changes, net of the common
    factor, on four subsets of weeks.  Correlation across endpoints:

      corr_interleaved  odd weeks against even weeks: both halves see every
                        period of the panel
      corr_halves       first half against second half: only what lasts
                        across the panel is shared

    Each half has the same number of changes, so sampling noise lowers both
    alike.  ratio = corr_halves / corr_interleaved is 1 for a fixed endpoint
    amplitude and below 1 when an endpoint's volatility moves over time.
    """
    T = R.shape[0]
    pr, pres = perm_rank(R, 0, T, floor)
    base = (pr <= K) & (pres >= pres_floor)
    Y = defactor(X, base)
    d1 = Y[1:] - Y[:-1]
    idx = np.arange(d1.shape[0])
    parts = dict(odd=idx % 2 == 1, even=idx % 2 == 0,
                 first=idx < d1.shape[0] // 2, second=idx >= d1.shape[0] // 2)
    ls = {}
    for name, sel in parts.items():
        seg = d1[sel]
        sc, _ = _mad_scale(seg)
        good = (np.isfinite(seg).sum(axis=0) >= min_changes) & (sc > 0)
        with np.errstate(divide="ignore", invalid="ignore"):
            ls[name] = np.where(good, np.log(sc), np.nan)

    def corr(a, b):
        ok = np.isfinite(a) & np.isfinite(b)
        return (float(np.corrcoef(a[ok], b[ok])[0, 1]) if ok.sum() >= 50 else np.nan,
                int(ok.sum()))

    rows = []
    for g, m in _groups(pr, K, groups).items():
        cols = m & base
        ci, n1 = corr(ls["odd"][cols], ls["even"][cols])
        ch, n2 = corr(ls["first"][cols], ls["second"][cols])
        rows.append(dict(group=g, n=min(n1, n2), corr_interleaved=ci, corr_halves=ch,
                         ratio=ch / ci if ci and ci > 0 else np.nan))
    return rows


def amplitude_coherence(R, X, K, h_long=13, groups=GROUPS, pres_floor=0.7,
                        min_changes=12, floor=None):
    """Is an endpoint's long-run volatility the same amplitude as its
    short-run volatility?

    Log robust scale of each endpoint's one-week changes (s1) and of its
    h_long-week changes (sL), each on the first and on the second half of
    the panel.  With a and b the two halves,

        coherence = mean(corr(s1_a, sL_b), corr(s1_b, sL_a))
                    / sqrt(corr(s1_a, s1_b) * corr(sL_a, sL_b))

    which is the correlation across endpoints between the two amplitudes
    with the sampling noise of each divided out.  One amplitude scaling
    everything gives 1; separate amplitudes for the lasting and passing
    parts give less.  Compare with the simulation, which carries whatever
    bias the short halves add.
    """
    T = R.shape[0]
    pr, pres = perm_rank(R, 0, T, floor)
    base = (pr <= K) & (pres >= pres_floor)
    Y = defactor(X, base)
    half = T // 2

    def logscale(seg, h):
        d = seg[h:] - seg[:-h]
        sc, _ = _mad_scale(d)
        good = (np.isfinite(d).sum(axis=0) >= min_changes) & (sc > 0)
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.where(good, np.log(sc), np.nan)

    s1a, s1b = logscale(Y[:half], 1), logscale(Y[half:], 1)
    sLa, sLb = logscale(Y[:half], h_long), logscale(Y[half:], h_long)

    def corr(a, b):
        ok = np.isfinite(a) & np.isfinite(b)
        return float(np.corrcoef(a[ok], b[ok])[0, 1]) if ok.sum() >= 50 else np.nan

    rows = []
    for g, m in _groups(pr, K, groups).items():
        c = m & base
        r1, rL = corr(s1a[c], s1b[c]), corr(sLa[c], sLb[c])
        cross = 0.5 * (corr(s1a[c], sLb[c]) + corr(s1b[c], sLa[c]))
        ok = np.isfinite(r1) and np.isfinite(rL) and r1 > 0 and rL > 0
        rows.append(dict(group=g, h=int(h_long), rel_short=r1, rel_long=rL, cross=cross,
                         coherence=float(cross / np.sqrt(r1 * rL)) if ok else np.nan))
    return rows
