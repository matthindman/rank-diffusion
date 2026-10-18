"""The moving-home (rank-drift) model of rank dynamics (dossier: runs/2026-10-18_rdm/DOSSIER.md;
MODEL_STATUS 2z-dd).  A new, opt-in model module: it imports the frozen estimator and the frozen gate's
scoring code and changes neither.

Log activity of endpoint i in week t:

    X_it = L_it + m_it + xi_it + eps_it

    eps   measurement noise, sigma_obs(z)                         (model A's estimate, unchanged)
    xi    fast swing, AR(1) phi(z), sigma_trans(z), Student-t      (model A's estimate, unchanged)
    m     medium swing, OU toward 0 at a pooled rate kappa_m, stationary variance V_m by band
    L     lasting level, a random walk that holds the ladder through DRIFT BY CURRENT RANK g(r)
          (hybrid-Atlas), with size-law volatility sigma_L(L) = sigma0 exp(-beta (L - L_ref)) and rare
          jumps (weekly probability lam, carrying a share pi of the lasting variance)

What it changes from model A: A's fixed personal home (seeded from the ladder, held by reversion) becomes a
MOVING lasting level that holds the ladder by drift by rank; A's home-reverting component becomes the medium
swing around it.

FROZEN SPECIFICATION (2026-10-18, before the Reddit gates; DOSSIER.md addendum): lasting = "movinghome" -- model
A's OU toward a home that itself random-walks with the size-law lasting volatility and the hold drift by rank
(fixed point, k_min = 20), A's sigma_perm reduced by the lasting variance so that short-lag increments are
unchanged; amplitude on the fast parts only; no jumps.  The variants lasting = "rdm" (separate pooled medium swing)
and jumps = True remain as recorded alternatives.  Parameters come from declared moments: the structure function D_b(h) = Var(X_{t+h} - X_t)
by permanent-rank band (medium and lasting parts), the kurtosis and tail share of standardised long changes
(jumps), and the train ladder (drift, 'hold') or the lasting slides of permanent-rank cohorts ('move').
"""
from __future__ import annotations

import math
from dataclasses import dataclass, replace

import numpy as np
import pandas as pd
from scipy.optimize import nnls

import minimal_rankdiff as mrd
import rankdiff_kalman as rk

Z_CLIP = rk.Z_CLIP
KAPPA_GRID = np.array([0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.07, 0.1, 0.14, 0.2, 0.3, 0.5])
VEL_HALF_LIVES = np.array([26.0, 52.0, 104.0, 208.0, 416.0])     # weeks (drift-by-name extension, DOSSIER addendum 2)


def _vel_shape(h, rho):
    """Var(sum of h consecutive AR(1) velocities) / Var(v): h(1+rho)/(1-rho) - 2 rho (1-rho^h)/(1-rho)^2."""
    h = np.asarray(h, float)
    return h * (1 + rho) / (1 - rho) - 2 * rho * (1 - rho ** h) / (1 - rho) ** 2
LAM_GRID = np.array([0.002, 0.003, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.2])
PI_GRID = np.linspace(0.0, 0.98, 50)
BETA_CLIP = 8.0          # cap on |beta (L - L_ref)| in the size law (keeps the far tail finite)


@dataclass
class RDMParams:
    base: mrd.RankParams            # model A's fast parts, exits, rebirth levels, w0
    K: int                          # scored universe size
    band_ranks: np.ndarray          # band edges in rank (len 9)
    band_z: np.ndarray              # band centres, z = log((rank-0.5)/N)
    band_level: np.ndarray          # mean log level per band
    band_n: np.ndarray              # endpoints per band
    kappa_m: float                  # medium-swing reversion rate (per week)
    V_m: np.ndarray                 # medium-swing stationary variance per band
    s_band: np.ndarray              # lasting variance per week per band (structure-function slope)
    sigma0: float                   # lasting sd per week at L_ref
    beta: float                     # size-variance exponent of the lasting sd
    L_ref: float                    # level at rank K
    lam: float                      # weekly jump probability
    pi: float                       # share of lasting variance carried by jumps
    drift_z: np.ndarray             # z grid of the drift by current rank
    drift_g: np.ndarray             # drift per week at drift_z (relative; mean over the universe ~ 0)
    variant: str = "hold"
    diag: dict | None = None        # estimation diagnostics (moments, fit errors)
    vel_var: float = 0.0            # stationary variance of the home's weekly velocity (drift by name; 0 = off)
    vel_rho: float = 0.0            # weekly persistence of the velocity
    amp_lasting: bool = False       # does the entity amplitude also scale the medium and lasting parts (b = 1)?
    #                                 False = as model A's frozen movement stacks (amplitude on the fast parts
    #                                 only); True scaled sigma_perm explodes held-out displacement (A's note in
    #                                 minimal_rankdiff.simulate; reproduced here, 2z-dd)


# --------------------------------------------------------------------------- #
# Estimation
# --------------------------------------------------------------------------- #
def _matrices(df):
    """Level and rank matrices (T x M) of a long frame, absence-penalised permanent rank, floors."""
    T = int(df["period"].max()) + 1
    X = df.pivot(index="period", columns="entity_id", values="X").reindex(range(T))
    R = df.pivot(index="period", columns="entity_id", values="rank").reindex(range(T))
    floors = df.groupby("period")["N"].first().reindex(range(T)).to_numpy(float) + 1.0
    Rm = R.to_numpy(float)
    pen = np.where(np.isfinite(Rm), Rm, floors[:, None])
    return X.to_numpy(float), Rm, np.nanmean(pen, axis=0), np.isfinite(Rm).mean(axis=0), np.asarray(X.columns)


def _band_edges(K, N):
    e = np.array([0, K / 64, K / 32, K / 16, K / 8, K / 4, K / 2, K, max(N, K + 1)], float)
    return np.maximum.accumulate(e)


def structure_function(X, band_of, nb, H):
    """D_b(h) = Var(X_{t+h} - X_t) by band, pooled over t, with the cross-sectional mean change of each
    (t, h) removed (the common factor).  Returns (nb x H) array and pair counts."""
    T = X.shape[0]
    D = np.full((nb, H), np.nan)
    C = np.zeros((nb, H))
    for h in range(1, H + 1):
        if h >= T:
            break
        d = X[h:] - X[:-h]
        d = d - np.nanmean(d, axis=1, keepdims=True)
        for b in range(nb):
            v = d[:, band_of == b]
            v = v[np.isfinite(v)]
            if v.size > 50:
                D[b, h - 1] = float(np.mean(v * v))
                C[b, h - 1] = v.size
    return D, C


def _passing_parts(base, z, h):
    so = np.interp(z, base.z_knots, base.sigma_obs)
    st = np.interp(z, base.z_knots, base.sigma_trans)
    ph = np.clip(np.interp(z, base.z_knots, base.phi), 0.0, 0.99)
    return 2 * so ** 2 + 2 * st ** 2 * (1 - ph ** h) / (1 - ph ** 2)


def fit_medium_lasting(D, base, band_z, H, velocity=False):
    """NNLS fit r_b(h) = V_b (1 - e^{-kappa h}) + s_b h [+ W_b f(h; rho)] for each kappa (and velocity
    persistence rho) on the grids; pooled kappa (and rho) chosen by the sum over bands of relative squared
    error.  Returns kappa, V, S, sse, and (rho, W) when velocity is on."""
    hs = np.arange(1, H + 1, dtype=float)
    best = None
    rhos = [None] if not velocity else list(0.5 ** (1.0 / VEL_HALF_LIVES))
    for kap in KAPPA_GRID:
        for rho in rhos:
            cols = [1 - np.exp(-kap * hs), hs] + ([_vel_shape(hs, rho)] if rho is not None else [])
            A = np.column_stack(cols)
            sse, V, S, W = 0.0, [], [], []
            for b in range(D.shape[0]):
                r = D[b] - _passing_parts(base, band_z[b], hs)
                ok = np.isfinite(r)
                if ok.sum() < len(cols) + 2:
                    V.append(np.nan); S.append(np.nan); W.append(np.nan)
                    continue
                coef, res = nnls(A[ok], r[ok])
                V.append(coef[0]); S.append(coef[1]); W.append(coef[2] if rho is not None else 0.0)
                sse += res ** 2 / max(np.sum(r[ok] ** 2), 1e-12)
            if best is None or sse < best[0]:
                best = (sse, kap, np.array(V), np.array(S), rho, np.array(W))
    if velocity:
        return best[1], best[2], best[3], best[0], (best[4], best[5])
    return best[1], best[2], best[3], best[0]


def fit_size_law(s_band, band_level, band_n, L_ref, use):
    ok = use & np.isfinite(s_band) & (s_band > 0) & np.isfinite(band_level)
    if ok.sum() < 3:
        s = float(np.nanmedian(s_band[s_band > 0])) if np.any(s_band > 0) else 1e-4
        return math.sqrt(s), 0.0
    x = band_level[ok] - L_ref
    y = np.log(s_band[ok])
    w = np.sqrt(band_n[ok])
    A = np.column_stack([np.ones_like(x), x]) * w[:, None]
    coef = np.linalg.lstsq(A, y * w, rcond=None)[0]
    return float(math.exp(0.5 * coef[0])), float(-0.5 * coef[1])


_GH_X, _GH_W = np.polynomial.hermite_e.hermegauss(24)
_GH_W = _GH_W / _GH_W.sum()


def _mixture_moments(w, lamh, pi, s_amp=0.0, amp_lasting=False):
    """Excess kurtosis and P(|z| > 3) of standardised changes over h weeks: swings (share 1-w) and lasting
    diffusion (share w(1-pi)) scaled by the entity amplitude v, log v ~ N(-s^2/2, s^2) (model A's temperament;
    one amplitude scales the diffusive components, b = 1), plus Poisson(lamh) normal jumps carrying share
    w pi, NOT amplitude-scaled (jumps are events; a lognormal-scaled jump has no moment generating function).
    Exact over Poisson counts, Gauss-Hermite over log v."""
    from scipy.stats import norm, poisson
    nmax = int(max(20, lamh + 10 * math.sqrt(lamh + 1)))
    n = np.arange(nmax + 1)
    pw = poisson.pmf(n, lamh)
    pw = pw / pw.sum()
    jump_n = w * (n * pi / lamh if lamh > 0 else 0.0 * n)
    v = np.exp(-0.5 * s_amp ** 2 + s_amp * _GH_X) if s_amp > 0 else np.ones(1)
    vw = _GH_W if s_amp > 0 else np.ones(1)
    if amp_lasting:
        var = v[:, None] * ((1 - w) + w * (1 - pi)) + jump_n[None, :]
    else:
        var = v[:, None] * (1 - w) + w * (1 - pi) + jump_n[None, :]
    wt = vw[:, None] * pw[None, :]
    m2 = float(np.sum(wt * var))
    m4 = float(np.sum(wt * 3 * var ** 2))
    exc = m4 / m2 ** 2 - 3.0
    q3 = float(np.sum(wt * 2 * norm.sf(3.0 * math.sqrt(m2) / np.sqrt(var))))
    return exc, q3


def _std_kurtosis(X, band_of, nb, h, B=4):
    """Excess kurtosis of changes over h weeks of B-week block means, standardised within band (so that
    differences in scale across bands do not masquerade as tails); the common change of each block removed."""
    T = X.shape[0]
    nbk = T // B
    s = max(1, h // B)
    if nbk < s + 2:
        return np.nan, 0
    Mb = np.array([np.nanmean(X[i * B:(i + 1) * B], axis=0) for i in range(nbk)])
    d = Mb[s:] - Mb[:-s]
    d = d - np.nanmean(d, axis=1, keepdims=True)
    zs = []
    for b in range(nb):
        v = d[:, band_of == b]
        v = v[np.isfinite(v)]
        if v.size > 100:
            zs.append(v / np.std(v))
    if not zs:
        return np.nan, 0
    z = np.concatenate(zs)
    return float(np.mean(z ** 4) - 3.0), int(z.size)


def fit_jumps(X, band_of, nb, horizons, shares, s_amp=0.0, amp_lasting=False):
    """Jump rate and share from the excess kurtosis of standardised long changes at two horizons.  Amplitude
    heterogeneity gives the same kurtosis at every horizon; jumps give kurtosis that decays as 1/(lam h); the
    pair identifies (lam, pi).  shares: the lasting share of the change variance at each horizon (from the
    structure-function fit).  Returns lam, pi, observed kurtoses."""
    obs = [_std_kurtosis(X, band_of, nb, h)[0] for h in horizons]
    if not all(np.isfinite(obs)):
        return 0.0, 0.0, obs
    ws = [float(np.clip(w, 0.02, 1.0)) for w in shares]
    pred0 = [_mixture_moments(w, 1.0, 0.0, s_amp, amp_lasting)[0] for w in ws]
    best = (sum(((p0 - o) / max(abs(o), 0.3)) ** 2 for p0, o in zip(pred0, obs)), 0.0, 0.0)
    for lam in LAM_GRID:
        for pi in PI_GRID[1:]:
            pred = [_mixture_moments(w, lam * h, pi, s_amp, amp_lasting)[0] for w, h in zip(ws, horizons)]
            err = sum(((pr - o) / max(abs(o), 0.3)) ** 2 for pr, o in zip(pred, obs))
            if err < best[0]:
                best = (err, lam, pi)
    return float(best[1]), float(best[2]), obs


def ladder_slope(levels_sorted, N, grid_r):
    """S(r) = -d level / d log rank of a smoothed ladder (cubic in log rank)."""
    r = np.arange(1, levels_sorted.size + 1, dtype=float)
    lr = np.log(r)
    c = np.polyfit(lr, levels_sorted, 3)
    dc = np.polyder(c)
    return np.clip(-np.polyval(dc, np.log(grid_r)), 0.15, None), np.polyval(c, np.log(grid_r))


def drift_from_slides(k, s, N):
    """Stratum drift by current rank from a cumulative net-slide profile s_k (the top k's mean drift deficit):
    g(k) = -d(k s_k)/dk, relative to the fixed floor that the exit-and-rebirth mechanism supplies at the
    bottom of the universe (the reflecting floor of Gabaix's and the Atlas models).  NOT centred: a common
    deficit of the upper ranks against the floor is what holds a ladder of slope S = sigma^2 / 2s."""
    k = np.asarray(k, float)
    F = k * np.asarray(s, float)
    km = 0.5 * (k[1:] + k[:-1])
    g = -np.diff(F) / np.diff(k)
    return np.log(np.clip((km - 0.5) / N, Z_CLIP, 1.0)), g


def balance_slide(sig2, S, lam, pi):
    """The net slide that holds a local slope S against lasting moves with weekly cumulant generating
    function kappa(theta) = (1-pi) sig2 theta^2/2 + lam (exp(sig2_J theta^2/2) - 1), sig2_J = pi sig2/lam:
    s = S kappa(1/S) (the boundary formula with all cumulants; theory note 8, Gomez 2023 Prop. 5).  The
    amplitude average of the diffusive part is its mean (E v = 1)."""
    s = (1 - pi) * sig2 / (2.0 * S)
    if lam > 0 and pi > 0:
        s = s + lam * S * np.expm1(np.clip(pi * sig2 / lam / (2.0 * S * S), 0, 50))
    return s


def drift_hold(p_partial, ladder, N):
    """Diffusion-only Atlas balance at the current lasting ladder, s_k = (1-pi) sigma_L^2(k) / (2 S_k): the
    starting point of drift_hold_numeric."""
    k = np.unique(np.round(np.geomspace(1, N, 80)).astype(int)).astype(float)
    S, lev = ladder_slope(ladder, N, k)
    lo, hi = p_partial["band_lo"], p_partial["band_hi"]
    sig2 = (p_partial["sigma0"] * np.exp(-p_partial["beta"] * (np.clip(lev, lo, hi) - p_partial["L_ref"]))) ** 2
    return drift_from_slides(k, (1 - p_partial["pi"]) * sig2 / (2.0 * S), N)


def drift_hold_numeric(p, L0, iters=8, weeks=52, seeds=(101, 102, 103), damp=0.8, k_min=20):
    """The drift by current rank that holds the CURRENT lasting ladder stationary in this model, found by
    fixed-point iteration on simulations of the lasting level alone (drift, size-law diffusion, amplitude,
    jumps, exit and rebirth): at each pass, the ladder's mean weekly change at each rank is subtracted from the
    drift there.  The closed form S kappa(1/S) breaks for jumps that reach beyond a locally exponential ladder;
    this includes every cumulant and finite-ladder effect.  The only moment used is the ladder itself.
    k_min: the ladder above rank k_min is a handful of extreme order statistics, driven by single jumps; it is
    not used to steer the drift, and the drift above k_min is held at its value at k_min (2z-dd)."""
    L0 = np.sort(np.asarray(L0, float))[::-1]
    N = L0.size
    zq = p.drift_z.copy()
    g = p.drift_g.copy()
    kq = np.exp(zq) * N + 0.5
    hist = []
    for it in range(iters):
        q = replace(p, drift_g=g)
        rates = []
        for sd in seeds:
            rng = np.random.default_rng(sd)
            st = _State(q, L0, rng)
            lad = np.empty((weeks, kq.size))
            for t in range(weeks):
                st.step_lasting()
                srt = np.sort(st.L)[::-1]
                lad[t] = srt[np.clip(np.round(kq).astype(int) - 1, 0, N - 1)]
            tt = np.arange(weeks) - (weeks - 1) / 2
            rates.append((tt @ (lad - lad.mean(0))) / (tt @ tt))
        rate = np.mean(rates, axis=0)
        use = (kq >= k_min) & (kq <= N)
        hist.append(float(np.sqrt(np.mean(rate[use & (kq <= p.K)] ** 2))))
        g = np.where(use, g - damp * rate, g)
        g = np.where(kq < k_min, np.interp(k_min, kq, g), g)
    return zq, g, hist


def estimate_rdm(df, K, base=None, variant="hold", H=None, hJ=None, base_kwargs=None, slides=None, amp_lasting=False,
                 jumps=True, velocity=False):
    """Estimate the RDM on a universe-restricted long frame (train window in the gate)."""
    if base is None:
        base = mrd.estimate(df, **(base_kwargs or {}))
    X, R, rperm, pres, ids = _matrices(df)
    T, M = X.shape
    N = base.N
    edges = _band_edges(K, N)
    nb = len(edges) - 1
    band_of = np.clip(np.searchsorted(edges, rperm, side="right") - 1, 0, nb - 1)
    band_of = np.where(pres >= 0.5, band_of, -1)
    band_z = np.log(np.clip((np.sqrt(np.maximum(edges[:-1], 1) * edges[1:]) - 0.5) / N, Z_CLIP, 1.0))
    band_level = np.array([np.nanmean(X[:, band_of == b]) if np.any(band_of == b) else np.nan for b in range(nb)])
    band_n = np.array([int(np.sum(band_of == b)) for b in range(nb)], float)
    H = H or int(min(52, max(4, T // 2)))
    D, C = structure_function(X, band_of, nb, H)
    vel_var, vel_rho = 0.0, 0.0
    if velocity:
        kap, V, S, sse, (vel_rho, W) = fit_medium_lasting(D, base, band_z, H, velocity=True)
        inK = (np.arange(nb) < nb - 1) & np.isfinite(W)
        vel_var = float(np.sum(W[inK] * band_n[inK]) / max(np.sum(band_n[inK]), 1.0))
    else:
        kap, V, S, sse = fit_medium_lasting(D, base, band_z, H)
    lad = np.sort(X[-1][np.isfinite(X[-1])])[::-1]
    L_ref = float(lad[min(K, lad.size) - 1])
    use = np.arange(nb) < nb - 1                           # bands inside the scored universe
    sigma0, beta = fit_size_law(S, band_level, band_n, L_ref, use)
    hJ = hJ or int(min(26, H))
    hJs = sorted({max(8, hJ // 2), hJ})
    shares = []
    for h in hJs:
        Dh = np.nanmean(D[:nb - 1, h - 1]) if h - 1 < D.shape[1] else np.nan
        shares.append(float(np.nanmean(S[:nb - 1]) * h / Dh) if np.isfinite(Dh) and Dh > 0 else 0.5)
    if len(hJs) == 2 and jumps:
        lam, pi, K_obs = fit_jumps(X, band_of, nb, hJs, shares, s_amp=float(getattr(base, "temper_s", 0.0)), amp_lasting=amp_lasting)
    else:
        lam, pi, K_obs = 0.0, 0.0, [np.nan]
    part = dict(sigma0=sigma0, beta=beta, L_ref=L_ref, lam=lam, pi=pi,
                band_lo=float(np.nanmin(band_level)), band_hi=float(np.nanmax(band_level)))
    lev_f = rk._filtered_levels(df, base).to_numpy()
    ladder = np.sort(lev_f[np.isfinite(lev_f)])[::-1]
    if variant == "move" and slides is not None:
        dz, dg = drift_from_slides(slides[0], slides[1], N)
    else:
        dz, dg = drift_hold(part, ladder, N)
    diag = dict(D=D, C=C, H=H, sse=sse, K_obs=K_obs, hJ=hJs, lasting_share=shares)
    p_out = RDMParams(base=base, K=int(K), band_ranks=edges, band_z=band_z, band_level=band_level, band_n=band_n,
                      kappa_m=float(kap), V_m=np.nan_to_num(V), s_band=np.nan_to_num(S), sigma0=sigma0, beta=beta,
                      L_ref=L_ref, lam=lam, pi=pi, drift_z=dz, drift_g=dg, variant=variant, diag=diag,
                      amp_lasting=amp_lasting, vel_var=vel_var, vel_rho=float(vel_rho))
    if variant == "hold":
        dz, dg, hist = drift_hold_numeric(p_out, ladder)
        diag["hold_rms_rate"] = hist
        p_out = replace(p_out, drift_z=dz, drift_g=dg)
    return p_out


# --------------------------------------------------------------------------- #
# Simulation
# --------------------------------------------------------------------------- #
def _sigma_L(p, L):
    """Size law, applied over the range of levels it was estimated on (the band levels): outside it the
    volatility is held at the end value rather than extrapolated."""
    lo, hi = np.nanmin(p.band_level), np.nanmax(p.band_level)
    return p.sigma0 * np.exp(-p.beta * (np.clip(L, lo, hi) - p.L_ref))


def _vm(p, z):
    return np.interp(z, p.band_z, p.V_m)


class _State:
    """Weekly state of the simulated universe (shared by the conditional, unconditional and panel sims)."""

    def __init__(self, p, L0, rng, lasting="rdm", home=None):
        b = p.base
        self.p, self.rng, self.lasting = p, rng, lasting
        self.N = L0.size
        self.L = L0.astype(float).copy()
        self.home = (self.L.copy() if home is None else home.astype(float).copy())
        self.m = np.zeros(self.N)
        self.xi = np.zeros(self.N)
        self.ids = np.arange(self.N, dtype=np.int64)
        self.next_id = self.N
        ts = getattr(b, "temper_s", 0.0)
        self.ts = ts
        self.sqv = np.sqrt(rng.lognormal(-0.5 * ts * ts, ts, self.N)) if ts > 0 else np.ones(self.N)
        self.vel = (math.sqrt(p.vel_var) * rng.standard_normal(self.N)) if p.vel_var > 0 else None
        mix_b = getattr(b, "mix_b", 0.0)                # model A's --mix-hetero: sigma_perm scaled by v^(b/2)
        self.mix_b = mix_b
        self.sqw = mrd._sqw(self.sqv, mix_b, ts) if (ts > 0 and mix_b > 0) else 1.0

    def step_lasting(self):
        """The lasting level alone (drift, size-law diffusion with amplitude, jumps, exit and rebirth).  For the
        moving-home variant the lasting process is the home, which has the same law."""
        p, b, rng, N = self.p, self.p.base, self.rng, self.N
        order = np.argsort(-self.L)
        prank = np.empty(N, np.int64)
        prank[order] = np.arange(1, N + 1)
        zp = np.log(np.clip((prank - 0.5) / N, Z_CLIP, 1.0))
        sL = _sigma_L(p, self.L)
        ampL = self.sqv if p.amp_lasting else 1.0
        self.L = self.L + np.interp(zp, p.drift_z, p.drift_g) + sL * ampL * math.sqrt(1.0 - p.pi) * rng.standard_normal(N)
        if self.vel is not None:
            self.L = self.L + self.vel
            self.vel = p.vel_rho * self.vel + math.sqrt(p.vel_var * (1 - p.vel_rho ** 2)) * rng.standard_normal(N)
        if p.pi > 0 and p.lam > 0:
            jmp = rng.random(N) < p.lam
            nj = int(jmp.sum())
            if nj:
                self.L[jmp] += sL[jmp] * math.sqrt(p.pi / p.lam) * rng.standard_normal(nj)
        ex = rng.random(N) < np.interp(zp, b.z_knots, b.exit_rate)
        ne = int(ex.sum())
        if ne:
            self.L[ex] = b.bottom_mu[rng.integers(0, b.bottom_mu.size, ne)]
            if self.ts > 0:
                self.sqv[ex] = np.sqrt(rng.lognormal(-0.5 * self.ts ** 2, self.ts, ne))
            if self.vel is not None:
                self.vel[ex] = math.sqrt(p.vel_var) * rng.standard_normal(ne)

    def step(self):
        p, b, rng, N = self.p, self.p.base, self.rng, self.N
        order = np.argsort(-self.L)
        prank = np.empty(N, np.int64)
        prank[order] = np.arange(1, N + 1)
        zp = np.log(np.clip((prank - 0.5) / N, Z_CLIP, 1.0))
        if self.lasting == "home":                     # model A's dynamics: OU toward a fixed home
            kz = np.interp(zp, b.z_knots, b.kappa_z) if b.kappa_z is not None else b.kappa
            sp = np.interp(zp, b.z_knots, b.sigma_perm)
            self.L = self.L - kz * (self.L - self.home) + sp * self.sqw * rng.standard_normal(N)
        elif self.lasting == "movinghome":             # A's OU toward a home that itself moves (2z-dd)
            horder = np.argsort(-self.home)
            hrank = np.empty(N, np.int64)
            hrank[horder] = np.arange(1, N + 1)
            zh = np.log(np.clip((hrank - 0.5) / N, Z_CLIP, 1.0))
            sLh = _sigma_L(p, self.home)
            self.home = self.home + np.interp(zh, p.drift_z, p.drift_g) + sLh * math.sqrt(1.0 - p.pi) * rng.standard_normal(N)
            if self.vel is not None:                      # drift by name: a persistent velocity of the home
                self.home = self.home + self.vel
                self.vel = p.vel_rho * self.vel + math.sqrt(p.vel_var * (1 - p.vel_rho ** 2)) * rng.standard_normal(N)
            if p.pi > 0 and p.lam > 0:
                jmp = rng.random(N) < p.lam
                nj = int(jmp.sum())
                if nj:
                    self.home[jmp] += sLh[jmp] * math.sqrt(p.pi / p.lam) * rng.standard_normal(nj)
            kz = np.interp(zp, b.z_knots, b.kappa_z) if b.kappa_z is not None else b.kappa
            sp2 = np.interp(zp, b.z_knots, b.sigma_perm) ** 2
            sp = np.sqrt(np.maximum(sp2 - sLh ** 2, 0.1 * sp2))      # short-lag increment variance preserved
            self.L = self.L - kz * (self.L - self.home) + sp * self.sqw * rng.standard_normal(N)
        else:
            sL = _sigma_L(p, self.L)
            g = np.interp(zp, p.drift_z, p.drift_g)
            # one amplitude scales the diffusive components (b = 1); jumps are events of band size, unscaled
            ampL = self.sqv if p.amp_lasting else 1.0
            self.L = self.L + g + sL * ampL * math.sqrt(1.0 - p.pi) * rng.standard_normal(N)
            if p.pi > 0 and p.lam > 0:
                jmp = rng.random(N) < p.lam
                nj = int(jmp.sum())
                if nj:
                    self.L[jmp] += sL[jmp] * math.sqrt(p.pi / p.lam) * rng.standard_normal(nj)
            a = math.exp(-p.kappa_m)
            self.m = a * self.m + ampL * np.sqrt(_vm(p, zp) * (1 - a * a)) * rng.standard_normal(N)
        phi = np.interp(zp, b.z_knots, b.phi)
        st = np.interp(zp, b.z_knots, b.sigma_trans)
        so = np.interp(zp, b.z_knots, b.sigma_obs)
        self.xi = phi * self.xi + st * self.sqv * mrd._tdraw(rng, getattr(b, "t_df", float("inf")), N)
        ex = rng.random(N) < np.interp(zp, b.z_knots, b.exit_rate)
        ne = int(ex.sum())
        if ne:
            sm = b.bottom_mu[rng.integers(0, b.bottom_mu.size, ne)]
            self.L[ex] = sm
            self.home[ex] = sm
            self.m[ex] = 0.0
            self.xi[ex] = 0.0
            self.ids[ex] = np.arange(self.next_id, self.next_id + ne)
            self.next_id += ne
            if self.vel is not None:
                self.vel[ex] = math.sqrt(p.vel_var) * rng.standard_normal(ne)
            if self.ts > 0:
                self.sqv[ex] = np.sqrt(rng.lognormal(-0.5 * self.ts ** 2, self.ts, ne))
                if self.mix_b > 0:
                    self.sqw[ex] = mrd._sqw(self.sqv[ex], self.mix_b, self.ts)
        return self.L + self.m + self.xi + so * self.sqv * rng.standard_normal(N)


def filtered_state_rdm(df_tr, p):
    """The RDM's own conditional state at the end of the train window: a per-endpoint two-state Kalman filter
    (lasting level L, a random walk with variance sigma_L^2 per week; medium swing m, AR(1) with rate kappa_m and
    stationary variance V_m) observed as X = L + m + noise, noise = measurement plus the fast swing folded in
    (sigma_obs^2 + sigma_tr^2/(1-phi^2)).  Parameters by permanent-rank band.  Returns (ids, L, m)."""
    b = p.base
    T0 = int(df_tr["period"].max()) + 1
    Xp = df_tr.pivot_table(index="period", columns="entity_id", values="X").reindex(range(T0))
    N = int(round(df_tr.groupby("period")["entity_id"].size().mean()))
    rbar = df_tr.groupby("entity_id")["rank"].mean().reindex(Xp.columns)
    zb = np.log(np.clip((rbar.to_numpy() - 0.5) / N, Z_CLIP, 1.0))
    so = np.interp(zb, b.z_knots, b.sigma_obs)
    st = np.interp(zb, b.z_knots, b.sigma_trans)
    ph = np.clip(np.interp(zb, b.z_knots, b.phi), 0, 0.99)
    R = np.clip(so ** 2 + st ** 2 / (1 - ph ** 2), 1e-6, None)
    lev = df_tr.groupby("entity_id")["X"].mean().reindex(Xp.columns).to_numpy()
    qL = _sigma_L(p, lev) ** 2 * (1 - p.pi) + (_sigma_L(p, lev) ** 2 * p.pi)        # lasting variance per week
    a = math.exp(-p.kappa_m)
    Vm = np.interp(zb, p.band_z, p.V_m)
    qm = Vm * (1 - a * a)
    M = Xp.to_numpy()
    n = M.shape[1]
    L = np.full(n, np.nan)
    m = np.zeros(n)
    P11 = np.full(n, np.nan); P12 = np.zeros(n); P22 = Vm.copy()
    for t in range(M.shape[0]):
        # predict
        m = a * m
        P11 = P11 + qL
        P12 = a * P12
        P22 = a * a * P22 + qm
        obs = np.isfinite(M[t])
        first = obs & ~np.isfinite(L)
        L[first] = M[t][first]
        m[first] = 0.0
        P11[first] = R[first]; P12[first] = 0.0; P22[first] = Vm[first]
        u = obs & ~first
        if u.any():
            S = P11[u] + 2 * P12[u] + P22[u] + R[u]
            K1 = (P11[u] + P12[u]) / S
            K2 = (P12[u] + P22[u]) / S
            e = M[t][u] - L[u] - m[u]
            L[u] = L[u] + K1 * e
            m[u] = m[u] + K2 * e
            p11, p12, p22 = P11[u], P12[u], P22[u]
            P11[u] = p11 - K1 * (p11 + p12)
            P12[u] = p12 - K1 * (p12 + p22)
            P22[u] = p22 - K2 * (p12 + p22)
    ok = np.isfinite(L)
    return np.asarray(Xp.columns)[ok], L[ok], m[ok]


def sim_cohort_conditional_rdm(p, df_tr, T_sim, seed=0, cohort_k=200, lasting="rdm"):
    """The gate's conditional cohort simulation (rk.sim_cohort_conditional), RDM dynamics.  Same start (the
    filtered end-of-train state), same step-0 timing (recording from step 1), same cohort and collision
    bookkeeping."""
    rng = np.random.default_rng(seed)
    if lasting in ("home", "movinghome"):
        levels = rk._filtered_levels(df_tr, p.base)
        st = _State(p, levels.to_numpy(), rng, lasting=lasting)
    else:
        _, L0, m0 = filtered_state_rdm(df_tr, p)
        st = _State(p, L0, rng, lasting=lasting)
        st.m = m0.copy()
    cohort_slots = cohort_ids = None
    cranks, topids = [], []
    for t in range(T_sim + 1):
        X = st.step()
        order = np.argsort(-X)
        rank = np.empty(st.N, np.int64)
        rank[order] = np.arange(1, st.N + 1)
        if t == 0:
            continue
        if cohort_slots is None:
            cohort_slots = order[:cohort_k].copy()
            cohort_ids = st.ids[cohort_slots].copy()
        alive = st.ids[cohort_slots] == cohort_ids
        cr = rank[cohort_slots].astype(float)
        cr[~alive] = np.nan
        cranks.append(cr)
        topids.append(st.ids[order[:300]])
    return np.array(cranks), np.array(topids)


def sim_cohort_rdm(p, T_sim, seed=0, cohort_k=200, burn=40, lasting="rdm"):
    """Unconditional counterpart (rk.sim_cohort), used only for the gate's sigma_obs scale calibration."""
    rng = np.random.default_rng(seed)
    L0 = mrd._extend(p.base.w0, p.base.N)
    st = _State(p, L0, rng, lasting=lasting)
    cohort_slots = cohort_ids = None
    cranks, topids = [], []
    for t in range(-burn, T_sim):
        X = st.step()
        if t < 0:
            continue
        order = np.argsort(-X)
        rank = np.empty(st.N, np.int64)
        rank[order] = np.arange(1, st.N + 1)
        if cohort_slots is None:
            cohort_slots = order[:cohort_k].copy()
            cohort_ids = st.ids[cohort_slots].copy()
        alive = st.ids[cohort_slots] == cohort_ids
        cr = rank[cohort_slots].astype(float)
        cr[~alive] = np.nan
        cranks.append(cr)
        topids.append(st.ids[order[:300]])
    return np.array(cranks), np.array(topids)


def _dist(fn, horizons, reps, coll_ranks=(1, 5, 20)):
    pooled = {h: [] for h in horizons}
    racfs, colls = [], {c: [] for c in coll_ranks}
    for s in range(reps):
        cr, ti = fn(s)
        d, rf = rk._rank_dist(cr, horizons)
        for h in horizons:
            pooled[h].append(d[h])
        racfs.append(rf)
        cc = rk._collisions_from_topids(ti, coll_ranks)
        for c in coll_ranks:
            colls[c].append(cc.get(f"coll{c}", np.nan))
    dist = {h: (np.concatenate(pooled[h]) if pooled[h] else np.array([])) for h in horizons}
    return dist, float(np.nanmedian(racfs)), {c: float(np.nanmean(colls[c])) for c in coll_ranks}


def _calibrate_scale_rdm(p, df_tr, horizons, Ltr, reps=3, lasting="rdm"):
    """rk._calibrate_scale with the RDM's own unconditional simulation (same grid, same train vector)."""
    ed, erf, ec = rk.emp_dist(df_tr, horizons)
    em = rk._moments(ed, erf, ec, horizons)
    best_scale, best_err = 1.0, np.inf
    for sc in (1.0, 0.7, 0.5, 0.35, 0.25, 0.15, 0.10, 0.05, 0.0):
        q = replace(p, base=rk.replace_obs(p.base, sc))
        sd, srf, scol = _dist(lambda s: sim_cohort_rdm(q, Ltr, seed=s, lasting=lasting), horizons, reps)
        e = rk._cal_error(em, rk._moments(sd, srf, scol, horizons))
        if e < best_err:
            best_err, best_scale = e, sc
    return best_scale


# --------------------------------------------------------------------------- #
# Gate v1 for the RDM (scoring code imported from rankdiff_kalman, unchanged)
# --------------------------------------------------------------------------- #
FROZEN_MOVEMENT = {   # the frozen movement stacks' estimator flags (frozen-specs.md)
    "facebook_a": dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True, spec_b=True),
    "reddit_comments": dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True, mix_hetero=True),
    "reddit": dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True),
}


def oos_rdm(platform, top_k, n_splits=5, reps=3, boot=400, buffer_mult=4, variant="hold", lasting="movinghome",
            amp_lasting=False, jumps=False):
    """Gate v1 with the RDM's conditional simulation.  Everything else as rk.oos_movement with the frozen
    movement-stack flags: windows, train-only universe, Spec-B where frozen, scale calibration on the train
    moment vector, empirical moments, baseline, MOM_FLOOR, bootstrap coverage."""
    flags = dict(FROZEN_MOVEMENT[platform])
    spec_b = flags.pop("spec_b", False)
    mix = flags.pop("mix_hetero", False)
    print(f"\n{'=' * 72}\n{platform.upper()} — Gate v1, RDM ({variant}, lasting={lasting})\n{'=' * 72}", flush=True)
    df_full = mrd.load_panel(mrd.PLATFORMS[platform])
    T = int(df_full["period"].max()) + 1
    origins, test_len = rk._gate_windows(T, n_splits)
    print(f"  T={T}  test_len={test_len}  origins={origins}  universe=top-{top_k}", flush=True)
    daily = None
    if spec_b:
        import spec_b_sigma_obs as sb
        cfg = mrd.PLATFORMS[platform]
        daily = sb.load_daily(set(df_full["entity_id"].unique()), path=cfg.get("daily_path"), day_guard=cfg.get("day_guard", False))
    rows = []
    for T0 in origins:
        df = mrd.restrict_universe(df_full, top_k, buffer_mult=buffer_mult, member_window=T0)
        df_tr, df_te = rk._split_panel(df, T0, test_len)
        hor = [h for h in (1, 4, 13) if h < test_len]
        so_fix = None
        if spec_b:
            cur = sb.spec_b_curve(df, daily, max_period=T0)
            so_fix = (cur["z"], cur["sigma_obs"])
        base = rk._estimate_fast(df_tr, 0.5, sigma_obs_fix=so_fix, mix_hetero=mix, **flags)
        p = estimate_rdm(df_tr, top_k, base=base, variant=variant, amp_lasting=amp_lasting, jumps=jumps)
        scale = _calibrate_scale_rdm(p, df_tr, hor, T0, reps=reps, lasting=lasting)
        p = replace(p, base=rk.replace_obs(p.base, scale))
        ed, erf, ec, ed_R = rk.emp_dist(df_te, hor, return_R=True)
        td, trf, tc = rk.emp_dist(df_tr, hor)
        sd, srf, sc = _dist(lambda s: sim_cohort_conditional_rdm(p, df_tr, test_len, seed=s, lasting=lasting), hor, reps)
        rows.append(dict(T0=T0, scale=scale, hor=hor, em=rk._moments(ed, erf, ec, hor), bm=rk._moments(td, trf, tc, hor),
                         sm=rk._moments(sd, srf, sc, hor), ed=ed, sd=sd, params=dict(kappa_m=p.kappa_m, sigma0=p.sigma0,
                         beta=p.beta, lam=p.lam, pi=p.pi, V_m=p.V_m.tolist(), s_band=p.s_band.tolist())))
        gh = np.interp(np.log((np.array([1, 20, 100, 1000]) - 0.5) / p.base.N), p.drift_z, p.drift_g)
        print(f"    T0={T0}: kappa_m {p.kappa_m:.3f}, sigma0 {p.sigma0:.4f}, beta {p.beta:+.3f}, lam {p.lam:.4f}, pi {p.pi:.2f}, scale {scale:.2f}; "
              f"drift at ranks 1/20/100/1000 {np.round(gh, 4).tolist()}", flush=True)
    keys = [f"dRank{h}" for h in rows[0]["hor"]] + ["RACF1"] + [f"coll{c}" for c in (1, 5, 20)]
    me_all, be_all, per_key = [], [], {k: [] for k in keys}
    for r in rows:
        em, sm, bm = r["em"], r["sm"], r["bm"]
        kk = [k for k in keys if np.isfinite(em.get(k, np.nan)) and np.isfinite(sm.get(k, np.nan)) and abs(em[k]) >= rk.MOM_FLOOR]
        me = float(np.mean([abs(sm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk]))
        be = float(np.mean([abs(bm[k] - em[k]) / max(abs(em[k]), 1e-6) for k in kk if np.isfinite(bm.get(k, np.nan))]))
        for k in kk:
            per_key[k].append(abs(sm[k] - em[k]) / max(abs(em[k]), 1e-6))
        me_all.append(me)
        be_all.append(be)
        h0 = r["hor"][0]
        lo, hi = rk._boot_ci(r["ed"][h0], B=boot)
        r["inci"] = bool(np.isfinite(lo) and lo <= sm[f"dRank{h0}"] <= hi)
        print(f"    T0={r['T0']:>3}  model relErr {me:.3f} | persistence {be:.3f} | dR{h0} in CI: {r['inci']}", flush=True)
    cov = float(np.mean([r["inci"] for r in rows]))
    print(f"  MODEL rel err = {np.mean(me_all):.3f} ± {np.std(me_all):.3f}   |   persistence = {np.mean(be_all):.3f} ± {np.std(be_all):.3f}", flush=True)
    print(f"  coverage {cov * 100:.0f}%;  per-moment model rel err: " + ", ".join(f"{k} {np.mean(v):.3f}" for k, v in per_key.items() if v), flush=True)
    return dict(model_rel=float(np.mean(me_all)), model_rel_sd=float(np.std(me_all)), base_rel=float(np.mean(be_all)),
                base_rel_sd=float(np.std(be_all)), coverage=cov, per_key={k: float(np.mean(v)) for k, v in per_key.items() if v},
                splits=[dict(T0=r["T0"], scale=r["scale"], model=me_all[i], base=be_all[i], params=r["params"],
                             em=r["em"], sm=r["sm"]) for i, r in enumerate(rows)])


# --------------------------------------------------------------------------- #
# Panel simulation (structure validation): full weekly X for every endpoint, as a long frame
# --------------------------------------------------------------------------- #
def simulate_panel(p, L0, T_sim, seed=0, lasting="rdm", home=None):
    """Weekly observed levels of the whole simulated universe, T_sim x N, with occupant ids (exits are
    reborn as new ids).  L0: starting lasting levels (e.g. filtered levels at the start date)."""
    rng = np.random.default_rng(seed)
    st = _State(p, np.asarray(L0, float), rng, lasting=lasting, home=home)
    Xs = np.empty((T_sim, st.N))
    ids = np.empty((T_sim, st.N), np.int64)
    for t in range(T_sim):
        Xs[t] = st.step()
        ids[t] = st.ids
    return Xs, ids


def panel_long_frame(Xs, ids, n_rank=None):
    """Long frame (entity_id, period, X, rank, N) from a simulated panel: every occupant id is an entity;
    ranks within the week among all simulated endpoints."""
    T, N = Xs.shape
    order = np.argsort(-Xs, axis=1)
    rank = np.empty_like(order)
    rows = np.arange(T)[:, None]
    rank[rows, order] = np.arange(1, N + 1)[None, :]
    df = pd.DataFrame(dict(entity_id=ids.ravel().astype(str), period=np.repeat(np.arange(T), N),
                           X=Xs.ravel(), rank=rank.ravel()))
    df["N"] = N
    df["metric"] = np.expm1(np.clip(df["X"].to_numpy(), None, 50))
    df["z"] = np.log(np.clip((df["rank"].to_numpy() - 0.5) / N, Z_CLIP, 1.0))
    return df
