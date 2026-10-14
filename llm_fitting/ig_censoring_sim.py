"""Planted Instagram-like worlds: true attention dynamics, then the "a"-query censoring measured in
MODEL_STATUS 2z-c applied on top (design in runs/2026-10-15_instagram/DESIGN.md).

True world: names on an exponential ladder (slope S) whose log activity is a random walk (lasting
variance sig2 a week) plus a passing swing (OU, variance swing_v, half-life swing_hl weeks); each name
posts P_i times a week (log-normal, median posts_median).

Censoring, layer by layer (2z-c):
  q   each name's share of posts whose caption matches "a": log q_i ~ N(q_logmean, q_logsd), persistent,
      optionally drifting as a random walk of variance q_drift a week (caption habits changing);
  L1  the matching posts M_it ~ Poisson(q_i P_i); the observed total is M_it posts' engagement,
      log Y_obs = log Y_true + log(M_it / P_i) + eta, eta ~ N(0, c2 / M_it): thinning noise of variance
      about (1 + c2) / M, the envelope 2z-c validated;
  L2  instrument dropout: each week a common rate r_t = 0.13 + 0.047 z_t, each name missing with
      probability r_t, independent of its posting;
  L3  ghosts: a share of names with log q ~ N(-4, 0.5), present only in the rare weeks a post matches.
A name with M_it = 0 is absent that week.
"""
import numpy as np


def true_world(N=25600, S=1.5, sig2=0.01, weeks=44, swing_v=0.2, swing_hl=6.0, posts_median=20.0, seed=0):
    rng = np.random.default_rng(seed)
    x = S * np.log(N / np.arange(1, N + 1))
    kap = np.log(2) / swing_hl
    z = rng.normal(0, np.sqrt(swing_v), N)
    out = np.empty((weeks, N))
    for t in range(weeks):
        x = x + rng.normal(0, np.sqrt(sig2), N)
        z = z * np.exp(-kap) + rng.normal(0, np.sqrt(swing_v * (1 - np.exp(-2 * kap))), N)
        out[t] = x + z
    P = posts_median * np.exp(rng.normal(0, 0.8, N))
    return out, P


def censor(X, P, q_logmean=-1.0, q_logsd=1.0, q_drift=0.0, c2=0.253, drop=0.13, drop_week_sd=0.047,
           ghosts=0.03, seed=1):
    rng = np.random.default_rng(seed)
    T, N = X.shape
    lq = rng.normal(q_logmean, q_logsd, N)
    g = rng.random(N) < ghosts
    lq[g] = rng.normal(-4.0, 0.5, g.sum())
    out = np.full((T, N), np.nan)
    for t in range(T):
        if q_drift > 0:
            lq = lq + rng.normal(0, np.sqrt(q_drift), N)
        q = np.minimum(np.exp(lq), 1.0)
        Mt = rng.poisson(q * P)
        r = float(np.clip(drop + drop_week_sd * rng.standard_normal(), 0.0, 0.5))
        up = (Mt > 0) & (rng.random(N) >= r)
        eta = rng.normal(0, 1, N) * np.sqrt(c2 / np.maximum(Mt, 1))
        out[t, up] = X[t, up] + np.log(Mt[up] / P[up]) + eta[up]
    return out
