"""V4 independent planted processes; no empirical loader or production import.

Scientific choices: handoff/V4_planted_rivals/DESIGN.md. The continuing
state makes it possible to freeze a prediction before generating its target.
"""
from __future__ import annotations

import copy
import numpy as np
import pandas as pd
from scipy.special import ndtri

from .planted_sim import extend_homes

PANELS = {
    "facebook_a": dict(K=3500, T=86, block=26, source="facebook_a", system="facebook"),
    "reddit_comments_ext": dict(K=12500, T=213, block=52, source="reddit_comments", system="reddit"),
    "reddit_submissions_long": dict(K=5000, T=212, block=52,
                                    source="reddit_submissions_long", system="reddit"),
}
CLASSES = {
    "home": "named stationary level", "fixed_walk": "common slide",
    "replacement": "flow", "rank_drift": "position", "unstabilized": "common slide",
    "equal_components": "named stationary level", "position_ou": "position",
    "rw_medium": "common slide", "rw_persistent": "common slide",
    "fixed_static": "nuisance only",
}
RIVALS = tuple(CLASSES)


def slope(v, k):
    r = np.arange(int(np.floor(k / np.sqrt(2))) + 1,
                  min(len(v), int(np.floor(k * np.sqrt(2)))) + 1)
    if len(r) < 3:
        return np.nan
    return float(-np.polyfit(np.log(r), np.asarray(v)[r - 1], 1)[0])


def calibrate(saved, config, absence=None):
    """Deterministic declared moments, never a simulation score."""
    p = copy.deepcopy(saved["parameters"])
    for key, val in p.items():
        if isinstance(val, list):
            p[key] = np.asarray(val, float)
        elif val == "Infinity":
            p[key] = np.inf
    K, H = config["K"], config["block"]
    rank = np.arange(max(1, int(K / 16)), K + 1)
    z = np.log((rank - .5) / p["N"])
    kap = np.interp(z, p["z_knots"], p["kappa_z"])
    sp = np.interp(z, p["z_knots"], p["sigma_perm"])
    phi = np.interp(z, p["z_knots"], p["phi"])
    phi2 = np.interp(z, p["z_knots"], p["phi2"])
    st = np.interp(z, p["z_knots"], p["sigma_trans"])
    st2 = np.interp(z, p["z_knots"], p["sigma_trans2"])
    so = np.interp(z, p["z_knots"], p["sigma_obs"])
    a = float(np.median(2 * sp**2 * (1 - (1 - kap)**H) / (kap * (2 - kap) * H)))
    S0 = float(np.median([slope(p["w0"], K / d) for d in (16, 8, 4, 2, 1)]))
    missing = 1 - saved["universe"]["rows"] / (saved["universe"]["identities"] * saved["T"])
    if not (a > 0 and S0 > 0 and 0 <= missing < 1):
        raise ValueError("Invalid marginal calibration")
    return dict(config=config, p=p, a=a, S0=S0, missing=missing, absence=absence,
                walk_scale=float(np.sqrt(a / np.median(sp**2))),
                kappa=float(np.median(kap)), phi=float(np.median(phi)),
                phi2=float(np.median(phi2)),
                passing_var=float(np.median(st**2/(1-phi**2) + st2**2/(1-phi2**2) + so**2)))


class Process:
    """One process with separate streams and a resumable future state."""

    def __init__(self, calibration, rival, seed):
        if rival not in RIVALS:
            raise ValueError(rival)
        self.c = c = calibration
        self.rival = rival
        self.seed = seed
        if rival == "home":
            self.frozen = FrozenProcess(calibration, seed)
            self.n = self.frozen.n
            return
        self.p = p = c["p"]
        self.n = n = c["config"]["K"] * (16 if rival == "replacement" else 8)
        self.t = 0
        self.rng = [np.random.default_rng(s) for s in np.random.SeedSequence(seed).spawn(12)]
        self.home = extend_homes(p["w0"], n)
        self.level = self.home.copy()
        self.ids = np.arange(n, dtype=np.int64)
        self.next_id = n
        self.initial_rank = np.arange(1, n + 1)
        self.absence_probability = missing_probabilities(c, self.initial_rank)
        self.amp = np.exp(-p["temper_s"]**2/4 + p["temper_s"]*self.rng[0].normal(size=n)/2)
        b, s = p["mix_b"], p["temper_s"]
        self.perm_amp = self.amp**b * np.exp(-(b*b-b)*s*s/4)
        self.phase = self.rng[8].uniform(0, 2*np.pi, n)
        self.fast = np.zeros(n)
        self.medium = np.zeros(n)
        self.extra = np.zeros(n)
        self.equal_slow = np.zeros(n)
        self.equal_fast = np.zeros(n)
        self.common = 0.
        self.fixed = self.coefficients(self.initial_rank)
        self.rho_s = 1-c["kappa"]
        self.equal_var = c["a"]*c["config"]["block"]/(2*(1-self.rho_s**c["config"]["block"]))
        self.extra_rho = .98 if rival == "rw_persistent" else c["phi2"]
        self.extra = np.sqrt(c["passing_var"])*self.rng[7].normal(size=n)
        self.equal_slow = np.sqrt(self.equal_var)*self.rng[9].normal(size=n)
        self.equal_fast = np.sqrt(self.equal_var)*self.rng[10].normal(size=n)
        if rival == "position_ou":
            normal_ladder = ndtri(1-(np.arange(n)+.5)/n)
            scale = c["S0"]/np.median([slope(normal_ladder, c["config"]["K"]/d)
                                      for d in (16, 8, 4, 2, 1)])
            self.ou_var = float(scale**2)
            frac = c["a"]*c["config"]["block"]/(2*self.ou_var)
            if not 0 < frac < 1:
                raise ValueError("Plain OU marginal moment match infeasible")
            self.ou_rho = (1-frac)**(1/c["config"]["block"])
            self.ou_centre = float(np.mean(self.home)-scale*np.mean(normal_ladder))
            self.level = self.ou_centre + scale*normal_ladder
        # Passing states only: the design does not burn away the initial ladder.
        for _ in range(400):
            self._passing()

    def coefficients(self, ranks):
        z = np.log(np.maximum((ranks-.5)/self.p["N"], 1e-12))
        return {name: np.interp(z, self.p["z_knots"], self.p[name]) for name in
                ("kappa_z", "sigma_perm", "phi", "sigma_trans", "phi2", "sigma_trans2",
                 "sigma_obs", "exit_rate", "lam")}

    def _passing(self):
        p, n, q = self.p, self.n, self.fixed
        z = self.rng[2].normal(size=n)
        if np.isfinite(p["t_df"]):
            z *= np.sqrt((p["t_df"]-2)/self.rng[2].chisquare(p["t_df"], n))
        self.fast = q["phi"]*self.fast + q["sigma_trans"]*self.amp*z
        self.medium = q["phi2"]*self.medium + q["sigma_trans2"]*self.amp*self.rng[3].normal(size=n)
        rho = self.p["factor_rho"]
        if rho is not None:
            self.common = rho*self.common + self.rng[5].normal()*p["sigma_F"]*np.sqrt((1+rho)/2)

    def step(self):
        c, p, n, rival = self.c, self.p, self.n, self.rival
        ranks = np.empty(n, dtype=np.int64)
        ranks[np.argsort(-self.level, kind="stable")] = np.arange(1, n+1)
        q = self.coefficients(ranks) if rival in ("home", "unstabilized") else self.fixed
        z = self.rng[1].normal(size=n)
        replace = np.zeros(n, bool)
        if rival == "home":
            self.level = self.home + (1-q["kappa_z"])*(self.level-self.home) + q["sigma_perm"]*self.perm_amp*z
            replace = self.rng[6].random(n) < q["exit_rate"]
        elif rival in ("fixed_walk", "unstabilized", "rw_medium", "rw_persistent"):
            self.level += c["walk_scale"]*q["sigma_perm"]*z
        elif rival == "replacement":
            delta = c["a"]/c["S0"]**2
            mu = delta*c["S0"] - c["a"]/(2*c["S0"])
            self.level += mu + np.sqrt(c["a"])*z
            replace = self.rng[6].random(n) < -np.expm1(-delta)
        elif rival == "rank_drift":
            g = c["a"]/(2*c["S0"])
            self.level += np.where(ranks <= .9*n, -g, 9*g) + np.sqrt(c["a"])*z
        elif rival == "position_ou":
            self.level = self.ou_centre + self.ou_rho*(self.level-self.ou_centre) + np.sqrt(self.ou_var*(1-self.ou_rho**2))*z
        elif rival == "equal_components":
            self.equal_slow = self.rho_s*self.equal_slow + np.sqrt(self.equal_var*(1-self.rho_s**2))*z
            rho_f = c["phi"]
            self.equal_fast = rho_f*self.equal_fast + np.sqrt(self.equal_var*(1-rho_f**2))*self.rng[10].normal(size=n)
            self.level = self.home + self.equal_slow + self.equal_fast
        # fixed_static retains its inherited offsets, without any lasting innovation.
        if replace.any():
            m = int(replace.sum())
            if rival == "home":
                born = self.rng[11].choice(p["bottom_mu"], m)
            else:
                born = np.full(m, float(extend_homes(p["w0"], n)[-1]))
            self.home[replace] = born
            self.level[replace] = born
            self.ids[replace] = np.arange(self.next_id, self.next_id+m)
            self.next_id += m
            self.fast[replace] = self.medium[replace] = 0.
            self.amp[replace] = np.exp(-p["temper_s"]**2/4 + p["temper_s"]*self.rng[0].normal(size=m)/2)
            b, s = p["mix_b"], p["temper_s"]
            self.perm_amp[replace] = self.amp[replace]**b*np.exp(-(b*b-b)*s*s/4)
            self.phase[replace] = self.rng[8].uniform(0, 2*np.pi, m)
            bottom_coeff = self.coefficients(np.full(m, n))
            for key in self.fixed:
                self.fixed[key][replace] = bottom_coeff[key]
            self.absence_probability[replace] = missing_probabilities(c, np.full(m, n))
        self._passing()
        nuisance = self.fast + self.medium + self.fixed["sigma_obs"]*self.amp*self.rng[4].normal(size=n)
        nuisance += self.fixed["lam"]*self.common
        if c["config"]["system"] == "reddit":
            nuisance += .5*self.amp*np.sin(2*np.pi*self.t/52 + self.phase)
        if rival in ("rw_medium", "rw_persistent"):
            self.extra = self.extra_rho*self.extra + np.sqrt(c["passing_var"]*(1-self.extra_rho**2))*self.rng[7].normal(size=n)
            nuisance += self.extra
        present = self.rng[8].random(n) >= self.absence_probability
        self.t += 1
        return (np.where(present, self.level+nuisance, np.nan),
                np.where(present, self.level, np.nan), self.ids.copy())

    def generate(self, weeks):
        if self.rival == "home":
            return self.frozen.generate(weeks)
        values = np.empty((weeks, self.n))
        latent = np.empty_like(values)
        ids = np.empty((weeks, self.n), np.int64)
        for t in range(weeks):
            values[t], latent[t], ids[t] = self.step()
        return dict(values=values, latent=latent, ids=ids)


class FrozenProcess:
    """Existing V2 kernel, with IID absence and the declared annual overlay.

    Replaying from the seed after a forecast is frozen gives the identical
    prefix and then the new target block. No future values exist on the
    first call. This retains the frozen burn-in and random-number sequence.
    """

    def __init__(self, calibration, seed):
        self.c, self.seed, self.t = calibration, seed, 0
        self.n = 8*calibration["config"]["K"]
        self.p = copy.deepcopy(calibration["p"])
        self.p["z_knots"] += np.log(self.p["N"]/self.n)
        self.p["N"] = self.n

    def generate(self, weeks):
        from .balance_rivals_frozen import simulate_with_layers
        total = self.t+weeks
        raw = simulate_with_layers(self.p, total, self.seed, track=0,
                                   top_record=self.c["config"]["K"], return_full=True)
        X, L, ids = raw["full_values"], raw["full_latent"], raw["full_ids"]
        streams = np.random.SeedSequence(self.seed).spawn(15)
        phase = np.random.default_rng(streams[12]).uniform(0, 2*np.pi, int(ids.max())+1)
        if self.c["config"]["system"] == "reddit":
            X += .5*raw["full_amp"]*np.sin(2*np.pi*np.arange(total)[:, None]/52+phase[ids])
        # Rank profile is fixed at initial identity rank; births enter below the reservoir.
        initial_rank = np.where(ids < self.n, ids+1, self.n)
        present = np.random.default_rng(streams[13]).random(X.shape) >= missing_probabilities(self.c, initial_rank)
        path = dict(values=np.where(present[self.t:], X[self.t:], np.nan),
                    latent=np.where(present[self.t:], L[self.t:], np.nan), ids=ids[self.t:].copy())
        self.t = total
        return path


def missing_probabilities(calibration, ranks):
    profile = calibration.get("absence")
    if profile is None:  # small unit fixtures only; native runner requires a measured profile
        return np.full(np.shape(ranks), calibration["missing"])
    rows = profile["rows"]
    return np.interp(np.log(np.asarray(ranks)), np.log([r["rank"] for r in rows]),
                     [r["probability"] for r in rows])


def long_frame(path):
    """Rank ALL present endpoints first; numerical ids, no object-string column."""
    X, ids = path["values"], path["ids"]
    T, n = X.shape
    rows = []
    for t in range(T):
        present = np.flatnonzero(np.isfinite(X[t]))
        order = present[np.argsort(-X[t, present], kind="stable")]
        rows.append(pd.DataFrame(dict(entity_id=ids[t, order], period=np.full(len(order), t, np.int32),
                                      rank=np.arange(1, len(order)+1, dtype=np.int32), X=X[t, order],
                                      N=np.full(len(order), len(order), np.int32))))
    return pd.concat(rows, ignore_index=True)


def aligned_layers(path, ids):
    """Identity alignment, including deaths; latent shares the observation mask."""
    order = np.asarray(ids)
    X = np.full((path["values"].shape[0], len(order)), np.nan)
    L = np.full_like(X, np.nan)
    for t, named in enumerate(path["ids"]):
        col = np.searchsorted(order, named)
        ok = col < len(order)
        ok[ok] &= order[col[ok]] == named[ok]
        X[t, col[ok]] = path["values"][t, ok]
        L[t, col[ok]] = path["latent"][t, ok]
    return X, L
