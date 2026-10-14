"""Simulation check of an external (GPT-6 pro, owner-supplied) count-only test of
fixed volatility types against volatility that responds to level.

Model as in that result: X log level, dX = (b + mu(X)) dt + sqrt(alpha + beta tanh X) dW;
Z an OU passing swing (optionally plus decaying positive jumps); daily counts
C_d ~ Poisson(100 * integral_day exp(X+Z)). Scores: U_g = prod_d f_{g w_d}(C_d),
f_t(c) = Gamma(c+1)/Gamma(c+1-i t), w = (1,-2,1)/L over three blocks of L=121 days;
S_A(m) = Im(e^{-im} U_1), S_B(m) = Im(e^{-3im} U_3) - 1.5 Im(e^{-im} U_1).
Reports the mean scores against the null, an oracle z-test with the phase known,
and the result's betting test (bias allowances, 32 stakes, phase profiled over
+-m_max, reject when min_m E > 20). Synthetic only; no data file is opened.
Usage: python sim_power.py [sets] [scenario ...]   (independent sets of 10,000 names per
scenario, default 20; named scenarios only, merged into sim_power.json)"""
import sys, json, time
import numpy as np
from scipy.special import loggamma, logsumexp

L, SUB = 121, 8
H = 3 * L
W = np.r_[np.full(L, 1 / L), np.full(L, -2 / L), np.full(L, 1 / L)]
EPS = {"A": 0.000284901, "B": 0.001838127}          # null bias bounds, T=365
R = {"A": 1.041624, "B": 3.005587}                  # score ranges, T=365
MMAX = 0.043778
MGRID = np.linspace(-MMAX, MMAX, 41)


def simulate(n, rng, beta=0.0, mu=None, v=0.09, kappa=1 / 30, spikes=None, arcs=None):
    alpha = rng.uniform(0.001, 0.0015, n)
    b = rng.uniform(-0.001, 0.001, n)
    curv = np.zeros(n)
    if arcs:                                         # concave life-cycle arcs on a share of names
        on = rng.random(n) < arcs["share"]
        curv[on] = rng.uniform(0, arcs["max"], on.sum())
    x = rng.uniform(-1, 1, n)
    vo = v - (spikes["var"] if spikes else 0.0)
    z = rng.normal(0, np.sqrt(vo), n)
    s = np.zeros(n)
    dt = 1 / SUB
    if spikes:                                       # burn the shot noise in
        for _ in range(200 * SUB):
            s *= np.exp(-spikes["kappa"] * dt)
            hit = rng.random(n) < spikes["rate"] * dt
            s[hit] += rng.exponential(spikes["mean"], hit.sum())
    az, sz = np.exp(-kappa * dt), np.sqrt(vo * (1 - np.exp(-2 * kappa * dt)))
    logU = {1: np.zeros(n, complex), 3: np.zeros(n, complex)}
    for d in range(H):
        acc = np.zeros(n)
        for _ in range(SUB):
            var = np.maximum(alpha + beta * np.tanh(x), 0.0)
            drift = b - curv * (d + 0.5) + (mu(x) if mu else 0.0)
            x = x + drift * dt + np.sqrt(var * dt) * rng.standard_normal(n)
            z = z * az + sz * rng.standard_normal(n)
            if spikes:
                s *= np.exp(-spikes["kappa"] * dt)
                hit = rng.random(n) < spikes["rate"] * dt
                s[hit] += rng.exponential(spikes["mean"], hit.sum())
            acc += np.exp(x + z + s)
        c = rng.poisson(100 * acc / SUB)
        for g in (1, 3):
            t = g * W[d]
            logU[g] += loggamma(c + 1.0) - loggamma(c + 1.0 - 1j * t)
    return np.exp(logU[1]), np.exp(logU[3])


def scores(u1, u3, m):
    sa = np.imag(np.exp(-1j * m) * u1)
    sb = np.imag(np.exp(-3j * m) * u3) - 1.5 * sa
    return sa, sb


def log_factor_matrix(u1, u3):
    cols = []
    for m in MGRID:
        sa, sb = scores(u1, u3, m)
        for r, sr in (("A", sa), ("B", sb)):
            for sg in (-1, 1):
                for j in range(8):
                    eta = 2.0 ** -j / (2 * (R[r] + EPS[r]))
                    cols.append(np.log1p(eta * (sg * sr - EPS[r])))
    return np.array(cols, dtype=np.float32).T.reshape(len(u1), len(MGRID), 32)


def betting_rejects(lf):
    """The result's test on one set of names: reject when min over the phase grid of E exceeds 20."""
    tot = lf.sum(axis=0, dtype=np.float64)            # (m, 32)
    logE = logsumexp(tot, axis=1) - np.log(32)
    return bool(logE.min() > np.log(20)), bool(logE[len(MGRID) // 2] > np.log(20))


SCEN = {
    "null": dict(),
    "alt_beta_-0.0004": dict(beta=-0.0004),
    "alt_beta_-0.0001": dict(beta=-0.0001),
    "alt_beta_-0.0004_quiet": dict(beta=-0.0004, v=0.02, kappa=1 / 7),
    "null_quiet": dict(v=0.02, kappa=1 / 7),
    "null_spikes": dict(spikes=dict(rate=1 / 60, mean=0.5, kappa=1 / 7, var=(1 / 60) * 0.5 / (2 / 7))),
    "null_drift_by_level_strong": dict(mu=lambda x: -0.0003 * np.tanh(x)),
    "null_drift_by_level_measured": dict(mu=lambda x: -0.00005 * x),
    # added after the first eight had run: the quiet case, where the test has power
    "alt_beta_-0.0001_quiet": dict(beta=-0.0001, v=0.02, kappa=1 / 7),
    "null_quiet_drift_by_level_strong": dict(v=0.02, kappa=1 / 7, mu=lambda x: -0.0003 * np.tanh(x)),
    "null_quiet_spikes": dict(v=0.02, kappa=1 / 7, spikes=dict(rate=1 / 60, mean=0.25, kappa=1 / 7, var=(1 / 60) * 2 * 0.25 ** 2 / (2 / 7))),
    "null_quiet_arcs": dict(v=0.02, kappa=1 / 7, arcs=dict(share=0.3, max=1.4e-5)),
    "null_quiet_slow_spikes": dict(v=0.02, kappa=1 / 7, spikes=dict(rate=1 / 180, mean=0.2, kappa=1 / 60, var=(1 / 180) * 2 * 0.2 ** 2 / (2 / 60))),
}
SETS, SET_N = 20, 10000

if __name__ == "__main__":
    sets = int(sys.argv[1]) if len(sys.argv) > 1 else SETS
    only = sys.argv[2:]
    rng = np.random.default_rng(20261014)
    out = {}
    for name, kw in SCEN.items():
        if only and name not in only:
            continue
        t0 = time.time()
        sa_all, sb_all, rej, rej_known, shiftA, shiftB = [], [], [], [], [], []
        lfs = []
        for _ in range(sets):
            u1, u3 = simulate(SET_N, rng, **kw)
            sa, sb = scores(u1, u3, 0.0)
            sa_all.append(sa); sb_all.append(sb)
            sam, sbm = scores(u1, u3, MMAX)
            shiftA.append(sam.mean() - sa.mean()); shiftB.append(sbm.mean() - sb.mean())
            lf = log_factor_matrix(u1, u3)
            a, b = betting_rejects(lf); rej.append(a); rej_known.append(b)
            lfs.append(lf.sum(axis=0, dtype=np.float64))
        # 40,000-name sets from four disjoint 10,000-name sets
        rej40 = []
        for q in range(0, sets - sets % 4, 4):
            tot = sum(lfs[q:q + 4]); logE = logsumexp(tot, axis=1) - np.log(32)
            rej40.append(bool(logE.min() > np.log(20)))
        sa, sb = np.concatenate(sa_all), np.concatenate(sb_all)
        res = dict(names=len(sa))
        for r, sr in (("A", sa), ("B", sb)):
            mean, sd = sr.mean(), sr.std(ddof=1)
            res[r] = dict(mean=mean, se=sd / np.sqrt(len(sr)), sd=sd,
                          n_for_80pct_oracle=((1.645 + 0.842) * sd / mean) ** 2 if mean != 0 else None)
        res["mean_shift_at_mmax"] = dict(A=float(np.mean(shiftA)), B=float(np.mean(shiftB)))
        res["reject_rate_10k_profiled"] = float(np.mean(rej))
        res["reject_rate_10k_phase_known"] = float(np.mean(rej_known))
        res["reject_rate_40k_profiled"] = float(np.mean(rej40)) if rej40 else None
        res["sets_10k"], res["sets_40k"] = sets, len(rej40)
        res["seconds"] = round(time.time() - t0, 1)
        out[name] = res
        print(name, json.dumps(res, default=float), flush=True)
    path = __file__.replace("sim_power.py", "sim_power.json")
    if only:
        try:
            out = {**json.load(open(path)), **out}
        except FileNotFoundError:
            pass
    json.dump(out, open(path, "w"), indent=1, default=float)
