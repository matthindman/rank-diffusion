"""R17-C scoring (PLAN.md addendum R17; written before the outputs of `r17_round8.py durability2` were read).

For each world (control_num, joint) and arm (head_step: -0.2 a year in the current top K/64's decline; sigma_step: lasting
volatility x1.2 at every rank), the response is the arm minus the control on common seeds, by 52-week block, at K/64.
  lasting shape:  fitted by  A (1 - exp(-t / tau))  on block means (A free: the step's long-run effect; no lag)
  observed shape: the same form passed through a first-order lag whose half-life is fitted too (A, tau, lag free)
  membership:     e-folding  -1 / ln(retention of the block-0 top K/64 after one year), control arm
tau's band: the 5-95% range of the fit to each seed's own response.

    python r17_durability_fit.py
"""
import json
from pathlib import Path

import numpy as np
from scipy.optimize import least_squares

HERE = Path(__file__).resolve().parent


def model(t_grid, A, tau, lag=None):
    r = A * (1 - np.exp(-t_grid / tau))
    if lag is None:
        return r
    k = np.log(2) / lag
    o = np.zeros_like(r)
    for i in range(1, t_grid.size):
        o[i] = o[i - 1] + k * (r[i - 1] - o[i - 1]) * (t_grid[i] - t_grid[i - 1])
    return o


def blocks(x, nb):
    t = np.linspace(0, nb, 200 * nb + 1)
    return t, lambda r: np.array([r[(t >= b) & (t < b + 1)].mean() for b in range(nb)])


def fit(resp, lagged):
    nb = len(resp)
    t, bm = blocks(None, nb)
    A0 = resp[-1] * 1.5 if resp[-1] != 0 else -0.5
    if lagged:
        f = lambda p: bm(model(t, p[0], p[1], p[2])) - resp
        r = least_squares(f, [A0, 4.0, 1.0], bounds=([-20, 0.2, 0.05], [20, 100, 10]))
        return dict(A=float(r.x[0]), tau=float(r.x[1]), lag=float(r.x[2]), sse=float(np.sum(r.fun ** 2)))
    f = lambda p: bm(model(t, p[0], p[1])) - resp
    r = least_squares(f, [A0, 4.0], bounds=([-20, 0.2], [20, 100]))
    return dict(A=float(r.x[0]), tau=float(r.x[1]), sse=float(np.sum(r.fun ** 2)))


def main():
    out = {}
    for f in sorted(HERE.glob("r17_durability__*__*.json")):
        d = json.loads(f.read_text())
        key = f"{d['panel']}__{d['world']}"
        rows = d["rows"]
        ret1 = np.mean([r["control"]["retention"][0][0] for r in rows])
        T_mem = -1 / np.log(ret1)
        res = dict(T_mem=float(T_mem), retention_by_year=np.mean([np.array(r["control"]["retention"])[:, 0] for r in rows], 0).tolist())
        for arm in ("head_step", "sigma_step"):
            obs = np.array([np.array(r[arm]["shape"])[:, 0] - np.array(r["control"]["shape"])[:, 0] for r in rows])
            las = np.array([np.array(r[arm]["lasting"])[:, 0] - np.array(r["control"]["lasting"])[:, 0] for r in rows])
            fl, fo = fit(las.mean(0), False), fit(obs.mean(0), True)
            seed_tau = [fit(x, False)["tau"] for x in las]
            res[arm] = dict(observed=obs.mean(0).tolist(), lasting=las.mean(0).tolist(), fit_lasting=fl, fit_observed=fo,
                            tau_lasting_band=[float(np.quantile(seed_tau, 0.05)), float(np.quantile(seed_tau, 0.95))],
                            ratio_lasting=float(fl["tau"] / T_mem), ratio_observed=float(fo["tau"] / T_mem))
            print(f"{key} {arm}: lasting tau {fl['tau']:.2f} [{res[arm]['tau_lasting_band'][0]:.2f}, {res[arm]['tau_lasting_band'][1]:.2f}] "
                  f"(A {fl['A']:+.2f}); observed tau {fo['tau']:.2f}, lag {fo['lag']:.2f} yr; membership {T_mem:.2f} yr; "
                  f"ratio {res[arm]['ratio_lasting']:.2f}", flush=True)
        out[key] = res
    (HERE / "r17_durability_scores.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
