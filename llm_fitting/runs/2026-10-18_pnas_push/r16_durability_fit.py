"""R16-F scoring: the response time of the shape to a step in the head's decline, fitted to the saved block responses
(`r16_durability__<panel>.json`), registered form and the post hoc lag-aware form (RESULTS R16-F).  Deposited
2026-10-21 after referee round 8 (H12): the fit had been run inline; this reproduces `r16_durability_scores.json`.

  registered:  block means over years b..b+1 of  -D tau (1 - exp(-t / tau))
  lag-aware:   the same lasting response passed through a first-order lag whose half-life is the model's median
               passing-swing half-life (69 weeks submissions, 34 comments), Euler steps on a grid of 0.001 years
  membership:  e-folding -1 / ln(retention of the block-0 top K/64 after one year)

    python r16_durability_fit.py [--check]
"""
import json
import sys
from pathlib import Path

import numpy as np
from scipy.optimize import minimize_scalar

HERE = Path(__file__).resolve().parent
D = 0.2
LAG_WEEKS = {"reddit_submissions_long": 69, "reddit_comments_ext": 34}


def block_mean_model(tau, lag=None, years=5):
    t = np.linspace(0, years, 1000 * years + 1)
    if lag is None:
        r = -D * tau * (1 - np.exp(-t / tau))
    else:
        k = np.log(2) / lag
        rate = -D * np.exp(-t / tau)
        last = np.concatenate([[0], np.cumsum(rate[1:] * np.diff(t))])
        r = np.zeros_like(t)
        for i in range(1, t.size):
            r[i] = r[i - 1] + k * (last[i - 1] - r[i - 1]) * (t[i] - t[i - 1])
    return np.array([r[(t >= b) & (t < b + 1)].mean() for b in range(years)])


def fit(resp, lag=None):
    f = lambda tau: float(np.sum((block_mean_model(tau, lag) - resp) ** 2))
    r = minimize_scalar(f, bounds=(0.2, 200), method="bounded")
    return float(r.x), float(r.fun)


def main():
    out = {}
    for pn, lagw in LAG_WEEKS.items():
        d = json.loads((HERE / f"r16_durability__{pn}.json").read_text())
        out[pn] = {}
        for m, v in d["by_m"].items():
            resp = np.array(v["response_mean"])[:, 0]
            ret1 = np.array(v["retention_mean"])[0, 0]
            T_mem = -1 / np.log(ret1)
            tr, sr = fit(resp)
            tl, sl = fit(resp, lagw / 52)
            out[pn][m] = dict(response=np.round(resp, 3).tolist(), retention=np.round(np.array(v["retention_mean"])[:, 0], 3).tolist(),
                              T_mem=round(T_mem, 2), tau_registered=round(tr, 2), sse_registered=round(sr, 4), ratio_registered=round(tr / T_mem, 2),
                              tau_lag=round(tl, 2), sse_lag=round(sl, 4), ratio_lag=round(tl / T_mem, 2), sigma0=v["sigma0"])
    if "--check" in sys.argv:
        saved = json.loads((HERE / "r16_durability_scores.json").read_text())
        print("reproduces r16_durability_scores.json:", json.dumps(saved, sort_keys=True) == json.dumps(out, sort_keys=True))
        return
    (HERE / "r16_durability_scores.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
