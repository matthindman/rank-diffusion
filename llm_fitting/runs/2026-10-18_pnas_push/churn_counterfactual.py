"""A3 of PLAN.md: churn creates and maintains the hierarchy (the moving-home model's lasting core alone).

sigma_L(z) estimated on the whole panel; drift by rank from B's measured slides (the 'move' variant of 2z-dd,
so the ladder is not an input); start from the filtered levels at the end of block 0.  Worlds:
  (i) churn + slide; (ii) slide without churn (sigma_L = 0); (iii) churn without slide (drift 0), 104 weeks,
  20 seeds; (iv) churn + slide from a flat start (every level at the median), 30 years, 10 seeds.
shape_k = U_k - U_K (U_k the mean of the top k, K the panel's universe size); the slide of the names above k
against U_K is measured inside the simulation, c_k = sigma_k^2 / (2 S_k) from the simulation's own sigma_L at the
rank-k level and the local slope.

    python llm_fitting/runs/2026-10-18_pnas_push/churn_counterfactual.py <panel>
"""
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
import minimal_rankdiff as mrd  # noqa: E402
import mts_step1 as m1  # noqa: E402
import rankdiff_kalman as rk  # noqa: E402
import rankdiff_rdm as rdm  # noqa: E402

panel = sys.argv[1]
FLAT_ONLY = len(sys.argv) > 2 and sys.argv[2] == "flat20"           # R3-C: the flat start alone, 20 seeds, with bands
K = m1.PANELS[panel]["K"]
EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True,
           mix_hetero=panel != "reddit_submissions_long")
df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]]), K, buffer_mult=4)
T = int(df["period"].max()) + 1
BLOCK = 52
t0 = T - (T // BLOCK) * BLOCK + BLOCK
base = mrd.estimate(df, **EST)
law = json.loads((ROOT / "llm_fitting/runs/2026-10-14_balance_law/cells" / f"law__{panel}.json").read_text())
prs = [q for q in law["pairs"] if q["primary"]]
kk = np.array(sorted(r["k"] for r in prs[0]["rows"]), float)
net = np.mean([[r["sbar"] - r["entry"] for r in sorted(q["rows"], key=lambda r: r["k"])] for q in prs], axis=0)
kk = np.concatenate([[1.0], kk, [4.0 * K]])
net = np.concatenate([[net[0]], net, [net[-1]]])
p = rdm.estimate_rdm(df, K, base=base, jumps=False, variant="move", slides=(kk, net))
L0 = rk._filtered_levels(df[df["period"] < t0].copy(), base).to_numpy()
N = L0.size
KS = [K // 64 * 2 ** i for i in range(6)]
print(f"{panel}: N {N}, K {K}; sigma0 {p.sigma0:.4f}, beta {p.beta:+.3f}; drift at ranks 1/20/100/1000/K "
      f"{np.round(np.interp(np.log((np.array([1, 20, 100, 1000, K]) - 0.5) / N), p.drift_z, p.drift_g), 4).tolist()}", flush=True)


def shape(L):
    s = np.sort(L)[::-1]
    UK = s[:K].mean()
    return np.array([s[:k].mean() - UK for k in KS]), s


def slope_at(s, k):
    lo, hi = int(np.floor(k / np.sqrt(2))), int(np.ceil(k * np.sqrt(2)))
    r = np.arange(lo, hi + 1)
    return float(-np.polyfit(np.log(r), s[lo - 1:hi], 1)[0])


def run(q, Lstart, weeks, seed, record_every=4):
    st = rdm._State(q, Lstart, np.random.default_rng(seed), lasting="rdm")
    path = [shape(st.L)[0]]
    slide = np.zeros(len(KS))
    n_sl = 0
    for t in range(weeks):
        Lb = st.L.copy()
        ob = np.argsort(-Lb)
        UKb = Lb[ob[:K]].mean()
        ids_b = st.ids.copy()
        st.step_lasting()
        if t >= weeks - 52:                       # the slide of the names above k against U_K, last year
            UKa = np.sort(st.L)[::-1][:K].mean()
            same = st.ids == ids_b
            for j, k in enumerate(KS):
                top = ob[:k]
                top = top[same[top]]
                slide[j] += -((st.L[top] - Lb[top]).mean() - (UKa - UKb))
            n_sl += 1
        if (t + 1) % record_every == 0:
            path.append(shape(st.L)[0])
    s_end = np.sort(st.L)[::-1]
    S_end = np.array([slope_at(s_end, k) for k in KS])
    sig2 = np.array([rdm._sigma_L(q, s_end[k - 1]) ** 2 for k in KS])
    return np.array(path), slide / max(n_sl, 1), S_end, sig2


worlds = {} if FLAT_ONLY else {
    "churn_slide": p,
    "slide_only": replace(p, sigma0=0.0),
    "churn_only": replace(p, drift_g=np.zeros_like(p.drift_g)),
}
out = dict(panel=panel, K=K, N=N, ks=KS, sigma0=p.sigma0, beta=p.beta)
for name, q in worlds.items():
    runs = [run(q, L0, 104, s) for s in range(20)]
    paths = np.array([r[0] for r in runs])
    rate = (paths[:, -1] - paths[:, 0]) / 2.0                  # per year
    sl = np.mean([r[1] for r in runs], axis=0)
    S_end = np.mean([r[2] for r in runs], axis=0)
    sig2 = np.mean([r[3] for r in runs], axis=0)
    S0 = np.array([slope_at(np.sort(L0)[::-1], k) for k in KS])
    c = sig2 / (2 * S0)
    out[name] = dict(rate_per_year=rate.mean(0).tolist(), rate_q=[np.quantile(rate, 0.05, 0).tolist(), np.quantile(rate, 0.95, 0).tolist()],
                     slide_week=sl.tolist(), c_week=c.tolist(), pred_rate_per_year=((c - sl) * 52).tolist(), S_start=S0.tolist(), S_end=S_end.tolist())
    print(f"  {name}: shape change per year at k={KS}: {np.round(rate.mean(0), 3).tolist()}; predicted (c - slide) x 52: "
          f"{np.round((c - sl) * 52, 3).tolist()}; slope start {np.round(S0, 2).tolist()} end {np.round(S_end, 2).tolist()}", flush=True)
# (iv) flat start, 30 years
flat = np.full(N, float(np.median(L0)))
runs = [run(p, flat, 30 * 52, s, record_every=52) for s in range(20 if FLAT_ONLY else 10)]
S_end = np.mean([r[2] for r in runs], axis=0)
sl = np.mean([r[1] for r in runs], axis=0)
sig2 = np.mean([r[3] for r in runs], axis=0)
S_star = sig2 / (2 * np.maximum(sl, 1e-9))
S_obs = np.array([slope_at(np.sort(L0)[::-1], k) for k in KS])
traj = np.mean([r[0] for r in runs], axis=0)
out["flat_start"] = dict(S_end=S_end.tolist(), S_star_sim=S_star.tolist(), S_observed=S_obs.tolist(), slide_week=sl.tolist(),
                         shape_by_year=traj.tolist(), n_seeds=len(runs),
                         S_end_q=[np.quantile([r[2] for r in runs], q, axis=0).tolist() for q in (0.05, 0.95)],
                         shape_q=[np.quantile([r[0] for r in runs], q, axis=0).tolist() for q in (0.05, 0.95)])
print(f"  flat start, 30 years: slope at k={KS}: {np.round(S_end, 2).tolist()}; sigma^2/(2 slide) in the simulation "
      f"{np.round(S_star, 2).tolist()}; observed ladder {np.round(S_obs, 2).tolist()}", flush=True)
print(f"  flat start: shape at K/64 by year {np.round(traj[:, 0], 2).tolist()}", flush=True)
Path(__file__).with_name(f"churn_counterfactual__{panel}{'__flat20' if FLAT_ONLY else ''}.json").write_text(json.dumps(out, indent=1, default=float))
