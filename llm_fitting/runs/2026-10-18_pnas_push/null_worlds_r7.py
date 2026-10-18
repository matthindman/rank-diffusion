"""R7 of PLAN.md: null worlds at 20 seeds, calibrated worlds from the model, beta calibration, Gomez noise baseline.

    python llm_fitting/runs/2026-10-18_pnas_push/null_worlds_r7.py <gibrat|atlas|neutral|rdm|sb_data|firm_data|beta05|beta10> <seed_from> <seed_to>

sb_data / firm_data / beta05 / beta10: the moving-home model (movinghome_vp) estimated on reddit_submissions_long, with
planted beta (0, 0.2, 0.05, 0.10) and no drift by rank, started from the data's filtered ladder (weeks < 30), with the
model's passing parts and exits.  Within-seed bootstraps are cut to 5 (bands come from the seeds).  Output:
null_world_r7__<world>__s<seed>.json.
"""
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import null_worlds as nw  # noqa: E402

_mb, _bl = nw.mb.analyse, nw.bl.analyse
nw.mb.analyse = lambda *a, **k: _mb(*a, **{**k, "boot": 5})
nw.bl.analyse = lambda *a, **k: _bl(*a, **{**k, "boot": 5})
K, T = nw.K, nw.T


def gomez(df):
    """Exact Gomez decomposition of the top-k arithmetic share (current-year and previous-year cohorts)."""
    X = nw.m1.matrices(df, K)[0]
    nb = T // 52
    start = T - nb * 52
    W = np.nan_to_num(np.expm1(X), nan=0.0)
    tots = [W[start + b * 52:start + (b + 1) * 52].sum(0) for b in range(nb)]
    out = []
    for k in (K // 64, K // 16):
        for a in range(nb - 1):
            ta, tb = tots[a], tots[a + 1]
            oa, ob = np.argsort(-ta, kind="stable"), np.argsort(-tb, kind="stable")
            Da, Db = ta[oa[:K]].sum(), tb[ob[:K]].sum()
            Wb = tb[ob[:k]].sum()
            g = Db / Da
            cohorts = dict(current=oa[:k])
            if a >= 1:
                cohorts["prior"] = np.argsort(-tots[a - 1], kind="stable")[:k]
            row = dict(k=int(k), pair=[a, a + 1], share_change=float((Wb / Db) / (ta[oa[:k]].sum() / Da) - 1))
            for name, P in cohorts.items():
                Wa = ta[P].sum()
                row[f"{name}_within"] = float((tb[P].sum() / Wa - g) / g)
                row[f"{name}_between"] = float(((Wb - tb[P].sum()) / Wa) / g)
            out.append(row)
    return out


which, s0, s1 = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
if which in ("rdm", "sb_data", "firm_data", "beta05", "beta10"):
    import rankdiff_kalman as rk
    import rankdiff_rdm as rdm
    EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True, mix_hetero=False)
    df0 = nw.mrd.restrict_universe(nw.mrd.load_panel(nw.mrd.PLATFORMS["reddit_submissions_long"]), K, buffer_mult=4)
    base = nw.mrd.estimate(df0, **EST)
    p = rdm.estimate_rdm(df0, K, base=base, jumps=False, variant="hold")
    L0 = rk._filtered_levels(df0[df0["period"] < 30].copy(), base).to_numpy()
    del df0
    planted = dict(sb_data=0.0, firm_data=0.2, beta05=0.05, beta10=0.10)
    if which != "rdm":
        p = replace(p, beta=planted[which], drift_g=np.zeros_like(p.drift_g))
    print(f"{which}: estimated lasting beta (structure function) on the data; world beta {p.beta:+.3f}, sigma0 {p.sigma0:.4f}", flush=True)
for seed in range(s0, s1 + 1):
    if which == "neutral":
        _, rows = nw.world_neutral(seed)
        df = nw.frame_from_rows(rows)
    elif which in ("gibrat", "atlas"):
        X, ids = {"gibrat": nw.world_gibrat, "atlas": nw.world_atlas}[which](seed)
        df = nw.frame_from_matrix(X, ids)
    else:
        Xs, ids = rdm.simulate_panel(p, L0, T, seed=seed, lasting="movinghome_vp")
        df = nw.frame_from_matrix(Xs, ids)
    res = nw.stats(df, which)
    res["seed"] = seed
    res["gomez"] = gomez(df)
    if which in ("rdm", "sb_data", "firm_data", "beta05", "beta10"):
        res["world_beta"] = float(p.beta)
    (HERE / f"null_world_r7__{which}__s{seed}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"{which} seed {seed}: turnover {[(k, round(v, 3)) for k, v in res['turnover'].items()]}; beta {res['beta']:.3f}; "
          f"gapped {res.get('gapped')}; boundary {res.get('boundary_pooled')}; law c/s "
          f"{(res['law']['mean_c'] / res['law']['mean_sbar']) if 'law' in res else None}", flush=True)
