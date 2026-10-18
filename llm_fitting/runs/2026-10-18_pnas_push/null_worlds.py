"""R3-A and R3-B of PLAN.md: every headline statistic in four simulated worlds, and Iniguez's rank change C(R) on
the data ladders.

Worlds (N = 24,000 names, K = 5,000, T = 212 weeks):
  gibrat   driftless random walk of the lasting level (sigma 0.10 a week); top K re-selected weekly
  atlas    firm-like: sigma(L) = 0.10 exp(-0.2 (L - L_K)), common drift -0.005, reflecting floor
  neutral  Wright-Fisher copying: 1.5e6 posts a week, innovation 0.003; 300 weeks of burn-in; activity = count
  rdm      the data-calibrated moving home (movinghome_vp, hold drift) estimated on reddit_submissions_long
Worlds gibrat and atlas start from a Zipf ladder (S = 1) with noise 0.3 and carry the same passing parts (AR(1),
half-life 5 weeks, sd 0.3; white noise sd 0.3).

    python llm_fitting/runs/2026-10-18_pnas_push/null_worlds.py <world|data>
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "llm_fitting"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import minimal_rankdiff as mrd  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_boundary_law as mb  # noqa: E402
import mts_step1 as m1  # noqa: E402

K, N, T, PRES = 5000, 24000, 212, 0.9
RANKS_C = [1, 2, 3, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]


def passing(rng, T, n):
    phi = 0.5 ** (1 / 5)
    xi = np.zeros((T, n))
    for t in range(1, T):
        xi[t] = phi * xi[t - 1] + 0.3 * np.sqrt(1 - phi * phi) * rng.standard_normal(n)
    return xi + 0.3 * rng.standard_normal((T, n))


def zipf_start(rng, n):
    return 12.0 - 1.0 * np.log(np.arange(1, n + 1)) + 0.3 * rng.standard_normal(n)


def world_gibrat(seed=0):
    rng = np.random.default_rng(seed)
    L = zipf_start(rng, N)
    path = L[None, :] + np.cumsum(0.10 * rng.standard_normal((T, N)), axis=0)
    return path + passing(rng, T, N), None


def world_atlas(seed=0):
    rng = np.random.default_rng(seed)
    L = zipf_start(rng, N)
    LK, floor = np.sort(L)[::-1][K - 1], L.min()
    out = np.empty((T, N))
    for t in range(T):
        sig = 0.10 * np.exp(-0.2 * (L - LK))
        L = np.maximum(L - 0.005 + sig * rng.standard_normal(N), floor)
        out[t] = L
    return out + passing(rng, T, N), None


def world_neutral(seed=0):
    rng = np.random.default_rng(seed)
    M, mu = 1_500_000, 0.003
    counts = np.maximum(1, (2e5 / np.arange(1, 60001) ** 1.0)).astype(np.int64)
    ids = np.arange(counts.size)
    nxt = counts.size
    rows = []
    for t in range(-300, T):
        p = counts / counts.sum()
        new = rng.multinomial(int(M * (1 - mu)), p)
        keep = new > 0
        n_new = rng.poisson(M * mu)
        counts = np.concatenate([new[keep], np.ones(n_new, np.int64)])
        ids = np.concatenate([ids[keep], np.arange(nxt, nxt + n_new)])
        nxt += n_new
        if t >= 0:
            rows.append((ids.copy(), counts.copy()))
    return None, rows


def frame_from_matrix(X, ids=None):
    """Long frame (entity_id, period, X, rank, N), truncated to ranks <= 4K + 50 with N the full count."""
    out = []
    for t in range(X.shape[0]):
        o = np.argsort(-X[t])
        keep = o[:4 * K + 50]
        out.append(pd.DataFrame(dict(entity_id=(keep if ids is None else ids[t][keep]).astype(str), period=t, X=X[t][keep],
                                     rank=np.arange(1, keep.size + 1), N=X.shape[1])))
    return pd.concat(out, ignore_index=True)


def frame_from_rows(rows):
    out = []
    for t, (ids, c) in enumerate(rows):
        o = np.argsort(-c, kind="stable")[:4 * K + 50]
        out.append(pd.DataFrame(dict(entity_id=ids[o].astype(str), period=t, X=np.log1p(c[o].astype(float)),
                                     rank=np.arange(1, o.size + 1), N=c.size)))
    return pd.concat(out, ignore_index=True)


def world_rdm(seed=0):
    import rankdiff_kalman as rk
    import rankdiff_rdm as rdm
    EST = dict(obs_frac=0.5, temper=True, min_knot_n=8, md_lags=6, t_tails=True, md_vr_long=True, mix_hetero=False)
    df = mrd.restrict_universe(mrd.load_panel(mrd.PLATFORMS["reddit_submissions_long"]), K, buffer_mult=4)
    base = mrd.estimate(df, **EST)
    p = rdm.estimate_rdm(df, K, base=base, jumps=False, variant="hold")
    L0 = rk._filtered_levels(df[df["period"] < 30].copy(), base).to_numpy()
    Xs, ids = rdm.simulate_panel(p, L0, T, seed=seed, lasting="movinghome_vp")
    return Xs, ids


def order(L):
    o = np.flatnonzero(np.isfinite(L))
    return o[np.argsort(-L[o], kind="mergesort")]


def stats(df, label):
    res = dict(world=label)
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    nb = T // 52
    start = T - nb * 52
    M = ba.block_means(X, 52, start=start, min_frac=PRES)
    # turnover by k (block 1 -> 2) and Eq. 1 (exact form)
    turn, eq1 = {}, []
    for a in range(nb - 1):
        for k in (10, 25, K // 64, K // 16, K // 4, K // 2):
            oa, ob = order(M[a]), order(M[a + 1])
            obs = 1 - np.intersect1d(oa[:k], ob[:k]).size / k
            turn.setdefault(k, []).append(obs)
            if k >= K // 64:
                La, Lb = M[a], M[a + 1]
                dth = Lb[ob[k - 1]] - La[oa[k - 1]]
                lo, hi = int(np.floor(k / np.sqrt(2))), int(np.ceil(k * np.sqrt(2)))
                S = -np.polyfit(np.log(np.arange(lo, hi + 1)), La[oa[lo - 1:hi]], 1)[0]
                v = np.mean(~np.isfinite(Lb[oa[:k]]))
                band = oa[k // 2:k]
                band = band[np.isfinite(Lb[band])]
                pred = v + (1 - v) * np.mean(1 - np.exp(-np.maximum(dth - (Lb[band] - La[band]), 0) / S))
                eq1.append(pred / obs if obs > 0 else np.nan)
    res["turnover"] = {int(k): float(np.mean(v)) for k, v in turn.items()}
    e = np.array(eq1)
    res["eq1_within"] = float(np.mean((e >= 0.8) & (e <= 1.25)))
    res["eq1_median"] = float(np.nanmedian(e))
    # beta: strata of permanent rank over block 0, change 1 -> 2
    pr, _ = ba.perm_rank(Rk, start, start + 52, floors)
    op = np.argsort(pr, kind="stable")
    edges = [K // 64 * 2 ** i for i in range(7)]
    sz, sd = [], []
    for lo, hi in zip([0] + edges[:-1], edges):
        s = op[lo:hi]
        m = s[np.isfinite(M[1][s]) & np.isfinite(M[2][s])]
        sz.append(np.mean(M[1][m]))
        sd.append(np.std(M[2][m] - M[1][m], ddof=1))
    res["beta"] = float(-np.polyfit(sz, np.log(sd), 1)[0])
    # gapped persistence (13-week windows, 52-week changes, cohort from weeks 0-25)
    def wmean(lo, hi):
        seg = X[lo:hi]
        ok = np.isfinite(seg).mean(0) >= PRES
        with np.errstate(all="ignore"):
            return np.where(ok, np.nanmean(seg, 0), np.nan)
    c0 = wmean(0, 26)
    coh = np.zeros(X.shape[1], bool)
    top = np.argsort(np.where(np.isfinite(c0), -c0, np.inf))[:K]
    coh[top[np.isfinite(c0[top])]] = True
    gp = {}
    for g in (13, 26):
        vals = []
        a = 26
        while a + 130 + g <= T:
            x = wmean(a + 52, a + 65) - wmean(a, a + 13)
            b = a + 65 + g
            y = wmean(b + 52, b + 65) - wmean(b, b + 13)
            ok = coh & np.isfinite(x) & np.isfinite(y)
            vals.append(spearmanr(x[ok], y[ok]).correlation)
            a += 13
        gp[g] = float(np.mean(vals))
    res["gapped"] = gp
    # Iniguez rank change C(R): occupant of rank R differs from the previous week
    piv = df[df["rank"].isin(RANKS_C)].pivot(index="period", columns="rank", values="entity_id").reindex(range(T))
    res["C_R"] = {int(r): float(np.mean(piv[r].values[1:] != piv[r].values[:-1])) for r in piv.columns}
    # boundary ratio (26-week blocks), pooled K/16..K
    try:
        bd = mb.analyse(df, K, block=26, boot=50)
        rows = [r for r in bd.get("pooled", []) if r["k"] >= K // 16 - 1]
        res["boundary_by_k"] = {int(r["k"]): float(r["ratio"]) for r in bd.get("pooled", [])}
        res["boundary_pooled"] = float(np.exp(np.mean(np.log([r["ratio"] for r in rows if r["ratio"] > 0])))) if rows else None
    except Exception as ex:                                     # noqa: BLE001
        res["boundary_error"] = repr(ex)
    # rate law (26-week blocks, pairs a year apart): slide, crossing gain, correlations
    try:
        law = bl.analyse(df, K, block=26, boot=20, step=2)
        cells = [r for q in law["pairs"] for r in q["rows"] if r["k"] < K]
        o = np.array([r["obs_P"] for r in cells])
        c = np.array([r["c"] for r in cells])
        s = np.array([r["sbar"] for r in cells])
        res["law"] = dict(n=len(cells), corr_c_minus_s=float(np.corrcoef(o, c - s)[0, 1]), corr_minus_s=float(np.corrcoef(o, -s)[0, 1]),
                          mean_c=float(c.mean()), mean_sbar=float(s.mean()), mean_obs=float(o.mean()), min_sbar=float(s.min()),
                          sbar_by_k={int(r["k"]): float(r["sbar"]) for r in law["pairs"][0]["rows"]})
    except Exception as ex:                                     # noqa: BLE001
        res["law_error"] = repr(ex)
    return res


if __name__ == "__main__":                                      # (R7: importable; behaviour unchanged)
    which = sys.argv[1]
    HERE = Path(__file__).resolve().parent
    if which == "data":
        out = {}
        for panel in ("reddit_submissions_long", "reddit_comments_ext", "facebook_a"):
            Kp = m1.PANELS[panel]["K"]
            df = mrd.load_panel(mrd.PLATFORMS[m1.PANELS[panel]["platform"]])
            Tp = int(df["period"].max()) + 1
            ranks = [r for r in RANKS_C if r <= Kp]
            piv = df[df["rank"].isin(ranks)].pivot(index="period", columns="rank", values="entity_id").reindex(range(Tp))
            out[panel] = {int(r): float(np.mean(piv[r].values[1:] != piv[r].values[:-1])) for r in piv.columns}
            print(f"{panel}: C(R) weekly " + ", ".join(f"R={r} {v:.2f}" for r, v in out[panel].items()), flush=True)
        (HERE / "rank_change_data.json").write_text(json.dumps(out, indent=1))
    else:
        if which == "neutral":
            _, rows = world_neutral()
            df = frame_from_rows(rows)
        else:
            X, ids = {"gibrat": world_gibrat, "atlas": world_atlas, "rdm": world_rdm}[which]()
            df = frame_from_matrix(X, ids)
        res = stats(df, which)
        print(json.dumps({k: v for k, v in res.items() if k not in ("boundary_by_k",)}, default=float), flush=True)
        (HERE / f"null_world__{which}.json").write_text(json.dumps(res, indent=1, default=float))
