"""Ex ante forecasts of lock-in: will today's top-k members still be in the top k h blocks from now?
(design in runs/2026-10-15_lockin/DESIGN.md)

At an origin block a, using only blocks a-2, a-1, a:
  training   for each stratum of CURRENT rank at block a-2, the members' change of level against the
             top-K mean U_K over one and two blocks: means mu1, mu2 and variances v1, v2, and the share
             still present (q1, q2).  The diffusion structure (a one-time constant plus drift linear in
             time; a constant plus variance linear in time) extends them to any horizon:
                 mu(h) = mu1 + (mu2 - mu1)(h - 1),   v(h) = v1 + (v2 - v1)(h - 1),
                 q(h) = q1 (q2 / q1)^(h - 1).
             The level of rank k against U_K drifts at its recent rate, beta_k = (b_k(a) - b_k(a-2)) / 2.
  forecast   member i of the top k at block a, with gap g_i = (level_i - U_K) - b_k at block a and
             stratum s of its current rank:
                 p_i(h) = q_s(h) * Phi( (g_i + mu_s(h) - beta_k h) / sqrt(v_s(h)) ).
             Nothing about whether anyone stayed in the top k enters the forecast.
  baselines  E: past turnover extrapolated geometrically, the share of block a-2's top k still in it at
             a-1 (r1) and at a (r2): r(h) = r1 (r2 / r1)^(h - 1), the same for every member;
             L: a logistic regression of retention on log rank and log horizon, fitted on the training
             pairs (top k at a-2 to a-1 and to a).
  scoring    observed: member i in the top k at block a+h.  Per (origin, k, h): forecast and observed
             retention shares; per member: Brier scores.

    python llm_fitting/mts_lockin.py run <panel>
    python llm_fitting/mts_lockin.py report
"""
import json
import sys
import time
from math import erf, sqrt
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_balance_law as bl  # noqa: E402
import mts_step1 as m1  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-15_lockin")
SETTINGS = {   # block length, presence for a block level, first date, universe K (registered values)
    "reddit_comments_ext": dict(block=26, pres=None, date_min=None, K=12500),
    "reddit_submissions_long": dict(block=26, pres=None, date_min=None, K=5000),
    "facebook_a": dict(block=13, pres=None, date_min=None, K=3500),
    "instagram_hm": dict(block=7, pres=0.5, date_min="2023-02-27", K=10000),
}
_phi = np.vectorize(lambda z: 0.5 * (1 + erf(z / sqrt(2))))


def _ranks(v):
    r = np.full(v.size, np.inf)
    ok = np.flatnonzero(np.isfinite(v))
    o = ok[np.argsort(-v[ok], kind="stable")]
    r[o] = np.arange(1, o.size + 1)
    return r


def _logit_fit(x, y, iters=50):
    """Logistic regression by Newton's method (x: n x p with intercept)."""
    b = np.zeros(x.shape[1])
    for _ in range(iters):
        p = 1 / (1 + np.exp(-x @ b))
        w = np.clip(p * (1 - p), 1e-9, None)
        H = x.T @ (x * w[:, None]) + 1e-6 * np.eye(x.shape[1])
        b = b + np.linalg.solve(H, x.T @ (y - p))
    return b


def analyse(M, K, ks, horizons, origins):
    st = bl.strata(K)
    nb = M.shape[0]
    UK = np.array([np.sort(M[b][np.isfinite(M[b])])[::-1][:K].mean() for b in range(nb)])
    Y = M - UK[:, None]
    R = np.array([_ranks(M[b]) for b in range(nb)])
    out = []
    for a in origins:
        t0 = a - 2
        # training moments by stratum of current rank at t0
        mom, Z, E = [], [], []
        rng_c = np.random.default_rng(1000 + a)
        for lo, hi in st:
            m = (R[t0] > lo) & (R[t0] <= hi)
            d1 = Y[t0 + 1][m] - Y[t0][m]
            d2 = Y[t0 + 2][m] - Y[t0][m]
            ok1, ok2 = np.isfinite(d1), np.isfinite(d2)
            # how far a member's squared move over one block carries into the next (opt-in individual volatility,
            # 2z-cz): the correlation, within the stratum, of the squared changes t0 -> t0+1 and t0+1 -> t0+2
            e1, e2 = d1, Y[t0 + 2][m] - Y[t0 + 1][m]
            okp = np.isfinite(e1) & np.isfinite(e2)
            lam = float(np.clip(np.corrcoef(e1[okp] ** 2, e2[okp] ** 2)[0, 1], 0, 1)) if okp.sum() > 20 else 0.0
            # opt-in empirical shape (2z-cz): the training changes standardised by their own mean and sd
            z1 = np.sort((d1[ok1] - d1[ok1].mean()) / d1[ok1].std()) if ok1.sum() > 20 else None
            z2 = np.sort((d2[ok2] - d2[ok2].mean()) / d2[ok2].std()) if ok2.sum() > 20 else None
            Z.append((z1, z2))
            # opt-in convolution (2z-cz): first-block changes d1 (they carry the one-time constant) and the next
            # block's increments of the same members, t0+1 -> t0+2, which follow selection; both empirical
            E.append((d1[ok1] if ok1.sum() > 20 else None, e2[okp] if okp.sum() > 20 else None))
            mom.append(dict(n=int(m.sum()), q1=float(ok1.mean()) if m.any() else np.nan, q2=float(ok2.mean()) if m.any() else np.nan,
                            mu1=float(np.mean(d1[ok1])) if ok1.sum() > 5 else np.nan, mu2=float(np.mean(d2[ok2])) if ok2.sum() > 5 else np.nan,
                            v1=float(np.var(d1[ok1])) if ok1.sum() > 5 else np.nan, v2=float(np.var(d2[ok2])) if ok2.sum() > 5 else np.nan,
                            lam=lam, msq=float(np.mean(e2[okp] ** 2)) if okp.sum() > 5 else np.nan))
        # training pairs for the logistic baseline, and past turnover for the geometric one
        cell = dict(origin=int(a), ks={})
        for k in ks:
            top0 = R[t0] <= k
            r1 = float(np.mean(R[t0 + 1][top0] <= k))
            r2 = float(np.mean(R[t0 + 2][top0] <= k))
            xs, ys = [], []
            for h, Rh in ((1, R[t0 + 1]), (2, R[t0 + 2])):
                xs.append(np.c_[np.ones(top0.sum()), np.log(R[t0][top0]), np.full(top0.sum(), np.log(h))])
                ys.append((Rh[top0] <= k).astype(float))
            bL = _logit_fit(np.vstack(xs), np.concatenate(ys))
            b_k = lambda t: float(np.sort(Y[t][np.isfinite(Y[t])])[::-1][k - 1])  # noqa: E731
            beta = (b_k(a) - b_k(t0)) / 2
            members = np.flatnonzero(R[a] <= k)
            g = Y[a][members] - b_k(a)
            sj = np.array([next(j for j, (lo, hi) in enumerate(st) if lo < r <= hi) for r in R[a][members]])
            hs = {}
            for h in horizons:
                if a + h >= nb:
                    continue
                mu = np.array([mom[j]["mu1"] + (mom[j]["mu2"] - mom[j]["mu1"]) * (h - 1) for j in sj])
                v = np.array([max(mom[j]["v1"] + (mom[j]["v2"] - mom[j]["v1"]) * (h - 1), 1e-6) for j in sj])
                q = np.array([mom[j]["q1"] * (mom[j]["q2"] / mom[j]["q1"]) ** (h - 1) if mom[j]["q1"] > 0 else 0.0 for j in sj])
                p_model = np.clip(q * _phi((g + mu - beta * h) / np.sqrt(v)), 0, 1)
                # individual volatility: scale the variance by the member's own last squared move relative to its
                # stratum, shrunk by the stratum's persistence lam (both from the training window and block a)
                own = (Y[a][members] - Y[a - 1][members]) ** 2
                msq = np.array([mom[j]["msq"] for j in sj])
                lam = np.array([mom[j]["lam"] for j in sj])
                ratio = np.where(np.isfinite(own) & (msq > 0), own / msq, 1.0)
                w = np.clip(1 + lam * (ratio - 1), 0.05, 20)
                p_ind = np.clip(q * _phi((g + mu - beta * h) / np.sqrt(v * w)), 0, 1)
                # empirical shape: P(stay) = P(z > -x), z from the stratum's standardised training changes at
                # horizon 1 (h = 1) or 2 (h >= 2); Phi where a stratum has too few
                x_ = (g + mu - beta * h) / np.sqrt(v)
                p_emp = np.empty(members.size)
                for i, j in enumerate(sj):
                    zz = Z[j][0 if h == 1 else 1]
                    p_emp[i] = _phi(x_[i]) if zz is None else 1 - np.searchsorted(zz, -x_[i], side="right") / zz.size
                p_emp = np.clip(q * p_emp, 0, 1)
                # convolution: change over h = one draw of d1 + (h - 1) independent draws of the later increment;
                # the stratum's distribution by 4,000 Monte Carlo draws, the boundary drifting as before
                p_conv = np.empty(members.size)
                cache = {}
                for i, j in enumerate(sj):
                    if j not in cache:
                        d1j, e2j = E[j]
                        if d1j is None or e2j is None:
                            cache[j] = None
                        else:
                            draws = d1j[rng_c.integers(0, d1j.size, 4000)]
                            for _ in range(h - 1):
                                draws = draws + e2j[rng_c.integers(0, e2j.size, 4000)]
                            cache[j] = np.sort(draws)
                    dd = cache[j]
                    thr = -(g[i] - beta * h)
                    p_conv[i] = (_phi((g[i] + mu[i] - beta * h) / np.sqrt(v[i])) if dd is None
                                 else 1 - np.searchsorted(dd, thr, side="right") / dd.size)
                p_conv = np.clip(q * p_conv, 0, 1)
                p_geo = np.full(members.size, r1 * (r2 / r1) ** (h - 1) if r1 > 0 else 0.0)
                p_log = 1 / (1 + np.exp(-(bL[0] + bL[1] * np.log(R[a][members]) + bL[2] * np.log(h))))
                obs = (R[a + h][members] <= k).astype(float)
                brier = lambda p: float(np.mean((p - obs) ** 2))  # noqa: E731
                # calibration: deciles of the model's probability
                order = np.argsort(p_model)
                bins = np.array_split(order, 10) if members.size >= 50 else [order]
                calib = [dict(p=float(p_model[b].mean()), obs=float(obs[b].mean()), n=int(b.size)) for b in bins if b.size]
                hs[str(h)] = dict(n=int(members.size), observed=float(obs.mean()), model=float(p_model.mean()),
                                  geometric=float(p_geo.mean()), logistic=float(p_log.mean()), individual=float(p_ind.mean()),
                                  empirical=float(p_emp.mean()), brier_empirical=brier(p_emp),
                                  convolution=float(p_conv.mean()), brier_convolution=brier(p_conv),
                                  brier_model=brier(p_model), brier_geometric=brier(p_geo), brier_logistic=brier(p_log),
                                  brier_individual=brier(p_ind),
                                  calibration=calib)
            cell["ks"][str(k)] = dict(r1=r1, r2=r2, beta=beta, horizons=hs)
        cell["moments"] = mom
        out.append(cell)
    return out


def run(panel):
    import minimal_rankdiff as mrd
    s = SETTINGS[panel]
    cfg = dict(mrd.PLATFORMS[m1.PANELS[panel]["platform"] if panel in m1.PANELS else panel])
    if s["date_min"]:
        cfg["date_min"] = s["date_min"]
    df = mrd.load_panel(cfg)
    K = s["K"]
    X, Rk, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    nb = T // s["block"]
    start = T - nb * s["block"]
    M = ba.block_means(X, s["block"], start=start, min_frac=m1.PRES if s["pres"] is None else s["pres"])
    del X
    dates = df.groupby("period")["ts"].first().reindex(range(T)).astype(str).str[:10].tolist()
    ks = (K // 64, K // 16, K // 4)
    res = dict(panel=panel, K=K, block=s["block"], n_blocks=int(nb), first_week=[dates[start + b * s["block"]] for b in range(nb)],
               ks=list(ks), origins=analyse(M, K, ks, horizons=range(1, nb), origins=range(2, nb - 1)),
               finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"lockin__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] blocks {nb}, origins {[c['origin'] for c in res['origins']]} written", flush=True)


def cells(res):
    for c in res["origins"]:
        for k, v in c["ks"].items():
            for h, x in v["horizons"].items():
                yield c["origin"], int(k), int(h), x


def score():
    """The predictions of DESIGN.md, rules as registered there before the run."""
    out, allc = {}, []
    for pn in SETTINGS:
        f = OUT / f"lockin__{pn}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        cs = list(cells(r))
        allc += [(pn, *c) for c in cs]
        n = sum(x["n"] for *_, x in cs)
        wb = lambda key: sum(x[key] * x["n"] for *_, x in cs) / n  # noqa: E731
        bm, bg, blg = wb("brier_model"), wb("brier_geometric"), wb("brier_logistic")
        err_m = [abs(x["model"] - x["observed"]) for *_, x in cs]
        err_g = [abs(x["geometric"] - x["observed"]) for *_, x in cs]
        long_m = [abs(x["model"] - x["observed"]) for _, _, h, x in cs if h >= 3]
        long_g = [abs(x["geometric"] - x["observed"]) for _, _, h, x in cs if h >= 3]
        out[pn] = dict(cells=len(cs), brier_model=bm, brier_geometric=bg, brier_logistic=blg, skill_vs_geometric=1 - bm / bg,
                       T1="PASS" if bm < bg else "FAIL",
                       median_abs_err_model=float(np.median(err_m)), median_abs_err_geometric=float(np.median(err_g)),
                       share_within_007=float(np.mean(np.array(err_m) <= 0.07)),
                       long_horizon=None if not long_m else dict(model=float(np.mean(long_m)), geometric=float(np.mean(long_g)), cells=len(long_m)))
    errs = np.array([abs(x["model"] - x["observed"]) for *_, x in allc])
    out["T2"] = dict(share_within_007=float(np.mean(errs <= 0.07)), cells=int(errs.size), verdict="PASS" if np.mean(errs <= 0.07) >= 0.75 else "FAIL")
    lm = [abs(x["model"] - x["observed"]) for _, _, _, h, x in allc if h >= 3]
    lg = [abs(x["geometric"] - x["observed"]) for _, _, _, h, x in allc if h >= 3]
    out["T3"] = dict(model=float(np.mean(lm)) if lm else None, geometric=float(np.mean(lg)) if lg else None, cells=len(lm),
                     verdict=("PASS" if lm and np.mean(lm) <= np.mean(lg) else "FAIL") if lm else "NOT ADJUDICATED")
    ind = {}
    for pn in SETTINGS:
        f = OUT / f"lockin__{pn}.json"
        if not f.exists():
            continue
        cs = list(cells(json.loads(f.read_text())))
        if not cs or "individual" not in cs[0][3]:
            continue
        n = sum(x["n"] for *_, x in cs)
        under_m = sum((x["observed"] - x["model"]) * x["n"] for *_, x in cs) / n
        under_i = sum((x["observed"] - x["individual"]) * x["n"] for *_, x in cs) / n
        bm = sum(x["brier_model"] * x["n"] for *_, x in cs) / n
        bi = sum(x["brier_individual"] * x["n"] for *_, x in cs) / n
        ind[pn] = dict(under_forecast_model=under_m, under_forecast_individual=under_i, brier_model=bm, brier_individual=bi,
)
    emp = {}
    for pn in SETTINGS:
        f = OUT / f"lockin__{pn}.json"
        if not f.exists():
            continue
        cs = list(cells(json.loads(f.read_text())))
        if not cs or "empirical" not in cs[0][3]:
            continue
        n = sum(x["n"] for *_, x in cs)
        um = sum((x["observed"] - x["model"]) * x["n"] for *_, x in cs) / n
        ue = sum((x["observed"] - x["empirical"]) * x["n"] for *_, x in cs) / n
        bm = sum(x["brier_model"] * x["n"] for *_, x in cs) / n
        be = sum(x["brier_empirical"] * x["n"] for *_, x in cs) / n
        bg = sum(x["brier_geometric"] * x["n"] for *_, x in cs) / n
        bl_ = sum(x["brier_logistic"] * x["n"] for *_, x in cs) / n
        uc = sum((x["observed"] - x["convolution"]) * x["n"] for *_, x in cs) / n
        bc = sum(x["brier_convolution"] * x["n"] for *_, x in cs) / n
        emp[pn] = dict(under_forecast_gaussian=um, under_forecast_empirical=ue, under_forecast_convolution=uc,
                       brier_gaussian=bm, brier_empirical=be, brier_convolution=bc, brier_geometric=bg, brier_logistic=bl_)
    if emp:                                        # POST HOC variants (2z-cz), reported, not registered or scored
        out["post_hoc_empirical_shape"] = emp
    if ind:
        out["post_hoc_individual_volatility"] = ind
    print(json.dumps(out, indent=1))
    return out


def report():
    lines = []
    for pn in SETTINGS:
        f = OUT / f"lockin__{pn}.json"
        if not f.exists():
            continue
        r = json.loads(f.read_text())
        bw = r["block"]
        lines += [f"\n## {pn} (K = {r['K']}; {bw}-week blocks; retention of the top k after h blocks)", "",
                  "| origin block (information through its last week) | target block | k | h (weeks) | members | observed | model | past turnover, geometric | logistic | Brier: model / geometric / logistic |",
                  "|---|---|---|---|---|---|---|---|---|---|"]
        for o, k, h, x in cells(r):                     # labels: the block's first week; the forecast uses the whole block (2z-da)
            lines.append(f"| {r['first_week'][o]} | {r['first_week'][o + h]} | {k} | {h * bw} | {x['n']} | {x['observed']:.3f} | {x['model']:.3f} | {x['geometric']:.3f} | "
                         f"{x['logistic']:.3f} | {x['brier_model']:.3f} / {x['brier_geometric']:.3f} / {x['brier_logistic']:.3f} |")
    (OUT / "report_tables.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) == 2 and args[1] in SETTINGS:
        run(args[1])
    elif args[:1] == ["report"]:
        report()
        score()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
