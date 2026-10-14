"""Forward simulation: does one year's movement, pushed through this year's ladder, give next
year's ladder?  Design in runs/2026-10-14_forward/DESIGN.md.

A transition is measured on one pair of blocks (a, a+1), for the top W = 2K of block a by rank
stratum: the block-to-block changes of those still present, the share with no level in a+1,
and the endpoints that enter the top W at a+1 from outside it (new, or from below W), with
their levels relative to block a's K-th level.  It is then applied to a later block b: each
member of b's top W gets a change drawn for its stratum, survives with that stratum's rate,
and the entrants are added at the measured offsets from b's K-th level.  The simulated ladder
is compared with the observed one.  Three versions of the transition:

  empirical  changes resampled from the stratum's own changes
  moments    changes Gaussian with the stratum's mean and variance (two moments only)
  smooth     mean and log variance linear in log rank, attrition logistic in log rank,
             entrant count and Gaussian offsets: eight numbers per transition

    python llm_fitting/mts_forward.py run <panel>
    python llm_fitting/mts_forward.py report
"""
import json
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import balance_accounting as ba  # noqa: E402
import mts_step1 as m1  # noqa: E402
from mts_balance_law import strata  # noqa: E402

OUT = Path("llm_fitting/runs/2026-10-14_forward")
PANELS = ("reddit_comments_ext", "reddit_submissions_long")
N_SIM = 40
MODES = ("empirical", "moments", "smooth")


def cut_positions(K):
    """Shape is read at K/64 ... K/2 (positions rounded), against the top-K mean."""
    return [int(round(hi)) for lo, hi in strata(K) if K / 64 - 1e-9 <= hi <= K / 2 + 1e-9]


def block_ladders(df, K, block):
    X, R, floors, outside, ids = m1.matrices(df, K)
    T = X.shape[0]
    nb = T // block
    start = T - nb * block
    M = ba.block_means(X, block, start=start, min_frac=m1.PRES)
    n_cert = []
    for b in range(nb):
        seg = outside[start + b * block:start + (b + 1) * block]
        bd = float(np.nanmax(seg)) if np.isfinite(seg).any() else -np.inf
        n_cert.append(int((M[b][np.isfinite(M[b])] > bd).sum()))
    return M, n_cert


def _window(L, W):
    """Indices of the top W endpoints of a block, best first."""
    ok = np.flatnonzero(np.isfinite(L))
    return ok[np.argsort(-L[ok], kind="stable")][:W]


def _stratum(ranks, st):
    edges = np.array([hi for lo, hi in st])
    return np.minimum(np.searchsorted(edges, ranks, side="left"), len(st) - 1)


def transition(M, a, K, W=None):
    """The transition from block a to a+1, by rank stratum of block a."""
    W = W or 2 * K
    st = strata(K)
    top = _window(M[a], W)
    kth = float(M[a][top[K - 1]])
    j = _stratum(np.arange(1, top.size + 1), st)
    nxt = M[a + 1][top]
    alive = np.isfinite(nxt)
    d = nxt - M[a][top]
    top_next = _window(M[a + 1], W)
    in_now = np.zeros(M.shape[1], bool)
    in_now[top] = True
    ent = top_next[~in_now[top_next]]
    offsets = M[a + 1][ent] - kth
    rows = []
    for s in range(len(st)):
        m = j == s
        x = d[m & alive]
        rows.append(dict(n=int(m.sum()), q=float(1 - alive[m].mean()) if m.any() else np.nan,
                         changes=x, mean=float(x.mean()) if x.size else np.nan,
                         var=float(x.var()) if x.size > 1 else np.nan))
    # smooth version: eight numbers
    u = np.log2([np.sqrt(max(lo, 0.5) * hi) / K for lo, hi in st])
    w = np.array([r["n"] for r in rows], float)
    ok = np.array([np.isfinite(r["mean"]) and np.isfinite(r["var"]) and r["var"] > 0 for r in rows])
    A = np.column_stack([np.ones(ok.sum()), u[ok]])
    Wt = np.sqrt(w[ok])
    mean_fit = np.linalg.lstsq(A * Wt[:, None], np.array([r["mean"] for r in rows])[ok] * Wt, rcond=None)[0]
    lv_fit = np.linalg.lstsq(A * Wt[:, None], np.log([r["var"] for r in rows if np.isfinite(r["var"]) and r["var"] > 0]) * Wt, rcond=None)[0]
    qs = np.clip(np.array([r["q"] for r in rows])[ok], 1e-4, 1 - 1e-4)
    q_fit = np.linalg.lstsq(A * Wt[:, None], np.log(qs / (1 - qs)) * Wt, rcond=None)[0]
    smooth = dict(mean=mean_fit.tolist(), logvar=lv_fit.tolist(), logit_q=q_fit.tolist(), n_in=int(ent.size),
                  off_mean=float(offsets.mean()) if offsets.size else 0.0, off_sd=float(offsets.std()) if offsets.size > 1 else 0.0)
    return dict(start=a, K=K, W=W, strata=st, u=u.tolist(), rows=rows, n_in=int(ent.size), offsets=offsets, smooth=smooth)


def step(levels, tr, rng, mode):
    """One simulated block step from the top-W levels `levels` (sorted best first)."""
    K, W, st = tr["K"], tr["W"], tr["strata"]
    kth = levels[K - 1]
    j = _stratum(np.arange(1, levels.size + 1), st)
    new = np.empty(levels.size)
    keep = np.ones(levels.size, bool)
    if mode == "smooth":
        sm = tr["smooth"]
        u = np.array(tr["u"])[j]
        mu = sm["mean"][0] + sm["mean"][1] * u
        sd = np.sqrt(np.exp(sm["logvar"][0] + sm["logvar"][1] * u))
        q = 1 / (1 + np.exp(-(sm["logit_q"][0] + sm["logit_q"][1] * u)))
        new = levels + mu + sd * rng.standard_normal(levels.size)
        keep = rng.random(levels.size) >= q
        ent = kth + sm["off_mean"] + sm["off_sd"] * rng.standard_normal(sm["n_in"])
    else:
        for s, r in enumerate(tr["rows"]):
            m = j == s
            n = int(m.sum())
            if not n:
                continue
            if r["changes"].size == 0:
                keep[m] = False
                continue
            if mode == "empirical":
                dd = r["changes"][rng.integers(0, r["changes"].size, n)]
            else:
                dd = r["mean"] + np.sqrt(max(r["var"], 0.0)) * rng.standard_normal(n)
            new[m] = levels[m] + dd
            keep[m] = rng.random(n) >= (r["q"] if np.isfinite(r["q"]) else 0.0)
        ent = kth + tr["offsets"][rng.integers(0, tr["offsets"].size, tr["n_in"])] if tr["n_in"] else np.empty(0)
    out = np.concatenate([new[keep], ent])
    return np.sort(out)[::-1][:W]


def shape(levels, K, ks):
    UK = levels[:K].mean()
    return dict(shape={int(k): float(levels[:k].mean() - UK) for k in ks},
                slope={int(k): float(ba.local_slope(levels, k / np.sqrt(2), k * np.sqrt(2))) for k in ks})


def forecast(M, b, trs, K, ks, n_sim=N_SIM, mode="empirical", seed=0):
    """Mean shape and slope after applying the transitions in `trs` in turn from block b."""
    rng = np.random.default_rng(seed)
    start = M[b][_window(M[b], trs[0]["W"])]
    sims = []
    for _ in range(n_sim):
        lv = start
        for tr in trs:
            lv = step(lv, tr, rng, mode)
        sims.append(shape(lv, K, ks))
    return dict(shape={k: float(np.mean([s["shape"][k] for s in sims])) for k in ks},
                shape_sd={k: float(np.std([s["shape"][k] for s in sims])) for k in ks},
                slope={k: float(np.mean([s["slope"][k] for s in sims])) for k in ks})


def observed(M, b, K, ks, W):
    return shape(M[b][_window(M[b], W)], K, ks)


def analyse(df, K, block, n_sim=N_SIM):
    M, n_cert = block_ladders(df, K, block)
    nb = M.shape[0]
    ks = cut_positions(K)
    W = 2 * K
    out = dict(K=int(K), block=int(block), blocks=int(nb), certified_top=n_cert, ks=ks, cases=[])
    if min(n_cert) < W:
        out["not_adjudicated"] = "the top 2K is not certified exact in every block"
        return out
    obs = {b: observed(M, b, K, ks, W) for b in range(nb)}
    out["observed"] = {str(b): obs[b] for b in range(nb)}
    trs = {a: transition(M, a, K, W) for a in range(nb - 1)}
    out["transitions"] = {str(a): dict(n_in=t["n_in"], smooth=t["smooth"],
                                       strata=[dict(lo=float(lo), hi=float(hi), n=r["n"], q=r["q"], mean=r["mean"], var=r["var"])
                                               for (lo, hi), r in zip(t["strata"], t["rows"])]) for a, t in trs.items()}
    cases = []
    for b in range(1, nb - 1):                                    # one step ahead, transition from the year before
        cases.append(dict(kind="one step, out of sample", origin=b, target=b + 1, transitions=[b - 1]))
        cases.append(dict(kind="one step, in sample", origin=b, target=b + 1, transitions=[b]))
    if nb >= 4:
        cases.append(dict(kind="two steps, out of sample", origin=1, target=3, transitions=[0, 0]))
        cases.append(dict(kind="two steps, out of sample", origin=1, target=3, transitions=[0, 1]))
    for c in cases:
        b, t = c["origin"], c["target"]
        c["observed_change"] = {k: obs[t]["shape"][k] - obs[b]["shape"][k] for k in ks}
        c["observed_slope"] = obs[t]["slope"]
        c["origin_slope"] = obs[b]["slope"]
        prev = b - (t - b)
        c["last_change"] = ({k: obs[b]["shape"][k] - obs[prev]["shape"][k] for k in ks} if prev >= 0 else None)
        c["predicted"] = {}
        for mode in MODES:
            f = forecast(M, b, [trs[a] for a in c["transitions"]], K, ks, n_sim=n_sim, mode=mode)
            c["predicted"][mode] = dict(change={k: f["shape"][k] - obs[b]["shape"][k] for k in ks},
                                        change_sd=f["shape_sd"], slope=f["slope"])
    out["cases"] = cases
    return out


def rms(a, b=None, ks=None):
    ks = ks or list(a)
    v = np.array([a[k] - (b[k] if b is not None else 0.0) for k in ks])
    return float(np.sqrt((v ** 2).mean()))


def scores(res):
    """Rms errors of the predicted change of shape, by case and version, with the baselines."""
    ks = res["ks"]
    rows = []
    for c in res["cases"]:
        o = {int(k): v for k, v in c["observed_change"].items()}
        row = dict(kind=c["kind"], origin=c["origin"], target=c["target"], transitions=c["transitions"],
                   no_change=rms(o, None, ks),
                   last_change=None if c["last_change"] is None else rms(o, {int(k): v for k, v in c["last_change"].items()}, ks))
        for mode, p in c["predicted"].items():
            pc = {int(k): v for k, v in p["change"].items()}
            row[mode] = rms(o, pc, ks)
            ps = np.array([p["slope"][str(k)] if str(k) in p["slope"] else p["slope"][k] for k in ks])
            os_ = np.array([c["observed_slope"][str(k)] if str(k) in c["observed_slope"] else c["observed_slope"][k] for k in ks])
            og = np.array([c["origin_slope"][str(k)] if str(k) in c["origin_slope"] else c["origin_slope"][k] for k in ks])
            row[mode + "_slope_err"] = float(np.max(np.abs(ps / os_ - 1)))
            row["persist_slope_err"] = float(np.max(np.abs(og / os_ - 1)))
        rows.append(row)
    return rows


def run(panel):
    import minimal_rankdiff as mrd
    cfg = m1.PANELS[panel]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    res = analyse(df, cfg["K"], cfg["block"])
    res.update(panel=panel, finished=time.strftime("%Y-%m-%d %H:%M:%S"))
    res["scores"] = scores(res) if "cases" in res and res["cases"] else []
    (OUT / "cells").mkdir(parents=True, exist_ok=True)
    (OUT / "cells" / f"forward__{panel}.json").write_text(json.dumps(res, indent=1, default=float))
    print(f"[{panel}] blocks={res['blocks']} certified_top={res['certified_top']} cases={len(res.get('cases', []))} written", flush=True)


def report():
    lines = []
    for panel in PANELS:
        f = OUT / "cells" / f"forward__{panel}.json"
        if not f.exists():
            continue
        res = json.loads(f.read_text())
        K, ks = res["K"], res["ks"]
        lines += [f"\n## {panel}  (K = {K}, blocks of {res['block']} weeks; shape read at {ks} against the top-K mean)", "",
                  "Rms error of the predicted change of shape over those cut-offs, log units over the step (lower is better):", "",
                  "| case | origin → target | transitions from | no change | repeat last change | empirical | moments | smooth | worst slope error: empirical, persistence |",
                  "|---|---|---|---|---|---|---|---|---|"]
        for s in res["scores"]:
            lc = "—" if s["last_change"] is None else f"{s['last_change']:.4f}"
            lines.append(f"| {s['kind']} | {s['origin']}→{s['target']} | {s['transitions']} | {s['no_change']:.4f} | {lc} | "
                         f"{s['empirical']:.4f} | {s['moments']:.4f} | {s['smooth']:.4f} | {s['empirical_slope_err']:.0%}, {s['persist_slope_err']:.0%} |")
        lines += ["", "Observed and predicted (empirical) change of shape, log units, by cut-off:", ""]
        for c in res["cases"]:
            if "out of sample" not in c["kind"]:
                continue
            o = c["observed_change"]
            p = c["predicted"]["empirical"]["change"]
            sd = c["predicted"]["empirical"]["change_sd"]
            lines.append(f"- {c['kind']}, {c['origin']}→{c['target']} with {c['transitions']}: " + "; ".join(
                f"{k}: obs {o[str(k)]:+.3f}, pred {p[str(k)]:+.3f} (sd {sd[str(k)]:.3f})" for k in ks))
        tr = res["transitions"]
        lines += ["", "Smooth transitions (mean and log variance of the change, linear in log2 rank/K; logit attrition; entrants):", ""]
        for a, t in tr.items():
            sm = t["smooth"]
            lines.append(f"- {a}→{int(a) + 1}: mean {sm['mean'][0]:+.3f} {sm['mean'][1]:+.4f}·u; log var {sm['logvar'][0]:+.3f} {sm['logvar'][1]:+.3f}·u; "
                         f"logit q {sm['logit_q'][0]:+.2f} {sm['logit_q'][1]:+.2f}·u; entrants {sm['n_in']} at {sm['off_mean']:+.2f} ± {sm['off_sd']:.2f} from the K-th level")
    (OUT / "report_tables.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) == 2 and args[1] in PANELS:
        run(args[1])
    elif args[:1] == ["report"]:
        report()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
