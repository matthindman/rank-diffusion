#!/usr/bin/env python3
"""EXIT AUDIT + auditable sign-flip runner (fifth-review items 1, 2, 4;
EXPLORATORY, extension panel per the §2z-q protocol line).

Reproduction: python3 llm_fitting/exit_audit.py

Design, declared before running:
  P1 SELF-TEST (runs first, hard-asserts): a transient spiker
     (rank oscillating 100<->20000) must be EXCLUDED from the
     permanent-rank cohort; a steady established entity must be INCLUDED
     and its single permanent departure counted once.
  P2 TRAIN-WINDOW COHORT SIGN-FLIP (outcome-independent): cohort defined
     on periods 0..135 ONLY (absence-penalized permanent rank, presence
     floor), exits scored on periods 136..212. Pre-declared robustness
     grid, ALL cells reported: perm-rank cut {K/4, K/2} x presence
     {0.6, 0.7, 0.8}. Empirical rate with an entity-clustered bootstrap
     CI (500 draws); sim baseline over 10 seeds. PREDICTION: the §2z-z
     sign flip (sim < emp) survives in every cell.
  P3 EXIT-CAUSE DECOMPOSITION + ESTIMATOR AUDIT: by permanent-rank third,
     empirical P(absent at t+1 | present at t) — the estimator's exit
     estimand — decomposed into: returns within 13 weeks / returns later /
     never returns in panel; compared against the fitted exit_rate(z)
     head/mid/tail. If the estimator matches the ALL-ABSENCE rate but the
     simulator implements exits as PERMANENT rebirth, temporary empirical
     inactivity is being simulated as death — the estimand-mismatch
     hypothesis.
  P4 SIM EXIT COMPOSITION: for the P2 cohort, sim exits split into
     crossings (present below K; can return) vs deaths (tracked rank 0
     onward; permanent by construction) vs the empirical split.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import minimal_rankdiff as mrd  # noqa: E402

K = 12_500
T0, T_FULL = 136, 213
N_FLOOR = 50_001
LONG = dict(temper=True, min_knot_n=8, md_lags=6, t_tails=True,
            md_vr_long=True, stat_factor=True, two_scale=True,
            mix_hetero=True)


def cohort_mask(ranks, lo_t, hi_t, cut, pres_floor):
    r = ranks[lo_t:hi_t].astype(float)
    pen = np.where(r > 0, r, N_FLOOR)
    return (pen.mean(axis=0) <= cut) & ((r > 0).mean(axis=0) >= pres_floor)


def exit_rate(ranks, coh, lo_t, hi_t, K=K):
    rr = ranks[lo_t:hi_t, coh].astype(float)
    inK = (rr > 0) & (rr <= K)
    ev = inK[:-1] & ((rr[1:] <= 0) | (rr[1:] > K))
    n_cw = int(inK[:-1].sum())
    return ev, n_cw, (int(ev.sum()) / max(n_cw, 1))


def self_test():
    T = 200
    spiker = np.tile([100, 20000], T // 2)
    established = np.full(T, 3000.0)
    established[150:] = 0                     # permanent departure at 150
    ranks = np.stack([spiker, established], axis=1)
    coh = cohort_mask(ranks, 0, 136, K // 2, 0.6)
    assert not coh[0], "spiker must be excluded (absence-penalized mean ~10k)"
    assert coh[1], "established entity must be in the cohort"
    ev, _, _ = exit_rate(ranks, coh, 136, T)
    assert int(ev.sum()) == 1, "exactly one departure event"
    print("P1 self-test: PASS (spiker excluded; established counted once)")


def main():
    self_test()
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    df = mrd.restrict_universe(df, K, buffer_mult=4)
    sk = df.attrs["score_k"]
    ev_, er, _, _ = mrd.empirical_structures(df, 10, topid_k=sk)
    p = mrd.estimate(df, **LONG)
    sims = [mrd.simulate(p, T_FULL, seed=s, top_record=sk) for s in range(10)]

    print("\nP2 train-window cohort (defined on 0..135, scored 136..212), "
          "robustness grid — ALL cells:")
    rng = np.random.default_rng(0)
    for cut, cname in ((K // 4, "K/4"), (K // 2, "K/2")):
        for pf in (0.6, 0.7, 0.8):
            coh = cohort_mask(er, 0, T0, cut, pf)
            ev2, ncw, rate = exit_rate(er, coh, T0, T_FULL)
            idx = np.arange(ev2.shape[1])
            boots = []
            for _ in range(500):
                c = rng.choice(idx, size=len(idx), replace=True)
                boots.append(ev2[:, c].sum() / max((ev2[:, c] >= 0).size, 1)
                             if False else
                             ev2[:, c].sum() / max(ncw * len(c) / max(len(idx), 1), 1))
            lo, hi = np.percentile(boots, [2.5, 97.5])
            srates = []
            for sim in sims:
                tr = np.asarray(sim["tranks"])
                cs = cohort_mask(tr, 0, T0, cut, pf)
                _, _, r_ = exit_rate(tr, cs, T0, T_FULL)
                srates.append(r_)
            print(f"  cut={cname} pres>={pf}: emp {rate:.5f} "
                  f"[{lo:.5f},{hi:.5f}] (n={int(coh.sum())})   "
                  f"sim {np.mean(srates):.5f} ± {np.std(srates):.5f}   "
                  f"sign-flip(sim<emp): {np.mean(srates) < lo}", flush=True)

    print("\nP3 exit-cause decomposition + estimator audit (empirical, "
          "by permanent-rank third of the universe):")
    r = er.astype(float)
    pen = np.where(r > 0, r, N_FLOOR)
    perm = pen.mean(axis=0)
    thirds = np.quantile(perm, [1 / 3, 2 / 3])
    ez = np.asarray(p.exit_rate)
    n3 = max(1, len(ez) // 3)
    print(f"  fitted exit_rate(z) head/mid/tail thirds (knot means): "
          f"{ez[:n3].mean():.4f} / {ez[n3:2 * n3].mean():.4f} / "
          f"{ez[2 * n3:].mean():.4f}")
    for name, m in (("head", perm <= thirds[0]),
                    ("mid", (perm > thirds[0]) & (perm <= thirds[1])),
                    ("tail", perm > thirds[1])):
        rr = r[:, m]
        pres = rr > 0
        gap = pres[:-1] & ~pres[1:]                    # estimator's "exit"
        n_pw = int(pres[:-1].sum())
        p_abs = gap.sum() / max(n_pw, 1)
        t_idx, e_idx = np.where(gap)
        ret13 = ret_ever = 0
        for t, e in zip(t_idx, e_idx):
            fut = pres[t + 1:, e]
            if fut.any():
                ret_ever += 1
                if fut[:13].any():
                    ret13 += 1
        n_ev = max(len(t_idx), 1)
        print(f"  {name}: P(absent next wk) {p_abs:.4f}; of those — "
              f"return<=13wk {ret13 / n_ev:.2f}, return later "
              f"{(ret_ever - ret13) / n_ev:.2f}, NEVER return "
              f"{1 - ret_ever / n_ev:.2f}   (events {len(t_idx):,})",
              flush=True)

    print("\nP4 sim exit composition for the (K/2, 0.7) cohort, scored "
          "136..212 (crossing = below-K, can return; death = tracked 0, "
          "permanent by construction):")
    coh_e = cohort_mask(er, 0, T0, K // 2, 0.7)
    rr = er[T0:, coh_e].astype(float)
    inK = (rr > 0) & (rr <= K)
    cross = (inK[:-1] & (rr[1:] > K)).sum()
    absn = (inK[:-1] & (rr[1:] <= 0)).sum()
    print(f"  emp: crossings {int(cross)}, absences {int(absn)} "
          f"(share absent {absn / max(cross + absn, 1):.2f})")
    for s, sim in enumerate(sims[:3]):
        tr = np.asarray(sim["tranks"])
        cs = cohort_mask(tr, 0, T0, K // 2, 0.7)
        rr = tr[T0:, cs].astype(float)
        inK = (rr > 0) & (rr <= K)
        cross = (inK[:-1] & (rr[1:] > K)).sum()
        dead = (inK[:-1] & (rr[1:] <= 0)).sum()
        print(f"  sim s{s}: crossings {int(cross)}, deaths {int(dead)} "
              f"(share death {dead / max(cross + dead, 1):.2f})")


if __name__ == "__main__":
    main()
