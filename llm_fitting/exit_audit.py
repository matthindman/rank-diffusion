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
import pandas as pd

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


def idsafe_cohort_events(tranks, tids, t0, K, cut, ref_t=None):
    """IDENTITY-SAFE simulated cohort scorer (sixth review): cohort =
    tracked slots whose occupant id is CONSTANT over [0, t0) and whose
    permanent rank (mean tranks over [0, t0)) <= cut. Scored on t0..T-2:
    crossing = in top-K at t, present-below-K at t+1 (same id); death =
    in top-K at t, id changes at t+1; a member leaves the risk set
    permanently at its first id change (dead exactly once)."""
    ref_t = t0 - 1 if ref_t is None else ref_t
    T = tranks.shape[0]
    ref = tids[ref_t]
    stable = (tids[:t0] == ref).all(axis=0)
    perm = tranks[:t0].astype(float).mean(axis=0)
    coh = np.where(stable & (perm <= cut))[0]
    deaths = crossings = risk = 0
    for j in coh:
        for t in range(t0, T - 1):
            if tids[t, j] != ref[j]:
                break                          # died earlier; out of risk
            inK = 0 < tranks[t, j] <= K
            if inK:
                risk += 1
            if tids[t + 1, j] != ref[j]:
                if inK:
                    deaths += 1
                break
            if inK and tranks[t + 1, j] > K:
                crossings += 1
    return dict(deaths=deaths, crossings=crossings, risk_weeks=risk,
                n_cohort=len(coh),
                rate=(deaths + crossings) / max(risk, 1))


def boot_ci_ratio(ev, inK, n=500, seed=0):
    """Corrected entity-cluster bootstrap (sixth review): resample each
    entity's event count AND its in-K exposure together; p* = sum E / sum N.
    Plus a moving-week block variant (block length 8)."""
    rng = np.random.default_rng(seed)
    E = ev.sum(axis=0).astype(float)
    Nw = inK[:-1].sum(axis=0).astype(float)
    idx = np.arange(len(E))
    ent = [ev[:, c].sum() / max(inK[:-1][:, c].sum(), 1)
           for c in (rng.choice(idx, len(idx), True) for _ in range(n))]
    Tm = ev.shape[0]
    L = 8
    starts = np.arange(0, Tm - L + 1)
    blk = []
    for _ in range(n):
        rows = np.concatenate([np.arange(s, s + L)
                               for s in rng.choice(starts,
                                                   int(np.ceil(Tm / L)), True)])[:Tm]
        blk.append(ev[rows].sum() / max(inK[:-1][rows].sum(), 1))
    return (np.percentile(ent, [2.5, 97.5]), np.percentile(blk, [2.5, 97.5]))


def idsafe_main():
    """--idsafe: the identity-safe rerun (20 seeds) + corrected empirical
    bootstrap. NOTE (sixth review): the presence grid was inactive
    (identical cohorts at 0.6/0.7/0.8) — only the two rank cuts are
    distinct cohorts and only those are reported."""
    self_test()
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    df = mrd.restrict_universe(df, K, buffer_mult=4)
    sk = df.attrs["score_k"]
    _, er, _, _ = mrd.empirical_structures(df, 10, topid_k=sk)
    p = mrd.estimate(df, **LONG)
    print("\nidentity-safe cohort comparison (train-defined 0..135, scored "
          "136..212; 20 seeds; corrected entity + week-block bootstrap):")
    for cut, cname in ((K // 4, "K/4"), (K // 2, "K/2")):
        coh = cohort_mask(er, 0, T0, cut, 0.7)
        ev2, ncw, rate = exit_rate(er, coh, T0, T_FULL)
        rr = er[T0:, coh].astype(float)
        inK = (rr > 0) & (rr <= K)
        (elo, ehi), (blo, bhi) = boot_ci_ratio(ev2, inK)
        res = []
        for s in range(20):
            sim = mrd.simulate(p, T_FULL, seed=s, top_record=sk,
                               track_ids=True)
            res.append(idsafe_cohort_events(np.asarray(sim["tranks"]),
                                            np.asarray(sim["tids"]),
                                            t0=T0, K=sk, cut=cut))
        rates = [r["rate"] for r in res]
        dsh = [r["deaths"] / max(r["deaths"] + r["crossings"], 1) for r in res]
        print(f"  cut={cname}: emp {rate:.5f} ent-CI [{elo:.5f},{ehi:.5f}] "
              f"wk-CI [{blo:.5f},{bhi:.5f}] (n={int(coh.sum())})")
        print(f"           sim {np.mean(rates):.5f} ± {np.std(rates):.5f} "
              f"(n_coh~{int(np.mean([r['n_cohort'] for r in res]))}); "
              f"death share of sim events {np.mean(dsh):.2f}; "
              f"sign-flip vs ent-CI: {np.mean(rates) < elo}", flush=True)


def identity_histories(tranks, tids):
    """Reconstruct per-IDENTITY rank histories from slot arrays: each
    identity's column has its ranks during its lifetime and 0 (absent =
    dead/unborn) outside. Symmetric with an empirical rank matrix where
    0 = absent week."""
    T, n = tranks.shape
    cols = []
    for j in range(n):
        col_ids = tids[:, j]
        t = 0
        while t < T:
            e = t
            while e + 1 < T and col_ids[e + 1] == col_ids[t]:
                e += 1
            h = np.zeros(T, dtype=np.int32)
            h[t:e + 1] = tranks[t:e + 1, j]
            cols.append(h)
            t = e + 1
    return np.stack(cols, axis=1)


def score_matrix(R, t0, K, cut, pf, is_sim):
    """Symmetric scorer on a rank matrix (0 = absent): cohort via
    cohort_mask (absence-penalized perm rank + presence over [0, t0));
    events on t0..T-2 from in-K weeks: crossing (present below K) vs
    absence (0 next week). Empirical absences decomposed into
    return<=13 / never-returns (right-censored, declared); simulated
    absences are deaths by construction."""
    coh = cohort_mask(R, 0, t0, cut, pf)
    rr = R[t0:, coh]
    inK = (rr > 0) & (rr <= K)
    cross = inK[:-1] & (rr[1:] > K)
    absent = inK[:-1] & (rr[1:] <= 0)
    risk = int(inK[:-1].sum())
    out = dict(n=int(coh.sum()), risk=risk,
               crossings=int(cross.sum()), absences=int(absent.sum()),
               rate=(int(cross.sum()) + int(absent.sum())) / max(risk, 1))
    if not is_sim and out["absences"]:
        t_i, e_i = np.where(absent)
        pres = rr > 0
        r13 = sum(1 for t, e in zip(t_i, e_i) if pres[t + 1:t + 14, e].any())
        rev = sum(1 for t, e in zip(t_i, e_i) if pres[t + 1:, e].any())
        out["abs_ret13"], out["abs_never"] = r13, out["absences"] - rev
    return out


def aligned_main(n_seeds=30):
    """--aligned (seventh review): train-only universe
    (member_window=136), NO survivor filter (full identity matrices both
    sides), symmetric cohort construction, pooled composition counts,
    quantiles. Presence thresholds are now ACTIVE (no pre-filter)."""
    self_test()
    df = mrd.load_panel(mrd.PLATFORMS["reddit_comments_ext"])
    df = mrd.restrict_universe(df, K, buffer_mult=4, member_window=T0)
    sk = df.attrs["score_k"]
    T_full = int(df["period"].max()) + 1
    piv = df.pivot_table(index="period", columns="entity_id", values="rank",
                         fill_value=0).reindex(range(T_full), fill_value=0)
    R_emp = piv.to_numpy().astype(np.int32)
    print(f"train-only universe: {R_emp.shape[1]:,} entities, no survivor "
          f"filter; score_k={sk}")
    p = mrd.estimate(df, **LONG)
    sims_h = []
    for s_ in range(n_seeds):
        sim = mrd.simulate(p, T_full, seed=s_, top_record=sk, track_ids=True)
        sims_h.append(identity_histories(np.asarray(sim["tranks"]),
                                         np.asarray(sim["tids"])))
    for cut, cname in ((top_k // 4, "K/4"), (top_k // 2, "K/2")):
        for pf in (0.6, 0.7, 0.8):
            e = score_matrix(R_emp, t0, sk, cut, pf, is_sim=False)
            ss = [score_matrix(Rs, t0, sk, cut, pf, is_sim=True)
                  for Rs in sims_h]
            rates = np.array([x["rate"] for x in ss])
            D = sum(x["absences"] for x in ss)
            C = sum(x["crossings"] for x in ss)
            print(f"  cut={cname} pres>={pf}: "
                  f"emp rate {e['rate']:.5f} (n={e['n']}, cross {e['crossings']}, "
                  f"abs {e['absences']}"
                  + (f": ret13 {e.get('abs_ret13', 0)}, never "
                     f"{e.get('abs_never', 0)}" if e["absences"] else "")
                  + f") | sim rate {rates.mean():.5f} "
                  f"[q10 {np.quantile(rates, .1):.5f}, "
                  f"q90 {np.quantile(rates, .9):.5f}] "
                  f"pooled deaths/(d+c) {D}/{D + C} = {D / max(D + C, 1):.2f}",
                  flush=True)


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
    if len(sys.argv) > 1 and sys.argv[1] == "--idsafe":
        idsafe_main()
    elif len(sys.argv) > 1 and sys.argv[1] == "--aligned":
        import argparse
        ap = argparse.ArgumentParser()
        ap.add_argument("--platform", default="reddit_comments_ext")
        ap.add_argument("--t0", type=int, default=T0)
        ap.add_argument("--top-k", type=int, default=K)
        ap.add_argument("--seeds", type=int, default=30)
        ap.add_argument("--anchor-date", default=None)
        a = ap.parse_args(sys.argv[2:])
        aligned_main(n_seeds=a.seeds, platform=a.platform, t0=a.t0,
                     top_k=a.top_k, anchor_date=a.anchor_date)
    else:
        main()
