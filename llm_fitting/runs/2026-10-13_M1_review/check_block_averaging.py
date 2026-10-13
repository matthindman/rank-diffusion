"""Self-review of step 1 (MODEL_STATUS 2z-bu): what averaging over a block does to the accounting.

For a random walk with weekly variance sigma^2, the difference between two L-week block
means whose blocks start D >= L weeks apart has variance sigma^2 (D - L/3 + 1/(3L)), not
sigma^2 D.  A mean drift is not affected.  So on block means the reordering gain, the
occupants' loss and the band variance are all short by the factor below, and the slide of
a cohort fixed earlier is not.

Part 1: known-answer check on synthetic random walks, through balance_accounting.
Part 2: the reordering rates already in the step 1 cells, by block length and spacing,
        divided by the factor.  Reads result files only: no panel is opened.

    python llm_fitting/runs/2026-10-13_M1_review/check_block_averaging.py
"""
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, "llm_fitting")
import balance_accounting as ba  # noqa: E402

CELLS = Path("llm_fitting/runs/2026-10-13_M1_balance/cells")


def factor(L, D):
    """Variance of the difference of two L-week block means D weeks apart, over sigma^2 D."""
    return 1.0 if L == 1 else (D - L / 3 + 1 / (3 * L)) / D


def synthetic(seed, N=60000, K=5000, S=1.5, sig2=0.009, L=52, nb=4, noise=0.0, rho=0.0):
    """Random walks on an exponential ladder (level = S log(N / rank)), where the level at
    every rank rises at sigma^2 / (2S) per week and the shape does not move."""
    rng = np.random.default_rng(seed)
    T = L * nb
    X = S * np.log(N / np.arange(1, N + 1)) + np.cumsum(rng.normal(0, np.sqrt(sig2), (T, N)), axis=0)
    if noise > 0:
        e = np.zeros((T, N))
        e[0] = rng.normal(0, np.sqrt(noise), N)
        for t in range(1, T):
            e[t] = rho * e[t - 1] + rng.normal(0, np.sqrt(noise * (1 - rho ** 2)), N)
        X = X + e
    M = X.reshape(nb, L, N).mean(axis=1)
    c = sig2 / (2 * S)
    ks = [K // 16, K // 8, K // 4, K // 2, K]
    centre = [np.sort(M[b])[::-1][:K].mean() for b in range(nb)]
    cohort = np.argsort(-M[0])[:K]
    slide = -((M[3, cohort] - M[2, cohort]).mean() - (centre[3] - centre[2])) / L
    rows = ba.topk_accounting(M[2], M[3], ks, K)
    r = float(np.median([x["R"] / (x["k"] * L) for x in rows]))
    g = float(np.median([x["G"] / (x["k"] * L) for x in rows]))
    return slide / c, r / c, g / c, r / slide


def part1():
    print("PART 1. Synthetic random walks, 52-week block means, cohort fixed on block 0, change 2 -> 3.")
    print("All rates over sigma^2 / (2S); mean (sd) over 6 seeds.  Random-walk value of the factor: "
          f"{factor(52, 52):.3f}\n")
    print("case                                   | cohort slide  | reordering    | occupant loss | reordering/slide")
    for label, kw in (("random walks only", {}),
                      ("plus AR(1) swings, rho 0.8, variance 0.05", dict(noise=0.05, rho=0.8))):
        a = np.array([synthetic(s, **kw) for s in range(6)])
        m, s = a.mean(0), a.std(0)
        print(f"{label:<38} | " + " | ".join(f"{m[i]:.3f} ({s[i]:.3f})" for i in range(4)))


def part2():
    print("\nPART 2. Step 1 cells: median over cut-offs K/16..K of the reordering gain per endpoint per week,")
    print("by block length L and spacing D, as measured and divided by the random-walk factor.\n")
    print("panel                      | L   | D   | pairs | measured            | factor | divided by the factor")
    for label, files in (("reddit_comments_ext", ("reddit_comments_ext", "reddit_comments_ext__block26")),
                         ("reddit_submissions_long", ("reddit_submissions_long", "reddit_submissions_long__block26")),
                         ("facebook_a", ("facebook_a",))):
        allv = []
        for f in files:
            c = json.loads((CELLS / f"m1__{f}.json").read_text())
            K, L = c["K"], c["block"]
            by = {}
            seen = set()
            for p in c["boundaries"]["1.0"]["blocks"]["pairs"]:
                if (p["start"], p["end"]) in seen:
                    continue
                seen.add((p["start"], p["end"]))
                rows = [x for x in p["accounting"] if K / 16.5 <= x["k"] <= K]
                by.setdefault(p["weeks"], []).append(float(np.median([x["R"] / (x["k"] * p["weeks"]) for x in rows])))
            for D in sorted(by):
                v = by[D]
                fac = factor(L, D)
                allv += [x / fac for x in v]
                meas = f"{min(v):.4f}" if len(v) == 1 else f"{min(v):.4f} to {max(v):.4f}"
                corr = f"{min(v) / fac:.4f}" if len(v) == 1 else f"{min(v) / fac:.4f} to {max(v) / fac:.4f}"
                print(f"{label:<26} | {L:<3} | {D:<3} | {len(v):<5} | {meas:<19} | {fac:.3f}  | {corr}")
        print(f"{label:<26} | all conventions, divided by the factor: {min(allv):.4f} to {max(allv):.4f}, "
              f"median {np.median(allv):.4f}\n")


if __name__ == "__main__":
    part1()
    part2()
