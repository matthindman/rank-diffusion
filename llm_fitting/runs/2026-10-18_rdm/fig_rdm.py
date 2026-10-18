"""Figure for the moving-home RDM validation (saved outputs only).
  A  Gate v1: model relative error by panel, RDM against model A's recorded reproduction and persistence
  B  no return to own level: slope of year-2 change on year-1 deviation, data against RDM and A (90% bands)
  C  turnover by scale: one-step retention of the top k, data against RDM and A
  D  recovery of the size-law exponent and the lasting sd

    python llm_fitting/runs/2026-10-18_rdm/fig_rdm.py
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

HERE = Path(__file__).resolve().parent
L = lambda f: json.loads((HERE / f).read_text()) if (HERE / f).exists() else None  # noqa: E731
A_REF = {"facebook_a": (0.103, 0.044), "reddit_comments": (0.137, 0.056), "reddit": (0.129, 0.028)}   # GATE_LEDGER, Codex V3 reproductions at 20 reps, 2,000 draws
LAB = {"facebook_a": "Facebook pages*", "reddit_comments": "Reddit comments", "reddit": "Reddit submissions (short)"}
fig, ax = plt.subplots(1, 4, figsize=(18, 4.3))
a = ax[0]
x = 0
for pn in ("reddit_comments", "reddit", "facebook_a"):
    g = L(f"gate__{pn}.json")
    if g is None:
        continue
    a.errorbar(x - 0.15, g["model_rel"], yerr=g["model_rel_sd"], fmt="o", color="#c0392b", capsize=3, label="moving-home RDM" if x == 0 else None)
    a.errorbar(x, A_REF[pn][0], yerr=A_REF[pn][1], fmt="s", color="#1f6fb4", capsize=3, label="model A (frozen, recorded)" if x == 0 else None)
    a.errorbar(x + 0.15, g["base_rel"], yerr=g["base_rel_sd"], fmt="^", color="grey", capsize=3, label="persistence" if x == 0 else None)
    x += 1
a.set_xticks(range(x))
a.set_xticklabels([LAB[p] for p in ("reddit_comments", "reddit", "facebook_a") if L(f"gate__{p}.json")], fontsize=7)
a.set_ylabel("Gate v1 relative error (mean ± sd over 5 splits)")
a.set_title("A  Held-out weekly movement", loc="left", fontsize=10)
a.legend(fontsize=7)
a = ax[1]
y = 0
names = []
for pn in ("reddit_comments_ext", "reddit_submissions_long", "facebook_a"):
    s = L(f"structure__{pn}.json")
    if s is None:
        continue
    a.plot([s["data"]["no_return_slope"]], [y], "k*", ms=12, label="data" if y == 0 else None)
    for lasting, col, dy in (("movinghome", "#c0392b", 0.15), ("home", "#1f6fb4", -0.15)):
        v = s[lasting]["no_return_slope"]
        a.errorbar(v["mean"], y + dy, xerr=[[v["mean"] - v["q"][0]], [v["q"][1] - v["mean"]]], fmt="o", color=col, capsize=3,
                   label=("moving-home RDM" if lasting == "movinghome" else "model A dynamics") if y == 0 else None)
    names.append(pn)
    y += 1
a.set_yticks(range(y))
a.set_yticklabels(names, fontsize=7)
a.axvline(0, color="grey", lw=0.8)
a.set_xlabel("slope of next-block change on last-block change")
a.set_title("B  No return to an endpoint's own level", loc="left", fontsize=10)
a.legend(fontsize=7)
a = ax[2]
for pn, mk in (("reddit_comments_ext", "o"), ("reddit_submissions_long", "s"), ("facebook_a", "^")):
    s = L(f"structure__{pn}.json")
    if s is None:
        continue
    K = s["K"]
    ks = [K // 64, K // 16, K // 8, K]
    keys = [f"ret_{k}_1" for k in ks]
    a.plot(np.array(ks) / K, [s["data"][k] for k in keys], mk + "-", color="k", lw=1)
    a.plot(np.array(ks) / K, [s["movinghome"][k]["mean"] for k in keys], mk + "--", color="#c0392b", lw=1)
    a.plot(np.array(ks) / K, [s["home"][k]["mean"] for k in keys], mk + ":", color="#1f6fb4", lw=1)
a.plot([], [], "k-", label="data")
a.plot([], [], "--", color="#c0392b", label="moving-home RDM")
a.plot([], [], ":", color="#1f6fb4", label="model A dynamics")
a.set_xscale("log")
a.set_xlabel("top k / K")
a.set_ylabel("share of the top k still in it one block later")
a.set_title("C  Turnover by scale", loc="left", fontsize=10)
a.legend(fontsize=7)
a = ax[3]
rc = L("recovery.json")
if rc:
    for lab, col in (("fitted", "#c0392b"), ("null_beta0", "#7f8c8d")):
        for T, mk in ((86, "o"), (160, "s")):
            run = rc["runs"].get(f"{lab}_T{T}")
            if run is None:
                continue
            tb = rc["truth"]["beta"] if lab == "fitted" else 0.0
            b = [r["beta"] for r in run["rows"]]
            a.scatter([tb] * len(b), b, color=col, marker=mk, s=20, label=f"{lab}, T={T}")
    lim = [-0.15, 0.35]
    a.plot(lim, lim, color="grey", lw=1)
    a.set_xlim(lim)
    a.set_ylim(lim)
    a.set_xlabel("true size-law exponent β")
    a.set_ylabel("recovered β")
    a.legend(fontsize=7)
a.set_title("D  Recovery through the full pipeline", loc="left", fontsize=10)
fig.text(0.01, 0.005, "*Facebook gate used during development (contaminated). In-sample structure: parameters estimated on the whole panel; "
         "simulations from the same communities' state at the end of the first block.", fontsize=6.5, color="grey")
fig.tight_layout(rect=(0, 0.03, 1, 1))
fig.savefig(HERE / "fig_rdm.png", dpi=150)
print("written fig_rdm.png")
