"""Check of a reviewer's claim (T2 Opus, threat 2): in homogeneous random growth the hierarchy's shape relaxes
about eight times more slowly than lasting membership turns over. Uses the GLLM (2016, eq. 13) reflecting-barrier
spectral gap lambda = sigma^2 / (8 S^2), evaluated at the measured lasting variance sigma_k^2 and local slope S_k
of the balance-law cells (2019-21 primary pairs), and the time S^2 / sigma^2 for lasting moves to span one slope
unit. Derived outputs only; no data file is opened.

    python llm_fitting/runs/2026-10-19_literature_and_design_reviews/gllm_relaxation_check.py
"""
import json
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[3]
for pn in ("reddit_comments_ext", "reddit_submissions_long"):
    d = json.loads((ROOT / "llm_fitting/runs/2026-10-14_balance_law/cells" / f"law__{pn}.json").read_text())
    prs = [p for p in d["pairs"] if p["primary"]]
    print(pn, "primary pairs", [(p["start"], p["end"]) for p in prs])
    for k in sorted(r["k"] for r in prs[0]["rows"]):
        rows = [next(r for r in p["rows"] if r["k"] == k) for p in prs]
        s2 = np.mean([r["sigma2"] for r in rows])
        S = np.mean([r["S"] for r in rows])
        Ss = np.mean([r["S_star"] for r in rows])
        lam = s2 / (8 * S ** 2)
        print(f"  k={k:5d}  S={S:.2f}  S*={Ss:.2f}  sigma2/week={s2:.4f}  1/lambda={1 / lam / 52:5.1f} yr  "
              f"S^2/sigma2={S ** 2 / s2 / 52:4.1f} yr")
