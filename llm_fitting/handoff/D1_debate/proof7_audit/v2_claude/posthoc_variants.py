"""POST HOC (not in DESIGN.md): two further restrictions evaluated from the saved Jacobian and
covariance in sensitivity.json, after the owner asked for assumptions that would tighten practical
use. Same caveats as sensitivity.py: delta method, iid retained records, small-epsilon limit.

  X1: rank feedback known to be absent (d = 0): parameters (g, v, a, k), five moments, efficient weights.
  X2: d = 0 and the OU nuisance (k, v) known: parameters (g, a).
Run:  python posthoc_variants.py  ->  posthoc_variants.json
"""
import json
import numpy as np

Z95, REL = 1.959963984540054, 0.10
S = json.load(open("sensitivity.json"))
OUT = {}
for name, r in S.items():
    A = np.array(r["C3"]["A_columns_g_v_a_k_d"]); Sig = np.array(r["C4"]["Sigma"])
    sc = np.array([r["scales"][p] for p in ("g", "v", "a", "k", "d")])
    Si = np.linalg.inv(Sig)
    def N(cols):
        J = A[:, cols]; cov = np.linalg.inv(J.T @ Si @ J)
        return {("g", "v", "a", "k", "d")[c]: float((Z95 / REL) ** 2 * cov[i, i] / sc[c] ** 2) for i, c in enumerate(cols)}
    OUT[name] = dict(X1_d_known=N([0, 1, 2, 3]), X2_d_and_nuisance_known=N([0, 2]))
    print(name, {k: f"{v:.3g}" for k, v in OUT[name]["X1_d_known"].items()}, "|", {k: f"{v:.3g}" for k, v in OUT[name]["X2_d_and_nuisance_known"].items()})
json.dump(OUT, open("posthoc_variants.json", "w"), indent=1)
