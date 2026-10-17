"""One quantity, several estimators: lasting variance by rank on four ladders (Fable 5.1 review; post hoc;
saved outputs only, no data file opened).

  - variance route: the cohort estimator of 2z-cm (sigma2 at the boundary; interval from c_q x 2S);
  - variogram: 2z-co (Reddit; sigma2 = 2 S c_vg);
  - crossing-implied: 2 S R~, the variance the measured crossings imply under the boundary formula.
The ratio test of 2z-cm is the ratio of the third to the first.  The local size-variance exponent,
beta = -d log sd / d level = (d log sigma2 / d log k) / (2 S), is read off the crossing-implied curve (the
smooth one) by least squares on log sigma2 against log k over K/16 ... K, with the mean S there.

    python llm_fitting/runs/2026-10-18_variance_by_rank/variance_by_rank.py
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

HERE = Path(__file__).resolve().parent
R = HERE.parent
L = lambda p: json.loads(Path(p).read_text())  # noqa: E731
SETS = [("reddit_comments_ext", R / "2026-10-14_boundary_law/cells/boundary__reddit_comments_ext.json", "Reddit comments (K = 12,500)"),
        ("reddit_submissions_long", R / "2026-10-14_boundary_law/cells/boundary__reddit_submissions_long.json", "Reddit submissions (K = 5,000)"),
        ("facebook_a", R / "2026-10-14_boundary_law/cells/boundary__facebook_a__block13.json", "Facebook pages (K = 3,500; 13-week blocks)"),
        ("instagram_hm", R / "2026-10-15_instagram/ig_boundary__instagram_hm.json", "Instagram (a-matching; 14→28 weeks)")]
out = {}
fig, ax = plt.subplots(1, 4, figsize=(17, 4.2))
for a, (pn, f, lab) in zip(ax, SETS):
    d = L(f)
    d = d["apparatus"] if "apparatus" in d else d
    K = d["K"]
    P = d["pooled"]
    k = np.array([r["k"] for r in P], float)
    S = np.array([r["S"] for r in P])
    vr = np.array([r["sigma2"] for r in P])
    vr_q = np.array([[2 * s * q for q in r["c_q"]] for s, r in zip(S, P)])
    vx = 2 * S * np.array([r["R_lasting"] for r in P])
    vx_q = np.array([[2 * s * q for q in r["R_lasting_q"]] for s, r in zip(S, P)])
    rec = dict(K=K, k=k.tolist(), S=S.tolist(), variance_route=vr.tolist(), crossing_implied=vx.tolist())
    a.errorbar(k * 0.95, 1000 * vr, yerr=[1000 * (vr - vr_q[:, 0]), 1000 * (vr_q[:, 1] - vr)], fmt="s--", color="#1f6fb4", capsize=2, label="variance route (cohort)")
    a.errorbar(k * 1.05, 1000 * vx, yerr=[1000 * (vx - vx_q[:, 0]), 1000 * (vx_q[:, 1] - vx)], fmt="o-", color="k", capsize=2, label="implied by crossings, 2S·R̃")
    vf = R / "2026-10-15_variogram/cells" / f"vario__{pn}.json"
    if vf.exists():
        v = L(vf)["pooled"]["cells"]
        vv = np.array([2 * s * r["c"] for s, r in zip(S, v)])
        rec["variogram"] = vv.tolist()
        a.plot(k, 1000 * vv, "^:", color="#e07b00", label="variogram")
    sel = k >= K / 16 - 1
    b = np.polyfit(np.log(k[sel]), np.log(vx[sel]), 1)[0]
    beta = b / (2 * S[sel].mean())
    rec["dlogvar_dlogk"] = float(b)
    rec["beta_sd"] = float(beta)
    disagree = np.abs(np.log(vr[sel] / vx[sel]))
    rec["median_abs_log_gap_route_vs_crossings"] = float(np.median(disagree))
    out[pn] = rec
    a.set_xscale("log")
    a.set_title(lab, fontsize=9.5)
    a.set_xlabel("rank k (cut-off)")
    a.set_ylabel("lasting variance per 1,000 weeks")
    a.text(0.03, 0.04, f"d log σ²/d log k = {b:.2f}; size–variance β = {beta:.2f}", transform=a.transAxes, fontsize=7.5)
    a.legend(fontsize=7, loc="upper left")
    print(f"{pn}: d log var / d log k (K/16..K, crossing-implied) {b:.3f}; beta (sd vs size) {beta:.3f}; "
          f"median |log(route / implied)| {np.median(disagree):.3f}", flush=True)
    for kk, s, x1, x2 in zip(k, S, vr, vx):
        print(f"   k={int(kk):>6}  S={s:.2f}  route {1000 * x1:6.2f}  implied {1000 * x2:6.2f}  per 1,000 weeks")
fig.suptitle("Lasting variance by rank: the cohort estimate, the variogram, and the variance the crossings imply", fontsize=11)
fig.tight_layout()
fig.savefig(HERE / "fig_variance_by_rank.png", dpi=150)
(HERE / "variance_by_rank.json").write_text(json.dumps(out, indent=1))
