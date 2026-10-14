"""Arithmetic on recorded rate-law cells (no data file opened): sustainable slope
with and without the entry term, and an illustrative climb from the entrants'
median rank to K/64 under a drifted Brownian approximation."""
import json, math
from statistics import NormalDist
Phi = NormalDist().cdf
med_entry = {"reddit_comments_ext": 7999, "reddit_submissions_long": 3520}  # §2z-bv, 2021→22
for p in ["reddit_comments_ext", "reddit_submissions_long"]:
    d = json.load(open(f"llm_fitting/runs/2026-10-14_balance_law/cells/law__{p}.json"))
    for q in [q for q in d["pairs"] if q["primary"]]:
        rows = [r for r in q["rows"] if r["k"] < d["K"]]
        print(p, f"blocks {q['start']}->{q['end']}")
        for r in rows:
            s0 = r["sigma2"] / (2 * r["sbar"])
            print(f"   k={r['k']:>5}  S={r['S']:.2f}  S*={r['S_star']:.2f}  S*(no entry)={s0:.2f}  ratio={s0/r['S_star']:.2f}  entry={r['entry']:+.4f}")
        # climb: from median entry rank up to the smallest k, trapezoid of S over log rank
        allr = sorted(q["rows"], key=lambda r: r["k"])
        top, m = allr[0]["k"], med_entry[p]
        pts = [(r["k"], r["S"], r["sbar"], r["sigma2"]) for r in allr]
        h = mu_int = s2_int = 0.0; L = 0.0
        for (k1, S1, m1, v1), (k2, S2, m2, v2) in zip(pts, pts[1:]):
            lo, hi = k1, min(k2, m)
            if hi <= lo: continue
            f = (hi - lo) / (k2 - k1) if k2 > k1 else 0
            dl = math.log(hi / lo)
            h += dl * (S1 + S2) / 2; mu_int += dl * (m1 + m2) / 2; s2_int += dl * (v1 + v2) / 2; L += dl
        mu, s2 = mu_int / L, s2_int / L
        pever = math.exp(-2 * mu * h / s2)
        def within(t):
            s = math.sqrt(s2 * t)
            return Phi((-h - mu * t) / s) + math.exp(-2 * mu * h / s2) * Phi((-h + mu * t) / s)
        print(f"   climb rank {m} -> {top}: gap {h:.2f} log units (x{math.exp(h):.0f}), drift -{mu:.4f}/wk, var {s2:.4f}/wk;"
              f" P(ever) {pever:.3f} (k/K-type benchmark {top/m:.3f}); P(within 5y) {within(260):.4f}, 10y {within(520):.4f}, 20y {within(1040):.4f};"
              f" mean time given success {h/mu/52:.0f} years")
