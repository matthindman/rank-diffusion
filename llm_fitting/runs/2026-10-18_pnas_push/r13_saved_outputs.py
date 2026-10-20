"""R13a and R13e of PLAN.md, from saved outputs only (no data file opened).
R13a: the head's shape (K/64) and slopes at 1, 2 and 5 years in the corrected runs (R11, __noentry): flat start and the
three worlds from the observed 2019 ladder, with 90% bands across 20 runs.
R13e: the turnover-profile exponent b in z = a y^b (z = new entries into the top y in a year, z = f(y) y), fitted on
log-log over y = K/64 .. K/2 (and with the top ten and 25 where available), data (turnover_kinematics.json; A7 top-10/25)
and the 20-seed worlds (null_worlds_r7_summary.json).

    python llm_fitting/runs/2026-10-18_pnas_push/r13_saved_outputs.py
"""
import json
from pathlib import Path
import numpy as np

HERE = Path(__file__).resolve().parent
out = {}
print("R13a: head shape U_{K/64}-U_K (log units), mean [5-95%] over 20 runs")
for pn in ("reddit_comments_ext", "reddit_submissions_long"):
    f = json.loads((HERE / f"churn_counterfactual__{pn}__flat20__noentry.json").read_text())["flat_start"]
    L = json.loads((HERE / f"churn_counterfactual__{pn}__long30__noentry.json").read_text())
    sh, q = np.array(f["shape_by_year"])[:, 0], np.array(f["shape_q"])[:, :, 0]
    row = dict(flat_start={y: [float(sh[y]), float(q[0][y]), float(q[1][y])] for y in (1, 2, 5, 10)})
    st = float(L["start"]["shape"][0])
    row["start_2019"] = st
    for w in ("churn_slide", "slide_only", "churn_only"):
        s, qq = np.array(L[w]["shape_by_year"])[:, 0], np.array(L[w]["shape_q"])[:, :, 0]
        row[w] = {y: [float(s[y]), float(qq[0][y]), float(qq[1][y]), float(s[y] / st - 1)] for y in (1, 2, 5)}
    out[pn] = row
    print(f"  {pn}: observed 2019 {st:.2f}; flat start yr1/2/5/10 " + ", ".join(f"{row['flat_start'][y][0]:.2f} [{row['flat_start'][y][1]:.2f}-{row['flat_start'][y][2]:.2f}]" for y in (1, 2, 5, 10)))
    for w in ("churn_slide", "slide_only", "churn_only"):
        print(f"    {w:12s} yr1/2/5 " + ", ".join(f"{row[w][y][0]:.2f} [{row[w][y][1]:.2f}-{row[w][y][2]:.2f}] ({row[w][y][3]:+.1%})" for y in (1, 2, 5)))

def bfit(ys, fs):
    ys, fs = np.asarray(ys, float), np.asarray(fs, float)
    ok = (fs > 0) & np.isfinite(fs)
    return float(np.polyfit(np.log(ys[ok]), np.log(fs[ok] * ys[ok]), 1)[0])
print("R13e: turnover-profile exponent b (neutral Wright-Fisher benchmark 0.86, Acerbi & Bentley 2014)")
kin = json.loads((HERE / "turnover_kinematics.json").read_text())
out["b_data"] = {}
for pn in ("reddit_submissions_long", "reddit_comments_ext"):
    rows = kin[pn]
    ks = sorted({r["k"] for r in rows})
    f = [np.mean([r["obs"] for r in rows if r["k"] == k]) for k in ks]
    out["b_data"][pn] = dict(ks=ks, f=f, b=bfit(ks, f))
    print(f"  data {pn}: k {ks[0]}..{ks[-1]}, f {f[0]:.3f}..{f[-1]:.3f}, b = {out['b_data'][pn]['b']:.3f}")
summ = json.loads((HERE / "null_worlds_r7_summary.json").read_text())
out["b_worlds"] = {}
for w, d in summ.items():
    t = d.get("turnover") if isinstance(d, dict) else None
    if not t:
        continue
    ks = sorted(int(k) for k in t.keys())
    fs = [float(t[str(k)]["mean"]) for k in ks]
    sel = [i for i, k in enumerate(ks) if k >= 78]
    out["b_worlds"][w] = dict(ks=ks, f=fs, b_body=bfit([ks[i] for i in sel], [fs[i] for i in sel]), b_all=bfit(ks, fs))
    print(f"  world {w:10s}: b (K/64..K/2) = {out['b_worlds'][w]['b_body']:.3f};  b (10..K/2) = {out['b_worlds'][w]['b_all']:.3f}")
(HERE / "r13_saved_outputs.json").write_text(json.dumps(out, indent=1, default=float))
