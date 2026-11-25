#!/usr/bin/env python3
"""Compare a candidate frozen-gate archive against Round 3 and Round 4 (Astra S2 + Astra filter-only S3).
Usage: diag_gate_compare.py <candidate_frozen_gate_results.json> [--moments]"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
cand_path = Path(sys.argv[1]).resolve()
show_moments = "--moments" in sys.argv
cand = json.loads(cand_path.read_text())
r3 = json.loads((BASE / "round3_frozen_gate_results.json").read_text())
r4s2 = json.loads((BASE / "astra_v2" / "candidate_frozen_gate_results.json").read_text())
r4s3 = json.loads((BASE / "astra_v2" / "diag_ablation.json").read_text())  # filter-only S3 in s2_* slots

print(f"{'panel':24s} | {'persist':>8s} | {'R3 S2':>8s} {'R4 S2':>8s} {'CAND S2':>8s} | {'R3 S3':>8s} {'R4 S3':>8s} {'CAND S3':>8s}")
for pn, cv in cand.items():
    a = r3[pn]
    b2 = r4s2[pn]
    b3 = r4s3[pn]["gate"]
    print(f"{pn:24s} | {cv['base_rel']:8.3f} | {a['s2_rel']:8.3f} {b2['s2_rel']:8.3f} {cv['s2_rel']:8.3f} | {a['s3_rel']:8.3f} {b3['s2_rel']:8.3f} {cv['s3_rel']:8.3f}")
    print(f"{'':24s} |          | {'':8s} {'±'+format(b2['s2_sd'],'.3f'):>8s} {'±'+format(cv['s2_sd'],'.3f'):>8s} | {'':8s} {'±'+format(b3['s2_sd'],'.3f'):>8s} {'±'+format(cv['s3_sd'],'.3f'):>8s}")
    print(f"{'':24s} |          | {'':8s} {str(int(b2['s2_cov']*100))+'%/'+str(b2['s2_wins_base']):>8s} {str(int(cv['s2_cov']*100))+'%/'+str(cv['s2_wins_base']):>8s} | {'':8s} {str(int(b3['s2_cov']*100))+'%/'+str(b3['s2_wins_base']):>8s} {str(int(cv['s3_cov']*100))+'%/'+str(cv['s3_wins_base']):>8s}")
    # paired per-split
    d2 = [c["s2_rel"] - b["s2_rel"] for c, b in zip(cv["splits"], b2["splits"])]
    d3 = [c["s3_rel"] - b["s2_rel"] for c, b in zip(cv["splits"], b3["splits"])]
    print(f"{'':24s}   paired dS2 vs R4: {np.mean(d2):+.3f} ± {np.std(d2):.3f} (cand better on {sum(d < 0 for d in d2)}/5) | dS3 vs R4: {np.mean(d3):+.3f} ± {np.std(d3):.3f} ({sum(d < 0 for d in d3)}/5)")
    if show_moments:
        keys = [f"dRank{h}" for h in cv["hor"]] + ["RACF1", "coll1", "coll5", "coll20"]
        print(f"      {'T0':>4s} {'moment':>7s} | {'emp':>7s} {'hist':>7s} | {'R4 S2':>7s} {'CAND S2':>7s} | {'R4 S3':>7s} {'CAND S3':>7s}")
        for cs, bs2, bs3 in zip(cv["splits"], b2["splits"], b3["splits"]):
            for k in keys:
                em = cs["em"].get(k, np.nan)
                if not np.isfinite(em):
                    continue
                print(f"      {cs['T0']:4d} {k:>7s} | {em:7.3f} {cs['bm'].get(k, np.nan):7.3f} | {bs2['sm2'].get(k, np.nan):7.3f} {cs['sm2'].get(k, np.nan):7.3f} | {bs3['sm2'].get(k, np.nan):7.3f} {cs['sm3'].get(k, np.nan):7.3f}")
        # mean over splits
        print(f"      {'MEAN':>4s} {'moment':>7s} | {'emp':>7s} {'hist':>7s} | {'R4 S2':>7s} {'CAND S2':>7s} | {'R4 S3':>7s} {'CAND S3':>7s}")
        for k in keys:
            def m(lst, slot):
                v = [s[slot].get(k, np.nan) for s in lst]
                return float(np.nanmean(v)) if np.any(np.isfinite(v)) else np.nan
            print(f"      {'':>4s} {k:>7s} | {m(cv['splits'],'em'):7.3f} {m(cv['splits'],'bm'):7.3f} | {m(b2['splits'],'sm2'):7.3f} {m(cv['splits'],'sm2'):7.3f} | {m(b3['splits'],'sm2'):7.3f} {m(cv['splits'],'sm3'):7.3f}")
