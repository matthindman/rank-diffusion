"""Post hoc (2026-10-21, round-9 Opus review, iteration 3): medians over runs for the worlds' level-sum statistics.

Head shares and Gomez's terms sum activity in levels, expm1(log activity). In one to four of 20 runs of most worlds a
single simulated week of the passing swing (Student-t innovations scaled by the unit's amplitude) lies far above
anything observed and swamps such a sum (`r18c_headshare_diag.py`), which moves the mean over runs and the 5-95% range.
This reads only saved outputs and reports, for every arm, the median and quartiles over runs, the count of runs whose
value lies more than 0.15 from the median, and, for the out-of-period forecasts, paired differences against the
matched control on the same seeds.

  r12_oop__*.json, r16_oop__*.json       2022 head share out of period (`shares`, one per run)
  null_world_r7__<world>__s*.json        Gomez's within term of the top 78 (`gomez`, one row per pair of years)

    python levelsum_medians.py            writes levelsum_medians.json
"""
import glob
import json
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent


def summ(v):
    v = np.asarray(v, float)
    m = float(np.median(v))
    return dict(n=int(v.size), median=m, q25=float(np.quantile(v, 0.25)), q75=float(np.quantile(v, 0.75)), mean=float(v.mean()),
                q05=float(np.quantile(v, 0.05)), q95=float(np.quantile(v, 0.95)), far_from_median=int(np.sum(np.abs(v - m) > 0.15)))


def main():
    out = dict(oop={}, oop_paired={}, gomez_within_k78={})
    for f in sorted(glob.glob(str(HERE / "r12_oop__*.json")) + glob.glob(str(HERE / "r16_oop__*.json"))):
        out["oop"][Path(f).stem] = summ(json.loads(Path(f).read_text())["shares"])
    for pn in ("reddit_submissions_long", "reddit_comments_ext"):
        c = np.array(json.loads((HERE / f"r12_oop__{pn}__control_num.json").read_text())["shares"], float)
        for v in ("fixed", "decay", "factor"):
            x = np.array(json.loads((HERE / f"r12_oop__{pn}__{v}.json").read_text())["shares"], float)
            out["oop_paired"][f"{pn}__{v}_minus_control_num"] = summ(x - c)
    for w in ("sb_data", "rdm", "firm_data"):
        rows = [r for f in sorted(glob.glob(str(HERE / f"null_world_r7__{w}__s*.json"))) for r in json.loads(Path(f).read_text())["gomez"]
                if r["k"] == 78]
        out["gomez_within_k78"][w] = dict(all=summ([r["current_within"] for r in rows]),
                                          steady=summ([r["current_within"] for r in rows if abs(r["share_change"]) < 0.05]))
    (HERE / "levelsum_medians.json").write_text(json.dumps(out, indent=1))
    for k, s in out["oop"].items():
        print(f"{k:52s} median {s['median']:.3f} [{s['q25']:.3f}, {s['q75']:.3f}]  mean {s['mean']:.3f}  far {s['far_from_median']}")
    for k, s in out["oop_paired"].items():
        print(f"{k:52s} median {s['median']:+.3f} [{s['q25']:+.3f}, {s['q75']:+.3f}]  mean {s['mean']:+.3f}")
    for k, s in out["gomez_within_k78"].items():
        print(f"gomez within k78 {k:10s} median {s['all']['median']:+.3f} [{s['all']['q25']:+.3f}, {s['all']['q75']:+.3f}] mean {s['all']['mean']:+.3f}; "
              f"steady median {s['steady']['median']:+.3f}")


if __name__ == "__main__":
    main()
