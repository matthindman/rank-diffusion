"""Table 1 of the manuscript from v9.1 (owner, 2026-10-21: "a common or nearly-common model across these different
platforms"): one table, two platforms, the same model's worlds. Every cell, the data's included, is computed by the
same code within its platform (Facebook: `r18_facebook.fb_stats`; Reddit submissions: `null_worlds.stats` and R17's D,
as `r17_table1.py`), and each world's value is followed by where the data's value falls relative to the world's 5-95%
range of runs (in, above, below; edge: within 0.005 of a bound).

  Facebook: r18_data__facebook_a.json, r18_summary.json (worlds calib, control, flat)
  Reddit submissions: r17_table1.json (worlds rdm, control, sb_flat, joint, decline)

    python r18_table1.py          writes r18_table1.json and prints the LaTeX rows
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
COLS = ("calibrated", "control", "flat", "joint", "decline")


def code(x, b):
    lo, hi = b["q05"], b["q95"]
    if abs(x - lo) < 0.005 or abs(x - hi) < 0.005:
        return "edge"
    return "in" if lo <= x <= hi else ("above" if x > hi else "below")


def fmt(v, signed):
    s = f"{v:+.2f}" if signed else f"{v:.2f}"
    return s.replace("+", "$+$").replace("-", "$-$")


def cell(x, b, signed):
    return "---" if b is None else f"{fmt(b['mean'], signed)} {code(x, b)}"


def main():
    fb = json.loads((HERE / "r18_summary.json").read_text())
    fd, fw = fb["data"], fb["worlds"]
    fwc = {"calibrated": fw["calib"], "control": fw["control"], "flat": fw["flat"], "joint": None, "decline": None}
    rd = json.loads((HERE / "r17_table1.json").read_text())
    rdd, rw = rd["data"], rd["worlds"]
    rwc = {"calibrated": rw["rdm"], "control": rw["control"], "flat": rw["sb_flat"], "joint": rw["joint"], "decline": rw["decline"]}
    rows = []
    fb_rows = [("turnover, top $K/64$", fd["turnover"]["54"], lambda w: w["turnover"]["54"], False),
               ("turnover, top $K/2$", fd["turnover"]["1750"], lambda w: w["turnover"]["1750"], False),
               ("turnover, top 10", fd["turnover"]["10"], lambda w: w["turnover"]["10"], False),
               ("crossings / predicted", fd["boundary_pooled"], lambda w: w["boundary_pooled"], False),
               ("persistence, gap of one window", fd["gapped"]["4"], lambda w: w["gapped"]["4"], True),
               ("asymmetry $D$", fd["D_FB"]["D"], lambda w: w["D_FB"], True)]
    rd_rows = [("turnover, top $K/64$", rdd["turn78"], lambda w: w.get("turn78"), False),
               ("turnover, top $K/2$", rdd["turn2500"], lambda w: w.get("turn2500"), False),
               ("turnover, top 10", rdd["turn10"], lambda w: w.get("turn10"), False),
               ("crossings / predicted", rdd["boundary"], lambda w: w.get("boundary"), False),
               ("persistence, gap of one window", rdd["gap13"], lambda w: w.get("gap13"), True),
               ("asymmetry $D$", rdd["D"], lambda w: w.get("D"), True)]
    out = dict(facebook=[], reddit_submissions=[])
    for key, title, spec, worlds in (("facebook", "Facebook pages (half-year changes; $K$ = 3,500)", fb_rows, fwc),
                                     ("reddit_submissions", "Reddit submissions (year-long changes; $K$ = 5,000)", rd_rows, rwc)):
        lines = [f"\\multicolumn{{7}}{{l}}{{\\textit{{{title}}}}}\\\\"]
        for lab, x, get, signed in spec:
            cells = [fmt(x, signed)]
            for c in COLS:
                w = worlds[c]
                cells.append(cell(x, get(w) if w is not None else None, signed))
            lines.append(f"{lab} & " + " & ".join(cells) + r" \\")
        out[key] = lines
        rows += lines
    (HERE / "r18_table1.json").write_text(json.dumps(out, indent=1))
    print("\n".join(rows))


if __name__ == "__main__":
    main()
