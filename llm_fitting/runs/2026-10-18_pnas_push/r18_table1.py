"""Table 1 of the manuscript from v9.1 (owner, 2026-10-21: "a common or nearly-common model across these different
platforms"): one table, two platforms, the same model's worlds. Every cell, the data's included, is computed by the
same code within its platform (Facebook: `r18_facebook.fb_stats`; Reddit submissions: `null_worlds.stats` and R17's D,
as `r17_table1.py`), and each world's value is followed by where the data's value falls relative to the world's 5-95%
range of runs (in, above, below; edge: within 0.005 of a bound).

  Facebook: r18_data__facebook_a.json, r18_summary.json (worlds calibb0.019, controlb0.019, flatb0.019; R18-C)
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
    # R18-C: every Facebook column at the two-step size exponent 0.019 (the structure-function 0.097 is biased on 86 weeks)
    fwc = {"calibrated": fw["calibb0.019"], "control": fw["controlb0.019"], "flat": fw["flatb0.019"], "joint": None, "decline": None}
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
    out["si_b0.019"] = si_rows(fd, fw, ("calibb0.019", "controlb0.019", "flatb0.019"))
    out["si_b0.097"] = si_rows(fd, fw, ("calib", "control", "flat"))
    (HERE / "r18_table1.json").write_text(json.dumps(out, indent=1))
    print("\n".join(rows))
    print("\n".join(out["si_b0.019"]))


def si_rows(fd, fw, arms):
    """The SI table's rows (SI "The same model on Facebook"): mean [5-95% over runs] for each arm; the data's
    persistence and D with the bootstrap bands of R18-A1. The head share, a level sum that one extreme simulated week can
    swamp (r18c_headshare_diag.py), is given as the median over runs."""
    boot = json.loads((HERE / "r18_boot__facebook_a.json").read_text())

    def band(b, signed):
        return f"{fmt(b['mean'], signed)} [{fmt(b['q05'], signed)}, {fmt(b['q95'], signed)}]"

    def dband(x, lohi, signed=True):
        return f"{fmt(x, signed)} [{fmt(lohi[0], signed)}, {fmt(lohi[-1], signed)}]"

    spec = [(f"turnover, top {k}" + (f" ({lab})" if lab else ""), fmt(fd["turnover"][kk], False), lambda w, kk=kk: band(w["turnover"][kk], False))
            for k, kk, lab in (("10", "10", ""), ("54", "54", "$K/64$"), ("219", "219", "$K/16$"), ("875", "875", "$K/4$"), ("1,750", "1750", "$K/2$"))]
    spec += [("rank 1 changes hands, share of weeks", fmt(fd["occupant_change"]["1"], False), lambda w: band(w["occupant_change"]["1"], False)),
             ("rank 10 changes hands, share of weeks", fmt(fd["occupant_change"]["10"], False), lambda w: band(w["occupant_change"]["10"], False)),
             ("level of rank 54 above rank $K$, first block", fmt(fd["ladder"][0]["54"], False), lambda w: band(w["ladder_first"]["54"], False)),
             ("its change, first to last block", fmt(fd["ladder_change"]["54"], True), lambda w: band(w["ladder_change"]["54"], True)),
             ("crossings / predicted, $K/16$--$K$", fmt(fd["boundary_pooled"], False), lambda w: band(w["boundary_pooled"], False))]
    spec += [(f"persistence, gap {g} weeks", dband(fd["gapped"][g], [boot["gapped_band"][g][0], boot["gapped_band"][g][2]]), lambda w, g=g: band(w["gapped"][g], True))
             for g in ("0", "4", "8")]
    spec += [("asymmetry $D$ (half-year changes)", dband(fd["D_FB"]["D"], [boot["D_FB_band"][0], boot["D_FB_band"][2]]), lambda w: band(w["D_FB"], True)),
             (r"\quad $p_{\rm fall}$ / $p_{\rm rise}$", f"{fmt(fd['D_FB']['p_fall'], True)} / {fmt(fd['D_FB']['p_rise'], True)}",
              lambda w: f"{fmt(w['p_fall']['mean'], True)} / {fmt(w['p_rise']['mean'], True)}"),
             ("head share, last half-year (median of runs)", fmt(fd["head_share"], False),
              lambda w: band(dict(w["head_share"], mean=w["head_share"]["median"]), False))]
    return [f"{lab} & {x} & " + " & ".join(get(fw[a]) for a in arms) + r"\\" for lab, x, get in spec]


if __name__ == "__main__":
    main()
