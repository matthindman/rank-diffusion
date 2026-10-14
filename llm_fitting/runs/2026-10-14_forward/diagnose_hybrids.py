"""Post hoc diagnosis of the forward simulation: which part of a transition fails to carry over,
the movement of present endpoints or the entrants?  Hybrids: movement from one year, entry from another."""
import sys, warnings, numpy as np
warnings.filterwarnings("ignore")
sys.path.insert(0, "llm_fitting")
import mts_forward as fw, mts_step1 as m1, minimal_rankdiff as mrd
out = []
for panel in ("reddit_comments_ext", "reddit_submissions_long"):
    cfg = m1.PANELS[panel]; K = cfg["K"]
    df = mrd.load_panel(mrd.PLATFORMS[cfg["platform"]])
    M, nc = fw.block_ladders(df, K, cfg["block"])
    ks = fw.cut_positions(K); W = 2 * K
    trs = {a: fw.transition(M, a, K, W) for a in range(3)}
    obs = {b: fw.observed(M, b, K, ks, W) for b in range(4)}
    def hybrid(move, entry):
        h = dict(trs[move]); h["n_in"] = trs[entry]["n_in"]; h["offsets"] = trs[entry]["offsets"]; return h
    def none_entry(move):
        h = dict(trs[move]); h["n_in"] = 0; h["offsets"] = np.empty(0); return h
    print(f"\n{panel}: rms error of the change of shape at {ks} (log units over the year)")
    for b in (1, 2):
        o = {k: obs[b + 1]["shape"][k] - obs[b]["shape"][k] for k in ks}
        res = {}
        for label, tr in (("movement and entry from the year before (as run)", trs[b - 1]),
                          ("movement from the year before, entry from the target year", hybrid(b - 1, b)),
                          ("movement from the target year, entry from the year before", hybrid(b, b - 1)),
                          ("both from the target year (in sample)", trs[b])):
            f = fw.forecast(M, b, [tr], K, ks, n_sim=30, mode="empirical")
            p = {k: f["shape"][k] - obs[b]["shape"][k] for k in ks}
            res[label] = fw.rms(o, p, ks)
        print(f"  {b}->{b+1}: no change {fw.rms(o, None, ks):.4f}; " + "; ".join(f"{k}: {v:.4f}" for k, v in res.items()))
        print(f"        entrants into the top 2K: year before {trs[b-1]['n_in']} at {np.mean(trs[b-1]['offsets']):+.2f}; target year {trs[b]['n_in']} at {np.mean(trs[b]['offsets']):+.2f}")
