#!/usr/bin/env python3
"""Bit-identity of scale_moment='variance' vs the Round 4 module (llm_fitting/common_rankdiff.py), and
quick robust-vs-variance parameter comparison on real panels (refine_drift=False for speed)."""
import sys, json, time, importlib.util, numpy as np
from pathlib import Path
HERE = Path(__file__).resolve().parent
def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path); m = importlib.util.module_from_spec(spec); sys.modules[name] = m; spec.loader.exec_module(m); return m
new = load(str(HERE/'common_rankdiff_fable.py'), 'cr_new')
old = load(str(HERE.parents[2]/'common_rankdiff.py'), 'cr_old')
CACHE = Path('/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache')
panels = sys.argv[1:] or ['facebook_a', 'instagram_hm', 'reddit_comments_ext', 'reddit_submissions_long']
KEYS = ['sigma_L0','beta','phi','a_xi','b_xi','a_obs','b_obs','s_vol','a_abs','b_abs','lam_exit','surge_lam','surge_mu','surge_rho','decline_var','decline_rho']
out = {}
for pn in panels:
    d = np.load(CACHE/f'{pn}.npz', allow_pickle=True)
    X = d['X'].astype(np.float64); R = d['R'].astype(np.float64); fl = d['floors'].astype(np.float64); K = int(d['K'])
    sb = None
    if (CACHE/f'{pn}_spec_b.json').exists():
        js = json.loads((CACHE/f'{pn}_spec_b.json').read_text()); sb = (np.array(js['z'], float), np.array(js['sigma_obs'], float))
    out[pn] = {}
    for asym in (False, True):
        t = time.time(); po = old.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=asym); t_old = time.time()-t
        t = time.time(); pv = new.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=asym, scale_moment='variance'); t_var = time.time()-t
        t = time.time(); pr = new.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=asym, scale_moment='robust'); t_rob = time.time()-t
        same = all(getattr(po,k)==getattr(pv,k) for k in KEYS) and np.array_equal(po.innov_pool, pv.innov_pool) and np.array_equal(po.drift_g, pv.drift_g) and np.array_equal(po.bottom_pool, pv.bottom_pool)
        print(f"\n{pn} asym={asym}: variance-path bit-identical to Round4 module: {same}  (t old {t_old:.1f}s, var {t_var:.1f}s, rob {t_rob:.1f}s)")
        for lbl, p in (("VAR", pv), ("ROB", pr)):
            print(f"   {lbl}: sL0={p.sigma_L0:.4f} beta={p.beta:+.3f} phi={p.phi:.2f} a_xi={p.a_xi:+.3f} b_xi={p.b_xi:+.3f} a_obs={p.a_obs:+.3f} b_obs={p.b_obs:+.3f} s={p.s_vol:.3f} "
                  f"surge(hl={(-np.log(2)/np.log(p.surge_rho)) if p.surge_rho>0 else 0:.0f},lam={p.surge_lam:.5f},mu={p.surge_mu:.2f},Vs={new._surge_variance(p.surge_lam,p.surge_mu,p.surge_rho):.3f}) sd={np.sqrt(p.decline_var):.5f} n={p.n_free_scalars()}")
        sv = np.array(pr.diag['sL2_band_var']); sr = np.array(pr.diag['sL2_band'])
        xv = np.array(pr.diag['vxi_band_var']); xr = np.array(pr.diag['vxi_band'])
        ov = np.array(pr.diag['so2_band_var']); orr = np.array(pr.diag['so2_band'])
        print("   band sL2  var/rob:", " ".join(f"{a:.4f}/{b:.4f}" for a,b in zip(sv[:7], sr[:7])))
        print("   band Vxi  var/rob:", " ".join(f"{a:.4f}/{b:.4f}" for a,b in zip(xv[:7], xr[:7])))
        print("   band so2  var/rob:", " ".join(f"{a:.4f}/{b:.4f}" for a,b in zip(ov[:7], orr[:7])))
        print("   jump share of lasting var (1-rob/var):", " ".join(f"{1-b/a:.2f}" if a>1e-9 else " nan" for a,b in zip(sv[:7], sr[:7])))
        rl = np.array(pr.diag['iqr_var_ratio_model']); print("   final model IQRv/var ratio band0 h=1,4,13,26,52:", " ".join(f"{rl[0,h-1]:.3f}" for h in (1,4,13,26,52) if h-1 < rl.shape[1]))
        out[pn][f"asym{asym}"] = dict(bit_identical=bool(same), var={k: float(getattr(pv,k)) for k in KEYS}, rob={k: float(getattr(pr,k)) for k in KEYS},
                                       sL2_var=sv.tolist(), sL2_rob=sr.tolist(), vxi_var=xv.tolist(), vxi_rob=xr.tolist(), so2_var=ov.tolist(), so2_rob=orr.tolist())
(HERE/'diag_bitident_quickfit.json').write_text(json.dumps(out, indent=1))
