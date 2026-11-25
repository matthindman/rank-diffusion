#!/usr/bin/env python3
"""Quick robust+jumps fits on the four real panels (refine_drift off): parameters, jump diagnostics, timing."""
import sys, json, time, importlib.util, numpy as np
from pathlib import Path
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('cr_new', str(HERE/'common_rankdiff_fable.py')); cr = importlib.util.module_from_spec(spec); sys.modules['cr_new'] = cr; spec.loader.exec_module(cr)
CACHE = Path('/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache')
panels = sys.argv[1:] or ['facebook_a', 'instagram_hm', 'reddit_comments_ext', 'reddit_submissions_long']
out = {}
for pn in panels:
    d = np.load(CACHE/f'{pn}.npz', allow_pickle=True)
    X = d['X'].astype(np.float64); R = d['R'].astype(np.float64); fl = d['floors'].astype(np.float64); K = int(d['K'])
    sb = None
    if (CACHE/f'{pn}_spec_b.json').exists():
        js = json.loads((CACHE/f'{pn}_spec_b.json').read_text()); sb = (np.array(js['z'], float), np.array(js['sigma_obs'], float))
    out[pn] = {}
    for asym in (False, True):
        t = time.time(); p = cr.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=asym); dt = time.time()-t
        j = p.diag.get('jumps', {})
        print(f"\n{pn} asym={asym} ({dt:.1f}s): n_free={p.n_free_scalars()} sL0={p.sigma_L0:.4f} beta={p.beta:+.3f} phi={p.phi:.2f} a_xi={p.a_xi:+.3f} b_xi={p.b_xi:+.3f} a_obs={p.a_obs:+.3f} b_obs={p.b_obs:+.3f} s={p.s_vol:.3f}")
        print(f"   surge(lam={p.surge_lam:.5f},mu={p.surge_mu:.2f}) sd={np.sqrt(p.decline_var):.5f} | JUMPS lam={p.jump_lam:.4f} c={p.jump_c:.4f} (sd/jump at K={np.sqrt(p.jump_c):.3f}, var/wk at K={p.jump_lam*p.jump_c:.5f}) gap_share={j.get('gap_share', float('nan')):.3f}")
        print("   sse profile:", {k: round(v, 4) for k, v in j.get('sse_profile', {}).items()})
        sv = np.array(p.diag['sL2_band_var']); sr = np.array(p.diag['sL2_band'])
        print("   band sL2 var :", " ".join(f"{x:.4f}" for x in sv[:7]), "| rob core:", " ".join(f"{x:.4f}" for x in sr[:7]))
        if 'gap_band' in j:
            print("   band gap     :", " ".join(f"{x:.4f}" for x in np.array(j['gap_band'])[:7]), "| model jump var/wk:", " ".join(f"{p.jump_lam*p.jump_c*cr._surge_scale_of(p.b_xi, z, p.z_K)**2:.4f}" for z in np.array(p.diag['band_z'])[:7]))
        rl = np.array(p.diag['iqr_var_ratio_model']); print("   model IQRv/var band0 h=1,4,13,26,52:", " ".join(f"{rl[0,h-1]:.3f}" for h in (1,4,13,26,52) if h-1 < rl.shape[1]))
        out[pn][f'asym{asym}'] = dict(n_free=p.n_free_scalars(), sigma_L0=p.sigma_L0, beta=p.beta, phi=p.phi, a_xi=p.a_xi, b_xi=p.b_xi, a_obs=p.a_obs, b_obs=p.b_obs, jump_lam=p.jump_lam, jump_c=p.jump_c, jumps=j, surge_lam=p.surge_lam, surge_mu=p.surge_mu, sigma_d=float(np.sqrt(p.decline_var)))
(HERE/'diag_quickfit_jumps.json').write_text(json.dumps(out, indent=1, default=float))
