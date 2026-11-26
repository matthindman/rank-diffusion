"""D11 (Round 8, POST HOC after the v1 gate): stability of the S3 smooth lasting law.
For every gate training window, the bandwise residual lasting variances of R6 S3 and of R8 S3 (rank-law drift),
the log-scale WLS law used by R4-R6 (zero bands dropped) and a variance-scale profile fit (zero bands kept).
Parameters only; no evaluation score is read."""
import importlib.util, sys, math, json
import numpy as np
sys.path.insert(0, '.')
import common_rankdiff_opus_r8 as cand
spec = importlib.util.spec_from_file_location('r6src', 'baseline_r6/common_rankdiff_r6_source_copy.py')
r6 = importlib.util.module_from_spec(spec); sys.modules['r6src'] = r6; spec.loader.exec_module(r6)
C = '/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache/'
WIN = {'facebook_a': [21, 32, 43, 54, 65, 86], 'instagram_hm': [13, 20, 26, 32, 39, 52],
       'reddit_comments_ext': [53, 80, 106, 133, 160, 213], 'reddit_submissions_long': [53, 80, 106, 132, 159, 212]}

def var_scale(u, y, w):
    best = None
    for b in np.linspace(-0.25, 0.50, 151):
        f = np.exp(-2 * b * u)
        s2 = max(float(np.sum(w * y * f) / np.sum(w * f * f)), 1e-12)
        sse = float(np.sum(w * (y - s2 * f) ** 2))
        if best is None or sse < best[0]:
            best = (sse, b, s2)
    return math.sqrt(best[2]), best[1]

out = {}
for pn, wins in WIN.items():
    d = np.load(C + pn + '.npz', allow_pickle=True)
    sb = None
    if pn == 'facebook_a':
        js = json.load(open(C + 'facebook_a_spec_b.json')); sb = (np.array(js['z']), np.array(js['sigma_obs']))
    for T0 in wins:
        X, R, fl, K = d['X'][:T0].astype(float), d['R'][:T0].astype(float), d['floors'][:T0].astype(float), int(d['K'])
        row = []
        for lab, mod in (('R6', r6), ('R8', cand)):
            p = mod.estimate_common(X, R, K=K, floors=fl, spec_b=sb, refine_drift=False, asymmetry=True)
            y = np.array(p.diag['sL2_band'][:7]); lev = np.array(p.diag['band_level'][:7]) - p.L_K
            bn = np.array([min(K / 64, 100)] + [100.0] * 6)
            ok = np.isfinite(y) & np.isfinite(lev)
            s0, b = var_scale(lev[ok], y[ok], bn[ok])
            row.append((lab, p.beta, p.sigma_L0, b, s0, int((y[ok] <= 1e-7).sum())))
        out[f'{pn}@{T0}'] = row
        print(f'{pn[:12]:12s} T0={T0:3d} ' + ' | '.join(f'{l}: log beta={bl:+.3f} sL0={sl:.4f} ; var beta={bv:+.3f} sL0={sv:.4f} (zero bands {nz})' for l, bl, sl, bv, sv, nz in row), flush=True)
json.dump(out, open('diag_corelaw.json', 'w'), indent=1)
