"""Inspect train-window variance budgets without calling the gate or scoring forecasts."""
import json
from pathlib import Path

import numpy as np
import common_rankdiff_astra_r7 as cr

CACHE = Path("/Users/hindman/.gemini/antigravity/brain/ce72fa6c-1e11-45f7-b5a4-535d19417227/scratch/panel_cache")
WORK = Path(__file__).resolve().parent
GATE = json.loads((WORK / 'baseline_r6/candidate_frozen_gate_results.json').read_text())


def inspect(x, r, k, floors, sb, asymmetry):
    p = cr.estimate_common(x, r, k, floors, spec_b=sb, refine_drift=False, asymmetry=asymmetry)
    j = p.diag['jumps']
    z = np.array(p.diag['band_z'][:-1])
    levels = np.array(p.diag['band_level'][:-1])
    variance = cr.sigma_L_of(p, levels)**2
    allocation = (p.jump_lam * cr._bounded_scales(p, z, np.sqrt(variance))[1]**2
                  if p.jump_line is not None else np.zeros_like(z))
    return dict(T=len(x), n_scalars=p.n_free_scalars(), active=j['active'],
                allocated_fraction=j['gap_share'], band_budget=j['budget'],
                proposed=j['proposed_weekly_variance'], realized=allocation.tolist(),
                total=variance.tolist(), surge_rate=p.surge_lam, drift_sd=np.sqrt(p.decline_var),
                symmetric_total=j['symmetric_total'][:-1], symmetric_core=j['symmetric_core'][:-1])


def main():
    result = {}
    for name in ('facebook_a', 'instagram_hm', 'reddit_comments_ext', 'reddit_submissions_long'):
        with np.load(CACHE / f'{name}.npz') as f:
            x, r, floors, k = f['X'], f['R'], f['floors'], int(f['K'])
        sb = None
        if name == 'facebook_a':
            j = json.loads((CACHE / 'facebook_a_spec_b.json').read_text())
            sb = np.array(j['z']), np.array(j['sigma_obs'])
        row = {}
        for end in [*GATE[name]['origins'], len(x)]:
            row[str(end)] = {arm: inspect(x[:end], r[:end], k, floors[:end], sb, arm == 'S3')
                             for arm in ['S2', 'S3']}
        result[name] = row
        (WORK / 'split_diagnostics.json').write_text(json.dumps(result, indent=2))
        print(name, 'complete', flush=True)


if __name__ == '__main__':
    main()
