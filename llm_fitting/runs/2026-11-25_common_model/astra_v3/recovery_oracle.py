"""Run post hoc known-budget and stronger falling-gradient synthetic controls."""
from pathlib import Path
import numpy as np
import common_rankdiff_astra_r7 as cr
from test_common_rankdiff_astra_r7 import _independent_jump_panel

for mode, base in [('rising',0), ('falling',0), ('both',0), ('falling',.007)]:
    for seed in (431,853,1289):
        x,r,k = _independent_jump_panel(seed,mode,common_jump_variance=base)
        n = x.shape[1]
        order = np.argsort(np.argsort(r.mean(0),kind='stable'),kind='stable') + 1
        edges = cr._band_edges(k,n)
        bands = np.searchsorted(edges,order,side='right')-1
        z = np.log((np.sqrt(np.maximum(edges[:-1],1)*edges[1:])-.5)/n)[:-1]
        ends = [.014,.0001] if mode=='falling' else [.0001,.014]
        truth = base + np.interp(np.log(np.arange(1,n+1)/k),[-5.5,-.35],ends)
        target = np.array([truth[bands==b].mean() for b in range(len(z))])
        w = np.minimum(np.bincount(bands)[:len(z)],100)
        proposed = cr._line_variance(cr._fit_jump_line(z,target,w),z)
        oracle_share = np.average(proposed,weights=w)/(.018+base)
        print(f'{mode} base={base:g} seed={seed} truth={target.round(5)} oracle_share={oracle_share:.4f}')
        if base:
            p = cr.estimate_common(x,r,k,refine_drift=False,innov_mode='gaussian')
            d = p.diag['jumps']
            print(f"  estimated active={d['active']} share={d['gap_share']:.4f} "
                  f"proposed head/body={d['proposed_weekly_variance'][0]:.5f}/{d['proposed_weekly_variance'][-1]:.5f}")
