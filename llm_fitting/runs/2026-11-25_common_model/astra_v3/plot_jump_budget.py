"""Plot the measured and realized full-panel weekly variance allocations."""
import json
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import common_rankdiff_astra_r7 as cr
from diagnose_split import CACHE, WORK
from compare_rounds import NAMES

data = json.loads((WORK / 'split_diagnostics.json').read_text())
fig, axes = plt.subplots(2, 2, figsize=(8.6, 6.0), sharex=True, sharey=True)
metrics = [
    ('Empirical gap', '#313a45', 'o', '-'),
    ('R6 assigned', '#af592b', 's', '--'),
    ('R7 assigned', '#007b72', 'D', '-'),
]
for ax, (panel, name) in zip(axes.flat, NAMES.items()):
    with np.load(CACHE/f'{panel}.npz') as f:
        x,r,floors,k = f['X'],f['R'],f['floors'],int(f['K'])
    sb = None
    if panel == 'facebook_a':
        spec = json.loads((CACHE/'facebook_a_spec_b.json').read_text())
        sb = np.array(spec['z']),np.array(spec['sigma_obs'])
    p = cr.estimate_common(x,r,k,floors,spec_b=sb,refine_drift=False,scale_moment='meta')
    z = np.array(p.diag['band_z'][:-1])
    r7 = data[panel][str(len(x))]['S2']
    curves = (p.diag['jumps']['gap_band'][:-1], p.jump_lam*cr._jump_sd_of(p,z)**2, r7['realized'])
    for (label,color,marker,line),values in zip(metrics,curves):
        ax.plot(np.arange(7),np.asarray(values)*1000,ls=line,lw=1.7,color=color,
                marker=marker,ms=4,label=label)
    ax.set_title(name,loc='left',fontsize=11,fontweight='bold')
    ax.grid(axis='y',color='#e0e3e5',lw=.6)
    ax.set_xticks(range(7))
    ax.set_ylim(-0.55,12)
    ax.spines[['top','right']].set_visible(False)
    ax.tick_params(length=0,pad=5)
fig.legend(*axes.flat[0].get_legend_handles_labels(),loc='upper center',ncol=3,
           bbox_to_anchor=(.54,1.00),frameon=False)
fig.supylabel('Weekly variance in log activity (× 10⁻³)',x=.03,fontsize=10)
fig.supxlabel('Permanent-rank band, from head (0) to K/2…K (6)',y=.045,fontsize=10)
fig.subplots_adjust(left=.12,right=.98,bottom=.15,top=.89,hspace=.31,wspace=.15)
fig.savefig(WORK/'jump_budget.png',dpi=165,facecolor='white')
