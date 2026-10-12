"""Summarize saved descriptive results; no data loading or model execution."""
import json, os
from pathlib import Path
import numpy as np
P=Path(__file__).resolve().parent
os.environ['MPLCONFIGDIR']=str(P/'.cache')
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
NAMES={'facebook_a':'Facebook Era A','reddit_comments':'Reddit comments','reddit_submissions_long':'Reddit submissions','reddit_comments_ext':'Reddit comments extension'}
def brown_mean_var(m,h):
    ds=np.arange(1,min(m-1,h-1)+1)
    return (m*h+2*np.sum((m-ds)*(h-ds)))/m**2
out={};lines=['| Panel | Horizons (weeks) | g from ladder | Variance → slope MARE | Slope → variance MARE | Spearman | LOBO slope MARE |','|---|---:|---:|---:|---:|---:|---:|']
fig,axes=plt.subplots(2,2,figsize=(11.8,8.3),layout='constrained')
for ax,(name,title) in zip(axes.flat,NAMES.items()):
    d=json.loads((P/f'measure_{name}.json').read_text());out[name]={}
    for z,(h,v) in enumerate(d['variants'].items()):
        rows=[r for r in v['bins'] if r['lo']>=d['K']/128-1e-8];s=v['seven_octaves'];g=s['g']
        r=np.array([x['mid']/d['K'] for x in rows]);slope=np.array([x['slope'] for x in rows]);a=np.array([x['var_rate'] for x in rows]);pred=a/(2*g)
        if z==0:ax.plot(r,slope,'o-',color='#163f59',label='Observed ladder slope',lw=2.4)
        ax.plot(r,pred,'s--' if z==0 else '^:',label=f'Variance-scaled slope, 13–{h} weeks',lw=1.8,ms=5)
        lines.append(f"| {title} | 13–{h} | {g:.6f} | {100*s['slope_mare']:.1f}% | {100*s['variance_mare']:.1f}% | {s['spearman']:.3f} | {100*s['lobo_slope_mare']:.1f}% |")
        widths=np.array([np.log(x['hi']/x['lo']) for x in rows])
        obs_level=np.r_[0.,-np.cumsum(slope*widths)];pred_level=np.r_[0.,-np.cumsum(pred*widths)]
        H=rows[0]['starts'];b=rows[0]['block'];fractions=[x['between_endpoint_rate']/x['var_rate'] for x in rows]
        out[name][h]={'summary':s,'all_but_top':v['all_but_top'],'mean_ladder':v['mean_ladder_seven'],
         'between_fraction_range':[min(fractions),max(fractions)],'brownian_between_fraction':float((brown_mean_var(H,int(h))-brown_mean_var(H,13))/(int(h)-13)),
         'min_pair_coverage':min(x['paired_fraction'] for x in rows),'origin_block_ratio':H/b,
         'block_relative_halfwidth_range':[min((x['block_q95']-x['block_q05'])/(2*x['var_rate']) for x in rows),max((x['block_q95']-x['block_q05'])/(2*x['var_rate']) for x in rows)],
         'integrated_ladder':{'description':'Piecewise slope projection, anchored to zero at K/128; not raw level observations',
          'relative_rank_edges':[rows[0]['lo']/d['K']]+[x['hi']/d['K'] for x in rows], 'observed_slope_projection':obs_level.tolist(),'variance_slope_projection':pred_level.tolist(),'max_abs_log_difference':float(max(abs(obs_level-pred_level))),'end_log_difference':float(pred_level[-1]-obs_level[-1])},
         'bins':[{**x,'implied_variance':float(2*g*x['slope']),'implied_slope':float(x['var_rate']/(2*g))} for x in rows]}
    ax.set_xscale('log',base=2);ax.set_xticks([1/64,1/16,1/4,1],['K/64','K/16','K/4','K'])
    ax.set_title(title,loc='left',fontweight='bold');ax.set_xlabel('Rank band midpoint');ax.set_ylabel('Local log-size / log-rank slope');ax.grid(alpha=.18);ax.legend(fontsize=8,loc='upper left')
fig.suptitle('Variance and ladder slope track each other across rank\nEach variance curve uses one scale calibrated from that panel’s ladder',fontsize=14)
fig.savefig(P/'variance_structure.png',dpi=160)
(P/'diagnostics.json').write_text(json.dumps(out,indent=2))
(P/'RESULTS_TABLE.md').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
for name,vals in out.items():
 for h,v in vals.items():
  print(name,h,{k:v[k] for k in ['between_fraction_range','brownian_between_fraction','min_pair_coverage','origin_block_ratio','block_relative_halfwidth_range']}, 'mean slope error',v['mean_ladder']['slope_mare'],'all but top',v['all_but_top'],'integrated max',v['integrated_ladder']['max_abs_log_difference'])
