"""Regenerate V2 scientific figures from the complete compact tables."""
import argparse,os,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
os.environ.setdefault('MPLCONFIGDIR',str(ROOT/'output/v2_plots/mplconfig'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
sys.path.insert(0,str(ROOT/'llm_fitting'))
from validation.planted_report import DISPLAY,LABELS
from validation import planted_crosscheck as pc

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--out',type=Path,required=True)
    ap.add_argument('--figures',type=Path,default=ROOT/'output/v2_plots')
    args=ap.parse_args();args.figures.mkdir(parents=True,exist_ok=True)
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False})
    rec=pd.read_csv(args.out/'recovery_knots.csv')
    fig,axes=plt.subplots(2,3,figsize=(13,7),layout='constrained')
    for j,p in enumerate(pc.PANELS):
        for i,(par,label) in enumerate((('kappa_z','Home reversion κ'),('V','Local V = σ²/(2κ)'))):
            q=rec[(rec.panel==p)&(rec.condition=='baseline')&(rec.parameter==par)&rec.knot.notna()].sort_values('z')
            ax=axes[i,j];x=np.exp(q.z)
            ax.fill_between(x,q.q05,q.q95,color='#2b6cb0',alpha=.18,label='5–95% seed band')
            ax.plot(x,q['mean'],color='#2b6cb0',label='Estimate mean')
            ax.plot(x,q.truth,color='#b45309',ls='--',label='Generating value')
            ax.set_xscale('log');ax.set_ylabel(label);ax.set_xlabel('Generating rank fraction')
            if i==0:ax.set_title(LABELS[p])
            ax.grid(alpha=.2)
    axes[0,0].legend(frameon=False,fontsize=8)
    fig.suptitle('V2 operational recovery • 20 seeds per native profile\nExploratory, conditional on frozen parameters; bands describe seed spread',fontsize=13)
    fig.savefig(args.figures/'recovery.png',dpi=180);fig.savefig(args.figures/'recovery.pdf');plt.close(fig)
    rates=pd.read_csv(args.out/'diagnostic_rates.csv')
    conditions=list(dict.fromkeys(rates.condition))
    fig,axes=plt.subplots(1,3,figsize=(17,8),layout='constrained',sharey=True)
    names=[x[0] for x in DISPLAY];keys=[x[1] for x in DISPLAY]
    for ax,p in zip(axes,pc.PANELS):
        q=rates[rates.panel==p].pivot(index='condition',columns='statistic',values='detection_rate').reindex(index=conditions,columns=keys)
        im=ax.imshow(q.to_numpy(float),vmin=0,vmax=1,cmap='viridis',aspect='auto')
        ax.set_title(LABELS[p]);ax.set_xticks(range(len(names)),names,rotation=65,ha='right',fontsize=8)
        ax.set_yticks(range(len(conditions)),conditions,fontsize=9)
        for i in range(len(conditions)):
            for j in range(len(names)):
                v=q.iloc[i,j]
                ax.text(j,i,'—' if not np.isfinite(v) else f'{v:.2f}',ha='center',va='center',fontsize=7,
                        color='white' if not np.isfinite(v) or v<.5 else '#18212b')
    fig.colorbar(im,ax=axes,label='Fraction outside the 39-seed baseline envelope',shrink=.7)
    fig.suptitle('V2 fixed diagnostic display • all mechanisms and strengths\n20 evaluation seeds; per-statistic rates, not a familywise test. Wilson intervals are in REPORT.md / diagnostic_rates.csv.',fontsize=12)
    fig.savefig(args.figures/'diagnostic_rates.png',dpi=180);fig.savefig(args.figures/'diagnostic_rates.pdf');plt.close(fig)
if __name__=='__main__':main()
