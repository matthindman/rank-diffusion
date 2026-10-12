#!/usr/bin/env python3
"""Figures from completed V1 analysis summaries (no fitting or simulation)."""
import argparse
import json
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

PANELS=(('facebook_a','Facebook Era A'),('reddit_comments','Reddit comments'),
        ('reddit','Short submissions'),('reddit_submissions_long','Long submissions'))

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',default='llm_fitting/runs/2026-10-12_V1_gate_instrument')
    p.add_argument('--dest',default='llm_fitting/handoff/V1_gate_instrument/figures')
    p.add_argument('--layout-test',action='store_true')
    a=p.parse_args();root=Path(a.root);dest=Path(a.dest);dest.mkdir(parents=True,exist_ok=True)
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,
                         'pdf.fonttype':42,'savefig.facecolor':'white'})
    fig,axs=plt.subplots(2,2,figsize=(9.4,7.0),sharex=True)
    noise=[]
    for ax,(panel,title) in zip(axs.flat,PANELS):
        assert (root/panel/'bank/COMPLETE.json').exists()
        d=json.loads((root/'analysis'/f'{panel}.json').read_text())
        assert 'disjoint_blocks' in d
        n=np.array(sorted(map(int,d['reps'])))
        r=[d['reps'][str(k)] for k in n]
        ax.fill_between(n,[x['score']['q025'] for x in r],[x['score']['q975'] for x in r],color='#0072B2',alpha=.16,label='95% ensemble band')
        ax.plot(n,[x['score']['mean'] for x in r],'o-',color='#0072B2',label='Calibration reselected')
        ax.plot(n,[x['fixed_score']['mean'] for x in r],'s--',color='#D55E00',markersize=4,label='Calibration fixed at bank choice')
        ref=json.loads((root/panel/'reference/RESULT.json').read_text())
        ax.axhline(ref['result']['base_rel'],color='#555555',linestyle=':',linewidth=1.3,label='Train historical-mobility baseline')
        ax.set(title=title,xscale='log',xticks=n,xticklabels=list(map(str,n)),ylabel='Mean relative error')
        ax.grid(axis='y',alpha=.2)
        noise.append([d['reps'][str(k)]['resolution95'] for k in (20,40)])
    for ax in axs[-1]:ax.set_xlabel('Simulations per origin')
    handles,labels=axs[0,0].get_legend_handles_labels()
    fig.legend(handles,labels,loc='lower center',bbox_to_anchor=(.5,.055),ncol=2,frameon=False)
    fig.suptitle('Frozen gate: error and Monte Carlo variability',fontsize=15,y=.98)
    fig.text(.5,.015,'Five origins per panel; 80-seed banks; 4,000 resampled ensembles per simulation count.\nBands are conditional simulation variability, not uncertainty over data, fitting or future time.',ha='center',fontsize=9,color='#444444')
    fig.tight_layout(rect=(0,.15,1,.94))
    if a.layout_test:fig.text(.5,.5,'LAYOUT CHECK — SYNTHETIC VALUES',ha='center',va='center',rotation=25,fontsize=24,color='crimson',alpha=.75)
    fig.savefig(dest/'error_vs_reps.png',dpi=180)
    fig.savefig(dest/'error_vs_reps.pdf')
    plt.close(fig)
    fig,ax=plt.subplots(figsize=(9,4.6));x=np.arange(4);noise=np.array(noise)
    ax.bar(x-.17,noise[:,0],width=.32,color='#0072B2',label='20 simulations')
    ax.bar(x+.17,noise[:,1],width=.32,color='#E69F00',label='40 simulations')
    ax.axhline(.05,color='#555555',linestyle=':',label='Existing +0.05 margin (context only)')
    for i in range(4):
        for j,offset in enumerate((-.17,.17)):
            ax.text(i+offset,noise[i,j],f'{noise[i,j]:.4f}',ha='center',va='bottom',fontsize=9)
    ax.set(xticks=x,xticklabels=[t for p,t in PANELS],ylabel='95th percentile of |same-stack difference|',
           title='Simulation noise scale for the five-origin mean')
    ax.set_ylim(0,max(.05,noise.max())*1.25);ax.legend(frameon=False,fontsize=9)
    ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
    fig.text(.5,.018,'Two independent resampled ensembles of the same frozen stack. This does not establish\na candidate-specific confidence bound, detection power, equivalence or an adoption rule.',ha='center',fontsize=9,color='#444444')
    fig.tight_layout(rect=(0,.13,1,1))
    if a.layout_test:fig.text(.5,.5,'LAYOUT CHECK — SYNTHETIC VALUES',ha='center',va='center',rotation=15,fontsize=24,color='crimson',alpha=.75)
    fig.savefig(dest/'simulation_noise.png',dpi=180)
    fig.savefig(dest/'simulation_noise.pdf')
    plt.close(fig)

if __name__=='__main__':main()
