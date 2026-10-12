#!/usr/bin/env python3
"""Build the V1 handoff only from all completed reference and bank analyses."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import gate_instrument as gi

NAMES={'facebook_a':'Facebook Era A','reddit_comments':'Reddit comments',
       'reddit':'Short submissions','reddit_submissions_long':'Long submissions'}

def table(head, rows):
    return '\n'.join(['| '+' | '.join(head)+' |','|'+'|'.join(['---']*len(head))+'|']+
                     ['| '+' | '.join(map(str,r))+' |' for r in rows])+'\n'

def pm(x):return f"{x['mean']:.4f} ± {x['sd']:.4f}"
def yn(x):return 'yes' if x else 'no'
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',default='llm_fitting/runs/2026-10-12_V1_gate_instrument')
    ap.add_argument('--dest',default='llm_fitting/handoff/V1_gate_instrument');a=ap.parse_args()
    root=Path(a.root);dest=Path(a.dest)
    analyses={};refs={};banks={}
    for p in gi.PANELS:
        refs[p]=json.loads((root/p/'reference/COMPLETE.json').read_text())
        assert refs[p]['reference_pass']
        banks[p]=json.loads((root/p/'bank/COMPLETE.json').read_text())
        analyses[p]=json.loads((root/'analysis'/f'{p}.json').read_text())
        assert set(analyses[p]['reps'])==set(map(str,gi.REPS_GRID))
        assert set(analyses[p]['disjoint_blocks'])=={'20','40'}
    ladder=json.loads((root/'analysis/ladder.json').read_text())
    assert all('null_noise_floor' in row for row in ladder['pairs'])
    text=['# V1 — Gate instrument study: exploratory, no change adopted\n',
          '2026-10-12 · OpenAI Codex · base ed75e9f. Predictions were committed in b787012 before the first measurement.\n',
          'All four recorded reference settings reproduced. The tables below measure Monte Carlo variability conditional on the development panels, their fitted parameters and one finite seed bank. They do not re-adjudicate a frozen specification or define a new adoption rule.\n',
          '## Reference reproduction\n',
          'Mean ± population SD across the five frozen origins; coverage is the fraction of empirical-median bands containing the simulated median, not predictive coverage.\n']
    rows=[]
    for p in gi.PANELS:
        r=refs[p]['result'];rows.append([NAMES[p],refs[p]['reps'],f"{r['model_rel']:.6f} ± {r['model_rel_sd']:.6f}",f"{r['base_rel']:.6f} ± {r['base_rel_sd']:.6f}",f"{r['coverage']:.0%}",r['scales']])
    text+=[table(['Panel','Reps','Model','Historical mobility','Coverage','Train scales'],rows),
           '## Error and repeatability against simulation count\n',
           'Each row uses 4,000 independently resampled ensembles from seeds 1000–1079. Ensemble weights are shared across all origins, calibration scales and train/test calls. Training calibration is reselected in each ensemble. SD describes bootstrap ensemble spread; the band is its 2.5–97.5 percentile interval. MCSE describes only the finite resampling calculation, not uncertainty in the 80-seed bank.\n']
    rows=[]
    for p,d in analyses.items():
        for n,r in d['reps'].items():
            s=r['score'];rows.append([NAMES[p],n,pm(s),f"[{s['q025']:.4f}, {s['q975']:.4f}]",f"{s['mcse_mean']:.5f}",f"{r['null_difference']['sd']:.4f}",f"{r['resolution95']:.4f}"])
    text+=[table(['Panel','Reps','Error mean ± MC SD','95% ensemble band','Mean MCSE','Same-stack difference SD','95% |difference|'],rows),
           '## What causes the 3-to-20 change\n',
           'The identity below is exact for the finite-bank resummaries: total change = change in calibration reselection effect + change in fixed-scale absolute-value/Jensen contribution + change in the score of the mean simulated moments. The last term includes finite-sample median bias and finite-bank effects; it is not known bias against the infinite-simulation truth. A scale is fixed separately at each origin to its full 80-seed selection.\n']
    rows=[];prediction=[]
    bounds={'facebook_a':(.01,.05),'reddit_comments':(.01,.06),'reddit':(.01,.06),'reddit_submissions_long':(0,.06)}
    for p,d in analyses.items():
        r3,r20,r40=(d['reps'][str(n)] for n in (3,20,40))
        change=r3['score']['mean']-r20['score']['mean']
        cal=r3['calibration_reselection_effect']-r20['calibration_reselection_effect']
        j=sum(r3['fixed_jensen'].values())-sum(r20['fixed_jensen'].values())
        center=r3['fixed_center_error']-r20['fixed_center_error']
        assert abs(change-cal-j-center)<1e-9
        ratio=r40['null_difference']['sd']/r20['null_difference']['sd']
        lo,hi=bounds[p]
        rows.append([NAMES[p],f'{change:.5f}',f'{cal:.5f}',f'{j:.5f}',f'{center:.5f}',f'{j/change:.1%}' if change>0 else 'no decline'])
        prediction.extend([[f'P2 {NAMES[p]}',f'positive decline in [{lo}, {hi}]',f'{change:.5f}','PASS' if change>0 and lo<=change<=hi else 'FAIL'],
            [f'P3 {NAMES[p]}','Jensen ≥50% of positive decline',f'{j/change:.1%}' if change>0 else 'no decline','PASS' if change>0 and j/change>=.5 else 'FAIL'],
            [f'P4 {NAMES[p]}','SD40 / SD20 ≤0.85',f'{ratio:.3f}','PASS' if ratio<=.85 else 'FAIL']])
        if p in ('facebook_a','reddit_submissions_long'):
            v=r20['moment_variance_share'];c=sum(v.get(k,0) for k in ('coll1','coll5','coll20'));largest=max(v,key=v.get)
            prediction.append([f'P5 {NAMES[p]}','collisions >50%; coll1 largest',f'{c:.1%}; {largest}','PASS' if c>.5 and largest=='coll1' else 'FAIL'])
    text+=[table(['Panel','Total error decline','Scale reselection','Jensen','Mean-moment score','Jensen / total'],rows),
           '## Moment contributions and exclusions\n',
           'Error shares and variance shares at 20 reps. Variance is allocated as Cov(component,total)/Var(total), including cross terms; shares may be negative. MOM_FLOOR is evaluated on fixed empirical moments, so its inclusion changes across origins, not across Monte Carlo seeds. Short submissions has no h=13 statistic because its test window is only 13 weeks; that is a structural omission, separate from the floor.\n']
    rows=[]
    for p,d in analyses.items():
        r=d['reps']['20']
        for k,e in r['moment_error_share'].items():
            floor=d['floor'][k]
            rows.append([NAMES[p],k,f'{e:.1%}',f"{r['moment_variance_share'][k]:.1%}",len(floor['included_origins']),str(floor['excluded_origins'])])
    text+=[table(['Panel','Moment','Mean-error share','MC variance share','Included /5','Excluded origins'],rows),
           'Full 40-rep decompositions, raw mean simulated moments, full-bank moments and calibration-selection frequencies are retained in analysis/*.json.\n',
           '## Conditional simulation resolution\n',
           'The following values are the 95th percentile of the absolute difference between two independent bootstrap ensembles of the SAME stack. They are a conditional simulation noise scale, not a powered minimum detectable candidate effect. Different specifications may have different variance and seed correlation. No candidate-versus-incumbent confidence interval can be inferred from this table alone.\n']
    rows=[]
    for p,d in analyses.items():
        for n in ('20','40'):
            r=d['reps'][n];rows.append([NAMES[p],n,'five-origin mean',f"{r['resolution95']:.5f}",f"{r['null_difference']['sd']:.5f}"])
            rows += [[NAMES[p],n,s['origin'],f"{s['resolution95']:.5f}",f"{s['null_difference']['sd']:.5f}"] for s in r['splits']]
    text+=[table(['Panel','Reps','Origin','95% |difference|','Difference SD'],rows),
           '### Actual disjoint seed blocks\n',
           'These use non-overlapping trajectories, unlike sampling with replacement. There are only four 20-seed ensembles and two 40-seed ensembles; they are a check, not a reliable tail estimate. Adjacent-pair differences below compare the first with second, and third with fourth when available.\n']
    rows=[]
    for p,d in analyses.items():
        for n,block in d['disjoint_blocks'].items():
            s=block['scores'];rows.append([NAMES[p],n,block['seed_ranges'],', '.join(f'{x:.5f}' for x in s),', '.join(f'{s[i]-s[i+1]:.5f}' for i in range(0,len(s),2))])
    text+=[table(['Panel','Reps','Seed ranges','Ensemble errors','Disjoint differences'],rows),
           '## Margin illustrations on every adjacent ladder pair\n',
           'Delta is newer minus preceding rung (negative favours newer). The three columns are descriptive error-only illustrations: A, delta ≤0.05; B, delta ≤ the frozen-stack 20-rep null noise scale; C, delta + that noise scale ≤0.05. B is merely absence of resolved worsening and does not establish equivalence. C is a conservative illustration, not a valid candidate-specific confidence bound. Coverage is displayed separately. No rule is selected.\n',
           'Stored ladder cells contain five pooled split results, not per-seed forecasts. Therefore only pairing across time windows is measurable. The reduction column is 1 − Var(paired differences)/(Var(old)+Var(new)) across those five windows; it is NOT evidence for variance reduction from shared simulation seeds. Overlapping origins are not independent experiments.\n']
    rows=[]
    for r in ladder['pairs']:
        rows.append([NAMES[r['panel']],f"{r['previous']}→{r['current']}",f"{r['delta_mean']:.4f}",f"{r['split_delta_sd']:.4f}",f"{r['split_pair_variance_reduction']:.1%}",yn(r['rule_fixed_005']),yn(r['rule_not_resolved_worsening']),yn(r['rule_005_plus_noise']),f"{r['coverage']:.0%}"])
    text+=[table(['Panel','Pair','Delta','Paired split SD','Split variance reduction','A','B','C','Coverage'],rows),
           '## Wrong-model controls\n']
    rows=[]
    for r in ladder['wrong_model']:
        rows.append([NAMES[r['panel']],r['rung'],f"{r['model']:.4f}",f"{r['baseline']:.4f}",f"{r['excess']:.4f}",yn(r['rejected_error_rule'])])
    text+=[table(['Panel','Rung','Model','Baseline','Excess','Reject >0.05'],rows),
           'R0/R1 are prior completed ladder cells (§2z-ap); no non-frozen stack was newly run.\n',
           '## Predictions, including failures\n']
    prediction.insert(0,['P1','all four references reproduce','4 / 4','PASS'])
    prediction.append(['P6','R0 and R1 rejected on all panels',f"{sum(r['rejected_error_rule'] for r in ladder['wrong_model'])} / 8",'PASS' if all(r['rejected_error_rule'] for r in ladder['wrong_model']) else 'FAIL'])
    text+=[table(['Prediction','Declared expectation','Measured','Verdict'],prediction),
           '## Execution, provenance and limits\n',
           'Commands: `zsh llm_fitting/handoff/V1_gate_instrument/launch.zsh`; then, for each panel, the registered Python interpreter runs `scripts/heavy_lock.py -- caffeinate -dims python -u llm_fitting/validation/gate_instrument_analysis.py --panel PANEL`; finally `gate_instrument_report.py`. Substitute the registered interpreter for both occurrences of python. The exact executable is `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`. All expensive jobs use the shared lock.\n',
           'The wrapper imports the frozen gate, caches primitive seed histograms atomically, preserves pooled medians and scale tie-breaking, and restores all wrapped functions. Both protected model files remain unchanged. Source/input SHA-256, runtime, commit, seeds, attempts and completion markers are beside each run. Nine possible test scales are collected for each bank so bootstrap calibration can be replayed without refitting. Descriptive distribution-score printing is off; it does not enter Gate v1.\n',
           'No Instagram optional run, sealed data, WD archive, parameter/default change, candidate gate, adoption decision or paper-claim change was made. Finite-bank uncertainty, temporal dependence, estimation uncertainty and new-specification detection power remain outside this measurement. QUESTIONS.md records the missing ladder seed output.\n',
           'Health-check logs in this handoff include one failed zsh invocation of a Bash script; this failed after the research suite and before package/guard checks. Its successful Bash rerun is separate. This is a harness error, not a failed or killed scientific run. All scientific attempts are in ATTEMPTS.jsonl; incomplete attempts must be disclosed if present.\n']
    (dest/'REPORT.md').write_text('\n'.join(text))
    rows=[]
    for p in gi.PANELS:
        for phase,run in [('reference',refs[p]),('bank',banks[p])]:
            r=run['result'];flags='temper, min-knot-n 8, md-lags 6, t-tails, conditional state'
            if p=='facebook_a':flags+=', spec-b'
            if p=='reddit_comments':flags+=', mix-hetero'
            rows.append(['2026-10-12','Codex V1',run['commit'][:10],p,'reproduction',flags,f"5 × {run['reps']}",f"{r['model_rel']:.6f} ± {r['model_rel_sd']:.6f}",f"{r['base_rel']:.6f} ± {r['base_rel_sd']:.6f}",f"{r['coverage']:.0%}",str(root/p/phase/'gate.log'),f"{phase}; seeds {run['seed_start']}–{run['seed_start']+run['reps']-1}; boot {run['boot']}; same frozen specification"])
    (dest/'GATE_ROWS.md').write_text('# V1 gate-ledger handoff\n\n'+table(['date','session','commit','panel','kind','spec (flags beyond defaults)','splits × reps','model relErr ± SD','baseline ± SD','coverage','log / MODEL_STATUS §','note'],rows)+'\nBootstrap ensembles are derived resummaries of these banks, not additional fitted gate calls. No non-frozen candidate was evaluated. See per-phase ATTEMPTS.jsonl for execution attempts.\n')

if __name__=='__main__':main()
