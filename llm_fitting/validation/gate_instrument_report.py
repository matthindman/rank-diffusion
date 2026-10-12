#!/usr/bin/env python3
"""Build the V1 handoff only from all completed reference and bank analyses."""
import argparse
import hashlib
from importlib.metadata import version
import subprocess
import json
from pathlib import Path
import sys
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parent))
import gate_instrument as gi
import gate_instrument_analysis as ga

NAMES={'facebook_a':'Facebook Era A','reddit_comments':'Reddit comments',
       'reddit':'Short submissions','reddit_submissions_long':'Long submissions'}

def table(head, rows):
    def cell(value):
        return str(value).replace('|', r'\|').replace('\n', '<br>')
    return '\n'.join(['| '+' | '.join(map(cell,head))+' |','|'+'|'.join(['---']*len(head))+'|']+
                     ['| '+' | '.join(map(cell,r))+' |' for r in rows])+'\n'


def pm(x):return f"{x['mean']:.4f} ± {x['sd']:.4f}"
def yn(x):return 'yes' if x else 'no'
def audit_completed_banks(root, panels=gi.PANELS):
    """Check provenance and expected seed coverage before reporting results."""
    audit={'complete_for_all_panels':set(panels)==set(gi.PANELS),'panels':{},'analysis_sources':{},'runtime':{p:version(p) for p in ('numpy','pandas','scipy','pyarrow','matplotlib')},'commit_at_audit':subprocess.check_output(
        ['git','rev-parse','HEAD'],text=True).strip()}
    for path in (Path(__file__),Path(__file__).with_name('gate_instrument_analysis.py'),Path(__file__).with_name('gate_instrument_plot.py')):
        audit['analysis_sources'][path.name]=gi.sha_file(path)
    for panel in panels:
        ref=json.loads((root/panel/'reference/RESULT.json').read_text())
        bank=json.loads((root/panel/'bank/RESULT.json').read_text())
        assert ref['inputs']==bank['inputs'], (panel,'input changed')
        assert ref['sources']==bank['sources'], (panel,'simulation source changed')
        assert len(ref['splits'])==len(bank['splits'])==5
        digest=hashlib.sha256();files=0;reconstructed=[]
        for before,after in zip(ref['splits'],bank['splits']):
            origin=str(before['origin'])
            assert before['origin']==after['origin']
            for key in ('emp','base','train_calibration','horizons'):
                assert json.dumps(before[key],sort_keys=True)==json.dumps(after[key],sort_keys=True), (panel,origin,key)
            assert (root/panel/'reference'/origin/'parameters.json').read_text()==(root/panel/'bank'/origin/'parameters.json').read_text(), (panel,origin,'fit changed')
            full_draw=np.arange(80)[None,:]
            calibration=[]
            for scale in gi.SCALES:
                moments=ga.moments_from_bank(root/panel/'bank'/origin/'train'/f'scale_{scale:g}',after['horizons'],full_draw)
                calibration.append(gi.rk._cal_error(after['train_calibration'],{k:float(v[0]) for k,v in moments.items()}))
            assert gi.SCALES[int(np.argmin(calibration))]==after['scale'], (panel,origin,'scale reconstruction')
            moments=ga.moments_from_bank(root/panel/'bank'/origin/'test'/f"scale_{after['scale']:g}",after['horizons'],full_draw)
            scalar={k:float(v[0]) for k,v in moments.items()}
            for key,value in scalar.items():
                assert np.isclose(value,after['sim'][key],rtol=5e-14,atol=1e-12,equal_nan=True), (panel,origin,key,'pooled reconstruction')
            components,_=gi.gate_components(after['emp'],scalar,after['horizons'])
            reconstructed.append(sum(components.values()))
            for mode in ('train','test'):
                for scale in gi.SCALES:
                    folder=root/panel/'bank'/origin/mode/f'scale_{scale:g}'
                    seeds=sorted(folder.glob('seed_*.npz'))
                    assert [f.name for f in seeds]==[f'seed_{i:04d}.npz' for i in range(1000,1080)]
                    for f in seeds:
                        digest.update(str(f.relative_to(root)).encode()+b'\0')
                        digest.update(bytes.fromhex(gi.sha_file(f)))
                        files+=1
        assert files==7200
        assert np.isclose(np.mean(reconstructed),bank['result']['model_rel'],rtol=5e-14,atol=1e-12)
        audit['panels'][panel]={'seed_files':files,'ordered_path_and_sha256_digest':digest.hexdigest(),
                                 'unchanged_inputs_fits_empirical_moments_and_model_sources':True,
                                 'full_bank_calibration_and_pooled_gate_score_reconstructed':True}
    name='AUDIT.json' if audit['complete_for_all_panels'] else 'AUDIT_PARTIAL.json'
    gi.write_json(root/name,audit)
    return audit


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
    audit_completed_banks(root)
    text=['# V1 — Gate instrument study: exploratory, no change adopted\n',
          '2026-10-12 · OpenAI Codex · base ed75e9f. Predictions were committed in b787012 before the first measurement.\n',
          'All four recorded reference settings reproduced. The tables below measure Monte Carlo variability conditional on the development panels, their fitted parameters and one finite seed bank. They do not re-adjudicate a frozen specification or define a new adoption rule.\n',
          '## Reference reproduction\n',
          'Recorded targets: MODEL_STATUS §2z-g for Facebook, comments and short submissions; §2z-ae P6 for long submissions; §2z-ap for the ladder rechecks. Mean ± population SD across the five frozen origins; coverage is the fraction of empirical-median bands containing the simulated median, not predictive coverage.\n']
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
           'Error shares and variance shares at 20 reps. Variance is allocated as Cov(component,total)/Var(total), including cross terms; shares may be negative. MOM_FLOOR is evaluated on fixed empirical test moments, so its inclusion changes across origins, not across Monte Carlo seeds (missing simulated moments are handled separately). The training calibration does not apply this floor: its relative errors use a denominator bounded below by 1e-6. In the reference comments run, training coll1 is zero at origins 34, 51, 68 and 85, and about 0.00990 at origin 102. These moments remain in calibration even where the test-score vector excludes coll1. Short submissions has no h=13 statistic because its test window is only 13 weeks; that is a structural omission, separate from the floor.\n']
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
        cell=json.loads((gi.ROOT/'llm_fitting/runs/2026-10-12_ladder/cells'/f"{r['panel']}__gate__{r['rung']}.json").read_text())
        rows.append([NAMES[r['panel']],r['rung'],f"{r['model']:.4f} ± {cell['model_rel_sd']:.4f}",f"{r['baseline']:.4f} ± {cell['base_rel_sd']:.4f}",f"{r['excess']:.4f}",yn(r['rejected_error_rule'])])
    text+=[table(['Panel','Rung','Model ± split SD','Baseline ± split SD','Excess','Reject >0.05'],rows),
           'R0/R1 are prior completed ladder cells (§2z-ap); no non-frozen stack was newly run.\n',
           '## Predictions, including failures\n']
    prediction.insert(0,['P1','all four references reproduce','4 / 4','PASS'])
    prediction.append(['P6','R0 and R1 rejected on all panels',f"{sum(r['rejected_error_rule'] for r in ladder['wrong_model'])} / 8",'PASS' if all(r['rejected_error_rule'] for r in ladder['wrong_model']) else 'FAIL'])
    text+=[table(['Prediction','Declared expectation','Measured','Verdict'],prediction),
           '## Execution, provenance and limits\n',
           'Commands: `zsh llm_fitting/handoff/V1_gate_instrument/launch.zsh`; then, for each panel, the registered Python interpreter runs `scripts/heavy_lock.py -- caffeinate -dims python -u llm_fitting/validation/gate_instrument_analysis.py --panel PANEL`; finally `gate_instrument_report.py`. Substitute the registered interpreter for both occurrences of python. The exact executable is `/Library/Frameworks/Python.framework/Versions/3.11/bin/python3`. All expensive jobs use the shared lock.\n',
           'The wrapper imports the frozen gate, caches primitive seed histograms atomically, preserves pooled medians and scale tie-breaking, and restores all wrapped functions. Both protected model files remain unchanged. Source/input SHA-256, runtime, commit, seeds, attempts and completion markers are beside each run. AUDIT.json verifies identical input hashes, fitted-parameter summaries and empirical moments between reference and bank, unchanged simulation sources, exact reconstruction of full-bank calibration and pooled score, exactly 7,200 seed files per panel, and an ordered digest of their paths and hashes. Full binary seed banks are retained locally under this run directory; compact summaries and provenance are committed. Nine possible test scales are collected for each bank so bootstrap calibration can be replayed without refitting. Descriptive distribution-score printing is off; it does not enter Gate v1.\n',
           'No Instagram optional run, sealed data, WD archive, parameter/default change, candidate gate, adoption decision or paper-claim change was made. Finite-bank uncertainty, temporal dependence, estimation uncertainty and new-specification detection power remain outside this measurement. The empirical-median coverage criterion is reported for actual completed calls but is not itself resampled; this is not a power study of the complete joint gate rule. QUESTIONS.md records the missing ladder seed output; Claude’s committed answer confirms that none exists. This is a resolved limitation, not a pending input.\n',
           'For later owner-approved D1 comparisons, the runner would need an explicit immutable specification/flag map and cache identity covering all parameter fields and forecast arguments. Its current CLI deliberately runs only the four hard-coded frozen stacks; arbitrary candidate support was not built. The primitive seed-histogram format can retain paired incumbent/candidate draws once that extension is approved.\n',
           'Health-check logs in this handoff include one failed zsh invocation of a Bash script; this failed after the research suite and before package/guard checks. Its successful Bash rerun is separate. This is a harness error, not a failed or killed scientific run. All scientific attempts are in ATTEMPTS.jsonl; incomplete attempts must be disclosed if present.\n']
    (dest/'REPORT.md').write_text('\n'.join(text))
    rows=[]
    for p in gi.PANELS:
        for phase,run in [('reference',refs[p]),('bank',banks[p])]:
            r=run['result'];flags='temper, min-knot-n 8, md-lags 6, t-tails, conditional state'
            if p=='facebook_a':flags+=', spec-b'
            if p=='reddit_comments':flags+=', mix-hetero'
            rows.append(['2026-10-12','Codex V1',run['commit'][:10],f"{p} K={gi.le.PANELS[p]['K']}",'reproduction',flags,f"5 × {run['reps']}",f"{r['model_rel']:.6f} ± {r['model_rel_sd']:.6f}",f"{r['base_rel']:.6f} ± {r['base_rel_sd']:.6f}",f"{r['coverage']:.0%}",str(root/p/phase/'gate.log'),f"{phase}; seeds {run['seed_start']}–{run['seed_start']+run['reps']-1}; boot {run['boot']}; same frozen specification"])
    (dest/'GATE_ROWS.md').write_text('# V1 gate-ledger handoff\n\n'+table(['date','session','commit','panel','kind','spec (flags beyond defaults)','splits × reps','model relErr ± SD','baseline ± SD','coverage','log / MODEL_STATUS §','note'],rows)+'\nBootstrap ensembles are derived resummaries of these banks, not additional fitted gate calls. No non-frozen candidate was evaluated. See per-phase ATTEMPTS.jsonl for execution attempts.\n')


    record=['## 2026-10-12 — V1 gate-instrument measurement: no change adopted\n',
        'Provenance: OpenAI Codex, 2026-10-12, starting from ed75e9f4f0a95b2f479f1001835410073b6ed0fe; governing skills at that commit. Design and predictions committed in b787012 before any V1 measurement. Exploratory instrument study and frozen-stack reproduction on DEVELOPMENT panels; Claude integrates this handoff under the approved coordination protocol (§2z-ar).\n',
        '**Question.** How much of the gate’s apparent improvement with more simulations is Monte Carlo bias, how noisy is a comparison, and which moments carry that noise? This measures Gate v1 unchanged, not a candidate or a replacement criterion.\n',
        '**Execution.** Four recorded references reproduced (§2z-g, §2z-ae, §2z-ap): 3 reps / 400 empirical-bootstrap draws on Facebook Era A, comments and short submissions; 20 / 2000 on long submissions. All five frozen origins per panel, seeds starting at zero. Then 80 fresh seeds (1000–1079), nine train calibration scales and all nine corresponding conditional test forecasts per origin. Full sparse displacement histograms, seed RACFs and collision rates preserve pooled medians. Each simulator call is restartable; every actual gate call is in GATE_ROWS.md.\n',
        '**Analysis.** 4,000 bootstrap ensembles per rep count 3/5/10/20/40/80 from the same 80-seed bank; shared weights across origins/scales/train/test, with calibration reselected. Two groups of 2,000 ensembles supply same-stack null differences. Actual disjoint 20/40-seed blocks are also reported. Bands below and in REPORT.md condition on the fixed panels, fits and finite bank: they are not temporal, estimation or population uncertainty.\n']
    validation=[]
    for panel in gi.PANELS:
        before=refs[panel]['result'];after=banks[panel]['result']
        validation.append([NAMES[panel],f"{before['model_rel']:.4f} ± {before['model_rel_sd']:.4f}",
            f"{after['model_rel']:.4f} ± {after['model_rel_sd']:.4f}",f"{after['base_rel']:.4f} ± {after['base_rel_sd']:.4f}",
            f"{before['coverage']:.0%} → {after['coverage']:.0%}",'reference reproduced; no new verdict'])
    record += [table(['Panel','Recorded-setting reproduction ± split SD','80-seed instrument call ± split SD','Historical baseline ± split SD','Coverage: reference → bank','Standing'],validation)]
    keyrows=[]
    for panel,d in analyses.items():
        a,b,c=[d['reps'][str(n)] for n in (3,20,40)]
        drop=a['score']['mean']-b['score']['mean']
        j=sum(a['fixed_jensen'].values())-sum(b['fixed_jensen'].values())
        keyrows.append([NAMES[panel],pm(a['score']),pm(b['score']),f'{drop:.5f}',
            f'{j/drop:.1%}' if drop>0 else 'no decline',f"{b['resolution95']:.5f} / {c['resolution95']:.5f}"])
    record += [table(['Panel','3-rep error ± MC SD','20-rep error ± MC SD','3→20 decline','Jensen share of decline','95% |same-stack difference|: 20 / 40 reps'],keyrows),
        'Jensen, scale reselection and the score of the mean simulated moments are separately tabulated in REPORT.md and add to the total change. Jensen shares may exceed 100% when another term offsets the decline. The per-moment error and covariance-allocated variance shares, every origin’s resolution estimate and MOM_FLOOR exclusions are retained. Short submissions has only six available moments: h=13 is outside its test-window rule. Calibration itself does not apply MOM_FLOOR.\n',
        '**Predictions, with failures retained.**\n',table(['Prediction','Declared expectation','Measured','Verdict'],prediction),
        '**Validation and scope.** Every recorded reference reproduces; the new bank changes only simulation seeds/count and records all calibration possibilities. No adoption is adjudicated. R0 and R1 from the stored ladder are rejected on all four panels by the existing +0.05 error rule; no non-frozen rung was rerun. All 26 adjacent ladder pairs are shown under three illustrative margin rules, with coverage separate. No rule is selected. The ladder did not save per-seed outputs, as Claude confirmed in ANSWERS.md, so its measured split-pairing benefit cannot be interpreted as seed-pairing variance reduction.\n',
        '**Limitations / not run.** No Instagram optional run, sealed data, WD archive, candidate gate, default change or paper-claim change. Candidate-specific paired resolution and power are not identified by same-stack noise. The empirical-median coverage criterion is not itself resampled. Whole-pipeline and future-time uncertainty are not measured here. No scientific run was killed or substituted. One queued health-check wrapper was cancelled before execution, and one health invocation used zsh on a Bash script and failed after the research suite; successful full Bash checks are retained separately.\n',
        '**Code and validation.** The two protected model files remain unchanged. New wrappers, analysis, report and plotting code are under validation/. Eleven targeted known-answer tests cover exact pooled histograms, original-simulator parity, cache restart, calibration ties, floor/missing moments, folded-normal bias/noise, covariance allocation and full analysis decomposition. The full check at the latest recorded pre-analysis commit passed 425 research tests + 13 subtests, 8 package tests and the legacy 14/15, churn 0.013 guard. Final handoff health is recorded in HEALTH_FINAL.log. AUDIT.json checks unchanged inputs/fits/empirical moments/source hashes, reconstructs full-bank calibration and scores, and verifies 28,800 seed files with digests.\n',
        '**Commands and archive.** Run from the assigned Codex copy with the registered Python 3.11 interpreter: `zsh llm_fitting/handoff/V1_gate_instrument/launch.zsh`, then `zsh llm_fitting/handoff/V1_gate_instrument/launch_analysis.zsh`; the launchers use heavy_lock.py and caffeinate. Plotting: `python llm_fitting/validation/gate_instrument_plot.py` with MPLCONFIGDIR inside the run directory. Archive: `llm_fitting/runs/2026-10-12_V1_gate_instrument/`; complete report, ledger rows, predictions and figures: `llm_fitting/handoff/V1_gate_instrument/`. Compact summaries/provenance are committed; full binary seed banks remain in the Codex working copy.\n']
    (dest/'STATUS_ENTRY.md').write_text('\n'.join(record))


if __name__=='__main__':main()
