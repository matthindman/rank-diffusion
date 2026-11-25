"""Render §5 of ASTRA_REPORT.md from the supplied Round 3 and candidate archives."""
from pathlib import Path
import json
import numpy as np

def load(path):
    return json.loads(Path(path).read_text())

def fmt(x, digits=3):
    return '—' if not np.isfinite(x) else f'{x:.{digits}f}'

def band(x, digits=3):
    if not np.isfinite(x["median"]):
        return "—"
    return f"{fmt(x['median'],digits)} [{fmt(x['q05'],digits)}, {fmt(x['q95'],digits)}]"

def table(headers, rows):
    return '\n'.join(['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|']+
                     ['| '+' | '.join(map(str,row))+' |' for row in rows])

def lookup(v, path):
    for key in path:
        v=v[key]
    return v

def structure(b2,b3,c2,c3,emp,K,ci=None,train=None):
    rows=[]
    def row(label,path):
        v=lookup(emp,path)
        cell=fmt(v)
        if ci and path[0] in ci:
            q=ci[path[0]]
            cell+=f" [{fmt(q['q05'])}, {fmt(q['q95'])}]"
        left=[label]+([fmt(lookup(train,path))] if train else [])+[cell]
        rows.append(left+[band(lookup(obj,path)) for obj in (b2,c2,b3,c3)])
    for label, key in [('Top-10 turnover',10),('K/64 turnover',K//64),('K/16 turnover',K//16),('K/4 turnover',K//4),('K/2 turnover',K//2)]:
        row(label,['turnover',str(key)])
    for key in (1,10):
        row(f'Weekly occupant change at rank {key}',['occ_change',str(key)])
    for label,key in [('Maximum log activity','max_X'),('Boundary ratio','boundary_ratio'),
                      ('Departure rate','dep_rate'),('Immediate crossing / departures','dep_imm_cross_frac'),
                      ('Absent then returns / departures','dep_return13_frac'),
                      ('Absent beyond window / departures','dep_terminal_frac'),
                      ('Gapped persistence ρ','gapped_rho'),('Asymmetry D','D'),('Faller slope','p_fall'),
                      ('Riser slope','p_rise'),('Mixed moment m21','m21'),('Mixed moment m12','m12'),
                      ('Leverage','lev'),('Two-block decline/gain run ratio','r2'),('Three-block decline/gain run ratio','r3')]:
        row(label,[key])
    for i in range(len(emp['head_share_blocks'])):
        row(f'Head share, block {i+1}',['head_share_blocks',i])
    for k in (1,K//64,K//16,K//4):
        for i in range(len(emp['lad_blocks'][str(k)])):
            row(f'Log ladder span, rank {k}, block {i+1}',['lad_blocks',str(k),i])
    for i in range(10):
        row(f'E[y | decile {i+1}]'.replace('|','given'),['dec_y',i])
    return table(['Statistic']+(['Train observed'] if train else [])+['Observed','R3 S2','Astra S2','R3 S3','Astra S3'],rows)

def render():
    b_gate=load('../round3_frozen_gate_results.json'); g_gate=load('candidate_frozen_gate_results.json')
    b_gold=load('../round3_gold_benchmark_results.json')['panels']
    c_all=load('candidate_gold_benchmark_results.json')
    g_gold=c_all['panels']; ref=load('diag_reference.json'); abl=load('diag_ablation.json')
    names=dict(facebook_a='Facebook',instagram_hm='Instagram apparatus',reddit_comments_ext='Reddit Comments',
               reddit_submissions_long='Reddit Submissions')
    out=['## 5. Full before-and-after results','### Frozen Gate v1',
         'Five rolling origins and 20 members per origin, with 2,000 bootstrap draws. Each cell gives mean relative error ± split SD, the percentage of empirical displacement-median intervals containing the model median, and wins against historical mobility. Paired differences reuse origins and seeds. The five overlapping splits limit inference.']
    rows=[]
    for n,g in g_gate.items():
        b=b_gate[n]
        for v in ['s2','s3']:
            def gate_cell(g):
                return f"{g[v+'_rel']:.3f} ± {g[v+'_sd']:.3f} · {g[v+'_cov']:.0%} · {g[v+'_wins_base']}/5"
            delta=np.array([c[v+'_rel']-o[v+'_rel'] for o,c in zip(b['splits'],g['splits'])])
            rows.append([names[n],v.upper(),f"{g['base_rel']:.3f} ± {g['base_sd']:.3f}",gate_cell(b),gate_cell(g),
                         f'{delta.mean():+.3f} ± {delta.std():.3f}',f'{sum(delta<0)}/5'])
    out.append(table(['Panel','Model','Historical mobility','Round 3','Astra','Paired Δ ± SD','Astra wins vs R3'],rows))
    moments=[]
    for n,g in g_gate.items():
        for key in ('dRank1','dRank4','dRank13','RACF1','coll1','coll5','coll20'):
            splits=g['splits']
            eligible=sum(key in r['err_vec_s2'] for r in splits)
            if key not in splits[0]['em']:
                continue
            def pooled(obj,which):
                a=[r[which].get(key,np.nan) for r in obj['splits']]
                return fmt(float(np.nanmean(a))) if np.any(np.isfinite(a)) else '—'
            moments.append([names[n],key,f'{eligible}/5',pooled(g,'em'),pooled(g,'bm'),
                           pooled(b_gate[n],'sm2'),pooled(g,'sm2'),pooled(b_gate[n],'sm3'),pooled(g,'sm3')])
    out.append('<details><summary>All gate moments · arithmetic means of the per-origin statistics</summary>\n\n'
               'The eligibility column counts splits in which the frozen moment floor admits the moment. Means here use all available split values. Gate scores above average eligible relative errors within each split.\n\n'+
               table(['Panel','Moment','Eligible','Observed','Historical','R3 S2','Astra S2','R3 S3','Astra S3'],moments)+'\n\n</details>')
    out.append('The historical frozen Model A errors in §2z-dy are 0.103, 0.257, 0.123 and 0.188 in panel order. The corresponding RDM′ values where reported are 0.123 on Facebook, 0.130 on Comments and 0.183 on Submissions. These comparisons reuse recorded runs.')
    out.append('### Structure, endpoint forecasts and time transfer')
    out.append('Structure cells give the median [5th, 95th percentile] of 20 seeded worlds. Observed asymmetry bands use the evaluator’s 200 endpoint-cluster resamples. Each block share uses top K/64 within the recorded top K. The endpoint forecast head is top K/16. Conditional activity scores use the common observed-truth cohort. PIT SD targets 0.289, coverage targets 0.900, and randomized PIT mean targets 0.500. All endpoint rows average the same five origins. Each arrow runs from Round 3 to Astra. Share MAEs are multiplied by 10⁶. A constant zero shift has undefined correlation. The JSON archives contain unrounded values, including every split and origin.')
    for n,p in g_gold.items():
        b=b_gold[n]; K=p['K']
        out.append(f"#### {names[n]} · K = {K:,}")
        out.append('<details><summary>All in-sample structure statistics, blocks and deciles</summary>\n\n'+structure(
            b['g1_s2'],b['g1_s3'],p['g1_s2'],p['g1_s3'],p['emp'],K,p['emp_cluster_ci'])+'\n\n</details>')
        c_g=p['goal2']['by_horizon'];b_g=b['goal2']['by_horizon']
        def pair(k,v,digits=4,mult=1):
            return fmt(b_g[h][k+'_'+v]*mult,digits)+' → '+fmt(c_g[h][k+'_'+v]*mult,digits)
        rows=[]; rank=[]; share=[]; pit=[]
        for h,cell in c_g.items():
            no_brier=np.mean([v['brier_no'][h] for v in ref[n]])
            for v in ('s2','s3'):
                rows.append([h,v.upper(),fmt(cell['mae_no'],4),fmt(cell['mae_ll'],4),
                            pair('mae_kal',v),pair('mae_ens',v),pair('crps_X',v)])
                pit.append([h,v.upper(),pair('cov90',v),pair('pit_mean',v),pair('pit_sd',v),pair('pit_ks',v)])
                rank.append([h,v.upper(),fmt(cell['crps_lr_no'],4),fmt(cell['crps_lr_rb'],4),pair('crps_lr',v),
                             fmt(no_brier,4),fmt(cell['brier_hd_rb'],4),pair('brier_hd',v)])
                share.append([h,v.upper(),fmt(cell['share_mae_no_hd']*1e6,2),fmt(cell['share_mae_ll_hd']*1e6,2),
                            pair('share_mae_hd',v,2,1e6),fmt(cell['share_corr_ll_hd']),pair('share_corr_hd',v,3)])
        out += [table(['Week','Model','No change MAE','EWMA MAE','Kalman X MAE','Median X MAE','Fair X CRPS'],rows),
                table(['Week','Model','90% coverage','PIT mean','PIT SD','PIT KS'],pit),
                table(['Week','Model','No change logR','Rank-bin logR','Fair logR CRPS','No change Brier','Rank-bin Brier','Fair head Brier'],rank),
                table(['Week','Model','Zero shift MAE','EWMA MAE','Head shift MAE','EWMA corr','Head shift corr'],share)]
        paired=[]
        for h,cell in c_g.items():
            for v in ('s2','s3'):
                row=[h,v.upper()]
                for k,mult in [('crps_X',1),('mae_kal',1),('cov90',1),('crps_lr',1),('brier_hd',1),('share_mae_hd',1e6)]:
                    key=k+'_'+v
                    d=mult*np.array([new[key]-old[key] for new,old in zip(cell['by_origin'],b_g[h]['by_origin'])])
                    row.append(f'{d.mean():+.4f} ± {d.std():.4f}')
                paired.append(row)
        out.append('<details><summary>Paired forecast changes across five origins · mean Δ ± split SD</summary>\n\n'+
                   table(['Week','Model','X CRPS','Kalman MAE','Coverage','logR CRPS','Head Brier','Head shift MAE × 10⁶'],paired)+'\n\n</details>')
        if p['oop_2019_20_to_2021_22']:
            c=p['oop_2019_20_to_2021_22'];o=b['oop_2019_20_to_2021_22']
            out.append(f"**Forward time transfer.** Fit and filter the first {c['T_tr']} weeks, then simulate all {c['T_te']} subsequent weeks. Each column uses its own training fit.")
            out.append('<details><summary>All forward structure statistics, blocks and deciles</summary>\n\n'+
                       structure(o['S2']['sim'],o['S3']['sim'],c['S2']['sim'],c['S3']['sim'],
                                 c['emp_2021_22'],K,train=c['emp_2019_20'])+'\n\n</details>')
            calendar=load('diag_calendar_oop.json')[n]
            exact=calendar['candidate']
            old=calendar['round3']
            out.append(f"**Exact calendar transfer.** Weekly labels {calendar['start']}–{calendar['train_end']} train the model, then {calendar['test_start']}–{calendar['test_end']} supply the target. There are only 103 test weeks, so the original 52-week block turnover is undefined. The annual comparison below uses the first 51 available weeks of each year. The omitted final week of 2021 is still simulated.")
            out.append('<details><summary>Exact calendar results, full frozen statistics</summary>\n\n'+
                       structure(old['S2']['sim'],old['S3']['sim'],exact['S2']['sim'],exact['S3']['sim'],
                                 exact['emp_2021_22'],K,train=exact['emp_2019_20'])+'\n\n</details>')
            y=load('diag_calendar_blocks.json')[n]
            exact,old=y['candidate'],y['round3']
            ys=[]
            for label,key in [('Top-10 turnover','turn_10'),('K/4 turnover',f'turn_{K//4}'),
                              ('2021 head share','year1_head'),('2022 head share','year2_head'),
                              ('2021 rank-1 span','year1_span_1'),('2022 rank-1 span','year2_span_1')]:
                ys.append([label,fmt(exact['emp_2021_22'][key])]+
                          [band(model[v]['sim'][key]) for v in ('S2','S3') for model in (old,exact)])
            out.append(table(['51-week annual blocks','Observed','R3 S2','Astra S2','R3 S3','Astra S3'],ys))
    out.append('### Attribution within S3')
    out.append('The off-diagonal runs apply each change on its own. The identical evaluator uses its two output slots for the filter-only and sign-only S3 variants, respectively. The separate ablation archive gives all proper scores by horizon and both Reddit forward tests. Unconditional structure depends only on the fitted process.')
    rows=[]
    for n,a in abl.items():
        def gcell(d,k):
            return f"{d[k+'_rel']:.3f} ± {d[k+'_sd']:.3f} · {d[k+'_cov']:.0%} · {d[k+'_wins_base']}/5"
        rows.append([names[n],gcell(b_gate[n],'s3'),gcell(a['gate'],'s2'),gcell(a['gate'],'s3'),gcell(g_gate[n],'s3')])
    out.append(table(['Panel','Round 3 S3','Filter only','Sign only','Both'],rows))
    rows=[]
    for n,a in abl.items():
        for h,cell in a['goal2']['by_horizon'].items():
            def triplet(d,s):
                return f"{d['crps_X_'+s]:.4f} / {d['cov90_'+s]:.3f} / {d['share_mae_hd_'+s]*1e6:.2f}"
            rows.append([names[n],h,triplet(b_gold[n]['goal2']['by_horizon'][h],'s3'),triplet(cell,'s2'),triplet(cell,'s3'),triplet(g_gold[n]['goal2']['by_horizon'][h],'s3')])
    out.append('<details><summary>All horizons for the four S3 combinations · activity CRPS / coverage / head shift MAE × 10⁶</summary>\n\n'+table(['Panel','Week','Round 3 S3','Filter only','Sign only','Both'],rows)+'\n\n</details>')
    return '\n\n'.join(out)

if __name__=='__main__':
    p=Path('ASTRA_REPORT.md')
    s=p.read_text()
    a=s.index('## 5.'); b=s.index('## 6.',a)
    p.write_text(s[:a]+render()+'\n\n'+s[b:])
