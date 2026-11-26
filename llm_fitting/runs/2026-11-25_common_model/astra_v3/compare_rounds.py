"""Build the tables used in ASTRA_R7_REPORT.md directly from the four rounds' JSONs."""
import json
from pathlib import Path

import numpy as np

WORK = Path(__file__).resolve().parent
PARENT = WORK.parent
NAMES = {'facebook_a': 'Facebook Era A', 'instagram_hm': 'Instagram apparatus',
         'reddit_comments_ext': 'Reddit Comments', 'reddit_submissions_long': 'Reddit Submissions'}
GATE = 'candidate_frozen_gate_results.json'
GOLD = 'candidate_gold_benchmark_results.json'
OOP = 'oop_2019_20_to_2021_22'
ARMS = ('s2', 's3')


def load(path):
    return json.loads(path.read_text())


def rounds():
    result = []
    for directory in (PARENT/'astra_v2', PARENT/'fable_v2', WORK/'baseline_r6', WORK):
        g = load(directory/GOLD)['panels']
        f = load((PARENT/'round4_baseline' if directory.name == 'astra_v2' else directory)/GATE)
        result.append((f, g))
    # Adopted R4 paired the median filter with the original S3, not the rejected
    # sign-screen S3 in astra_v2's main output. The factorial filter-only slot
    # is named S2 by its runner, even though it fits S3. R4 unconditional S3
    # has the original R3 dynamics and its seed bank is identical.
    ab = load(PARENT/'astra_v2/diag_ablation.json')
    old = load(PARENT/'round3_gold_benchmark_results.json')['panels']
    for pn, x in result[0][1].items():
        x['g1_s3'] = old[pn]['g1_s3']
        x['params_s3'] = old[pn]['params_s3']
        for h, y in x['goal2']['by_horizon'].items():
            a = ab[pn]['goal2']['by_horizon'][h]
            for key in tuple(y):
                if key.endswith('_s3') and key[:-3] + '_s2' in a:
                    y[key] = a[key[:-3] + '_s2']
        if x[OOP]:
            x[OOP]['S3'] = ab[pn]['oop']['S2']
    return result


def num(x, d=3):
    return f'{x:.{d}f}' if np.isfinite(x) else '—'


def band(x):
    return f"{num(x['median'])} [{num(x['q05'])}, {num(x['q95'])}]"


def table(header, rows):
    def line(cells):
        return '| ' + ' | '.join(map(str, cells)) + ' |'
    return '\n'.join([line(header), line(['---']*len(header)), *(line(row) for row in rows)])


def get(d, path):
    for p in path:
        d = d[p]
    return d


def struct_rows(k, calendar=False):
    rows = [('Top-10 turnover', ('turnover','10')), ('K/4 turnover', ('turnover',str(k//4))),
            ('Weekly rank-1 change', ('occ_change','1')), ('K/64 share in top K', ('head_share',)),
            ('Boundary ratio', ('boundary_ratio',)), ('Weekly departure rate', ('dep_rate',)),
            ('Immediate crossing fraction', ('dep_imm_cross_frac',)), ('Absent then observed within 13w', ('dep_return13_frac',)),
            ('Absent, no 13w return seen', ('dep_terminal_frac',)), ('Gapped ρ', ('gapped_rho',)),
            ('D', ('D',)), ('Faller persistence', ('p_fall',)), ('Riser persistence', ('p_rise',))]
    for label, r in [('1',1),('K/64',k//64),('K/16',k//16),('K/4',k//4)]:
        rows.extend([(f'Final ladder span at {label}', ('lad_last',str(r))),
                     (f'Change in ladder span at {label}', ('lad_change',str(r)))])
    if calendar:
        rows[:2] = [('Matched top-10 turnover', ('calendar_turn_10',)),
                     ('Matched K/4 turnover', (f'calendar_turn_{k//4}',))]
    return rows


def gate_table(rs):
    rows = []
    for pn, name in NAMES.items():
        f = [data[0][pn] for data in rs]
        for arm in ARMS:
            key = arm+'_rel'
            diffs = np.array([x[key] for x in f[-1]['splits']])-np.array([x[key] for x in f[-2]['splits']])
            rows.append([name, arm.upper(), num(f[-1]['base_rel']),
                         *[f"{num(x[key])} ± {num(x[arm+'_sd'])}" for x in f],
                         f'{diffs.mean():+.3f} ± {diffs.std():.3f}', int(np.sum(diffs < -1e-12)),
                         f"{f[-1][arm+'_cov']:.0%}", f"{f[-1][arm+'_wins_base']}/5"])
    return table(['Panel', 'Arm','Historical', 'R4', 'R5', 'Staged R6', 'R7',
                  'Paired Δ ± SD', 'R7 < R6 / 5', 'R7 median in CI', 'R7 < historical'],rows)


def gate_contributions(rs):
    rows = []
    for pn, panel in NAMES.items():
        old, new = [r[0][pn] for r in rs[-2:]]
        for a in ARMS:
            key = 'err_vec_'+a
            parts = {}
            for o, n in zip(old['splits'], new['splits']):
                for k in set(o[key]) | set(n[key]):
                    change = n[key].get(k,0) / len(n[key]) - o[key].get(k,0) / len(o[key])
                    parts[k] = parts.get(k,0) + change / len(new['splits'])
            total = sum(parts.values())
            assert np.isclose(total, new[a+'_rel']-old[a+'_rel'])
            cells=[f'{parts[k]:+.3f}' if k in parts else '—'
                   for k in ('dRank1','dRank4','dRank13','RACF1','coll1','coll5','coll20')]
            rows.append([panel,a.upper(),*cells,f'{total:+.3f}'])
    return table(['Panel','Arm','Δrank 1w','Δrank 4w','Δrank 13w','RACF1','Coll. 1','Coll. 5','Coll. 20','Total'],rows)


def panel_tables(pn, rs):
    name = NAMES[pn]
    xs = [data[1][pn] for data in rs]
    c = xs[-1]
    emp = c['emp']
    k = c['K']
    text = [f'### {name}', 'Full-panel structure. Entries for R6 and R7 give the seed median and its 5th–95th percentile band. The earlier rounds show the median. The empirical intervals on ρ and D are entity-cluster bootstrap intervals.']
    for arm in ARMS:
        rows = []
        for label, path in struct_rows(k):
            data = get(emp, path)
            cell = num(data)
            if path[0] in ('gapped_rho','D') and path[0] in c['emp_cluster_ci']:
                ci = c['emp_cluster_ci'][path[0]]
                cell += f" [{num(ci['q05'])}, {num(ci['q95'])}]"
            vals = [get(x['g1_'+arm], path) for x in xs]
            rows.append([label, cell, *[num(v['median']) for v in vals[:2]], *map(band, vals[2:])])
        text += [f'**{arm.upper()} structure**', table(['Measure', 'Observed', 'R4', 'R5', 'Staged R6', 'R7'], rows)]
    rows = [['Observed', *map(num, emp['dec_y'])]]
    for arm in ARMS:
        for i, x in enumerate(xs,4):
            dec = x['g1_'+arm]['dec_y']
            rows.append([f'R{i} {arm.upper()}', *[num(v['median']) for v in dec]])
            if i==7:
                rows.extend([[f'R7 {arm.upper()} {label}', *[num(v[key]) for v in dec]] for key,label in [('q05','5%'),('q95','95%')]])
    text += ['**Ten-decile hook**', 'Cells give the mean next-window change by decile of prior change, summarized by the median across simulated worlds.',
             table(['Curve', *range(1,11)],rows)]
    for arm in ARMS:
        rows = []
        for h in c['goal2']['by_horizon']:
            for i, x in enumerate(xs,4):
                y=x['goal2']['by_horizon'][h]
                metrics=[y[key+'_'+arm] for key in ['crps_X','mae_kal','cov90','pit_sd','crps_lr','brier_hd']]
                rows.append([h,f'R{i}',*[num(v,4) for v in metrics[:2]],num(y['mae_ll'],4),
                             *[num(v,4) for v in metrics[2:]],num(1e6*y['share_mae_hd_'+arm],1),num(y['share_corr_hd_'+arm])])
        text += [f'**{arm.upper()} endpoint scores**',
                 table(['h (weeks)', 'Round', 'X CRPS','Kalman MAE','EWMA MAE','90% coverage','PIT SD','logR CRPS','Fair head Brier','Head Δshare MAE ×10⁶','Head Δshare corr.'],rows)]
    rows=[]
    for h,new in c['goal2']['by_horizon'].items():
        old=xs[-2]['goal2']['by_horizon'][h]
        for arm in ARMS:
            cells=[]
            for key,scale in [('crps_X',1),('crps_lr',1),('share_mae_hd',1e6)]:
                field=key+'_'+arm
                delta=np.array([n[field]-o[field] for o,n in zip(old['by_origin'],new['by_origin'])])*scale
                d=1 if scale>1 else 4
                cells.append(f'{delta.mean():+.{d}f} ± {delta.std():.{d}f} ({int(np.sum(delta < -1e-12))}/5)')
            rows.append([h,arm.upper(),*cells])
    text+=['**Paired endpoint changes against staged R6**',
           'Mean R7 minus R6, between-origin SD and number of origins with lower R7 loss. The five overlapping origins give descriptive spread.',
           table(['h (weeks)','Arm','X CRPS','logR CRPS','Head Δshare MAE ×10⁶'],rows)]
    if c[OOP]:
        text += ['**Frozen row-104 transfer**']
        for arm in ARMS:
            rows=[]
            for label,path in struct_rows(k):
                vals = [get(x[OOP][arm.upper()]['sim'],path) for x in xs]
                rows.append([label,num(get(c[OOP]['emp_2019_20'],path)), num(get(c[OOP]['emp_2021_22'],path)),
                             *[num(v['median']) for v in vals[:2]], *map(band,vals[2:])])
            text += [arm.upper(),table(['Measure','Train','Test','R4','R5','Staged R6','R7'],rows)]
    return '\n\n'.join(text)


def calendar_tables(cals, rs):
    rows = []
    for pn, d in cals.items():
        k=rs[-1][1][pn]['K']
        for label,path in struct_rows(k,calendar=True):
            if path[0]=='lad_change': continue  # only one complete frozen block
            x=d['r7']
            models=[get(d[tag][arm]['sim'],path) for arm in ('S2','S3') for tag in ('r6_source','r7')]
            rows.append([NAMES[pn],label,num(get(x['emp_2021_22'],path)), *map(band,models)])
    return table(['Panel','Measure','Calendar test','R6 source S2','R7 S2','R6 source S3','R7 S3'],rows)


def same_measurements(x,y):
    if isinstance(x, dict):
        assert x.keys() == y.keys()
        for key in x:
            same_measurements(x[key],y[key])
    else:
        np.testing.assert_equal(x,y)


def validate(rs, cal):
    for pn in NAMES:
        f,c = rs[-1][0][pn],rs[-1][1][pn]
        assert len(f['origins']) == len(f['splits']) == 5
        assert c['goal2']['n_ens'] == 39
        horizons = {'facebook_a':[1,4,13,26],'instagram_hm':[1,4,13]}.get(pn,[1,4,13,26,52])
        assert list(map(int,c['goal2']['by_horizon'])) == horizons
        for baseline, gold in rs[:-1]:
            b,x = baseline[pn],gold[pn]
            assert b['origins'] == f['origins'] and x['goal2']['origins'] == c['goal2']['origins']
            assert b['base_rel'] == f['base_rel']
            for v,w in zip(f['splits'],b['splits']):
                np.testing.assert_allclose(list(v['em'].values()),list(w['em'].values()),equal_nan=True)
            np.testing.assert_allclose(c['emp']['dec_y'], x['emp']['dec_y'],equal_nan=True)
        staged=rs[-2][1][pn]
        same_measurements(c['emp'],staged['emp'])
        same_measurements(c['emp_cluster_ci'],staged['emp_cluster_ci'])
        for h, y in c['goal2']['by_horizon'].items():
            baseline=staged['goal2']['by_horizon'][h]
            for key in ('n_ok_x','n_topK','n_head','alpha_ll','mae_ll','crps_lr_rb','brier_hd_rb','share_mae_ll_hd'):
                assert y[key] == baseline[key]
                assert [o[key] for o in y['by_origin']] == [o[key] for o in baseline['by_origin']]
        for a in ARMS:
            assert np.isclose(f[a+'_rel'],np.mean([v[a+'_rel'] for v in f['splits']]))
            assert len(c['g1_'+a]['dec_y'])==10
            for h, y in c['goal2']['by_horizon'].items():
                assert len(y['by_origin']) == 5
                assert [o['T0'] for o in y['by_origin']] == c['goal2']['origins']
                for field in ('crps_X','mae_kal','cov90','pit_sd','crps_lr','brier_hd','share_mae_hd','share_corr_hd'):
                    k=field+'_'+a
                    assert np.isfinite(y[k]) and np.isclose(y[k],np.mean([o[k] for o in y['by_origin']]))
        if c[OOP]:
            same_measurements(c[OOP]['emp_2021_22'],staged[OOP]['emp_2021_22'])
            same_measurements(cal[pn]['r7']['emp_2021_22'],cal[pn]['r6_source']['emp_2021_22'])
            assert cal[pn]['dates'] == ['2019-01-07','2020-12-28','2021-01-04','2022-12-19']
            for tag in ('r6_source','r7'):
                assert cal[pn][tag]['T_tr']==104 and cal[pn][tag]['T_te']==103


def validate_strata(rs):
    d=load(WORK/'forecast_strata.json')
    for name, models in d.items():
        for label, rows in models.items():
            gold = rs[-1 if label=='r7' else -2][1][name]['goal2']
            for h,ref in gold['by_horizon'].items():
                part=[o for o in rows if o['h']==int(h) and o['band']=='all']
                assert [o['T0'] for o in part] == gold['origins']
                for o,r in zip(part,ref['by_origin']):
                    assert o['n'] == r['n_ok_x']
                    assert np.isclose(1-o['below']-o['above'],r['cov90_s2'],atol=1e-12,rtol=0)
                    assert np.isclose(o['crps'],r['crps_X_s2'],atol=1e-12,rtol=0)


if __name__ == '__main__':
    rs=rounds()
    calendar=load(WORK/'calendar_transfer.json')
    validate(rs,calendar)
    validate_strata(rs)
    same_measurements(load(WORK/GOLD)['oracle_check'], load(WORK/'baseline_r6'/GOLD)['oracle_check'])
    print('## Gate v1 across four rounds\n')
    print(gate_table(rs))
    print('\nAverage contributions to the paired gate change, with the frozen per-split denominator.\n')
    print(gate_contributions(rs))
    print('\n## Exact-calendar Reddit transfer\n')
    print(calendar_tables(calendar,rs))
    print('\n## Full results by panel\n')
    print('\n\n'.join(panel_tables(pn,rs) for pn in NAMES))
