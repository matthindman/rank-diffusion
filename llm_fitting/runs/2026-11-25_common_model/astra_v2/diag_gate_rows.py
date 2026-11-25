"""Emit the local gate-ledger handoff for every primary and ablation gate."""
from pathlib import Path
import json
from diag_result_tables import table

def main():
    native=json.loads(Path('candidate_frozen_gate_results.json').read_text())
    ab=json.loads(Path('diag_ablation.json').read_text())
    rows=[]
    for data,name,file in [(native,('Astra S2, robust filter','Astra S3, robust filter and mixed-moment check'),'candidate_frozen_gate_results.json'),
                           ({n:v['gate'] for n,v in ab.items()},('S3, robust filter alone','S3, mixed-moment check alone'),'diag_ablation.json')]:
        for n,g in data.items():
            for v,label in zip(('s2','s3'),name):
                rows.append(['2026-11-25','OpenAI Astra','7ebb761+',
                             f"{n} K={g['K']}",'candidate' if data is native else 'exploratory',
                             label,'5 × 20 (boot 2000)',f"{g[v+'_rel']:.3f} ± {g[v+'_sd']:.3f}",
                             f"{g['base_rel']:.3f} ± {g['base_sd']:.3f}",f"{g[v+'_cov']:.0%}",file,
                             f"{g[v+'_wins_base']}/5 vs historical mobility"])
    Path('GATE_ROWS.md').write_text('# Gate ledger handoff\n\nExact frozen scoring on the four authorized DEVELOPMENT caches. See ASTRA_REPORT.md and EXPERIMENT_LOG.md.\n\n'+table(['Date','Session','Commit','Panel','Kind','Specification','Splits × reps','Model error ± SD','Baseline ± SD','Coverage','Local archive','Note'],rows)+'\n')
if __name__=='__main__':
    main()
