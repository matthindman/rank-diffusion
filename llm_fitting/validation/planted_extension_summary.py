"""Fitted home-reversion as a diagnostic under the addendum alternatives."""
import itertools
from pathlib import Path
import numpy as np
import pandas as pd
from validation import planted_crosscheck as pc
from validation import planted_extension as extension
from validation import planted_summary as summary

def fitted_kappa(out):
    out=Path(out);all_seeds=[];all_knots=[];summaries=[];pairs=[];failures=[];cycle=[]
    conditions=["baseline"]+extension.FIT_CONDITIONS
    for panel in pc.PANELS:
        pars,p=extension.study.get_params(panel,out)
        groups=summary.group_for(p.z_knots,p.N,pars["K"])
        values={}
        for c in conditions:
            stage="recovery" if c=="baseline" else "extension_fits"
            rows=summary.load_rows(out,panel,stage,c,range(1000,1020))
            vals={g:[] for g in ("head","middle","shell","tail","native_edge_fraction")}
            for r in rows:
                if r["status"]!="RAN-OK":
                    failures.append(dict(panel=panel,condition=c,seed=r["seed"],traceback=r["traceback"]))
                    for g in vals:vals[g].append(np.nan)
                    continue
                k=np.asarray(r["result"]["estimates"]["kappa_z"],float)
                for j,a in enumerate(k):
                    all_knots.append(dict(panel=panel,condition=c,seed=r["seed"],knot=j,z=p.z_knots[j],group=groups[j],kappa=a))
                for g in vals:
                    v=r["result"]["kappa_edge_fraction"] if g=="native_edge_fraction" else np.median(k[groups==g]) if np.any(groups==g) else np.nan
                    vals[g].append(v)
                    all_seeds.append(dict(panel=panel,condition=c,seed=r["seed"],statistic=g,value=v))
            values[c]=vals
            for g,v in vals.items():
                summaries.append(dict(panel=panel,condition=c,statistic=g,**summary.summary(v)))
        for ca,cb in itertools.combinations(conditions,2):
            for g in values[ca]:
                pairs.append(dict(panel=panel,condition_a=ca,condition_b=cb,statistic=g,
                                  **summary.separation(values[ca][g],values[cb][g])))
        for c in ("baseline","annual_cycle_0","annual_cycle_1"):
            rows=summary.load_rows(out,panel,"diagnostics",c,range(1000,1020))
            a=[]
            for r in rows:
                f=r["result"]["features"] if r["status"]=="RAN-OK" else {}
                one=f.get("momentum/block=13/group=middle/pair=gap4/slope")
                two=f.get("momentum/block=13/group=middle/pair=gap2/slope")
                a.append(float(one)-float(two) if one is not None and two is not None else np.nan)
            cycle.append(dict(panel=panel,condition=c,**summary.summary(a)))
    for name,rows in (("fitted_kappa_seeds",all_seeds),("fitted_kappa_knots",all_knots),
                      ("fitted_kappa_summary",summaries),("fitted_kappa_pairs",pairs),
                      ("annual_cycle_gap_contrast",cycle)):
        pd.DataFrame(rows).to_csv(out/(name+".csv"),index=False)
    pc.write(out/"extension_failures.json",failures)
    pc.write(out/"extension_analysis_provenance.json",dict(sources=extension.sources(),analysis_sha256=pc.digest(__file__)))
