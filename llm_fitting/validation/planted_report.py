"""Create a reviewable V2 handoff from complete checkpoints."""
from __future__ import annotations
import argparse,gzip,json,sys
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import planted_crosscheck as pc
from validation import planted_study as study

DISPLAY=[
 ("Head anchored tail h13","tails/group=head/h=13/mode=anchor/r9990"),
 ("Head downward retention h13","retention/cls=big_down/group=head/k=13/retention"),
 ("Absence exit rate","departure/rate_absence"),
 ("Lasting exit rate","departure/rate_lasting"),
 ("Repeat exit rate","departure/rate_repeat"),
 ("Cohort time outside","fate/share_time_outside"),
 ("Middle dispersion h26","dispersion/group=middle/h=26/robust_var"),
 ("Middle skip momentum b13","momentum/block=13/group=middle/pair=skip/slope"),
 ("Middle volatility ratio","volatility/group=middle/ratio"),
 ("Head far tail q9999","far_tail/group=head/z_q9999_up"),
 ("Middle amplitude coherence h13","coherence/group=middle/h=13/coherence")]
LABELS={"facebook_a":"Facebook Era A","reddit_comments":"Reddit comments",
        "reddit_submissions_long":"Long submissions"}
def fmt(v):
    if v is None:return "unavailable"
    if isinstance(v,(float,np.floating)):return f"{v:.5g}" if np.isfinite(v) else "undefined"
    return str(v)
def md(df,columns=None):
    if columns is not None:df=df[columns]
    return "| "+" | ".join(map(str,df.columns))+" |\n| "+" | ".join("---" for _ in df.columns)+" |\n"+"\n".join(
        "| "+" | ".join(fmt(v) for v in row)+" |" for row in df.itertuples(index=False,name=None))
def predictions(out):
    rec=pd.read_csv(out/"recovery_knots.csv");ext=pd.read_csv(out/"recovery_extras.csv")
    rates=pd.read_csv(out/"diagnostic_rates.csv");eff=pd.read_csv(out/"diagnostic_effects.csv");pairs=pd.read_csv(out/"diagnostic_pairs.csv")
    result=[dict(id="P1",status="PASS",detail="All 57 discrepancy checks pass; minimum Bonferroni-adjusted p=1 on each profile.")]
    def each(frame,filters,col):
        q=frame
        for k,v in filters.items():q=q[q[k]==v]
        return {p:float(q.loc[q.panel==p,col].iloc[0]) if (q.panel==p).sum()==1 else np.nan for p in pc.PANELS}
    def decide(id,values,test,text):
        count=sum(bool(test(v)) for v in values.values() if np.isfinite(v))
        result.append(dict(id=id,status="PASS" if count>=2 else "FAIL",
                           detail=text+"; "+", ".join(f"{p}={fmt(v)}" for p,v in values.items())))
    p2=each(ext,dict(condition="baseline",parameter="kappa_edge_fraction"),"mean")
    decide("P2",p2,lambda v:v>=.25,"Mean kappa edge fraction >=.25 on >=2 profiles")
    p3={}
    for p in pc.PANELS:
        vals={}
        for name in ("V","kappa_z"):
            a=rec[(rec.panel==p)&(rec.condition=="baseline")&(rec.parameter==name)&rec.knot.notna()]
            good=(a.truth>0)&np.isfinite(a.truth)&np.isfinite(a.rmse)
            vals[name]=dict(median=float(np.median(a.loc[good,"rmse"]/a.loc[good,"truth"])),included=int(good.sum()),excluded=int((~good).sum()))
        p3[p]=vals
    result.append(dict(id="P3",status="PASS" if sum(v["V"]["median"]<v["kappa_z"]["median"] for v in p3.values())>=2 else "FAIL",
        detail="V relative RMSE < kappa on >=2 profiles; "+json.dumps(p3)))
    p4=each(ext,dict(condition="zero_temperament",parameter="s_positive"),"rate")
    decide("P4",p4,lambda v:v>=.25,"Positive s at true zero in >=25% of seeds on >=2 profiles")
    p5=each(rates,dict(condition="off_1",statistic="departure/rate_absence"),"detection_rate")
    decide("P5",p5,lambda v:v>=.8,"Strong off: absence-exit detection >=.8 on >=2 profiles")
    p6=each(eff,dict(condition="log_volatility_1",control="gaussian_control",statistic="volatility/group=middle/ratio"),"standardized_difference")
    decide("P6",p6,lambda v:v<=-2,"Strong volatility: middle ratio <=-2 matched-control SD on >=2 profiles")
    p7=each(eff,dict(condition="fat_lasting_1",control="baseline",statistic="tails/group=head/h=13/mode=anchor/r9990"),"standardized_difference")
    decide("P7",p7,lambda v:v>=1,"Strong fat lasting: head anchored tail >=1 baseline SD on >=2 profiles")
    base=each(rates,dict(condition="baseline",statistic="far_tail/group=head/z_q9999_up"),"mean")
    bounded=each(rates,dict(condition="bounded_fast_1",statistic="far_tail/group=head/z_q9999_up"),"mean")
    p8={p:(base[p]-bounded[p])/base[p] if base[p]>0 else np.nan for p in pc.PANELS}
    decide("P8",p8,lambda v:v>=.2,"Strong bounded fast: head q9999 reduction >=20% on >=2 profiles")
    q=pairs[(pairs.panel=="facebook_a")&pairs.condition_a.str.startswith("slow_memory")&
            pairs.condition_b.str.startswith("random_walk")&(pairs.statistic=="momentum/block=13/group=middle/pair=skip/slope")]
    result.append(dict(id="P9",status="PASS" if (q.label=="unresolved").any() else "FAIL",
                       detail="T86 slow-memory/RW pairs: "+q[["condition_a","condition_b","label","separation"]].to_json(orient="records")))
    fk=pd.read_csv(out/"fitted_kappa_summary.csv")
    medbase=each(fk,dict(condition="baseline",statistic="middle"),"mean")
    medladder=each(fk,dict(condition="ladder_attraction_1",statistic="middle"),"mean")
    p10={p:medladder[p]/medbase[p] if medbase[p]>0 else np.nan for p in pc.PANELS}
    decide("P10",p10,lambda v:v<=.75,"Strong current-rank attraction: fitted middle kappa <=.75 baseline on >=2 profiles")
    q=pairs[(pairs.condition_a=="random_walk_1")&(pairs.condition_b=="ladder_attraction_1")&
            (pairs.statistic=="dispersion/group=middle/h=26/robust_var")]
    labels={p:q.loc[q.panel==p,"label"].iloc[0] if (q.panel==p).sum()==1 else "unavailable" for p in pc.PANELS}
    result.append(dict(id="P11",status="PASS" if sum(v=="separated" for v in labels.values())>=2 else "FAIL",
                       detail="Strong current-rank attraction vs strong RW, h26 middle dispersion: "+json.dumps(labels)))
    gap=pd.read_csv(out/"annual_cycle_gap_contrast.csv")
    p12=each(gap,dict(condition="annual_cycle_1"),"mean")
    decide("P12",p12,lambda v:v>=.5,"Strong cycle: middle b13 slope gap4-minus-gap2 >=.5 on >=2 profiles")
    medcycle=each(fk,dict(condition="annual_cycle_1",statistic="middle"),"mean")
    p13={p:medcycle[p]-medbase[p] for p in pc.PANELS}
    decide("P13",p13,lambda v:v>=.005,"Strong cycle: fitted middle kappa rises >=.005 on >=2 profiles")
    # No full-study prediction is promoted to PASS when a failed seed makes its denominator incomplete.
    failures=pc.read(out/"recovery_summary.json")["failures"]+pc.read(out/"diagnostic_failures.json")+pc.read(out/"extension_failures.json")
    if failures:
        for r in result[1:]:
            if r["status"]=="PASS":r["status"]="PARTIAL"
    pc.write(out/"predictions.json",result)
    return result,rec,ext,rates,eff,pairs

def compact(out):
    dest=out/"compact";dest.mkdir(exist_ok=True)
    manifest={}
    for panel in pc.PANELS:
        full=pc.read(out/panel/"parameters.json")
        params=full["parameters"]
        # Empirical seed ladders remain local; profile summaries and hashes are enough to audit extraction.
        brief={**full,"parameters":{k:v for k,v in params.items() if k not in ("w0","bottom_mu")}}
        brief["local_full_parameter_sha256"]=pc.digest(out/panel/"parameters.json")
        brief["local_only_arrays"]={k:dict(length=len(params[k])) for k in ("w0","bottom_mu")}
        pc.write(dest/(panel+"_parameters.json"),brief)
    for stage in ("recovery","diagnostics","extension_fits"):
        n=0
        target=dest/(stage+"_seeds.jsonl.gz")
        # mtime=0 makes the compressed archive reproducible.
        with target.open("wb") as raw:
            with gzip.GzipFile(filename="",fileobj=raw,mode="wb",mtime=0) as stream:
                for panel in pc.PANELS:
                    for path in sorted((out/panel/stage).glob("*.json")):
                        r=pc.read(path)
                        if "result" in r:r["result"].pop("fitted_parameters",None)
                        stream.write((json.dumps(r,separators=(",",":"),sort_keys=True)+"\n").encode());n+=1
        manifest[stage]=dict(files=n,sha256=pc.digest(target))
    pc.write(dest/"manifest.json",manifest)

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--out",type=Path,required=True);ap.add_argument("--handoff",type=Path,required=True)
    args=ap.parse_args();out=args.out;hand=args.handoff
    cross=pc.read(out/"crosscheck_summary.json");pc.check_sources(cross)
    rows=[]
    for p,c in cross["panels"].items():
        for j,key in enumerate(cross["keys"]):
            row=dict(panel=p,statistic=key)
            for source in ("production","independent"):
                for name in ("mean","sd","q05","q95"):row[source+"_"+name]=c[source][name][j]
            for name in ("delta","mcse","p","p_bonferroni","null_lower","null_upper","reject"):row[name]=c[name][j]
            rows.append(row)
    frame=pd.DataFrame(rows);frame.to_csv(out/"crosscheck.csv",index=False)
    if cross["status"]!="PASS":raise RuntimeError("Write STOP handoff; remaining V2 stages must not run")
    pred,rec,ext,rates,eff,pairs=predictions(out)
    compact(out)
    text="# V2 — Independent simulation, parameter recovery and planted mechanisms\n\n"
    text+="2026-10-12 · Exploratory, conditional on frozen DEVELOPMENT fits. **No change adopted.**\n\n"
    text+="The independent simulator passes the fixed 57-comparison baseline discrepancy check. "
    text+="All native baseline and zero-heterogeneity recovery fits are included. Reversion often lands at an estimator bound; "
    text+="true zero heterogeneity is returned as positive on all 20 seeds of every profile. Facebook tail degrees of freedom "
    text+="are returned as infinity on all baseline and zero-heterogeneity fits despite finite generating tails. "
    text+="These are operational recovery failures, not failed execution. Diagnostic findings retain all declared strengths. "
    text+="A passed cross-check is not proof of equivalence; conditional diagnostic separation is not unique mechanism identification.\n\n"
    text+="## Design, provenance and execution\n\n"
    text+="Base main: 4ae73b43b852ac540cadd39808f27200885f1be7. OpenAI Codex. "
    text+="DESIGN committed at 4f823c3; independent evaluator at 0e401dd; study/reporting evaluator at 57428df; dated two-case addendum at aca704f. "
    text+="Protected models and frozen defaults unchanged. No movement-gate calls.\n\n"
    text+="Commands (registered Python 3.11.6):\n\n"
    text+="\u0060\u0060\u0060bash\nbash llm_fitting/handoff/V2_planted_mechanisms/launch_crosscheck.sh\nbash llm_fitting/handoff/V2_planted_mechanisms/launch_study.sh\n\u0060\u0060\u0060\n\n"
    text+="For a restart of the full added-case/diagnostic study, use `bash llm_fitting/handoff/V2_planted_mechanisms/launch_complete_efficient.sh`; "
    text+="it skips complete atomic cells and runs `planted_summary.py --include-extensions`. Then run `planted_audit.py` and "
    text+="`planted_report.py`, each with `--out llm_fitting/runs/2026-10-12_V2_planted_mechanisms`; the report also takes "
    text+="`--handoff llm_fitting/handoff/V2_planted_mechanisms`. All scripts live under `llm_fitting/validation/`.\n\n"
    text+="Launchers use caffeinate and heavy_lock. Native profiles run through the shared lock; each seed writes an atomic, source-identified checkpoint. "
    text+="Full empirical seed ladders and large per-seed fitted objects stay local. Compact seed evidence, source/input hashes, tables and logs are provided. "
    text+="A1/A2 real-panel findings were not opened before this handoff.\n\n"
    text+="## Baseline cross-check\n\n"
    text+="Twenty production seeds 0–19 and twenty independent seeds 1000–1019 per profile; 19 macro statistics each. "
    text+="All 57 pass the predeclared 20,000-permutation, Bonferroni .05 family rule. Minimum adjusted p is 1 on every profile. "
    text+="S1/S10/S100 are shares within the simulator's recorded top min(N,2000), not empirical platform-wide shares. "
    text+="The full table is crosscheck.csv; it gives both mean ± SD, 5–95% seed bands, difference/MCSE and simultaneous permutation-null bands.\n\n"
    for p in pc.PANELS:
        q=frame[frame.panel==p].copy()
        q["production mean ± SD [5–95%]"]=q.apply(lambda r:f'{r.production_mean:.5g} ± {r.production_sd:.3g} [{r.production_q05:.5g}, {r.production_q95:.5g}]',axis=1)
        q["independent mean ± SD [5–95%]"]=q.apply(lambda r:f'{r.independent_mean:.5g} ± {r.independent_sd:.3g} [{r.independent_q05:.5g}, {r.independent_q95:.5g}]',axis=1)
        text+="### "+LABELS[p]+"\n\n"+md(q,["statistic","production mean ± SD [5–95%]","independent mean ± SD [5–95%]","p_bonferroni"])+"\n\n"
    text+="## Recovery\n\n"
    text+="Twenty synthetic panels at frozen parameters and twenty at true s=0 per native profile. "
    text+="The real intake mapping, absence-penalized 4K membership and S4 minimum-distance estimator are used. "
    text+="Zero metrics are retained. Rank-varying coefficients are generated on latent-level rank, while estimation uses mean observed-rank bands. "
    text+="Thus this is operational recovery, including observation-floor and selection effects, not a fixed-band asymptotic test.\n\n"
    e=ext[ext.parameter.isin(["kappa_edge_fraction","zero_floor_fraction","s_positive"])].copy()
    text+=md(e,["panel","condition","parameter","valid","mean","sd","rate","lower","upper","failed"])+"\n\n"
    scal=rec[rec.group=="scalar"].copy()
    text+=md(scal,["panel","condition","parameter","truth","mean","bias","sd","rmse","mcse_mean","valid","positive_infinity"])+"\n\n"
    text+="All knot-level biases, SDs, RMSEs, quantiles, MCSEs and valid counts are in recovery_knots.csv. "
    text+="recovery_groups.csv averages knots without pretending knots are independent simulation replications. "
    text+="recovery_extras.csv includes NNLS-zero fractions, kappa-edge fractions, sample sizes and s=0 positive-estimate rates. "
    text+="MCSE of SD/RMSE uses the seed jackknife. Positive s frequency is not a nominal Type I error rate. "
    text+="The column `valid` counts finite estimates; infinite degrees of freedom are valid boundary outputs, not failed fits. "
    text+="Infinite t degrees of freedom are retained as an atom; finite conditional summaries and 1/(df−4) are reported separately. "
    text+="V=sp²/(2 kappa) and its discrete counterpart are local functionals, not claims about global stationary endpoint variance.\n\n"
    compare=[]
    for p in pc.PANELS:
        d=dict(panel=p)
        for par in ("V","kappa_z"):
            a=rec[(rec.panel==p)&(rec.condition=="baseline")&(rec.parameter==par)&rec.knot.notna()]
            good=(a.truth>0)&np.isfinite(a.truth)&np.isfinite(a.rmse)
            d[par+" median relative RMSE"]=float(np.median(a.loc[good,"rmse"]/a.loc[good,"truth"]))
        compare.append(d)
    text+=md(pd.DataFrame(compare))+"\n\n"
    text+="V recovers better than kappa by this comparison, but its absolute error remains substantial. "
    text+="The estimator does not yet justify interpreting fitted reversion, tail thickness or a small positive heterogeneity "
    text+="estimate as a well-recovered mechanism. This conclusion is limited to these native frozen profiles and observation rules.\n\n"
    text+="## Fixed diagnostic display\n\n"
    text+="Baseline calibration uses 39 independent seeds 2000–2038. Evaluation uses 20 paired seeds 1000–1019 per condition. "
    text+="Strict outside-min/max rates are per-statistic; their nominal bound of 2/40=.05 is averaged over calibration sets. "
    text+="The conditional binomial MCSE/Wilson interval does not include calibration-set uncertainty. Baseline evaluation seeds "
    text+="also entered the earlier cross-check; these intervals are descriptive references, not claimed post-selection coverage. "
    text+="The any-statistic baseline false-alarm rate is separately reported and is not a 5% global test.\n\n"
    for p in pc.PANELS:
        d=rates[(rates.panel==p)&rates.statistic.isin([x[1] for x in DISPLAY])].copy()
        d["display"]=d.statistic.map({k:v for v,k in DISPLAY})
        d["rate [95% Wilson]"]=d.apply(lambda r:f"{r.detection_rate:.2f} [{r.detection_lower:.2f}, {r.detection_upper:.2f}]" if np.isfinite(r.detection_rate) else "unavailable",axis=1)
        pivot=d.pivot(index="display",columns="condition",values="rate [95% Wilson]")
        pivot=pivot.reindex([x[0] for x in DISPLAY]).reset_index()
        text+="### "+LABELS[p]+" — fraction outside baseline envelope\n\n"
        columns=[c for c in pivot.columns if c!="display"]
        for first in range(0,len(columns),6):
            text+=md(pivot,["display"]+columns[first:first+6])+"\n\n"
    text+="See diagnostic_rates.csv for all statistics, means/SDs/seed bands, rates/MCSEs/Wilson intervals and missing denominators; "
    text+="diagnostic_effects.csv for signed baseline/control SD effects and paired MCSE; diagnostic_pairs.csv for every condition pair, "
    text+="its pooled-SD separation and 5–95% bands. Descriptive labels are unresolved (<1 SD), separated (>=2 SD and disjoint bands), "
    text+="or overlapping; degenerate/missing spreads remain labelled. Gaussian fixed-volatility controls isolate changing volatility "
    text+="from removal of the original passing t tail. All strengths are retained.\n\n"
    anyflag=pd.read_csv(out/"diagnostic_any_flag.csv")
    text+="The separation labels are descriptive, not hypothesis tests; uncertainty in the 20-seed standardizing SD is not propagated. "
    text+="Lasting t innovations with df=3 have no fourth moment, so variance-based SDs and MCSEs can be unstable; "
    text+="quantile bands and robust diagnostics are supplied alongside them.\n\n"
    text+="Baseline any-statistic false-alarm rate:\n\n"+md(anyflag[anyflag.condition=="baseline"])+"\n\n"
    text+="## Added cases: fitted reversion and annual signatures\n\n"
    text+="The dated addendum fixes current-rank attraction at .02/.10 and annual mean peak amplitudes .5/1.5 log units. "
    text+="The additional simulator is versioned separately and bit-identical on controlled original-mechanism cases. "
    text+="Its fitted home kappa is a misspecification diagnostic, not recovery of the current-rank attraction coefficient. "
    text+="Both added cases and both random-walk strengths have 20 fits per profile.\n\n"
    fk=pd.read_csv(out/"fitted_kappa_summary.csv")
    text+=md(fk[fk.statistic=="middle"],["panel","condition","mean","sd","q05","q95","mcse_mean","valid"])+"\n\n"
    text+="fitted_kappa_knots.csv retains every fitted profile; fitted_kappa_pairs.csv gives every pair and group under the original separation rule. "
    text+="annual_cycle_gap_contrast.csv gives the paired gap4-minus-gap2 slope, SD and MCSE.\n\n"
    text+="## Predictions\n\n"+md(pd.DataFrame(pred))+"\n\n"
    failed=pc.read(out/"recovery_summary.json")["failures"]+pc.read(out/"diagnostic_failures.json")+pc.read(out/"extension_failures.json")
    text+="## Failures, limits and work not run\n\n"
    text+=f"Failed seed records: {len(failed)}. Every failed seed is preserved in the summary and denominator accounting. "
    text+="The original diagnostic workers stalled in quadratic warning capture and were terminated; three queued workers were cancelled. "
    text+="Two completed added-case fit cells from those attempts were archived unused and replayed; the 120 completed original recovery fits were retained. "
    text+="A warning-only wrapper bounded capture without changing mathematical functions. One long-panel restart then failed before scientific work "
    text+="because attempt filenames collided; panel/stage/PID names fixed this, and the worker restarted. "
    text+="The comments worker was later restarted after an exact eligibility optimization passed native and controlled equivalence checks. "
    text+="Complete seed cells were retained; fits lacking their companion diagnostic were archived unused and replayed. "
    text+="A redundant queued long-panel diagnostics request was cancelled before execution. Immutable STARTED records from terminated processes "
    text+="are resolved by separate attempt-event records, not rewritten as successful. No incomplete seed or partial aggregate is interpreted. "
    text+="The eligibility wrapper preserves the union of the two declared cohorts, original absence floor, identity order and every numeric diagnostic/count. "
    text+="Runtime wrapper hashes identify mixed execution versions; the scientific producer hashes are constant. "
    text+="See audit.json, eligibility_equivalence.json, attempt events and launch logs.\n\n"
    text+="No empirical A1/A2 comparison, candidate gate, default change, daily-instrument recovery, parameter-refitted composite mechanism "
    text+="comparison, isolated panel-length factorial, or new-data confirmation was run. Native profile and length are confounded. "
    text+="Each diagnostic's finite-sample power is conditional on the frozen nuisance parameters. Tail disappearance from a statistic "
    text+="does not by itself establish the absence of a mechanism.\n\n"
    text+="## Reproduction and review files\n\n"
    text+="Code under validation/: planted_sim.py, planted_crosscheck.py, planted_study.py, planted_extended.py, planted_extension.py, "
    text+="planted_summary.py, planted_extension_summary.py, planted_report.py and planted_audit.py; execution-only wrappers are "
    text+="planted_warning_guard.py and planted_efficient_run.py. "
    text+="Synthetic known-answer tests cover covariance, the baseline and all eight mechanisms, replacement identities, loader mapping, permutation logic and reporting. "
    text+="`planted_plot.py --out <run folder>` regenerates recovery bands and the fixed diagnostic-rate display under `output/v2_plots/`; images stay out of git. "
    text+="The final HEALTH log records both suites and the 14/15, churn .013 legacy guard. "
    text+="Sources: [ADEMP and Monte Carlo reporting](https://pmc.ncbi.nlm.nih.gov/articles/PMC6492164/); "
    text+="[finite randomization p values](https://gksmyth.github.io/pubs/PermPValuesPreprint.pdf).\n"
    (hand/"REPORT.md").write_text(text)
    pc.write(out/"report_provenance.json",dict(report_sha256=pc.digest(__file__),sources=study.sources()))
if __name__=="__main__":main()
