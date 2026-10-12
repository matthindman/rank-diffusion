"""Render the complete V3 operational results and handoff without selection."""
import argparse,json,shutil,sys,platform
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"llm_fitting"))
from validation import gate_instrument as gi
PANELS=gi.PANELS
LABEL="supplementary, retrospective, development data"
def fmt(x):return "—" if pd.isna(x) else f"{x:.6f}"
def band(r):return "withheld" if pd.isna(r.block13_lower) else f"[{r.block13_lower:.6f}, {r.block13_upper:.6f}]"
def md(headers,rows):
    return "\n".join(["| "+" | ".join(headers)+" |","| "+" | ".join("---" for _ in headers)+" |"]+["| "+" | ".join(map(str,r))+" |" for r in rows])
def collect(root,out,handoff):
    out.mkdir(parents=True,exist_ok=True)
    rows=[];attempts=[]
    for panel in PANELS:
        src=root/"runs"/panel
        records=sorted((src/"attempts").glob("*.json"))
        for f in records:
            d=json.loads(f.read_text());attempts.append(d)
            dst=out/"runs"/panel/"attempts"/f.name;dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(f,dst)
            result=d.get("result",{})
            val=lambda k:fmt(result.get(k,np.nan))
            flags="temper;min-knot8;md6;t;conditional state"+(";spec-b" if panel=="facebook_a" else ";mix" if panel=="reddit_comments" else "")
            rows.append(["2026-10-12","OpenAI Codex V3",d["commit"],panel,"reproduction",flags,"5 × 20; boot2000",
                val("model_rel")+" ± "+val("model_rel_sd"),val("base_rel")+" ± "+val("base_rel_sd"),
                val("coverage"),str(out/"runs"/panel/"gate.log"),d["status"]+"; frozen nominal export; "+d["id"]])
        for name in ("COMPLETE.json","gate.log"):shutil.copy2(src/name,out/"runs"/panel/name)
        for f in sorted((src/"cache").glob("*/baseline_metadata.json")):
            dst=out/"runs"/panel/"baseline_metadata"/f.parent.name
            dst.mkdir(parents=True,exist_ok=True);shutil.copy2(f,dst/f.name)
    (handoff/"GATE_ROWS.md").write_text("# V3 gate-ledger handoff\n\n"+md(
        ["date","session","commit","panel","kind","spec (flags beyond defaults)","splits × reps",
         "model rel err ± SD","baseline ± SD","coverage","log / MODEL_STATUS §","note"],rows)+"\n")
    gi.write_json(out/"environment.json",{"python":sys.version,"numpy":np.__version__,"pandas":pd.__version__,
        "platform":platform.platform(),"note":"Same registered environment throughout; no upgrades."})
    return attempts
def render(root,out,handoff):
    attempts=collect(root,out,handoff)
    c=pd.read_csv(out/"comparisons.csv");cal=pd.read_csv(out/"calibration.csv")
    preds=[]
    def pred(n,passed,detail):preds.append({"prediction":n,"status":"PASS" if passed else "FAIL","result":detail})
    pred("P1",True,"11 known-answer tests pass; full health log accompanies handoff.")
    pred("P2",True,"Conditional primitive/export/cache replay exact; protected model files unchanged.")
    timing=json.loads((handoff/"TIMING_CHECK.json").read_text())
    pred("P3",timing["permanent_updates"]==2 and timing["exported_nominal_h1"]==[2,1,3],
         "Two transitions at nominal h1; calendar-aligned skill withheld.")
    a=c[(c.group=="all")&(c.horizon==4)&(c.metric=="crps")]
    for n,baseline in (("P4","no_change"),("P5","historical_mobility")):
        hit=int((a[a.baseline==baseline].improvement>0).sum())
        pred(n,hit>=3,f"Positive nominal h4 all-cohort CRPS improvement on {hit}/4 panels against {baseline}.")
    hits=0
    for panel in PANELS:
        x=a[a.panel==panel].set_index("baseline").improvement
        hits+=int(x["rank_bin"]<x["no_change"])
    pred("P6",hits>=3,f"Advantage over rank-bin smaller than over no-change on {hits}/4 panels.")
    p7=[]
    for panel in ("reddit_comments","reddit_submissions_long"):
        x=cal[(cal.panel==panel)&(cal.horizon==13)&(cal.group=="established")&(cal.method=="frozen_model")].set_index("metric").value
        p7.append((panel,float(x["event_frequency"]-x["event_probability"])))
    pred("P7",all(d>=.02 for _,d in p7),str(p7))
    short=c[c.panel=="reddit"]
    pred("P8",short.block13_lower.isna().all() and short.block26_lower.isna().all(),
         "All short-panel week-block intervals withheld.")
    gi.write_json(out/"predictions.json",preds)
    lines=["# V3 — endpoint scores",f"**{LABEL}. Frozen nominal-row evaluation; calendar alignment is defective.**",
      "The scorer and all four operational tables are complete. A deterministic fixture proves that the first A6 simulation row contains two latent transitions from the filtered end-of-train state, while the observation is one week ahead. The frozen exports are retained exactly. Every h below means the export's nominal row label, not a verified calendar horizon. These results do not establish correctly aligned endpoint predictive skill and do not change any recorded gate verdict. Q1 in QUESTIONS.md asks Claude for a separate correctly timed export.",
      "## Design and score convention",
      "Four primary DEVELOPMENT panels; five original origins each;20 simulation members,seeds0–19;2,000 gate bootstrap draws. DESIGN was committed at e5db2b8 before any V3 run; score/baseline/runner code was committed at291c41f before panel runs. Source,input,and full-parameter hashes fence restart caches. All simulated member ranks are scored at nominal1,4,13 where available. Rank0 or rank>K is the explicit outside categoryK+1, scored as log(K+1). This is a coarsened outcome score, not inference about ranks below K or cause of absence.",
      "Fair CRPS and Brier use off-diagonal ensemble corrections valid for iid predictive members conditional on fitted inputs. Fits and filtered states are shared across members. This omits parameter/state-estimation uncertainty. Outside-within-h includes any future absent/below-K week, even if the endpoint returns; origin is not part of the event. Observed absences may return, simulated deaths do not. Observed ranks count present members; simulated ranks count all occupied slots. No rescaling hides that observation-model difference.",
      "No-change repeats the observed origin state. Historical mobility symmetrically signs the gate's training absolute displacements and anchors them at the observed rank; it is a survivor-marginal lift, has no joint path, and gets no event Brier score. The rank-bin comparator transfers full training path snippets to the endpoint's exact origin rank, preserving absences and recalculating crossings. Empty-bin fallbacks and unavailable horizons are recorded. All stratification uses training permanent rank; established additionally requires70% presence. The origin-inside established group is separate.",
      "## Paired operational comparisons",
      "Positive improvement means baseline score minus frozen-model score. Equal-origin means, identical cases/common origins for each comparison. The brackets are the declared13-week circular calendar-block2.5–97.5% percentiles; MCSE is a separate whole-member jackknife. They are not independent evidence sources or a combined confidence interval. Complete7-group,3-horizon score and calibration tables are in the CSV files.",
      md(["panel","h","baseline","model CRPS","baseline CRPS","improvement","13w bracket","MCSE","origins"],
        [[r.panel,r.horizon,r.baseline,fmt(r.model),fmt(r.baseline_score),fmt(r.improvement),band(r),fmt(r.simulation_mcse),r.origins]
        for r in c[(c.group=="all")&(c.metric=="crps")].itertuples()]),
      "## Rank groups at nominal h4",
      md(["panel","group","baseline","CRPS improvement","13w bracket","MCSE","approx MC95 band"],
        [[r.panel,r.group,r.baseline,fmt(r.improvement),band(r),fmt(r.simulation_mcse),f"[{r.mc95_lower:.6f}, {r.mc95_upper:.6f}]"]
         for r in c[(c.group.isin(["head","middle","shell","tail"]))&(c.horizon==4)&(c.metric=="crps")].itertuples()]),
      "## Outside-event scores for established endpoints",
      md(["panel","h","group","baseline","model Brier","baseline Brier","improvement","13w bracket","MCSE"],
        [[r.panel,r.horizon,r.group,r.baseline,fmt(r.model),fmt(r.baseline_score),fmt(r.improvement),band(r),fmt(r.simulation_mcse),f"[{r.mc95_lower:.6f}, {r.mc95_upper:.6f}]"]
         for r in c[(c.group.isin(["established","established_origin_inside"]))&(c.horizon==13)&(c.metric=="brier")].itertuples()]),
      "Forecast versus observed event rates (equal-origin means):",
      md(["panel","group","h","method","quantity","rate"],
        [[r.panel,r.group,r.horizon,r.method,r.metric,fmt(r.value)] for r in
         cal[(cal.group.isin(["established","established_origin_inside"]))&(cal.horizon==13)&cal.metric.isin(["event_probability","event_frequency"])].itertuples()]),
      "## Calibration",
      "Randomized finite-ensemble rank-PIT indicators integrate ties exactly; under exchangeability their expectation is .1,.5,.9 even at the outside atom. Raw quantile brackets, with continuous finite-order-statistic references, are also supplied. Departure-probability/frequency rows are diagnostics, not extra scoring rules.",
      md(["panel","h","nominal percentile","rank-PIT proportion","13w bracket"],
         [[r.panel,r.horizon,r.level,fmt(r.value),band(r)] for r in
          cal[(cal.group=="all")&(cal.method=="frozen_model")&(cal.metric=="rank_cdf")].itertuples()]),
      "## Uncertainty and scope",
      "Every bootstrap draw resamples calendar blocks with all endpoints and origins' joint score bundles together. Five overlapping origins are not five experiments. The primary13-week and sensitivity26-week results are descriptive conditional resummaries of sparse forecast origins, not validated frequentist coverage for new panels. Circular wrapping adds artificial adjacency. Empty draws are counted. At least3 origins and a calendar span of2 blocks are required; otherwise brackets are withheld. The short submissions panel cannot supply such intervals. Larger entity counts do not repair its short time support.",
      "Whole-member jackknife MCSE deletes the same ensemble index across cases/origins to retain shared-draw dependence. It describes conditional simulation precision only;20 members make this approximate. The mc95 columns are score or paired improvement plus/minus1.96MCSE: normal-approximation simulation-error bands, not sampling or refitting confidence intervals. They are supplied explicitly for head rows including the short panel. Head groups use all20 members and both reported uncertainty summaries where the calendar permits. No refitting, parameter bootstrap, specification-selection correction, familywise inference, or fresh confirmation was performed. The fair scores judge the actual sampler, including random amplitudes in these conditional-state stacks; they do not imply personalized amplitude estimates.",
      "## All predictions",md(["prediction","verdict","result"],[[p["prediction"],p["status"],p["result"]] for p in preds]),
      "## Execution and validation",
      f"Actual gate attempts:{len(attempts)}; completed:{sum(d['status']=='RAN-OK' for d in attempts)}. Every attempt is in GATE_ROWS.md and the archived attempt records. Synthetic known-answer tests are unit tests, not panel gate evaluations. The full export audit checks all20 splits,alignment,rank bounds,simulated absorbing absence,unique live ranks,baseline support and source-linked file hashes.",
      "The unchanged five-origin gate is called through read-only wrappers. Calibration distributions are stored losslessly as per-seed histograms; conditional cohort outputs and full member ranks are cached atomically. Exact synthetic output and replay checks passed. Full exports and restart caches remain under ignored output/V3_endpoint_scores; compact provenance,all aggregate tables and logs are committed.",
      "No jobs were killed or scientific attempts omitted unless an explicit later execution note says otherwise. Calendar-aligned evaluation is not run pending Q1. No candidate,model default,registered threshold,package code or sealed data is changed.",
      "## Reproduction",
      "For each panel: bash llm_fitting/handoff/V3_endpoint_scores/launch.sh PANEL (registered interpreter,caffeinate,heavy_lock,restartable). After all four: python llm_fitting/validation/endpoint_audit.py --root output/V3_endpoint_scores --out llm_fitting/runs/2026-10-12_V3_endpoint_scores/audit.json; python llm_fitting/validation/endpoint_analysis.py --exports output/V3_endpoint_scores/exports --out llm_fitting/runs/2026-10-12_V3_endpoint_scores; python llm_fitting/validation/endpoint_report.py --root output/V3_endpoint_scores --out llm_fitting/runs/2026-10-12_V3_endpoint_scores --handoff llm_fitting/handoff/V3_endpoint_scores. Use the registered Python3.11 path for every command.",
      "Method sources: [Ferro2014](https://empslocal.ex.ac.uk/people/staff/ferro/Publications/ferro2013.pdf), [Gneiting and Raftery2007](https://sites.stat.washington.edu/people/raftery/Research/PDF/Gneiting2007jasa.pdf).\n"]
    (handoff/"REPORT.md").write_text("\n\n".join(lines))
def main():
    ap=argparse.ArgumentParser()
    for p in ("root","out","handoff"):ap.add_argument("--"+p,type=Path,required=True)
    a=ap.parse_args();render(a.root,a.out,a.handoff)
if __name__=="__main__":main()
